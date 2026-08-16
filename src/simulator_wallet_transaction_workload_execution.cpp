#include "simulator_wallet_transaction_workload_execution.h"

#include <algorithm>
#include <atomic>
#include <chrono>
#include <exception>
#include <limits>
#include <memory>
#include <mutex>
#include <optional>
#include <stdexcept>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include "bbp/logging.h"
#include "bbp/runtime_node_inventory.h"
#include "bbp/simulation_cancelled.h"
#include "bbp/simulation_event_kind.h"
#include "bbp/simulation_registry.h"
#include "bbp/simulator/constants.h"
#include "bbp/simulator/node_runtime.h"
#include "bbp/simulator/options.h"
#include "bbp/simulator/wallet_funding_strategy.h"
#include "simulator_block_generation_boundary.h"
#include "simulator_cancellable_waiting.h"
#include "simulator_event_writing.h"
#include "simulator_node_process_state.h"
#include "simulator_transaction_observation_tracking.h"
#include "simulator_wallet_transaction_validation.h"
#include "simulator_workload_event_details.h"

namespace bbp::simulator_app_internal {
namespace {

class WalletWorkloadSettleRequested final : public std::exception {
 public:
  const char* what() const noexcept override {
    return "wallet workload settlement requested";
  }
};

bool WaitUntilWalletWorkloadDeadline(
    std::chrono::steady_clock::time_point deadline, std::stop_token stop_token,
    const std::function<bool()>& yield_requested) {
  constexpr std::chrono::milliseconds kMaximumPoll{25};
  while (std::chrono::steady_clock::now() < deadline) {
    ThrowIfStopRequested(stop_token);
    if (yield_requested && yield_requested()) {
      return false;
    }
    const auto poll_deadline =
        std::min(deadline, std::chrono::steady_clock::now() + kMaximumPoll);
    WaitUntil(poll_deadline, stop_token);
  }
  ThrowIfStopRequested(stop_token);
  return !yield_requested || !yield_requested();
}

}  // namespace

WalletWorkloadExecutionResult ApplyWalletTransactionsWorkload(
    const Options& options, const std::filesystem::path& events_path,
    const ChainDriver& driver, std::timed_mutex& block_generation_mutex,
    const RuntimeNodeSnapshot& nodes, const SimulationRegistry& registry,
    TransactionObservationTracker& transaction_tracker,
    std::vector<PendingTransactionLoadCompletion>* pending_load_completions,
    const WalletTransactionsWorkload& workload, std::uint32_t workload_index,
    std::uint32_t workload_count, std::stop_token stop_token,
    WalletWorkloadExecutionContext* execution) {
  const std::vector<WalletIdentity>& wallets = registry.wallets();
  std::vector<WalletWorkloadFundingState> new_funding;
  std::vector<WalletWorkloadFundingState>* funding = nullptr;
  if (execution != nullptr && execution->prepared_funding != nullptr &&
      execution->prepared_funding->has_value()) {
    funding = &execution->prepared_funding->value();
  } else {
    const std::vector<uint32_t> funding_miner_indexes = WalletFundingMinerNodes(
        registry.topology().miner_nodes, wallets.size(),
        workload.funding_strategy, workload.random_seed);
    new_funding.reserve(wallets.size());
    for (size_t wallet_index = 0; wallet_index < wallets.size();
         ++wallet_index) {
      ThrowIfStopRequested(stop_token);
      const WalletIdentity& wallet = wallets[wallet_index];
      if (wallet.address.empty() || wallet.funding_address.empty()) {
        throw std::runtime_error(
            "wallet-backed workload requires initialized WalletNode addresses "
            "and funding addresses");
      }
      const uint32_t miner_node = funding_miner_indexes[wallet_index] + 1U;
      NodeRuntime& miner = nodes[miner_node - 1U];
      RequireNodeRunning(miner, "wallet funding");
      WalletWorkloadFundingState state;
      state.miner_node = miner_node;
      state.start_height = driver.ReadMetrics(miner.config, stop_token).height;
      state.hashes =
          GenerateBlocksSerialized(block_generation_mutex, driver, miner.config,
                                   workload.funding_blocks_per_wallet,
                                   wallet.funding_address, stop_token);
      RecordGeneratedBlocks(driver, miner, state.hashes, stop_token);
      if (state.start_height >
          std::numeric_limits<std::uint64_t>::max() - state.hashes.size()) {
        throw std::runtime_error("wallet funding target height overflow");
      }
      state.target_height =
          state.start_height + static_cast<uint64_t>(state.hashes.size());
      for (NodeRuntime& node : nodes) {
        if (!node.AllowsChainMetrics()) {
          continue;
        }
        driver.WaitForHeight(node.config, state.target_height,
                             std::chrono::seconds(workload.timeout_sec),
                             stop_token);
      }
      NodeRuntime& wallet_node = nodes[wallet.node - 1U];
      RequireNodeRunning(wallet_node, "wallet funding preparation");
      state.preparation = driver.PrepareWalletFunding(
          wallet_node.config,
          ToChainWalletMode(registry.wallet_initialization()), wallet.address,
          workload.funding_threshold_satoshis, workload.readiness_confirmations,
          std::chrono::seconds(workload.timeout_sec), stop_token);
      for (const std::string& txid : state.preparation.txids) {
        for (NodeRuntime& node : nodes) {
          if (!node.AllowsChainMetrics()) {
            continue;
          }
          driver.WaitForMempoolTransaction(
              node.config, txid, std::chrono::seconds(workload.timeout_sec),
              stop_token);
        }
      }
      state.ready_height = state.target_height;
      if (state.preparation.confirmation_blocks_required != 0U &&
          state.preparation.txids.empty()) {
        throw std::runtime_error(
            "wallet funding preparation requested confirmation blocks without "
            "transaction ids");
      }
      std::uint64_t preparation_block_count =
          state.preparation.confirmation_blocks_required;
      if (state.preparation.minimum_chain_height > state.ready_height) {
        preparation_block_count = std::max(
            preparation_block_count,
            state.preparation.minimum_chain_height - state.ready_height);
      }
      if (preparation_block_count > std::numeric_limits<std::uint32_t>::max()) {
        throw std::runtime_error(
            "wallet funding preparation block count exceeds uint32");
      }
      if (preparation_block_count != 0U) {
        state.preparation_hashes = GenerateBlocksSerialized(
            block_generation_mutex, driver, miner.config,
            static_cast<std::uint32_t>(preparation_block_count),
            wallet.funding_address, stop_token);
        if (state.preparation_hashes.size() != preparation_block_count) {
          throw std::runtime_error(
              "wallet funding preparation returned an unexpected block count");
        }
        RecordGeneratedBlocks(driver, miner, state.preparation_hashes,
                              stop_token);
        if (state.ready_height > std::numeric_limits<std::uint64_t>::max() -
                                     state.preparation_hashes.size()) {
          throw std::runtime_error("wallet funding ready height overflow");
        }
        state.ready_height +=
            static_cast<uint64_t>(state.preparation_hashes.size());
        for (NodeRuntime& node : nodes) {
          if (!node.AllowsChainMetrics()) {
            continue;
          }
          driver.WaitForHeight(node.config, state.ready_height,
                               std::chrono::seconds(workload.timeout_sec),
                               stop_token);
        }
      }
      if (state.ready_height < state.preparation.minimum_chain_height) {
        throw std::runtime_error(
            "wallet funding preparation did not reach its minimum chain "
            "height");
      }
      state.ready_balance_satoshis = driver.WaitForWalletBalance(
          wallet_node.config,
          ToChainWalletMode(registry.wallet_initialization()),
          workload.funding_threshold_satoshis, workload.readiness_confirmations,
          std::chrono::seconds(workload.timeout_sec), stop_token);
      WriteEvent(events_path, options.run_id, wallet_node.config.id,
                 SimulationEventKind::kWalletFunded,
                 WalletFundingDetail(
                     workload_index, workload_count, workload, wallet,
                     miner_node, state.start_height, state.target_height,
                     static_cast<uint64_t>(state.hashes.size()),
                     state.ready_height, state.preparation,
                     static_cast<uint64_t>(state.preparation_hashes.size()),
                     state.ready_balance_satoshis));
      new_funding.push_back(std::move(state));
    }
    if (execution != nullptr && execution->prepared_funding != nullptr) {
      execution->prepared_funding->emplace(std::move(new_funding));
      funding = &execution->prepared_funding->value();
    } else {
      funding = &new_funding;
    }
  }
  if (funding == nullptr || funding->size() != wallets.size()) {
    throw std::runtime_error(
        "wallet workload funding preparation is incomplete");
  }

  if (execution && execution->execution_started) {
    execution->execution_started();
  }

  if (IsTransactionLoadStrategy(workload.strategy)) {
    std::vector<std::uint64_t> initial_available_balances(wallets.size(), 0U);
    for (std::size_t wallet_index = 0U; wallet_index < wallets.size();
         ++wallet_index) {
      ThrowIfStopRequested(stop_token);
      const WalletIdentity& wallet = wallets[wallet_index];
      const NodeRuntime& wallet_node = nodes[wallet.node - 1U];
      initial_available_balances[wallet_index] =
          driver
              .ReadWalletSnapshot(
                  wallet_node.config,
                  ToChainWalletMode(registry.wallet_initialization()), 1U,
                  stop_token)
              .available_balance_satoshis;
    }

    const std::optional<std::uint64_t> attempt_limit =
        ExplicitWalletTransactionAttemptLimit(workload);
    WalletTransactionLoadPlanner planner(
        wallets.size(), workload,
        execution ? execution->next_transaction_index : 0U);
    BoundedWalletTransactionQueue queue(workload.queue_capacity);
    if (workload.queue_capacity >
            std::numeric_limits<std::size_t>::max() - workload.concurrency ||
        workload.queue_capacity + workload.concurrency >
            std::numeric_limits<std::size_t>::max() - planner.batch_size()) {
      throw std::runtime_error(
          "transaction load reservation bound overflows size_t");
    }
    TransactionLoadBalanceReservations balance_reservations(
        std::move(initial_available_balances),
        EffectiveWalletTransactionFeeReserveSatoshis(workload),
        workload.queue_capacity + workload.concurrency + planner.batch_size());
    std::shared_ptr<TransactionLoadAccounting> accounting =
        execution ? execution->accounting
                  : std::make_shared<TransactionLoadAccounting>();
    if (!accounting) {
      throw std::runtime_error(
          "wallet workload execution accounting is missing");
    }
    std::vector<std::mutex> sender_submission_mutexes(wallets.size());
    const auto load_started_at =
        execution ? execution->started_at : std::chrono::steady_clock::now();
    const std::optional<std::chrono::steady_clock::time_point>
        duration_deadline =
            workload.duration
                ? std::optional<std::chrono::steady_clock::time_point>(
                      SteadyDeadline(
                          load_started_at,
                          options.time_scale.WallDuration(*workload.duration)))
                : std::nullopt;
    std::mutex infrastructure_error_mutex;
    std::exception_ptr infrastructure_error;
    std::atomic<bool> worker_cancelled = false;
    std::stop_source load_stop_source;
    std::stop_callback propagate_stop(stop_token, [&load_stop_source] {
      static_cast<void>(load_stop_source.request_stop());
    });
    const std::stop_token load_stop_token = load_stop_source.get_token();

    const auto record_infrastructure_error =
        [&infrastructure_error_mutex,
         &infrastructure_error](std::exception_ptr error) {
          std::lock_guard<std::mutex> lock(infrastructure_error_mutex);
          if (!infrastructure_error) {
            infrastructure_error = std::move(error);
          }
        };
    const auto record_load_progress =
        [&](const TransactionLoadSnapshot& value) {
          WriteTransactionLoadProgress(options, events_path, workload_index,
                                       workload_count, value);
        };
    const auto terminal_latency = [](const WalletTransactionLoadTask& task) {
      const auto now = std::chrono::steady_clock::now();
      if (now <= task.scheduled_at) {
        return std::chrono::microseconds(0);
      }
      return std::chrono::duration_cast<std::chrono::microseconds>(
          now - task.scheduled_at);
    };
    const auto record_dropped_tasks =
        [&](std::vector<WalletTransactionLoadTask> dropped_tasks) {
          for (const WalletTransactionLoadTask& task : dropped_tasks) {
            try {
              balance_reservations.Settle(
                  task.transaction_index, std::nullopt, true,
                  execution ? execution->record_released_atomic_units
                            : std::function<void(std::uint64_t)>{});
            } catch (...) {
              record_infrastructure_error(std::current_exception());
            }
            try {
              const TransactionLoadSnapshot progress =
                  accounting->RecordOutcome(TransactionLoadOutcome::kDropped,
                                            terminal_latency(task));
              record_load_progress(progress);
            } catch (...) {
              record_infrastructure_error(std::current_exception());
            }
          }
        };

    std::vector<std::thread> workers;
    workers.reserve(workload.concurrency);
    try {
      for (std::uint32_t worker_index = 0U; worker_index < workload.concurrency;
           ++worker_index) {
        static_cast<void>(worker_index);
        workers.emplace_back([&] {
          try {
            while (const std::optional<WalletTransactionLoadTask> next =
                       queue.Pop(load_stop_token)) {
              const WalletTransactionLoadTask& task = *next;
              const WalletIdentity& sender = wallets.at(task.plan.sender_index);
              const WalletIdentity& receiver =
                  wallets.at(task.plan.receiver_index);
              const WalletWorkloadFundingState& sender_funding =
                  funding->at(task.plan.sender_index);
              NodeRuntime& sender_node = nodes.at(sender.node - 1U);
              TransactionLoadOutcome outcome = TransactionLoadOutcome::kFailed;
              ChainWalletTransactionResult transaction;
              std::vector<TrackedTransaction> tracked;
              std::shared_ptr<TransactionLoadConfirmation> confirmation;
              std::optional<TransactionObservationTracker::Reservation>
                  observation_reservation;
              std::string error_class;
              std::string error_message;
              bool submitted = false;
              bool submission_started = false;
              bool release_if_balance_unavailable = true;
              bool cancel_queue_after_settlement = false;
              std::optional<std::uint64_t> actual_available_balance;
              {
                std::unique_lock<std::mutex> sender_submission_lock(
                    sender_submission_mutexes.at(task.plan.sender_index),
                    std::defer_lock);
                try {
                  if (queue.cancelled() ||
                      !WaitUntilWalletWorkloadDeadline(
                          task.scheduled_at, load_stop_token,
                          execution ? execution->settle_requested
                                    : std::function<bool()>{})) {
                    throw WalletWorkloadSettleRequested();
                  }
                  if (queue.cancelled()) {
                    throw SimulationCancelled();
                  }
                  sender_submission_lock.lock();
                  if (execution && execution->settle_requested &&
                      execution->settle_requested()) {
                    throw WalletWorkloadSettleRequested();
                  }
                  if (queue.cancelled()) {
                    throw SimulationCancelled();
                  }
                  ThrowIfStopRequested(load_stop_token);
                  RequireNodeRunning(sender_node,
                                     "transaction load submission");
                  observation_reservation =
                      transaction_tracker.TryReserve(nodes);
                  if (!observation_reservation) {
                    outcome = TransactionLoadOutcome::kBackpressured;
                    error_class = "backpressure";
                    error_message =
                        "bounded transaction observation capacity is full";
                  } else {
                    submission_started = true;
                    release_if_balance_unavailable = false;
                    transaction = driver.SubmitWalletTransaction(
                        sender_node.config,
                        ToChainWalletMode(registry.wallet_initialization()),
                        receiver.address, task.plan.amount_satoshis,
                        workload.fee_satoshis,
                        std::chrono::seconds(workload.timeout_sec),
                        load_stop_token);
                    const std::string& txid = RequireSingleWalletTransactionId(
                        transaction, "transaction load submission");
                    confirmation =
                        std::make_shared<TransactionLoadConfirmation>(
                            accounting, ExpectedTransactionLoadObservations(
                                            transaction.txids, nodes));
                    tracked.push_back(TrackedTransaction{
                        .txid = txid,
                        .submission_kind = "wallet_transaction_submitted",
                        .workload_id = execution ? execution->workload_id : "",
                        .workload_index = workload_index,
                        .workload_count = workload_count,
                        .transaction_index = task.transaction_index,
                        .transaction_count =
                            workload.transaction_count == 0U
                                ? std::nullopt
                                : std::optional<std::uint32_t>(
                                      workload.transaction_count),
                        .transaction_rate =
                            workload.transaction_rate
                                ? std::optional<double>(
                                      workload.transaction_rate->value())
                                : std::nullopt,
                        .txid_index = 1U,
                        .submission_node = sender.node,
                        .load_confirmation = confirmation,
                    });
                    transaction_tracker.TrackSet(
                        std::move(*observation_reservation), tracked);
                    outcome = TransactionLoadOutcome::kSubmitted;
                    submitted = true;
                  }
                } catch (const ChainTransactionRejected& error) {
                  outcome = TransactionLoadOutcome::kRejected;
                  error_class = "policy_rejection";
                  error_message = error.what();
                  release_if_balance_unavailable = true;
                } catch (const ChainTransactionTimedOut& error) {
                  outcome = TransactionLoadOutcome::kTimedOut;
                  error_class = "timeout";
                  error_message = error.what();
                } catch (const ChainTransactionRpcWarmup& error) {
                  outcome = TransactionLoadOutcome::kFailed;
                  error_class = "warmup";
                  error_message = error.what();
                } catch (const ChainTransactionRpcMethodUnavailable& error) {
                  outcome = TransactionLoadOutcome::kFailed;
                  error_class = "method_unavailable";
                  error_message = error.what();
                } catch (const ChainTransactionTransportFailure& error) {
                  outcome = TransactionLoadOutcome::kFailed;
                  error_class = "transport";
                  error_message = error.what();
                } catch (const ChainTransactionInternalRpcFailure& error) {
                  outcome = TransactionLoadOutcome::kFailed;
                  error_class = "internal_rpc";
                  error_message = error.what();
                } catch (const WalletWorkloadSettleRequested& error) {
                  outcome = TransactionLoadOutcome::kDropped;
                  error_class = "settled";
                  error_message = error.what();
                  release_if_balance_unavailable = true;
                } catch (const SimulationCancelled& error) {
                  outcome = TransactionLoadOutcome::kCancelled;
                  error_class = "cancellation";
                  error_message = error.what();
                  release_if_balance_unavailable = !submission_started;
                  worker_cancelled.store(true);
                  static_cast<void>(load_stop_source.request_stop());
                  cancel_queue_after_settlement = true;
                } catch (const std::exception& error) {
                  outcome = TransactionLoadOutcome::kFailed;
                  error_class = "internal";
                  error_message = error.what();
                  release_if_balance_unavailable = !submission_started;
                } catch (...) {
                  outcome = TransactionLoadOutcome::kFailed;
                  error_class = "unknown";
                  error_message = "unknown transaction submission error";
                  release_if_balance_unavailable = !submission_started;
                }

                if (submission_started && !submitted &&
                    outcome != TransactionLoadOutcome::kCancelled) {
                  try {
                    actual_available_balance =
                        driver
                            .ReadWalletSnapshot(
                                sender_node.config,
                                ToChainWalletMode(
                                    registry.wallet_initialization()),
                                1U, load_stop_token)
                            .available_balance_satoshis;
                  } catch (const SimulationCancelled&) {
                    worker_cancelled.store(true);
                    static_cast<void>(load_stop_source.request_stop());
                    cancel_queue_after_settlement = true;
                  } catch (const std::exception& error) {
                    BBP_LOG(warning)
                        << "transaction load balance reconciliation failed "
                           "for "
                        << sender_node.config.id << ": " << error.what();
                  } catch (...) {
                    BBP_LOG(warning)
                        << "transaction load balance reconciliation failed "
                           "for "
                        << sender_node.config.id << ": unknown exception";
                  }
                }
                if (!sender_submission_lock.owns_lock()) {
                  sender_submission_lock.lock();
                }
                try {
                  balance_reservations.Settle(
                      task.transaction_index, actual_available_balance,
                      !submitted && release_if_balance_unavailable,
                      execution ? execution->record_released_atomic_units
                                : std::function<void(std::uint64_t)>{});
                } catch (...) {
                  record_infrastructure_error(std::current_exception());
                }
              }
              if (cancel_queue_after_settlement) {
                record_dropped_tasks(queue.Cancel());
              }

              const std::chrono::microseconds latency = terminal_latency(task);
              const TransactionLoadSnapshot outcome_progress =
                  accounting->RecordOutcome(outcome, latency);
              try {
                record_load_progress(outcome_progress);
                if (submitted) {
                  WriteEvent(
                      events_path, options.run_id, sender_node.config.id,
                      SimulationEventKind::kWalletTransactionSubmitted,
                      WalletTransactionDetail(
                          workload_index, workload_count, workload,
                          task.transaction_index, sender, receiver,
                          sender_funding.miner_node,
                          sender_funding.start_height,
                          sender_funding.target_height,
                          static_cast<std::uint64_t>(
                              sender_funding.hashes.size()),
                          sender_funding.ready_height,
                          sender_funding.preparation,
                          static_cast<std::uint64_t>(
                              sender_funding.preparation_hashes.size()),
                          sender_funding.ready_balance_satoshis,
                          task.plan.amount_satoshis, task.plan.interval_before,
                          task.scheduled_simulation_elapsed,
                          task.scheduled_wall_elapsed, transaction));
                }
                WriteEvent(events_path, options.run_id, sender_node.config.id,
                           SimulationEventKind::kTransactionLoadAttempt,
                           TransactionLoadAttemptDetail(
                               workload_index, workload_count, workload,
                               attempt_limit, task, sender, receiver, outcome,
                               latency, submitted ? &transaction : nullptr,
                               error_class, error_message));
              } catch (...) {
                record_infrastructure_error(std::current_exception());
              }

              if (!submitted) {
                continue;
              }
              try {
                const std::vector<TransactionSetObservation> observations =
                    transaction_tracker.ObserveTrackedSetsUntilVisible(
                        options, events_path, driver, nodes, {tracked},
                        std::chrono::seconds(workload.timeout_sec),
                        load_stop_token);
                if (observations.front().observation_error) {
                  record_load_progress(accounting->RecordObservationError());
                }
              } catch (const SimulationCancelled&) {
                try {
                  record_load_progress(accounting->RecordObservationError());
                } catch (...) {
                  record_infrastructure_error(std::current_exception());
                }
                worker_cancelled.store(true);
                static_cast<void>(load_stop_source.request_stop());
                record_dropped_tasks(queue.Cancel());
              } catch (...) {
                try {
                  record_load_progress(accounting->RecordObservationError());
                } catch (...) {
                  record_infrastructure_error(std::current_exception());
                }
                record_infrastructure_error(std::current_exception());
              }
            }
          } catch (...) {
            record_infrastructure_error(std::current_exception());
          }
        });
      }
    } catch (...) {
      const std::exception_ptr creation_error = std::current_exception();
      record_dropped_tasks(queue.Cancel());
      for (std::thread& worker : workers) {
        worker.join();
      }
      std::rethrow_exception(creation_error);
    }

    std::exception_ptr producer_error;
    const auto rate_epoch =
        execution ? execution->started_at : std::chrono::steady_clock::now();
    std::uint64_t transaction_index =
        execution ? execution->next_transaction_index : 0U;
    bool yielded = false;
    bool balance_refresh_required = false;
    try {
      while (!attempt_limit || transaction_index < *attempt_limit) {
        ThrowIfStopRequested(load_stop_token);
        if (duration_deadline &&
            std::chrono::steady_clock::now() >= *duration_deadline) {
          break;
        }
        if (execution && execution->yield_requested &&
            execution->yield_requested()) {
          if (execution->settle_requested && execution->settle_requested()) {
            record_dropped_tasks(queue.DropPendingAndClose());
          }
          yielded = true;
          break;
        }
        const std::chrono::steady_clock::time_point batch_admission_at =
            std::chrono::steady_clock::now();
        if (workload.transaction_rate) {
          WalletTransactionLoadTask first_task{
              .transaction_index = transaction_index + 1U,
              .plan = {},
              .scheduled_simulation_elapsed = std::nullopt,
              .scheduled_wall_elapsed = std::nullopt,
              .scheduled_at = batch_admission_at,
          };
          ApplyTransactionLoadRateSchedule(
              &first_task, *workload.transaction_rate, options.time_scale,
              rate_epoch, transaction_index);
          const std::chrono::steady_clock::time_point admission_deadline =
              duration_deadline
                  ? std::min(first_task.scheduled_at, *duration_deadline)
                  : first_task.scheduled_at;
          if (!WaitUntilWalletWorkloadDeadline(
                  admission_deadline, load_stop_token,
                  execution ? execution->yield_requested
                            : std::function<bool()>{})) {
            if (execution && execution->settle_requested &&
                execution->settle_requested()) {
              record_dropped_tasks(queue.DropPendingAndClose());
            }
            yielded = true;
            break;
          }
          if (duration_deadline &&
              std::chrono::steady_clock::now() >= *duration_deadline) {
            break;
          }
        }

        std::vector<WalletTransactionLoadTask> tasks;
        const TransactionLoadBatchAdmission admission =
            balance_reservations.PlanAndReserve(
                &planner, transaction_index + 1U,
                attempt_limit ? *attempt_limit - transaction_index
                              : std::numeric_limits<std::uint64_t>::max() -
                                    transaction_index,
                [&](const std::vector<WalletTransactionPlanEntry>& plans) {
                  tasks.reserve(plans.size());
                  for (std::size_t offset = 0U; offset < plans.size();
                       ++offset) {
                    WalletTransactionLoadTask task{
                        .transaction_index = transaction_index + offset + 1U,
                        .plan = plans.at(offset),
                        .scheduled_simulation_elapsed = std::nullopt,
                        .scheduled_wall_elapsed = std::nullopt,
                        .scheduled_at = batch_admission_at,
                    };
                    if (workload.transaction_rate) {
                      ApplyTransactionLoadRateSchedule(
                          &task, *workload.transaction_rate, options.time_scale,
                          rate_epoch, transaction_index + offset);
                    }
                    tasks.push_back(std::move(task));
                  }
                  return queue.TryPushBatch(tasks, [&] {
                    if (execution && execution->record_load_admission) {
                      execution->record_load_admission(plans);
                    }
                  });
                });
        if (admission.has_plan() && execution && execution->record_planned) {
          execution->record_planned(
              static_cast<std::uint64_t>(admission.plans.size()));
        }
        if (!admission.has_plan()) {
          if (balance_reservations.WaitForResolution(admission.balance_revision,
                                                     load_stop_token)) {
            continue;
          }
          ThrowIfStopRequested(load_stop_token);
          balance_refresh_required = true;
          break;
        }
        if (!admission.admitted) {
          for (const WalletTransactionLoadTask& task : tasks) {
            const WalletIdentity& sender = wallets.at(task.plan.sender_index);
            const WalletIdentity& receiver =
                wallets.at(task.plan.receiver_index);
            const std::chrono::microseconds latency = terminal_latency(task);
            const TransactionLoadSnapshot progress = accounting->RecordOutcome(
                TransactionLoadOutcome::kBackpressured, latency);
            record_load_progress(progress);
            WriteEvent(
                events_path, options.run_id,
                nodes.at(sender.node - 1U).config.id,
                SimulationEventKind::kTransactionLoadAttempt,
                TransactionLoadAttemptDetail(
                    workload_index, workload_count, workload, attempt_limit,
                    task, sender, receiver,
                    TransactionLoadOutcome::kBackpressured, latency, nullptr,
                    "backpressure", "bounded transaction load queue is full"));
          }
        }
        transaction_index += static_cast<std::uint64_t>(admission.plans.size());
        if (execution != nullptr) {
          execution->next_transaction_index = transaction_index;
        }
        if (workload.strategy == WalletTransferStrategy::kRandomBruteforce &&
            admission.admitted) {
          balance_refresh_required = true;
          break;
        }
      }
    } catch (...) {
      producer_error = std::current_exception();
    }

    if (producer_error || load_stop_token.stop_requested() ||
        worker_cancelled.load()) {
      static_cast<void>(load_stop_source.request_stop());
      record_dropped_tasks(queue.Cancel());
    } else {
      queue.Close();
    }
    for (std::thread& worker : workers) {
      worker.join();
    }
    if (load_stop_token.stop_requested() || worker_cancelled.load()) {
      record_dropped_tasks(queue.Cancel());
    }
    const bool cancelled = queue.cancelled() || worker_cancelled.load() ||
                           load_stop_token.stop_requested();
    if (cancelled && execution == nullptr) {
      const auto elapsed =
          std::chrono::duration_cast<std::chrono::microseconds>(
              std::chrono::steady_clock::now() - load_started_at);
      const TransactionLoadSnapshot snapshot = accounting->Snapshot(elapsed);
      WriteEvent(events_path, options.run_id, "sim",
                 SimulationEventKind::kTransactionLoadCompleted,
                 TransactionLoadCompletedDetail(
                     workload_index, workload_count, workload, attempt_limit,
                     queue.maximum_size(), snapshot));
    }
    if (producer_error) {
      std::rethrow_exception(producer_error);
    }
    {
      std::lock_guard<std::mutex> lock(infrastructure_error_mutex);
      if (infrastructure_error) {
        std::rethrow_exception(infrastructure_error);
      }
    }
    if (cancelled) {
      throw SimulationCancelled();
    }
    const auto elapsed = std::chrono::duration_cast<std::chrono::microseconds>(
        std::chrono::steady_clock::now() - load_started_at);
    if (execution != nullptr) {
      execution->next_transaction_index = transaction_index;
      return WalletWorkloadExecutionResult{
          .end = yielded ? WalletWorkloadExecutionEnd::kYielded
                 : balance_refresh_required &&
                         (!attempt_limit || transaction_index < *attempt_limit)
                     ? WalletWorkloadExecutionEnd::kRefreshBalances
                     : WalletWorkloadExecutionEnd::kCompleted,
          .next_transaction_index = transaction_index,
          .queue_maximum_size = queue.maximum_size(),
      };
    }
    if (pending_load_completions == nullptr) {
      throw std::runtime_error(
          "transaction load completion destination is missing");
    }
    if (pending_load_completions->size() >= kMaximumScenarioActionCount) {
      throw std::runtime_error(
          "pending transaction load completion count exceeds retained limit " +
          std::to_string(kMaximumScenarioActionCount));
    }
    pending_load_completions->push_back(PendingTransactionLoadCompletion{
        .workload_index = workload_index,
        .workload_count = workload_count,
        .workload = workload,
        .attempt_limit = attempt_limit,
        .queue_maximum_size = queue.maximum_size(),
        .accounting = std::move(accounting),
        .elapsed = elapsed,
    });
    return WalletWorkloadExecutionResult{
        .end = WalletWorkloadExecutionEnd::kCompleted,
        .next_transaction_index = transaction_index,
        .queue_maximum_size = queue.maximum_size(),
    };
  }

  WalletTransactionPlanner transaction_planner(
      wallets.size(), workload,
      execution ? execution->next_transaction_index : 0U);
  const auto rate_epoch =
      execution ? execution->started_at : std::chrono::steady_clock::now();
  std::shared_ptr<TransactionLoadAccounting> accounting =
      execution ? execution->accounting : nullptr;
  std::uint64_t transaction_index =
      execution ? execution->next_transaction_index : 0U;
  while (workload.transaction_count == 0U ||
         transaction_index < workload.transaction_count) {
    ThrowIfStopRequested(stop_token);
    if (execution && execution->yield_requested &&
        execution->yield_requested()) {
      execution->next_transaction_index = transaction_index;
      return WalletWorkloadExecutionResult{
          .end = WalletWorkloadExecutionEnd::kYielded,
          .next_transaction_index = transaction_index,
          .queue_maximum_size = 0U,
      };
    }
    const WalletTransactionPlanEntry plan_entry = transaction_planner.Next();
    if (execution && execution->record_planned) {
      execution->record_planned(1U);
    }
    std::optional<std::chrono::milliseconds> scheduled_simulation_elapsed;
    std::optional<std::chrono::milliseconds> scheduled_wall_elapsed;
    if (workload.transaction_rate) {
      scheduled_simulation_elapsed =
          workload.transaction_rate->SimulationElapsedBefore(transaction_index);
      scheduled_wall_elapsed =
          options.time_scale.WallDuration(*scheduled_simulation_elapsed);
      if (!WaitUntilWalletWorkloadDeadline(
              SteadyDeadline(rate_epoch, *scheduled_wall_elapsed), stop_token,
              execution ? execution->yield_requested
                        : std::function<bool()>{})) {
        execution->next_transaction_index = transaction_index;
        return WalletWorkloadExecutionResult{
            .end = WalletWorkloadExecutionEnd::kYielded,
            .next_transaction_index = transaction_index,
            .queue_maximum_size = 0U,
        };
      }
    } else if (plan_entry.interval_before != std::chrono::milliseconds(0)) {
      if (!WaitUntilWalletWorkloadDeadline(
              SteadyDeadline(std::chrono::steady_clock::now(),
                             plan_entry.interval_before),
              stop_token,
              execution ? execution->yield_requested
                        : std::function<bool()>{})) {
        execution->next_transaction_index = transaction_index;
        return WalletWorkloadExecutionResult{
            .end = WalletWorkloadExecutionEnd::kYielded,
            .next_transaction_index = transaction_index,
            .queue_maximum_size = 0U,
        };
      }
    }
    const size_t sender_index = plan_entry.sender_index;
    const size_t receiver_index = plan_entry.receiver_index;
    const WalletIdentity& sender = wallets[sender_index];
    const WalletIdentity& receiver = wallets[receiver_index];
    const WalletWorkloadFundingState& sender_funding =
        funding->at(sender_index);
    NodeRuntime& sender_node = nodes[sender.node - 1U];
    RequireNodeRunning(sender_node, "wallet transaction submission");

    TransactionObservationTracker::Reservation observation_reservation =
        transaction_tracker.Reserve(nodes);
    if (execution && execution->record_accepted) {
      execution->record_accepted(1U);
    }
    ChainWalletTransactionResult transaction;
    try {
      transaction = driver.SendWalletTransaction(
          sender_node.config,
          ToChainWalletMode(registry.wallet_initialization()), receiver.address,
          plan_entry.amount_satoshis, workload.fee_satoshis,
          std::chrono::seconds(workload.timeout_sec), stop_token);
    } catch (const ChainTransactionRejected&) {
      if (accounting) {
        static_cast<void>(accounting->RecordOutcome(
            TransactionLoadOutcome::kRejected, std::chrono::microseconds(0)));
      }
      throw;
    } catch (const ChainTransactionTimedOut&) {
      if (accounting) {
        static_cast<void>(accounting->RecordOutcome(
            TransactionLoadOutcome::kTimedOut, std::chrono::microseconds(0)));
      }
      throw;
    } catch (const SimulationCancelled&) {
      if (accounting) {
        static_cast<void>(accounting->RecordOutcome(
            TransactionLoadOutcome::kCancelled, std::chrono::microseconds(0)));
      }
      throw;
    } catch (...) {
      if (accounting) {
        static_cast<void>(accounting->RecordOutcome(
            TransactionLoadOutcome::kFailed, std::chrono::microseconds(0)));
      }
      throw;
    }
    const std::string& txid = RequireSingleWalletTransactionId(
        transaction, "wallet transaction submission");
    if (accounting) {
      static_cast<void>(accounting->RecordOutcome(
          TransactionLoadOutcome::kSubmitted, std::chrono::microseconds(0)));
    }
    WriteEvent(
        events_path, options.run_id, sender_node.config.id,
        SimulationEventKind::kWalletTransactionSubmitted,
        WalletTransactionDetail(
            workload_index, workload_count, workload, transaction_index + 1U,
            sender, receiver, sender_funding.miner_node,
            sender_funding.start_height, sender_funding.target_height,
            static_cast<uint64_t>(sender_funding.hashes.size()),
            sender_funding.ready_height, sender_funding.preparation,
            static_cast<uint64_t>(sender_funding.preparation_hashes.size()),
            sender_funding.ready_balance_satoshis, plan_entry.amount_satoshis,
            plan_entry.interval_before, scheduled_simulation_elapsed,
            scheduled_wall_elapsed, transaction));
    std::shared_ptr<TransactionLoadConfirmation> confirmation;
    if (accounting) {
      confirmation = std::make_shared<TransactionLoadConfirmation>(
          accounting,
          ExpectedTransactionLoadObservations(transaction.txids, nodes));
    }
    TrackedTransaction tracked{
        .txid = txid,
        .submission_kind = "wallet_transaction_submitted",
        .workload_id = execution ? execution->workload_id : "",
        .workload_index = workload_index,
        .workload_count = workload_count,
        .transaction_index = transaction_index + 1U,
        .transaction_count =
            workload.transaction_count == 0U
                ? std::nullopt
                : std::optional<std::uint32_t>(workload.transaction_count),
        .transaction_rate =
            workload.transaction_rate
                ? std::optional<double>(workload.transaction_rate->value())
                : std::nullopt,
        .txid_index = 1U,
        .submission_node = sender.node,
        .load_confirmation = confirmation,
    };
    if (execution || workload.transaction_rate) {
      transaction_tracker.Track(std::move(observation_reservation),
                                std::move(tracked));
    } else {
      transaction_tracker.TrackAndWaitForVisibility(
          std::move(observation_reservation), options, events_path, driver,
          nodes, std::move(tracked), std::chrono::seconds(workload.timeout_sec),
          stop_token);
    }
    if (!execution && workload.transaction_rate) {
      transaction_tracker.ObserveAll(options, events_path, driver, nodes,
                                     stop_token);
    }
    if (transaction_index == std::numeric_limits<std::uint64_t>::max()) {
      throw std::runtime_error("wallet transaction index exceeds uint64");
    }
    ++transaction_index;
    if (execution) {
      execution->next_transaction_index = transaction_index;
    }
  }
  return WalletWorkloadExecutionResult{
      .end = WalletWorkloadExecutionEnd::kCompleted,
      .next_transaction_index = transaction_index,
      .queue_maximum_size = 0U,
  };
}

}  // namespace bbp::simulator_app_internal
