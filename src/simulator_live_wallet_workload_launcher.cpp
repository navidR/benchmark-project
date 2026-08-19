#include "simulator_live_wallet_workload_launcher.h"

#include <algorithm>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <exception>
#include <limits>
#include <memory>
#include <mutex>
#include <optional>
#include <span>
#include <stdexcept>
#include <stop_token>
#include <string>
#include <thread>
#include <utility>

#include "bbp/logging.h"
#include "bbp/mcp_live_application.h"
#include "bbp/mcp_operation_service.h"
#include "bbp/runtime_node_inventory.h"
#include "bbp/runtime_wallet_registry.h"
#include "bbp/simulation_cancelled.h"
#include "bbp/simulation_event_kind.h"
#include "bbp/simulator/options.h"
#include "bbp/simulator/transaction_load.h"
#include "bbp/simulator/wallet_transaction_plan.h"
#include "simulator_combined_stop_token.h"
#include "simulator_event_writing.h"
#include "simulator_live_workload_state.h"
#include "simulator_transaction_observation_tracking.h"
#include "simulator_wallet_transaction_validation.h"
#include "simulator_wallet_transaction_workload_execution.h"
#include "simulator_workload_event_details.h"

namespace bbp::simulator_app_internal {

LiveWalletWorkloadLauncher MakeLiveWalletWorkloadLauncher(
    const Options& options, const std::filesystem::path& events_path,
    const ChainDriver& driver, const RuntimeNodeInventory& node_inventory,
    const RuntimeWalletRegistry& runtime_wallet_registry,
    TransactionObservationTracker& transaction_tracker,
    std::timed_mutex& block_generation_mutex,
    McpLiveWorkloadService& workload_service,
    std::shared_ptr<LiveWalletWorkloadRegistry> wallet_workloads,
    std::shared_ptr<LiveBlockGenerationWorkloadRegistry>
        block_generation_workloads,
    std::shared_ptr<LiveWaitUntilHeightWorkloadRegistry>
        wait_until_height_workloads,
    std::shared_ptr<LiveWaitForPeersWorkloadRegistry>
        wait_for_peers_workloads) {
  return [&options, &events_path, &driver, &node_inventory,
          &runtime_wallet_registry, &transaction_tracker,
          &block_generation_mutex, &workload_service,
          wallet_workloads = std::move(wallet_workloads),
          block_generation_workloads = std::move(block_generation_workloads),
          wait_until_height_workloads = std::move(wait_until_height_workloads),
          wait_for_peers_workloads = std::move(wait_for_peers_workloads)](
             WalletTransactionsWorkload workload,
             std::optional<std::string> requested_id)
             -> std::shared_ptr<LiveWalletWorkloadRecord> {
    auto record = std::make_shared<LiveWalletWorkloadRecord>();
    record->workload = std::move(workload);
    {
      std::scoped_lock registry_lock(
          wallet_workloads->mutex, block_generation_workloads->mutex,
          wait_until_height_workloads->mutex, wait_for_peers_workloads->mutex);
      if (wallet_workloads->shutting_down ||
          block_generation_workloads->shutting_down ||
          wait_until_height_workloads->shutting_down ||
          wait_for_peers_workloads->shutting_down) {
        throw McpOperationFailure(
            "run_not_active",
            "the run is stopping and cannot start another workload", false);
      }
      record->wallet_snapshot = runtime_wallet_registry.Snapshot();
      Options validation = options;
      validation.topology = record->wallet_snapshot.registry().topology();
      validation.wallet_backed_workload_requested =
          !record->wallet_snapshot.wallets().empty();
      ValidateWalletTransactionsWorkload(record->workload, validation);
      record->claimed_wallets = ClaimedWalletsForWorkload(
          record->workload, record->wallet_snapshot.wallets().size());
      if (wallet_workloads->records.size() +
              block_generation_workloads->records.size() +
              wait_until_height_workloads->records.size() +
              wait_for_peers_workloads->records.size() >=
          kMaximumScenarioActionCount) {
        throw McpOperationFailure(
            "workload_capacity_exceeded",
            "workload retained-instance capacity is exhausted", false);
      }
      std::size_t active_count = 0U;
      for (const auto& [existing_id, existing] : wallet_workloads->records) {
        static_cast<void>(existing_id);
        std::lock_guard<std::mutex> existing_lock(existing->mutex);
        if (IsTerminalLiveWalletWorkloadState(existing->state)) {
          continue;
        }
        ++active_count;
        if (WalletClaimsOverlap(record->claimed_wallets,
                                existing->claimed_wallets)) {
          throw McpOperationFailure(
              "wallet_admission_conflict",
              "wallet workload sender admission overlaps active workload " +
                  existing->id,
              true);
        }
      }
      if (active_count >= kMaximumWalletTransactionLoadConcurrency) {
        throw McpOperationFailure(
            "workload_capacity_exceeded",
            "active wallet workload capacity is exhausted", true);
      }
      if (requested_id) {
        ValidateMcpIdentifier(*requested_id, "workload_id");
        if (wallet_workloads->records.contains(*requested_id) ||
            block_generation_workloads->records.contains(*requested_id) ||
            wait_until_height_workloads->records.contains(*requested_id) ||
            wait_for_peers_workloads->records.contains(*requested_id)) {
          throw McpOperationFailure(
              "workload_id_conflict",
              "workload_id is already retained: " + *requested_id, false);
        }
        record->id = *requested_id;
      } else {
        do {
          if (wallet_workloads->next_id ==
              std::numeric_limits<std::uint64_t>::max()) {
            throw McpOperationFailure(
                "workload_id_exhausted",
                "wallet workload identity sequence is exhausted", false);
          }
          record->id =
              "wallet-workload-" + std::to_string(wallet_workloads->next_id++);
        } while (wallet_workloads->records.contains(record->id) ||
                 block_generation_workloads->records.contains(record->id) ||
                 wait_until_height_workloads->records.contains(record->id) ||
                 wait_for_peers_workloads->records.contains(record->id));
      }
      record->ordinal = static_cast<std::uint32_t>(
          wallet_workloads->records.size() +
          block_generation_workloads->records.size() +
          wait_until_height_workloads->records.size() +
          wait_for_peers_workloads->records.size() + 1U);
      wallet_workloads->records.emplace(record->id, record);

      try {
        auto worker_lease = workload_service.AcquireWorkerLease();
        record->worker = std::thread([&options, &events_path, &driver,
                                      &node_inventory, &transaction_tracker,
                                      &block_generation_mutex, record,
                                      worker_lease = std::move(worker_lease)] {
          const std::stop_token service_stop_token = worker_lease.stop_token();
          const auto set_terminal =
              [&](LiveWalletWorkloadState state, std::string outcome,
                  std::optional<std::string> failure = std::nullopt) {
                {
                  std::lock_guard<std::mutex> lock(record->mutex);
                  record->state = state;
                  record->terminal_outcome = std::move(outcome);
                  record->failure = std::move(failure);
                  record->changed.notify_all();
                }
                try {
                  WriteLiveWalletWorkloadState(events_path, options, *record);
                } catch (const std::exception& error) {
                  BBP_LOG(error)
                      << "failed to publish terminal wallet workload "
                      << record->id << ": " << error.what();
                }
              };
          const auto cancel_outstanding_tracking = [&] {
            const std::size_t cancelled =
                transaction_tracker.CancelWorkload(record->id);
            std::lock_guard<std::mutex> lock(record->mutex);
            CheckedWalletWorkloadAdd(static_cast<std::uint64_t>(cancelled),
                                     &record->cancelled_tracking,
                                     "cancelled tracking");
          };
          try {
            while (true) {
              WalletTransactionsWorkload epoch_workload;
              RuntimeWalletSnapshot epoch_wallets;
              std::stop_token epoch_stop_token;
              {
                std::unique_lock<std::mutex> lock(record->mutex);
                while (record->request == LiveWalletWorkloadRequest::kPause) {
                  record->state = LiveWalletWorkloadState::kPaused;
                  record->changed.notify_all();
                  lock.unlock();
                  WriteLiveWalletWorkloadState(events_path, options, *record);
                  lock.lock();
                  if (!record->changed.wait(lock, service_stop_token, [&] {
                        return record->request !=
                               LiveWalletWorkloadRequest::kPause;
                      })) {
                    record->request = LiveWalletWorkloadRequest::kShutdown;
                    break;
                  }
                }
                if (record->request == LiveWalletWorkloadRequest::kStopCancel) {
                  lock.unlock();
                  cancel_outstanding_tracking();
                  set_terminal(LiveWalletWorkloadState::kStopped, "stopped");
                  return;
                }
                if (record->request == LiveWalletWorkloadRequest::kStopSettle) {
                  lock.unlock();
                  set_terminal(LiveWalletWorkloadState::kStopped, "stopped");
                  return;
                }
                if (record->request == LiveWalletWorkloadRequest::kShutdown) {
                  lock.unlock();
                  cancel_outstanding_tracking();
                  set_terminal(LiveWalletWorkloadState::kCancelled,
                               "cancelled");
                  return;
                }
                if (record->request == LiveWalletWorkloadRequest::kRunFailure) {
                  lock.unlock();
                  cancel_outstanding_tracking();
                  set_terminal(LiveWalletWorkloadState::kFailed, "failed",
                               "run failed while wallet workload was active");
                  return;
                }
                if (record->request ==
                    LiveWalletWorkloadRequest::kReconfigure) {
                  if (!record->pending_workload) {
                    throw std::logic_error(
                        "wallet workload reconfigure has no "
                        "configuration");
                  }
                  record->workload = std::move(*record->pending_workload);
                  record->pending_workload.reset();
                  if (record->configuration_revision ==
                      std::numeric_limits<std::uint64_t>::max()) {
                    throw std::runtime_error(
                        "wallet workload configuration revision exceeds "
                        "uint64");
                  }
                  ++record->configuration_revision;
                  record->request = LiveWalletWorkloadRequest::kNone;
                  record->state = LiveWalletWorkloadState::kStarting;
                  record->changed.notify_all();
                  lock.unlock();
                  WriteLiveWalletWorkloadState(events_path, options, *record);
                  lock.lock();
                }
                record->epoch_stop_source = std::stop_source();
                epoch_stop_token = record->epoch_stop_source.get_token();
                epoch_workload = record->workload;
                epoch_wallets = record->wallet_snapshot;
              }

              CombinedStopToken execution_stop(service_stop_token,
                                               epoch_stop_token);
              WalletWorkloadExecutionContext execution{
                  .accounting = record->accounting,
                  .workload_id = record->id,
                  .started_at = record->started_at,
                  .next_transaction_index = record->next_transaction_index,
                  .prepared_funding = &record->prepared_funding,
                  .yield_requested =
                      [record] {
                        std::lock_guard<std::mutex> lock(record->mutex);
                        return record->request !=
                               LiveWalletWorkloadRequest::kNone;
                      },
                  .settle_requested =
                      [record] {
                        std::lock_guard<std::mutex> lock(record->mutex);
                        return record->request ==
                               LiveWalletWorkloadRequest::kStopSettle;
                      },
                  .record_planned =
                      [record](std::uint64_t count) {
                        std::lock_guard<std::mutex> lock(record->mutex);
                        CheckedWalletWorkloadAdd(count, &record->planned,
                                                 "planned");
                      },
                  .record_accepted =
                      [record](std::uint64_t count) {
                        std::lock_guard<std::mutex> lock(record->mutex);
                        CheckedWalletWorkloadAdd(count, &record->accepted,
                                                 "accepted");
                      },
                  .record_load_admission =
                      [record, fee_reserve =
                                   EffectiveWalletTransactionFeeReserveSatoshis(
                                       epoch_workload)](
                          std::span<const WalletTransactionPlanEntry> plans) {
                        std::uint64_t reserved = 0U;
                        for (const WalletTransactionPlanEntry& plan : plans) {
                          if (plan.amount_satoshis >
                              std::numeric_limits<std::uint64_t>::max() -
                                  fee_reserve) {
                            throw std::runtime_error(
                                "wallet workload reservation amount "
                                "exceeds uint64");
                          }
                          const std::uint64_t amount =
                              plan.amount_satoshis + fee_reserve;
                          if (reserved >
                              std::numeric_limits<std::uint64_t>::max() -
                                  amount) {
                            throw std::runtime_error(
                                "wallet workload admission reservation "
                                "exceeds uint64");
                          }
                          reserved += amount;
                        }
                        std::lock_guard<std::mutex> lock(record->mutex);
                        const std::uint64_t accepted =
                            static_cast<std::uint64_t>(plans.size());
                        if (record->accepted >
                                std::numeric_limits<std::uint64_t>::max() -
                                    accepted ||
                            record->reserved_atomic_units >
                                std::numeric_limits<std::uint64_t>::max() -
                                    reserved) {
                          throw std::runtime_error(
                              "wallet workload admission accounting "
                              "exceeds uint64");
                        }
                        record->accepted += accepted;
                        record->reserved_atomic_units += reserved;
                      },
                  .record_released_atomic_units =
                      [record](std::uint64_t amount) {
                        std::lock_guard<std::mutex> lock(record->mutex);
                        CheckedWalletWorkloadAdd(amount,
                                                 &record->released_atomic_units,
                                                 "released atomic units");
                      },
                  .execution_started =
                      [&events_path, &options, record] {
                        {
                          std::lock_guard<std::mutex> lock(record->mutex);
                          if (record->request ==
                              LiveWalletWorkloadRequest::kNone) {
                            record->state = LiveWalletWorkloadState::kRunning;
                          }
                          record->changed.notify_all();
                        }
                        WriteLiveWalletWorkloadState(events_path, options,
                                                     *record);
                      },
              };
              try {
                RuntimeNodeSnapshot execution_nodes = node_inventory.Snapshot();
                const WalletWorkloadExecutionResult result =
                    ApplyWalletTransactionsWorkload(
                        options, events_path, driver, block_generation_mutex,
                        execution_nodes, epoch_wallets.registry(),
                        transaction_tracker, nullptr, epoch_workload,
                        record->ordinal, 0U, execution_stop.get_token(),
                        &execution);
                bool stop_settled = false;
                {
                  std::lock_guard<std::mutex> lock(record->mutex);
                  record->next_transaction_index =
                      result.next_transaction_index;
                  record->queue_maximum_size = std::max(
                      record->queue_maximum_size, result.queue_maximum_size);
                  if (record->request ==
                      LiveWalletWorkloadRequest::kStopSettle) {
                    stop_settled = true;
                  }
                  if (record->request == LiveWalletWorkloadRequest::kPause ||
                      record->request ==
                          LiveWalletWorkloadRequest::kReconfigure) {
                    continue;
                  }
                }
                if (stop_settled) {
                  set_terminal(LiveWalletWorkloadState::kStopped, "stopped");
                  return;
                }
                if (result.end ==
                    WalletWorkloadExecutionEnd::kRefreshBalances) {
                  std::unique_lock<std::mutex> lock(record->mutex);
                  record->changed.wait_for(
                      lock, execution_stop.get_token(),
                      std::chrono::milliseconds(100), [&] {
                        return record->request !=
                               LiveWalletWorkloadRequest::kNone;
                      });
                  continue;
                }
                const std::string outcome =
                    epoch_workload.transaction_count != 0U ? "count_reached"
                    : epoch_workload.duration              ? "duration_expired"
                                                           : "failed";
                if (outcome == "failed") {
                  cancel_outstanding_tracking();
                  set_terminal(LiveWalletWorkloadState::kFailed, "failed",
                               "continuous wallet workload sender ended "
                               "unexpectedly");
                } else {
                  set_terminal(LiveWalletWorkloadState::kCompleted, outcome);
                  const auto elapsed =
                      std::chrono::duration_cast<std::chrono::microseconds>(
                          std::chrono::steady_clock::now() -
                          record->started_at);
                  const TransactionLoadSnapshot snapshot =
                      record->accounting->Snapshot(elapsed);
                  WriteEvent(
                      events_path, options.run_id, "sim",
                      SimulationEventKind::kTransactionLoadCompleted,
                      TransactionLoadCompletedDetail(
                          record->ordinal, 0U, epoch_workload,
                          ExplicitWalletTransactionAttemptLimit(epoch_workload),
                          record->queue_maximum_size, snapshot));
                }
                return;
              } catch (const SimulationCancelled&) {
                LiveWalletWorkloadRequest request;
                {
                  std::lock_guard<std::mutex> lock(record->mutex);
                  record->next_transaction_index =
                      execution.next_transaction_index;
                  request = record->request;
                }
                if (request == LiveWalletWorkloadRequest::kPause ||
                    request == LiveWalletWorkloadRequest::kReconfigure) {
                  continue;
                }
                if (request == LiveWalletWorkloadRequest::kStopCancel ||
                    request == LiveWalletWorkloadRequest::kStopSettle) {
                  if (request == LiveWalletWorkloadRequest::kStopCancel) {
                    cancel_outstanding_tracking();
                  }
                  set_terminal(LiveWalletWorkloadState::kStopped, "stopped");
                  return;
                }
                if (request == LiveWalletWorkloadRequest::kRunFailure) {
                  cancel_outstanding_tracking();
                  set_terminal(LiveWalletWorkloadState::kFailed, "failed",
                               "run failed while wallet workload was active");
                  return;
                }
                if (request == LiveWalletWorkloadRequest::kShutdown ||
                    service_stop_token.stop_requested()) {
                  cancel_outstanding_tracking();
                  set_terminal(LiveWalletWorkloadState::kCancelled,
                               "cancelled");
                  return;
                }
                cancel_outstanding_tracking();
                set_terminal(LiveWalletWorkloadState::kFailed, "failed",
                             "wallet workload execution was cancelled "
                             "unexpectedly");
                return;
              }
            }
          } catch (const std::exception& error) {
            cancel_outstanding_tracking();
            set_terminal(LiveWalletWorkloadState::kFailed, "failed",
                         error.what());
          } catch (...) {
            cancel_outstanding_tracking();
            set_terminal(LiveWalletWorkloadState::kFailed, "failed",
                         "unknown wallet workload failure");
          }
        });
      } catch (...) {
        const auto found = wallet_workloads->records.find(record->id);
        if (found != wallet_workloads->records.end() &&
            found->second == record) {
          wallet_workloads->records.erase(found);
        }
        throw;
      }
    }
    return record;
  };
}

void JoinLiveWalletWorkloadWorker(LiveWalletWorkloadRecord& record) {
  if (!record.worker.joinable()) {
    return;
  }
  try {
    record.worker.join();
  } catch (...) {
    BBP_LOG(error) << "drained workload worker could not be joined";
    std::terminate();
  }
}

}  // namespace bbp::simulator_app_internal
