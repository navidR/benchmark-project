#include "simulator_raw_transaction_workload.h"

#include <chrono>
#include <cstdint>
#include <exception>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "bbp/drivers/chain_driver.h"
#include "bbp/drivers/chain_driver_registry.h"
#include "bbp/runtime_node_inventory.h"
#include "bbp/simulation_cancelled.h"
#include "bbp/simulation_command.h"
#include "bbp/simulation_event_kind.h"
#include "bbp/simulator/options.h"
#include "bbp/simulator/send_raw_transaction_workload.h"
#include "simulator_block_generation_boundary.h"
#include "simulator_cancellable_waiting.h"
#include "simulator_event_writing.h"
#include "simulator_node_process_state.h"
#include "simulator_transaction_observation_tracking.h"
#include "simulator_workload_event_details.h"

namespace bbp::simulator_app_internal {

void ApplySendRawTransactionWorkload(
    const Options& options, const std::filesystem::path& events_path,
    const ChainDriver& driver, std::timed_mutex& block_generation_mutex,
    const RuntimeNodeSnapshot& nodes,
    TransactionObservationTracker& transaction_tracker,
    const SendRawTransactionWorkload& workload, std::uint32_t workload_index,
    std::uint32_t workload_count, std::stop_token stop_token,
    SimulationCommandControl* cancellation_commit_control) {
  NodeRuntime& funder = nodes[workload.funding_node - 1U];
  NodeRuntime& submitter = nodes[workload.submit_node - 1U];
  RequireNodeRunning(funder, "raw transaction funding");
  RequireNodeRunning(submitter, "raw transaction submission");
  const uint64_t start_height =
      driver.ReadMetrics(funder.config, stop_token).height;
  const auto begin_irreversible_commit = [&] {
    if (cancellation_commit_control == nullptr) {
      return;
    }
    SimulationCommandCommitPhase phase =
        cancellation_commit_control->CommitPhase();
    if (phase == SimulationCommandCommitPhase::kCommitStarted ||
        phase == SimulationCommandCommitPhase::kCommitted) {
      return;
    }
    if (phase == SimulationCommandCommitPhase::kOpen &&
        cancellation_commit_control->TryBeginCommit()) {
      return;
    }
    phase = cancellation_commit_control->CommitPhase();
    if (phase == SimulationCommandCommitPhase::kCommitStarted ||
        phase == SimulationCommandCommitPhase::kCommitted) {
      return;
    }
    if (phase == SimulationCommandCommitPhase::kCancelled) {
      throw SimulationCancelled();
    }
    ThrowIfStopRequested(stop_token);
    throw std::logic_error(
        "raw transaction cancellation won without requesting stop");
  };
  std::vector<std::string> funding_hashes = GenerateBlocksSerialized(
      block_generation_mutex, driver, funder.config, workload.funding_blocks,
      workload.source_address, stop_token, begin_irreversible_commit);
  RecordGeneratedBlocks(driver, funder, funding_hashes, stop_token);
  const uint64_t target_height =
      start_height + static_cast<uint64_t>(funding_hashes.size());
  for (auto& node : nodes) {
    if (!node.AllowsChainMetrics()) {
      continue;
    }
    driver.WaitForHeight(node.config, target_height,
                         std::chrono::seconds(workload.timeout_sec),
                         stop_token);
  }

  const uint64_t minimum_amount =
      workload.amount_satoshis + workload.fee_satoshis;
  const ChainUtxo utxo = driver.FindSpendableOutput(
      funder.config, funding_hashes, workload.source_address, minimum_amount,
      ChainDriverSpecFor(options.chain).coinbase_spendable_confirmations,
      stop_token);
  TransactionObservationTracker::Reservation observation_reservation =
      transaction_tracker.Reserve(nodes);
  std::optional<ChainRawTransactionResult> accepted_transaction;
  bool deterministic_rejection = false;
  ChainRawTransactionBroadcastControl broadcast_control;
  broadcast_control.after_broadcast =
      [&](const ChainRawTransactionResult& transaction) {
        accepted_transaction = transaction;
      };
  if (cancellation_commit_control != nullptr) {
    broadcast_control.before_broadcast = begin_irreversible_commit;
    broadcast_control.deterministic_rejection = [&] {
      deterministic_rejection = true;
    };
  }
  const auto tracked_transaction = [&](std::string txid) {
    return TrackedTransaction{
        .txid = std::move(txid),
        .submission_kind = "raw_transaction_submitted",
        .workload_id = {},
        .workload_index = workload_index,
        .workload_count = workload_count,
        .transaction_index = 1U,
        .transaction_count = 1U,
        .transaction_rate = std::nullopt,
        .txid_index = 1U,
        .submission_node = workload.submit_node,
        .load_confirmation = nullptr,
    };
  };
  ChainRawTransactionResult transaction;
  try {
    transaction = driver.SendRawTransaction(
        submitter.config, utxo, workload.source_address,
        workload.source_private_key, workload.destination_address,
        workload.amount_satoshis, workload.fee_satoshis,
        std::chrono::seconds(workload.timeout_sec), stop_token,
        &broadcast_control);
  } catch (...) {
    const std::exception_ptr failure = std::current_exception();
    if (accepted_transaction && !accepted_transaction->txid.empty()) {
      const TrackedTransaction tracked =
          tracked_transaction(accepted_transaction->txid);
      try {
        WriteEvent(
            events_path, options.run_id, submitter.config.id,
            SimulationEventKind::kRawTransactionSubmitted,
            RawTransactionDetail(workload_index, workload_count, workload,
                                 start_height, target_height, funding_hashes,
                                 *accepted_transaction, false));
      } catch (...) {
        const std::exception_ptr event_failure = std::current_exception();
        transaction_tracker.Track(std::move(observation_reservation), tracked);
        std::rethrow_exception(event_failure);
      }
      transaction_tracker.Track(std::move(observation_reservation), tracked);
    }
    if (deterministic_rejection) {
      throw OneShotRawTransactionRejected();
    }
    std::rethrow_exception(failure);
  }
  if (transaction.txid.empty()) {
    throw std::runtime_error(
        "raw transaction submission returned an empty transaction id");
  }
  const TrackedTransaction tracked = tracked_transaction(transaction.txid);
  try {
    WriteEvent(events_path, options.run_id, submitter.config.id,
               SimulationEventKind::kRawTransactionSubmitted,
               RawTransactionDetail(workload_index, workload_count, workload,
                                    start_height, target_height, funding_hashes,
                                    transaction));
  } catch (...) {
    const std::exception_ptr event_failure = std::current_exception();
    transaction_tracker.Track(std::move(observation_reservation), tracked);
    std::rethrow_exception(event_failure);
  }
  transaction_tracker.Track(std::move(observation_reservation), tracked);
  transaction_tracker.WaitForVisibility(
      options, events_path, driver, nodes, tracked,
      std::chrono::seconds(workload.timeout_sec), stop_token);
  if (cancellation_commit_control != nullptr) {
    cancellation_commit_control->MarkCommitted();
  }
}

}  // namespace bbp::simulator_app_internal
