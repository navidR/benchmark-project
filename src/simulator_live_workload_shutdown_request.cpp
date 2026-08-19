#include "simulator_live_workload_shutdown_request.h"

#include <chrono>
#include <cstddef>
#include <memory>
#include <mutex>
#include <stdexcept>

namespace bbp::simulator_app_internal {

WorkloadShutdownRecords RequestLiveWorkloadShutdown(
    const std::shared_ptr<LiveWalletWorkloadRegistry>& wallet_workloads,
    const std::shared_ptr<LiveBlockGenerationWorkloadRegistry>&
        block_generation_workloads,
    const std::shared_ptr<LiveWaitUntilHeightWorkloadRegistry>&
        wait_until_height_workloads,
    const std::shared_ptr<LiveWaitForPeersWorkloadRegistry>&
        wait_for_peers_workloads,
    bool run_failed,
    std::chrono::steady_clock::time_point shutdown_requested_at) {
  WorkloadShutdownRecords retained;
  {
    std::scoped_lock lock(
        wallet_workloads->mutex, block_generation_workloads->mutex,
        wait_until_height_workloads->mutex, wait_for_peers_workloads->mutex);
    wallet_workloads->shutting_down = true;
    block_generation_workloads->shutting_down = true;
    wait_until_height_workloads->shutting_down = true;
    wait_for_peers_workloads->shutting_down = true;
    for (const auto& [id, record] : wallet_workloads->records) {
      static_cast<void>(id);
      if (retained.wallet_count == retained.wallets.size()) {
        throw std::logic_error(
            "wallet workload registry exceeds its configured capacity");
      }
      retained.wallets[retained.wallet_count++] = record;
    }
    for (const auto& [id, record] : block_generation_workloads->records) {
      static_cast<void>(id);
      if (retained.block_generator_count == retained.block_generators.size()) {
        throw std::logic_error(
            "block generation workload registry exceeds its configured "
            "capacity");
      }
      retained.block_generators[retained.block_generator_count++] = record;
    }
    for (const auto& [id, record] : wait_until_height_workloads->records) {
      static_cast<void>(id);
      if (retained.height_wait_count == retained.height_waits.size()) {
        throw std::logic_error(
            "wait-until-height workload registry exceeds its configured "
            "capacity");
      }
      retained.height_waits[retained.height_wait_count++] = record;
    }
    for (const auto& [id, record] : wait_for_peers_workloads->records) {
      static_cast<void>(id);
      if (retained.peer_wait_count == retained.peer_waits.size()) {
        throw std::logic_error(
            "wait-for-peers workload registry exceeds its configured "
            "capacity");
      }
      retained.peer_waits[retained.peer_wait_count++] = record;
    }
  }
  for (std::size_t index = 0U; index < retained.wallet_count; ++index) {
    const std::shared_ptr<LiveWalletWorkloadRecord>& record =
        retained.wallets[index];
    std::lock_guard<std::mutex> lock(record->mutex);
    if (IsTerminalLiveWalletWorkloadState(record->state)) {
      continue;
    }
    record->state = LiveWalletWorkloadState::kStopping;
    record->request = run_failed ? LiveWalletWorkloadRequest::kRunFailure
                                 : LiveWalletWorkloadRequest::kShutdown;
    record->changed.notify_all();
  }
  for (std::size_t index = 0U; index < retained.block_generator_count;
       ++index) {
    const std::shared_ptr<LiveBlockGenerationWorkloadRecord>& record =
        retained.block_generators[index];
    std::lock_guard<std::mutex> lock(record->mutex);
    if (IsTerminalLiveWorkloadState(record->state)) {
      continue;
    }
    record->state = LiveWorkloadState::kStopping;
    record->request = run_failed ? LiveWorkloadRequest::kRunFailure
                                 : LiveWorkloadRequest::kShutdown;
    record->changed.notify_all();
  }
  for (std::size_t index = 0U; index < retained.height_wait_count; ++index) {
    const std::shared_ptr<LiveWaitUntilHeightWorkloadRecord>& record =
        retained.height_waits[index];
    std::lock_guard<std::mutex> lock(record->mutex);
    if (IsTerminalLiveWorkloadState(record->state) ||
        record->completion_pending) {
      continue;
    }
    if ((record->state == LiveWorkloadState::kRunning ||
         (record->state == LiveWorkloadState::kStopping &&
          record->request == LiveWorkloadRequest::kStopSettle)) &&
        record->epoch_deadline != std::chrono::steady_clock::time_point{} &&
        shutdown_requested_at >= record->epoch_deadline &&
        (record->request == LiveWorkloadRequest::kNone ||
         record->request == LiveWorkloadRequest::kStopSettle)) {
      record->epoch_timed_out = true;
      record->changed.notify_all();
      continue;
    }
    record->state = LiveWorkloadState::kStopping;
    record->request = run_failed ? LiveWorkloadRequest::kRunFailure
                                 : LiveWorkloadRequest::kShutdown;
    record->changed.notify_all();
  }
  for (std::size_t index = 0U; index < retained.peer_wait_count; ++index) {
    const std::shared_ptr<LiveWaitForPeersWorkloadRecord>& record =
        retained.peer_waits[index];
    std::lock_guard<std::mutex> lock(record->mutex);
    if (IsTerminalLiveWorkloadState(record->state) ||
        record->completion_pending) {
      continue;
    }
    if ((record->state == LiveWorkloadState::kRunning ||
         (record->state == LiveWorkloadState::kStopping &&
          record->request == LiveWorkloadRequest::kStopSettle)) &&
        record->epoch_deadline != std::chrono::steady_clock::time_point{} &&
        shutdown_requested_at >= record->epoch_deadline &&
        (record->request == LiveWorkloadRequest::kNone ||
         record->request == LiveWorkloadRequest::kStopSettle)) {
      record->epoch_timed_out = true;
      record->changed.notify_all();
      continue;
    }
    record->state = LiveWorkloadState::kStopping;
    record->request = run_failed ? LiveWorkloadRequest::kRunFailure
                                 : LiveWorkloadRequest::kShutdown;
    record->changed.notify_all();
  }
  return retained;
}

}  // namespace bbp::simulator_app_internal
