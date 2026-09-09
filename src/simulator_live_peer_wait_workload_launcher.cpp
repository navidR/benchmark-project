#include "simulator_live_peer_wait_workload_launcher.h"

#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <exception>
#include <limits>
#include <memory>
#include <mutex>
#include <optional>
#include <stdexcept>
#include <stop_token>
#include <string>
#include <thread>
#include <utility>

#include "bbp/drivers/chain_driver.h"
#include "bbp/logging.h"
#include "bbp/mcp_live_application.h"
#include "bbp/mcp_operation_service.h"
#include "bbp/runtime_node_inventory.h"
#include "bbp/simulation_cancelled.h"
#include "bbp/simulation_event_kind.h"
#include "bbp/simulator/options.h"
#include "simulator_block_generation_boundary.h"
#include "simulator_combined_stop_token.h"
#include "simulator_event_writing.h"
#include "simulator_live_workload_state.h"
#include "simulator_node_process_state.h"
#include "simulator_workload_event_details.h"

namespace bbp::simulator_app_internal {

LivePeerWaitWorkloadLauncher MakeLivePeerWaitWorkloadLauncher(
    const Options& options, const std::filesystem::path& events_path,
    const ChainDriver& driver, const RuntimeNodeInventory& node_inventory,
    std::atomic<RunStopTick>& run_stop_tick,
    std::function<std::unique_lock<std::timed_mutex>(std::stop_token)>
        acquire_node_mutation_lock,
    McpLiveWorkloadService& workload_service,
    std::shared_ptr<LiveWalletWorkloadRegistry> wallet_workloads,
    std::shared_ptr<LiveBlockGenerationWorkloadRegistry>
        block_generation_workloads,
    std::shared_ptr<LiveWaitUntilHeightWorkloadRegistry>
        wait_until_height_workloads,
    std::shared_ptr<LiveWaitForPeersWorkloadRegistry>
        wait_for_peers_workloads) {
  return [&options, &events_path, &driver, &node_inventory, &run_stop_tick,
          &workload_service,
          acquire_node_mutation_lock = std::move(acquire_node_mutation_lock),
          wallet_workloads = std::move(wallet_workloads),
          block_generation_workloads = std::move(block_generation_workloads),
          wait_until_height_workloads = std::move(wait_until_height_workloads),
          wait_for_peers_workloads = std::move(wait_for_peers_workloads)](
             WaitForPeersWorkload workload,
             std::optional<std::string> requested_id)
             -> std::shared_ptr<LiveWaitForPeersWorkloadRecord> {
    auto record = std::make_shared<LiveWaitForPeersWorkloadRecord>();
    record->workload = workload;
    {
      std::scoped_lock registry_lock(
          wallet_workloads->mutex, block_generation_workloads->mutex,
          wait_for_peers_workloads->mutex, wait_until_height_workloads->mutex);
      if (wallet_workloads->shutting_down ||
          block_generation_workloads->shutting_down ||
          wait_for_peers_workloads->shutting_down ||
          wait_until_height_workloads->shutting_down) {
        throw McpOperationFailure(
            "run_not_active",
            "the run is stopping and cannot start another workload", false);
      }
      if (wallet_workloads->records.size() +
              block_generation_workloads->records.size() +
              wait_for_peers_workloads->records.size() +
              wait_until_height_workloads->records.size() >=
          kMaximumScenarioActionCount) {
        throw McpOperationFailure(
            "workload_capacity_exceeded",
            "workload retained-instance capacity is exhausted", false);
      }
      if (requested_id) {
        ValidateMcpIdentifier(*requested_id, "workload_id");
        if (wallet_workloads->records.contains(*requested_id) ||
            block_generation_workloads->records.contains(*requested_id) ||
            wait_for_peers_workloads->records.contains(*requested_id) ||
            wait_until_height_workloads->records.contains(*requested_id)) {
          throw McpOperationFailure(
              "workload_id_conflict",
              "workload_id is already retained: " + *requested_id, false);
        }
        record->id = *requested_id;
      } else {
        do {
          if (wait_for_peers_workloads->next_id ==
              std::numeric_limits<std::uint64_t>::max()) {
            throw McpOperationFailure(
                "workload_id_exhausted",
                "wait-for-peers workload identity sequence is exhausted",
                false);
          }
          record->id = "wait-for-peers-workload-" +
                       std::to_string(wait_for_peers_workloads->next_id++);
        } while (wallet_workloads->records.contains(record->id) ||
                 block_generation_workloads->records.contains(record->id) ||
                 wait_for_peers_workloads->records.contains(record->id) ||
                 wait_until_height_workloads->records.contains(record->id));
      }
      record->ordinal = static_cast<std::uint32_t>(
          wallet_workloads->records.size() +
          block_generation_workloads->records.size() +
          wait_for_peers_workloads->records.size() +
          wait_until_height_workloads->records.size() + 1U);
      wait_for_peers_workloads->records.emplace(record->id, record);

      try {
        auto worker_lease = workload_service.AcquireWorkerLease();
        record->worker = std::thread([&options, &events_path, &driver,
                                      &node_inventory, &run_stop_tick,
                                      acquire_node_mutation_lock, record,
                                      worker_lease = std::move(worker_lease)] {
          const std::stop_token service_stop_token = worker_lease.stop_token();
          const auto set_terminal = [&](LiveWorkloadState state,
                                        std::string outcome,
                                        std::optional<std::string> failure =
                                            std::nullopt) {
            {
              std::lock_guard<std::mutex> lock(record->mutex);
              record->state = state;
              record->terminal_outcome = std::move(outcome);
              record->failure = std::move(failure);
              record->completion_pending = false;
              record->changed.notify_all();
            }
            try {
              WriteLiveWaitForPeersWorkloadState(events_path, options, *record);
            } catch (const std::exception& error) {
              BBP_LOG(error) << "failed to publish terminal wait-for-peers "
                                "workload "
                             << record->id << ": " << error.what();
            }
          };
          try {
            while (true) {
              WaitForPeersWorkload epoch_workload;
              std::stop_source epoch_stop_source;
              std::chrono::steady_clock::time_point deadline;
              bool publish_running = false;
              {
                std::unique_lock<std::mutex> lock(record->mutex);
                while (record->request == LiveWorkloadRequest::kPause) {
                  record->state = LiveWorkloadState::kPaused;
                  record->epoch_deadline =
                      std::chrono::steady_clock::time_point{};
                  record->epoch_timed_out = false;
                  record->epoch_run_stop_requested_at.reset();
                  record->changed.notify_all();
                  lock.unlock();
                  WriteLiveWaitForPeersWorkloadState(events_path, options,
                                                     *record);
                  lock.lock();
                  if (!record->changed.wait(lock, service_stop_token, [&] {
                        return record->request != LiveWorkloadRequest::kPause;
                      })) {
                    record->request = LiveWorkloadRequest::kShutdown;
                    record->epoch_stop_source.request_stop();
                    break;
                  }
                }
                if (record->request == LiveWorkloadRequest::kStopCancel ||
                    record->request == LiveWorkloadRequest::kStopSettle) {
                  lock.unlock();
                  set_terminal(LiveWorkloadState::kStopped, "stopped");
                  return;
                }
                if (record->request == LiveWorkloadRequest::kShutdown) {
                  lock.unlock();
                  set_terminal(LiveWorkloadState::kCancelled, "cancelled");
                  return;
                }
                if (record->request == LiveWorkloadRequest::kRunFailure) {
                  lock.unlock();
                  set_terminal(LiveWorkloadState::kFailed, "failed",
                               "run failed while wait-for-peers workload "
                               "was active");
                  return;
                }
                if (record->request == LiveWorkloadRequest::kReconfigure) {
                  if (!record->pending_workload) {
                    throw std::logic_error(
                        "wait-for-peers workload reconfigure has no "
                        "configuration");
                  }
                  if (record->configuration_revision ==
                      std::numeric_limits<std::uint64_t>::max()) {
                    throw std::runtime_error(
                        "wait-for-peers workload configuration revision "
                        "exceeds uint64");
                  }
                  record->workload = *record->pending_workload;
                  record->pending_workload.reset();
                  ++record->configuration_revision;
                  record->request = LiveWorkloadRequest::kNone;
                  record->state = LiveWorkloadState::kStarting;
                  record->epoch_deadline =
                      std::chrono::steady_clock::time_point{};
                  record->changed.notify_all();
                  lock.unlock();
                  WriteLiveWaitForPeersWorkloadState(events_path, options,
                                                     *record);
                  lock.lock();
                  if (record->request != LiveWorkloadRequest::kNone) {
                    continue;
                  }
                }
                if (service_stop_token.stop_requested()) {
                  record->request = LiveWorkloadRequest::kShutdown;
                  lock.unlock();
                  set_terminal(LiveWorkloadState::kCancelled, "cancelled");
                  return;
                }
                record->epoch_stop_source = std::stop_source();
                epoch_stop_source = record->epoch_stop_source;
                epoch_workload = record->workload;
                deadline = std::chrono::steady_clock::now() +
                           std::chrono::seconds(epoch_workload.timeout_sec);
                record->epoch_deadline = deadline;
                record->epoch_timed_out = false;
                record->epoch_run_stop_requested_at.reset();
                if (record->state != LiveWorkloadState::kRunning) {
                  record->state = LiveWorkloadState::kRunning;
                  publish_running = true;
                }
                record->changed.notify_all();
              }
              if (publish_running) {
                WriteLiveWaitForPeersWorkloadState(events_path, options,
                                                   *record);
              }

              CombinedStopToken execution_stop(service_stop_token,
                                               epoch_stop_source.get_token());
              const std::stop_token execution_stop_token =
                  execution_stop.get_token();
              std::stop_callback stop_epoch_on_service_stop(
                  service_stop_token, [&, epoch_stop_source] {
                    const auto observed_at = std::chrono::steady_clock::now();
                    RecordRunStop(run_stop_tick, observed_at);
                    const auto requested_at =
                        ObservedRunStop(run_stop_tick).value_or(observed_at);
                    std::lock_guard<std::mutex> lock(record->mutex);
                    if (record->completion_pending) {
                      return;
                    }
                    record->epoch_run_stop_requested_at = requested_at;
                    epoch_stop_source.request_stop();
                    record->changed.notify_all();
                  });
              std::mutex deadline_mutex;
              std::condition_variable_any deadline_changed;
              std::jthread deadline_timer(
                  [&, epoch_stop_source](std::stop_token timer_stop_token) {
                    {
                      std::unique_lock<std::mutex> lock(deadline_mutex);
                      static_cast<void>(deadline_changed.wait_until(
                          lock, timer_stop_token, deadline,
                          [] { return false; }));
                    }
                    if (timer_stop_token.stop_requested()) {
                      return;
                    }
                    std::lock_guard<std::mutex> lock(record->mutex);
                    if (!timer_stop_token.stop_requested() &&
                        (!record->epoch_run_stop_requested_at ||
                         *record->epoch_run_stop_requested_at >= deadline) &&
                        (record->request == LiveWorkloadRequest::kNone ||
                         record->request == LiveWorkloadRequest::kStopSettle)) {
                      record->epoch_timed_out = true;
                      epoch_stop_source.request_stop();
                    }
                  });
              try {
                ChainNodeConfig target_config;
                {
                  auto mutation_lock =
                      acquire_node_mutation_lock(execution_stop_token);
                  const RuntimeNodeSnapshot execution_nodes =
                      node_inventory.Snapshot();
                  NodeRuntime& node = RequireRuntimeNodeNumber(
                      execution_nodes, epoch_workload.node,
                      "wait_for_peers workload");
                  RequireNodeRunning(node, "wait_for_peers workload");
                  target_config = node.config;
                }
                while (true) {
                  const std::uint64_t observed_peer_count =
                      driver.WaitForPeerCount(
                          target_config, epoch_workload.peer_count,
                          std::chrono::seconds(epoch_workload.timeout_sec),
                          execution_stop_token);
                  std::optional<LiveWaitForPeersResult> completed_result;
                  if (observed_peer_count >= epoch_workload.peer_count) {
                    completed_result.emplace(LiveWaitForPeersResult{
                        .node = epoch_workload.node,
                        .node_id = target_config.id,
                        .target_peer_count = epoch_workload.peer_count,
                        .observed_peer_count = observed_peer_count,
                    });
                  }
                  std::unique_lock<std::mutex> lock(record->mutex);
                  if ((record->request != LiveWorkloadRequest::kNone &&
                       record->request != LiveWorkloadRequest::kStopSettle)) {
                    throw SimulationCancelled();
                  }
                  const auto require_open_epoch = [&] {
                    if (record->epoch_run_stop_requested_at &&
                        *record->epoch_run_stop_requested_at < deadline) {
                      throw SimulationCancelled();
                    }
                    if (record->epoch_timed_out ||
                        std::chrono::steady_clock::now() >= deadline) {
                      record->epoch_timed_out = true;
                      epoch_stop_source.request_stop();
                      throw SimulationCancelled();
                    }
                    if (execution_stop_token.stop_requested()) {
                      throw SimulationCancelled();
                    }
                  };
                  require_open_epoch();
                  if (observed_peer_count < epoch_workload.peer_count) {
                    lock.unlock();
                    continue;
                  }
                  deadline_timer.request_stop();
                  require_open_epoch();
                  const bool settle =
                      record->request == LiveWorkloadRequest::kStopSettle;
                  record->completion_pending = true;
                  record->state = LiveWorkloadState::kStopping;
                  record->changed.notify_all();
                  lock.unlock();
                  WriteEvent(events_path, options.run_id, target_config.id,
                             SimulationEventKind::kPeerCountReached,
                             PeerCountWaitDetail(
                                 record->ordinal, 0U, epoch_workload.node,
                                 epoch_workload.peer_count, observed_peer_count,
                                 record->id));
                  lock.lock();
                  record->result = std::move(*completed_result);
                  record->completion_pending = false;
                  if (settle) {
                    record->state = LiveWorkloadState::kStopped;
                    record->terminal_outcome = "stopped";
                  } else {
                    record->state = LiveWorkloadState::kCompleted;
                    record->terminal_outcome = "peer_count_reached";
                  }
                  record->changed.notify_all();
                  lock.unlock();
                  try {
                    WriteLiveWaitForPeersWorkloadState(events_path, options,
                                                       *record);
                  } catch (const std::exception& error) {
                    BBP_LOG(error)
                        << "failed to publish completed wait-for-peers "
                           "workload "
                        << record->id << ": " << error.what();
                  }
                  return;
                }
              } catch (const SimulationCancelled&) {
                deadline_timer.request_stop();
                LiveWorkloadRequest request;
                bool timed_out = false;
                bool run_stop_admitted = false;
                {
                  std::lock_guard<std::mutex> lock(record->mutex);
                  request = record->request;
                  run_stop_admitted =
                      record->epoch_run_stop_requested_at &&
                      *record->epoch_run_stop_requested_at < deadline;
                  if (!run_stop_admitted &&
                      (request == LiveWorkloadRequest::kNone ||
                       request == LiveWorkloadRequest::kStopSettle) &&
                      std::chrono::steady_clock::now() >= deadline) {
                    record->epoch_timed_out = true;
                    epoch_stop_source.request_stop();
                  }
                  timed_out = record->epoch_timed_out && !run_stop_admitted;
                }
                if (timed_out) {
                  throw std::runtime_error(
                      "wait_for_peers workload timed out after " +
                      std::to_string(epoch_workload.timeout_sec) +
                      " seconds waiting for peer count " +
                      std::to_string(epoch_workload.peer_count));
                }
                if (!run_stop_admitted &&
                    (request == LiveWorkloadRequest::kPause ||
                     request == LiveWorkloadRequest::kReconfigure)) {
                  continue;
                }
                throw;
              }
            }
          } catch (const SimulationCancelled&) {
            LiveWorkloadRequest request;
            bool run_stop_admitted = false;
            {
              std::lock_guard<std::mutex> lock(record->mutex);
              request = record->request;
              run_stop_admitted =
                  record->epoch_run_stop_requested_at &&
                  *record->epoch_run_stop_requested_at < record->epoch_deadline;
            }
            if (request == LiveWorkloadRequest::kRunFailure) {
              set_terminal(
                  LiveWorkloadState::kFailed, "failed",
                  "run failed while wait-for-peers workload was active");
            } else if (run_stop_admitted ||
                       request == LiveWorkloadRequest::kShutdown ||
                       service_stop_token.stop_requested()) {
              set_terminal(LiveWorkloadState::kCancelled, "cancelled");
            } else if (request == LiveWorkloadRequest::kStopCancel ||
                       request == LiveWorkloadRequest::kStopSettle) {
              set_terminal(LiveWorkloadState::kStopped, "stopped");
            } else {
              set_terminal(LiveWorkloadState::kFailed, "failed",
                           "wait-for-peers workload execution was cancelled "
                           "unexpectedly");
            }
          } catch (const std::exception& error) {
            set_terminal(LiveWorkloadState::kFailed, "failed", error.what());
          } catch (...) {
            set_terminal(LiveWorkloadState::kFailed, "failed",
                         "unknown wait-for-peers workload failure");
          }
        });
      } catch (...) {
        const auto found = wait_for_peers_workloads->records.find(record->id);
        if (found != wait_for_peers_workloads->records.end() &&
            found->second == record) {
          wait_for_peers_workloads->records.erase(found);
        }
        throw;
      }
    }
    return record;
  };
}

void JoinLivePeerWaitWorkloadWorker(LiveWaitForPeersWorkloadRecord& record) {
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
