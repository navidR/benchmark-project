#include "simulator_live_block_generation_workload_launcher.h"

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

#include "bbp/logging.h"
#include "bbp/mcp_live_application.h"
#include "bbp/mcp_operation_service.h"
#include "bbp/runtime_node_inventory.h"
#include "bbp/simulation_cancelled.h"
#include "bbp/simulator/options.h"
#include "simulator_block_generation_boundary.h"
#include "simulator_combined_stop_token.h"
#include "simulator_live_workload_state.h"

namespace bbp::simulator_app_internal {

LiveBlockGenerationWorkloadLauncher MakeLiveBlockGenerationWorkloadLauncher(
    const Options& options, const std::filesystem::path& events_path,
    const ChainDriver& driver, const RuntimeNodeInventory& node_inventory,
    const std::string& default_reward_address,
    std::timed_mutex& block_generation_mutex,
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
  return [&options, &events_path, &driver, &node_inventory,
          &default_reward_address, &block_generation_mutex, &workload_service,
          acquire_node_mutation_lock = std::move(acquire_node_mutation_lock),
          wallet_workloads = std::move(wallet_workloads),
          block_generation_workloads = std::move(block_generation_workloads),
          wait_until_height_workloads = std::move(wait_until_height_workloads),
          wait_for_peers_workloads = std::move(wait_for_peers_workloads)](
             BlockGenerationWorkload workload,
             std::optional<std::string> requested_id)
             -> std::shared_ptr<LiveBlockGenerationWorkloadRecord> {
    auto record = std::make_shared<LiveBlockGenerationWorkloadRecord>();
    record->workload = workload;
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
      if (wallet_workloads->records.size() +
              block_generation_workloads->records.size() +
              wait_until_height_workloads->records.size() +
              wait_for_peers_workloads->records.size() >=
          kMaximumScenarioActionCount) {
        throw McpOperationFailure(
            "workload_capacity_exceeded",
            "workload retained-instance capacity is exhausted", false);
      }
      for (const auto& [id, existing] : block_generation_workloads->records) {
        static_cast<void>(id);
        std::lock_guard<std::mutex> existing_lock(existing->mutex);
        if (!IsTerminalLiveWorkloadState(existing->state)) {
          throw McpOperationFailure(
              "workload_capacity_exceeded",
              "another block generation workload is already active", true);
        }
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
          if (block_generation_workloads->next_id ==
              std::numeric_limits<std::uint64_t>::max()) {
            throw McpOperationFailure(
                "workload_id_exhausted",
                "block generation workload identity sequence is exhausted",
                false);
          }
          record->id = "block-generation-workload-" +
                       std::to_string(block_generation_workloads->next_id++);
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
      block_generation_workloads->records.emplace(record->id, record);

      try {
        auto worker_lease = workload_service.AcquireWorkerLease();
        record->worker = std::thread([&options, &events_path, &driver,
                                      &node_inventory, &default_reward_address,
                                      &block_generation_mutex,
                                      acquire_node_mutation_lock, record,
                                      worker_lease = std::move(worker_lease)] {
          const std::stop_token service_stop_token = worker_lease.stop_token();
          const auto set_terminal =
              [&](LiveWorkloadState state, std::string outcome,
                  std::optional<std::string> failure = std::nullopt) {
                {
                  std::lock_guard<std::mutex> lock(record->mutex);
                  record->state = state;
                  record->terminal_outcome = std::move(outcome);
                  record->failure = std::move(failure);
                  record->changed.notify_all();
                }
                try {
                  WriteLiveBlockGenerationWorkloadState(events_path, options,
                                                        *record);
                } catch (const std::exception& error) {
                  BBP_LOG(error)
                      << "failed to publish terminal block generation "
                         "workload "
                      << record->id << ": " << error.what();
                }
              };
          const auto finalize_outstanding_attempt = [&](bool cancelled) {
            std::lock_guard<std::mutex> lock(record->mutex);
            const std::uint64_t finalized = record->completed_boundaries +
                                            record->failed + record->cancelled;
            if (record->attempted > finalized) {
              if (cancelled) {
                ++record->cancelled;
              } else {
                ++record->failed;
              }
            }
          };
          try {
            while (true) {
              BlockGenerationWorkload boundary_workload;
              std::stop_token boundary_stop_token;
              bool publish_running = false;
              {
                std::unique_lock<std::mutex> lock(record->mutex);
                record->boundary_mutation_admitted = false;
                while (record->request == LiveWorkloadRequest::kPause) {
                  record->state = LiveWorkloadState::kPaused;
                  record->changed.notify_all();
                  lock.unlock();
                  WriteLiveBlockGenerationWorkloadState(events_path, options,
                                                        *record);
                  lock.lock();
                  if (!record->changed.wait(lock, service_stop_token, [&] {
                        return record->request != LiveWorkloadRequest::kPause;
                      })) {
                    record->request = LiveWorkloadRequest::kShutdown;
                    record->boundary_stop_source.request_stop();
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
                               "run failed while block generation workload "
                               "was active");
                  return;
                }
                if (record->request == LiveWorkloadRequest::kReconfigure) {
                  if (!record->pending_workload) {
                    throw std::logic_error(
                        "block generation workload reconfigure has no "
                        "configuration");
                  }
                  if (record->configuration_revision ==
                      std::numeric_limits<std::uint64_t>::max()) {
                    throw std::runtime_error(
                        "block generation workload configuration revision "
                        "exceeds uint64");
                  }
                  record->workload = *record->pending_workload;
                  record->pending_workload.reset();
                  ++record->configuration_revision;
                  record->request = LiveWorkloadRequest::kNone;
                  record->state = LiveWorkloadState::kStarting;
                  record->changed.notify_all();
                  lock.unlock();
                  WriteLiveBlockGenerationWorkloadState(events_path, options,
                                                        *record);
                  lock.lock();
                }
                if (record->completed_boundaries >= record->workload.count) {
                  lock.unlock();
                  set_terminal(LiveWorkloadState::kCompleted, "count_reached");
                  return;
                }
                if (service_stop_token.stop_requested()) {
                  record->request = LiveWorkloadRequest::kShutdown;
                  lock.unlock();
                  set_terminal(LiveWorkloadState::kCancelled, "cancelled");
                  return;
                }
                record->boundary_stop_source = std::stop_source();
                boundary_stop_token = record->boundary_stop_source.get_token();
                boundary_workload = record->workload;
                boundary_workload.count = 1U;
                if (record->state != LiveWorkloadState::kRunning) {
                  record->state = LiveWorkloadState::kRunning;
                  publish_running = true;
                }
                if (record->attempted ==
                    std::numeric_limits<std::uint64_t>::max()) {
                  throw std::runtime_error(
                      "block generation workload attempt count exceeds "
                      "uint64");
                }
                ++record->attempted;
                record->changed.notify_all();
              }
              if (publish_running) {
                WriteLiveBlockGenerationWorkloadState(events_path, options,
                                                      *record);
              }

              CombinedStopToken execution_stop(service_stop_token,
                                               boundary_stop_token);
              const std::stop_token execution_stop_token =
                  execution_stop.get_token();
              auto mutation_lock =
                  acquire_node_mutation_lock(execution_stop_token);
              RuntimeNodeSnapshot execution_nodes = node_inventory.Snapshot();
              const auto authorize_mutation = [&, record] {
                std::lock_guard<std::mutex> lock(record->mutex);
                if (execution_stop_token.stop_requested() ||
                    record->request == LiveWorkloadRequest::kStopCancel ||
                    record->request == LiveWorkloadRequest::kShutdown ||
                    record->request == LiveWorkloadRequest::kRunFailure) {
                  throw SimulationCancelled();
                }
                if (record->boundary_mutation_admitted) {
                  throw std::logic_error(
                      "block generation workload admitted one mutation "
                      "twice");
                }
                record->boundary_mutation_admitted = true;
                record->changed.notify_all();
              };
              const GeneratedBlockWorkloadBoundary boundary =
                  GenerateBlockWorkloadBoundary(
                      driver, block_generation_mutex, execution_nodes,
                      boundary_workload, default_reward_address,
                      std::stop_token{}, execution_stop_token,
                      authorize_mutation);
              if (boundary.hashes.size() != 1U) {
                throw std::logic_error(
                    "single block generation boundary returned an invalid "
                    "hash count");
              }
              {
                std::lock_guard<std::mutex> lock(record->mutex);
                ++record->generated;
                record->last_result = LiveBlockGenerationBoundaryResult{
                    .generator_node = boundary.generator_node,
                    .generator_node_id = boundary.generator_node_id,
                    .start_height = boundary.start_height,
                    .target_height = boundary.target_height,
                    .block_hash = boundary.hashes.front(),
                    .reward_address = boundary.reward_address,
                    .synchronized = false,
                };
                record->changed.notify_all();
              }
              RecordAndPublishGeneratedBlockWorkloadBoundary(
                  options, events_path, driver, execution_nodes, boundary,
                  record->ordinal, 0U, std::stop_token{}, record->id);
              SynchronizeBlockWorkloadBoundary(
                  options, events_path, driver, execution_nodes, boundary,
                  boundary_workload.sync_timeout_sec, execution_stop_token);
              {
                std::lock_guard<std::mutex> lock(record->mutex);
                ++record->completed_boundaries;
                if (!record->last_result || record->last_result->block_hash !=
                                                boundary.hashes.front()) {
                  throw std::logic_error(
                      "block generation workload lost its boundary result");
                }
                record->last_result->synchronized = true;
                record->changed.notify_all();
              }
            }
          } catch (const SimulationCancelled&) {
            LiveWorkloadRequest request;
            {
              std::lock_guard<std::mutex> lock(record->mutex);
              request = record->request;
            }
            if (request == LiveWorkloadRequest::kRunFailure) {
              finalize_outstanding_attempt(false);
              set_terminal(
                  LiveWorkloadState::kFailed, "failed",
                  "run failed while block generation workload was active");
            } else if (request == LiveWorkloadRequest::kStopCancel ||
                       request == LiveWorkloadRequest::kStopSettle) {
              finalize_outstanding_attempt(true);
              set_terminal(LiveWorkloadState::kStopped, "stopped");
            } else if (request == LiveWorkloadRequest::kShutdown ||
                       service_stop_token.stop_requested()) {
              finalize_outstanding_attempt(true);
              set_terminal(LiveWorkloadState::kCancelled, "cancelled");
            } else {
              finalize_outstanding_attempt(false);
              set_terminal(LiveWorkloadState::kFailed, "failed",
                           "block generation workload execution was cancelled "
                           "unexpectedly");
            }
          } catch (const BlockGenerationOutcomeUnconfirmed& error) {
            {
              std::lock_guard<std::mutex> lock(record->mutex);
              record->generation_outcome_unconfirmed = true;
            }
            finalize_outstanding_attempt(false);
            set_terminal(LiveWorkloadState::kFailed, "failed", error.what());
          } catch (const std::exception& error) {
            finalize_outstanding_attempt(false);
            set_terminal(LiveWorkloadState::kFailed, "failed", error.what());
          } catch (...) {
            finalize_outstanding_attempt(false);
            set_terminal(LiveWorkloadState::kFailed, "failed",
                         "unknown block generation workload failure");
          }
        });
      } catch (...) {
        const auto found = block_generation_workloads->records.find(record->id);
        if (found != block_generation_workloads->records.end() &&
            found->second == record) {
          block_generation_workloads->records.erase(found);
        }
        throw;
      }
    }
    return record;
  };
}

void JoinLiveBlockGenerationWorkloadWorker(
    LiveBlockGenerationWorkloadRecord& record) {
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
