#include "simulator_live_workload_shutdown.h"

#include <boost/json/serialize.hpp>
#include <chrono>
#include <cstddef>
#include <exception>
#include <memory>
#include <mutex>
#include <optional>

#include "bbp/logging.h"
#include "bbp/mcp_live_application.h"
#include "bbp/simulation_event_kind.h"
#include "bbp/simulator/options.h"
#include "simulator_event_writing.h"
#include "simulator_live_block_generation_workload_launcher.h"
#include "simulator_live_height_wait_workload_launcher.h"
#include "simulator_live_peer_wait_workload_launcher.h"
#include "simulator_live_wallet_workload_launcher.h"
#include "simulator_live_workload_shutdown_request.h"
#include "simulator_live_workload_state.h"
#include "simulator_workload_service_shutdown_diagnostic.h"

namespace bbp::simulator_app_internal {

void StopLiveWorkloads(
    const Options& options, const std::filesystem::path& events_path,
    McpLiveApplication& mcp_application,
    std::shared_ptr<McpLiveWorkloadService>& installed_workload_service,
    const std::shared_ptr<LiveWalletWorkloadRegistry>& wallet_workloads,
    const std::shared_ptr<LiveBlockGenerationWorkloadRegistry>&
        block_generation_workloads,
    const std::shared_ptr<LiveWaitUntilHeightWorkloadRegistry>&
        wait_until_height_workloads,
    const std::shared_ptr<LiveWaitForPeersWorkloadRegistry>&
        wait_for_peers_workloads,
    const std::atomic<RunStopTick>& run_stop_tick,
    LiveWorkloadShutdownState& shutdown_state, bool run_failed) {
  const auto request_workload_shutdown =
      [&](bool request_run_failed) -> WorkloadShutdownRecords {
    const std::chrono::steady_clock::time_point shutdown_requested_at =
        ObservedRunStop(run_stop_tick)
            .value_or(std::chrono::steady_clock::now());
    return RequestLiveWorkloadShutdown(
        wallet_workloads, block_generation_workloads,
        wait_until_height_workloads, wait_for_peers_workloads,
        request_run_failed, shutdown_requested_at);
  };
  if (shutdown_state.complete) {
    if (shutdown_state.failure) {
      std::rethrow_exception(shutdown_state.failure);
    }
    return;
  }
  const std::chrono::steady_clock::time_point shutdown_deadline =
      std::chrono::steady_clock::now() + kWorkloadServiceShutdownBound;
  std::exception_ptr shutdown_failure;
  const auto remember_shutdown_failure =
      [&](const std::exception_ptr& failure) noexcept {
        if (!shutdown_failure) {
          shutdown_failure = failure;
        }
      };
  try {
    mcp_application.CloseWorkloadService(shutdown_deadline);
  } catch (const std::exception& error) {
    remember_shutdown_failure(std::current_exception());
    BBP_LOG(error) << "workload service admission closure failed; retrying: "
                   << error.what();
    try {
      mcp_application.CloseWorkloadService(shutdown_deadline);
    } catch (...) {
      BBP_LOG(error) << "workload service admission closure failed repeatedly";
      std::terminate();
    }
  } catch (...) {
    remember_shutdown_failure(std::current_exception());
    BBP_LOG(error) << "workload service admission closure failed; retrying";
    try {
      mcp_application.CloseWorkloadService(shutdown_deadline);
    } catch (...) {
      BBP_LOG(error) << "workload service admission closure failed repeatedly";
      std::terminate();
    }
  }
  WorkloadShutdownRecords retained_records;
  try {
    retained_records = request_workload_shutdown(run_failed);
  } catch (const std::exception& error) {
    remember_shutdown_failure(std::current_exception());
    BBP_LOG(error) << "workload lifecycle cancellation failed; retrying: "
                   << error.what();
    try {
      retained_records = request_workload_shutdown(run_failed);
    } catch (...) {
      BBP_LOG(error) << "workload lifecycle cancellation failed repeatedly";
      std::terminate();
    }
  } catch (...) {
    remember_shutdown_failure(std::current_exception());
    BBP_LOG(error) << "workload lifecycle cancellation failed; retrying";
    try {
      retained_records = request_workload_shutdown(run_failed);
    } catch (...) {
      BBP_LOG(error) << "workload lifecycle cancellation failed repeatedly";
      std::terminate();
    }
  }
  try {
    mcp_application.RequestWorkloadServiceCancellation();
  } catch (const std::exception& error) {
    remember_shutdown_failure(std::current_exception());
    BBP_LOG(error) << "workload service cancellation failed; retrying: "
                   << error.what();
    try {
      mcp_application.RequestWorkloadServiceCancellation();
    } catch (...) {
      BBP_LOG(error) << "workload service cancellation failed repeatedly";
      std::terminate();
    }
  } catch (...) {
    remember_shutdown_failure(std::current_exception());
    BBP_LOG(error) << "workload service cancellation failed; retrying";
    try {
      mcp_application.RequestWorkloadServiceCancellation();
    } catch (...) {
      BBP_LOG(error) << "workload service cancellation failed repeatedly";
      std::terminate();
    }
  }
  std::optional<McpLiveWorkloadDrainResult> shutdown_deadline_snapshot;
  bool safe_to_destroy = false;
  try {
    const McpLiveWorkloadDrainResult drain_result =
        mcp_application.WaitForWorkloadServiceDrain();
    safe_to_destroy = drain_result.safe_to_destroy();
    if (!safe_to_destroy) {
      shutdown_deadline_snapshot = drain_result;
      mcp_application.PublishWorkloadServiceShutdownTimeout(
          drain_result, std::chrono::duration_cast<std::chrono::milliseconds>(
                            kWorkloadServiceShutdownBound));
      BBP_LOG(error)
          << "workload service did not drain within the 15000 ms shutdown "
             "bound; active_callbacks="
          << drain_result.active_callback_count
          << "; active_workers=" << drain_result.active_worker_count;
      try {
        const WorkloadServiceShutdownTimeout shutdown_timeout(drain_result);
        WriteEvent(events_path, options.run_id, "sim",
                   SimulationEventKind::kRunFailed,
                   boost::json::serialize(shutdown_timeout.Diagnostic()));
      } catch (const std::exception& error) {
        BBP_LOG(error) << "workload service shutdown timeout evidence "
                          "publication failed: "
                       << error.what();
      } catch (...) {
        BBP_LOG(error) << "workload service shutdown timeout evidence "
                          "publication failed";
      }
    }
  } catch (const std::exception& error) {
    remember_shutdown_failure(std::current_exception());
    BBP_LOG(error) << "bounded workload service drain failed: " << error.what();
  } catch (...) {
    remember_shutdown_failure(std::current_exception());
    BBP_LOG(error) << "bounded workload service drain failed";
  }
  if (!safe_to_destroy) {
    try {
      const McpLiveWorkloadDrainResult quarantine_result =
          mcp_application.WaitForWorkloadServiceQuarantine();
      safe_to_destroy = quarantine_result.safe_to_destroy();
    } catch (const std::exception& error) {
      remember_shutdown_failure(std::current_exception());
      BBP_LOG(error) << "workload service quarantine wait failed: "
                     << error.what();
    } catch (...) {
      remember_shutdown_failure(std::current_exception());
      BBP_LOG(error) << "workload service quarantine wait failed";
    }
  }
  if (!safe_to_destroy && installed_workload_service) {
    try {
      const McpLiveWorkloadDrainResult quarantine_result =
          installed_workload_service->WaitUntilDrained();
      safe_to_destroy = quarantine_result.safe_to_destroy();
    } catch (const std::exception& error) {
      remember_shutdown_failure(std::current_exception());
      BBP_LOG(error) << "direct workload service quarantine wait failed: "
                     << error.what();
    } catch (...) {
      remember_shutdown_failure(std::current_exception());
      BBP_LOG(error) << "direct workload service quarantine wait failed";
    }
  }
  if (!safe_to_destroy) {
    BBP_LOG(error)
        << "workload service quarantine did not prove referenced simulator "
           "state safe to destroy";
    std::terminate();
  }
  shutdown_state.safe_to_destroy = true;
  for (std::size_t index = 0U; index < retained_records.wallet_count; ++index) {
    JoinLiveWalletWorkloadWorker(*retained_records.wallets[index]);
  }
  for (std::size_t index = 0U; index < retained_records.height_wait_count;
       ++index) {
    JoinLiveHeightWaitWorkloadWorker(*retained_records.height_waits[index]);
  }
  for (std::size_t index = 0U; index < retained_records.peer_wait_count;
       ++index) {
    JoinLivePeerWaitWorkloadWorker(*retained_records.peer_waits[index]);
  }
  for (std::size_t index = 0U; index < retained_records.block_generator_count;
       ++index) {
    JoinLiveBlockGenerationWorkloadWorker(
        *retained_records.block_generators[index]);
  }
  for (std::size_t index = 0U; index < retained_records.wallet_count; ++index) {
    const std::shared_ptr<LiveWalletWorkloadRecord>& record =
        retained_records.wallets[index];
    try {
      std::lock_guard<std::mutex> record_lock(record->mutex);
      if (!IsTerminalLiveWalletWorkloadState(record->state)) {
        record->state = run_failed ? LiveWalletWorkloadState::kFailed
                                   : LiveWalletWorkloadState::kCancelled;
        record->terminal_outcome = run_failed ? "failed" : "cancelled";
        if (run_failed && !record->failure) {
          record->failure = "run failed while wallet workload was active";
        }
        record->changed.notify_all();
      }
    } catch (...) {
      remember_shutdown_failure(std::current_exception());
    }
  }
  for (std::size_t index = 0U; index < retained_records.height_wait_count;
       ++index) {
    const std::shared_ptr<LiveWaitUntilHeightWorkloadRecord>& record =
        retained_records.height_waits[index];
    try {
      std::lock_guard<std::mutex> record_lock(record->mutex);
      if (!IsTerminalLiveWorkloadState(record->state)) {
        record->state = run_failed ? LiveWorkloadState::kFailed
                                   : LiveWorkloadState::kCancelled;
        record->terminal_outcome = run_failed ? "failed" : "cancelled";
        if (run_failed && !record->failure) {
          record->failure =
              "run failed while wait-until-height workload was active";
        }
        record->changed.notify_all();
      }
    } catch (...) {
      remember_shutdown_failure(std::current_exception());
    }
  }
  for (std::size_t index = 0U; index < retained_records.peer_wait_count;
       ++index) {
    const std::shared_ptr<LiveWaitForPeersWorkloadRecord>& record =
        retained_records.peer_waits[index];
    try {
      std::lock_guard<std::mutex> record_lock(record->mutex);
      if (!IsTerminalLiveWorkloadState(record->state)) {
        record->state = run_failed ? LiveWorkloadState::kFailed
                                   : LiveWorkloadState::kCancelled;
        record->terminal_outcome = run_failed ? "failed" : "cancelled";
        if (run_failed && !record->failure) {
          record->failure =
              "run failed while wait-for-peers workload was active";
        }
        record->changed.notify_all();
      }
    } catch (...) {
      remember_shutdown_failure(std::current_exception());
    }
  }
  for (std::size_t index = 0U; index < retained_records.block_generator_count;
       ++index) {
    const std::shared_ptr<LiveBlockGenerationWorkloadRecord>& record =
        retained_records.block_generators[index];
    try {
      std::lock_guard<std::mutex> record_lock(record->mutex);
      if (!IsTerminalLiveWorkloadState(record->state)) {
        record->state = run_failed ? LiveWorkloadState::kFailed
                                   : LiveWorkloadState::kCancelled;
        record->terminal_outcome = run_failed ? "failed" : "cancelled";
        if (run_failed && !record->failure) {
          record->failure =
              "run failed while block generation workload was active";
        }
        record->changed.notify_all();
      }
    } catch (...) {
      remember_shutdown_failure(std::current_exception());
    }
  }
  installed_workload_service.reset();
  shutdown_state.complete = true;
  if (shutdown_deadline_snapshot) {
    try {
      shutdown_state.failure = std::make_exception_ptr(
          WorkloadServiceShutdownTimeout(*shutdown_deadline_snapshot));
    } catch (...) {
      shutdown_state.failure = std::current_exception();
    }
  } else {
    shutdown_state.failure = shutdown_failure;
  }
  if (shutdown_state.failure) {
    std::rethrow_exception(shutdown_state.failure);
  }
}

}  // namespace bbp::simulator_app_internal
