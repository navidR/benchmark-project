#pragma once

#include <atomic>
#include <exception>
#include <filesystem>
#include <memory>

#include "simulator_stop_coordination.h"

namespace bbp {

class McpLiveApplication;
struct McpLiveWorkloadService;
struct Options;

namespace simulator_app_internal {

struct LiveWalletWorkloadRegistry;
struct LiveBlockGenerationWorkloadRegistry;
struct LiveWaitUntilHeightWorkloadRegistry;
struct LiveWaitForPeersWorkloadRegistry;

struct LiveWorkloadShutdownState {
  bool complete = false;
  bool safe_to_destroy = false;
  std::exception_ptr failure;
};

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
    LiveWorkloadShutdownState& shutdown_state, bool run_failed);

}  // namespace simulator_app_internal
}  // namespace bbp
