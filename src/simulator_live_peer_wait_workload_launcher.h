#pragma once

#include <atomic>
#include <filesystem>
#include <functional>
#include <memory>
#include <mutex>
#include <optional>
#include <stop_token>
#include <string>

#include "bbp/simulator/wait_for_peers_workload.h"
#include "simulator_stop_coordination.h"

namespace bbp {

class ChainDriver;
class RuntimeNodeInventory;
struct McpLiveWorkloadService;
struct Options;

namespace simulator_app_internal {

struct LiveBlockGenerationWorkloadRegistry;
struct LiveWaitForPeersWorkloadRegistry;
struct LiveWaitForPeersWorkloadRecord;
struct LiveWaitUntilHeightWorkloadRegistry;
struct LiveWalletWorkloadRegistry;

using LivePeerWaitWorkloadLauncher =
    std::function<std::shared_ptr<LiveWaitForPeersWorkloadRecord>(
        WaitForPeersWorkload, std::optional<std::string>)>;

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
    std::shared_ptr<LiveWaitForPeersWorkloadRegistry> wait_for_peers_workloads);

void JoinLivePeerWaitWorkloadWorker(LiveWaitForPeersWorkloadRecord& record);

}  // namespace simulator_app_internal
}  // namespace bbp
