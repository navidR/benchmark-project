#pragma once

#include <filesystem>
#include <functional>
#include <memory>
#include <mutex>
#include <optional>
#include <stop_token>
#include <string>

#include "bbp/simulator/block_generation_workload.h"

namespace bbp {

class ChainDriver;
class RuntimeNodeInventory;
struct McpLiveWorkloadService;
struct Options;

namespace simulator_app_internal {

struct LiveBlockGenerationWorkloadRecord;
struct LiveBlockGenerationWorkloadRegistry;
struct LiveWaitForPeersWorkloadRegistry;
struct LiveWaitUntilHeightWorkloadRegistry;
struct LiveWalletWorkloadRegistry;

using LiveBlockGenerationWorkloadLauncher =
    std::function<std::shared_ptr<LiveBlockGenerationWorkloadRecord>(
        BlockGenerationWorkload, std::optional<std::string>)>;

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
    std::shared_ptr<LiveWaitForPeersWorkloadRegistry> wait_for_peers_workloads);

void JoinLiveBlockGenerationWorkloadWorker(
    LiveBlockGenerationWorkloadRecord& record);

}  // namespace simulator_app_internal
}  // namespace bbp
