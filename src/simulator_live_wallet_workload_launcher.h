#pragma once

#include <filesystem>
#include <functional>
#include <memory>
#include <mutex>
#include <optional>
#include <string>

#include "bbp/simulator/wallet_transactions_workload.h"

namespace bbp {

class ChainDriver;
class RuntimeNodeInventory;
class RuntimeWalletRegistry;
struct McpLiveWorkloadService;
struct Options;

namespace simulator_app_internal {

class TransactionObservationTracker;
struct LiveBlockGenerationWorkloadRegistry;
struct LiveWaitForPeersWorkloadRegistry;
struct LiveWaitUntilHeightWorkloadRegistry;
struct LiveWalletWorkloadRecord;
struct LiveWalletWorkloadRegistry;

using LiveWalletWorkloadLauncher =
    std::function<std::shared_ptr<LiveWalletWorkloadRecord>(
        WalletTransactionsWorkload, std::optional<std::string>)>;

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
    std::shared_ptr<LiveWaitForPeersWorkloadRegistry> wait_for_peers_workloads);

void JoinLiveWalletWorkloadWorker(LiveWalletWorkloadRecord& record);

}  // namespace simulator_app_internal
}  // namespace bbp
