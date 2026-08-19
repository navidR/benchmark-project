#pragma once

#include <boost/json/object.hpp>
#include <functional>
#include <memory>
#include <optional>
#include <stop_token>
#include <string>

#include "bbp/mcp_registry.h"
#include "bbp/runtime_wallet_registry.h"
#include "bbp/simulator/wallet_transactions_workload.h"

namespace bbp::simulator_app_internal {

struct LiveWalletWorkloadRecord;
struct LiveWalletWorkloadRegistry;

using LiveWalletWorkloadOperation = std::function<boost::json::object(
    McpOperationKind, const boost::json::object&, std::stop_token)>;
using LiveWalletWorkloadStarter =
    std::function<std::shared_ptr<LiveWalletWorkloadRecord>(
        const boost::json::object&, std::optional<std::string>,
        std::stop_token)>;
using LiveWalletWorkloadReconfigurationParser =
    std::function<WalletTransactionsWorkload(const boost::json::object&)>;
using LiveWalletWorkloadSnapshotValidator =
    std::function<RuntimeWalletSnapshot(const WalletTransactionsWorkload&)>;

LiveWalletWorkloadOperation MakeLiveWalletWorkloadOperation(
    std::shared_ptr<LiveWalletWorkloadRegistry> wallet_workloads,
    LiveWalletWorkloadStarter start_workload,
    LiveWalletWorkloadReconfigurationParser parse_reconfiguration,
    LiveWalletWorkloadSnapshotValidator snapshot_validator);

}  // namespace bbp::simulator_app_internal
