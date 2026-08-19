#pragma once

#include <boost/json/object.hpp>
#include <functional>
#include <memory>
#include <optional>
#include <stop_token>
#include <string>
#include <string_view>

#include "bbp/mcp_registry.h"
#include "bbp/simulator/wait_for_peers_workload.h"

namespace bbp::simulator_app_internal {

struct LiveWaitForPeersWorkloadRecord;

using LiveWaitForPeersOperation = std::function<boost::json::object(
    McpOperationKind, const boost::json::object&, std::stop_token)>;
using LiveWaitForPeersRecordFinder =
    std::function<std::shared_ptr<LiveWaitForPeersWorkloadRecord>(
        std::string_view)>;
using LiveWaitForPeersStarter =
    std::function<std::shared_ptr<LiveWaitForPeersWorkloadRecord>(
        const boost::json::object&, std::optional<std::string>,
        std::stop_token)>;
using LiveWaitForPeersReconfigurationParser =
    std::function<WaitForPeersWorkload(const boost::json::object&,
                                       std::stop_token)>;
using LiveWaitForPeersStatePublisher =
    std::function<void(const LiveWaitForPeersWorkloadRecord&)>;

LiveWaitForPeersOperation MakeLiveWaitForPeersOperation(
    LiveWaitForPeersRecordFinder find_record,
    LiveWaitForPeersStarter start_workload,
    LiveWaitForPeersReconfigurationParser parse_reconfiguration,
    LiveWaitForPeersStatePublisher publish_state);

}  // namespace bbp::simulator_app_internal
