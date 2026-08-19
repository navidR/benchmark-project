#pragma once

#include <boost/json/object.hpp>
#include <functional>
#include <memory>
#include <optional>
#include <stop_token>
#include <string>
#include <string_view>

#include "bbp/mcp_registry.h"
#include "bbp/simulator/wait_until_height_workload.h"

namespace bbp::simulator_app_internal {

struct LiveWaitUntilHeightWorkloadRecord;

using LiveWaitUntilHeightOperation = std::function<boost::json::object(
    McpOperationKind, const boost::json::object&, std::stop_token)>;
using LiveWaitUntilHeightRecordFinder =
    std::function<std::shared_ptr<LiveWaitUntilHeightWorkloadRecord>(
        std::string_view)>;
using LiveWaitUntilHeightStarter =
    std::function<std::shared_ptr<LiveWaitUntilHeightWorkloadRecord>(
        const boost::json::object&, std::optional<std::string>,
        std::stop_token)>;
using LiveWaitUntilHeightReconfigurationParser =
    std::function<WaitUntilHeightWorkload(const boost::json::object&,
                                          std::stop_token)>;
using LiveWaitUntilHeightStatePublisher =
    std::function<void(const LiveWaitUntilHeightWorkloadRecord&)>;

LiveWaitUntilHeightOperation MakeLiveWaitUntilHeightOperation(
    LiveWaitUntilHeightRecordFinder find_record,
    LiveWaitUntilHeightStarter start_workload,
    LiveWaitUntilHeightReconfigurationParser parse_reconfiguration,
    LiveWaitUntilHeightStatePublisher publish_state);

}  // namespace bbp::simulator_app_internal
