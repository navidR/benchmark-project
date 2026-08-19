#pragma once

#include <boost/json/object.hpp>
#include <functional>
#include <memory>
#include <optional>
#include <stop_token>
#include <string>
#include <string_view>

#include "bbp/mcp_registry.h"
#include "bbp/simulator/block_generation_workload.h"

namespace bbp::simulator_app_internal {

struct LiveBlockGenerationWorkloadRecord;

using LiveBlockGenerationOperation = std::function<boost::json::object(
    McpOperationKind, const boost::json::object&, std::stop_token)>;
using LiveBlockGenerationRecordFinder =
    std::function<std::shared_ptr<LiveBlockGenerationWorkloadRecord>(
        std::string_view)>;
using LiveBlockGenerationStarter =
    std::function<std::shared_ptr<LiveBlockGenerationWorkloadRecord>(
        const boost::json::object&, std::optional<std::string>,
        std::stop_token)>;
using LiveBlockGenerationReconfigurationParser =
    std::function<BlockGenerationWorkload(const boost::json::object&)>;
using LiveBlockGenerationStatePublisher =
    std::function<void(const LiveBlockGenerationWorkloadRecord&)>;

LiveBlockGenerationOperation MakeLiveBlockGenerationOperation(
    LiveBlockGenerationRecordFinder find_record,
    LiveBlockGenerationStarter start_workload,
    LiveBlockGenerationReconfigurationParser parse_reconfiguration,
    LiveBlockGenerationStatePublisher publish_state);

}  // namespace bbp::simulator_app_internal
