#pragma once

#include <filesystem>
#include <string>
#include <string_view>

#include "bbp/simulation_event_kind.h"
#include "bbp/simulator/node_runtime_lifecycle.h"

namespace bbp {

struct NodeRuntime;

namespace simulator_app_internal {

void WriteEvent(const std::filesystem::path& events_path,
                const std::string& run_id, const std::string& node_id,
                SimulationEventKind event_kind, std::string_view detail = "");
void TransitionNodeState(const std::filesystem::path& events_path,
                         const std::string& run_id, NodeRuntime& node,
                         NodeRuntimeLifecycle state);
void WriteNodeStateEvent(const std::filesystem::path& events_path,
                         const std::string& run_id, const NodeRuntime& node,
                         NodeRuntimeLifecycle state);

}  // namespace simulator_app_internal
}  // namespace bbp
