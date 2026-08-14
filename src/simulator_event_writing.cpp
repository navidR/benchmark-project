#include "simulator_event_writing.h"

#include <boost/json/object.hpp>
#include <boost/json/serialize.hpp>

#include "bbp/simulator/node_runtime.h"
#include "bbp/util.h"

namespace bbp::simulator_app_internal {

void WriteEvent(const std::filesystem::path& events_path,
                const std::string& run_id, const std::string& node_id,
                SimulationEventKind event_kind, std::string_view detail) {
  boost::json::object object;
  object["timestamp"] = NowIso8601();
  object["run_id"] = run_id;
  object["node_id"] = node_id;
  object["event"] = SimulationEventKindName(event_kind);
  object["detail"] = detail;
  AppendLine(events_path, boost::json::serialize(object));
}

void TransitionNodeState(const std::filesystem::path& events_path,
                         const std::string& run_id, NodeRuntime& node,
                         NodeRuntimeLifecycle state) {
  if (node.run_process_state != nullptr) {
    auto process_guard = node.run_process_state->Lock();
    node.SetLifecycle(state);
  } else {
    node.SetLifecycle(state);
  }
  WriteEvent(events_path, run_id, node.config.id, SimulationEventKind::kState,
             NodeRuntimeLifecycleName(state));
}

void WriteNodeStateEvent(const std::filesystem::path& events_path,
                         const std::string& run_id, const NodeRuntime& node,
                         NodeRuntimeLifecycle state) {
  WriteEvent(events_path, run_id, node.config.id, SimulationEventKind::kState,
             NodeRuntimeLifecycleName(state));
}

}  // namespace bbp::simulator_app_internal
