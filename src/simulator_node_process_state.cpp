#include "simulator_node_process_state.h"

#include <stdexcept>
#include <string>

#include "bbp/simulator/node_runtime.h"

namespace bbp::simulator_app_internal {

RunProcessState::Guard LockNodeProcessState(const NodeRuntime& node) {
  if (node.run_process_state == nullptr) {
    throw std::logic_error("node has no run process synchronization state: " +
                           node.config.id);
  }
  return node.run_process_state->Lock();
}

void RequireNodeRunning(const NodeRuntime& node, const RunProcessState::Guard&,
                        std::string_view action) {
  if (!node.AllowsChainMetrics() || !node.process.running()) {
    throw std::runtime_error(
        std::string(action) + " requires a Running node: " + node.config.id +
        " (state=" + std::string(NodeRuntimeLifecycleName(node.Lifecycle())) +
        ")");
  }
}

void RequireNodeRunning(const NodeRuntime& node, std::string_view action) {
  auto process_guard = LockNodeProcessState(node);
  RequireNodeRunning(node, process_guard, action);
}

bool NodeProcessRunning(const NodeRuntime& node) {
  auto process_guard = LockNodeProcessState(node);
  return node.process.running();
}

bool RequestNodeTerminate(NodeRuntime& node) {
  auto process_guard = LockNodeProcessState(node);
  return node.process.RequestTerminate();
}

bool RequestNodeKill(NodeRuntime& node) {
  auto process_guard = LockNodeProcessState(node);
  return node.process.RequestKill();
}

NodeProcessGeneration RunningNodeProcessGeneration(
    NodeRuntime& node, const RunProcessState::Guard& process_guard,
    std::string_view action) {
  RequireNodeRunning(node, process_guard, action);
  return {.pid = node.process.pid(), .restart_count = node.RestartCount()};
}

bool IsCurrentRunningNodeProcess(NodeRuntime& node,
                                 const RunProcessState::Guard&,
                                 const NodeProcessGeneration& generation) {
  return node.Lifecycle() == NodeRuntimeLifecycle::kRunning &&
         node.process.pid() == generation.pid &&
         node.RestartCount() == generation.restart_count &&
         node.process.running();
}

}  // namespace bbp::simulator_app_internal
