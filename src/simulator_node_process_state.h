#pragma once

#include <sys/types.h>

#include <cstdint>
#include <string_view>

#include "bbp/run_process_state.h"

namespace bbp {

struct NodeRuntime;

namespace simulator_app_internal {

struct NodeProcessGeneration {
  pid_t pid = -1;
  std::uint64_t restart_count = 0U;
};

RunProcessState::Guard LockNodeProcessState(const NodeRuntime& node);
void RequireNodeRunning(const NodeRuntime& node,
                        const RunProcessState::Guard& process_guard,
                        std::string_view action);
void RequireNodeRunning(const NodeRuntime& node, std::string_view action);
bool NodeProcessRunning(const NodeRuntime& node);
bool RequestNodeTerminate(NodeRuntime& node);
bool RequestNodeKill(NodeRuntime& node);
NodeProcessGeneration RunningNodeProcessGeneration(
    NodeRuntime& node, const RunProcessState::Guard& process_guard,
    std::string_view action);
bool IsCurrentRunningNodeProcess(NodeRuntime& node,
                                 const RunProcessState::Guard& process_guard,
                                 const NodeProcessGeneration& generation);

}  // namespace simulator_app_internal
}  // namespace bbp
