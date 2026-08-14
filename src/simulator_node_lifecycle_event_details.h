#pragma once

#include <sys/types.h>

#include <chrono>
#include <cstdint>
#include <string>
#include <string_view>

#include "bbp/run_process_state.h"

namespace bbp {

class ChildProcess;
class SimulationTimeScale;
struct NodeRuntime;

namespace simulator_app_internal {

std::uint64_t ElapsedMilliseconds(
    std::chrono::steady_clock::time_point epoch,
    std::chrono::steady_clock::time_point timestamp);
std::string ProcessExitDetail(const ChildProcess& process,
                              const RunProcessState::Guard& guard);
std::string RestartDetail(pid_t pid, std::uint64_t restart_count,
                          std::string_view reason);
std::string RestartRequestedDetail(NodeRuntime& node, std::string_view reason);
std::string RestartPolicyAppliedDetail(
    const NodeRuntime& node, int wait_status, bool restart,
    std::string_view suppression_reason = {});
std::string NodeLifecycleDeadlineDetail(
    const NodeRuntime& node, const SimulationTimeScale& time_scale,
    std::chrono::steady_clock::time_point simulation_epoch,
    std::chrono::milliseconds simulation_offset, std::string_view reason);
std::string ProcessStartedDetail(
    const NodeRuntime& node, std::string_view reason,
    std::chrono::steady_clock::time_point simulation_epoch,
    const SimulationTimeScale& time_scale, const RunProcessState::Guard& guard);

}  // namespace simulator_app_internal
}  // namespace bbp
