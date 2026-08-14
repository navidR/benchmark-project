#pragma once

#include <chrono>
#include <optional>
#include <stop_token>
#include <string>
#include <string_view>

namespace bbp {

class ChainDriver;
class RunProcessState;
struct NodeRuntime;

namespace simulator_app_internal {

bool StartNativeMiningForCurrentProcess(
    const ChainDriver& driver, NodeRuntime& node,
    RunProcessState& run_process_state, std::string_view reward_address,
    std::stop_token stop_token, std::string_view action,
    std::optional<std::chrono::steady_clock::time_point> lock_deadline =
        std::nullopt,
    bool* rpc_attempted = nullptr);
void StopNativeMining(const ChainDriver& driver, NodeRuntime& node,
                      RunProcessState& run_process_state,
                      std::stop_token stop_token = {});
bool StopNativeMiningBeforeDeadline(
    const ChainDriver& driver, const NodeRuntime& node,
    std::chrono::steady_clock::time_point deadline, std::string* error);

}  // namespace simulator_app_internal
}  // namespace bbp
