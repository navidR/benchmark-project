#pragma once

#include <chrono>
#include <stop_token>

namespace bbp {

struct NodeRuntime;

namespace simulator_app_internal {

void ThrowIfStopRequested(std::stop_token stop_token);
std::chrono::steady_clock::time_point SteadyDeadline(
    std::chrono::steady_clock::time_point epoch,
    std::chrono::milliseconds delay);
void WaitForDuration(std::chrono::milliseconds duration,
                     std::stop_token stop_token);
void WaitUntil(std::chrono::steady_clock::time_point deadline,
               std::stop_token stop_token);
bool WaitForNodeProcessExitUntil(NodeRuntime& node,
                                 std::chrono::steady_clock::time_point deadline,
                                 std::stop_token stop_token = {});

}  // namespace simulator_app_internal
}  // namespace bbp
