#include "simulator_cancellable_waiting.h"

#include <condition_variable>
#include <mutex>
#include <stdexcept>

#include "bbp/simulation_cancelled.h"
#include "simulator_node_process_state.h"

namespace bbp::simulator_app_internal {

void ThrowIfStopRequested(std::stop_token stop_token) {
  if (stop_token.stop_requested()) {
    throw SimulationCancelled();
  }
}

std::chrono::steady_clock::time_point SteadyDeadline(
    std::chrono::steady_clock::time_point epoch,
    std::chrono::milliseconds delay) {
  const auto remaining = std::chrono::steady_clock::time_point::max() - epoch;
  const auto maximum_delay =
      std::chrono::duration_cast<std::chrono::milliseconds>(remaining);
  if (delay > maximum_delay) {
    throw std::runtime_error(
        "scheduled event time exceeds monotonic clock range");
  }
  return epoch +
         std::chrono::duration_cast<std::chrono::steady_clock::duration>(delay);
}

void WaitForDuration(std::chrono::milliseconds duration,
                     std::stop_token stop_token) {
  ThrowIfStopRequested(stop_token);
  std::condition_variable_any condition;
  std::mutex mutex;
  std::unique_lock<std::mutex> lock(mutex);
  condition.wait_for(lock, stop_token, duration, [] { return false; });
  ThrowIfStopRequested(stop_token);
}

void WaitUntil(std::chrono::steady_clock::time_point deadline,
               std::stop_token stop_token) {
  ThrowIfStopRequested(stop_token);
  if (std::chrono::steady_clock::now() >= deadline) {
    return;
  }
  std::condition_variable_any condition;
  std::mutex mutex;
  std::unique_lock<std::mutex> lock(mutex);
  condition.wait_until(lock, stop_token, deadline, [] { return false; });
  ThrowIfStopRequested(stop_token);
}

bool WaitForNodeProcessExitUntil(NodeRuntime& node,
                                 std::chrono::steady_clock::time_point deadline,
                                 std::stop_token stop_token) {
  while (std::chrono::steady_clock::now() < deadline) {
    if (!NodeProcessRunning(node)) {
      return true;
    }
    WaitForDuration(std::chrono::milliseconds(20), stop_token);
  }
  return !NodeProcessRunning(node);
}

}  // namespace bbp::simulator_app_internal
