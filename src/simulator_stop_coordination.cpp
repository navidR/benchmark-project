#include "simulator_stop_coordination.h"

#include <atomic>
#include <chrono>
#include <optional>

namespace bbp::simulator_app_internal {

void RecordRunStop(std::atomic<RunStopTick>& run_stop_tick,
                   std::chrono::steady_clock::time_point observed_at) {
  const RunStopTick observed_tick = observed_at.time_since_epoch().count();
  RunStopTick current = run_stop_tick.load(std::memory_order_relaxed);
  while (observed_tick < current &&
         !run_stop_tick.compare_exchange_weak(current, observed_tick,
                                              std::memory_order_release,
                                              std::memory_order_relaxed)) {
  }
}

std::optional<std::chrono::steady_clock::time_point> ObservedRunStop(
    const std::atomic<RunStopTick>& run_stop_tick) {
  const RunStopTick observed_tick =
      run_stop_tick.load(std::memory_order_acquire);
  if (observed_tick == kRunStopNotObserved) {
    return std::nullopt;
  }
  return std::chrono::steady_clock::time_point{
      std::chrono::steady_clock::duration{observed_tick}};
}

}  // namespace bbp::simulator_app_internal
