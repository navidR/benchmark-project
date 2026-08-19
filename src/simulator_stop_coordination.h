#pragma once

#include <atomic>
#include <chrono>
#include <limits>
#include <optional>

namespace bbp::simulator_app_internal {

using RunStopTick = std::chrono::steady_clock::duration::rep;

inline constexpr RunStopTick kRunStopNotObserved =
    std::numeric_limits<RunStopTick>::max();

void RecordRunStop(std::atomic<RunStopTick>& run_stop_tick,
                   std::chrono::steady_clock::time_point observed_at);

std::optional<std::chrono::steady_clock::time_point> ObservedRunStop(
    const std::atomic<RunStopTick>& run_stop_tick);

}  // namespace bbp::simulator_app_internal
