#pragma once

#include <chrono>
#include <cstdint>
#include <vector>

#include "bbp/simulator/scenario_workload.h"

namespace bbp {

struct ScheduledScenarioEvent {
  std::chrono::milliseconds at{0};
  std::uint32_t sequence = 0;
  ScenarioWorkload action;
};

std::vector<ScheduledScenarioEvent> OrderScheduledScenarioEvents(
    const std::vector<ScheduledScenarioEvent>& events);

}  // namespace bbp
