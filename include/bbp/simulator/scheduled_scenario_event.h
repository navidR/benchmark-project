#pragma once

#include <chrono>
#include <cstdint>
#include <variant>
#include <vector>

#include "bbp/simulation_command.h"
#include "bbp/simulator/scenario_workload.h"

namespace bbp {

using ScheduledScenarioAction =
    std::variant<ScenarioWorkload, SimulationCommand>;

struct ScheduledScenarioEvent {
  ScheduledScenarioEvent(std::chrono::milliseconds scheduled_at,
                         std::uint32_t source_sequence,
                         ScenarioWorkload workload);
  ScheduledScenarioEvent(std::chrono::milliseconds scheduled_at,
                         std::uint32_t source_sequence,
                         SimulationCommand command);

  std::chrono::milliseconds at;
  std::uint32_t sequence;
  ScheduledScenarioAction action;
};

std::vector<ScheduledScenarioEvent> OrderScheduledScenarioEvents(
    const std::vector<ScheduledScenarioEvent>& events);

}  // namespace bbp
