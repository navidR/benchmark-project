#include "bbp/simulator/scheduled_scenario_event.h"

#include <algorithm>
#include <utility>

namespace bbp {

ScheduledScenarioEvent::ScheduledScenarioEvent(
    std::chrono::milliseconds scheduled_at, std::uint32_t source_sequence,
    ScenarioWorkload workload)
    : at(scheduled_at),
      sequence(source_sequence),
      action(std::move(workload)) {}

ScheduledScenarioEvent::ScheduledScenarioEvent(
    std::chrono::milliseconds scheduled_at, std::uint32_t source_sequence,
    SimulationCommand command)
    : at(scheduled_at), sequence(source_sequence), action(std::move(command)) {}

std::vector<ScheduledScenarioEvent> OrderScheduledScenarioEvents(
    const std::vector<ScheduledScenarioEvent>& events) {
  std::vector<ScheduledScenarioEvent> ordered = events;
  std::stable_sort(
      ordered.begin(), ordered.end(),
      [](const ScheduledScenarioEvent& lhs, const ScheduledScenarioEvent& rhs) {
        if (lhs.at != rhs.at) {
          return lhs.at < rhs.at;
        }
        return lhs.sequence < rhs.sequence;
      });
  return ordered;
}

}  // namespace bbp
