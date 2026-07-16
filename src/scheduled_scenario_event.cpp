#include "bbp/simulator/scheduled_scenario_event.h"

#include <algorithm>

namespace bbp {

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
