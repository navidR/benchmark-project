#include <boost/test/unit_test.hpp>
#include <chrono>
#include <variant>
#include <vector>

#include "bbp/simulator/scheduled_scenario_event.h"

BOOST_AUTO_TEST_CASE(scheduled_scenario_events_order_by_time_then_sequence) {
  using namespace std::chrono_literals;
  const bbp::ScenarioWorkload workload;
  std::vector<bbp::ScheduledScenarioEvent> events{
      {2s, 4, workload},
      {1s, 3, workload},
      {1s, 1, workload},
      {3s, 2, workload},
  };

  const std::vector<bbp::ScheduledScenarioEvent> ordered =
      bbp::OrderScheduledScenarioEvents(events);

  BOOST_REQUIRE_EQUAL(ordered.size(), 4U);
  BOOST_TEST(ordered[0].sequence == 1U);
  BOOST_TEST(ordered[1].sequence == 3U);
  BOOST_TEST(ordered[2].sequence == 4U);
  BOOST_TEST(ordered[3].sequence == 2U);
  BOOST_TEST(events.front().sequence == 4U);
}

BOOST_AUTO_TEST_CASE(scheduled_scenario_event_has_exactly_one_typed_action) {
  using namespace std::chrono_literals;
  bbp::ScenarioWorkload workload;
  workload.kind = bbp::WorkloadKind::kCheckpoint;
  bbp::SimulationCommand command;
  command.kind = bbp::SimulationCommandKind::kRestartNode;
  command.node_id = "firo-1";
  command.confirmed = true;
  command.scheduled_event_sequence = 2U;

  const bbp::ScheduledScenarioEvent workload_event(1s, 1U, workload);
  const bbp::ScheduledScenarioEvent command_event(2s, 2U, command);

  BOOST_TEST(
      std::holds_alternative<bbp::ScenarioWorkload>(workload_event.action));
  BOOST_TEST(
      !std::holds_alternative<bbp::SimulationCommand>(workload_event.action));
  BOOST_TEST(
      std::holds_alternative<bbp::SimulationCommand>(command_event.action));
  BOOST_TEST(
      !std::holds_alternative<bbp::ScenarioWorkload>(command_event.action));
}
