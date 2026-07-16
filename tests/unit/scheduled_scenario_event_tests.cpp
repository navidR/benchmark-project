#include <boost/test/unit_test.hpp>
#include <chrono>
#include <vector>

#include "bbp/simulator/scheduled_scenario_event.h"

BOOST_AUTO_TEST_CASE(scheduled_scenario_events_order_by_time_then_sequence) {
  using namespace std::chrono_literals;
  std::vector<bbp::ScheduledScenarioEvent> events{
      {.at = 2s, .sequence = 4, .action = {}},
      {.at = 1s, .sequence = 3, .action = {}},
      {.at = 1s, .sequence = 1, .action = {}},
      {.at = 3s, .sequence = 2, .action = {}},
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
