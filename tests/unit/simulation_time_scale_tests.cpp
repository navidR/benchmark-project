#include <boost/test/unit_test.hpp>
#include <chrono>
#include <limits>
#include <stdexcept>

#include "bbp/simulation_time_scale.h"

BOOST_AUTO_TEST_CASE(simulation_time_scale_converts_to_wall_time) {
  const bbp::SimulationTimeScale realtime =
      bbp::SimulationTimeScale::FromDouble(1.0);
  BOOST_TEST(realtime.millionths() == 1'000'000U);
  BOOST_TEST(realtime.value() == 1.0);
  BOOST_TEST(realtime.WallDuration(std::chrono::milliseconds(250)).count() ==
             250);

  const bbp::SimulationTimeScale faster =
      bbp::SimulationTimeScale::FromDouble(2.0);
  BOOST_TEST(faster.WallDuration(std::chrono::milliseconds(250)).count() ==
             125);
  BOOST_TEST(faster.WallDuration(std::chrono::milliseconds(1)).count() == 1);

  const bbp::SimulationTimeScale slower =
      bbp::SimulationTimeScale::FromDouble(0.5);
  BOOST_TEST(slower.WallDuration(std::chrono::milliseconds(250)).count() ==
             500);
}

BOOST_AUTO_TEST_CASE(simulation_time_scale_rejects_invalid_values) {
  BOOST_CHECK_THROW(bbp::SimulationTimeScale::FromDouble(0.0),
                    std::runtime_error);
  BOOST_CHECK_THROW(bbp::SimulationTimeScale::FromDouble(
                        std::numeric_limits<double>::infinity()),
                    std::runtime_error);
  BOOST_CHECK_THROW(bbp::SimulationTimeScale::FromDouble(0.1234567),
                    std::runtime_error);
  BOOST_CHECK_THROW(bbp::SimulationTimeScale::FromDouble(1'000'001.0),
                    std::runtime_error);
}

BOOST_AUTO_TEST_CASE(simulation_time_scale_checks_scaled_overflow) {
  const bbp::SimulationTimeScale minimum =
      bbp::SimulationTimeScale::FromDouble(0.000001);
  const auto maximum = std::chrono::milliseconds::max();
  BOOST_CHECK_THROW(static_cast<void>(minimum.WallDuration(maximum)),
                    std::runtime_error);
}
