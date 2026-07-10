#include <boost/test/unit_test.hpp>
#include <chrono>
#include <limits>

#include "bbp/block_production_policy.h"
#include "bbp/mining_difficulty.h"

using namespace std::chrono_literals;

BOOST_AUTO_TEST_CASE(block_production_policy_validates_inputs) {
  const bbp::BlockProductionPolicy policy(1s, 0.5, 17U);
  BOOST_TEST(policy.period() == 1s);
  BOOST_TEST(policy.probability() == 0.5);
  BOOST_TEST(policy.seed() == 17U);

  BOOST_CHECK_THROW(bbp::BlockProductionPolicy(0ms, 0.5, 0U),
                    std::runtime_error);
  BOOST_CHECK_THROW(bbp::BlockProductionPolicy(1s, -0.1, 0U),
                    std::runtime_error);
  BOOST_CHECK_THROW(bbp::BlockProductionPolicy(1s, 1.1, 0U),
                    std::runtime_error);
  BOOST_CHECK_THROW(bbp::BlockProductionPolicy(
                        1s, std::numeric_limits<double>::quiet_NaN(), 0U),
                    std::runtime_error);
}

BOOST_AUTO_TEST_CASE(mining_difficulty_validates_inputs) {
  const bbp::MiningDifficulty difficulty(2.5);
  BOOST_TEST(difficulty.value() == 2.5);
  BOOST_CHECK_THROW(bbp::MiningDifficulty(0.0), std::runtime_error);
  BOOST_CHECK_THROW(bbp::MiningDifficulty(-1.0), std::runtime_error);
  BOOST_CHECK_THROW(
      bbp::MiningDifficulty(std::numeric_limits<double>::infinity()),
      std::runtime_error);
}
