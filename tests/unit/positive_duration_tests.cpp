#include <boost/test/unit_test.hpp>
#include <chrono>
#include <stdexcept>

#include "bbp/positive_duration.h"

BOOST_AUTO_TEST_CASE(positive_duration_parses_explicit_units) {
  BOOST_TEST(bbp::PositiveDuration::Parse("250ms").value().count() == 250);
  BOOST_TEST(bbp::PositiveDuration::Parse("2s").value().count() == 2000);
  BOOST_TEST(bbp::PositiveDuration::Parse("3m").value().count() == 180000);
  BOOST_TEST(bbp::PositiveDuration::Parse("1h").value().count() == 3600000);
}

BOOST_AUTO_TEST_CASE(positive_duration_rejects_invalid_values) {
  BOOST_CHECK_THROW(bbp::PositiveDuration::Parse("0ms"), std::runtime_error);
  BOOST_CHECK_THROW(bbp::PositiveDuration::Parse("10"), std::runtime_error);
  BOOST_CHECK_THROW(bbp::PositiveDuration::Parse("1day"), std::runtime_error);
}
