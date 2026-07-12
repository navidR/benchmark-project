#include <boost/test/unit_test.hpp>
#include <stdexcept>

#include "bbp/log_level.h"

BOOST_AUTO_TEST_CASE(log_level_parses_canonical_names) {
  BOOST_TEST(static_cast<int>(bbp::ParseLogLevel("trace")) ==
             static_cast<int>(bbp::LogLevel::kTrace));
  BOOST_TEST(static_cast<int>(bbp::ParseLogLevel("warning")) ==
             static_cast<int>(bbp::LogLevel::kWarning));
  BOOST_TEST(static_cast<int>(bbp::ParseLogLevel("fatal")) ==
             static_cast<int>(bbp::LogLevel::kFatal));
}

BOOST_AUTO_TEST_CASE(log_level_rejects_unknown_name) {
  BOOST_CHECK_THROW(bbp::ParseLogLevel("verbose"), std::runtime_error);
}
