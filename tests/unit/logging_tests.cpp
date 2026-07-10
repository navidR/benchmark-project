#include <boost/log/core.hpp>
#include <boost/test/unit_test.hpp>
#include <iostream>
#include <sstream>
#include <string>

#include "bbp/logging.h"

BOOST_AUTO_TEST_CASE(console_logging_can_be_suspended_for_ncurses) {
  bbp::InitLogging();
  std::ostringstream output;
  std::streambuf* previous_buffer = std::clog.rdbuf(output.rdbuf());

  bbp::SetConsoleLoggingEnabled(false);
  BBP_LOG(info) << "hidden-while-ncurses-owns-terminal";
  boost::log::core::get()->flush();
  const std::string suspended_output = output.str();

  bbp::SetConsoleLoggingEnabled(true);
  BBP_LOG(info) << "visible-after-ncurses-releases-terminal";
  boost::log::core::get()->flush();
  const std::string restored_output = output.str();
  std::clog.rdbuf(previous_buffer);

  BOOST_TEST(suspended_output.find("hidden-while-ncurses-owns-terminal") ==
             std::string::npos);
  BOOST_TEST(restored_output.find("visible-after-ncurses-releases-terminal") !=
             std::string::npos);
}
