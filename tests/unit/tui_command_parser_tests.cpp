#include <boost/test/unit_test.hpp>

#include "benchmark_sim/tui_command_parser.h"

BOOST_AUTO_TEST_CASE(tui_command_parser_builds_typed_block_policy) {
  const bsim::ParsedTuiCommand command =
      bsim::TuiCommandParser::Parse("block-production 0.25 2000", 71U);
  BOOST_CHECK(command.kind ==
              bsim::SimulationCommandKind::kSetBlockProductionPolicy);
  BOOST_REQUIRE(command.block_production_policy);
  BOOST_TEST(command.block_production_policy->probability() == 0.25);
  BOOST_TEST(command.block_production_policy->period().count() == 2000);
  BOOST_TEST(command.block_production_policy->seed() == 71U);
}

BOOST_AUTO_TEST_CASE(tui_command_parser_builds_driver_commands) {
  BOOST_CHECK(bsim::TuiCommandParser::Parse("reconnect", 0U).kind ==
              bsim::SimulationCommandKind::kReconnectNode);
  const bsim::ParsedTuiCommand difficulty =
      bsim::TuiCommandParser::Parse("mining-difficulty 3.5", 0U);
  BOOST_REQUIRE(difficulty.mining_difficulty);
  BOOST_TEST(difficulty.mining_difficulty->value() == 3.5);
}

BOOST_AUTO_TEST_CASE(tui_command_parser_completes_unique_command_prefix) {
  BOOST_TEST(bsim::TuiCommandParser::Complete("reco") == "reconnect ");
  BOOST_TEST(bsim::TuiCommandParser::Complete("log-") == "log-");
  BOOST_CHECK_THROW(bsim::TuiCommandParser::Parse("unknown", 0U),
                    std::runtime_error);
  BOOST_CHECK_THROW(
      bsim::TuiCommandParser::Parse("block-production nope 1000", 0U),
      std::runtime_error);
}
