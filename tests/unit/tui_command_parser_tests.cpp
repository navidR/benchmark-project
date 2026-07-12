#include <boost/test/unit_test.hpp>

#include "bbp/tui_command_parser.h"

BOOST_AUTO_TEST_CASE(tui_command_parser_builds_typed_block_policy) {
  const bbp::ParsedTuiCommand command =
      bbp::TuiCommandParser::Parse("block-production 0.25 2000", 71U);
  BOOST_CHECK(command.kind ==
              bbp::SimulationCommandKind::kSetBlockProductionPolicy);
  BOOST_REQUIRE(command.block_production_policy);
  BOOST_TEST(command.block_production_policy->probability() == 0.25);
  BOOST_TEST(command.block_production_policy->period().count() == 2000);
  BOOST_TEST(command.block_production_policy->seed() == 71U);
}

BOOST_AUTO_TEST_CASE(tui_command_parser_builds_driver_commands) {
  BOOST_CHECK(bbp::TuiCommandParser::Parse("reconnect", 0U).kind ==
              bbp::SimulationCommandKind::kReconnectNode);
  const bbp::ParsedTuiCommand difficulty =
      bbp::TuiCommandParser::Parse("mining-difficulty 3.5", 0U);
  BOOST_REQUIRE(difficulty.mining_difficulty);
  BOOST_TEST(difficulty.mining_difficulty->value() == 3.5);

  const bbp::ParsedTuiCommand connect =
      bbp::TuiCommandParser::Parse("connect-peer firo-2", 0U);
  BOOST_CHECK(connect.kind == bbp::SimulationCommandKind::kConnectPeer);
  BOOST_REQUIRE(connect.peer_node_id);
  BOOST_TEST(*connect.peer_node_id == "firo-2");

  const bbp::ParsedTuiCommand disconnect =
      bbp::TuiCommandParser::Parse("disconnect-peer firo-3", 0U);
  BOOST_CHECK(disconnect.kind == bbp::SimulationCommandKind::kDisconnectPeer);
  BOOST_REQUIRE(disconnect.peer_node_id);
  BOOST_TEST(*disconnect.peer_node_id == "firo-3");

  const bbp::ParsedTuiCommand policy =
      bbp::TuiCommandParser::Parse("peer-policy 1 3", 0U);
  BOOST_CHECK(policy.kind == bbp::SimulationCommandKind::kSetPeerCountPolicy);
  BOOST_REQUIRE(policy.peer_count_policy);
  BOOST_TEST(policy.peer_count_policy->minimum() == 1U);
  BOOST_TEST(policy.peer_count_policy->maximum() == 3U);
}

BOOST_AUTO_TEST_CASE(tui_command_parser_completes_unique_command_prefix) {
  BOOST_TEST(bbp::TuiCommandParser::Complete("reco") == "reconnect ");
  BOOST_TEST(bbp::TuiCommandParser::Complete("log-") == "log-");
  BOOST_CHECK_THROW(bbp::TuiCommandParser::Parse("unknown", 0U),
                    std::runtime_error);
  BOOST_CHECK_THROW(
      bbp::TuiCommandParser::Parse("block-production nope 1000", 0U),
      std::runtime_error);
  BOOST_CHECK_THROW(bbp::TuiCommandParser::Parse("connect-peer", 0U),
                    std::runtime_error);
  BOOST_CHECK_THROW(bbp::TuiCommandParser::Parse("peer-policy 2 1", 0U),
                    std::runtime_error);
}
