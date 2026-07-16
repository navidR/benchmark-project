#include <boost/test/unit_test.hpp>

#include "bbp/simulation_command.h"

BOOST_AUTO_TEST_CASE(simulation_command_kind_round_trips_names) {
  constexpr bbp::SimulationCommandKind kKinds[] = {
      bbp::SimulationCommandKind::kIncreaseLogVerbosity,
      bbp::SimulationCommandKind::kDecreaseLogVerbosity,
      bbp::SimulationCommandKind::kStopMining,
      bbp::SimulationCommandKind::kDisconnectNode,
      bbp::SimulationCommandKind::kReconnectNode,
      bbp::SimulationCommandKind::kSetBlockProductionPolicy,
      bbp::SimulationCommandKind::kSetMiningDifficulty,
      bbp::SimulationCommandKind::kKillNode,
      bbp::SimulationCommandKind::kConnectPeer,
      bbp::SimulationCommandKind::kDisconnectPeer,
      bbp::SimulationCommandKind::kSetPeerCountPolicy,
      bbp::SimulationCommandKind::kFreezeNode,
      bbp::SimulationCommandKind::kThawNode,
      bbp::SimulationCommandKind::kStopNode,
      bbp::SimulationCommandKind::kRestartNode,
      bbp::SimulationCommandKind::kGenerateBlocks,
      bbp::SimulationCommandKind::kSetResourceProfile,
      bbp::SimulationCommandKind::kSetNetworkProfile,
      bbp::SimulationCommandKind::kSetNetworkCondition,
      bbp::SimulationCommandKind::kBlockNetworkFlow,
      bbp::SimulationCommandKind::kUnblockNetworkFlow,
      bbp::SimulationCommandKind::kPartitionNodes,
      bbp::SimulationCommandKind::kHealPartition,
  };

  for (bbp::SimulationCommandKind kind : kKinds) {
    const std::optional<bbp::SimulationCommandKind> parsed =
        bbp::SimulationCommandKindFromName(
            bbp::SimulationCommandKindName(kind));
    BOOST_REQUIRE(parsed);
    BOOST_CHECK(*parsed == kind);
  }
  BOOST_TEST(!bbp::SimulationCommandKindFromName("unknown"));
}

BOOST_AUTO_TEST_CASE(simulation_command_classifies_destructive_actions) {
  BOOST_TEST(bbp::SimulationCommandRequiresConfirmation(
      bbp::SimulationCommandKind::kKillNode));
  BOOST_TEST(bbp::SimulationCommandRequiresConfirmation(
      bbp::SimulationCommandKind::kRestartNode));
  BOOST_TEST(bbp::SimulationCommandRequiresConfirmation(
      bbp::SimulationCommandKind::kSetNetworkProfile));
  BOOST_TEST(bbp::SimulationCommandRequiresConfirmation(
      bbp::SimulationCommandKind::kSetNetworkCondition));
  BOOST_TEST(bbp::SimulationCommandRequiresConfirmation(
      bbp::SimulationCommandKind::kBlockNetworkFlow));
  BOOST_TEST(bbp::SimulationCommandRequiresConfirmation(
      bbp::SimulationCommandKind::kPartitionNodes));
  BOOST_TEST(!bbp::SimulationCommandRequiresConfirmation(
      bbp::SimulationCommandKind::kGenerateBlocks));
  BOOST_TEST(!bbp::SimulationCommandRequiresConfirmation(
      bbp::SimulationCommandKind::kThawNode));
  BOOST_TEST(!bbp::SimulationCommandRequiresConfirmation(
      bbp::SimulationCommandKind::kUnblockNetworkFlow));
  BOOST_TEST(!bbp::SimulationCommandRequiresConfirmation(
      bbp::SimulationCommandKind::kHealPartition));
}
