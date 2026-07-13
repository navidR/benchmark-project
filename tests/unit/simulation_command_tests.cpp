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
