#include <boost/test/unit_test.hpp>

#include "benchmark_sim/simulation_registry.h"

namespace {

bsim::NodeRoleTopology TestTopology() {
  bsim::NodeRoleTopology topology;
  topology.configured = true;
  topology.node_count = 3;
  topology.wallet_node_count = 2;
  topology.miner_node_count = 1;
  topology.wallet_nodes = {0, 2};
  topology.miner_nodes = {1};
  return topology;
}

}  // namespace

BOOST_AUTO_TEST_CASE(simulation_registry_initializes_wallet_nodes) {
  const bsim::Result<bsim::SimulationRegistry> registry_result =
      bsim::SimulationRegistry::FromTopology(TestTopology(), {});
  BOOST_REQUIRE(registry_result.has_value());
  const bsim::SimulationRegistry& registry = registry_result.unsafe_value();

  BOOST_REQUIRE_EQUAL(registry.wallets().size(), 2U);
  BOOST_TEST(registry.wallets()[0].wallet_index == 1U);
  BOOST_TEST(registry.wallets()[0].node == 1U);
  BOOST_TEST(registry.wallets()[0].address.empty());
  BOOST_TEST(registry.wallets()[1].node == 3U);
  const bsim::Result<uint32_t> first_miner =
      registry.MinerNodeForWalletIndex(0);
  const bsim::Result<uint32_t> wrapped_miner =
      registry.MinerNodeForWalletIndex(5);
  BOOST_REQUIRE(first_miner.has_value());
  BOOST_REQUIRE(wrapped_miner.has_value());
  BOOST_TEST(first_miner.unsafe_value() == 2U);
  BOOST_TEST(wrapped_miner.unsafe_value() == 2U);
}

BOOST_AUTO_TEST_CASE(simulation_registry_accepts_private_wallet_mode) {
  bsim::WalletInitialization initialization;
  initialization.mode = bsim::WalletPrivacyMode::kPrivate;

  const bsim::Result<bsim::SimulationRegistry> registry_result =
      bsim::SimulationRegistry::FromTopology(TestTopology(), initialization);
  BOOST_REQUIRE(registry_result.has_value());
  const bsim::SimulationRegistry& registry = registry_result.unsafe_value();

  BOOST_TEST(static_cast<int>(registry.wallet_initialization().mode) ==
             static_cast<int>(bsim::WalletPrivacyMode::kPrivate));
}

BOOST_AUTO_TEST_CASE(simulation_registry_reports_topology_mismatch) {
  bsim::NodeRoleTopology topology = TestTopology();
  topology.wallet_node_count = 3;

  const bsim::Result<bsim::SimulationRegistry> registry_result =
      bsim::SimulationRegistry::FromTopology(topology, {});

  BOOST_REQUIRE(!registry_result.has_value());
  BOOST_TEST(registry_result.error() ==
             "resolved topology wallet_nodes size must match "
             "wallet_node_count");
}

BOOST_AUTO_TEST_CASE(simulation_registry_reports_missing_miners) {
  bsim::NodeRoleTopology topology = TestTopology();
  topology.miner_nodes.clear();
  topology.miner_node_count = 0;
  const bsim::Result<bsim::SimulationRegistry> registry_result =
      bsim::SimulationRegistry::FromTopology(topology, {});
  BOOST_REQUIRE(registry_result.has_value());
  const bsim::SimulationRegistry& registry = registry_result.unsafe_value();

  const bsim::Result<uint32_t> miner_result =
      registry.MinerNodeForWalletIndex(0);

  BOOST_REQUIRE(!miner_result.has_value());
  BOOST_TEST(miner_result.error() == "simulation registry has no miner nodes");
}
