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
  const bsim::SimulationRegistry registry =
      bsim::SimulationRegistry::FromTopology(TestTopology(), {});

  BOOST_REQUIRE_EQUAL(registry.wallets().size(), 2U);
  BOOST_TEST(registry.wallets()[0].wallet_index == 1U);
  BOOST_TEST(registry.wallets()[0].node == 1U);
  BOOST_TEST(registry.wallets()[0].address.empty());
  BOOST_TEST(registry.wallets()[1].node == 3U);
  BOOST_TEST(registry.MinerNodeForWalletIndex(0) == 2U);
  BOOST_TEST(registry.MinerNodeForWalletIndex(5) == 2U);
}

BOOST_AUTO_TEST_CASE(simulation_registry_accepts_private_wallet_mode) {
  bsim::WalletInitialization initialization;
  initialization.mode = bsim::WalletPrivacyMode::kPrivate;

  const bsim::SimulationRegistry registry =
      bsim::SimulationRegistry::FromTopology(TestTopology(), initialization);

  BOOST_TEST(static_cast<int>(registry.wallet_initialization().mode) ==
             static_cast<int>(bsim::WalletPrivacyMode::kPrivate));
}
