#include <boost/test/unit_test.hpp>
#include <optional>

#include "bbp/simulation_registry.h"

namespace {

bbp::NodeRoleTopology TestTopology() {
  bbp::NodeRoleTopology topology;
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
  const bbp::SimulationRegistry registry =
      bbp::SimulationRegistry::FromTopology(TestTopology(), {});

  BOOST_REQUIRE_EQUAL(registry.wallets().size(), 2U);
  BOOST_TEST(registry.wallets()[0].wallet_index == 1U);
  BOOST_TEST(registry.wallets()[0].node == 1U);
  BOOST_TEST(registry.wallets()[0].address.empty());
  BOOST_TEST(registry.wallets()[1].node == 3U);
  BOOST_TEST(registry.MinerNodeForWalletIndex(0) == 2U);
  BOOST_TEST(registry.MinerNodeForWalletIndex(5) == 2U);
}

BOOST_AUTO_TEST_CASE(simulation_registry_accepts_private_wallet_mode) {
  bbp::WalletInitialization initialization;
  initialization.mode = bbp::WalletPrivacyMode::kPrivate;

  const bbp::SimulationRegistry registry =
      bbp::SimulationRegistry::FromTopology(TestTopology(), initialization);

  BOOST_TEST(static_cast<int>(registry.wallet_initialization().mode) ==
             static_cast<int>(bbp::WalletPrivacyMode::kPrivate));
}

BOOST_AUTO_TEST_CASE(wallet_initialization_names_round_trip) {
  const std::optional<bbp::WalletInitializationStrategy> strategy =
      bbp::WalletInitializationStrategyFromName(
          bbp::WalletInitializationStrategyName(
              bbp::WalletInitializationStrategy::kDriverRpc));
  BOOST_REQUIRE(strategy);
  BOOST_CHECK(*strategy == bbp::WalletInitializationStrategy::kDriverRpc);

  const std::optional<bbp::WalletPrivacyMode> public_mode =
      bbp::WalletPrivacyModeFromName(
          bbp::WalletPrivacyModeName(bbp::WalletPrivacyMode::kPublic));
  BOOST_REQUIRE(public_mode);
  BOOST_CHECK(*public_mode == bbp::WalletPrivacyMode::kPublic);

  const std::optional<bbp::WalletPrivacyMode> private_mode =
      bbp::WalletPrivacyModeFromName(
          bbp::WalletPrivacyModeName(bbp::WalletPrivacyMode::kPrivate));
  BOOST_REQUIRE(private_mode);
  BOOST_CHECK(*private_mode == bbp::WalletPrivacyMode::kPrivate);

  BOOST_TEST(!bbp::WalletInitializationStrategyFromName("unknown_strategy"));
  BOOST_TEST(!bbp::WalletPrivacyModeFromName("unknown_mode"));
}
