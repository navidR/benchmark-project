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

bbp::MasternodeIdentity Masternode(std::uint32_t node, std::string node_id,
                                   std::string suffix) {
  return bbp::MasternodeIdentity{
      .node = node,
      .node_id = std::move(node_id),
      .funding_wallet_node_id = "firo-wallet",
      .pro_tx_hash = "protx-" + suffix,
      .service = "10.77.0." + suffix + ":18168",
      .collateral_address = "collateral-" + suffix,
      .owner_address = "owner-" + suffix,
      .operator_public_key = "public-" + suffix,
      .operator_secret_key = "secret-" + suffix,
      .voting_address = "voting-" + suffix,
      .payout_address = "payout-" + suffix,
      .collateral_hash = "collateral-hash-" + suffix,
      .collateral_index = 1U,
      .state = "READY",
      .status = "ready",
  };
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

BOOST_AUTO_TEST_CASE(
    simulation_registry_validates_and_publishes_runtime_miners) {
  bbp::NodeRoleTopology topology;
  topology.configured = true;
  topology.node_count = 4U;
  topology.wallet_node_count = 1U;
  topology.wallet_nodes = {1U};
  bbp::SimulationRegistry registry =
      bbp::SimulationRegistry::FromTopology(topology, {});

  registry.AddMinerNode(3U);
  registry.AddMinerNode(0U);
  BOOST_TEST(
      registry.topology().miner_nodes == std::vector<std::uint32_t>({0U, 3U}),
      boost::test_tools::per_element());
  BOOST_TEST(registry.topology().miner_node_count == 2U);
  BOOST_CHECK_THROW(registry.AddMinerNode(0U), std::runtime_error);
  BOOST_CHECK_THROW(registry.AddMinerNode(4U), std::runtime_error);
  BOOST_CHECK_THROW(registry.AddMinerNode(1U), std::runtime_error);
  registry.RemoveMinerNodes({3U});
  BOOST_TEST(
      registry.topology().miner_nodes == std::vector<std::uint32_t>({0U}),
      boost::test_tools::per_element());
  BOOST_TEST(registry.topology().miner_node_count == 1U);
  BOOST_CHECK_THROW(registry.RemoveMinerNodes({3U}), std::runtime_error);
  BOOST_CHECK_THROW(registry.RemoveMinerNodes({0U, 0U}), std::runtime_error);
  registry.RemoveMinerNodes({0U});
  BOOST_TEST(registry.topology().miner_nodes.empty());
  BOOST_TEST(registry.topology().miner_node_count == 0U);

  topology.allow_miner_wallet_overlap = true;
  bbp::SimulationRegistry overlap =
      bbp::SimulationRegistry::FromTopology(topology, {});
  overlap.AddMinerNode(1U);
  BOOST_TEST(overlap.topology().miner_nodes == std::vector<std::uint32_t>({1U}),
             boost::test_tools::per_element());
}

BOOST_AUTO_TEST_CASE(
    simulation_registry_publishes_removes_and_validates_masternodes) {
  bbp::NodeRoleTopology topology;
  topology.configured = true;
  topology.node_count = 3U;
  topology.wallet_node_count = 1U;
  topology.miner_node_count = 1U;
  topology.wallet_nodes = {1U};
  topology.miner_nodes = {1U};
  topology.allow_miner_wallet_overlap = true;
  bbp::SimulationRegistry registry =
      bbp::SimulationRegistry::FromTopology(topology, {});

  registry.AddMasternode(Masternode(2U, "firo-2", "2"));
  registry.AddMasternode(Masternode(1U, "firo-1", "1"));
  BOOST_TEST(registry.topology().masternode_nodes ==
                 std::vector<std::uint32_t>({0U, 1U}),
             boost::test_tools::per_element());
  BOOST_TEST(registry.topology().masternode_node_count == 2U);
  BOOST_REQUIRE_EQUAL(registry.masternodes().size(), 2U);
  BOOST_TEST(registry.masternodes().front().node_id == "firo-1");
  BOOST_TEST(registry.masternodes().front().operator_secret_key == "secret-1");
  BOOST_CHECK_THROW(registry.AddMasternode(Masternode(1U, "firo-1", "3")),
                    std::runtime_error);
  bbp::MasternodeIdentity duplicate_service = Masternode(3U, "firo-3", "3");
  duplicate_service.service = registry.masternodes().front().service;
  BOOST_CHECK_THROW(registry.AddMasternode(std::move(duplicate_service)),
                    std::runtime_error);

  registry.RemoveMasternodeNodes({0U});
  BOOST_TEST(
      registry.topology().masternode_nodes == std::vector<std::uint32_t>({1U}),
      boost::test_tools::per_element());
  BOOST_REQUIRE_EQUAL(registry.masternodes().size(), 1U);
  BOOST_TEST(registry.masternodes().front().node_id == "firo-2");
  BOOST_CHECK_THROW(registry.RemoveMasternodeNodes({0U}), std::runtime_error);
}

BOOST_AUTO_TEST_CASE(
    simulation_registry_remaps_surviving_roles_and_all_peer_policy) {
  bbp::NodeRoleTopology topology;
  topology.configured = true;
  topology.node_count = 4U;
  topology.wallet_node_count = 2U;
  topology.miner_node_count = 1U;
  topology.allow_miner_wallet_overlap = true;
  topology.wallet_nodes = {0U, 3U};
  topology.miner_nodes = {3U};
  topology.peer_connectivity = {{.node = 0U,
                                 .mode = bbp::PeerConnectivityMode::kAllPeers,
                                 .peer_count = bbp::PeerCountPolicy(3U, 3U)}};
  bbp::SimulationRegistry registry =
      bbp::SimulationRegistry::FromTopology(topology, {});
  registry.MutableWalletByIndex(0U).node_id = "firo-1";
  registry.MutableWalletByIndex(1U).node_id = "firo-4";
  registry.AddMasternode(Masternode(4U, "firo-4", "4"));

  const bbp::SimulationRegistry remapped = registry.RemapRuntimeNodes(
      {0U, std::nullopt, 1U, 2U}, bbp::PeerTopologyConfig{});

  BOOST_TEST(remapped.topology().node_count == 3U);
  BOOST_TEST(
      remapped.topology().wallet_nodes == std::vector<std::uint32_t>({0U, 2U}),
      boost::test_tools::per_element());
  BOOST_TEST(
      remapped.topology().miner_nodes == std::vector<std::uint32_t>({2U}),
      boost::test_tools::per_element());
  BOOST_TEST(
      remapped.topology().masternode_nodes == std::vector<std::uint32_t>({2U}),
      boost::test_tools::per_element());
  BOOST_REQUIRE_EQUAL(remapped.wallets().size(), 2U);
  BOOST_TEST(remapped.wallets()[0U].node == 1U);
  BOOST_TEST(remapped.wallets()[0U].node_id == "firo-1");
  BOOST_TEST(remapped.wallets()[1U].node == 3U);
  BOOST_TEST(remapped.wallets()[1U].node_id == "firo-4");
  BOOST_REQUIRE_EQUAL(remapped.masternodes().size(), 1U);
  BOOST_TEST(remapped.masternodes().front().node == 3U);
  BOOST_TEST(remapped.masternodes().front().node_id == "firo-4");
  BOOST_REQUIRE_EQUAL(remapped.topology().peer_connectivity.size(), 1U);
  const bbp::PeerConnectivityPolicy& policy =
      remapped.topology().peer_connectivity.front();
  BOOST_TEST(policy.node == 0U);
  BOOST_CHECK(policy.mode == bbp::PeerConnectivityMode::kAllPeers);
  BOOST_TEST(policy.peer_count.minimum() == 2U);
  BOOST_TEST(policy.peer_count.maximum() == 2U);
}

BOOST_AUTO_TEST_CASE(
    simulation_registry_rejects_role_removal_and_incompatible_fixed_policy) {
  bbp::NodeRoleTopology role_topology;
  role_topology.configured = true;
  role_topology.node_count = 3U;
  role_topology.wallet_node_count = 1U;
  role_topology.wallet_nodes = {1U};
  const bbp::SimulationRegistry roles =
      bbp::SimulationRegistry::FromTopology(role_topology, {});
  BOOST_CHECK_THROW(roles.RemapRuntimeNodes({0U, std::nullopt, 1U},
                                            bbp::PeerTopologyConfig{}),
                    std::runtime_error);

  bbp::SimulationRegistry masternode_roles =
      bbp::SimulationRegistry::FromTopology(role_topology, {});
  masternode_roles.AddMasternode(Masternode(2U, "firo-2", "2"));
  BOOST_CHECK_THROW(masternode_roles.RemapRuntimeNodes(
                        {0U, std::nullopt, 1U}, bbp::PeerTopologyConfig{}),
                    std::runtime_error);

  bbp::NodeRoleTopology policy_topology;
  policy_topology.configured = true;
  policy_topology.node_count = 4U;
  policy_topology.peer_connectivity = {
      {.node = 0U,
       .mode = bbp::PeerConnectivityMode::kFixedCount,
       .peer_count = bbp::PeerCountPolicy(3U, 3U)}};
  const bbp::SimulationRegistry fixed =
      bbp::SimulationRegistry::FromTopology(policy_topology, {});
  BOOST_CHECK_THROW(fixed.RemapRuntimeNodes({0U, std::nullopt, 1U, 2U},
                                            bbp::PeerTopologyConfig{}),
                    std::runtime_error);
}
