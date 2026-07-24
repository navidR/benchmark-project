#include <boost/test/unit_test.hpp>
#include <stdexcept>
#include <utility>
#include <vector>

#include "bbp/runtime_wallet_registry.h"

namespace {

bbp::SimulationRegistry EmptyRegistry(std::uint32_t node_count = 2U) {
  bbp::NodeRoleTopology topology;
  topology.configured = true;
  topology.node_count = node_count;
  return bbp::SimulationRegistry::FromTopology(topology, {});
}

bbp::WalletIdentity Wallet(std::uint32_t node, std::string node_id,
                           std::string address) {
  return bbp::WalletIdentity{
      .node = node,
      .node_id = std::move(node_id),
      .address = address,
      .funding_address = std::move(address),
  };
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

BOOST_AUTO_TEST_CASE(runtime_wallet_registry_publishes_one_atomic_generation) {
  bbp::RuntimeWalletRegistry registry;
  registry.Initialize(EmptyRegistry());
  const bbp::RuntimeWalletSnapshot before = registry.Snapshot();

  {
    auto abandoned = registry.PrepareAppend(
        before.generation(), {Wallet(1U, "firo-1", "abandoned-address")}, 2U);
    BOOST_TEST(before.wallets().empty());
  }
  BOOST_TEST(registry.Snapshot().generation() == before.generation());

  auto prepared = registry.PrepareAppend(
      before.generation(),
      {Wallet(1U, "firo-1", "address-1"), Wallet(2U, "firo-2", "address-2")},
      2U);
  BOOST_TEST(before.wallets().empty());

  const bbp::RuntimeWalletSnapshot after = prepared.Commit();
  BOOST_TEST(after.generation() == before.generation() + 1U);
  BOOST_REQUIRE_EQUAL(after.wallets().size(), 2U);
  BOOST_TEST(after.wallets()[0].wallet_index == 1U);
  BOOST_TEST(after.wallets()[0].node_id == "firo-1");
  BOOST_TEST(after.registry().topology().wallet_node_count == 2U);
  BOOST_TEST(before.wallets().empty());
}

BOOST_AUTO_TEST_CASE(runtime_wallet_registry_rejects_stale_or_invalid_append) {
  bbp::RuntimeWalletRegistry registry;
  registry.Initialize(EmptyRegistry());
  const std::uint64_t generation = registry.Snapshot().generation();

  auto prepared = registry.PrepareAppend(
      generation, {Wallet(1U, "firo-1", "address-1")}, 2U);
  static_cast<void>(prepared.Commit());

  BOOST_CHECK_THROW(registry.PrepareAppend(
                        generation, {Wallet(2U, "firo-2", "address-2")}, 2U),
                    std::runtime_error);
  BOOST_CHECK_THROW(
      registry.PrepareAppend(registry.Snapshot().generation(),
                             {Wallet(3U, "firo-3", "address-3")}, 2U),
      std::invalid_argument);
}

BOOST_AUTO_TEST_CASE(
    runtime_wallet_registry_prepares_wallet_role_for_new_node_generation) {
  bbp::RuntimeWalletRegistry registry;
  registry.Initialize(EmptyRegistry(1U));
  const bbp::RuntimeWalletSnapshot before = registry.Snapshot();

  auto prepared = registry.PrepareAppend(
      before.generation(), {Wallet(2U, "firo-2", "address-2")}, 2U);
  BOOST_TEST(before.registry().topology().node_count == 1U);

  const bbp::RuntimeWalletSnapshot after = prepared.Commit();
  BOOST_TEST(after.registry().topology().node_count == 2U);
  BOOST_TEST(after.registry().topology().wallet_nodes ==
                 std::vector<std::uint32_t>{1U},
             boost::test_tools::per_element());
  BOOST_TEST(before.registry().topology().node_count == 1U);
  BOOST_TEST(before.wallets().empty());
}

BOOST_AUTO_TEST_CASE(
    runtime_wallet_registry_prepares_atomic_miner_role_generation) {
  bbp::RuntimeWalletRegistry registry;
  registry.Initialize(EmptyRegistry(2U));
  const bbp::RuntimeWalletSnapshot before = registry.Snapshot();

  {
    auto abandoned =
        registry.PrepareUpdate(before.generation(), {}, {1U}, {}, 3U);
    BOOST_TEST(abandoned.Commit().registry().topology().node_count == 3U);
  }
  const bbp::RuntimeWalletSnapshot after_first = registry.Snapshot();
  BOOST_TEST(after_first.generation() == before.generation() + 1U);

  {
    auto abandoned =
        registry.PrepareUpdate(after_first.generation(), {}, {0U}, {}, 3U);
    static_cast<void>(abandoned);
  }
  BOOST_TEST(registry.Snapshot().generation() == after_first.generation());
  BOOST_TEST(registry.Snapshot().registry().topology().miner_nodes ==
                 std::vector<std::uint32_t>({1U}),
             boost::test_tools::per_element());

  auto prepared =
      registry.PrepareUpdate(after_first.generation(), {}, {0U}, {}, 3U);
  const bbp::RuntimeWalletSnapshot after = prepared.Commit();
  BOOST_TEST(after.generation() == after_first.generation() + 1U);
  BOOST_TEST(after.registry().topology().node_count == 3U);
  BOOST_TEST(after.registry().topology().miner_nodes ==
                 std::vector<std::uint32_t>({0U, 1U}),
             boost::test_tools::per_element());
  BOOST_TEST(after.registry().topology().miner_node_count == 2U);

  bbp::SimulationRegistry replacement = after.registry();
  replacement.RemoveMinerNodes({1U});
  auto prepared_removal =
      registry.PrepareReplace(after.generation(), std::move(replacement));
  const bbp::RuntimeWalletSnapshot removed = prepared_removal.Commit();
  BOOST_TEST(removed.generation() == after.generation() + 1U);
  BOOST_TEST(removed.registry().topology().miner_nodes ==
                 std::vector<std::uint32_t>({0U}),
             boost::test_tools::per_element());
  BOOST_TEST(after.registry().topology().miner_node_count == 2U);
}

BOOST_AUTO_TEST_CASE(
    runtime_wallet_registry_publishes_private_masternode_state_atomically) {
  bbp::RuntimeWalletRegistry registry;
  registry.Initialize(EmptyRegistry(3U));
  const bbp::RuntimeWalletSnapshot before = registry.Snapshot();

  {
    auto abandoned = registry.PrepareUpdate(
        before.generation(), {}, {}, {Masternode(2U, "firo-2", "2")}, 3U);
    static_cast<void>(abandoned);
  }
  BOOST_TEST(registry.Snapshot().generation() == before.generation());
  BOOST_TEST(registry.Snapshot().masternodes().empty());

  auto prepared = registry.PrepareUpdate(before.generation(), {}, {},
                                         {Masternode(2U, "firo-2", "2")}, 3U);
  const bbp::RuntimeWalletSnapshot published = prepared.Commit();
  BOOST_TEST(published.generation() == before.generation() + 1U);
  BOOST_REQUIRE_EQUAL(published.masternodes().size(), 1U);
  BOOST_TEST(published.masternodes().front().operator_secret_key == "secret-2");
  BOOST_TEST(published.registry().topology().masternode_nodes ==
                 std::vector<std::uint32_t>{1U},
             boost::test_tools::per_element());

  bbp::MasternodeIdentity duplicate = Masternode(3U, "firo-3", "3");
  duplicate.pro_tx_hash = published.masternodes().front().pro_tx_hash;
  BOOST_CHECK_THROW(
      registry.PrepareUpdate(published.generation(), {}, {}, {duplicate}, 3U),
      std::invalid_argument);

  bbp::SimulationRegistry replacement = published.registry();
  replacement.RemoveMasternodeNodes({1U});
  auto prepared_removal =
      registry.PrepareReplace(published.generation(), std::move(replacement));
  const bbp::RuntimeWalletSnapshot removed = prepared_removal.Commit();
  BOOST_TEST(removed.generation() == published.generation() + 1U);
  BOOST_TEST(removed.masternodes().empty());
  BOOST_TEST(removed.registry().topology().masternode_nodes.empty());
  BOOST_REQUIRE_EQUAL(published.masternodes().size(), 1U);

  bbp::NodeRoleTopology inconsistent_topology;
  inconsistent_topology.configured = true;
  inconsistent_topology.node_count = 3U;
  inconsistent_topology.masternode_node_count = 1U;
  inconsistent_topology.masternode_nodes = {1U};
  bbp::SimulationRegistry inconsistent =
      bbp::SimulationRegistry::FromTopology(inconsistent_topology, {});
  BOOST_CHECK_THROW(
      registry.PrepareReplace(removed.generation(), std::move(inconsistent)),
      std::invalid_argument);
}

BOOST_AUTO_TEST_CASE(
    runtime_wallet_registry_replaces_remapped_roles_immutably) {
  bbp::NodeRoleTopology topology;
  topology.configured = true;
  topology.node_count = 3U;
  topology.wallet_node_count = 1U;
  topology.miner_node_count = 1U;
  topology.wallet_nodes = {2U};
  topology.miner_nodes = {2U};
  topology.allow_miner_wallet_overlap = true;
  bbp::SimulationRegistry initial =
      bbp::SimulationRegistry::FromTopology(topology, {});
  bbp::WalletIdentity& wallet = initial.MutableWalletByIndex(0U);
  wallet.node_id = "firo-3";
  wallet.address = "address-3";
  wallet.funding_address = "funding-3";

  bbp::RuntimeWalletRegistry registry;
  registry.Initialize(initial);
  const bbp::RuntimeWalletSnapshot before = registry.Snapshot();
  bbp::SimulationRegistry replacement = before.registry().RemapRuntimeNodes(
      {0U, std::nullopt, 1U}, bbp::PeerTopologyConfig{});
  auto prepared =
      registry.PrepareReplace(before.generation(), std::move(replacement));
  BOOST_TEST(before.registry().topology().node_count == 3U);

  const bbp::RuntimeWalletSnapshot after = prepared.Commit();
  BOOST_TEST(after.generation() == before.generation() + 1U);
  BOOST_TEST(after.registry().topology().node_count == 2U);
  BOOST_TEST(after.registry().topology().wallet_nodes ==
                 std::vector<std::uint32_t>({1U}),
             boost::test_tools::per_element());
  BOOST_TEST(after.registry().topology().miner_nodes ==
                 std::vector<std::uint32_t>({1U}),
             boost::test_tools::per_element());
  BOOST_REQUIRE_EQUAL(after.wallets().size(), 1U);
  BOOST_TEST(after.wallets().front().node == 2U);
  BOOST_TEST(after.wallets().front().node_id == "firo-3");
  BOOST_TEST(before.registry().topology().node_count == 3U);
  BOOST_TEST(before.wallets().front().node == 3U);
}
