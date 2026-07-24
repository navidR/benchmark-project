#include <boost/test/unit_test.hpp>
#include <stdexcept>
#include <utility>
#include <vector>

#include "bbp/runtime_wallet_registry.h"

namespace {

bbp::SimulationRegistry EmptyRegistry() {
  bbp::NodeRoleTopology topology;
  topology.configured = true;
  topology.node_count = 2U;
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

}  // namespace

BOOST_AUTO_TEST_CASE(runtime_wallet_registry_publishes_one_atomic_generation) {
  bbp::RuntimeWalletRegistry registry;
  registry.Initialize(EmptyRegistry());
  const bbp::RuntimeWalletSnapshot before = registry.Snapshot();

  auto prepared = registry.PrepareAppend(
      before.generation(),
      {Wallet(1U, "firo-1", "address-1"), Wallet(2U, "firo-2", "address-2")},
      2U);
  BOOST_TEST(registry.Snapshot().wallets().empty());

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
