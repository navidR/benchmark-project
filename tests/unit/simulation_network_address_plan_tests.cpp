#include <algorithm>
#include <boost/json/array.hpp>
#include <boost/test/unit_test.hpp>
#include <cstdint>
#include <limits>
#include <set>
#include <string>
#include <vector>

#include "bbp/default_peer_topology.h"
#include "bbp/simulation_network_address_plan.h"

BOOST_AUTO_TEST_CASE(simulation_network_plan_assigns_distinct_node_subnets) {
  const bbp::SimulationNetworkAddressPlan plan =
      bbp::SimulationNetworkAddressPlan::FromCidr("10.210.4.0/26", 16U);

  BOOST_TEST(plan.Cidr() == "10.210.4.0/26");
  BOOST_TEST(plan.HostAddress(0U) == "10.210.4.0");
  BOOST_TEST(plan.NodeAddress(0U) == "10.210.4.1");
  BOOST_TEST(plan.HostAddress(15U) == "10.210.4.30");
  BOOST_TEST(plan.NodeAddress(15U) == "10.210.4.31");
  BOOST_TEST(plan.NodePrefixLength() == 31U);
}

BOOST_AUTO_TEST_CASE(simulation_network_plan_skips_overlapping_kernel_route) {
  const bbp::SimulationNetworkAddressPlan first =
      bbp::SimulationNetworkAddressPlan::Allocate("same-run", 8U, {});
  bbp::RouteInfo occupied;
  occupied.destination = first.NodeAddress(0U);
  occupied.prefix_len = 32U;

  const bbp::SimulationNetworkAddressPlan second =
      bbp::SimulationNetworkAddressPlan::Allocate("same-run", 8U, {occupied});

  BOOST_TEST(second.HostAddress(0U) != first.HostAddress(0U));
}

BOOST_AUTO_TEST_CASE(simulation_network_plan_ignores_default_route) {
  bbp::RouteInfo default_route;
  default_route.destination = "0.0.0.0";
  default_route.prefix_len = 0U;

  const bbp::SimulationNetworkAddressPlan plan =
      bbp::SimulationNetworkAddressPlan::Allocate("default-route", 1U,
                                                  {default_route});

  BOOST_TEST(!plan.Cidr().empty());
}

BOOST_AUTO_TEST_CASE(
    simulation_network_plan_revalidates_selected_node_subnets) {
  const bbp::SimulationNetworkAddressPlan plan =
      bbp::SimulationNetworkAddressPlan::FromCidr("10.210.4.0/26", 16U);
  bbp::RouteInfo occupied;
  occupied.destination = "10.210.4.8";
  occupied.prefix_len = 29U;

  BOOST_CHECK_THROW(plan.RequireNodeSlotsAvailable({4U}, {occupied}),
                    std::runtime_error);
  BOOST_CHECK_NO_THROW(plan.RequireNodeSlotsAvailable({1U}, {occupied}));

  bbp::RouteInfo default_route;
  default_route.destination = "0.0.0.0";
  default_route.prefix_len = 0U;
  BOOST_CHECK_NO_THROW(plan.RequireNodeSlotsAvailable({2U}, {default_route}));

  bbp::AddressInfo route_free_collision;
  route_free_collision.if_name = "foreign0";
  route_free_collision.address = plan.HostAddress(1U);
  route_free_collision.prefix_len = 32U;
  BOOST_CHECK_THROW(
      plan.RequireNodeSlotsAvailable({1U}, {}, {route_free_collision}),
      std::runtime_error);
  BOOST_CHECK_NO_THROW(
      plan.RequireNodeSlotsAvailable({2U}, {}, {route_free_collision}));
}

BOOST_AUTO_TEST_CASE(simulation_network_plan_rejects_invalid_persisted_range) {
  BOOST_CHECK_THROW(
      bbp::SimulationNetworkAddressPlan::FromCidr("not-a-network", 1U),
      std::runtime_error);
  BOOST_CHECK_THROW(bbp::SimulationNetworkAddressPlan::FromCidr("::1/64", 1U),
                    std::runtime_error);
  BOOST_CHECK_THROW(
      bbp::SimulationNetworkAddressPlan::FromCidr("10.210.0.1/32", 1U),
      std::runtime_error);
}

BOOST_AUTO_TEST_CASE(directional_network_policies_keep_canonical_edge_bands) {
  bbp::NetworkCondition delayed;
  delayed.delay_ms = 25U;
  bbp::NetworkCondition limited;
  limited.bandwidth_kbps = 8U;

  bbp::PeerTopologyConfig topology;
  topology.kind = bbp::PeerTopologyKind::kCustomEdgeList;
  topology.edges = {
      {.from = 0U,
       .to = 1U,
       .bidirectional = false,
       .active = true,
       .condition = delayed},
      {.from = 0U, .to = 2U, .bidirectional = false, .active = true},
      {.from = 0U,
       .to = 3U,
       .bidirectional = false,
       .active = true,
       .condition = limited},
  };
  const bbp::SimulationNetworkAddressPlan plan =
      bbp::SimulationNetworkAddressPlan::FromCidr("10.210.4.0/26", 4U);

  const auto policies =
      bbp::ResolveDirectionalNetworkPolicies(topology, plan, 4U, 0U);
  BOOST_REQUIRE_EQUAL(policies.size(), 2U);
  BOOST_TEST(policies[0].band == 1U);
  BOOST_TEST(policies[0].destination_address == plan.NodeAddress(1U));
  BOOST_CHECK(policies[0].condition == delayed);
  BOOST_TEST(policies[1].band == 3U);
  BOOST_TEST(policies[1].destination_address == plan.NodeAddress(3U));
  BOOST_CHECK(policies[1].condition == limited);
}

BOOST_AUTO_TEST_CASE(simulation_network_plan_growth_preserves_reserved_links) {
  const auto initial =
      bbp::SimulationNetworkAddressPlan::FromCidr("10.42.0.0/26", 16U);
  bbp::RouteInfo route;
  route.destination = "10.42.0.32";
  route.prefix_len = 31U;
  bbp::AddressInfo address;
  address.address = "10.42.0.35";
  address.prefix_len = 32U;
  const auto grown = initial.Expanded(21U, {route}, {address});
  const auto original_links = initial.LinkCidrs();
  const auto grown_links = grown.LinkCidrs();
  BOOST_TEST(initial.capacity() == 16U);
  BOOST_TEST(grown.capacity() == 21U);
  BOOST_TEST(std::equal(original_links.begin(), original_links.end(),
                        grown_links.begin()));
  std::set<std::string> addresses;
  for (std::uint32_t slot = 0U; slot < grown.capacity(); ++slot) {
    addresses.insert(grown.HostAddress(slot));
    addresses.insert(grown.NodeAddress(slot));
  }
  BOOST_TEST(addresses.size() == 42U);
  for (const std::string excluded :
       {"10.42.0.32", "10.42.0.33", "10.42.0.34", "10.42.0.35"}) {
    BOOST_TEST(!addresses.contains(excluded));
  }
}

BOOST_AUTO_TEST_CASE(simulation_network_plan_exhaustion_and_ipv4_boundaries) {
  BOOST_TEST(bbp::SimulationNetworkAddressPlan::PoolLinkCapacity(
                 "10.0.0.0/20") == 2048U);
  BOOST_TEST(bbp::SimulationNetworkAddressPlan::PoolLinkCapacity(
                 "10.0.0.0/13") == 262144U);
  const auto initial =
      bbp::SimulationNetworkAddressPlan::FromCidr("198.18.0.0/30", 1U);
  bbp::RouteInfo occupied;
  occupied.destination = "198.18.0.2";
  occupied.prefix_len = 31U;
  BOOST_CHECK_EXCEPTION(
      initial.Expanded(2U, {occupied}), std::runtime_error,
      [](const std::runtime_error& error) {
        return std::string(error.what()).find("requested 1, available 0") !=
               std::string::npos;
      });
  BOOST_TEST(initial.capacity() == 1U);
  const auto highest =
      bbp::SimulationNetworkAddressPlan::FromCidr("255.255.255.254/31", 1U);
  BOOST_TEST(highest.NodeAddress(0U) == "255.255.255.255");
  BOOST_CHECK_EXCEPTION(
      bbp::SimulationNetworkAddressPlan::FromCidr(
          "0.0.0.0/0", std::numeric_limits<std::uint32_t>::max()),
      std::runtime_error, [](const std::runtime_error& error) {
        return std::string(error.what())
                   .find("requested 4294967295, available 2147483648") !=
               std::string::npos;
      });
  const auto empty =
      bbp::SimulationNetworkAddressPlan::Allocate("empty", 0U, {});
  BOOST_TEST(empty.capacity() == 0U);
  BOOST_TEST(empty.Expanded(1U, {}).capacity() == 1U);
}

BOOST_AUTO_TEST_CASE(simulation_network_plan_restores_ordered_allocation) {
  auto serialized =
      bbp::SimulationNetworkAddressPlan::FromCidr("10.42.0.0/29", 2U)
          .ToSerialized();
  auto& links = serialized.at("link_cidrs").as_array();
  std::swap(links[0U], links[1U]);
  const auto restored =
      bbp::SimulationNetworkAddressPlan::FromSerialized(serialized);
  BOOST_TEST(restored.HostAddress(0U) == "10.42.0.2");
  BOOST_TEST(restored.HostAddress(1U) == "10.42.0.0");
  BOOST_CHECK(restored.ToSerialized() == serialized);
  const auto grown = restored.Expanded(3U, {});
  BOOST_TEST(grown.HostAddress(0U) == restored.HostAddress(0U));
  BOOST_TEST(grown.HostAddress(1U) == restored.HostAddress(1U));
  links[1U] = links[0U];
  BOOST_CHECK_THROW(
      bbp::SimulationNetworkAddressPlan::FromSerialized(serialized),
      std::runtime_error);
  links[1U] = "10.43.0.0/31";
  BOOST_CHECK_THROW(
      bbp::SimulationNetworkAddressPlan::FromSerialized(serialized),
      std::runtime_error);
  links[1U] = "10.42.0.4/30";
  BOOST_CHECK_THROW(
      bbp::SimulationNetworkAddressPlan::FromSerialized(serialized),
      std::runtime_error);
}
