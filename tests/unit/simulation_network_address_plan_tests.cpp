#include <boost/test/unit_test.hpp>
#include <cstdint>
#include <string>
#include <vector>

#include "bbp/default_peer_topology.h"
#include "bbp/simulation_network_address_plan.h"

BOOST_AUTO_TEST_CASE(simulation_network_plan_assigns_distinct_node_subnets) {
  const bbp::SimulationNetworkAddressPlan plan =
      bbp::SimulationNetworkAddressPlan::FromCidr("10.210.4.0/26", 16U);

  BOOST_TEST(plan.Cidr() == "10.210.4.0/26");
  BOOST_TEST(plan.HostAddress(0U) == "10.210.4.1");
  BOOST_TEST(plan.NodeAddress(0U) == "10.210.4.2");
  BOOST_TEST(plan.HostAddress(15U) == "10.210.4.61");
  BOOST_TEST(plan.NodeAddress(15U) == "10.210.4.62");
  BOOST_TEST(plan.NodePrefixLength() == 30U);
}

BOOST_AUTO_TEST_CASE(simulation_network_plan_skips_overlapping_kernel_route) {
  const bbp::SimulationNetworkAddressPlan first =
      bbp::SimulationNetworkAddressPlan::Allocate("same-run", 8U, {});
  bbp::RouteInfo occupied;
  occupied.destination = first.NodeAddress(0U);
  occupied.prefix_len = 32U;

  const bbp::SimulationNetworkAddressPlan second =
      bbp::SimulationNetworkAddressPlan::Allocate("same-run", 8U, {occupied});

  BOOST_TEST(second.Cidr() != first.Cidr());
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

  BOOST_CHECK_THROW(plan.RequireNodeSlotsAvailable({2U}, {occupied}),
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
      bbp::SimulationNetworkAddressPlan::FromCidr("10.211.0.0/26", 1U),
      std::runtime_error);
  BOOST_CHECK_THROW(
      bbp::SimulationNetworkAddressPlan::FromCidr("10.210.0.0/24", 1U),
      std::runtime_error);
  BOOST_CHECK_THROW(
      bbp::SimulationNetworkAddressPlan::FromCidr("10.210.0.1/26", 1U),
      std::runtime_error);
}

BOOST_AUTO_TEST_CASE(directional_network_policies_keep_canonical_edge_bands) {
  bbp::NetworkCondition delayed;
  delayed.delay_ms = 25U;
  bbp::NetworkCondition limited;
  limited.bandwidth_mbps = 8U;

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
