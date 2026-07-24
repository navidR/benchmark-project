#include <boost/test/unit_test.hpp>
#include <stdexcept>
#include <vector>

#include "bbp/runtime_peer_topology.h"

BOOST_AUTO_TEST_CASE(runtime_peer_topology_preserves_bands_across_edge_state) {
  bbp::NetworkCondition delayed;
  delayed.delay_ms = 25U;
  bbp::NetworkCondition limited;
  limited.bandwidth_mbps = 8U;

  bbp::PeerTopologyConfig config;
  config.kind = bbp::PeerTopologyKind::kCustomEdgeList;
  config.edges = {
      {.from = 0U,
       .to = 1U,
       .bidirectional = false,
       .active = true,
       .condition = delayed},
      {.from = 0U, .to = 2U, .bidirectional = false, .active = false},
      {.from = 0U,
       .to = 3U,
       .bidirectional = false,
       .active = true,
       .condition = limited},
  };
  bbp::RuntimePeerTopology topology(config, 4U);
  const bbp::SimulationNetworkAddressPlan plan =
      bbp::SimulationNetworkAddressPlan::FromCidr("10.210.8.0/26", 4U);

  BOOST_TEST(
      topology.ActivePeerIndexes(0U) == std::vector<std::uint32_t>({1U, 3U}),
      boost::test_tools::per_element());
  auto policies = topology.DirectionalPolicies(plan, 0U);
  BOOST_REQUIRE_EQUAL(policies.size(), 2U);
  BOOST_TEST(policies[0].band == 1U);
  BOOST_TEST(policies[1].band == 3U);

  const bbp::RuntimePeerTopologyEdge before_deactivate =
      topology.SetActive(0U, 1U, false);
  BOOST_TEST(topology.ActivePeerIndexes(0U) == std::vector<std::uint32_t>({3U}),
             boost::test_tools::per_element());
  topology.SetActive(0U, 2U, true);
  topology.SetCondition(0U, 2U, delayed);
  policies = topology.DirectionalPolicies(plan, 0U);
  BOOST_REQUIRE_EQUAL(policies.size(), 2U);
  BOOST_TEST(policies[0].band == 2U);
  BOOST_TEST(policies[1].band == 3U);

  topology.RestoreBaseline(0U, 2U);
  BOOST_TEST(!topology.Edge(0U, 2U).active);
  BOOST_CHECK_THROW(topology.SetCondition(0U, 2U, limited), std::runtime_error);
  topology.RestoreState(before_deactivate);
  BOOST_TEST(topology.Edge(0U, 1U).active);
  BOOST_TEST(topology.Edge(0U, 1U).band == 1U);
}

BOOST_AUTO_TEST_CASE(runtime_peer_topology_validates_inventory_and_updates) {
  bbp::PeerTopologyConfig config;
  config.kind = bbp::PeerTopologyKind::kCustomEdgeList;
  config.edges = {
      {.from = 0U, .to = 0U, .bidirectional = false, .active = false}};
  BOOST_CHECK_THROW(bbp::RuntimePeerTopology(config, 2U), std::runtime_error);

  config.edges = {
      {.from = 0U, .to = 1U, .bidirectional = false, .active = true}};
  bbp::RuntimePeerTopology topology(config, 2U);
  BOOST_CHECK_THROW(topology.Edge(1U, 0U), std::runtime_error);
  BOOST_CHECK_THROW(topology.SetActive(0U, 1U, true), std::runtime_error);
  BOOST_CHECK_THROW(topology.SetActive(1U, 0U, false), std::runtime_error);
  BOOST_CHECK_THROW(topology.ActivePeerIndexes(2U), std::out_of_range);
}

BOOST_AUTO_TEST_CASE(
    runtime_peer_topology_keeps_physical_session_for_reverse_active_edge) {
  bbp::PeerTopologyConfig config;
  config.kind = bbp::PeerTopologyKind::kCustomEdgeList;
  config.edges = {
      {.from = 0U, .to = 1U, .bidirectional = true, .active = true}};
  bbp::RuntimePeerTopology topology(config, 2U);

  BOOST_TEST(topology.PhysicalPeerRequired(0U, 1U));
  topology.SetActive(0U, 1U, false);
  BOOST_TEST(topology.PhysicalPeerRequired(0U, 1U));
  topology.SetActive(1U, 0U, false);
  BOOST_TEST(!topology.PhysicalPeerRequired(0U, 1U));
}

BOOST_AUTO_TEST_CASE(runtime_peer_topology_allows_only_explicit_default_empty) {
  bbp::PeerTopologyConfig config;
  BOOST_CHECK_THROW(bbp::RuntimePeerTopology(config, 0U), std::runtime_error);

  const bbp::RuntimePeerTopology empty(config, 0U, true);
  BOOST_TEST(empty.edges().empty());
  BOOST_CHECK_THROW(empty.ActivePeerIndexes(0U), std::out_of_range);

  bbp::RuntimePeerTopology expanded(config, 1U);
  expanded.PreserveCommonStateFrom(empty);
  BOOST_TEST(expanded.PreservesPhysicalPeerRequirementsFrom(empty, 0U));
  BOOST_TEST(expanded.edges().empty());
  BOOST_TEST(expanded.ActivePeerIndexes(0U).empty());
  BOOST_TEST(!expanded.PhysicalPeerRequired(0U, 0U));

  config.kind = bbp::PeerTopologyKind::kCustomEdgeList;
  BOOST_CHECK_THROW(bbp::RuntimePeerTopology(config, 0U, true),
                    std::runtime_error);
}

BOOST_AUTO_TEST_CASE(
    runtime_peer_topology_preserves_inactive_conditioned_region_edges) {
  bbp::NetworkCondition condition;
  condition.bandwidth_mbps = 9U;
  condition.delay_ms = 45U;

  bbp::PeerTopologyConfig config;
  config.kind = bbp::PeerTopologyKind::kInternetLikeRegionGraph;
  config.regions = {{0U, 1U}, {2U, 3U}};
  config.region_edges = {{.from_region = 0U,
                          .to_region = 1U,
                          .bidirectional = false,
                          .active = false,
                          .latency_ms = 45U,
                          .condition = condition}};

  bbp::RuntimePeerTopology topology(config, 4U);
  BOOST_TEST(!topology.Edge(0U, 2U).active);
  BOOST_REQUIRE(topology.Edge(0U, 2U).condition.has_value());
  BOOST_CHECK(*topology.Edge(0U, 2U).condition == condition);

  topology.SetActive(0U, 2U, true);
  const bbp::SimulationNetworkAddressPlan plan =
      bbp::SimulationNetworkAddressPlan::FromCidr("10.210.9.0/26", 4U);
  const auto policies = topology.DirectionalPolicies(plan, 0U);
  BOOST_REQUIRE_EQUAL(policies.size(), 1U);
  BOOST_TEST(policies.front().destination_address == plan.NodeAddress(2U));
  BOOST_CHECK(policies.front().condition == condition);

  topology.RestoreBaseline(0U, 2U);
  BOOST_TEST(!topology.Edge(0U, 2U).active);
}

BOOST_AUTO_TEST_CASE(
    runtime_peer_topology_carries_common_runtime_state_across_scale_up) {
  bbp::PeerTopologyConfig config;
  config.kind = bbp::PeerTopologyKind::kRing;
  bbp::RuntimePeerTopology previous(config, 3U);
  bbp::NetworkCondition delayed;
  delayed.delay_ms = 17U;
  previous.SetCondition(0U, 1U, delayed);
  previous.SetActive(1U, 0U, false);

  bbp::RuntimePeerTopology expanded(config, 4U);
  expanded.PreserveCommonStateFrom(previous);

  BOOST_TEST(!expanded.PreservesPhysicalPeerRequirementsFrom(previous, 3U));
  BOOST_TEST(previous.PhysicalPeerRequired(0U, 2U));
  BOOST_TEST(!expanded.PhysicalPeerRequired(0U, 2U));
  BOOST_REQUIRE(expanded.Edge(0U, 1U).condition);
  BOOST_CHECK(*expanded.Edge(0U, 1U).condition == delayed);
  BOOST_TEST(!expanded.Edge(1U, 0U).active);
  BOOST_CHECK_THROW(expanded.Edge(0U, 2U), std::runtime_error);
  BOOST_CHECK_NO_THROW(expanded.Edge(0U, 3U));
}
