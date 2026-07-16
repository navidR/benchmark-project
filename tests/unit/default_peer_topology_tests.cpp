#include <algorithm>
#include <array>
#include <boost/test/unit_test.hpp>
#include <optional>

#include "bbp/default_peer_topology.h"

namespace {

std::vector<std::uint32_t> Peers(const bbp::PeerTopologyConfig& topology,
                                 std::uint32_t node_count, std::uint32_t node) {
  return bbp::ResolvePeerTopologyPeerIndexes(topology, node_count, node);
}

bool HasEdge(const std::vector<bbp::ResolvedPeerTopologyEdge>& edges,
             std::uint32_t from, std::uint32_t to) {
  return std::any_of(edges.begin(), edges.end(), [&](const auto& edge) {
    return edge.from == from && edge.to == to;
  });
}

bool SameEdges(const std::vector<bbp::ResolvedPeerTopologyEdge>& left,
               const std::vector<bbp::ResolvedPeerTopologyEdge>& right) {
  if (left.size() != right.size()) {
    return false;
  }
  for (std::size_t index = 0; index < left.size(); ++index) {
    if (!(left[index] == right[index])) {
      return false;
    }
  }
  return true;
}

}  // namespace

BOOST_AUTO_TEST_CASE(default_peer_topology_connects_every_node_pair) {
  const std::vector<std::uint32_t> first =
      bbp::DefaultStartupPeerIndexes(4U, 0U);
  const std::vector<std::uint32_t> middle =
      bbp::DefaultStartupPeerIndexes(4U, 2U);

  BOOST_TEST(first == std::vector<std::uint32_t>({1U, 2U, 3U}),
             boost::test_tools::per_element());
  BOOST_TEST(middle == std::vector<std::uint32_t>({0U, 1U, 3U}),
             boost::test_tools::per_element());
}

BOOST_AUTO_TEST_CASE(default_peer_topology_handles_single_node) {
  BOOST_TEST(bbp::DefaultStartupPeerIndexes(1U, 0U).empty());
}

BOOST_AUTO_TEST_CASE(default_peer_topology_rejects_invalid_node) {
  BOOST_CHECK_THROW(bbp::DefaultStartupPeerIndexes(0U, 0U), std::runtime_error);
  BOOST_CHECK_THROW(bbp::DefaultStartupPeerIndexes(3U, 3U), std::runtime_error);
}

BOOST_AUTO_TEST_CASE(peer_topology_kind_names_round_trip) {
  const std::array kinds = {
      bbp::PeerTopologyKind::kFullMesh,
      bbp::PeerTopologyKind::kRing,
      bbp::PeerTopologyKind::kStar,
      bbp::PeerTopologyKind::kRandomGraph,
      bbp::PeerTopologyKind::kScaleFreeGraph,
      bbp::PeerTopologyKind::kLatencyMatrix,
      bbp::PeerTopologyKind::kCustomEdgeList,
      bbp::PeerTopologyKind::kPartitionedGroups,
      bbp::PeerTopologyKind::kInternetLikeRegionGraph,
  };
  for (const bbp::PeerTopologyKind kind : kinds) {
    const auto parsed =
        bbp::PeerTopologyKindFromName(bbp::PeerTopologyKindName(kind));
    BOOST_REQUIRE(parsed.has_value());
    BOOST_TEST(static_cast<int>(*parsed) == static_cast<int>(kind));
  }
  BOOST_TEST(!bbp::PeerTopologyKindFromName("small_world").has_value());
}

BOOST_AUTO_TEST_CASE(peer_topology_resolves_full_mesh_ring_and_star) {
  bbp::PeerTopologyConfig topology;
  BOOST_TEST(
      Peers(topology, 4U, 2U) == std::vector<std::uint32_t>({0U, 1U, 3U}),
      boost::test_tools::per_element());

  topology.kind = bbp::PeerTopologyKind::kRing;
  BOOST_TEST(Peers(topology, 4U, 0U) == std::vector<std::uint32_t>({1U, 3U}),
             boost::test_tools::per_element());

  topology.kind = bbp::PeerTopologyKind::kStar;
  topology.star_center = 2U;
  BOOST_TEST(Peers(topology, 4U, 0U) == std::vector<std::uint32_t>({2U}),
             boost::test_tools::per_element());
  BOOST_TEST(
      Peers(topology, 4U, 2U) == std::vector<std::uint32_t>({0U, 1U, 3U}),
      boost::test_tools::per_element());
}

BOOST_AUTO_TEST_CASE(peer_topology_random_graph_is_seeded_and_bounded) {
  bbp::PeerTopologyConfig topology;
  topology.kind = bbp::PeerTopologyKind::kRandomGraph;
  topology.seed = 42U;
  topology.average_degree = 2U;
  const auto first = bbp::ResolvePeerTopologyEdges(topology, 6U);
  const auto second = bbp::ResolvePeerTopologyEdges(topology, 6U);
  BOOST_TEST(SameEdges(first, second));
  BOOST_REQUIRE_EQUAL(first.size(), 12U);
  for (const auto& edge : first) {
    BOOST_TEST(edge.from != edge.to);
    BOOST_TEST(HasEdge(first, edge.to, edge.from));
  }
  topology.average_degree = 6U;
  BOOST_CHECK_THROW(bbp::ResolvePeerTopologyEdges(topology, 6U),
                    std::runtime_error);
}

BOOST_AUTO_TEST_CASE(peer_topology_scale_free_graph_is_seeded_and_weighted) {
  bbp::PeerTopologyConfig topology;
  topology.kind = bbp::PeerTopologyKind::kScaleFreeGraph;
  topology.seed = 7U;
  topology.attachment_count = 2U;
  const auto edges = bbp::ResolvePeerTopologyEdges(topology, 8U);
  BOOST_REQUIRE_EQUAL(edges.size(), 26U);
  BOOST_TEST(SameEdges(edges, bbp::ResolvePeerTopologyEdges(topology, 8U)));
  for (std::uint32_t node = 3U; node < 8U; ++node) {
    std::uint32_t earlier_peers = 0U;
    for (const auto& edge : edges) {
      if (edge.from == node && edge.to < node) {
        ++earlier_peers;
      }
    }
    BOOST_TEST(earlier_peers == 2U);
  }
}

BOOST_AUTO_TEST_CASE(peer_topology_latency_matrix_preserves_direction) {
  bbp::PeerTopologyConfig topology;
  topology.kind = bbp::PeerTopologyKind::kLatencyMatrix;
  topology.latency_matrix_ms = {
      {0U, 10U, std::nullopt},
      {20U, 0U, 30U},
      {std::nullopt, std::nullopt, 0U},
  };
  const auto edges = bbp::ResolvePeerTopologyEdges(topology, 3U);
  BOOST_REQUIRE_EQUAL(edges.size(), 3U);
  BOOST_TEST(HasEdge(edges, 0U, 1U));
  BOOST_TEST(HasEdge(edges, 1U, 0U));
  BOOST_TEST(HasEdge(edges, 1U, 2U));
  BOOST_TEST(!HasEdge(edges, 2U, 1U));
  BOOST_REQUIRE(edges[0].latency_ms.has_value());
  BOOST_TEST(*edges[0].latency_ms == 10U);
  BOOST_REQUIRE(edges[0].condition.has_value());
  BOOST_TEST(edges[0].condition->delay_ms == 10U);

  topology.latency_matrix_ms[2][2] = 1U;
  BOOST_CHECK_THROW(bbp::ResolvePeerTopologyEdges(topology, 3U),
                    std::runtime_error);
}

BOOST_AUTO_TEST_CASE(peer_topology_custom_edges_preserve_typed_conditions) {
  bbp::NetworkCondition condition;
  condition.bandwidth_mbps = 7U;
  condition.delay_ms = 23U;
  condition.jitter_ms = 4U;
  condition.loss_basis_points = 5U;
  condition.duplicate_basis_points = 6U;
  condition.corrupt_basis_points = 7U;
  condition.reorder_basis_points = 8U;
  condition.limit_packets = 900U;

  bbp::PeerTopologyConfig topology;
  topology.kind = bbp::PeerTopologyKind::kCustomEdgeList;
  topology.edges = {{.from = 0U,
                     .to = 1U,
                     .bidirectional = true,
                     .active = true,
                     .latency_ms = 23U,
                     .condition = condition}};

  const auto edges = bbp::ResolvePeerTopologyEdges(topology, 2U);
  BOOST_REQUIRE_EQUAL(edges.size(), 2U);
  for (const auto& edge : edges) {
    BOOST_REQUIRE(edge.condition.has_value());
    BOOST_CHECK(*edge.condition == condition);
  }
}

BOOST_AUTO_TEST_CASE(peer_topology_rejects_conflicting_duplicate_conditions) {
  bbp::NetworkCondition first;
  first.delay_ms = 10U;
  bbp::NetworkCondition second = first;
  second.delay_ms = 20U;

  bbp::PeerTopologyConfig topology;
  topology.kind = bbp::PeerTopologyKind::kCustomEdgeList;
  topology.edges = {
      {.from = 0U,
       .to = 1U,
       .bidirectional = false,
       .active = true,
       .latency_ms = 10U,
       .condition = first},
      {.from = 0U,
       .to = 1U,
       .bidirectional = false,
       .active = true,
       .latency_ms = 20U,
       .condition = second},
  };

  BOOST_CHECK_THROW(bbp::ResolvePeerTopologyEdges(topology, 2U),
                    std::runtime_error);
}

BOOST_AUTO_TEST_CASE(peer_topology_custom_edges_honor_active_and_directional) {
  bbp::PeerTopologyConfig topology;
  topology.kind = bbp::PeerTopologyKind::kCustomEdgeList;
  topology.edges = {
      {.from = 0U,
       .to = 1U,
       .bidirectional = false,
       .active = true,
       .latency_ms = std::nullopt},
      {.from = 1U,
       .to = 2U,
       .bidirectional = true,
       .active = true,
       .latency_ms = std::nullopt},
      {.from = 2U,
       .to = 3U,
       .bidirectional = true,
       .active = false,
       .latency_ms = std::nullopt},
  };
  const auto edges = bbp::ResolvePeerTopologyEdges(topology, 4U);
  BOOST_REQUIRE_EQUAL(edges.size(), 3U);
  BOOST_TEST(HasEdge(edges, 0U, 1U));
  BOOST_TEST(!HasEdge(edges, 1U, 0U));
  BOOST_TEST(HasEdge(edges, 1U, 2U));
  BOOST_TEST(HasEdge(edges, 2U, 1U));
  BOOST_TEST(!HasEdge(edges, 2U, 3U));
}

BOOST_AUTO_TEST_CASE(peer_topology_partitioned_groups_isolate_groups) {
  bbp::PeerTopologyConfig topology;
  topology.kind = bbp::PeerTopologyKind::kPartitionedGroups;
  topology.groups = {{0U, 2U}, {1U, 3U}};
  const auto edges = bbp::ResolvePeerTopologyEdges(topology, 4U);
  BOOST_REQUIRE_EQUAL(edges.size(), 4U);
  BOOST_TEST(HasEdge(edges, 0U, 2U));
  BOOST_TEST(HasEdge(edges, 2U, 0U));
  BOOST_TEST(!HasEdge(edges, 0U, 1U));

  topology.groups = {{0U, 2U}, {2U, 3U}};
  BOOST_CHECK_THROW(bbp::ResolvePeerTopologyEdges(topology, 4U),
                    std::runtime_error);
}

BOOST_AUTO_TEST_CASE(peer_topology_region_graph_uses_region_gateways) {
  bbp::PeerTopologyConfig topology;
  topology.kind = bbp::PeerTopologyKind::kInternetLikeRegionGraph;
  topology.regions = {{0U, 1U}, {2U, 3U}, {4U}};
  topology.region_edges = {
      {.from_region = 0U,
       .to_region = 1U,
       .bidirectional = true,
       .active = true},
      {.from_region = 1U,
       .to_region = 2U,
       .bidirectional = false,
       .active = true},
  };
  const auto edges = bbp::ResolvePeerTopologyEdges(topology, 5U);
  BOOST_TEST(HasEdge(edges, 0U, 1U));
  BOOST_TEST(HasEdge(edges, 0U, 2U));
  BOOST_TEST(HasEdge(edges, 2U, 0U));
  BOOST_TEST(HasEdge(edges, 2U, 4U));
  BOOST_TEST(!HasEdge(edges, 4U, 2U));
  BOOST_TEST(!HasEdge(edges, 1U, 3U));
}
