#pragma once

#include <cstdint>
#include <optional>
#include <string_view>
#include <vector>

#include "bbp/network.h"

namespace bbp {

enum class PeerTopologyKind {
  kFullMesh,
  kRing,
  kStar,
  kRandomGraph,
  kScaleFreeGraph,
  kLatencyMatrix,
  kCustomEdgeList,
  kPartitionedGroups,
  kInternetLikeRegionGraph,
};

constexpr std::string_view PeerTopologyKindName(PeerTopologyKind kind) {
  switch (kind) {
    case PeerTopologyKind::kFullMesh:
      return "full_mesh";
    case PeerTopologyKind::kRing:
      return "ring";
    case PeerTopologyKind::kStar:
      return "star";
    case PeerTopologyKind::kRandomGraph:
      return "random_graph";
    case PeerTopologyKind::kScaleFreeGraph:
      return "scale_free_graph";
    case PeerTopologyKind::kLatencyMatrix:
      return "latency_matrix";
    case PeerTopologyKind::kCustomEdgeList:
      return "custom_edge_list";
    case PeerTopologyKind::kPartitionedGroups:
      return "partitioned_groups";
    case PeerTopologyKind::kInternetLikeRegionGraph:
      return "internet_like_region_graph";
  }
  return "unknown";
}

constexpr std::optional<PeerTopologyKind> PeerTopologyKindFromName(
    std::string_view name) {
  if (name == "full_mesh") {
    return PeerTopologyKind::kFullMesh;
  }
  if (name == "ring") {
    return PeerTopologyKind::kRing;
  }
  if (name == "star") {
    return PeerTopologyKind::kStar;
  }
  if (name == "random_graph") {
    return PeerTopologyKind::kRandomGraph;
  }
  if (name == "scale_free_graph") {
    return PeerTopologyKind::kScaleFreeGraph;
  }
  if (name == "latency_matrix") {
    return PeerTopologyKind::kLatencyMatrix;
  }
  if (name == "custom_edge_list") {
    return PeerTopologyKind::kCustomEdgeList;
  }
  if (name == "partitioned_groups") {
    return PeerTopologyKind::kPartitionedGroups;
  }
  if (name == "internet_like_region_graph" || name == "region_graph") {
    return PeerTopologyKind::kInternetLikeRegionGraph;
  }
  return std::nullopt;
}

struct PeerTopologyEdge {
  std::uint32_t from = 0;
  std::uint32_t to = 0;
  bool bidirectional = true;
  bool active = true;
  std::optional<std::uint32_t> latency_ms = std::nullopt;
  std::optional<NetworkCondition> condition = std::nullopt;
};

struct PeerTopologyRegionEdge {
  std::uint32_t from_region = 0;
  std::uint32_t to_region = 0;
  bool bidirectional = true;
  bool active = true;
};

struct PeerTopologyConfig {
  PeerTopologyKind kind = PeerTopologyKind::kFullMesh;
  std::uint64_t seed = 0;
  std::uint32_t star_center = 0;
  std::uint32_t average_degree = 0;
  std::uint32_t attachment_count = 0;
  std::vector<PeerTopologyEdge> edges;
  std::vector<std::vector<std::uint32_t>> groups;
  std::vector<std::vector<std::optional<std::uint32_t>>> latency_matrix_ms;
  std::vector<std::vector<std::uint32_t>> regions;
  std::vector<PeerTopologyRegionEdge> region_edges;
};

struct ResolvedPeerTopologyEdge {
  std::uint32_t from = 0;
  std::uint32_t to = 0;
  std::optional<std::uint32_t> latency_ms = std::nullopt;
  std::optional<NetworkCondition> condition = std::nullopt;

  bool operator==(const ResolvedPeerTopologyEdge&) const = default;
};

std::vector<std::uint32_t> DefaultStartupPeerIndexes(std::uint32_t node_count,
                                                     std::uint32_t node_index);

std::vector<ResolvedPeerTopologyEdge> ResolvePeerTopologyEdges(
    const PeerTopologyConfig& topology, std::uint32_t node_count);

std::vector<std::uint32_t> ResolvePeerTopologyPeerIndexes(
    const PeerTopologyConfig& topology, std::uint32_t node_count,
    std::uint32_t node_index);

}  // namespace bbp
