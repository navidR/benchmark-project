#include "bbp/default_peer_topology.h"

#include <algorithm>
#include <limits>
#include <map>
#include <random>
#include <set>
#include <stdexcept>
#include <string>
#include <utility>

namespace bbp {
namespace {

using EdgeKey = std::pair<std::uint32_t, std::uint32_t>;

struct EdgeAttributes {
  std::optional<std::uint32_t> latency_ms;
  std::optional<NetworkCondition> condition;

  bool operator==(const EdgeAttributes&) const = default;
};

class EdgeCollector {
 public:
  explicit EdgeCollector(std::uint32_t node_count) : node_count_(node_count) {}

  void Add(std::uint32_t from, std::uint32_t to,
           std::optional<std::uint32_t> latency_ms = std::nullopt,
           std::optional<NetworkCondition> condition = std::nullopt) {
    if (from >= node_count_ || to >= node_count_) {
      throw std::runtime_error("peer topology edge node is out of range");
    }
    if (from == to) {
      throw std::runtime_error("peer topology must not contain self edges");
    }
    if (condition) {
      ValidateNetworkCondition(*condition);
    }
    const EdgeAttributes attributes{.latency_ms = latency_ms,
                                    .condition = condition};
    const auto [entry, inserted] =
        edges_.emplace(EdgeKey{from, to}, attributes);
    if (!inserted && entry->second != attributes) {
      throw std::runtime_error(
          "peer topology contains conflicting duplicate edges");
    }
  }

  void AddBidirectional(
      std::uint32_t left, std::uint32_t right,
      std::optional<std::uint32_t> latency_ms = std::nullopt,
      std::optional<NetworkCondition> condition = std::nullopt) {
    Add(left, right, latency_ms, condition);
    Add(right, left, latency_ms, condition);
  }

  std::vector<ResolvedPeerTopologyEdge> Resolve() const {
    std::vector<ResolvedPeerTopologyEdge> result;
    result.reserve(edges_.size());
    for (const auto& [nodes, attributes] : edges_) {
      result.push_back(
          ResolvedPeerTopologyEdge{.from = nodes.first,
                                   .to = nodes.second,
                                   .latency_ms = attributes.latency_ms,
                                   .condition = attributes.condition});
    }
    return result;
  }

 private:
  std::uint32_t node_count_ = 0;
  std::map<EdgeKey, EdgeAttributes> edges_;
};

void RequireNodeCount(std::uint32_t node_count) {
  if (node_count == 0U) {
    throw std::runtime_error(
        "peer topology node count must be greater than zero");
  }
}

void AddFullMesh(const std::vector<std::uint32_t>& nodes,
                 EdgeCollector* edges) {
  for (std::uint32_t from : nodes) {
    for (std::uint32_t to : nodes) {
      if (from != to) {
        edges->Add(from, to);
      }
    }
  }
}

std::vector<std::uint32_t> AllNodes(std::uint32_t node_count) {
  std::vector<std::uint32_t> nodes;
  nodes.reserve(node_count);
  for (std::uint32_t node = 0; node < node_count; ++node) {
    nodes.push_back(node);
  }
  return nodes;
}

void RequireExactNodePartition(
    const std::vector<std::vector<std::uint32_t>>& groups,
    std::uint32_t node_count, std::string_view field) {
  if (groups.empty()) {
    throw std::runtime_error(std::string(field) + " must not be empty");
  }
  std::vector<bool> seen(node_count, false);
  for (const std::vector<std::uint32_t>& group : groups) {
    if (group.empty()) {
      throw std::runtime_error(std::string(field) +
                               " must not contain empty groups");
    }
    for (std::uint32_t node : group) {
      if (node >= node_count) {
        throw std::runtime_error(std::string(field) +
                                 " contains an out-of-range node");
      }
      if (seen[node]) {
        throw std::runtime_error(std::string(field) +
                                 " contains a duplicate node");
      }
      seen[node] = true;
    }
  }
  if (std::find(seen.begin(), seen.end(), false) != seen.end()) {
    throw std::runtime_error(std::string(field) +
                             " must assign every simulated node");
  }
}

std::uint64_t BoundedRandom(std::mt19937_64* random,
                            std::uint64_t upper_bound) {
  if (upper_bound == 0U) {
    throw std::runtime_error("random topology bound must be positive");
  }
  const std::uint64_t rejection_threshold =
      (std::numeric_limits<std::uint64_t>::max() - upper_bound + 1U) %
      upper_bound;
  while (true) {
    const std::uint64_t value = (*random)();
    if (value >= rejection_threshold) {
      return value % upper_bound;
    }
  }
}

void AddRandomGraph(const PeerTopologyConfig& topology,
                    std::uint32_t node_count, EdgeCollector* edges) {
  if (topology.average_degree >= node_count) {
    throw std::runtime_error(
        "random graph average_degree must be less than node count");
  }
  std::vector<EdgeKey> candidates;
  const std::uint64_t candidate_count =
      (static_cast<std::uint64_t>(node_count) * (node_count - 1U)) / 2U;
  if (candidate_count > std::numeric_limits<std::size_t>::max()) {
    throw std::runtime_error("random graph edge count exceeds size_t");
  }
  candidates.reserve(static_cast<std::size_t>(candidate_count));
  for (std::uint32_t left = 0; left < node_count; ++left) {
    for (std::uint32_t right = left + 1U; right < node_count; ++right) {
      candidates.emplace_back(left, right);
    }
  }

  std::mt19937_64 random(topology.seed);
  for (std::size_t remaining = candidates.size(); remaining > 1U; --remaining) {
    const std::uint64_t selected =
        BoundedRandom(&random, static_cast<std::uint64_t>(remaining));
    std::swap(candidates[remaining - 1U],
              candidates[static_cast<std::size_t>(selected)]);
  }
  const std::uint64_t desired_twice =
      static_cast<std::uint64_t>(node_count) * topology.average_degree;
  const std::uint64_t desired_edges = desired_twice / 2U;
  for (std::size_t index = 0;
       index < candidates.size() && index < desired_edges; ++index) {
    edges->AddBidirectional(candidates[index].first, candidates[index].second);
  }
}

void AddScaleFreeGraph(const PeerTopologyConfig& topology,
                       std::uint32_t node_count, EdgeCollector* edges) {
  if (node_count == 1U) {
    return;
  }
  if (topology.average_degree >= node_count && topology.average_degree != 0U) {
    throw std::runtime_error(
        "scale-free graph average_degree must be less than node count");
  }
  std::uint32_t attachment_count = topology.attachment_count;
  if (attachment_count == 0U) {
    if (topology.average_degree == 0U) {
      throw std::runtime_error(
          "scale-free graph requires attachment_count or average_degree");
    }
    attachment_count = std::max(1U, topology.average_degree / 2U);
  }
  if (attachment_count >= node_count) {
    throw std::runtime_error(
        "scale-free graph attachment_count must be less than node count");
  }

  const std::uint32_t initial_count = attachment_count + 1U;
  std::vector<std::uint64_t> degrees(node_count, 0U);
  for (std::uint32_t left = 0; left < initial_count; ++left) {
    for (std::uint32_t right = left + 1U; right < initial_count; ++right) {
      edges->AddBidirectional(left, right);
      ++degrees[left];
      ++degrees[right];
    }
  }

  std::mt19937_64 random(topology.seed);
  for (std::uint32_t node = initial_count; node < node_count; ++node) {
    std::set<std::uint32_t> selected;
    while (selected.size() < attachment_count) {
      std::uint64_t total_weight = 0U;
      for (std::uint32_t candidate = 0; candidate < node; ++candidate) {
        if (!selected.contains(candidate)) {
          if (degrees[candidate] >
              std::numeric_limits<std::uint64_t>::max() - total_weight) {
            throw std::runtime_error("scale-free graph degree weight overflow");
          }
          total_weight += degrees[candidate];
        }
      }
      if (total_weight == 0U) {
        throw std::runtime_error(
            "scale-free graph has no weighted attachment candidate");
      }
      std::uint64_t draw = BoundedRandom(&random, total_weight);
      for (std::uint32_t candidate = 0; candidate < node; ++candidate) {
        if (selected.contains(candidate)) {
          continue;
        }
        if (draw < degrees[candidate]) {
          selected.insert(candidate);
          break;
        }
        draw -= degrees[candidate];
      }
    }
    for (std::uint32_t candidate : selected) {
      edges->AddBidirectional(node, candidate);
      ++degrees[node];
      ++degrees[candidate];
    }
  }
}

void AddConfiguredEdge(const PeerTopologyEdge& edge, std::uint32_t node_count,
                       EdgeCollector* edges) {
  if (edge.from >= node_count || edge.to >= node_count) {
    throw std::runtime_error("peer topology edge node is out of range");
  }
  if (edge.from == edge.to) {
    throw std::runtime_error("peer topology must not contain self edges");
  }
  if (!edge.active) {
    return;
  }
  edges->Add(edge.from, edge.to, edge.latency_ms, edge.condition);
  if (edge.bidirectional) {
    edges->Add(edge.to, edge.from, edge.latency_ms, edge.condition);
  }
}

}  // namespace

std::vector<std::uint32_t> DefaultStartupPeerIndexes(std::uint32_t node_count,
                                                     std::uint32_t node_index) {
  if (node_count == 0U || node_index >= node_count) {
    throw std::runtime_error("default peer topology node is out of range");
  }

  std::vector<std::uint32_t> peers;
  peers.reserve(node_count - 1U);
  for (std::uint32_t peer_index = 0; peer_index < node_count; ++peer_index) {
    if (peer_index != node_index) {
      peers.push_back(peer_index);
    }
  }
  return peers;
}

std::vector<ResolvedPeerTopologyEdge> ResolvePeerTopologyEdges(
    const PeerTopologyConfig& topology, std::uint32_t node_count) {
  RequireNodeCount(node_count);
  EdgeCollector edges(node_count);
  switch (topology.kind) {
    case PeerTopologyKind::kFullMesh:
      AddFullMesh(AllNodes(node_count), &edges);
      break;
    case PeerTopologyKind::kRing:
      if (node_count > 1U) {
        for (std::uint32_t node = 0; node < node_count; ++node) {
          edges.AddBidirectional(node, (node + 1U) % node_count);
        }
      }
      break;
    case PeerTopologyKind::kStar:
      if (topology.star_center >= node_count) {
        throw std::runtime_error("star topology center node is out of range");
      }
      for (std::uint32_t node = 0; node < node_count; ++node) {
        if (node != topology.star_center) {
          edges.AddBidirectional(topology.star_center, node);
        }
      }
      break;
    case PeerTopologyKind::kRandomGraph:
      AddRandomGraph(topology, node_count, &edges);
      break;
    case PeerTopologyKind::kScaleFreeGraph:
      AddScaleFreeGraph(topology, node_count, &edges);
      break;
    case PeerTopologyKind::kLatencyMatrix:
      if (topology.latency_matrix_ms.size() != node_count) {
        throw std::runtime_error(
            "latency matrix row count must match node count");
      }
      for (std::uint32_t from = 0; from < node_count; ++from) {
        const auto& row = topology.latency_matrix_ms[from];
        if (row.size() != node_count) {
          throw std::runtime_error(
              "latency matrix column count must match node count");
        }
        for (std::uint32_t to = 0; to < node_count; ++to) {
          if (from == to) {
            if (row[to] && *row[to] != 0U) {
              throw std::runtime_error(
                  "latency matrix diagonal must be null or zero");
            }
          } else if (row[to]) {
            NetworkCondition condition;
            condition.delay_ms = *row[to];
            edges.Add(from, to, row[to], condition);
          }
        }
      }
      break;
    case PeerTopologyKind::kCustomEdgeList:
      for (const PeerTopologyEdge& edge : topology.edges) {
        AddConfiguredEdge(edge, node_count, &edges);
      }
      break;
    case PeerTopologyKind::kPartitionedGroups:
      RequireExactNodePartition(topology.groups, node_count,
                                "peer topology groups");
      for (const std::vector<std::uint32_t>& group : topology.groups) {
        AddFullMesh(group, &edges);
      }
      break;
    case PeerTopologyKind::kInternetLikeRegionGraph:
      RequireExactNodePartition(topology.regions, node_count,
                                "peer topology regions");
      for (const std::vector<std::uint32_t>& region : topology.regions) {
        AddFullMesh(region, &edges);
      }
      if (topology.region_edges.empty()) {
        for (std::uint32_t left = 0; left < topology.regions.size(); ++left) {
          for (std::uint32_t right = left + 1U; right < topology.regions.size();
               ++right) {
            edges.AddBidirectional(topology.regions[left].front(),
                                   topology.regions[right].front());
          }
        }
      } else {
        for (const PeerTopologyRegionEdge& edge : topology.region_edges) {
          if (edge.from_region >= topology.regions.size() ||
              edge.to_region >= topology.regions.size()) {
            throw std::runtime_error(
                "region topology edge region is out of range");
          }
          if (edge.from_region == edge.to_region) {
            throw std::runtime_error(
                "region topology must not contain self edges");
          }
          if (!edge.active) {
            continue;
          }
          edges.Add(topology.regions[edge.from_region].front(),
                    topology.regions[edge.to_region].front(), edge.latency_ms,
                    edge.condition);
          if (edge.bidirectional) {
            edges.Add(topology.regions[edge.to_region].front(),
                      topology.regions[edge.from_region].front(),
                      edge.latency_ms, edge.condition);
          }
        }
      }
      break;
    case PeerTopologyKind::kCount:
      throw std::logic_error("unknown peer topology kind");
  }
  return edges.Resolve();
}

std::vector<std::uint32_t> ResolvePeerTopologyPeerIndexes(
    const PeerTopologyConfig& topology, std::uint32_t node_count,
    std::uint32_t node_index) {
  if (node_index >= node_count) {
    throw std::runtime_error("peer topology node is out of range");
  }
  std::vector<std::uint32_t> peers;
  for (const ResolvedPeerTopologyEdge& edge :
       ResolvePeerTopologyEdges(topology, node_count)) {
    if (edge.from == node_index) {
      peers.push_back(edge.to);
    }
  }
  return peers;
}

}  // namespace bbp
