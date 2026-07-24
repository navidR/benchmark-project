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

PeerTopologyConfig RemapPeerTopologyConfig(
    const PeerTopologyConfig& topology,
    const std::vector<std::optional<std::uint32_t>>& old_to_new) {
  std::uint32_t next_node_count = 0U;
  std::vector<bool> seen(old_to_new.size(), false);
  for (const std::optional<std::uint32_t> mapped : old_to_new) {
    if (!mapped) {
      continue;
    }
    if (*mapped >= old_to_new.size() || seen[*mapped]) {
      throw std::invalid_argument(
          "peer topology node remap must contain unique compact indexes");
    }
    seen[*mapped] = true;
    next_node_count = std::max(next_node_count, *mapped + 1U);
  }
  if (std::find(seen.begin(), seen.begin() + next_node_count, false) !=
      seen.begin() + next_node_count) {
    throw std::invalid_argument(
        "peer topology node remap must contain unique compact indexes");
  }
  if (next_node_count == 0U) {
    return PeerTopologyConfig{};
  }

  PeerTopologyConfig result = topology;
  const auto map_node =
      [&](std::uint32_t node) -> std::optional<std::uint32_t> {
    if (node >= old_to_new.size()) {
      throw std::invalid_argument(
          "peer topology contains an out-of-range node during remap");
    }
    return old_to_new[node];
  };

  if (result.kind == PeerTopologyKind::kStar) {
    const std::optional<std::uint32_t> center = map_node(result.star_center);
    result.star_center = center.value_or(0U);
  } else if (result.kind == PeerTopologyKind::kRandomGraph) {
    result.average_degree =
        std::min(result.average_degree, next_node_count - 1U);
  } else if (result.kind == PeerTopologyKind::kScaleFreeGraph &&
             next_node_count > 1U) {
    if (result.average_degree != 0U) {
      result.average_degree =
          std::min(result.average_degree, next_node_count - 1U);
    }
    if (result.attachment_count != 0U) {
      result.attachment_count =
          std::min(result.attachment_count, next_node_count - 1U);
    }
  }

  if (result.kind == PeerTopologyKind::kCustomEdgeList) {
    result.edges.clear();
    for (const PeerTopologyEdge& edge : topology.edges) {
      const std::optional<std::uint32_t> from = map_node(edge.from);
      const std::optional<std::uint32_t> to = map_node(edge.to);
      if (!from || !to) {
        continue;
      }
      PeerTopologyEdge remapped = edge;
      remapped.from = *from;
      remapped.to = *to;
      result.edges.push_back(std::move(remapped));
    }
  }

  const auto remap_groups =
      [&](const std::vector<std::vector<std::uint32_t>>& groups) {
        std::vector<std::vector<std::uint32_t>> remapped;
        remapped.reserve(groups.size());
        for (const std::vector<std::uint32_t>& group : groups) {
          std::vector<std::uint32_t> members;
          members.reserve(group.size());
          for (const std::uint32_t node : group) {
            if (const std::optional<std::uint32_t> mapped = map_node(node)) {
              members.push_back(*mapped);
            }
          }
          if (!members.empty()) {
            remapped.push_back(std::move(members));
          }
        }
        return remapped;
      };
  if (result.kind == PeerTopologyKind::kPartitionedGroups) {
    result.groups = remap_groups(topology.groups);
  }

  if (result.kind == PeerTopologyKind::kLatencyMatrix) {
    std::vector<std::uint32_t> retained_old_nodes(next_node_count);
    for (std::uint32_t old = 0U; old < old_to_new.size(); ++old) {
      if (old_to_new[old]) {
        retained_old_nodes[*old_to_new[old]] = old;
      }
    }
    result.latency_matrix_ms.assign(
        next_node_count,
        std::vector<std::optional<std::uint32_t>>(next_node_count));
    for (std::uint32_t from = 0U; from < next_node_count; ++from) {
      const std::uint32_t old_from = retained_old_nodes[from];
      if (old_from >= topology.latency_matrix_ms.size()) {
        throw std::invalid_argument(
            "peer topology latency matrix is incomplete during remap");
      }
      for (std::uint32_t to = 0U; to < next_node_count; ++to) {
        const std::uint32_t old_to = retained_old_nodes[to];
        if (old_to >= topology.latency_matrix_ms[old_from].size()) {
          throw std::invalid_argument(
              "peer topology latency matrix is incomplete during remap");
        }
        result.latency_matrix_ms[from][to] =
            topology.latency_matrix_ms[old_from][old_to];
      }
    }
  }

  if (result.kind == PeerTopologyKind::kInternetLikeRegionGraph) {
    result.regions.clear();
    std::vector<std::optional<std::uint32_t>> region_remap(
        topology.regions.size());
    for (std::uint32_t region = 0U; region < topology.regions.size();
         ++region) {
      std::vector<std::uint32_t> members;
      members.reserve(topology.regions[region].size());
      for (const std::uint32_t node : topology.regions[region]) {
        if (const std::optional<std::uint32_t> mapped = map_node(node)) {
          members.push_back(*mapped);
        }
      }
      if (!members.empty()) {
        region_remap[region] =
            static_cast<std::uint32_t>(result.regions.size());
        result.regions.push_back(std::move(members));
      }
    }
    result.region_edges.clear();
    for (const PeerTopologyRegionEdge& edge : topology.region_edges) {
      if (edge.from_region >= region_remap.size() ||
          edge.to_region >= region_remap.size()) {
        throw std::invalid_argument(
            "peer topology region edge is out of range during remap");
      }
      if (!region_remap[edge.from_region] || !region_remap[edge.to_region]) {
        continue;
      }
      PeerTopologyRegionEdge remapped = edge;
      remapped.from_region = *region_remap[edge.from_region];
      remapped.to_region = *region_remap[edge.to_region];
      result.region_edges.push_back(std::move(remapped));
    }
  }
  return result;
}

}  // namespace bbp
