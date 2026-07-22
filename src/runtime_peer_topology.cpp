#include "bbp/runtime_peer_topology.h"

#include <algorithm>
#include <map>
#include <stdexcept>
#include <utility>

namespace bbp {
namespace {

using EdgeKey = std::pair<std::uint32_t, std::uint32_t>;

struct InitialEdgeState {
  bool active = true;
  std::optional<std::uint32_t> latency_ms = std::nullopt;
  std::optional<NetworkCondition> condition = std::nullopt;

  bool operator==(const InitialEdgeState&) const = default;
};

void ValidateInitialEdge(std::uint32_t from, std::uint32_t to,
                         std::uint32_t node_count,
                         const InitialEdgeState& state) {
  if (from >= node_count || to >= node_count) {
    throw std::runtime_error("runtime topology edge node is out of range");
  }
  if (from == to) {
    throw std::runtime_error("runtime topology must not contain self edges");
  }
  if (state.condition) {
    ValidateNetworkCondition(*state.condition);
    if (state.latency_ms && state.condition->delay_ms != *state.latency_ms) {
      throw std::runtime_error(
          "runtime topology latency and typed delay must match");
    }
  }
}

void AddInitialEdge(std::map<EdgeKey, InitialEdgeState>* edges,
                    std::uint32_t from, std::uint32_t to,
                    std::uint32_t node_count, const InitialEdgeState& state) {
  ValidateInitialEdge(from, to, node_count, state);
  const auto [entry, inserted] = edges->emplace(EdgeKey{from, to}, state);
  if (!inserted && entry->second != state) {
    throw std::runtime_error(
        "runtime topology contains conflicting duplicate edges");
  }
}

std::map<EdgeKey, InitialEdgeState> InitialEdges(
    const PeerTopologyConfig& topology, std::uint32_t node_count) {
  std::map<EdgeKey, InitialEdgeState> edges;
  if (topology.kind == PeerTopologyKind::kCustomEdgeList) {
    for (const PeerTopologyEdge& edge : topology.edges) {
      const InitialEdgeState state{.active = edge.active,
                                   .latency_ms = edge.latency_ms,
                                   .condition = edge.condition};
      AddInitialEdge(&edges, edge.from, edge.to, node_count, state);
      if (edge.bidirectional) {
        AddInitialEdge(&edges, edge.to, edge.from, node_count, state);
      }
    }
    return edges;
  }

  for (const ResolvedPeerTopologyEdge& edge :
       ResolvePeerTopologyEdges(topology, node_count)) {
    AddInitialEdge(&edges, edge.from, edge.to, node_count,
                   InitialEdgeState{.active = true,
                                    .latency_ms = edge.latency_ms,
                                    .condition = edge.condition});
  }
  if (topology.kind == PeerTopologyKind::kInternetLikeRegionGraph &&
      !topology.region_edges.empty()) {
    for (const PeerTopologyRegionEdge& edge : topology.region_edges) {
      if (edge.active) {
        continue;
      }
      const InitialEdgeState state{.active = false,
                                   .latency_ms = edge.latency_ms,
                                   .condition = edge.condition};
      const std::uint32_t from = topology.regions[edge.from_region].front();
      const std::uint32_t to = topology.regions[edge.to_region].front();
      AddInitialEdge(&edges, from, to, node_count, state);
      if (edge.bidirectional) {
        AddInitialEdge(&edges, to, from, node_count, state);
      }
    }
  }
  return edges;
}

}  // namespace

RuntimePeerTopology::RuntimePeerTopology(const PeerTopologyConfig& topology,
                                         std::uint32_t node_count,
                                         bool allow_empty)
    : node_count_(node_count) {
  if (node_count_ == 0U) {
    const bool default_empty_topology =
        topology.kind == PeerTopologyKind::kFullMesh && topology.seed == 0U &&
        topology.star_center == 0U && topology.average_degree == 0U &&
        topology.attachment_count == 0U && topology.edges.empty() &&
        topology.groups.empty() && topology.latency_matrix_ms.empty() &&
        topology.regions.empty() && topology.region_edges.empty();
    if (!allow_empty || !default_empty_topology) {
      throw std::runtime_error("runtime topology requires at least one node");
    }
    return;
  }
  std::vector<std::uint32_t> next_band(node_count_, 0U);
  for (const auto& [nodes, state] : InitialEdges(topology, node_count_)) {
    const std::uint32_t band = ++next_band[nodes.first];
    if (band > 15U) {
      throw std::runtime_error(
          "runtime topology exceeds the 15-peer directional band limit");
    }
    edges_.push_back(RuntimePeerTopologyEdge{
        .from = nodes.first,
        .to = nodes.second,
        .band = band,
        .active = state.active,
        .condition = state.condition,
    });
  }
  baseline_edges_ = edges_;
}

const std::vector<RuntimePeerTopologyEdge>& RuntimePeerTopology::edges() const {
  return edges_;
}

const RuntimePeerTopologyEdge& RuntimePeerTopology::Edge(
    std::uint32_t from, std::uint32_t to) const {
  const auto edge = std::find_if(
      edges_.begin(), edges_.end(), [from, to](const auto& candidate) {
        return candidate.from == from && candidate.to == to;
      });
  if (edge == edges_.end()) {
    throw std::runtime_error("runtime topology edge does not exist");
  }
  return *edge;
}

std::vector<std::uint32_t> RuntimePeerTopology::ActivePeerIndexes(
    std::uint32_t node_index) const {
  if (node_index >= node_count_) {
    throw std::out_of_range("runtime topology node index is out of range");
  }
  std::vector<std::uint32_t> peers;
  for (const RuntimePeerTopologyEdge& edge : edges_) {
    if (edge.from == node_index && edge.active) {
      peers.push_back(edge.to);
    }
  }
  return peers;
}

std::vector<DirectionalNetworkPolicy> RuntimePeerTopology::DirectionalPolicies(
    const SimulationNetworkAddressPlan& address_plan,
    std::uint32_t node_index) const {
  if (node_index >= node_count_) {
    throw std::out_of_range("runtime topology node index is out of range");
  }
  std::vector<DirectionalNetworkPolicy> policies;
  for (const RuntimePeerTopologyEdge& edge : edges_) {
    if (edge.from == node_index && edge.active && edge.condition) {
      policies.push_back(DirectionalNetworkPolicy{
          .band = edge.band,
          .destination_address = address_plan.NodeAddress(edge.to),
          .condition = *edge.condition,
      });
    }
  }
  return policies;
}

RuntimePeerTopologyEdge RuntimePeerTopology::SetCondition(
    std::uint32_t from, std::uint32_t to, NetworkCondition condition) {
  ValidateNetworkCondition(condition);
  RuntimePeerTopologyEdge& edge = MutableEdge(from, to);
  if (!edge.active) {
    throw std::runtime_error(
        "cannot set a condition on an inactive runtime topology edge");
  }
  const RuntimePeerTopologyEdge previous = edge;
  edge.condition = condition;
  return previous;
}

RuntimePeerTopologyEdge RuntimePeerTopology::SetActive(std::uint32_t from,
                                                       std::uint32_t to,
                                                       bool active) {
  RuntimePeerTopologyEdge& edge = MutableEdge(from, to);
  if (edge.active == active) {
    throw std::runtime_error(active
                                 ? "runtime topology edge is already active"
                                 : "runtime topology edge is already inactive");
  }
  const RuntimePeerTopologyEdge previous = edge;
  edge.active = active;
  return previous;
}

RuntimePeerTopologyEdge RuntimePeerTopology::RestoreBaseline(std::uint32_t from,
                                                             std::uint32_t to) {
  RuntimePeerTopologyEdge& edge = MutableEdge(from, to);
  const RuntimePeerTopologyEdge previous = edge;
  const auto baseline =
      std::find_if(baseline_edges_.begin(), baseline_edges_.end(),
                   [from, to](const auto& candidate) {
                     return candidate.from == from && candidate.to == to;
                   });
  if (baseline == baseline_edges_.end()) {
    throw std::runtime_error("runtime topology baseline edge does not exist");
  }
  edge = *baseline;
  return previous;
}

void RuntimePeerTopology::RestoreState(const RuntimePeerTopologyEdge& state) {
  RuntimePeerTopologyEdge& edge = MutableEdge(state.from, state.to);
  if (edge.band != state.band) {
    throw std::runtime_error("runtime topology restore band mismatch");
  }
  if (state.condition) {
    ValidateNetworkCondition(*state.condition);
  }
  edge = state;
}

RuntimePeerTopologyEdge& RuntimePeerTopology::MutableEdge(std::uint32_t from,
                                                          std::uint32_t to) {
  const auto edge = std::find_if(
      edges_.begin(), edges_.end(), [from, to](const auto& candidate) {
        return candidate.from == from && candidate.to == to;
      });
  if (edge == edges_.end()) {
    throw std::runtime_error("runtime topology edge does not exist");
  }
  return *edge;
}

}  // namespace bbp
