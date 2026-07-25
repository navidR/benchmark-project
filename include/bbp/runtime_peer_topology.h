#pragma once

#include <cstdint>
#include <map>
#include <optional>
#include <string>
#include <vector>

#include "bbp/default_peer_topology.h"
#include "bbp/simulation_network_address_plan.h"

namespace bbp {

struct RuntimePeerTopologyEdge {
  std::uint32_t from = 0;
  std::uint32_t to = 0;
  std::uint32_t band = 0;
  bool active = true;
  std::optional<NetworkCondition> condition = std::nullopt;

  bool operator==(const RuntimePeerTopologyEdge&) const = default;
};

struct RuntimeNodeReplacementPeerEdge {
  std::uint32_t from = 0;
  std::uint32_t to = 0;

  bool operator==(const RuntimeNodeReplacementPeerEdge&) const = default;
};

class RuntimePeerTopology {
 public:
  RuntimePeerTopology(const PeerTopologyConfig& topology,
                      std::uint32_t node_count, bool allow_empty = false);

  const std::vector<RuntimePeerTopologyEdge>& edges() const;
  const RuntimePeerTopologyEdge& Edge(std::uint32_t from,
                                      std::uint32_t to) const;
  std::vector<std::uint32_t> ActivePeerIndexes(std::uint32_t node_index) const;
  bool PhysicalPeerRequired(std::uint32_t first, std::uint32_t second) const;
  bool PreservesPhysicalPeerRequirementsFrom(
      const RuntimePeerTopology& previous,
      std::uint32_t common_node_count) const;
  std::vector<DirectionalNetworkPolicy> DirectionalPolicies(
      const SimulationNetworkAddressPlan& address_plan,
      std::uint32_t node_index) const;

  RuntimePeerTopologyEdge SetCondition(std::uint32_t from, std::uint32_t to,
                                       NetworkCondition condition);
  RuntimePeerTopologyEdge SetActive(std::uint32_t from, std::uint32_t to,
                                    bool active);
  RuntimePeerTopologyEdge RestoreBaseline(std::uint32_t from, std::uint32_t to);
  void RestoreState(const RuntimePeerTopologyEdge& state);
  void PreserveCommonStateFrom(const RuntimePeerTopology& previous);
  void PreserveRemappedStateFrom(
      const RuntimePeerTopology& previous,
      const std::vector<std::optional<std::uint32_t>>& old_to_new);

 private:
  RuntimePeerTopologyEdge& MutableEdge(std::uint32_t from, std::uint32_t to);

  std::uint32_t node_count_ = 0;
  std::vector<RuntimePeerTopologyEdge> baseline_edges_;
  std::vector<RuntimePeerTopologyEdge> edges_;
};

std::vector<RuntimeNodeReplacementPeerEdge>
ResolveRuntimeNodeReplacementPeerEdges(
    const RuntimePeerTopology& topology,
    const std::vector<std::string>& node_ids,
    const std::map<std::string, std::vector<std::string>>& allowed_peers,
    std::uint32_t target_index);

}  // namespace bbp
