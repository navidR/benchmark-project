#pragma once

#include <cstdint>
#include <optional>
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

class RuntimePeerTopology {
 public:
  RuntimePeerTopology(const PeerTopologyConfig& topology,
                      std::uint32_t node_count);

  const std::vector<RuntimePeerTopologyEdge>& edges() const;
  const RuntimePeerTopologyEdge& Edge(std::uint32_t from,
                                      std::uint32_t to) const;
  std::vector<std::uint32_t> ActivePeerIndexes(std::uint32_t node_index) const;
  std::vector<DirectionalNetworkPolicy> DirectionalPolicies(
      const SimulationNetworkAddressPlan& address_plan,
      std::uint32_t node_index) const;

  RuntimePeerTopologyEdge SetCondition(std::uint32_t from, std::uint32_t to,
                                       NetworkCondition condition);
  RuntimePeerTopologyEdge SetActive(std::uint32_t from, std::uint32_t to,
                                    bool active);
  RuntimePeerTopologyEdge RestoreBaseline(std::uint32_t from, std::uint32_t to);
  void RestoreState(const RuntimePeerTopologyEdge& state);

 private:
  RuntimePeerTopologyEdge& MutableEdge(std::uint32_t from, std::uint32_t to);

  std::uint32_t node_count_ = 0;
  std::vector<RuntimePeerTopologyEdge> baseline_edges_;
  std::vector<RuntimePeerTopologyEdge> edges_;
};

}  // namespace bbp
