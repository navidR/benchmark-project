#include "simulator_runtime_topology_publication_planning.h"

#include <cstdint>
#include <stdexcept>
#include <string>
#include <vector>

#include "bbp/drivers/chain_driver.h"
#include "bbp/network.h"
#include "bbp/peer_connectivity_policy.h"
#include "bbp/runtime_peer_topology.h"
#include "bbp/simulation_network_address_plan.h"
#include "bbp/simulation_registry.h"
#include "simulator_network_launch_planning.h"

namespace bbp::simulator_app_internal {

std::vector<DirectionalNetworkPolicy> DynamicDirectionalNetworkPolicies(
    const RuntimePeerTopology& topology,
    const SimulationNetworkAddressPlan& address_plan,
    const std::vector<std::uint32_t>& resource_slots,
    std::uint32_t node_index) {
  std::vector<DirectionalNetworkPolicy> policies;
  for (const RuntimePeerTopologyEdge& edge : topology.edges()) {
    if (edge.from == node_index && edge.active && edge.condition) {
      policies.push_back(DirectionalNetworkPolicy{
          .band = edge.band,
          .destination_address =
              address_plan.NodeAddress(resource_slots.at(edge.to)),
          .condition = *edge.condition,
      });
    }
  }
  return policies;
}

std::vector<std::string> DynamicTopologyPeerIds(
    const RuntimePeerTopology& topology,
    const std::vector<ChainNodeConfig>& configs, std::uint32_t node_index) {
  std::vector<std::string> peers;
  for (const std::uint32_t peer_index :
       topology.ActivePeerIndexes(node_index)) {
    peers.push_back(configs.at(peer_index).id);
  }
  return peers;
}

std::vector<std::string> DynamicRestartPeerEndpoints(
    const NodeRoleTopology& role_topology, const RuntimePeerTopology& topology,
    const std::vector<ChainNodeConfig>& configs, std::uint32_t node_index) {
  std::vector<std::uint32_t> peer_indexes =
      topology.ActivePeerIndexes(node_index);
  const PeerConnectivityPolicy* policy =
      FindPeerConnectivityPolicy(role_topology, node_index);
  if (policy != nullptr) {
    const std::uint32_t initial_peer_count = policy->peer_count.minimum();
    if (initial_peer_count > peer_indexes.size()) {
      throw std::runtime_error(
          "initial peer count exceeds eligible logical topology peers");
    }
    peer_indexes.resize(initial_peer_count);
  }
  std::vector<std::string> peers;
  peers.reserve(peer_indexes.size());
  for (const std::uint32_t peer_index : peer_indexes) {
    const ChainNodeConfig& peer = configs.at(peer_index);
    peers.push_back(peer.p2p_host + ":" + std::to_string(peer.p2p_port));
  }
  return peers;
}

}  // namespace bbp::simulator_app_internal
