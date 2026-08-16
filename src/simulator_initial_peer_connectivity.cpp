#include "simulator_initial_peer_connectivity.h"

#include <cstdint>
#include <map>
#include <set>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

#include "bbp/runtime_node_inventory.h"
#include "bbp/runtime_peer_topology.h"
#include "bbp/simulator/options.h"

namespace bbp::simulator_app_internal {

std::map<std::string, PeerCountPolicy> InitialPeerCountPolicies(
    const Options& options, const RuntimeNodeSnapshot& nodes) {
  std::map<std::string, PeerCountPolicy> policies;
  for (const PeerConnectivityPolicy& policy :
       options.topology.peer_connectivity) {
    if (policy.node >= nodes.size()) {
      throw std::runtime_error(
          "peer connectivity policy references an unknown node");
    }
    policies.emplace(nodes[policy.node].config.id, policy.peer_count);
  }
  return policies;
}

std::set<std::string> InitialAllPeerPolicyNodeIds(
    const Options& options, const RuntimeNodeSnapshot& nodes) {
  std::set<std::string> node_ids;
  for (const PeerConnectivityPolicy& policy :
       options.topology.peer_connectivity) {
    if (policy.mode != PeerConnectivityMode::kAllPeers) {
      continue;
    }
    if (policy.node >= nodes.size()) {
      throw std::runtime_error("all-peers policy references an unknown node");
    }
    node_ids.insert(nodes[policy.node].config.id);
  }
  return node_ids;
}

PeerConnectivityController::AllowedPeerMap InitialAllowedPeers(
    const RuntimePeerTopology& topology, const RuntimeNodeSnapshot& nodes) {
  PeerConnectivityController::AllowedPeerMap allowed;
  for (std::uint32_t node_index = 0; node_index < nodes.size(); ++node_index) {
    std::vector<std::string> peer_ids;
    for (const std::uint32_t peer_index :
         topology.ActivePeerIndexes(node_index)) {
      if (peer_index >= nodes.size()) {
        throw std::runtime_error(
            "logical topology peer references an unknown running node");
      }
      peer_ids.push_back(nodes[peer_index].config.id);
    }
    allowed.emplace(nodes[node_index].config.id, std::move(peer_ids));
  }
  return allowed;
}

}  // namespace bbp::simulator_app_internal
