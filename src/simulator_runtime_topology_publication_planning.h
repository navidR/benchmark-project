#pragma once

#include <cstdint>
#include <string>
#include <vector>

namespace bbp {

class RuntimePeerTopology;
class SimulationNetworkAddressPlan;
struct ChainNodeConfig;
struct DirectionalNetworkPolicy;
struct NodeRoleTopology;

namespace simulator_app_internal {

std::vector<DirectionalNetworkPolicy> DynamicDirectionalNetworkPolicies(
    const RuntimePeerTopology& topology,
    const SimulationNetworkAddressPlan& address_plan,
    const std::vector<std::uint32_t>& resource_slots, std::uint32_t node_index);

std::vector<std::string> DynamicTopologyPeerIds(
    const RuntimePeerTopology& topology,
    const std::vector<ChainNodeConfig>& configs, std::uint32_t node_index);

std::vector<std::string> DynamicRestartPeerEndpoints(
    const NodeRoleTopology& role_topology, const RuntimePeerTopology& topology,
    const std::vector<ChainNodeConfig>& configs, std::uint32_t node_index);

}  // namespace simulator_app_internal
}  // namespace bbp
