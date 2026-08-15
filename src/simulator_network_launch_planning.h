#pragma once

#include <cstdint>
#include <stop_token>
#include <string>
#include <vector>

namespace bbp {

struct ChainDriverSpec;
struct DirectionalNetworkPolicy;
struct NodeRoleTopology;
struct NodeVethConfig;
struct Options;
struct PeerConnectivityPolicy;
struct RunOwnership;
class RuntimePeerTopology;
class SimulationNetworkAddressPlan;

namespace simulator_app_internal {

const SimulationNetworkAddressPlan& NetworkAddressPlan(const Options& options);
const RunOwnership& RequireRunOwnership(const Options& options);
const PeerConnectivityPolicy* FindPeerConnectivityPolicy(
    const NodeRoleTopology& topology, std::uint32_t node_index);
void RequireRunNetworkInterfacesAvailable(const Options& options,
                                          std::stop_token stop_token);
NodeVethConfig MakeNodeVethConfig(const Options& options,
                                  std::uint32_t node_index);
std::vector<DirectionalNetworkPolicy> DirectionalNetworkPoliciesForNode(
    const Options& options, const RuntimePeerTopology& runtime_topology,
    std::uint32_t node_index);
std::vector<std::string> StartupPeerAddresses(
    const Options& options, const RuntimePeerTopology& topology,
    const ChainDriverSpec& chain_spec, std::uint32_t node_index);
bool HostIpv4ForwardingEnabled();

}  // namespace simulator_app_internal
}  // namespace bbp
