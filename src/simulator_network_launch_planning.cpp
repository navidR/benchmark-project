#include "simulator_network_launch_planning.h"

#include <algorithm>
#include <stdexcept>
#include <string>
#include <vector>

#include "bbp/drivers/chain_driver_registry.h"
#include "bbp/network.h"
#include "bbp/peer_connectivity_policy.h"
#include "bbp/runtime_peer_topology.h"
#include "bbp/simulator/options.h"
#include "bbp/util.h"

namespace bbp::simulator_app_internal {

const PeerConnectivityPolicy* FindPeerConnectivityPolicy(
    const NodeRoleTopology& topology, uint32_t node_index) {
  for (const PeerConnectivityPolicy& policy : topology.peer_connectivity) {
    if (policy.node == node_index) {
      return &policy;
    }
  }
  return nullptr;
}

const SimulationNetworkAddressPlan& NetworkAddressPlan(const Options& options) {
  if (!options.network_address_plan) {
    throw std::logic_error(
        "isolated simulation network address plan is not initialized");
  }
  return *options.network_address_plan;
}

const RunOwnership& RequireRunOwnership(const Options& options) {
  if (!options.run_ownership) {
    throw std::logic_error("run ownership is not initialized");
  }
  return *options.run_ownership;
}

void RequireRunNetworkInterfacesAvailable(const Options& options,
                                          std::stop_token stop_token) {
  const std::vector<LinkInfo> links = ListNetworkLinks(stop_token);
  for (std::uint32_t node_index = 0; node_index < options.node_capacity;
       ++node_index) {
    for (const char suffix : {'h', 'p'}) {
      const std::string name =
          RunInterfaceName(RequireRunOwnership(options), node_index, suffix);
      const auto collision =
          std::find_if(links.begin(), links.end(),
                       [&](const LinkInfo& link) { return link.name == name; });
      if (collision != links.end()) {
        throw std::runtime_error(
            "isolated simulation network interface collision: " + name);
      }
    }
  }
}

NodeVethConfig MakeNodeVethConfig(const Options& options, uint32_t node_index) {
  NodeVethConfig config;
  const RunOwnership& ownership = RequireRunOwnership(options);
  config.host_name = RunInterfaceName(ownership, node_index, 'h');
  config.peer_name = RunInterfaceName(ownership, node_index, 'p');
  config.host_ownership_alias = RunInterfaceAlias(ownership, node_index, 'h');
  config.peer_ownership_alias = RunInterfaceAlias(ownership, node_index, 'p');
  config.host_address = NetworkAddressPlan(options).HostAddress(node_index);
  config.node_address = NetworkAddressPlan(options).NodeAddress(node_index);
  config.prefix_len = NetworkAddressPlan(options).NodePrefixLength();
  const auto node_condition = options.node_network_conditions.find(node_index);
  if (node_condition != options.node_network_conditions.end()) {
    config.apply_condition = true;
    config.condition = node_condition->second;
  } else {
    config.apply_condition = options.network_condition_requested;
    config.condition = options.network_condition;
  }
  return config;
}

namespace {

std::string PeerHost(const Options& options, uint32_t node_index) {
  if (options.isolate_network) {
    return NetworkAddressPlan(options).NodeAddress(node_index);
  }
  return "127.0.0.1";
}

std::string StartupPeerAddress(const Options& options,
                               const ChainDriverSpec& chain_spec,
                               uint32_t node_index) {
  return PeerHost(options, node_index) + ":" +
         std::to_string(static_cast<uint32_t>(chain_spec.p2p_port_base) +
                        node_index);
}

std::vector<uint32_t> ConfiguredStartupPeerIndexes(
    const Options& options, const RuntimePeerTopology& runtime_topology,
    uint32_t node_index) {
  std::vector<uint32_t> eligible =
      runtime_topology.ActivePeerIndexes(node_index);
  const PeerConnectivityPolicy* policy =
      FindPeerConnectivityPolicy(options.topology, node_index);
  if (policy == nullptr) {
    return eligible;
  }
  const uint32_t initial_peer_count = policy->peer_count.minimum();
  if (initial_peer_count > eligible.size()) {
    throw std::runtime_error(
        "initial peer count exceeds eligible logical topology peers");
  }
  eligible.resize(initial_peer_count);
  return eligible;
}

}  // namespace

std::vector<DirectionalNetworkPolicy> DirectionalNetworkPoliciesForNode(
    const Options& options, const RuntimePeerTopology& runtime_topology,
    uint32_t node_index) {
  return runtime_topology.DirectionalPolicies(NetworkAddressPlan(options),
                                              node_index);
}

std::vector<std::string> StartupPeerAddresses(
    const Options& options, const RuntimePeerTopology& topology,
    const ChainDriverSpec& chain_spec, uint32_t node_index) {
  const std::vector<uint32_t> peer_indexes =
      ConfiguredStartupPeerIndexes(options, topology, node_index);
  std::vector<std::string> peers;
  peers.reserve(peer_indexes.size());
  for (uint32_t peer_index : peer_indexes) {
    peers.push_back(StartupPeerAddress(options, chain_spec, peer_index));
  }
  return peers;
}

bool HostIpv4ForwardingEnabled() {
  const std::string value = ReadText("/proc/sys/net/ipv4/ip_forward");
  return !value.empty() && value.front() == '1';
}

}  // namespace bbp::simulator_app_internal
