#include "simulator_runtime_published_node_config.h"

#include <boost/json/array.hpp>
#include <boost/json/value.hpp>
#include <string>
#include <utility>

#include "bbp/chain_network.h"
#include "bbp/drivers/chain_driver.h"
#include "bbp/drivers/chain_driver_registry.h"
#include "bbp/node_lifecycle_policy.h"
#include "bbp/simulator/node_runtime.h"
#include "bbp/simulator/node_runtime_lifecycle.h"
#include "bbp/simulator/options.h"
#include "simulator_runtime_identity_details.h"
#include "simulator_scenario_serialization.h"

namespace bbp::simulator_app_internal {

boost::json::object RuntimePublishedNodeConfig(
    const Options& options, const NodeRuntime& runtime,
    const ChainNodeConfig& config, std::uint32_t node_index,
    const NodeRoleTopology* runtime_topology) {
  const ChainDriverSpec& chain_spec = ChainDriverSpecFor(options.chain);
  boost::json::object node{
      {"index", node_index + 1U},
      {"id", config.id},
      {"chain", chain_spec.name},
      {"binary", config.binary.string()},
      {"data_dir", config.data_dir.generic_string()},
      {"role", NodeRoleName(options, node_index, runtime_topology)},
      {"lifecycle", std::string(NodeRuntimeLifecycleName(runtime.Lifecycle()))},
      {"restart_policy", std::string(NodeRestartPolicyName(
                             runtime.lifecycle_policy.restart_policy))}};

  boost::json::array extra_args;
  for (const std::string& argument : config.extra_args.arguments()) {
    extra_args.emplace_back(argument);
  }
  node["chain_config"] =
      boost::json::object{{"network", ChainNetworkName(config.network)},
                          {"extra_args", std::move(extra_args)}};

  boost::json::array rpc_allow_ips;
  for (const std::string& address : config.rpc_allow_ips) {
    rpc_allow_ips.emplace_back(address);
  }
  boost::json::object rpc{
      {"host", config.rpc_host},
      {"bind", config.rpc_bind},
      {"port", config.rpc_port},
      {"allow_ips", std::move(rpc_allow_ips)},
      {"credentials", "<generated-redacted>"},
      {"binding_scope",
       options.isolate_network ? "node_veth_only" : "loopback_only"}};
  switch (config.rpc_authentication) {
    case RpcAuthenticationMode::kCookieFile:
      rpc["authentication"] = "cookie";
      rpc["credential_file_lifecycle"] = "ephemeral";
      break;
    case RpcAuthenticationMode::kDigest:
      rpc["authentication"] = "digest";
      rpc["credential_file_lifecycle"] = nullptr;
      break;
    case RpcAuthenticationMode::kBasic:
      rpc["authentication"] = "basic";
      rpc["credential_file_lifecycle"] = nullptr;
      break;
  }
  node["rpc"] = std::move(rpc);

  boost::json::array connect_peers;
  for (const std::string& endpoint : config.connect_peers) {
    connect_peers.emplace_back(endpoint);
  }
  node["p2p"] =
      boost::json::object{{"host", config.p2p_host},
                          {"bind", config.p2p_bind},
                          {"port", config.p2p_port},
                          {"listen", config.listen},
                          {"connect_peers", std::move(connect_peers)}};

  ScenarioNodeWalletConfig wallet;
  wallet.enabled = config.wallet_enabled;
  wallet.strategy = options.wallet_initialization.strategy;
  wallet.mode = options.wallet_initialization.mode;
  node["wallet"] = ScenarioNodeWalletConfigJson(wallet);

  node["start_time_ms"] =
      runtime.lifecycle_policy.start_time
          ? boost::json::value(runtime.lifecycle_policy.start_time->count())
          : boost::json::value(nullptr);
  node["stop_time_ms"] =
      runtime.lifecycle_policy.stop_time
          ? boost::json::value(runtime.lifecycle_policy.stop_time->count())
          : boost::json::value(nullptr);
  node["resources"] = boost::json::object{
      {"profile", runtime.resource_profile.empty()
                      ? boost::json::value(nullptr)
                      : boost::json::value(runtime.resource_profile)},
      {"resolved", ResourceLimitsJson(runtime.resources)}};
  boost::json::value network = nullptr;
  if (runtime.network && runtime.network->apply_condition) {
    network = NetworkConditionJson(runtime.network->condition);
  }
  node["network"] = boost::json::object{
      {"profile", runtime.network_profile.empty()
                      ? boost::json::value(nullptr)
                      : boost::json::value(runtime.network_profile)},
      {"resolved", std::move(network)}};
  return node;
}

}  // namespace bbp::simulator_app_internal
