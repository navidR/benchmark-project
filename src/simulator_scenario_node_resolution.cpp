#include "simulator_scenario_node_resolution.h"

#include <algorithm>
#include <filesystem>
#include <stdexcept>
#include <string>

#include "bbp/drivers/chain_driver_registry.h"
#include "bbp/scenario_node_config.h"
#include "bbp/simulator/options.h"

namespace bbp::simulator_app_internal {

bool NodeListContains(const std::vector<uint32_t>& nodes, uint32_t node_index) {
  for (uint32_t candidate : nodes) {
    if (candidate == node_index) {
      return true;
    }
  }
  return false;
}

std::string ScenarioNodeId(const Options& options, uint32_t node_index) {
  if (node_index >= options.nodes) {
    throw std::runtime_error("scenario node index is out of range");
  }
  if (!options.node_ids.empty()) {
    return options.node_ids.at(node_index);
  }
  return ChainDriverSpecFor(options.chain).node_id_prefix + "-" +
         std::to_string(node_index + 1U);
}

const ScenarioNodeConfig* ScenarioNodeConfigAt(const Options& options,
                                               uint32_t node_index) {
  if (options.scenario_node_configs.empty()) {
    return nullptr;
  }
  return &options.scenario_node_configs.at(node_index);
}

std::filesystem::path EffectiveNodeBinary(const Options& options,
                                          uint32_t node_index) {
  const ScenarioNodeConfig* config = ScenarioNodeConfigAt(options, node_index);
  if (!options.chain_daemon_cli_override && config != nullptr &&
      config->binary) {
    return *config->binary;
  }
  return options.chain_daemon;
}

std::filesystem::path NodeDataDirectoryRelative(const Options& options,
                                                uint32_t node_index) {
  const ScenarioNodeConfig* config = ScenarioNodeConfigAt(options, node_index);
  if (config != nullptr && config->data_dir) {
    return *config->data_dir;
  }
  return std::filesystem::path("nodes") / ScenarioNodeId(options, node_index) /
         "data";
}

ChainNetwork EffectiveNodeChainNetwork(const Options& options,
                                       uint32_t node_index) {
  const ScenarioNodeConfig* config = ScenarioNodeConfigAt(options, node_index);
  return config == nullptr ? ChainNetwork::kRegtest : config->network;
}

const ChainExtraArgs& EffectiveNodeExtraArgs(const Options& options,
                                             uint32_t node_index) {
  static const ChainExtraArgs empty;
  const ScenarioNodeConfig* config = ScenarioNodeConfigAt(options, node_index);
  return config == nullptr ? empty : config->extra_args;
}

NodeLifecyclePolicy EffectiveNodeLifecyclePolicy(const Options& options,
                                                 uint32_t node_index) {
  const ScenarioNodeConfig* config = ScenarioNodeConfigAt(options, node_index);
  return config == nullptr ? NodeLifecyclePolicy{} : config->lifecycle;
}

bool HasTimedNodeLifecycle(const Options& options) {
  return std::any_of(options.scenario_node_configs.begin(),
                     options.scenario_node_configs.end(),
                     [](const ScenarioNodeConfig& node) {
                       return node.lifecycle.start_time.has_value() ||
                              node.lifecycle.stop_time.has_value();
                     });
}

ScenarioNodeWalletConfig EffectiveNodeWalletConfig(const Options& options,
                                                   uint32_t node_index) {
  const ScenarioNodeConfig* config = ScenarioNodeConfigAt(options, node_index);
  if (config != nullptr && config->wallet) {
    return *config->wallet;
  }
  ScenarioNodeWalletConfig wallet;
  wallet.enabled = NodeListContains(options.topology.wallet_nodes, node_index);
  wallet.strategy = options.wallet_initialization.strategy;
  wallet.mode = options.wallet_initialization.mode;
  return wallet;
}

}  // namespace bbp::simulator_app_internal
