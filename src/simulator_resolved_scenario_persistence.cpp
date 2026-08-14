#include "simulator_resolved_scenario_persistence.h"

#include <boost/json/array.hpp>
#include <boost/json/serialize.hpp>
#include <cstdint>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "bbp/drivers/chain_driver_registry.h"
#include "bbp/runtime_peer_topology.h"
#include "bbp/simulator/options.h"
#include "bbp/simulator/process_control_config.h"
#include "bbp/util.h"
#include "simulator_resource_profile_decoding.h"
#include "simulator_scenario_node_resolution.h"
#include "simulator_scenario_serialization.h"

namespace bbp::simulator_app_internal {

std::vector<ScenarioWorkload> EffectiveWorkloads(const Options& options) {
  return options.workloads;
}

namespace {

std::optional<uint32_t> CommonBlockGenerationNode(
    const std::vector<ScenarioWorkload>& workloads) {
  std::optional<uint32_t> node;
  for (const ScenarioWorkload& workload : workloads) {
    if (workload.kind != WorkloadKind::kBlockGeneration) {
      continue;
    }
    if (!node) {
      node = workload.block_generation.node;
    } else if (*node != workload.block_generation.node) {
      return std::nullopt;
    }
  }
  return node;
}

}  // namespace

boost::json::object BuildResolvedScenarioDocument(
    const Options& options, const ChainDriverSpec& chain_spec) {
  const std::vector<ScenarioWorkload> workloads = EffectiveWorkloads(options);
  boost::json::object resolved;
  resolved["run_id"] = options.run_id;
  boost::json::object simulation;
  simulation["name"] = options.simulation_name;
  simulation["seed"] = options.simulation_seed;
  if (options.simulation_duration) {
    simulation["duration"] =
        std::to_string(options.simulation_duration->count()) + "ms";
    simulation["duration_ms"] = options.simulation_duration->count();
    simulation["wall_duration_ms"] =
        options.time_scale.WallDuration(*options.simulation_duration).count();
  } else {
    simulation["duration"] = nullptr;
    simulation["duration_ms"] = nullptr;
    simulation["wall_duration_ms"] = nullptr;
  }
  simulation["time_scale"] = options.time_scale.value();
  simulation["time_scale_millionths"] = options.time_scale.millionths();
  simulation["cleanup_policy"] =
      std::string(CleanupPolicyName(options.cleanup_policy));
  simulation["privilege_mode"] =
      std::string(PrivilegeModeName(options.privilege_mode));
  simulation["log_retention_policy"] =
      std::string(LogRetentionPolicyName(options.log_retention_policy));
  simulation["metrics_interval"] =
      std::to_string(options.metrics_interval.count()) + "ms";
  simulation["metrics_interval_ms"] = options.metrics_interval.count();
  simulation["output_dir"] =
      std::filesystem::absolute(options.output_dir).string();
  simulation["tui_refresh_interval"] =
      std::to_string(options.tui_refresh_ms) + "ms";
  simulation["tui_refresh_interval_ms"] = options.tui_refresh_ms;
  resolved["simulation"] = std::move(simulation);
  resolved["chain"] = chain_spec.name;
  boost::json::object chains;
  for (const auto& [name, chain] : options.chains) {
    boost::json::object definition;
    definition["driver"] = std::string(ChainKindName(chain.driver));
    definition["default_binary"] = chain.default_binary.string();
    chains[name] = std::move(definition);
  }
  resolved["chains"] = std::move(chains);
  resolved["nodes"] = options.nodes;
  resolved["node_capacity"] = options.node_capacity;
  if (const std::optional<uint32_t> generate_node =
          CommonBlockGenerationNode(workloads)) {
    resolved["generate_node"] = *generate_node;
  } else {
    resolved["generate_node"] = nullptr;
  }
  resolved["chain_daemon"] = options.chain_daemon.string();
  resolved[chain_spec.daemon_scenario_field] = options.chain_daemon.string();
  if (!options.scenario_json.empty()) {
    resolved["scenario_json"] = options.scenario_json.string();
  }
  if (!options.scenario_yaml.empty()) {
    resolved["scenario_yaml"] = options.scenario_yaml.string();
  }
  resolved["isolated_network"] = options.isolate_network;
  if (options.network_address_plan) {
    resolved["network_address_range"] = options.network_address_plan->Cidr();
  } else {
    resolved["network_address_range"] = nullptr;
  }
  resolved["ready_timeout_sec"] = options.ready_timeout_sec;
  resolved["sync_timeout_sec"] = options.sync_timeout_sec;
  resolved["metrics_sample_count"] = options.metrics_sample_count;
  resolved["metrics_interval_ms"] = options.metrics_interval.count();
  resolved["keep_artifacts"] = options.keep_artifacts;
  boost::json::object block_production;
  block_production["enabled"] = options.block_production.enabled;
  block_production["native_mining"] =
      options.block_production.mode == MiningMode::kNativeMining;
  block_production["period_ms"] =
      options.block_production.policy.period().count();
  block_production["probability"] =
      options.block_production.policy.probability();
  block_production["seed"] = options.block_production.policy.seed();
  if (options.block_production.difficulty) {
    block_production["difficulty"] =
        options.block_production.difficulty->value();
  } else {
    block_production["difficulty"] = nullptr;
  }
  resolved["block_production"] = std::move(block_production);
  if (options.topology.configured) {
    resolved["topology"] =
        NodeRoleTopologyJson(options.topology, options.wallet_initialization);
  }
  resolved["topology_initial_edges"] = RuntimePeerTopologyEdgesJson(
      RuntimePeerTopology(options.topology.peer_topology, options.nodes,
                          options.empty_control_plane));
  if (!options.resource_profiles.empty()) {
    boost::json::object profiles;
    for (const auto& [name, limits] : options.resource_profiles) {
      profiles[name] = ResourceLimitsJson(limits);
    }
    resolved["resource_profiles"] = std::move(profiles);
  }
  if (!options.network_profiles.empty()) {
    boost::json::object profiles;
    for (const auto& [name, condition] : options.network_profiles) {
      profiles[name] = NetworkConditionJson(condition);
    }
    resolved["network_profiles"] = std::move(profiles);
  }
  boost::json::array node_configs;
  for (uint32_t node_index = 0U; node_index < options.nodes; ++node_index) {
    boost::json::object node;
    node["index"] = node_index + 1U;
    node["id"] = options.node_ids.empty() ? chain_spec.node_id_prefix + "-" +
                                                std::to_string(node_index + 1U)
                                          : options.node_ids.at(node_index);
    node["chain"] = chain_spec.name;
    node["binary"] = EffectiveNodeBinary(options, node_index).string();
    node["data_dir"] =
        NodeDataDirectoryRelative(options, node_index).generic_string();
    boost::json::object chain_config;
    chain_config["network"] =
        ChainNetworkName(EffectiveNodeChainNetwork(options, node_index));
    boost::json::array extra_args;
    for (const std::string& argument :
         EffectiveNodeExtraArgs(options, node_index).arguments()) {
      extra_args.emplace_back(argument);
    }
    chain_config["extra_args"] = std::move(extra_args);
    node["chain_config"] = std::move(chain_config);
    boost::json::object rpc;
    switch (chain_spec.rpc_authentication) {
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
    rpc["credentials"] = "<generated-redacted>";
    rpc["binding_scope"] =
        options.isolate_network ? "node_veth_only" : "loopback_only";
    node["rpc"] = std::move(rpc);
    node["wallet"] = ScenarioNodeWalletConfigJson(
        EffectiveNodeWalletConfig(options, node_index));
    const ScenarioNodeConfig* configured_node =
        ScenarioNodeConfigAt(options, node_index);
    const NodeLifecyclePolicy lifecycle = configured_node == nullptr
                                              ? NodeLifecyclePolicy{}
                                              : configured_node->lifecycle;
    if (lifecycle.start_time) {
      node["start_time"] = std::to_string(lifecycle.start_time->count()) + "ms";
      node["start_time_ms"] = lifecycle.start_time->count();
      node["wall_start_time_ms"] =
          options.time_scale.WallDuration(*lifecycle.start_time).count();
    } else {
      node["start_time"] = nullptr;
      node["start_time_ms"] = 0;
      node["wall_start_time_ms"] = 0;
    }
    if (lifecycle.stop_time) {
      node["stop_time"] = std::to_string(lifecycle.stop_time->count()) + "ms";
      node["stop_time_ms"] = lifecycle.stop_time->count();
      node["wall_stop_time_ms"] =
          options.time_scale.WallDuration(*lifecycle.stop_time).count();
    } else {
      node["stop_time"] = nullptr;
      node["stop_time_ms"] = nullptr;
      node["wall_stop_time_ms"] = nullptr;
    }
    node["restart_policy"] =
        std::string(NodeRestartPolicyName(lifecycle.restart_policy));
    if (!options.node_roles.empty()) {
      node["role"] = options.node_roles.at(node_index);
    } else {
      const bool wallet =
          NodeListContains(options.topology.wallet_nodes, node_index);
      const bool miner =
          NodeListContains(options.topology.miner_nodes, node_index);
      node["role"] = wallet && miner ? "wallet_miner"
                     : wallet        ? "wallet"
                     : miner         ? "miner"
                                     : "base";
    }
    boost::json::object resources_config;
    const auto resource_profile =
        options.node_resource_profiles.find(node_index);
    if (resource_profile != options.node_resource_profiles.end()) {
      resources_config["profile"] = resource_profile->second;
    } else {
      resources_config["profile"] = nullptr;
    }
    resources_config["resolved"] =
        ResourceLimitsJson(InitialResourceLimits(options, node_index));
    node["resources"] = std::move(resources_config);

    boost::json::object network_config;
    const auto network_profile = options.node_network_profiles.find(node_index);
    if (network_profile != options.node_network_profiles.end()) {
      network_config["profile"] = network_profile->second;
    } else {
      network_config["profile"] = nullptr;
    }
    const auto node_condition =
        options.node_network_conditions.find(node_index);
    if (node_condition != options.node_network_conditions.end()) {
      network_config["resolved"] = NetworkConditionJson(node_condition->second);
    } else if (options.network_condition_requested) {
      network_config["resolved"] =
          NetworkConditionJson(options.network_condition);
    } else {
      network_config["resolved"] = nullptr;
    }
    node["network"] = std::move(network_config);
    node_configs.push_back(std::move(node));
  }
  resolved["node_configs"] = std::move(node_configs);
  boost::json::array workload_array;
  for (const ScenarioWorkload& workload : workloads) {
    workload_array.push_back(WorkloadJson(workload));
  }
  resolved["workloads"] = std::move(workload_array);
  boost::json::array scheduled_event_array;
  for (const ScheduledScenarioEvent& event : options.scheduled_events) {
    scheduled_event_array.push_back(
        ScheduledScenarioEventJson(event, options.time_scale));
  }
  resolved["events"] = std::move(scheduled_event_array);
  boost::json::object resources;
  resources["memory_high_bytes"] = options.memory_high_bytes;
  resources["memory_max_bytes"] = options.memory_max_bytes;
  if (options.cpu_quota_requested) {
    resources["cpu_quota_us"] = options.cpu_quota_us;
  } else {
    resources["cpu_quota_us"] = nullptr;
  }
  resources["cpu_period_us"] = options.cpu_period_us;
  resources["cpu_weight"] = options.cpu_weight;
  resources["io_weight"] = options.io_weight;
  resources["io_max"] = IoLimitsJson(options.io_limits);
  resources["pids_max"] = options.pids_max;
  resolved["resources"] = std::move(resources);
  if (options.network_condition_requested) {
    resolved["default_network_condition"] =
        NetworkConditionJson(options.network_condition);
  }
  if (!options.node_network_conditions.empty()) {
    boost::json::array node_conditions;
    for (const auto& [node_index, condition] :
         options.node_network_conditions) {
      boost::json::object node_condition;
      node_condition["node"] = node_index + 1U;
      node_condition["condition"] = NetworkConditionJson(condition);
      node_conditions.push_back(std::move(node_condition));
    }
    resolved["node_network_conditions"] = std::move(node_conditions);
  }
  if (!options.runtime_node_network_conditions.empty()) {
    boost::json::array runtime_node_conditions;
    for (const auto& [node_index, condition] :
         options.runtime_node_network_conditions) {
      boost::json::object node_condition;
      node_condition["node"] = node_index + 1U;
      node_condition["condition"] = NetworkConditionJson(condition);
      runtime_node_conditions.push_back(std::move(node_condition));
    }
    resolved["runtime_node_network_conditions"] =
        std::move(runtime_node_conditions);
  }
  if (!options.runtime_node_blocks.empty()) {
    boost::json::array runtime_node_blocks;
    for (const NetworkBlockRule& rule : options.runtime_node_blocks) {
      runtime_node_blocks.push_back(NetworkBlockRuleJson(rule));
    }
    resolved["runtime_node_blocks"] = std::move(runtime_node_blocks);
  }
  if (!options.runtime_node_unblocks.empty()) {
    boost::json::array runtime_node_unblocks;
    for (const NetworkBlockRule& rule : options.runtime_node_unblocks) {
      runtime_node_unblocks.push_back(NetworkBlockRuleJson(rule));
    }
    resolved["runtime_node_unblocks"] = std::move(runtime_node_unblocks);
  }
  if (!options.runtime_partitions.empty()) {
    boost::json::array runtime_partitions;
    for (const NetworkPartitionRule& rule : options.runtime_partitions) {
      runtime_partitions.push_back(NetworkPartitionRuleJson(rule));
    }
    resolved["runtime_partitions"] = std::move(runtime_partitions);
  }
  if (!options.runtime_partition_heals.empty()) {
    boost::json::array runtime_partition_heals;
    for (const NetworkPartitionRule& rule : options.runtime_partition_heals) {
      runtime_partition_heals.push_back(NetworkPartitionRuleJson(rule));
    }
    resolved["runtime_partition_heals"] = std::move(runtime_partition_heals);
  }
  if (!options.runtime_node_resource_updates.empty()) {
    boost::json::array runtime_node_limits;
    for (const auto& [node_index, patch] :
         options.runtime_node_resource_updates) {
      boost::json::object node_limits;
      node_limits["node"] = node_index + 1U;
      node_limits["limits"] = ResourceLimitPatchJson(patch);
      runtime_node_limits.push_back(std::move(node_limits));
    }
    resolved["runtime_node_resource_limits"] = std::move(runtime_node_limits);
  }
  if (!options.runtime_node_restarts.empty() ||
      !options.runtime_node_freezes.empty()) {
    resolved["process"] = ProcessControlConfigJson(ProcessControlConfig{
        .restart_node_indexes = options.runtime_node_restarts,
        .freezes = options.runtime_node_freezes,
    });
  }
  return resolved;
}

void WriteScenarioFiles(const Options& options,
                        const std::filesystem::path& run_root,
                        const ChainDriverSpec& chain_spec,
                        int reserved_run_root_fd) {
  const boost::json::object resolved =
      BuildResolvedScenarioDocument(options, chain_spec);
  const std::string resolved_text = boost::json::serialize(resolved) + "\n";
  const std::string yaml_text = YamlFromJson(resolved);
  if (reserved_run_root_fd >= 0) {
    CreateTextAt(reserved_run_root_fd, "resolved-scenario.json", resolved_text);
    CreateTextAt(reserved_run_root_fd, "scenario.yaml", yaml_text);
  } else {
    WriteText(run_root / "resolved-scenario.json", resolved_text);
    WriteText(run_root / "scenario.yaml", yaml_text);
  }
}

}  // namespace bbp::simulator_app_internal
