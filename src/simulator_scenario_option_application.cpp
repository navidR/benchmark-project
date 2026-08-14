#include "simulator_scenario_option_application.h"

#include <algorithm>
#include <boost/json/array.hpp>
#include <boost/json/value.hpp>
#include <chrono>
#include <cstdint>
#include <limits>
#include <optional>
#include <stdexcept>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "bbp/drivers/chain_driver_registry.h"
#include "bbp/positive_duration.h"
#include "bbp/scenario_fields.h"
#include "bbp/simulator/options.h"
#include "bbp/simulator/process_control_config.h"
#include "simulator_json_field_decoding.h"
#include "simulator_network_rule_decoding.h"
#include "simulator_peer_topology_decoding.h"
#include "simulator_profile_assignment.h"
#include "simulator_resource_limit_decoding.h"
#include "simulator_resource_profile_decoding.h"
#include "simulator_scenario_chain_decoding.h"
#include "simulator_scenario_mutation_option_decoding.h"
#include "simulator_scenario_node_decoding.h"
#include "simulator_scenario_workload_decoding.h"
#include "simulator_scheduled_event_decoding.h"
#include "simulator_wallet_configuration_decoding.h"

namespace bbp::simulator_app_internal {
namespace {

bool OptionProvided(const boost::program_options::variables_map& vm,
                    const char* name) {
  const auto iter = vm.find(name);
  return iter != vm.end() && !iter->second.defaulted();
}

bool SameNodeSet(std::vector<uint32_t> left, std::vector<uint32_t> right) {
  std::sort(left.begin(), left.end());
  std::sort(right.begin(), right.end());
  return left == right;
}

}  // namespace

void ApplyScenarioJson(const boost::json::object& scenario,
                       const boost::program_options::variables_map& vm,
                       Options& options) {
  if (scenario.if_contains("generate_blocks") != nullptr) {
    throw std::runtime_error(
        "scenario generate_blocks was removed; use block_production.enabled "
        "or an explicit block_generation workload");
  }
  const boost::json::value* simulation_value =
      scenario.if_contains("simulation");
  const boost::json::object* simulation = nullptr;
  if (simulation_value != nullptr) {
    if (!simulation_value->is_object()) {
      throw std::runtime_error("scenario simulation must be a JSON object");
    }
    simulation = &simulation_value->as_object();
    RejectUnsupportedFields(
        *simulation, ScenarioObjectFields(ScenarioObjectKind::kSimulation),
        "scenario simulation");
    if (simulation->if_contains("name") != nullptr) {
      options.simulation_name = JsonStringField(*simulation, "name");
      RequireSafeScenarioIdentifier(options.simulation_name, "simulation name");
    }
    options.simulation_seed =
        JsonOptionalUint64Field(*simulation, "seed", options.simulation_seed);
    const boost::json::value* duration = simulation->if_contains("duration");
    if (duration != nullptr) {
      if (!duration->is_string()) {
        throw std::runtime_error(
            "scenario simulation.duration must be a duration string");
      }
      options.simulation_duration =
          PositiveDuration::Parse(
              std::string_view(duration->as_string().data(),
                               duration->as_string().size()))
              .value();
    }
    options.time_scale =
        SimulationTimeScale::FromDouble(JsonOptionalDoubleField(
            *simulation, "time_scale", options.time_scale.value()));

    if (!OptionProvided(vm, "keep-cgroups")) {
      const std::string cleanup_policy = JsonOptionalStringField(
          *simulation, "cleanup_policy",
          std::string(CleanupPolicyName(options.cleanup_policy)));
      const std::optional<CleanupPolicy> parsed_cleanup_policy =
          CleanupPolicyFromName(cleanup_policy);
      if (!parsed_cleanup_policy) {
        throw std::runtime_error(
            "scenario simulation.cleanup_policy must be automatic or "
            "retain_cgroups");
      }
      options.cleanup_policy = *parsed_cleanup_policy;
    }

    const std::string privilege_mode = JsonOptionalStringField(
        *simulation, "privilege_mode",
        std::string(PrivilegeModeName(options.privilege_mode)));
    const std::optional<PrivilegeMode> parsed_privilege_mode =
        PrivilegeModeFromName(privilege_mode);
    if (!parsed_privilege_mode) {
      throw std::runtime_error(
          "scenario simulation.privilege_mode currently supports only "
          "direct");
    }
    options.privilege_mode = *parsed_privilege_mode;

    const std::string log_retention_policy = JsonOptionalStringField(
        *simulation, "log_retention_policy",
        std::string(LogRetentionPolicyName(options.log_retention_policy)));
    const std::optional<LogRetentionPolicy> parsed_log_retention_policy =
        LogRetentionPolicyFromName(log_retention_policy);
    if (!parsed_log_retention_policy) {
      throw std::runtime_error(
          "scenario simulation.log_retention_policy currently supports only "
          "preserve");
    }
    options.log_retention_policy = *parsed_log_retention_policy;

    const bool has_metrics_interval =
        simulation->if_contains("metrics_interval") != nullptr;
    const bool has_tick_interval =
        simulation->if_contains("tick_interval") != nullptr;
    if (has_metrics_interval && has_tick_interval) {
      throw std::runtime_error(
          "scenario simulation.metrics_interval and tick_interval are "
          "aliases and must not both be provided");
    }
    if ((has_metrics_interval || has_tick_interval) &&
        scenario.if_contains("metrics_interval_ms") != nullptr) {
      throw std::runtime_error(
          "scenario simulation metrics interval and top-level "
          "metrics_interval_ms must not be combined");
    }
    if (!OptionProvided(vm, "metrics-interval") &&
        !OptionProvided(vm, "metrics-interval-ms") &&
        (has_metrics_interval || has_tick_interval)) {
      const char* field =
          has_metrics_interval ? "metrics_interval" : "tick_interval";
      options.metrics_interval =
          PositiveDuration::Parse(JsonStringField(*simulation, field)).value();
    }

    if (simulation->if_contains("output_dir") != nullptr &&
        scenario.if_contains("output_dir") != nullptr) {
      throw std::runtime_error(
          "scenario simulation.output_dir and top-level output_dir must not "
          "be combined");
    }
    if (!OptionProvided(vm, "benchmark-root") &&
        !OptionProvided(vm, "output-dir")) {
      options.output_dir =
          JsonOptionalPathField(*simulation, "output_dir", options.output_dir);
    }

    if (simulation->if_contains("tui_refresh_interval") != nullptr &&
        !OptionProvided(vm, "refresh-ms")) {
      const auto refresh = PositiveDuration::Parse(
          JsonStringField(*simulation, "tui_refresh_interval"));
      if (refresh.value().count() >
          static_cast<std::chrono::milliseconds::rep>(
              std::numeric_limits<std::uint32_t>::max())) {
        throw std::runtime_error(
            "scenario simulation.tui_refresh_interval exceeds uint32 "
            "milliseconds");
      }
      options.tui_refresh_ms =
          static_cast<std::uint32_t>(refresh.value().count());
    }
  }
  if (!OptionProvided(vm, "block-production-seed")) {
    options.block_production.policy = BlockProductionPolicy(
        options.block_production.policy.period(),
        options.block_production.policy.probability(), options.simulation_seed);
  }
  ParseScenarioChains(scenario, &options);
  if (!OptionProvided(vm, "chain")) {
    if (scenario.if_contains("chain") != nullptr) {
      options.chain = ParseChainKind(JsonStringField(scenario, "chain"));
    } else if (options.chains.size() == 1U) {
      options.chain = options.chains.begin()->second.driver;
    } else if (!options.chains.empty()) {
      throw std::runtime_error(
          "scenario with multiple chains requires an active chain");
    }
  }
  const std::string active_chain_name(ChainKindName(options.chain));
  const auto active_chain = options.chains.find(active_chain_name);
  if (!options.chains.empty() && active_chain == options.chains.end()) {
    throw std::runtime_error("scenario chains does not define active chain: " +
                             active_chain_name);
  }
  const ChainDriverSpec& chain_spec = ChainDriverSpecFor(options.chain);
  RejectUnsupportedFields(scenario,
                          ScenarioObjectFields(ScenarioObjectKind::kRoot),
                          "scenario", chain_spec.daemon_scenario_field);
  if (!OptionProvided(vm, "node-capacity")) {
    options.node_capacity = JsonOptionalUint32Field(scenario, "node_capacity",
                                                    options.node_capacity);
    if (scenario.if_contains("node_capacity") != nullptr &&
        options.node_capacity == 0U) {
      throw std::runtime_error(
          "scenario node_capacity must be greater than zero");
    }
  }
  if (options.node_capacity == 0U) {
    options.node_capacity = chain_spec.max_nodes;
  }
  const bool chain_daemon_provided =
      OptionProvided(vm, "node-binary") || OptionProvided(vm, "chain-daemon") ||
      OptionProvided(vm, chain_spec.daemon_option_name.c_str());
  const bool legacy_chain_daemon =
      scenario.if_contains("chain_daemon") != nullptr;
  const bool legacy_driver_daemon =
      scenario.if_contains(chain_spec.daemon_scenario_field) != nullptr;
  if (!options.chains.empty() &&
      (legacy_chain_daemon || legacy_driver_daemon)) {
    throw std::runtime_error(
        "scenario chains default_binary must not be combined with legacy "
        "daemon fields");
  }
  if (!chain_daemon_provided) {
    if (active_chain != options.chains.end()) {
      options.chain_daemon = active_chain->second.default_binary;
    } else {
      if (legacy_chain_daemon && legacy_driver_daemon) {
        throw std::runtime_error(
            "scenario chain_daemon and its driver-specific alias must not "
            "both be provided");
      }
      options.chain_daemon =
          JsonOptionalPathField(scenario, "chain_daemon", options.chain_daemon);
      options.chain_daemon = JsonOptionalPathField(
          scenario, chain_spec.daemon_scenario_field.c_str(),
          options.chain_daemon);
    }
  }
  if (!OptionProvided(vm, "benchmark-root") &&
      !OptionProvided(vm, "output-dir") &&
      (simulation == nullptr ||
       simulation->if_contains("output_dir") == nullptr)) {
    options.output_dir =
        JsonOptionalPathField(scenario, "output_dir", options.output_dir);
  }
  if (!OptionProvided(vm, "run-id")) {
    const boost::json::value* run_id = scenario.if_contains("run_id");
    if (run_id != nullptr) {
      if (!run_id->is_string()) {
        throw std::runtime_error("scenario run_id must be a string");
      }
      options.run_id = std::string(run_id->as_string());
    }
  }
  if (options.simulation_name.empty()) {
    options.simulation_name = options.run_id;
  }
  const boost::json::value* topology = scenario.if_contains("topology");
  if (topology != nullptr) {
    if (!topology->is_object()) {
      throw std::runtime_error("scenario topology must be a JSON object");
    }
    options.wallet_initialization =
        ParseWalletInitializationObject(topology->as_object());
  }
  const ScenarioNodeRoles node_roles =
      ParseScenarioNodes(scenario, vm, &options);
  if (topology != nullptr) {
    options.topology = ParseNodeRoleTopologyObject(
        topology->as_object(), options.nodes, options.simulation_seed);
    if (node_roles.configured &&
        (!SameNodeSet(options.topology.wallet_nodes, node_roles.wallet_nodes) ||
         !SameNodeSet(options.topology.miner_nodes, node_roles.miner_nodes))) {
      throw std::runtime_error(
          "scenario nodes roles must match topology wallet_nodes and "
          "miner_nodes");
    }
    if (!OptionProvided(vm, "generate-node") &&
        !options.topology.miner_nodes.empty()) {
      options.generate_node = options.topology.miner_nodes.front() + 1U;
    }
  } else if (node_roles.configured) {
    boost::json::object derived_topology;
    derived_topology["node_count"] = options.nodes;
    boost::json::array wallet_nodes;
    for (const uint32_t node_index : node_roles.wallet_nodes) {
      wallet_nodes.push_back(node_index + 1U);
    }
    derived_topology["wallet_nodes"] = std::move(wallet_nodes);
    boost::json::array miner_nodes;
    for (const uint32_t node_index : node_roles.miner_nodes) {
      miner_nodes.push_back(node_index + 1U);
    }
    derived_topology["miner_nodes"] = std::move(miner_nodes);
    derived_topology["allow_miner_wallet_overlap"] =
        NodeListsOverlap(node_roles.wallet_nodes, node_roles.miner_nodes);
    derived_topology["type"] = "full_mesh";
    options.topology = ParseNodeRoleTopologyObject(
        derived_topology, options.nodes, options.simulation_seed);
    if (!OptionProvided(vm, "generate-node") &&
        !options.topology.miner_nodes.empty()) {
      options.generate_node = options.topology.miner_nodes.front() + 1U;
    }
  }
  options.wallet_backed_workload_requested =
      options.wallet_backed_workload_requested ||
      !options.topology.wallet_nodes.empty();
  if (!OptionProvided(vm, "generate-node")) {
    options.generate_node = JsonOptionalNullableUint32Field(
        scenario, "generate_node", options.generate_node);
  }
  const boost::json::value* block_production =
      scenario.if_contains("block_production");
  if (block_production != nullptr) {
    if (!block_production->is_object()) {
      throw std::runtime_error(
          "scenario block_production must be a JSON object");
    }
    const boost::json::object& object = block_production->as_object();
    RejectUnsupportedFields(
        object, ScenarioObjectFields(ScenarioObjectKind::kBlockProduction),
        "scenario block_production");
    if (!OptionProvided(vm, "no-mining")) {
      options.block_production.enabled = JsonOptionalBoolField(
          object, "enabled", options.block_production.enabled);
    }
    if (!OptionProvided(vm, "native-mining")) {
      options.block_production.mode =
          JsonOptionalBoolField(object, "native_mining", false)
              ? MiningMode::kNativeMining
              : MiningMode::kScheduledBlockProduction;
    }
    const std::uint32_t period_ms =
        OptionProvided(vm, "block-production-period-ms")
            ? static_cast<std::uint32_t>(
                  options.block_production.policy.period().count())
            : JsonOptionalUint32Field(
                  object, "period_ms",
                  static_cast<std::uint32_t>(
                      options.block_production.policy.period().count()));
    const double probability =
        OptionProvided(vm, "block-production-probability")
            ? options.block_production.policy.probability()
            : JsonOptionalDoubleField(
                  object, "probability",
                  options.block_production.policy.probability());
    const std::uint64_t seed =
        OptionProvided(vm, "block-production-seed")
            ? options.block_production.policy.seed()
            : JsonOptionalUint64Field(object, "seed",
                                      options.block_production.policy.seed());
    options.block_production.policy = BlockProductionPolicy(
        std::chrono::milliseconds(period_ms), probability, seed);
    if (!OptionProvided(vm, "mining-difficulty") &&
        object.if_contains("difficulty") != nullptr) {
      const std::optional<double> difficulty =
          JsonOptionalNullableDoubleField(object, "difficulty");
      options.block_production.difficulty =
          difficulty
              ? std::optional<MiningDifficulty>(MiningDifficulty(*difficulty))
              : std::nullopt;
    }
  }
  if (!OptionProvided(vm, "ready-timeout-sec")) {
    options.ready_timeout_sec = JsonOptionalUint32Field(
        scenario, "ready_timeout_sec", options.ready_timeout_sec);
  }
  if (!OptionProvided(vm, "sync-timeout-sec")) {
    options.sync_timeout_sec = JsonOptionalNullableUint32Field(
        scenario, "sync_timeout_sec", options.sync_timeout_sec);
  }
  if (!OptionProvided(vm, "metrics-sample-count")) {
    options.metrics_sample_count = JsonOptionalUint32Field(
        scenario, "metrics_sample_count", options.metrics_sample_count);
  }
  if (!OptionProvided(vm, "metrics-interval") &&
      !OptionProvided(vm, "metrics-interval-ms") &&
      (simulation == nullptr ||
       (simulation->if_contains("metrics_interval") == nullptr &&
        simulation->if_contains("tick_interval") == nullptr))) {
    options.metrics_interval =
        PositiveDuration::FromMilliseconds(
            JsonOptionalUint32Field(
                scenario, "metrics_interval_ms",
                static_cast<std::uint32_t>(options.metrics_interval.count())))
            .value();
  }
  if (!OptionProvided(vm, "isolate-network") &&
      !OptionProvided(vm, "no-isolate-network")) {
    options.isolate_network = JsonOptionalBoolField(
        scenario, "isolated_network", options.isolate_network);
  }
  const boost::json::value* workloads = scenario.if_contains("workloads");
  if (workloads != nullptr) {
    if (!workloads->is_array()) {
      throw std::runtime_error("scenario workloads must be a JSON array");
    }
    options.workloads_configured = true;
    ApplyScenarioWorkloads(workloads->as_array(), vm, options);
  }
  const boost::json::value* events = scenario.if_contains("events");
  if (events != nullptr) {
    if (!events->is_array()) {
      throw std::runtime_error("scenario events must be a JSON array");
    }
    ApplyScheduledScenarioEvents(events->as_array(), vm, options);
  }

  const boost::json::value* resources = scenario.if_contains("resources");
  if (resources != nullptr) {
    if (!resources->is_object()) {
      throw std::runtime_error("scenario resources must be a JSON object");
    }
    const boost::json::object& object = resources->as_object();
    RejectUnsupportedFields(
        object, ScenarioObjectFields(ScenarioObjectKind::kResources),
        "scenario resources");
    if (!OptionProvided(vm, "memory-high-bytes")) {
      options.memory_high_bytes = JsonOptionalUint64Field(
          object, "memory_high_bytes", options.memory_high_bytes);
    }
    if (!OptionProvided(vm, "memory-max-bytes")) {
      options.memory_max_bytes = JsonOptionalUint64Field(
          object, "memory_max_bytes", options.memory_max_bytes);
    }
    if (!OptionProvided(vm, "cpu-period-us")) {
      options.cpu_period_us = JsonOptionalUint64Field(object, "cpu_period_us",
                                                      options.cpu_period_us);
    }
    if (!OptionProvided(vm, "cpu-weight")) {
      options.cpu_weight =
          JsonOptionalUint64Field(object, "cpu_weight", options.cpu_weight);
    }
    if (!OptionProvided(vm, "io-weight")) {
      options.io_weight =
          JsonOptionalUint64Field(object, "io_weight", options.io_weight);
    }
    const boost::json::value* initial_io_limits = object.if_contains("io_max");
    if (initial_io_limits != nullptr) {
      options.io_limits =
          ParseIoLimits(*initial_io_limits, "scenario resources.io_max");
    }
    if (!OptionProvided(vm, "pids-max")) {
      options.pids_max =
          JsonOptionalUint64Field(object, "pids_max", options.pids_max);
    }
    if (!OptionProvided(vm, "cpu-quota-us")) {
      const boost::json::value* quota = object.if_contains("cpu_quota_us");
      if (quota != nullptr && !quota->is_null()) {
        if (!quota->is_uint64() &&
            !(quota->is_int64() && quota->as_int64() >= 0)) {
          throw std::runtime_error(
              "scenario resources.cpu_quota_us must be uint or null");
        }
        options.cpu_quota_us = quota->is_uint64()
                                   ? quota->as_uint64()
                                   : static_cast<uint64_t>(quota->as_int64());
        options.cpu_quota_requested = true;
      }
    }
    const boost::json::value* runtime_node_limits =
        object.if_contains("runtime_node_limits");
    if (runtime_node_limits != nullptr) {
      if (!runtime_node_limits->is_array()) {
        throw std::runtime_error(
            "scenario resources.runtime_node_limits must be a JSON array");
      }
      ApplyResourceLimitPatches(runtime_node_limits->as_array(), options.nodes,
                                "scenario resources.runtime_node_limits",
                                options.runtime_node_resource_updates);
    }
  }
  ParseResourceProfiles(scenario, &options);

  const boost::json::value* process = scenario.if_contains("process");
  if (process != nullptr) {
    if (!process->is_object()) {
      throw std::runtime_error("scenario process must be a JSON object");
    }
    ProcessControlConfig config =
        ParseProcessControlConfig(process->as_object(), options.nodes);
    options.runtime_node_restarts.insert(options.runtime_node_restarts.end(),
                                         config.restart_node_indexes.begin(),
                                         config.restart_node_indexes.end());
    options.runtime_node_freezes.insert(options.runtime_node_freezes.end(),
                                        config.freezes.begin(),
                                        config.freezes.end());
  }

  ParseNetworkProfiles(scenario, &options);
  const boost::json::value* network = scenario.if_contains("network");
  if (network != nullptr) {
    if (!network->is_object()) {
      throw std::runtime_error("scenario network must be a JSON object");
    }
    const boost::json::object& object = network->as_object();
    RejectUnsupportedFields(object,
                            ScenarioObjectFields(ScenarioObjectKind::kNetwork),
                            "scenario network");
    if (!OptionProvided(vm, "isolate-network") &&
        !OptionProvided(vm, "no-isolate-network")) {
      options.isolate_network =
          JsonOptionalBoolField(object, "isolated", options.isolate_network);
    }
    const boost::json::value* default_condition =
        object.if_contains("default_condition");
    if (default_condition != nullptr) {
      if (!default_condition->is_object()) {
        throw std::runtime_error(
            "scenario network.default_condition must be a JSON object");
      }
      RejectUnsupportedFields(
          default_condition->as_object(),
          ScenarioObjectFields(ScenarioObjectKind::kNetworkCondition),
          "scenario network.default_condition");
      const NetworkCondition scenario_condition =
          ParseNetworkConditionObject(default_condition->as_object());
      if (!OptionProvided(vm, "network-bandwidth-kbps")) {
        options.network_condition.bandwidth_kbps =
            scenario_condition.bandwidth_kbps;
      }
      if (!OptionProvided(vm, "network-delay-ms")) {
        options.network_condition.delay_ms = scenario_condition.delay_ms;
      }
      if (!OptionProvided(vm, "network-jitter-ms")) {
        options.network_condition.jitter_ms = scenario_condition.jitter_ms;
      }
      if (!OptionProvided(vm, "network-loss-bps")) {
        options.network_condition.loss_basis_points =
            scenario_condition.loss_basis_points;
      }
      if (!OptionProvided(vm, "network-duplicate-bps")) {
        options.network_condition.duplicate_basis_points =
            scenario_condition.duplicate_basis_points;
      }
      if (!OptionProvided(vm, "network-corrupt-bps")) {
        options.network_condition.corrupt_basis_points =
            scenario_condition.corrupt_basis_points;
      }
      if (!OptionProvided(vm, "network-reorder-bps")) {
        options.network_condition.reorder_basis_points =
            scenario_condition.reorder_basis_points;
      }
      if (!OptionProvided(vm, "network-limit-packets")) {
        options.network_condition.limit_packets =
            scenario_condition.limit_packets;
      }
      options.network_condition_requested = true;
    }
    const boost::json::value* node_conditions =
        object.if_contains("node_conditions");
    if (node_conditions != nullptr) {
      if (!node_conditions->is_array()) {
        throw std::runtime_error(
            "scenario network.node_conditions must be a JSON array");
      }
      ApplyNodeConditions(node_conditions->as_array(), options.nodes,
                          "scenario network.node_conditions",
                          options.node_network_conditions);
    }
    const boost::json::value* runtime_node_conditions =
        object.if_contains("runtime_node_conditions");
    if (runtime_node_conditions != nullptr) {
      if (!runtime_node_conditions->is_array()) {
        throw std::runtime_error(
            "scenario network.runtime_node_conditions must be a JSON array");
      }
      ApplyNodeConditions(runtime_node_conditions->as_array(), options.nodes,
                          "scenario network.runtime_node_conditions",
                          options.runtime_node_network_conditions);
    }
    const boost::json::value* runtime_node_blocks =
        object.if_contains("runtime_node_blocks");
    if (runtime_node_blocks != nullptr) {
      if (!runtime_node_blocks->is_array()) {
        throw std::runtime_error(
            "scenario network.runtime_node_blocks must be a JSON array");
      }
      ApplyNetworkBlockRules(runtime_node_blocks->as_array(), options.nodes,
                             "scenario network.runtime_node_blocks",
                             options.runtime_node_blocks);
    }
    const boost::json::value* runtime_node_unblocks =
        object.if_contains("runtime_node_unblocks");
    if (runtime_node_unblocks != nullptr) {
      if (!runtime_node_unblocks->is_array()) {
        throw std::runtime_error(
            "scenario network.runtime_node_unblocks must be a JSON array");
      }
      ApplyNetworkBlockRules(runtime_node_unblocks->as_array(), options.nodes,
                             "scenario network.runtime_node_unblocks",
                             options.runtime_node_unblocks);
    }
    const boost::json::value* runtime_partitions =
        object.if_contains("runtime_partitions");
    if (runtime_partitions != nullptr) {
      if (!runtime_partitions->is_array()) {
        throw std::runtime_error(
            "scenario network.runtime_partitions must be a JSON array");
      }
      ApplyNetworkPartitionRules(runtime_partitions->as_array(), options.nodes,
                                 "scenario network.runtime_partitions",
                                 options.runtime_partitions);
    }
    const boost::json::value* runtime_partition_heals =
        object.if_contains("runtime_partition_heals");
    if (runtime_partition_heals != nullptr) {
      if (!runtime_partition_heals->is_array()) {
        throw std::runtime_error(
            "scenario network.runtime_partition_heals must be a JSON array");
      }
      ApplyNetworkPartitionRules(runtime_partition_heals->as_array(),
                                 options.nodes,
                                 "scenario network.runtime_partition_heals",
                                 options.runtime_partition_heals);
    }
  }
  ResolveNodeProfileAssignments(&options);
  ValidateProfileSwitchReferences(&options);
}

}  // namespace bbp::simulator_app_internal
