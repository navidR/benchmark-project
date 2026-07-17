#pragma once

#include <chrono>
#include <cstdint>
#include <filesystem>
#include <map>
#include <optional>
#include <string>
#include <vector>

#include "bbp/block_production_config.h"
#include "bbp/chain_kind.h"
#include "bbp/log_level.h"
#include "bbp/network.h"
#include "bbp/scenario_chain.h"
#include "bbp/scenario_node_config.h"
#include "bbp/simulation_network_address_plan.h"
#include "bbp/simulation_policy.h"
#include "bbp/simulation_registry.h"
#include "bbp/simulation_time_scale.h"
#include "bbp/simulator/freeze_request.h"
#include "bbp/simulator/network_block_rule.h"
#include "bbp/simulator/network_partition_rule.h"
#include "bbp/simulator/resource_limit_patch.h"
#include "bbp/simulator/resource_limits.h"
#include "bbp/simulator/scenario_workload.h"
#include "bbp/simulator/scheduled_scenario_event.h"
#include "bbp/util.h"

namespace bbp {

struct Options {
  ChainKind chain = ChainKind::kFiro;
  LogLevel log_level = LogLevel::kInfo;
  std::filesystem::path scenario;
  std::filesystem::path scenario_json;
  std::filesystem::path scenario_yaml;
  std::filesystem::path chain_daemon;
  bool chain_daemon_cli_override = false;
  std::map<std::string, ScenarioChain> chains;
  std::filesystem::path output_dir = "runs";
  std::filesystem::path report_run;
  std::filesystem::path tui_run;
  std::string run_id = MakeRunId();
  std::string simulation_name;
  std::uint64_t simulation_seed = 0U;
  std::optional<std::chrono::milliseconds> simulation_duration;
  SimulationTimeScale time_scale = SimulationTimeScale::FromDouble(1.0);
  CleanupPolicy cleanup_policy = CleanupPolicy::kAutomatic;
  PrivilegeMode privilege_mode = PrivilegeMode::kDirect;
  LogRetentionPolicy log_retention_policy = LogRetentionPolicy::kPreserve;
  std::uint32_t nodes = 1;
  std::vector<std::string> node_ids;
  std::vector<std::string> node_roles;
  std::vector<ScenarioNodeConfig> scenario_node_configs;
  std::uint32_t generate_node = 1;
  std::uint32_t ready_timeout_sec = 30;
  std::uint32_t sync_timeout_sec = 30;
  std::uint32_t tui_refresh_ms = 1000;
  std::uint32_t metrics_sample_count = 0;
  std::chrono::milliseconds metrics_interval = std::chrono::seconds(1);
  std::uint64_t memory_high_bytes = 1536ULL * 1024ULL * 1024ULL;
  std::uint64_t memory_max_bytes = 2ULL * 1024ULL * 1024ULL * 1024ULL;
  std::uint64_t cpu_period_us = 100000;
  std::uint64_t cpu_quota_us = 0;
  bool cpu_quota_requested = false;
  std::uint64_t cpu_weight = 100;
  std::uint64_t io_weight = 100;
  std::vector<IoLimit> io_limits;
  std::uint64_t pids_max = 256;
  std::map<std::string, ResourceLimits> resource_profiles;
  std::map<std::string, NetworkCondition> network_profiles;
  std::map<std::uint32_t, std::string> node_resource_profiles;
  std::map<std::uint32_t, std::string> node_network_profiles;
  std::map<std::uint32_t, ResourceLimits> node_resource_limits;
  bool keep_artifacts = true;
  bool cleanup_run = false;
  bool no_tui = false;
  bool tui_once = false;
  bool isolate_network = false;
  std::optional<SimulationNetworkAddressPlan> network_address_plan;
  bool network_condition_requested = false;
  NetworkCondition network_condition;
  std::map<std::uint32_t, NetworkCondition> node_network_conditions;
  std::map<std::uint32_t, NetworkCondition> runtime_node_network_conditions;
  std::map<std::uint32_t, ResourceLimitPatch> runtime_node_resource_updates;
  std::vector<NetworkBlockRule> runtime_node_blocks;
  std::vector<NetworkBlockRule> runtime_node_unblocks;
  std::vector<NetworkPartitionRule> runtime_partitions;
  std::vector<NetworkPartitionRule> runtime_partition_heals;
  std::vector<std::uint32_t> runtime_node_restarts;
  std::vector<FreezeRequest> runtime_node_freezes;
  std::vector<ScenarioWorkload> workloads;
  std::vector<ScheduledScenarioEvent> scheduled_events;
  bool workloads_configured = false;
  bool wallet_backed_workload_requested = false;
  bool replace_run = false;
  bool probe_address = false;
  bool probe_bandwidth_limit = false;
  bool probe_capabilities = false;
  bool probe_cgroup_freeze = false;
  bool probe_drop_filter = false;
  bool probe_directional_network_condition = false;
  bool probe_netns = false;
  bool probe_combined_network_condition = false;
  bool probe_network_condition = false;
  bool probe_network_condition_update = false;
  bool probe_qdisc = false;
  bool probe_qdisc_mutation = false;
  bool probe_route = false;
  bool probe_veth = false;
  bool probe_network = false;
  NodeRoleTopology topology;
  WalletInitialization wallet_initialization;
  BlockProductionConfig block_production;
};

}  // namespace bbp
