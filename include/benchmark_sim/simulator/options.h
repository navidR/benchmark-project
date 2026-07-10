#pragma once

#include <cstdint>
#include <filesystem>
#include <map>
#include <string>
#include <vector>

#include "benchmark_sim/block_production_config.h"
#include "benchmark_sim/network.h"
#include "benchmark_sim/simulation_registry.h"
#include "benchmark_sim/simulator/freeze_request.h"
#include "benchmark_sim/simulator/network_block_rule.h"
#include "benchmark_sim/simulator/network_partition_rule.h"
#include "benchmark_sim/simulator/resource_limit_patch.h"
#include "benchmark_sim/simulator/scenario_workload.h"
#include "benchmark_sim/util.h"

namespace bsim {

struct Options {
  std::filesystem::path scenario_json;
  std::filesystem::path scenario_yaml;
  std::filesystem::path chain_daemon;
  std::filesystem::path output_dir = "runs";
  std::filesystem::path report_run;
  std::filesystem::path tui_run;
  std::string run_id = MakeRunId();
  std::uint32_t nodes = 1;
  std::uint32_t generate_node = 1;
  std::uint32_t ready_timeout_sec = 30;
  std::uint32_t sync_timeout_sec = 30;
  std::uint32_t tui_refresh_ms = 1000;
  std::uint32_t metrics_sample_count = 0;
  std::uint32_t metrics_interval_ms = 1000;
  std::uint64_t memory_high_bytes = 1536ULL * 1024ULL * 1024ULL;
  std::uint64_t memory_max_bytes = 2ULL * 1024ULL * 1024ULL * 1024ULL;
  std::uint64_t cpu_period_us = 100000;
  std::uint64_t cpu_quota_us = 0;
  bool cpu_quota_requested = false;
  std::uint64_t pids_max = 256;
  bool keep_cgroups = false;
  bool cleanup_run = false;
  bool no_tui = false;
  bool tui_once = false;
  bool isolate_network = false;
  bool network_condition_requested = false;
  NetworkCondition network_condition;
  std::vector<std::string> node_network_condition_json;
  std::vector<std::string> runtime_node_network_condition_json;
  std::vector<std::string> runtime_node_block_json;
  std::vector<std::string> runtime_node_unblock_json;
  std::vector<std::string> runtime_partition_json;
  std::vector<std::string> runtime_heal_partition_json;
  std::vector<std::string> runtime_node_resource_json;
  std::vector<std::string> runtime_node_restart_json;
  std::vector<std::string> runtime_node_freeze_json;
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
  bool workloads_configured = false;
  bool wallet_backed_workload_requested = false;
  bool replace_run = false;
  bool probe_address = false;
  bool probe_bandwidth_limit = false;
  bool probe_capabilities = false;
  bool probe_cgroup_freeze = false;
  bool probe_drop_filter = false;
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

}  // namespace bsim
