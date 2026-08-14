#pragma once

#include <sys/types.h>

#include <cstdint>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

#include "bbp/cgroup.h"
#include "bbp/drivers/chain_driver.h"
#include "bbp/network.h"
#include "bbp/perf_counter.h"

namespace bbp::simulator_app_internal {

struct NodeRuntimeMetrics {
  std::uint32_t node_index = 0;
  std::string chain;
  std::string role;
  std::string lifecycle;
  pid_t pid = -1;
  bool pidfd_available = false;
  bool process_running = false;
  std::optional<int> exit_status;
  std::optional<std::uint64_t> uptime_ms;
  std::string cgroup_path;
  std::string data_dir;
  std::string log_dir;
  std::string rpc_host;
  std::uint16_t rpc_port = 0;
  std::optional<std::uint64_t> network_namespace_inode;
  pid_t network_namespace_helper_pid = -1;
  std::string host_interface;
  std::string child_interface;
  std::string host_address;
  std::string node_address;
  std::uint8_t prefix_length = 0;
  std::vector<RouteInfo> routes;
  std::vector<PerfCounterKind> perf_counter_kinds;
  PerfCounterTargetKind perf_counter_target_kind = PerfCounterTargetKind::kNode;
  std::string perf_counter_target_id;
  pid_t perf_counter_target_pid = -1;
  pid_t perf_counter_attached_pid = -1;
  std::uint64_t perf_counter_process_generation = 0;
  std::string perf_counter_cgroup_path;
  std::vector<int> perf_counter_cpus;
  bool perf_counters_available = false;
  std::optional<PerfCounterErrorKind> perf_counter_error_kind;
  std::string perf_counter_error;
  std::vector<PerfCounterValue> perf_counter_values;
};

std::string MetricsJson(
    const std::string& run_id, const std::string& node_id,
    const NodeRuntimeMetrics& runtime, const ChainMetrics& chain,
    std::uint64_t generated_block_count, std::uint64_t mined_transaction_count,
    bool mined_transaction_count_complete, std::uint64_t restart_count,
    std::string_view resource_profile, std::string_view network_profile,
    const NetworkCondition* network_condition, const CgroupMetrics* cgroup,
    const LinkInfo* link, const QdiscInfo* qdisc,
    const std::vector<QdiscInfo>* qdisc_tree,
    const std::vector<TcFilterInfo>* filters,
    const DirectionalNetworkPolicyStats* directional_stats);

}  // namespace bbp::simulator_app_internal
