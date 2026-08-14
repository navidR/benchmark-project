#include "simulator_metrics_json.h"

#include <boost/json/array.hpp>
#include <boost/json/object.hpp>
#include <boost/json/serialize.hpp>
#include <utility>

#include "bbp/util.h"
#include "simulator_host_probes.h"
#include "simulator_scenario_serialization.h"

namespace bbp::simulator_app_internal {
namespace {

boost::json::object NetworkPolicyCounterJson(const TcFilterInfo& filter) {
  boost::json::object object;
  object["kind"] = "ipv4_tcp_drop";
  object["handle"] = filter.handle;
  if (filter.has_ipv4_src) {
    object["src_address"] = filter.ipv4_src;
  } else {
    object["src_address"] = nullptr;
  }
  if (filter.has_tcp_src) {
    object["src_port"] = filter.tcp_src;
  } else {
    object["src_port"] = nullptr;
  }
  object["dst_address"] = filter.ipv4_dst;
  object["dst_port"] = filter.tcp_dst;
  object["has_stats"] = filter.has_stats;
  object["match_bytes"] = filter.match_bytes;
  object["match_packets"] = filter.match_packets;
  object["drop_packets"] = filter.drop_packets;
  return object;
}

boost::json::object DirectionalNetworkPolicyCounterJson(
    const DirectionalNetworkPolicyCounter& counter) {
  boost::json::object object;
  object["band"] = counter.band;
  object["destination_address"] = counter.destination_address;
  object["filter_handle"] = counter.filter.handle;
  object["filter_has_stats"] = counter.filter.has_stats;
  object["filter_match_bytes"] = counter.filter.match_bytes;
  object["filter_match_packets"] = counter.filter.match_packets;
  object["qdisc_bytes"] = counter.qdisc_bytes;
  object["qdisc_packets"] = counter.qdisc_packets;
  object["qdisc_drops"] = counter.qdisc_drops;
  object["qdisc_overlimits"] = counter.qdisc_overlimits;
  object["qdisc_qlen"] = counter.qdisc_qlen;
  object["qdisc_backlog"] = counter.qdisc_backlog;
  object["qdisc_requeues"] = counter.qdisc_requeues;
  object["qdiscs"] = QdiscsJson(counter.qdiscs);
  return object;
}

boost::json::array PerfCounterValuesJson(
    const std::vector<PerfCounterValue>& values) {
  boost::json::array counters;
  counters.reserve(values.size());
  for (const PerfCounterValue& value : values) {
    boost::json::object counter;
    counter["name"] = PerfCounterKindName(value.kind);
    counter["raw_value"] = value.raw_value;
    if (value.scaled_value) {
      counter["scaled_value"] = *value.scaled_value;
    } else {
      counter["scaled_value"] = nullptr;
    }
    counter["time_enabled_ns"] = value.time_enabled_ns;
    counter["time_running_ns"] = value.time_running_ns;
    counter["multiplexed"] = value.multiplexed;
    counter["scaled"] = value.scaled;
    counter["scaled_overflow"] = value.scaled_overflow;
    counters.push_back(std::move(counter));
  }
  return counters;
}

}  // namespace

std::string MetricsJson(
    const std::string& run_id, const std::string& node_id,
    const NodeRuntimeMetrics& runtime, const ChainMetrics& chain,
    uint64_t generated_block_count, uint64_t mined_transaction_count,
    bool mined_transaction_count_complete, uint64_t restart_count,
    std::string_view resource_profile, std::string_view network_profile,
    const NetworkCondition* network_condition, const CgroupMetrics* cgroup,
    const LinkInfo* link, const QdiscInfo* qdisc,
    const std::vector<QdiscInfo>* qdisc_tree,
    const std::vector<TcFilterInfo>* filters,
    const DirectionalNetworkPolicyStats* directional_stats) {
  boost::json::object object;
  object["timestamp_ms"] = NowUnixMillis();
  object["run_id"] = run_id;
  object["node_id"] = node_id;
  object["node_index"] = runtime.node_index;
  object["chain"] = runtime.chain;
  object["role"] = runtime.role;
  object["lifecycle"] = runtime.lifecycle;
  object["pid"] = runtime.pid;
  object["pidfd_available"] = runtime.pidfd_available;
  object["process_group"] = runtime.pid;
  object["process_running"] = runtime.process_running;
  if (runtime.exit_status) {
    object["exit_status"] = *runtime.exit_status;
  } else {
    object["exit_status"] = nullptr;
  }
  if (runtime.uptime_ms) {
    object["uptime_ms"] = *runtime.uptime_ms;
  } else {
    object["uptime_ms"] = nullptr;
  }
  object["restart_count"] = restart_count;
  object["perf_counter_names"] =
      PerfCounterNamesJson(runtime.perf_counter_kinds);
  object["perf_counter_target_kind"] =
      PerfCounterTargetKindName(runtime.perf_counter_target_kind);
  object["perf_counter_target_id"] = runtime.perf_counter_target_id;
  if (runtime.perf_counter_target_pid > 0) {
    object["perf_counter_target_pid"] = runtime.perf_counter_target_pid;
  } else {
    object["perf_counter_target_pid"] = nullptr;
  }
  if (runtime.perf_counter_attached_pid > 0) {
    object["perf_counter_attached_pid"] = runtime.perf_counter_attached_pid;
  } else {
    object["perf_counter_attached_pid"] = nullptr;
  }
  object["perf_counter_process_generation"] =
      runtime.perf_counter_process_generation;
  if (runtime.perf_counter_cgroup_path.empty()) {
    object["perf_counter_cgroup_path"] = nullptr;
  } else {
    object["perf_counter_cgroup_path"] = runtime.perf_counter_cgroup_path;
  }
  boost::json::array perf_counter_cpus;
  perf_counter_cpus.reserve(runtime.perf_counter_cpus.size());
  for (const int cpu : runtime.perf_counter_cpus) {
    perf_counter_cpus.emplace_back(cpu);
  }
  object["perf_counter_cpus"] = std::move(perf_counter_cpus);
  object["perf_counters_available"] = runtime.perf_counters_available;
  if (runtime.perf_counter_error_kind) {
    object["perf_counter_error_kind"] =
        PerfCounterErrorKindName(*runtime.perf_counter_error_kind);
  } else {
    object["perf_counter_error_kind"] = nullptr;
  }
  if (runtime.perf_counter_error.empty()) {
    object["perf_counter_error"] = nullptr;
  } else {
    object["perf_counter_error"] = runtime.perf_counter_error;
  }
  object["perf_counters"] = PerfCounterValuesJson(runtime.perf_counter_values);
  object["cgroup_path"] = runtime.cgroup_path;
  object["data_dir"] = runtime.data_dir;
  object["log_dir"] = runtime.log_dir;
  object["rpc_host"] = runtime.rpc_host;
  object["rpc_port"] = runtime.rpc_port;
  if (runtime.network_namespace_inode) {
    object["network_namespace_inode"] = *runtime.network_namespace_inode;
  } else {
    object["network_namespace_inode"] = nullptr;
  }
  if (runtime.network_namespace_helper_pid > 0) {
    object["network_namespace_helper_pid"] =
        runtime.network_namespace_helper_pid;
  } else {
    object["network_namespace_helper_pid"] = nullptr;
  }
  if (runtime.host_interface.empty()) {
    object["host_interface"] = nullptr;
    object["child_interface"] = nullptr;
    object["host_address"] = nullptr;
    object["node_address"] = nullptr;
    object["network_prefix_length"] = nullptr;
    object["network_routes"] = boost::json::array{};
  } else {
    object["host_interface"] = runtime.host_interface;
    object["child_interface"] = runtime.child_interface;
    object["host_address"] = runtime.host_address;
    object["node_address"] = runtime.node_address;
    object["network_prefix_length"] = runtime.prefix_length;
    object["network_routes"] = RoutesJson(runtime.routes);
  }
  object["chain_version"] = chain.version;
  object["chain_protocol_version"] = chain.protocol_version;
  object["chain_subversion"] = chain.subversion;
  object["height"] = chain.height;
  object["best_hash"] = chain.best_hash;
  object["peer_count"] = chain.peer_count;
  boost::json::array peer_addresses;
  peer_addresses.reserve(chain.peer_addresses.size());
  for (const std::string& address : chain.peer_addresses) {
    peer_addresses.emplace_back(address);
  }
  object["peer_addresses"] = std::move(peer_addresses);
  object["mempool_tx_count"] = chain.mempool_tx_count;
  object["mempool_bytes"] = chain.mempool_bytes;
  object["generated_block_count"] = generated_block_count;
  object["mined_transaction_count"] = mined_transaction_count;
  object["mined_transaction_count_complete"] = mined_transaction_count_complete;
  if (resource_profile.empty()) {
    object["active_resource_profile"] = nullptr;
  } else {
    object["active_resource_profile"] = resource_profile;
  }
  if (network_profile.empty()) {
    object["active_network_profile"] = nullptr;
  } else {
    object["active_network_profile"] = network_profile;
  }
  if (network_condition == nullptr) {
    object["network_condition"] = nullptr;
  } else {
    object["network_condition"] = NetworkConditionJson(*network_condition);
  }
  if (chain.initial_block_download) {
    object["initial_block_download"] = *chain.initial_block_download;
  } else {
    object["initial_block_download"] = nullptr;
  }
  if (chain.headers) {
    object["headers"] = *chain.headers;
  } else {
    object["headers"] = nullptr;
  }
  object["sync_status"] = ChainSyncStatusName(chain.sync_status);
  if (chain.verification_progress) {
    object["verification_progress"] = *chain.verification_progress;
  } else {
    object["verification_progress"] = nullptr;
  }
  if (chain.difficulty) {
    object["difficulty"] = *chain.difficulty;
  } else {
    object["difficulty"] = nullptr;
  }
  if (chain.hashrate_estimate) {
    object["hashrate_estimate"] = *chain.hashrate_estimate;
  } else {
    object["hashrate_estimate"] = nullptr;
  }
  if (chain.last_block_time) {
    object["last_block_time"] = *chain.last_block_time;
  } else {
    object["last_block_time"] = nullptr;
  }
  if (chain.median_time) {
    object["median_time"] = *chain.median_time;
  } else {
    object["median_time"] = nullptr;
  }
  if (chain.chainwork) {
    object["chainwork"] = *chain.chainwork;
  } else {
    object["chainwork"] = nullptr;
  }
  if (chain.reorg_count) {
    object["reorg_count"] = *chain.reorg_count;
  } else {
    object["reorg_count"] = nullptr;
  }
  object["rpc_latency_ms"] = chain.rpc_latency_ms;
  if (cgroup != nullptr) {
    object["cpu_usage_usec"] = cgroup->cpu_usage_usec;
    object["cpu_throttled_usec"] = cgroup->cpu_throttled_usec;
    object["cpu_pressure_some_total_usec"] =
        cgroup->cpu_pressure_some_total_usec;
    object["cpu_pressure_full_total_usec"] =
        cgroup->cpu_pressure_full_total_usec;
    object["memory_current"] = cgroup->memory_current;
    object["memory_peak"] = cgroup->memory_peak;
    if (cgroup->memory_high_limit_bytes) {
      object["memory_high_limit_bytes"] = *cgroup->memory_high_limit_bytes;
    } else {
      object["memory_high_limit_bytes"] = nullptr;
    }
    if (cgroup->memory_max_limit_bytes) {
      object["memory_max_limit_bytes"] = *cgroup->memory_max_limit_bytes;
    } else {
      object["memory_max_limit_bytes"] = nullptr;
    }
    if (cgroup->cpu_quota_us) {
      object["cpu_quota_us"] = *cgroup->cpu_quota_us;
    } else {
      object["cpu_quota_us"] = nullptr;
    }
    object["cpu_period_us"] = cgroup->cpu_period_us;
    object["cpu_weight"] = cgroup->cpu_weight;
    object["io_weight"] = cgroup->io_weight;
    object["io_max"] = IoLimitsJson(cgroup->io_limits);
    object["io_read_bytes"] = cgroup->io_read_bytes;
    object["io_write_bytes"] = cgroup->io_write_bytes;
    object["io_read_operations"] = cgroup->io_read_operations;
    object["io_write_operations"] = cgroup->io_write_operations;
    object["io_discard_bytes"] = cgroup->io_discard_bytes;
    object["io_discard_operations"] = cgroup->io_discard_operations;
    object["io_pressure_some_total_usec"] = cgroup->io_pressure_some_total_usec;
    object["io_pressure_full_total_usec"] = cgroup->io_pressure_full_total_usec;
    object["pids_current"] = cgroup->pids_current;
    if (cgroup->pids_max_limit) {
      object["pids_max_limit"] = *cgroup->pids_max_limit;
    } else {
      object["pids_max_limit"] = nullptr;
    }
    object["pids_max_events"] = cgroup->pids_max_events;
    object["cgroup_populated"] = cgroup->cgroup_populated;
    object["cgroup_frozen"] = cgroup->cgroup_frozen;
    object["memory_low"] = cgroup->memory_low;
    object["memory_high"] = cgroup->memory_high;
    object["memory_max"] = cgroup->memory_max;
    object["oom"] = cgroup->oom;
    object["oom_kill"] = cgroup->oom_kill;
    object["oom_group_kill"] = cgroup->oom_group_kill;
    boost::json::object memory_stat;
    for (const auto& [name, value] : cgroup->memory_stat) {
      memory_stat[name] = value;
    }
    object["memory_stat"] = std::move(memory_stat);
  }
  if (link != nullptr) {
    object["network_has_stats"] = link->has_stats;
    object["network_rx_bytes"] = link->rx_bytes;
    object["network_tx_bytes"] = link->tx_bytes;
    object["network_rx_packets"] = link->rx_packets;
    object["network_tx_packets"] = link->tx_packets;
    object["network_rx_dropped"] = link->rx_dropped;
    object["network_tx_dropped"] = link->tx_dropped;
    object["network_rx_errors"] = link->rx_errors;
    object["network_tx_errors"] = link->tx_errors;
  }
  if (qdisc != nullptr) {
    object["qdisc_kind"] = qdisc->kernel_kind;
    object["qdisc_handle"] = qdisc->handle;
    object["qdisc_parent"] = qdisc->parent;
    object["qdisc_has_stats"] = qdisc->has_stats;
    object["qdisc_bytes"] = qdisc->bytes;
    object["qdisc_packets"] = qdisc->packets;
    object["qdisc_drops"] = qdisc->drops;
    object["qdisc_overlimits"] = qdisc->overlimits;
    object["qdisc_qlen"] = qdisc->qlen;
    object["qdisc_backlog"] = qdisc->backlog;
    object["qdisc_requeues"] = qdisc->requeues;
    object["qdisc_has_netem_options"] = qdisc->has_netem_options;
    object["qdisc_netem_latency_us"] = qdisc->netem_latency_us;
    object["qdisc_netem_jitter_us"] = qdisc->netem_jitter_us;
    object["qdisc_netem_loss"] = qdisc->netem_loss;
    object["qdisc_netem_duplicate"] = qdisc->netem_duplicate;
    object["qdisc_netem_corrupt"] = qdisc->netem_corrupt;
    object["qdisc_netem_reorder"] = qdisc->netem_reorder;
    object["qdisc_netem_limit_packets"] = qdisc->netem_limit_packets;
    object["qdisc_has_tbf_options"] = qdisc->has_tbf_options;
    object["qdisc_tbf_rate_bytes_per_sec"] = qdisc->tbf_rate_bytes_per_sec;
    object["qdisc_tbf_limit_bytes"] = qdisc->tbf_limit_bytes;
    object["qdisc_tbf_buffer_ticks"] = qdisc->tbf_buffer_ticks;
    object["qdisc_tbf_mtu_ticks"] = qdisc->tbf_mtu_ticks;
  }
  if (qdisc_tree != nullptr) {
    object["network_qdiscs"] = QdiscsJson(*qdisc_tree);
  } else {
    object["network_qdiscs"] = boost::json::array{};
  }
  if (filters != nullptr) {
    const TcFilterStatsSummary summary =
        SummarizeEgressIpv4TcpDropPolicies(*filters, link->name);
    object["network_filter_policy_count"] = summary.policy_count;
    object["network_filter_policies_with_stats"] = summary.policies_with_stats;
    object["network_filter_match_bytes"] = summary.match_bytes;
    object["network_filter_match_packets"] = summary.match_packets;
    object["network_filter_drop_packets"] = summary.drop_packets;
    boost::json::array policies;
    for (const TcFilterInfo& filter : *filters) {
      if (TcFilterIsEgressIpv4TcpDropPolicy(filter, link->name)) {
        policies.push_back(NetworkPolicyCounterJson(filter));
      }
    }
    object["network_policy_counters"] = policies;
    object["network_active_block_rules"] = std::move(policies);
  }
  if (directional_stats != nullptr) {
    object["directional_network_policy_count"] =
        directional_stats->policy_count;
    object["directional_network_policies_with_filter_stats"] =
        directional_stats->policies_with_filter_stats;
    object["directional_network_filter_match_bytes"] =
        directional_stats->filter_match_bytes;
    object["directional_network_filter_match_packets"] =
        directional_stats->filter_match_packets;
    object["directional_network_qdisc_count"] = directional_stats->qdisc_count;
    object["directional_network_qdiscs_with_stats"] =
        directional_stats->qdiscs_with_stats;
    object["directional_network_qdisc_bytes"] = directional_stats->qdisc_bytes;
    object["directional_network_qdisc_packets"] =
        directional_stats->qdisc_packets;
    object["directional_network_qdisc_drops"] = directional_stats->qdisc_drops;
    object["directional_network_qdisc_overlimits"] =
        directional_stats->qdisc_overlimits;
    object["directional_network_qdisc_qlen"] = directional_stats->qdisc_qlen;
    object["directional_network_qdisc_backlog"] =
        directional_stats->qdisc_backlog;
    object["directional_network_qdisc_requeues"] =
        directional_stats->qdisc_requeues;
    boost::json::array policies;
    policies.reserve(directional_stats->policies.size());
    for (const DirectionalNetworkPolicyCounter& counter :
         directional_stats->policies) {
      policies.push_back(DirectionalNetworkPolicyCounterJson(counter));
    }
    object["directional_network_policy_counters"] = std::move(policies);
  }
  return boost::json::serialize(object);
}

}  // namespace bbp::simulator_app_internal
