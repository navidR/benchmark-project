#include "bbp/run_report.h"

#include <sys/stat.h>

#include <algorithm>
#include <boost/json/array.hpp>
#include <boost/json/object.hpp>
#include <boost/json/parse.hpp>
#include <boost/json/serialize.hpp>
#include <boost/json/value.hpp>
#include <cerrno>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <initializer_list>
#include <iterator>
#include <limits>
#include <map>
#include <optional>
#include <set>
#include <stdexcept>
#include <string>
#include <string_view>
#include <system_error>
#include <utility>
#include <vector>

#include "bbp/operator_command_status.h"
#include "bbp/simulation_command.h"
#include "bbp/simulation_event_kind.h"
#include "bbp/simulation_registry.h"
#include "bbp/simulator/node_runtime_lifecycle.h"
#include "bbp/util.h"

namespace bbp {
namespace {

constexpr std::size_t kMaximumNodeLogTailBytes = 256U * 1024U;
constexpr std::size_t kMaximumResolvedScenarioBytes = 4U * 1024U * 1024U;
constexpr std::size_t kMaximumNodeMetricHistorySamples = 120U;
constexpr std::size_t kMaximumOperatorCommandSummaries =
    kMaximumRunReportSummaryRecords;
constexpr std::size_t kMaximumScheduledBlockSummaries =
    kMaximumRunReportSummaryRecords;
constexpr std::size_t kMaximumScheduledEventSummaries =
    kMaximumRunReportSummaryRecords;
constexpr std::size_t kMaximumCheckpointSummaries =
    kMaximumRunReportSummaryRecords;
constexpr std::size_t kMaximumDirectionalPolicyVerifications =
    kMaximumRunReportSummaryRecords;
constexpr std::size_t kMaximumTopologyEdgeSummaries =
    kMaximumRunReportSummaryRecords;
constexpr std::size_t kMaximumProfileUpdateSummaries =
    kMaximumRunReportSummaryRecords;
constexpr std::size_t kMaximumWalletTransactionSummaries =
    kMaximumRunReportSummaryRecords;
constexpr std::size_t kMaximumTransactionLoadAttemptSummaries =
    kMaximumRunReportSummaryRecords;
constexpr std::size_t kMaximumTransactionLoadCompletionSummaries =
    kMaximumRunReportSummaryRecords;

void ThrowIfReportCancelled(std::stop_token stop_token) {
  if (stop_token.stop_requested()) {
    throw std::runtime_error("run report refresh was cancelled");
  }
}

struct NodeReport {
  std::optional<std::uint64_t> index;
  std::string chain;
  std::string role;
  std::string last_error;
  std::optional<SimulationEventKind> last_error_kind;
  std::uint64_t metric_samples = 0;
  std::optional<NodeRuntimeLifecycle> final_state;
  std::string unknown_final_state;
  boost::json::object last_metrics;
  boost::json::array metrics_history;
  boost::json::object log_tails;
  std::optional<std::uint64_t> previous_timestamp_ms;
  std::optional<std::uint64_t> previous_network_rx_bytes;
  std::optional<std::uint64_t> previous_network_tx_bytes;
  std::optional<std::uint64_t> previous_cpu_usage_usec;
  std::optional<std::uint64_t> previous_cpu_throttled_usec;
  std::optional<std::uint64_t> previous_io_read_bytes;
  std::optional<std::uint64_t> previous_io_write_bytes;
};

struct WalletReport {
  std::uint64_t wallet_index = 0;
  std::uint64_t node = 0;
  std::string address;
  std::string funding_address;
  std::optional<WalletInitializationStrategy> strategy;
  std::string unknown_strategy;
  std::optional<WalletPrivacyMode> mode;
  std::string unknown_mode;
  std::uint64_t transactions_sent = 0;
  std::uint64_t transactions_received = 0;
  std::uint64_t simulated_amount_sent_satoshis = 0;
  std::uint64_t simulated_amount_received_satoshis = 0;
  boost::json::object last_sent_transaction;
  boost::json::object last_received_transaction;
  boost::json::object last_funding;
  boost::json::object last_metrics;
};

enum class RunReportStatus {
  kFinished,
  kFailed,
  kCancelled,
  kIncomplete,
};

std::string_view RunReportStatusName(RunReportStatus status) {
  switch (status) {
    case RunReportStatus::kFinished:
      return "finished";
    case RunReportStatus::kFailed:
      return "failed";
    case RunReportStatus::kCancelled:
      return "cancelled";
    case RunReportStatus::kIncomplete:
      return "incomplete";
  }
  return "incomplete";
}

std::string OptionalStringField(const boost::json::object& object,
                                std::string_view field) {
  const boost::json::value* value = object.if_contains(field);
  if (value == nullptr || !value->is_string()) {
    return "";
  }
  return std::string(value->as_string());
}

std::optional<std::uint64_t> OptionalUint64Field(
    const boost::json::object& object, std::string_view field) {
  const boost::json::value* value = object.if_contains(field);
  if (value == nullptr) {
    return std::nullopt;
  }
  if (value->is_uint64()) {
    return value->as_uint64();
  }
  if (value->is_int64() && value->as_int64() >= 0) {
    return static_cast<std::uint64_t>(value->as_int64());
  }
  return std::nullopt;
}

void CopyField(const boost::json::object& source, std::string_view field,
               boost::json::object* target) {
  const boost::json::value* value = source.if_contains(field);
  if (value != nullptr) {
    (*target)[field] = *value;
  }
}

boost::json::value ParseEventDetail(const boost::json::object& event) {
  const std::string detail = OptionalStringField(event, "detail");
  if (detail.empty()) {
    return nullptr;
  }
  try {
    return boost::json::parse(detail);
  } catch (...) {
    return boost::json::value(boost::json::string(detail));
  }
}

bool IsNodeErrorEvent(SimulationEventKind kind) {
  switch (kind) {
    case SimulationEventKind::kProcessExitedBeforeRpcReady:
    case SimulationEventKind::kResourcePressureRestoredAfterError:
    case SimulationEventKind::kProfileUpdateRollbackFailed:
    case SimulationEventKind::kTopologyEdgeUpdateRollbackFailed:
    case SimulationEventKind::kCgroupRemoveFailed:
    case SimulationEventKind::kScheduledEventFailed:
    case SimulationEventKind::kScheduledBlockFailed:
    case SimulationEventKind::kPeerPolicyEnforcementFailed:
    case SimulationEventKind::kOperatorCommandFailed:
    case SimulationEventKind::kMetricsNodeUnavailable:
    case SimulationEventKind::kWalletMetricsUnavailable:
      return true;
    default:
      return false;
  }
}

bool IsRpcRecoveryErrorEvent(SimulationEventKind kind) {
  return kind == SimulationEventKind::kProcessExitedBeforeRpcReady ||
         kind == SimulationEventKind::kMetricsNodeUnavailable ||
         kind == SimulationEventKind::kWalletMetricsUnavailable;
}

std::string NodeErrorText(const boost::json::object& event) {
  boost::json::value detail = ParseEventDetail(event);
  std::string text;
  if (detail.is_object()) {
    const boost::json::object& object = detail.as_object();
    text = OptionalStringField(object, "error");
    if (text.empty()) {
      text = OptionalStringField(object, "original_error");
    }
  }
  if (text.empty() && detail.is_string()) {
    text = std::string(detail.as_string());
  }
  if (text.empty() && !detail.is_null()) {
    text = boost::json::serialize(detail);
  }
  const std::string event_name = OptionalStringField(event, "event");
  if (text.empty()) {
    return event_name;
  }
  constexpr std::size_t kMaximumLastErrorBytes = 1024U;
  if (text.size() > kMaximumLastErrorBytes) {
    text.resize(kMaximumLastErrorBytes);
  }
  return event_name + ": " + text;
}

void CopySelectedMetricFields(const boost::json::object& source,
                              boost::json::object* target) {
  constexpr std::string_view kFields[] = {
      "timestamp_ms",
      "node_index",
      "chain",
      "role",
      "lifecycle",
      "pid",
      "pidfd_available",
      "process_group",
      "process_running",
      "exit_status",
      "uptime_ms",
      "cgroup_path",
      "data_dir",
      "log_dir",
      "rpc_host",
      "rpc_port",
      "network_namespace_inode",
      "network_namespace_helper_pid",
      "host_interface",
      "child_interface",
      "host_address",
      "node_address",
      "network_prefix_length",
      "network_routes",
      "height",
      "headers",
      "best_hash",
      "peer_count",
      "peer_addresses",
      "mempool_tx_count",
      "mempool_bytes",
      "generated_block_count",
      "mined_transaction_count",
      "mined_transaction_count_complete",
      "restart_count",
      "perf_counter_names",
      "perf_counter_target_kind",
      "perf_counter_target_id",
      "perf_counter_target_pid",
      "perf_counter_attached_pid",
      "perf_counter_process_generation",
      "perf_counter_cgroup_path",
      "perf_counter_cpus",
      "perf_counters_available",
      "perf_counter_error_kind",
      "perf_counter_error",
      "perf_counters",
      "active_resource_profile",
      "active_network_profile",
      "network_condition",
      "initial_block_download",
      "sync_status",
      "verification_progress",
      "difficulty",
      "hashrate_estimate",
      "last_block_time",
      "median_time",
      "chainwork",
      "reorg_count",
      "rpc_latency_ms",
      "chain_version",
      "chain_protocol_version",
      "chain_subversion",
      "cpu_usage_usec",
      "cpu_throttled_usec",
      "cpu_pressure_some_total_usec",
      "cpu_pressure_full_total_usec",
      "memory_current",
      "memory_peak",
      "memory_high_limit_bytes",
      "memory_max_limit_bytes",
      "cpu_quota_us",
      "cpu_period_us",
      "cpu_weight",
      "io_weight",
      "io_max",
      "io_read_bytes",
      "io_write_bytes",
      "io_read_operations",
      "io_write_operations",
      "io_discard_bytes",
      "io_discard_operations",
      "io_pressure_some_total_usec",
      "io_pressure_full_total_usec",
      "pids_current",
      "pids_max_limit",
      "pids_max_events",
      "cgroup_populated",
      "cgroup_frozen",
      "memory_low",
      "memory_high",
      "memory_max",
      "oom",
      "oom_kill",
      "oom_group_kill",
      "memory_stat",
      "network_has_stats",
      "network_rx_bytes",
      "network_tx_bytes",
      "network_rx_packets",
      "network_tx_packets",
      "network_rx_dropped",
      "network_tx_dropped",
      "network_rx_errors",
      "network_tx_errors",
      "network_filter_policy_count",
      "network_filter_policies_with_stats",
      "network_filter_match_bytes",
      "network_filter_match_packets",
      "network_filter_drop_packets",
      "network_policy_counters",
      "network_active_block_rules",
      "directional_network_policy_count",
      "directional_network_policies_with_filter_stats",
      "directional_network_filter_match_bytes",
      "directional_network_filter_match_packets",
      "directional_network_qdisc_count",
      "directional_network_qdiscs_with_stats",
      "directional_network_qdisc_bytes",
      "directional_network_qdisc_packets",
      "directional_network_qdisc_drops",
      "directional_network_qdisc_overlimits",
      "directional_network_qdisc_qlen",
      "directional_network_qdisc_backlog",
      "directional_network_qdisc_requeues",
      "directional_network_policy_counters",
      "qdisc_kind",
      "qdisc_handle",
      "qdisc_parent",
      "qdisc_has_stats",
      "qdisc_bytes",
      "qdisc_packets",
      "qdisc_drops",
      "qdisc_overlimits",
      "qdisc_qlen",
      "qdisc_backlog",
      "qdisc_requeues",
      "qdisc_has_netem_options",
      "qdisc_netem_latency_us",
      "qdisc_netem_jitter_us",
      "qdisc_netem_loss",
      "qdisc_netem_duplicate",
      "qdisc_netem_corrupt",
      "qdisc_netem_reorder",
      "qdisc_netem_limit_packets",
      "qdisc_has_tbf_options",
      "qdisc_tbf_rate_bytes_per_sec",
      "qdisc_tbf_limit_bytes",
      "qdisc_tbf_buffer_ticks",
      "qdisc_tbf_mtu_ticks",
      "network_qdiscs",
  };
  for (std::string_view field : kFields) {
    CopyField(source, field, target);
  }
}

std::optional<std::uint64_t> BytesPerSecond(std::uint64_t current_bytes,
                                            std::uint64_t previous_bytes,
                                            std::uint64_t elapsed_ms) {
  if (elapsed_ms == 0U || current_bytes < previous_bytes) {
    return std::nullopt;
  }
  const std::uint64_t delta_bytes = current_bytes - previous_bytes;
  if (delta_bytes > std::numeric_limits<std::uint64_t>::max() / 1000ULL) {
    return std::numeric_limits<std::uint64_t>::max();
  }
  return (delta_bytes * 1000ULL) / elapsed_ms;
}

std::optional<double> CounterPercent(std::uint64_t current,
                                     std::uint64_t previous,
                                     std::uint64_t elapsed_ms) {
  if (elapsed_ms == 0U || current < previous) {
    return std::nullopt;
  }
  const long double elapsed_usec =
      static_cast<long double>(elapsed_ms) * 1000.0L;
  return static_cast<double>(
      (static_cast<long double>(current - previous) * 100.0L) / elapsed_usec);
}

std::uint64_t SaturatingAdd(std::uint64_t left, std::uint64_t right) {
  if (right > std::numeric_limits<std::uint64_t>::max() - left) {
    return std::numeric_limits<std::uint64_t>::max();
  }
  return left + right;
}

void AddNodeDerivedMetrics(const boost::json::object& metric,
                           const NodeReport& node,
                           boost::json::object* target) {
  const std::optional<std::uint64_t> timestamp_ms =
      OptionalUint64Field(metric, "timestamp_ms");
  const std::optional<std::uint64_t> network_rx_bytes =
      OptionalUint64Field(metric, "network_rx_bytes");
  const std::optional<std::uint64_t> network_tx_bytes =
      OptionalUint64Field(metric, "network_tx_bytes");

  (*target)["cpu_percent"] = nullptr;
  (*target)["cpu_throttled_percent"] = nullptr;
  (*target)["io_read_bytes_per_sec"] = nullptr;
  (*target)["io_write_bytes_per_sec"] = nullptr;
  (*target)["network_downlink_bytes_per_sec"] = nullptr;
  (*target)["network_uplink_bytes_per_sec"] = nullptr;

  if (network_tx_bytes) {
    (*target)["network_downlink_bytes"] = *network_tx_bytes;
  }
  if (network_rx_bytes) {
    (*target)["network_uplink_bytes"] = *network_rx_bytes;
  }
  if (!timestamp_ms || !node.previous_timestamp_ms ||
      *timestamp_ms <= *node.previous_timestamp_ms) {
  } else {
    const std::uint64_t elapsed_ms =
        *timestamp_ms - *node.previous_timestamp_ms;
    const auto add_rate = [&](std::string_view output, std::string_view input,
                              const std::optional<std::uint64_t>& previous) {
      const std::optional<std::uint64_t> current =
          OptionalUint64Field(metric, input);
      if (current && previous) {
        const std::optional<std::uint64_t> rate =
            BytesPerSecond(*current, *previous, elapsed_ms);
        if (rate) {
          (*target)[output] = *rate;
        }
      }
    };
    add_rate("network_downlink_bytes_per_sec", "network_tx_bytes",
             node.previous_network_tx_bytes);
    add_rate("network_uplink_bytes_per_sec", "network_rx_bytes",
             node.previous_network_rx_bytes);
    add_rate("io_read_bytes_per_sec", "io_read_bytes",
             node.previous_io_read_bytes);
    add_rate("io_write_bytes_per_sec", "io_write_bytes",
             node.previous_io_write_bytes);

    const auto add_percent = [&](std::string_view output,
                                 std::string_view input,
                                 const std::optional<std::uint64_t>& previous) {
      const std::optional<std::uint64_t> current =
          OptionalUint64Field(metric, input);
      if (current && previous) {
        const std::optional<double> percent =
            CounterPercent(*current, *previous, elapsed_ms);
        if (percent) {
          (*target)[output] = *percent;
        }
      }
    };
    add_percent("cpu_percent", "cpu_usage_usec", node.previous_cpu_usage_usec);
    add_percent("cpu_throttled_percent", "cpu_throttled_usec",
                node.previous_cpu_throttled_usec);
  }

  std::uint64_t drop_count = 0U;
  bool has_drop_count = false;
  constexpr std::string_view kDropFields[] = {
      "network_rx_dropped", "network_tx_dropped", "qdisc_drops",
      "network_filter_drop_packets", "directional_network_qdisc_drops"};
  for (std::string_view field : kDropFields) {
    const std::optional<std::uint64_t> count =
        OptionalUint64Field(metric, field);
    if (count) {
      drop_count = SaturatingAdd(drop_count, *count);
      has_drop_count = true;
    }
  }
  if (has_drop_count) {
    (*target)["network_drop_count"] = drop_count;
  } else {
    (*target)["network_drop_count"] = nullptr;
  }
}

void AppendNodeMetricHistory(const boost::json::object& metric,
                             NodeReport* node) {
  boost::json::object sample;
  constexpr std::string_view kFields[] = {
      "timestamp_ms",
      "cpu_percent",
      "cpu_throttled_percent",
      "memory_current",
      "memory_max_limit_bytes",
      "io_read_bytes_per_sec",
      "io_write_bytes_per_sec",
      "network_downlink_bytes_per_sec",
      "network_uplink_bytes_per_sec",
      "network_drop_count",
      "height",
      "peer_count",
      "mempool_tx_count",
      "rpc_latency_ms",
  };
  for (const std::string_view field : kFields) {
    CopyField(metric, field, &sample);
  }
  if (node->metrics_history.size() >= kMaximumNodeMetricHistorySamples) {
    node->metrics_history.erase(node->metrics_history.begin());
  }
  node->metrics_history.emplace_back(std::move(sample));
}

void RememberNodeMetricSample(const boost::json::object& metric,
                              NodeReport* node) {
  node->previous_timestamp_ms = OptionalUint64Field(metric, "timestamp_ms");
  node->previous_network_rx_bytes =
      OptionalUint64Field(metric, "network_rx_bytes");
  node->previous_network_tx_bytes =
      OptionalUint64Field(metric, "network_tx_bytes");
  node->previous_cpu_usage_usec = OptionalUint64Field(metric, "cpu_usage_usec");
  node->previous_cpu_throttled_usec =
      OptionalUint64Field(metric, "cpu_throttled_usec");
  node->previous_io_read_bytes = OptionalUint64Field(metric, "io_read_bytes");
  node->previous_io_write_bytes = OptionalUint64Field(metric, "io_write_bytes");
}

boost::json::object ParseJsonObjectLine(const std::filesystem::path& path,
                                        std::string_view line,
                                        std::uint64_t line_number) {
  boost::json::value value = boost::json::parse(line);
  if (!value.is_object()) {
    throw std::runtime_error(path.string() + ":" + std::to_string(line_number) +
                             " JSONL entry is not an object");
  }
  return value.as_object();
}

template <typename Callback>
void ForEachJsonLine(const std::filesystem::path& path, Callback callback) {
  if (!std::filesystem::exists(path)) {
    return;
  }
  std::ifstream input(path);
  if (!input) {
    throw std::runtime_error("open failed for " + path.string());
  }

  std::string line;
  std::uint64_t line_number = 0;
  while (std::getline(input, line)) {
    ++line_number;
    if (input.eof()) {
      break;
    }
    if (line.empty()) {
      continue;
    }
    callback(ParseJsonObjectLine(path, line, line_number));
  }
}

void LoadProcessControlFields(const boost::json::object& scenario,
                              boost::json::object* report) {
  boost::json::object canonical;
  bool has_process = false;
  if (const boost::json::value* process = scenario.if_contains("process")) {
    if (!process->is_object()) {
      throw std::runtime_error(
          "resolved scenario process is not a JSON object");
    }
    canonical = process->as_object();
    has_process = true;
  }

  constexpr std::string_view kFields[] = {
      "runtime_node_restarts",
      "runtime_node_freezes",
  };
  for (const std::string_view field : kFields) {
    const boost::json::value* nested = canonical.if_contains(field);
    const boost::json::value* legacy = scenario.if_contains(field);
    if (nested != nullptr && legacy != nullptr && *nested != *legacy) {
      throw std::runtime_error("resolved scenario has conflicting process." +
                               std::string(field) + " and legacy " +
                               std::string(field));
    }
    const boost::json::value* selected = nested != nullptr ? nested : legacy;
    if (selected == nullptr) {
      continue;
    }
    if (!selected->is_array()) {
      throw std::runtime_error("resolved scenario process." +
                               std::string(field) + " is not a JSON array");
    }
    canonical[field] = *selected;
    (*report)[field] = *selected;
  }
  if (has_process || !canonical.empty()) {
    (*report)["process"] = std::move(canonical);
  }
}

void LoadResolvedScenario(const std::filesystem::path& path,
                          boost::json::object* report,
                          std::stop_token stop_token) {
  ThrowIfReportCancelled(stop_token);
  if (!std::filesystem::exists(path)) {
    return;
  }
  boost::json::value value = boost::json::parse(
      ReadText(path, kMaximumResolvedScenarioBytes, stop_token));
  ThrowIfReportCancelled(stop_token);
  if (!value.is_object()) {
    throw std::runtime_error("resolved scenario is not a JSON object: " +
                             path.string());
  }
  const boost::json::object& scenario = value.as_object();
  CopyField(scenario, "run_id", report);
  CopyField(scenario, "simulation", report);
  CopyField(scenario, "chain", report);
  CopyField(scenario, "chains", report);
  CopyField(scenario, "nodes", report);
  CopyField(scenario, "generate_blocks", report);
  CopyField(scenario, "generate_node", report);
  CopyField(scenario, "block_production", report);
  CopyField(scenario, "isolated_network", report);
  CopyField(scenario, "sync_timeout_sec", report);
  CopyField(scenario, "topology", report);
  CopyField(scenario, "topology_initial_edges", report);
  CopyField(scenario, "resource_profiles", report);
  CopyField(scenario, "network_profiles", report);
  CopyField(scenario, "node_configs", report);
  CopyField(scenario, "workloads", report);
  CopyField(scenario, "events", report);
  CopyField(scenario, "resources", report);
  CopyField(scenario, "default_network_condition", report);
  CopyField(scenario, "node_network_conditions", report);
  CopyField(scenario, "runtime_node_network_conditions", report);
  CopyField(scenario, "runtime_node_blocks", report);
  CopyField(scenario, "runtime_node_unblocks", report);
  CopyField(scenario, "runtime_partitions", report);
  CopyField(scenario, "runtime_partition_heals", report);
  CopyField(scenario, "runtime_node_resource_limits", report);
  LoadProcessControlFields(scenario, report);
}

void SeedConfiguredNodes(const boost::json::object& report,
                         std::map<std::string, NodeReport>* nodes) {
  const boost::json::value* configs_value = report.if_contains("node_configs");
  if (configs_value == nullptr || !configs_value->is_array()) {
    return;
  }
  for (const boost::json::value& config_value : configs_value->as_array()) {
    if (!config_value.is_object()) {
      continue;
    }
    const boost::json::object& config = config_value.as_object();
    const std::string node_id = OptionalStringField(config, "id");
    if (node_id.empty()) {
      continue;
    }
    const auto found = nodes->find(node_id);
    if (found == nodes->end()) {
      continue;
    }
    NodeReport& node = found->second;
    node.index = OptionalUint64Field(config, "index");
    node.chain = OptionalStringField(config, "chain");
    node.role = OptionalStringField(config, "role");
  }
}

boost::json::object EventCountsJson(
    const std::map<std::string, std::uint64_t>& event_counts) {
  boost::json::object object;
  for (const auto& [event, count] : event_counts) {
    object[event] = count;
  }
  return object;
}

boost::json::array NodesJson(const std::map<std::string, NodeReport>& nodes) {
  boost::json::array array;
  std::vector<std::pair<std::string_view, const NodeReport*>> ordered;
  ordered.reserve(nodes.size());
  for (const auto& [node_id, node] : nodes) {
    ordered.emplace_back(node_id, &node);
  }
  std::stable_sort(
      ordered.begin(), ordered.end(), [](const auto& left, const auto& right) {
        if (left.second->index && right.second->index &&
            left.second->index != right.second->index) {
          return *left.second->index < *right.second->index;
        }
        if (left.second->index.has_value() != right.second->index.has_value()) {
          return left.second->index.has_value();
        }
        return left.first < right.first;
      });
  for (const auto& [node_id, node_pointer] : ordered) {
    const NodeReport& node = *node_pointer;
    boost::json::object object;
    object["node_id"] = node_id;
    if (node.index) {
      object["node_index"] = *node.index;
    }
    if (!node.chain.empty()) {
      object["chain"] = node.chain;
    }
    if (!node.role.empty()) {
      object["role"] = node.role;
    }
    object["metric_samples"] = node.metric_samples;
    if (node.final_state) {
      object["final_state"] = NodeRuntimeLifecycleName(*node.final_state);
    } else if (!node.unknown_final_state.empty()) {
      object["final_state"] = node.unknown_final_state;
    }
    object["last_metrics"] = node.last_metrics;
    object["metrics_history"] = node.metrics_history;
    if (!node.log_tails.empty()) {
      object["log_tails"] = node.log_tails;
    }
    if (!node.last_error.empty()) {
      object["last_error"] = node.last_error;
    } else {
      object["last_error"] = nullptr;
    }
    array.push_back(std::move(object));
  }
  return array;
}

const boost::json::object* ObjectField(const boost::json::object& object,
                                       std::string_view field) {
  const boost::json::value* value = object.if_contains(field);
  return value != nullptr && value->is_object() ? &value->as_object() : nullptr;
}

const boost::json::array* ArrayField(const boost::json::object& object,
                                     std::string_view field) {
  const boost::json::value* value = object.if_contains(field);
  return value != nullptr && value->is_array() ? &value->as_array() : nullptr;
}

bool PositiveNumericField(const boost::json::object& object,
                          std::string_view field) {
  const boost::json::value* value = object.if_contains(field);
  if (value == nullptr) {
    return false;
  }
  if (value->is_uint64()) {
    return value->as_uint64() != 0U;
  }
  if (value->is_int64()) {
    return value->as_int64() > 0;
  }
  return value->is_double() && value->as_double() > 0.0;
}

bool IsDegradedCondition(const boost::json::object& condition) {
  constexpr std::string_view kFields[] = {
      "bandwidth_mbps",
      "delay_ms",
      "jitter_ms",
      "loss_basis_points",
      "duplicate_basis_points",
      "corrupt_basis_points",
      "reorder_basis_points",
  };
  for (const std::string_view field : kFields) {
    if (PositiveNumericField(condition, field)) {
      return true;
    }
  }
  return false;
}

std::uint64_t NodeSummaryIndex(const boost::json::object& node,
                               std::size_t fallback_index) {
  return OptionalUint64Field(node, "node_index")
      .value_or(static_cast<std::uint64_t>(fallback_index + 1U));
}

std::vector<std::uint64_t> NodeIndexesFromArray(
    const boost::json::array& values,
    const std::set<std::uint64_t>& known_nodes) {
  std::vector<std::uint64_t> result;
  for (const boost::json::value& value : values) {
    std::optional<std::uint64_t> index;
    if (value.is_uint64()) {
      index = value.as_uint64();
    } else if (value.is_int64() && value.as_int64() > 0) {
      index = static_cast<std::uint64_t>(value.as_int64());
    }
    if (index && known_nodes.contains(*index)) {
      result.push_back(*index);
    }
  }
  std::sort(result.begin(), result.end());
  result.erase(std::unique(result.begin(), result.end()), result.end());
  return result;
}

struct TopologyGroupDefinition {
  std::string name;
  std::string kind;
  std::vector<std::uint64_t> node_indexes;
};

std::vector<TopologyGroupDefinition> TopologyGroupDefinitions(
    const boost::json::object& report, const boost::json::array& nodes) {
  std::set<std::uint64_t> known_nodes;
  std::map<std::string, std::vector<std::uint64_t>> role_nodes;
  for (std::size_t position = 0; position < nodes.size(); ++position) {
    if (!nodes[position].is_object()) {
      continue;
    }
    const boost::json::object& node = nodes[position].as_object();
    const std::uint64_t index = NodeSummaryIndex(node, position);
    known_nodes.insert(index);
    const std::string role = OptionalStringField(node, "role");
    if (!role.empty()) {
      role_nodes[role].push_back(index);
    }
  }

  std::vector<TopologyGroupDefinition> groups;
  if (!known_nodes.empty()) {
    std::vector<std::uint64_t> all_nodes(known_nodes.begin(),
                                         known_nodes.end());
    groups.push_back(TopologyGroupDefinition{
        .name = "all",
        .kind = "all",
        .node_indexes = std::move(all_nodes),
    });
  }

  const boost::json::object* topology = ObjectField(report, "topology");
  const auto append_configured_groups = [&](std::string_view field,
                                            std::string_view name_prefix,
                                            std::string_view kind) {
    const boost::json::array* configured =
        topology == nullptr ? nullptr : ArrayField(*topology, field);
    if (configured == nullptr) {
      return;
    }
    for (std::size_t index = 0; index < configured->size(); ++index) {
      if (!(*configured)[index].is_array()) {
        continue;
      }
      std::vector<std::uint64_t> members =
          NodeIndexesFromArray((*configured)[index].as_array(), known_nodes);
      if (members.empty()) {
        continue;
      }
      groups.push_back(TopologyGroupDefinition{
          .name = std::string(name_prefix) + std::to_string(index + 1U),
          .kind = std::string(kind),
          .node_indexes = std::move(members),
      });
    }
  };
  append_configured_groups("groups", "topology-", "partition_group");
  append_configured_groups("regions", "region-", "region");
  for (auto& [role, indexes] : role_nodes) {
    std::sort(indexes.begin(), indexes.end());
    groups.push_back(TopologyGroupDefinition{
        .name = "role-" + role,
        .kind = "role",
        .node_indexes = std::move(indexes),
    });
  }
  return groups;
}

boost::json::array ActiveBlockRules(const boost::json::array& nodes) {
  boost::json::array rules;
  for (std::size_t position = 0; position < nodes.size(); ++position) {
    if (!nodes[position].is_object()) {
      continue;
    }
    const boost::json::object& node = nodes[position].as_object();
    const boost::json::object* metrics = ObjectField(node, "last_metrics");
    const boost::json::array* node_rules =
        metrics == nullptr ? nullptr
                           : ArrayField(*metrics, "network_active_block_rules");
    if (node_rules == nullptr) {
      continue;
    }
    for (const boost::json::value& rule_value : *node_rules) {
      if (!rule_value.is_object()) {
        continue;
      }
      boost::json::object rule = rule_value.as_object();
      rule["node_id"] = OptionalStringField(node, "node_id");
      rule["node_index"] = NodeSummaryIndex(node, position);
      rules.push_back(std::move(rule));
    }
  }
  return rules;
}

boost::json::array DegradedLinks(const boost::json::object& report,
                                 const boost::json::array& nodes) {
  boost::json::array degraded;
  for (std::size_t position = 0; position < nodes.size(); ++position) {
    if (!nodes[position].is_object()) {
      continue;
    }
    const boost::json::object& node = nodes[position].as_object();
    const boost::json::object* metrics = ObjectField(node, "last_metrics");
    const boost::json::object* condition =
        metrics == nullptr ? nullptr
                           : ObjectField(*metrics, "network_condition");
    if (condition == nullptr || !IsDegradedCondition(*condition)) {
      continue;
    }
    boost::json::object link;
    link["scope"] = "node";
    link["node_id"] = OptionalStringField(node, "node_id");
    link["node_index"] = NodeSummaryIndex(node, position);
    link["condition"] = *condition;
    degraded.push_back(std::move(link));
  }

  const boost::json::array* edges =
      ArrayField(report, "topology_current_edges");
  if (edges == nullptr) {
    return degraded;
  }
  for (const boost::json::value& edge_value : *edges) {
    if (!edge_value.is_object()) {
      continue;
    }
    const boost::json::object& edge = edge_value.as_object();
    const boost::json::value* active = edge.if_contains("active");
    if (active != nullptr && active->is_bool() && !active->as_bool()) {
      continue;
    }
    const boost::json::object* condition = ObjectField(edge, "condition");
    if (condition == nullptr || !IsDegradedCondition(*condition)) {
      continue;
    }
    boost::json::object link;
    link["scope"] = "edge";
    CopyField(edge, "from", &link);
    CopyField(edge, "to", &link);
    CopyField(edge, "band", &link);
    link["condition"] = *condition;
    degraded.push_back(std::move(link));
  }
  return degraded;
}

bool GroupContainsAny(const std::set<std::uint64_t>& members,
                      const boost::json::array& indexes) {
  for (const boost::json::value& index_value : indexes) {
    if (index_value.is_uint64() && members.contains(index_value.as_uint64())) {
      return true;
    }
    if (index_value.is_int64() && index_value.as_int64() > 0 &&
        members.contains(static_cast<std::uint64_t>(index_value.as_int64()))) {
      return true;
    }
  }
  return false;
}

bool PartitionReferencesGroup(const boost::json::object& partition,
                              const std::set<std::uint64_t>& members) {
  const boost::json::object* detail = ObjectField(partition, "detail");
  if (detail == nullptr) {
    return false;
  }
  const boost::json::array* group_a = ArrayField(*detail, "group_a");
  const boost::json::array* group_b = ArrayField(*detail, "group_b");
  return (group_a != nullptr && GroupContainsAny(members, *group_a)) ||
         (group_b != nullptr && GroupContainsAny(members, *group_b));
}

bool LinkReferencesGroup(const boost::json::object& link,
                         const std::set<std::uint64_t>& members) {
  const std::optional<std::uint64_t> node =
      OptionalUint64Field(link, "node_index");
  const std::optional<std::uint64_t> from = OptionalUint64Field(link, "from");
  const std::optional<std::uint64_t> to = OptionalUint64Field(link, "to");
  return (node && members.contains(*node)) ||
         (from && members.contains(*from)) || (to && members.contains(*to));
}

std::uint64_t ReferencedObjectCount(const boost::json::array& values,
                                    const std::set<std::uint64_t>& members,
                                    bool partition_shape) {
  std::uint64_t count = 0U;
  for (const boost::json::value& value : values) {
    if (!value.is_object()) {
      continue;
    }
    const bool referenced =
        partition_shape ? PartitionReferencesGroup(value.as_object(), members)
                        : LinkReferencesGroup(value.as_object(), members);
    if (referenced && count != std::numeric_limits<std::uint64_t>::max()) {
      ++count;
    }
  }
  return count;
}

std::optional<std::uint64_t> AggregateNodeMetric(
    const boost::json::array& nodes, const std::set<std::uint64_t>& members,
    std::string_view field) {
  std::uint64_t total = 0U;
  std::size_t matched = 0U;
  for (std::size_t position = 0; position < nodes.size(); ++position) {
    if (!nodes[position].is_object()) {
      continue;
    }
    const boost::json::object& node = nodes[position].as_object();
    if (!members.contains(NodeSummaryIndex(node, position))) {
      continue;
    }
    ++matched;
    const boost::json::object* metrics = ObjectField(node, "last_metrics");
    const std::optional<std::uint64_t> value =
        metrics == nullptr ? std::nullopt
                           : OptionalUint64Field(*metrics, field);
    if (!value) {
      return std::nullopt;
    }
    total = SaturatingAdd(total, *value);
  }
  return matched == members.size() && matched != 0U
             ? std::optional<std::uint64_t>(total)
             : std::nullopt;
}

void AddOptionalUint(std::string_view field,
                     const std::optional<std::uint64_t>& value,
                     boost::json::object* object) {
  if (value) {
    (*object)[field] = *value;
  } else {
    (*object)[field] = nullptr;
  }
}

struct AggregatedPerfCounter {
  std::string name;
  std::uint64_t raw_value = 0U;
  std::optional<std::uint64_t> scaled_value = 0U;
  std::uint64_t time_enabled_ns = 0U;
  std::uint64_t time_running_ns = 0U;
  bool multiplexed = false;
  bool scaled = false;
  bool scaled_overflow = false;
  bool aggregation_overflow = false;
};

void AddPerfCounterValue(std::uint64_t next, std::uint64_t* total,
                         bool* overflow) {
  if (next > std::numeric_limits<std::uint64_t>::max() - *total) {
    *total = std::numeric_limits<std::uint64_t>::max();
    *overflow = true;
    return;
  }
  *total += next;
}

bool AggregateGroupPerfCounters(const boost::json::array& nodes,
                                const std::set<std::uint64_t>& members,
                                std::string_view group_id,
                                boost::json::object* summary) {
  std::optional<boost::json::array> expected_names;
  std::vector<AggregatedPerfCounter> totals;
  std::size_t matched = 0U;
  for (std::size_t position = 0; position < nodes.size(); ++position) {
    if (!nodes[position].is_object()) {
      continue;
    }
    const boost::json::object& node = nodes[position].as_object();
    if (!members.contains(NodeSummaryIndex(node, position))) {
      continue;
    }
    ++matched;
    const boost::json::object* metrics = ObjectField(node, "last_metrics");
    if (metrics == nullptr ||
        OptionalStringField(*metrics, "perf_counter_target_kind") != "group" ||
        OptionalStringField(*metrics, "perf_counter_target_id") != group_id) {
      return false;
    }
    const boost::json::value* available =
        metrics->if_contains("perf_counters_available");
    const boost::json::array* names =
        ArrayField(*metrics, "perf_counter_names");
    const boost::json::array* counters = ArrayField(*metrics, "perf_counters");
    if (available == nullptr || !available->is_bool() ||
        !available->as_bool() || names == nullptr || names->empty() ||
        counters == nullptr || counters->size() != names->size()) {
      return false;
    }
    if (!expected_names) {
      expected_names = *names;
      totals.resize(names->size());
      for (std::size_t index = 0; index < names->size(); ++index) {
        if (!(*names)[index].is_string() ||
            (*names)[index].as_string().empty()) {
          return false;
        }
        totals[index].name = std::string((*names)[index].as_string());
      }
    } else if (*expected_names != *names) {
      return false;
    }

    for (std::size_t index = 0; index < counters->size(); ++index) {
      if (!(*counters)[index].is_object()) {
        return false;
      }
      const boost::json::object& counter = (*counters)[index].as_object();
      AggregatedPerfCounter& total = totals[index];
      if (OptionalStringField(counter, "name") != total.name) {
        return false;
      }
      const std::optional<std::uint64_t> raw =
          OptionalUint64Field(counter, "raw_value");
      const std::optional<std::uint64_t> enabled =
          OptionalUint64Field(counter, "time_enabled_ns");
      const std::optional<std::uint64_t> running =
          OptionalUint64Field(counter, "time_running_ns");
      if (!raw || !enabled || !running) {
        return false;
      }
      AddPerfCounterValue(*raw, &total.raw_value, &total.aggregation_overflow);
      AddPerfCounterValue(*enabled, &total.time_enabled_ns,
                          &total.aggregation_overflow);
      AddPerfCounterValue(*running, &total.time_running_ns,
                          &total.aggregation_overflow);

      const boost::json::value* scaled_value =
          counter.if_contains("scaled_value");
      if (scaled_value == nullptr || scaled_value->is_null()) {
        total.scaled_value.reset();
      } else {
        const std::optional<std::uint64_t> scaled =
            OptionalUint64Field(counter, "scaled_value");
        if (!scaled) {
          return false;
        }
        if (total.scaled_value) {
          AddPerfCounterValue(*scaled, &*total.scaled_value,
                              &total.aggregation_overflow);
        }
      }
      for (const auto& [field, destination] :
           std::initializer_list<std::pair<std::string_view, bool*>>{
               {"multiplexed", &total.multiplexed},
               {"scaled", &total.scaled},
               {"scaled_overflow", &total.scaled_overflow}}) {
        const boost::json::value* flag = counter.if_contains(field);
        if (flag == nullptr || !flag->is_bool()) {
          return false;
        }
        *destination = *destination || flag->as_bool();
      }
    }
  }
  if (!expected_names || matched == 0U || matched != members.size()) {
    return false;
  }

  boost::json::array counters;
  counters.reserve(totals.size());
  bool aggregate_overflow = false;
  for (const AggregatedPerfCounter& total : totals) {
    boost::json::object counter;
    counter["name"] = total.name;
    counter["raw_value"] = total.raw_value;
    if (total.scaled_value) {
      counter["scaled_value"] = *total.scaled_value;
    } else {
      counter["scaled_value"] = nullptr;
    }
    counter["time_enabled_ns"] = total.time_enabled_ns;
    counter["time_running_ns"] = total.time_running_ns;
    counter["multiplexed"] = total.multiplexed;
    counter["scaled"] = total.scaled;
    counter["scaled_overflow"] =
        total.scaled_overflow || total.aggregation_overflow;
    counter["aggregation_overflow"] = total.aggregation_overflow;
    aggregate_overflow = aggregate_overflow || total.aggregation_overflow;
    counters.push_back(std::move(counter));
  }
  (*summary)["perf_counters_available"] = true;
  (*summary)["perf_counter_names"] = std::move(*expected_names);
  (*summary)["perf_counters"] = std::move(counters);
  (*summary)["perf_counter_aggregation_overflow"] = aggregate_overflow;
  return true;
}

void AddTopologyViewSummaries(boost::json::object* report) {
  const boost::json::array* nodes = ArrayField(*report, "nodes_summary");
  if (nodes == nullptr) {
    (*report)["topology_blocked_rules"] = boost::json::array{};
    (*report)["topology_degraded_links"] = boost::json::array{};
    (*report)["topology_groups_summary"] = boost::json::array{};
    return;
  }
  boost::json::array blocked_rules = ActiveBlockRules(*nodes);
  boost::json::array degraded_links = DegradedLinks(*report, *nodes);
  const boost::json::array* active_partitions =
      ArrayField(*report, "active_network_partitions");
  const boost::json::array no_partitions;
  if (active_partitions == nullptr) {
    active_partitions = &no_partitions;
  }

  boost::json::array group_summaries;
  for (const TopologyGroupDefinition& definition :
       TopologyGroupDefinitions(*report, *nodes)) {
    const std::set<std::uint64_t> members(definition.node_indexes.begin(),
                                          definition.node_indexes.end());
    boost::json::object summary;
    summary["group"] = definition.name;
    summary["kind"] = definition.kind;
    boost::json::array node_indexes;
    boost::json::array node_ids;
    for (const std::uint64_t index : definition.node_indexes) {
      node_indexes.push_back(index);
      for (std::size_t position = 0; position < nodes->size(); ++position) {
        if ((*nodes)[position].is_object() &&
            NodeSummaryIndex((*nodes)[position].as_object(), position) ==
                index) {
          node_ids.emplace_back(
              OptionalStringField((*nodes)[position].as_object(), "node_id"));
          break;
        }
      }
    }
    summary["node_indexes"] = std::move(node_indexes);
    summary["node_ids"] = std::move(node_ids);
    summary["node_count"] = definition.node_indexes.size();
    summary["active_partition_count"] =
        ReferencedObjectCount(*active_partitions, members, true);
    summary["degraded_link_count"] =
        ReferencedObjectCount(degraded_links, members, false);
    summary["blocked_rule_count"] =
        ReferencedObjectCount(blocked_rules, members, false);
    AddOptionalUint(
        "network_downlink_bytes",
        AggregateNodeMetric(*nodes, members, "network_downlink_bytes"),
        &summary);
    AddOptionalUint(
        "network_uplink_bytes",
        AggregateNodeMetric(*nodes, members, "network_uplink_bytes"), &summary);
    AddOptionalUint(
        "network_downlink_bytes_per_sec",
        AggregateNodeMetric(*nodes, members, "network_downlink_bytes_per_sec"),
        &summary);
    AddOptionalUint(
        "network_uplink_bytes_per_sec",
        AggregateNodeMetric(*nodes, members, "network_uplink_bytes_per_sec"),
        &summary);
    AddOptionalUint("network_drop_count",
                    AggregateNodeMetric(*nodes, members, "network_drop_count"),
                    &summary);
    summary["perf_counters_available"] = false;
    summary["perf_counter_names"] = boost::json::array{};
    summary["perf_counters"] = boost::json::array{};
    summary["perf_counter_aggregation_overflow"] = false;
    static_cast<void>(
        AggregateGroupPerfCounters(*nodes, members, definition.name, &summary));
    group_summaries.push_back(std::move(summary));
  }
  (*report)["topology_blocked_rules"] = std::move(blocked_rules);
  (*report)["topology_degraded_links"] = std::move(degraded_links);
  (*report)["topology_groups_summary"] = std::move(group_summaries);
}

void RememberNodeState(std::string_view state, NodeReport* node) {
  const std::optional<NodeRuntimeLifecycle> lifecycle =
      ParseNodeRuntimeLifecycleName(state);
  if (lifecycle) {
    node->final_state = *lifecycle;
    node->unknown_final_state.clear();
    return;
  }
  node->final_state = std::nullopt;
  node->unknown_final_state = std::string(state);
}

void CopyOptionalStringField(const boost::json::object& source,
                             std::string_view field, std::string* target) {
  const std::string value = OptionalStringField(source, field);
  if (!value.empty()) {
    *target = value;
  }
}

void CopyOptionalWalletStrategyField(const boost::json::object& source,
                                     std::string_view field,
                                     WalletReport* wallet) {
  const std::string value = OptionalStringField(source, field);
  if (value.empty()) {
    return;
  }
  const std::optional<WalletInitializationStrategy> strategy =
      WalletInitializationStrategyFromName(value);
  if (strategy) {
    wallet->strategy = *strategy;
    wallet->unknown_strategy.clear();
    return;
  }
  wallet->strategy = std::nullopt;
  wallet->unknown_strategy = value;
}

void CopyOptionalWalletModeField(const boost::json::object& source,
                                 std::string_view field, WalletReport* wallet) {
  const std::string value = OptionalStringField(source, field);
  if (value.empty()) {
    return;
  }
  const std::optional<WalletPrivacyMode> mode =
      WalletPrivacyModeFromName(value);
  if (mode) {
    wallet->mode = *mode;
    wallet->unknown_mode.clear();
    return;
  }
  wallet->mode = std::nullopt;
  wallet->unknown_mode = value;
}

void CopyOptionalUint64Field(const boost::json::object& source,
                             std::string_view field, std::uint64_t* target) {
  const std::optional<std::uint64_t> value = OptionalUint64Field(source, field);
  if (value) {
    *target = *value;
  }
}

void RememberWalletAddressEvent(
    const boost::json::value& detail,
    std::map<std::uint64_t, WalletReport>* wallets) {
  if (!detail.is_object()) {
    return;
  }
  const boost::json::object& object = detail.as_object();
  const std::optional<std::uint64_t> wallet_index =
      OptionalUint64Field(object, "wallet_index");
  if (!wallet_index) {
    return;
  }
  WalletReport& wallet = (*wallets)[*wallet_index];
  wallet.wallet_index = *wallet_index;
  CopyOptionalUint64Field(object, "node", &wallet.node);
  CopyOptionalStringField(object, "address", &wallet.address);
  CopyOptionalStringField(object, "funding_address", &wallet.funding_address);
  CopyOptionalWalletStrategyField(object, "strategy", &wallet);
  CopyOptionalWalletModeField(object, "mode", &wallet);
}

void RememberWalletTransactionEvent(
    const boost::json::value& detail,
    std::map<std::uint64_t, WalletReport>* wallets) {
  if (!detail.is_object()) {
    return;
  }
  const boost::json::object& object = detail.as_object();
  const std::optional<std::uint64_t> sender_index =
      OptionalUint64Field(object, "sender_wallet_index");
  const std::optional<std::uint64_t> receiver_index =
      OptionalUint64Field(object, "receiver_wallet_index");
  const std::uint64_t amount_satoshis =
      OptionalUint64Field(object, "amount_satoshis").value_or(0U);
  if (sender_index) {
    WalletReport& sender = (*wallets)[*sender_index];
    sender.wallet_index = *sender_index;
    ++sender.transactions_sent;
    if (sender.simulated_amount_sent_satoshis >
        std::numeric_limits<std::uint64_t>::max() - amount_satoshis) {
      throw std::runtime_error("wallet sent amount total overflow");
    }
    sender.simulated_amount_sent_satoshis += amount_satoshis;
    CopyOptionalUint64Field(object, "sender_node", &sender.node);
    CopyOptionalStringField(object, "sender_address", &sender.address);
    sender.last_sent_transaction = object;
  }
  if (receiver_index) {
    WalletReport& receiver = (*wallets)[*receiver_index];
    receiver.wallet_index = *receiver_index;
    ++receiver.transactions_received;
    if (receiver.simulated_amount_received_satoshis >
        std::numeric_limits<std::uint64_t>::max() - amount_satoshis) {
      throw std::runtime_error("wallet received amount total overflow");
    }
    receiver.simulated_amount_received_satoshis += amount_satoshis;
    CopyOptionalUint64Field(object, "receiver_node", &receiver.node);
    CopyOptionalStringField(object, "receiver_address", &receiver.address);
    receiver.last_received_transaction = object;
  }
}

void RememberWalletFundingEvent(
    const boost::json::value& detail,
    std::map<std::uint64_t, WalletReport>* wallets) {
  if (!detail.is_object()) {
    return;
  }
  const boost::json::object& object = detail.as_object();
  const std::optional<std::uint64_t> wallet_index =
      OptionalUint64Field(object, "wallet_index");
  if (!wallet_index) {
    return;
  }
  WalletReport& wallet = (*wallets)[*wallet_index];
  wallet.wallet_index = *wallet_index;
  CopyOptionalUint64Field(object, "node", &wallet.node);
  CopyOptionalStringField(object, "address", &wallet.address);
  CopyOptionalStringField(object, "funding_address", &wallet.funding_address);
  wallet.last_funding = object;
}

boost::json::array WalletsJson(
    const std::map<std::uint64_t, WalletReport>& wallets) {
  boost::json::array array;
  for (const auto& [wallet_index, wallet] : wallets) {
    boost::json::object object;
    object["wallet_index"] = wallet_index;
    if (wallet.node != 0U) {
      object["node"] = wallet.node;
    }
    if (!wallet.address.empty()) {
      object["address"] = wallet.address;
    }
    if (!wallet.funding_address.empty()) {
      object["funding_address"] = wallet.funding_address;
    }
    if (wallet.strategy) {
      object["strategy"] = WalletInitializationStrategyName(*wallet.strategy);
    } else if (!wallet.unknown_strategy.empty()) {
      object["strategy"] = wallet.unknown_strategy;
    }
    if (wallet.mode) {
      object["mode"] = WalletPrivacyModeName(*wallet.mode);
    } else if (!wallet.unknown_mode.empty()) {
      object["mode"] = wallet.unknown_mode;
    }
    object["transactions_sent"] = wallet.transactions_sent;
    object["transactions_received"] = wallet.transactions_received;
    object["simulated_amount_sent_satoshis"] =
        wallet.simulated_amount_sent_satoshis;
    object["simulated_amount_received_satoshis"] =
        wallet.simulated_amount_received_satoshis;
    if (!wallet.last_sent_transaction.empty()) {
      object["last_sent_transaction"] = wallet.last_sent_transaction;
    }
    if (!wallet.last_received_transaction.empty()) {
      object["last_received_transaction"] = wallet.last_received_transaction;
    }
    if (!wallet.last_funding.empty()) {
      object["last_funding"] = wallet.last_funding;
    }
    if (!wallet.last_metrics.empty()) {
      object["last_metrics"] = wallet.last_metrics;
    }
    array.push_back(std::move(object));
  }
  return array;
}

std::optional<std::string_view> LogTailKind(SimulationEventKind event_kind) {
  switch (event_kind) {
    case SimulationEventKind::kStdoutTail:
      return "stdout";
    case SimulationEventKind::kStderrTail:
      return "stderr";
    case SimulationEventKind::kDaemonLogTail:
      return "daemon_log";
    default:
      return std::nullopt;
  }
}

void RememberLogTail(boost::json::value detail, std::string_view kind,
                     boost::json::object* log_tails) {
  if (!detail.is_object()) {
    (*log_tails)[kind] = std::move(detail);
    return;
  }

  boost::json::object next = std::move(detail).as_object();
  const boost::json::value* next_text_value = next.if_contains("text");
  if (next_text_value == nullptr || !next_text_value->is_string()) {
    (*log_tails)[kind] = std::move(next);
    return;
  }

  std::string text;
  std::optional<std::uint64_t> start_offset =
      OptionalUint64Field(next, "start_offset");
  const boost::json::value* previous_value = log_tails->if_contains(kind);
  const boost::json::value* reset_value = next.if_contains("offset_reset");
  const bool offset_reset = reset_value != nullptr && reset_value->is_bool() &&
                            reset_value->as_bool();
  if (!offset_reset && previous_value != nullptr &&
      previous_value->is_object()) {
    const boost::json::object& previous = previous_value->as_object();
    const std::optional<std::uint64_t> previous_next_offset =
        OptionalUint64Field(previous, "next_offset");
    if (start_offset && previous_next_offset &&
        *start_offset == *previous_next_offset) {
      const boost::json::value* previous_text = previous.if_contains("text");
      if (previous_text != nullptr && previous_text->is_string()) {
        text = std::string(previous_text->as_string());
        start_offset = OptionalUint64Field(previous, "start_offset");
      }
    }
  }
  text += std::string(next_text_value->as_string());
  std::size_t removed_bytes = 0;
  if (text.size() > kMaximumNodeLogTailBytes) {
    const std::size_t first_newline =
        text.find('\n', text.size() - kMaximumNodeLogTailBytes);
    removed_bytes = first_newline == std::string::npos
                        ? text.size() - kMaximumNodeLogTailBytes
                        : first_newline + 1U;
    text.erase(0, removed_bytes);
  }
  if (start_offset &&
      *start_offset <= std::numeric_limits<std::uint64_t>::max() -
                           static_cast<std::uint64_t>(removed_bytes)) {
    next["start_offset"] =
        *start_offset + static_cast<std::uint64_t>(removed_bytes);
  }
  next["text"] = std::move(text);
  (*log_tails)[kind] = std::move(next);
}

void AppendEventSummary(const boost::json::object& event,
                        boost::json::array* summaries) {
  boost::json::object summary;
  const std::string node_id = OptionalStringField(event, "node_id");
  if (!node_id.empty()) {
    summary["node_id"] = node_id;
  }
  const std::string timestamp = OptionalStringField(event, "timestamp");
  if (!timestamp.empty()) {
    summary["timestamp"] = timestamp;
  }

  boost::json::value detail = ParseEventDetail(event);
  if (!detail.is_null()) {
    summary["detail"] = std::move(detail);
  }
  summaries->push_back(std::move(summary));
  if (summaries->size() > kMaximumRunReportSummaryRecords) {
    summaries->erase(summaries->begin());
  }
}

void AppendBoundedEventSummary(const boost::json::object& event,
                               std::size_t maximum_size,
                               boost::json::array* summaries) {
  AppendEventSummary(event, summaries);
  if (summaries->size() > maximum_size) {
    summaries->erase(summaries->begin());
  }
}

std::optional<std::vector<std::uint64_t>> PartitionGroup(
    const boost::json::object& detail, std::string_view field) {
  const boost::json::value* value = detail.if_contains(field);
  if (value == nullptr || !value->is_array() || value->as_array().empty()) {
    return std::nullopt;
  }
  std::vector<std::uint64_t> nodes;
  nodes.reserve(value->as_array().size());
  for (const boost::json::value& node_value : value->as_array()) {
    std::optional<std::uint64_t> node;
    if (node_value.is_uint64()) {
      node = node_value.as_uint64();
    } else if (node_value.is_int64() && node_value.as_int64() > 0) {
      node = static_cast<std::uint64_t>(node_value.as_int64());
    }
    if (!node || *node == 0U) {
      return std::nullopt;
    }
    nodes.push_back(*node);
  }
  std::sort(nodes.begin(), nodes.end());
  nodes.erase(std::unique(nodes.begin(), nodes.end()), nodes.end());
  return nodes;
}

std::string PartitionGroupKey(const std::vector<std::uint64_t>& nodes) {
  std::string key;
  for (const std::uint64_t node : nodes) {
    if (!key.empty()) {
      key.push_back(',');
    }
    key += std::to_string(node);
  }
  return key;
}

std::optional<std::string> PartitionKey(const boost::json::object& detail) {
  const std::optional<std::vector<std::uint64_t>> group_a =
      PartitionGroup(detail, "group_a");
  const std::optional<std::vector<std::uint64_t>> group_b =
      PartitionGroup(detail, "group_b");
  if (!group_a || !group_b) {
    return std::nullopt;
  }
  std::string left = PartitionGroupKey(*group_a);
  std::string right = PartitionGroupKey(*group_b);
  if (right < left) {
    std::swap(left, right);
  }
  return left + '|' + right;
}

void RememberActivePartition(
    const boost::json::object& event, bool active,
    std::map<std::string, boost::json::object>* active_partitions) {
  const boost::json::value detail = ParseEventDetail(event);
  if (!detail.is_object()) {
    return;
  }
  const std::optional<std::string> key = PartitionKey(detail.as_object());
  if (!key) {
    return;
  }
  if (!active) {
    active_partitions->erase(*key);
    return;
  }
  boost::json::array summary;
  AppendEventSummary(event, &summary);
  active_partitions->insert_or_assign(*key,
                                      std::move(summary.front()).as_object());
}

boost::json::array ActivePartitionsJson(
    const std::map<std::string, boost::json::object>& active_partitions) {
  boost::json::array result;
  result.reserve(active_partitions.size());
  for (const auto& [key, partition] : active_partitions) {
    static_cast<void>(key);
    result.push_back(partition);
  }
  return result;
}

void AppendScheduledBlockSummary(const boost::json::object& event,
                                 boost::json::array* summaries) {
  AppendEventSummary(event, summaries);
  if (summaries->size() > kMaximumScheduledBlockSummaries) {
    summaries->erase(summaries->begin());
  }
}

void AppendScheduledEventSummary(const boost::json::object& event,
                                 boost::json::array* summaries) {
  AppendEventSummary(event, summaries);
  if (summaries->size() > kMaximumScheduledEventSummaries) {
    summaries->erase(summaries->begin());
  }
}

void AppendCheckpointSummary(const boost::json::object& event,
                             boost::json::array* summaries) {
  AppendEventSummary(event, summaries);
  if (summaries->size() > kMaximumCheckpointSummaries) {
    summaries->erase(summaries->begin());
  }
}

void AppendDirectionalPolicyVerification(const boost::json::object& event,
                                         boost::json::array* summaries) {
  AppendEventSummary(event, summaries);
  if (summaries->size() > kMaximumDirectionalPolicyVerifications) {
    summaries->erase(summaries->begin());
  }
}

void RememberTopologyEdgeUpdate(const boost::json::object& event,
                                boost::json::array* summaries,
                                boost::json::array* current_edges) {
  AppendEventSummary(event, summaries);
  if (summaries->size() > kMaximumTopologyEdgeSummaries) {
    summaries->erase(summaries->begin());
  }
  const boost::json::value detail = ParseEventDetail(event);
  if (!detail.is_object()) {
    return;
  }
  const boost::json::value* current = detail.as_object().if_contains("current");
  if (current == nullptr || !current->is_object()) {
    return;
  }
  const std::optional<std::uint64_t> from =
      OptionalUint64Field(current->as_object(), "from");
  const std::optional<std::uint64_t> to =
      OptionalUint64Field(current->as_object(), "to");
  if (!from || !to) {
    return;
  }
  for (boost::json::value& edge : *current_edges) {
    if (!edge.is_object()) {
      continue;
    }
    if (OptionalUint64Field(edge.as_object(), "from") == from &&
        OptionalUint64Field(edge.as_object(), "to") == to) {
      edge = *current;
      return;
    }
  }
  current_edges->push_back(*current);
}

void AppendBoundedTopologyEdgeSummary(const boost::json::object& event,
                                      boost::json::array* summaries) {
  AppendEventSummary(event, summaries);
  if (summaries->size() > kMaximumTopologyEdgeSummaries) {
    summaries->erase(summaries->begin());
  }
}

void AppendBoundedProfileUpdateSummary(const boost::json::object& event,
                                       boost::json::array* summaries) {
  AppendEventSummary(event, summaries);
  if (summaries->size() > kMaximumProfileUpdateSummaries) {
    summaries->erase(summaries->begin());
  }
}

void ApplyBlockProductionPolicyEvent(const boost::json::object& event,
                                     boost::json::object* report) {
  const boost::json::value detail = ParseEventDetail(event);
  if (!detail.is_object()) {
    return;
  }
  const boost::json::object& command = detail.as_object();
  const std::optional<SimulationCommandKind> kind =
      SimulationCommandKindFromName(OptionalStringField(command, "kind"));
  if (!kind || *kind != SimulationCommandKind::kSetBlockProductionPolicy) {
    return;
  }
  boost::json::value* block_production =
      report->if_contains("block_production");
  if (block_production == nullptr || !block_production->is_object()) {
    (*report)["block_production"] = boost::json::object{};
    block_production = report->if_contains("block_production");
  }
  boost::json::object& policy = block_production->as_object();
  CopyField(command, "period_ms", &policy);
  CopyField(command, "probability", &policy);
  CopyField(command, "seed", &policy);
}

void AppendOperatorCommandSummary(const boost::json::object& event,
                                  OperatorCommandStatus status,
                                  boost::json::array* summaries) {
  boost::json::object summary;
  summary["status"] = OperatorCommandStatusName(status);
  const std::string node_id = OptionalStringField(event, "node_id");
  if (!node_id.empty()) {
    summary["node_id"] = node_id;
  }
  const std::string timestamp = OptionalStringField(event, "timestamp");
  if (!timestamp.empty()) {
    summary["timestamp"] = timestamp;
  }
  boost::json::value detail = ParseEventDetail(event);
  if (!detail.is_null()) {
    summary["detail"] = std::move(detail);
  }
  summaries->push_back(std::move(summary));
  if (summaries->size() > kMaximumOperatorCommandSummaries) {
    summaries->erase(summaries->begin());
  }
}

}  // namespace

struct IncrementalRunReport::Impl {
  struct FileObservation {
    std::uintmax_t device = 0;
    std::uintmax_t inode = 0;
    std::uintmax_t size = 0;
    std::int64_t modified_seconds = 0;
    std::int64_t modified_nanoseconds = 0;
  };

  struct InputCursor {
    std::uint64_t offset = 0;
    std::uint64_t line_number = 0;
    FileObservation observation;
    bool observed_exists = false;
  };

  struct TransactionLoadLiveReport {
    boost::json::object detail;
    std::uint64_t revision = 0U;
    std::uint64_t last_update_event_count = 0U;
    bool authoritative = false;
    bool completed = false;
  };

  explicit Impl(std::filesystem::path root, std::stop_token stop_token)
      : run_root(std::move(root)) {
    ThrowIfReportCancelled(stop_token);
    if (!std::filesystem::is_directory(run_root)) {
      throw std::runtime_error("run root is not a directory: " +
                               run_root.string());
    }
    Reset(stop_token);
  }

  boost::json::array& Array(std::string_view field) {
    boost::json::value* value = report.if_contains(field);
    if (value == nullptr || !value->is_array()) {
      report[field] = boost::json::array{};
      value = report.if_contains(field);
    }
    return value->as_array();
  }

  TransactionLoadLiveReport& LiveTransactionLoad(std::uint64_t workload_index) {
    const auto existing = transaction_load_live.find(workload_index);
    if (existing != transaction_load_live.end()) {
      return existing->second;
    }
    if (transaction_load_live.size() >= kMaximumRunReportSummaryRecords) {
      auto oldest = transaction_load_live.begin();
      for (auto current = std::next(oldest);
           current != transaction_load_live.end(); ++current) {
        if (current->second.last_update_event_count <
            oldest->second.last_update_event_count) {
          oldest = current;
        }
      }
      transaction_load_live.erase(oldest);
    }
    TransactionLoadLiveReport live;
    live.detail["workload_index"] = workload_index;
    for (const std::string_view field :
         {"attempted", "submitted", "rejected", "timed_out", "backpressured",
          "dropped", "propagated", "confirmed", "failed", "cancelled"}) {
      live.detail[field] = 0U;
    }
    return transaction_load_live.emplace(workload_index, std::move(live))
        .first->second;
  }

  static void IncrementLiveTransactionLoadCounter(boost::json::object* detail,
                                                  std::string_view field) {
    const std::uint64_t current =
        OptionalUint64Field(*detail, field).value_or(0U);
    if (current == std::numeric_limits<std::uint64_t>::max()) {
      throw std::runtime_error("transaction load live " + std::string(field) +
                               " counter overflow");
    }
    (*detail)[field] = current + 1U;
  }

  void RememberFallbackTransactionLoadAttempt(
      const boost::json::object& event) {
    boost::json::value parsed = ParseEventDetail(event);
    if (!parsed.is_object()) {
      return;
    }
    const boost::json::object& detail = parsed.as_object();
    const std::optional<std::uint64_t> workload_index =
        OptionalUint64Field(detail, "workload_index");
    if (!workload_index || *workload_index == 0U) {
      return;
    }
    TransactionLoadLiveReport& live = LiveTransactionLoad(*workload_index);
    if (live.authoritative) {
      return;
    }
    const std::optional<std::uint64_t> workload_count =
        OptionalUint64Field(detail, "workload_count");
    if (workload_count) {
      live.detail["workload_count"] = *workload_count;
    }
    IncrementLiveTransactionLoadCounter(&live.detail, "attempted");
    const std::string outcome = OptionalStringField(detail, "outcome");
    if (outcome == "submitted") {
      IncrementLiveTransactionLoadCounter(&live.detail, "submitted");
    } else if (outcome == "rejected") {
      IncrementLiveTransactionLoadCounter(&live.detail, "rejected");
    } else if (outcome == "timed_out") {
      IncrementLiveTransactionLoadCounter(&live.detail, "timed_out");
    } else if (outcome == "backpressured") {
      IncrementLiveTransactionLoadCounter(&live.detail, "backpressured");
    } else if (outcome == "dropped") {
      IncrementLiveTransactionLoadCounter(&live.detail, "dropped");
    } else if (outcome == "failed") {
      IncrementLiveTransactionLoadCounter(&live.detail, "failed");
    } else if (outcome == "cancelled") {
      IncrementLiveTransactionLoadCounter(&live.detail, "cancelled");
    }
    live.last_update_event_count = event_count;
  }

  void RememberFallbackTransactionLoadObservation(
      const boost::json::object& event, std::string_view counter) {
    if (OptionalUint64Field(report, "nodes") != 1U) {
      return;
    }
    boost::json::value parsed = ParseEventDetail(event);
    if (!parsed.is_object()) {
      return;
    }
    const boost::json::object& detail = parsed.as_object();
    const std::optional<std::uint64_t> workload_index =
        OptionalUint64Field(detail, "workload_index");
    if (!workload_index || *workload_index == 0U) {
      return;
    }
    const auto existing = transaction_load_live.find(*workload_index);
    if (existing == transaction_load_live.end()) {
      return;
    }
    TransactionLoadLiveReport& live = existing->second;
    if (live.authoritative) {
      return;
    }
    IncrementLiveTransactionLoadCounter(&live.detail, counter);
    live.last_update_event_count = event_count;
  }

  void RememberTransactionLoadSnapshot(const boost::json::object& event,
                                       bool completed) {
    boost::json::value parsed = ParseEventDetail(event);
    if (!parsed.is_object()) {
      return;
    }
    boost::json::object detail = std::move(parsed).as_object();
    const std::optional<std::uint64_t> workload_index =
        OptionalUint64Field(detail, "workload_index");
    if (!workload_index || *workload_index == 0U) {
      return;
    }
    TransactionLoadLiveReport& live = LiveTransactionLoad(*workload_index);
    const std::optional<std::uint64_t> revision =
        OptionalUint64Field(detail, "revision");
    if (revision && live.authoritative && *revision < live.revision) {
      if (completed) {
        live.completed = true;
      }
      return;
    }
    if (!revision && live.authoritative && !completed) {
      return;
    }
    live.detail = std::move(detail);
    live.revision = revision.value_or(live.revision);
    live.authoritative = true;
    live.completed = live.completed || completed;
    live.last_update_event_count = event_count;
  }

  boost::json::array TransactionLoadLiveJson() const {
    std::vector<const TransactionLoadLiveReport*> ordered;
    ordered.reserve(transaction_load_live.size());
    for (const auto& [workload_index, live] : transaction_load_live) {
      static_cast<void>(workload_index);
      ordered.push_back(&live);
    }
    std::sort(ordered.begin(), ordered.end(),
              [](const auto* left, const auto* right) {
                return left->last_update_event_count <
                       right->last_update_event_count;
              });
    boost::json::array result;
    result.reserve(ordered.size());
    for (const TransactionLoadLiveReport* live : ordered) {
      boost::json::object value = live->detail;
      value["revision"] = live->revision;
      value["authoritative"] = live->authoritative;
      value["completed"] = live->completed;
      value["last_update_event_count"] = live->last_update_event_count;
      result.push_back(std::move(value));
    }
    return result;
  }

  void Reset(std::stop_token stop_token) {
    ThrowIfReportCancelled(stop_token);
    report = {};
    report["run_root"] = std::filesystem::absolute(run_root).string();
    LoadResolvedScenario(run_root / "resolved-scenario.json", &report,
                         stop_token);
    expected_run_id = OptionalStringField(report, "run_id");

    boost::json::array topology_current_edges;
    const boost::json::value* topology_initial_edges =
        report.if_contains("topology_initial_edges");
    if (topology_initial_edges != nullptr &&
        topology_initial_edges->is_array()) {
      topology_current_edges = topology_initial_edges->as_array();
    }
    report["topology_current_edges"] = std::move(topology_current_edges);

    constexpr std::string_view kArrayFields[] = {
        "generated_blocks",
        "scheduled_blocks",
        "scheduled_events_started",
        "scheduled_events_completed",
        "scheduled_events_failed",
        "checkpoints",
        "height_reached",
        "height_waits",
        "peer_waits",
        "peer_connects",
        "peer_disconnects",
        "raw_transactions",
        "transaction_visibility",
        "transaction_confirmations",
        "node_restarts",
        "node_freezes",
        "resource_updates",
        "resource_profile_updates",
        "network_profile_updates",
        "profile_update_rollback_failures",
        "network_condition_updates",
        "network_blocks",
        "network_unblocks",
        "network_partitions",
        "network_partition_heals",
        "directional_network_policy_verifications",
        "topology_edge_updates",
        "topology_edge_rollback_failures",
        "wallet_funding",
        "wallet_transactions",
        "transaction_load_attempts",
        "transaction_load_summaries",
        "operator_commands",
    };
    for (const std::string_view field : kArrayFields) {
      ThrowIfReportCancelled(stop_token);
      report[field] = boost::json::array{};
    }
    report["operator_connection_command"] = nullptr;

    event_count = 0;
    metric_count = 0;
    scheduled_block_count = 0;
    scheduled_event_started_count = 0;
    scheduled_event_completed_count = 0;
    scheduled_event_failed_count = 0;
    checkpoint_count = 0;
    wallet_transaction_count = 0;
    transaction_load_attempt_count = 0;
    transaction_load_completed_count = 0;
    run_started = false;
    run_finished = false;
    run_failed = false;
    run_cancelled = false;
    simulation_duration_reached = false;
    event_counts.clear();
    nodes.clear();
    wallets.clear();
    active_network_partitions.clear();
    transaction_load_live.clear();
    event_cursor = {};
    metric_cursor = {};
    wallet_metric_cursor = {};
    stats = {};
    RememberScenarioFile();
    UpdateReport();
  }

  void RememberScenarioFile() {
    const std::filesystem::path path = run_root / "resolved-scenario.json";
    scenario_exists = std::filesystem::exists(path);
    if (!scenario_exists) {
      scenario_size = 0;
      scenario_write_time.reset();
      return;
    }
    scenario_size = std::filesystem::file_size(path);
    scenario_write_time = std::filesystem::last_write_time(path);
  }

  bool ScenarioFileChanged() const {
    const std::filesystem::path path = run_root / "resolved-scenario.json";
    const bool exists = std::filesystem::exists(path);
    if (exists != scenario_exists) {
      return true;
    }
    if (!exists) {
      return false;
    }
    return std::filesystem::file_size(path) != scenario_size ||
           std::filesystem::last_write_time(path) != scenario_write_time;
  }

  static std::optional<FileObservation> ObserveFile(
      const std::filesystem::path& path) {
    struct stat status{};
    if (::stat(path.c_str(), &status) != 0) {
      const int error = errno;
      if (error == ENOENT) {
        return std::nullopt;
      }
      throw std::filesystem::filesystem_error(
          "stat failed", path, std::error_code(error, std::generic_category()));
    }
    if (!S_ISREG(status.st_mode) || status.st_size < 0) {
      throw std::runtime_error("JSONL input is not a regular file: " +
                               path.string());
    }
    return FileObservation{
        .device = static_cast<std::uintmax_t>(status.st_dev),
        .inode = static_cast<std::uintmax_t>(status.st_ino),
        .size = static_cast<std::uintmax_t>(status.st_size),
        .modified_seconds = static_cast<std::int64_t>(status.st_mtim.tv_sec),
        .modified_nanoseconds =
            static_cast<std::int64_t>(status.st_mtim.tv_nsec),
    };
  }

  static bool SameFile(const FileObservation& left,
                       const FileObservation& right) {
    return left.device == right.device && left.inode == right.inode;
  }

  bool InputFileWasReplaced(const std::filesystem::path& path,
                            const InputCursor& cursor) const {
    const std::optional<FileObservation> current = ObserveFile(path);
    if (!cursor.observed_exists) {
      return false;
    }
    if (!current) {
      return true;
    }
    if (!SameFile(*current, cursor.observation) ||
        current->size < cursor.observation.size ||
        current->size < cursor.offset) {
      return true;
    }
    return current->size == cursor.observation.size &&
           (current->modified_seconds != cursor.observation.modified_seconds ||
            current->modified_nanoseconds !=
                cursor.observation.modified_nanoseconds);
  }

  bool AnyInputFileWasReplaced() const {
    return InputFileWasReplaced(run_root / "events.jsonl", event_cursor) ||
           InputFileWasReplaced(run_root / "metrics.jsonl", metric_cursor) ||
           InputFileWasReplaced(run_root / "wallet-metrics.jsonl",
                                wallet_metric_cursor);
  }

  template <typename Callback>
  bool ConsumeFile(const std::filesystem::path& path, InputCursor* cursor,
                   std::size_t maximum_records, std::uint64_t* records_read,
                   std::stop_token stop_token, Callback callback) {
    ThrowIfReportCancelled(stop_token);
    const std::optional<FileObservation> initial_observation =
        ObserveFile(path);
    if (!initial_observation) {
      cursor->observed_exists = false;
      cursor->observation = {};
      return false;
    }
    if (cursor->offset > static_cast<std::uint64_t>(
                             std::numeric_limits<std::streamoff>::max())) {
      throw std::runtime_error("JSONL offset exceeds stream range for " +
                               path.string());
    }

    std::ifstream input(path);
    if (!input) {
      throw std::runtime_error("open failed for " + path.string());
    }
    input.seekg(static_cast<std::streamoff>(cursor->offset));
    if (!input) {
      throw std::runtime_error("seek failed for " + path.string());
    }

    std::string line;
    std::size_t lines_consumed = 0;
    while (lines_consumed < maximum_records) {
      ThrowIfReportCancelled(stop_token);
      const std::uint64_t line_offset = cursor->offset;
      if (!std::getline(input, line)) {
        break;
      }
      if (input.eof()) {
        cursor->offset = line_offset;
        break;
      }
      const std::streampos next_offset = input.tellg();
      if (next_offset < 0) {
        throw std::runtime_error("tell failed for " + path.string());
      }
      const std::uint64_t next_line_number = cursor->line_number + 1U;
      if (!line.empty()) {
        const boost::json::object object =
            ParseJsonObjectLine(path, line, next_line_number);
        callback(object);
        ++*records_read;
      }
      cursor->offset = static_cast<std::uint64_t>(next_offset);
      cursor->line_number = next_line_number;
      ++lines_consumed;
    }

    ThrowIfReportCancelled(stop_token);
    cursor->observed_exists = true;
    const std::optional<FileObservation> final_observation = ObserveFile(path);
    if (!final_observation ||
        !SameFile(*initial_observation, *final_observation)) {
      cursor->observation = *initial_observation;
      return true;
    }
    cursor->observation = *final_observation;
    return cursor->offset < cursor->observation.size;
  }

  void ProcessEvent(const boost::json::object& event) {
    RequireMatchingRunId(event, "event");
    ++event_count;
    const std::string event_name = OptionalStringField(event, "event");
    const std::string node_id = OptionalStringField(event, "node_id");
    ++event_counts[event_name];
    const std::optional<SimulationEventKind> event_kind =
        SimulationEventKindFromName(event_name);
    if (!event_kind) {
      return;
    }
    if (!node_id.empty() && node_id != "sim" && IsNodeErrorEvent(*event_kind)) {
      nodes[node_id].last_error = NodeErrorText(event);
      nodes[node_id].last_error_kind = *event_kind;
    }
    switch (*event_kind) {
      case SimulationEventKind::kRunStarted:
        run_started = true;
        report["started_at"] = OptionalStringField(event, "timestamp");
        break;
      case SimulationEventKind::kRunFinished:
        run_finished = true;
        report["finished_at"] = OptionalStringField(event, "timestamp");
        break;
      case SimulationEventKind::kRunFailed: {
        run_failed = true;
        report["failed_at"] = OptionalStringField(event, "timestamp");
        boost::json::value detail = ParseEventDetail(event);
        if (!detail.is_null()) {
          report["failure"] = std::move(detail);
        }
        break;
      }
      case SimulationEventKind::kRunCancelled:
        run_cancelled = true;
        report["cancelled_at"] = OptionalStringField(event, "timestamp");
        break;
      case SimulationEventKind::kSimulationDurationReached: {
        simulation_duration_reached = true;
        boost::json::value detail = ParseEventDetail(event);
        if (!detail.is_null()) {
          report["simulation_duration"] = std::move(detail);
        }
        break;
      }
      case SimulationEventKind::kState:
        if (!node_id.empty()) {
          RememberNodeState(OptionalStringField(event, "detail"),
                            &nodes[node_id]);
        }
        break;
      case SimulationEventKind::kRpcReady:
        if (!node_id.empty() && nodes[node_id].last_error_kind &&
            IsRpcRecoveryErrorEvent(*nodes[node_id].last_error_kind)) {
          nodes[node_id].last_error.clear();
          nodes[node_id].last_error_kind.reset();
        }
        break;
      case SimulationEventKind::kOperatorConnectionCommand: {
        boost::json::value detail = ParseEventDetail(event);
        if (detail.is_object()) {
          boost::json::object connection = detail.as_object();
          connection["node_id"] = node_id;
          connection["timestamp"] = OptionalStringField(event, "timestamp");
          report["operator_connection_command"] = std::move(connection);
        }
        break;
      }
      case SimulationEventKind::kStdoutTail:
      case SimulationEventKind::kStderrTail:
      case SimulationEventKind::kDaemonLogTail:
        if (!node_id.empty()) {
          const std::optional<std::string_view> log_kind =
              LogTailKind(*event_kind);
          RememberLogTail(ParseEventDetail(event), *log_kind,
                          &nodes[node_id].log_tails);
        }
        break;
      case SimulationEventKind::kGeneratedBlocks:
        AppendEventSummary(event, &Array("generated_blocks"));
        break;
      case SimulationEventKind::kNodeStartDeadlineReached:
        AppendEventSummary(event, &Array("node_start_deadlines"));
        break;
      case SimulationEventKind::kNodeStopDeadlineReached:
        AppendEventSummary(event, &Array("node_stop_deadlines"));
        break;
      case SimulationEventKind::kProcessExited:
        AppendEventSummary(event, &Array("process_exits"));
        break;
      case SimulationEventKind::kProcessExitedBeforeRpcReady:
        AppendEventSummary(event, &Array("process_exits"));
        break;
      case SimulationEventKind::kRestartPolicyApplied:
        AppendEventSummary(event, &Array("restart_policy_actions"));
        break;
      case SimulationEventKind::kScheduledBlockProduced:
        ++scheduled_block_count;
        AppendScheduledBlockSummary(event, &Array("scheduled_blocks"));
        break;
      case SimulationEventKind::kScheduledEventStarted:
        ++scheduled_event_started_count;
        AppendScheduledEventSummary(event, &Array("scheduled_events_started"));
        break;
      case SimulationEventKind::kScheduledEventCompleted:
        ++scheduled_event_completed_count;
        AppendScheduledEventSummary(event,
                                    &Array("scheduled_events_completed"));
        break;
      case SimulationEventKind::kScheduledEventFailed:
        ++scheduled_event_failed_count;
        AppendScheduledEventSummary(event, &Array("scheduled_events_failed"));
        break;
      case SimulationEventKind::kCheckpointRecorded:
        ++checkpoint_count;
        AppendCheckpointSummary(event, &Array("checkpoints"));
        break;
      case SimulationEventKind::kHeightReached:
        AppendEventSummary(event, &Array("height_reached"));
        break;
      case SimulationEventKind::kHeightWaitReached:
        AppendEventSummary(event, &Array("height_waits"));
        break;
      case SimulationEventKind::kPeerCountReached:
        AppendEventSummary(event, &Array("peer_waits"));
        break;
      case SimulationEventKind::kPeerConnected:
        AppendEventSummary(event, &Array("peer_connects"));
        break;
      case SimulationEventKind::kPeerDisconnected:
        AppendEventSummary(event, &Array("peer_disconnects"));
        break;
      case SimulationEventKind::kRawTransactionSubmitted:
        AppendEventSummary(event, &Array("raw_transactions"));
        break;
      case SimulationEventKind::kTransactionVisible:
        RememberFallbackTransactionLoadObservation(event, "propagated");
        AppendEventSummary(event, &Array("transaction_visibility"));
        break;
      case SimulationEventKind::kTransactionConfirmed:
        RememberFallbackTransactionLoadObservation(event, "confirmed");
        AppendEventSummary(event, &Array("transaction_confirmations"));
        break;
      case SimulationEventKind::kTransactionLoadAttempt:
        ++transaction_load_attempt_count;
        RememberFallbackTransactionLoadAttempt(event);
        AppendBoundedEventSummary(event,
                                  kMaximumTransactionLoadAttemptSummaries,
                                  &Array("transaction_load_attempts"));
        break;
      case SimulationEventKind::kTransactionLoadProgress:
        RememberTransactionLoadSnapshot(event, false);
        break;
      case SimulationEventKind::kTransactionLoadCompleted:
        ++transaction_load_completed_count;
        RememberTransactionLoadSnapshot(event, true);
        AppendBoundedEventSummary(event,
                                  kMaximumTransactionLoadCompletionSummaries,
                                  &Array("transaction_load_summaries"));
        break;
      case SimulationEventKind::kNodeRestarted:
        AppendEventSummary(event, &Array("node_restarts"));
        break;
      case SimulationEventKind::kNodeFreezeCompleted:
        AppendEventSummary(event, &Array("node_freezes"));
        break;
      case SimulationEventKind::kResourceLimitsUpdated:
        AppendEventSummary(event, &Array("resource_updates"));
        break;
      case SimulationEventKind::kResourceProfileUpdated:
        AppendBoundedProfileUpdateSummary(event,
                                          &Array("resource_profile_updates"));
        break;
      case SimulationEventKind::kNetworkProfileUpdated:
        AppendBoundedProfileUpdateSummary(event,
                                          &Array("network_profile_updates"));
        break;
      case SimulationEventKind::kProfileUpdateRollbackFailed:
        AppendBoundedProfileUpdateSummary(
            event, &Array("profile_update_rollback_failures"));
        break;
      case SimulationEventKind::kNetworkConditionUpdated:
        AppendEventSummary(event, &Array("network_condition_updates"));
        break;
      case SimulationEventKind::kNetworkBlockApplied:
        AppendEventSummary(event, &Array("network_blocks"));
        break;
      case SimulationEventKind::kNetworkBlockRemoved:
        AppendEventSummary(event, &Array("network_unblocks"));
        break;
      case SimulationEventKind::kNetworkPartitionApplied:
        AppendEventSummary(event, &Array("network_partitions"));
        RememberActivePartition(event, true, &active_network_partitions);
        break;
      case SimulationEventKind::kNetworkPartitionHealed:
        AppendEventSummary(event, &Array("network_partition_heals"));
        RememberActivePartition(event, false, &active_network_partitions);
        break;
      case SimulationEventKind::kRuntimeGenerationPublished: {
        const boost::json::value detail = ParseEventDetail(event);
        if (!detail.is_object()) {
          throw std::runtime_error(
              "runtime generation publication detail is not an object");
        }
        const boost::json::object& publication = detail.as_object();
        const std::optional<std::uint64_t> generation =
            OptionalUint64Field(publication, "generation");
        const std::optional<std::uint64_t> node_count =
            OptionalUint64Field(publication, "node_count");
        const boost::json::value* node_ids =
            publication.if_contains("node_ids");
        const boost::json::value* node_configs =
            publication.if_contains("node_configs");
        const boost::json::value* topology =
            publication.if_contains("topology");
        const boost::json::value* topology_current_edges =
            publication.if_contains("topology_current_edges");
        if (!generation || !node_count || node_ids == nullptr ||
            !node_ids->is_array() || node_configs == nullptr ||
            !node_configs->is_array() || topology == nullptr ||
            !topology->is_object() || topology_current_edges == nullptr ||
            !topology_current_edges->is_array() ||
            node_ids->as_array().size() != *node_count ||
            node_configs->as_array().size() != *node_count ||
            OptionalStringField(publication, "manifest_state") != "live") {
          throw std::runtime_error(
              "runtime generation publication is incomplete");
        }
        std::set<std::string> active_node_ids;
        for (const boost::json::value& value : node_ids->as_array()) {
          if (!value.is_string() ||
              !active_node_ids.insert(std::string(value.as_string())).second) {
            throw std::runtime_error(
                "runtime generation publication node ids are invalid");
          }
        }
        for (const boost::json::value& value : node_configs->as_array()) {
          if (!value.is_object()) {
            throw std::runtime_error(
                "runtime generation publication node config is invalid");
          }
          const boost::json::object& config = value.as_object();
          const std::string node_id = OptionalStringField(config, "id");
          if (!active_node_ids.contains(node_id)) {
            throw std::runtime_error(
                "runtime generation node config identity is inconsistent");
          }
          NodeReport& node = nodes[node_id];
          node.index = OptionalUint64Field(config, "index");
          node.chain = OptionalStringField(config, "chain");
          node.role = OptionalStringField(config, "role");
          const std::string lifecycle =
              OptionalStringField(config, "lifecycle");
          if (!lifecycle.empty()) {
            RememberNodeState(lifecycle, &node);
          }
        }
        std::erase_if(nodes, [&](const auto& item) {
          return !active_node_ids.contains(item.first);
        });
        report["inventory_generation"] = *generation;
        report["nodes"] = *node_count;
        report["node_ids"] = *node_ids;
        report["node_configs"] = *node_configs;
        report["topology"] = *topology;
        report["topology_current_edges"] = *topology_current_edges;
        break;
      }
      case SimulationEventKind::kDirectionalNetworkPoliciesVerified:
        AppendDirectionalPolicyVerification(
            event, &Array("directional_network_policy_verifications"));
        break;
      case SimulationEventKind::kTopologyEdgeUpdated:
        RememberTopologyEdgeUpdate(event, &Array("topology_edge_updates"),
                                   &Array("topology_current_edges"));
        break;
      case SimulationEventKind::kTopologyEdgeUpdateRollbackFailed:
        AppendBoundedTopologyEdgeSummary(
            event, &Array("topology_edge_rollback_failures"));
        break;
      case SimulationEventKind::kWalletAddressRequested:
      case SimulationEventKind::kWalletAddressCreated:
        RememberWalletAddressEvent(ParseEventDetail(event), &wallets);
        break;
      case SimulationEventKind::kWalletFunded: {
        boost::json::value detail = ParseEventDetail(event);
        RememberWalletFundingEvent(detail, &wallets);
        AppendEventSummary(event, &Array("wallet_funding"));
        break;
      }
      case SimulationEventKind::kWalletTransactionSubmitted: {
        ++wallet_transaction_count;
        boost::json::value detail = ParseEventDetail(event);
        RememberWalletTransactionEvent(detail, &wallets);
        AppendBoundedEventSummary(event, kMaximumWalletTransactionSummaries,
                                  &Array("wallet_transactions"));
        break;
      }
      case SimulationEventKind::kOperatorCommandCompleted:
        AppendOperatorCommandSummary(event, OperatorCommandStatus::kCompleted,
                                     &Array("operator_commands"));
        ApplyBlockProductionPolicyEvent(event, &report);
        break;
      case SimulationEventKind::kOperatorCommandFailed:
        AppendOperatorCommandSummary(event, OperatorCommandStatus::kFailed,
                                     &Array("operator_commands"));
        break;
      default:
        break;
    }
  }

  void ProcessMetric(const boost::json::object& metric) {
    RequireMatchingRunId(metric, "metric");
    ++metric_count;
    const std::string node_id = OptionalStringField(metric, "node_id");
    if (node_id.empty()) {
      return;
    }
    NodeReport& node = nodes[node_id];
    if (!node.index) {
      node.index = OptionalUint64Field(metric, "node_index");
    }
    if (node.chain.empty()) {
      node.chain = OptionalStringField(metric, "chain");
    }
    if (node.role.empty()) {
      node.role = OptionalStringField(metric, "role");
    }
    ++node.metric_samples;
    node.last_metrics = {};
    CopySelectedMetricFields(metric, &node.last_metrics);
    AddNodeDerivedMetrics(metric, node, &node.last_metrics);
    AppendNodeMetricHistory(node.last_metrics, &node);
    RememberNodeMetricSample(metric, &node);
  }

  void ProcessWalletMetric(const boost::json::object& metric) {
    RequireMatchingRunId(metric, "wallet metric");
    const std::optional<std::uint64_t> wallet_index =
        OptionalUint64Field(metric, "wallet_index");
    if (!wallet_index || *wallet_index == 0U) {
      return;
    }
    WalletReport& wallet = wallets[*wallet_index];
    wallet.wallet_index = *wallet_index;
    CopyOptionalUint64Field(metric, "node", &wallet.node);
    CopyOptionalWalletModeField(metric, "mode", &wallet);
    wallet.last_metrics = metric;
  }

  void UpdateReport() {
    const bool ok =
        run_started && run_finished && !run_failed && !run_cancelled;
    const RunReportStatus status =
        run_failed ? RunReportStatus::kFailed
                   : (run_cancelled ? RunReportStatus::kCancelled
                                    : (ok ? RunReportStatus::kFinished
                                          : RunReportStatus::kIncomplete));
    SeedConfiguredNodes(report, &nodes);
    report["ok"] = ok;
    report["status"] = RunReportStatusName(status);
    report["simulation_duration_reached"] = simulation_duration_reached;
    report["event_count"] = event_count;
    report["summary_retention_limit"] = kMaximumRunReportSummaryRecords;
    report["metric_count"] = metric_count;
    report["event_counts"] = EventCountsJson(event_counts);
    report["scheduled_block_count"] = scheduled_block_count;
    report["scheduled_event_started_count"] = scheduled_event_started_count;
    report["scheduled_event_completed_count"] = scheduled_event_completed_count;
    report["scheduled_event_failed_count"] = scheduled_event_failed_count;
    report["checkpoint_count"] = checkpoint_count;
    report["wallet_transaction_count"] = wallet_transaction_count;
    report["transaction_load_attempt_count"] = transaction_load_attempt_count;
    report["transaction_load_completed_count"] =
        transaction_load_completed_count;
    report["transaction_load_live"] = TransactionLoadLiveJson();
    report["active_network_partitions"] =
        ActivePartitionsJson(active_network_partitions);
    report["wallets_summary"] = WalletsJson(wallets);
    report["nodes_summary"] = NodesJson(nodes);
    AddTopologyViewSummaries(&report);
  }

  void RequireMatchingRunId(const boost::json::object& record,
                            std::string_view record_kind) const {
    if (expected_run_id.empty()) {
      return;
    }
    const boost::json::value* run_id = record.if_contains("run_id");
    if (run_id == nullptr || !run_id->is_string() ||
        run_id->as_string() != expected_run_id) {
      throw std::runtime_error(std::string(record_kind) +
                               " run_id does not match resolved scenario");
    }
  }

  const boost::json::object& Refresh(std::size_t maximum_records_per_file,
                                     std::stop_token stop_token) {
    ThrowIfReportCancelled(stop_token);
    if (ScenarioFileChanged() || AnyInputFileWasReplaced()) {
      Reset(stop_token);
    }
    ThrowIfReportCancelled(stop_token);
    stats = {};
    const bool event_backlog = ConsumeFile(
        run_root / "events.jsonl", &event_cursor, maximum_records_per_file,
        &stats.event_records, stop_token,
        [this](const boost::json::object& event) { ProcessEvent(event); });
    ThrowIfReportCancelled(stop_token);
    const bool metric_backlog = ConsumeFile(
        run_root / "metrics.jsonl", &metric_cursor, maximum_records_per_file,
        &stats.metric_records, stop_token,
        [this](const boost::json::object& metric) { ProcessMetric(metric); });
    ThrowIfReportCancelled(stop_token);
    const bool wallet_metric_backlog =
        ConsumeFile(run_root / "wallet-metrics.jsonl", &wallet_metric_cursor,
                    maximum_records_per_file, &stats.wallet_metric_records,
                    stop_token, [this](const boost::json::object& metric) {
                      ProcessWalletMetric(metric);
                    });
    stats.has_backlog =
        event_backlog || metric_backlog || wallet_metric_backlog;
    ThrowIfReportCancelled(stop_token);
    UpdateReport();
    ThrowIfReportCancelled(stop_token);
    return report;
  }

  std::filesystem::path run_root;
  std::string expected_run_id;
  boost::json::object report;
  std::uint64_t event_count = 0;
  std::uint64_t metric_count = 0;
  std::uint64_t scheduled_block_count = 0;
  std::uint64_t scheduled_event_started_count = 0;
  std::uint64_t scheduled_event_completed_count = 0;
  std::uint64_t scheduled_event_failed_count = 0;
  std::uint64_t checkpoint_count = 0;
  std::uint64_t wallet_transaction_count = 0;
  std::uint64_t transaction_load_attempt_count = 0;
  std::uint64_t transaction_load_completed_count = 0;
  bool run_started = false;
  bool run_finished = false;
  bool run_failed = false;
  bool run_cancelled = false;
  bool simulation_duration_reached = false;
  std::map<std::string, std::uint64_t> event_counts;
  std::map<std::string, NodeReport> nodes;
  std::map<std::uint64_t, WalletReport> wallets;
  std::map<std::string, boost::json::object> active_network_partitions;
  std::map<std::uint64_t, TransactionLoadLiveReport> transaction_load_live;
  InputCursor event_cursor;
  InputCursor metric_cursor;
  InputCursor wallet_metric_cursor;
  bool scenario_exists = false;
  std::uintmax_t scenario_size = 0;
  std::optional<std::filesystem::file_time_type> scenario_write_time;
  RunReportRefreshStats stats;
};

IncrementalRunReport::IncrementalRunReport(
    const std::filesystem::path& run_root, std::stop_token stop_token)
    : impl_(std::make_unique<Impl>(run_root, stop_token)) {}

IncrementalRunReport::~IncrementalRunReport() = default;

IncrementalRunReport::IncrementalRunReport(IncrementalRunReport&&) noexcept =
    default;

IncrementalRunReport& IncrementalRunReport::operator=(
    IncrementalRunReport&&) noexcept = default;

const boost::json::object& IncrementalRunReport::Refresh(
    std::size_t maximum_records_per_file, std::stop_token stop_token) {
  return impl_->Refresh(maximum_records_per_file, stop_token);
}

const RunReportRefreshStats& IncrementalRunReport::last_refresh_stats() const {
  return impl_->stats;
}

boost::json::object BuildRunReport(const std::filesystem::path& run_root,
                                   std::stop_token stop_token) {
  IncrementalRunReport report(run_root, stop_token);
  return report.Refresh(std::numeric_limits<std::size_t>::max(), stop_token);
}

std::string BuildRunReportJson(const std::filesystem::path& run_root,
                               std::stop_token stop_token) {
  return boost::json::serialize(BuildRunReport(run_root, stop_token));
}

std::string BuildNodeReportJson(const std::filesystem::path& run_root,
                                std::string_view node_id,
                                std::uint64_t operator_command_sequence,
                                std::stop_token stop_token) {
  if (node_id.empty()) {
    throw std::runtime_error("node report requires a node id");
  }
  const boost::json::object full_report = BuildRunReport(run_root, stop_token);
  ThrowIfReportCancelled(stop_token);
  const boost::json::array* nodes = ArrayField(full_report, "nodes_summary");
  if (nodes == nullptr) {
    throw std::runtime_error("run report has no node summaries");
  }
  const boost::json::object* selected_node = nullptr;
  for (const boost::json::value& node_value : *nodes) {
    ThrowIfReportCancelled(stop_token);
    if (node_value.is_object() &&
        OptionalStringField(node_value.as_object(), "node_id") == node_id) {
      selected_node = &node_value.as_object();
      break;
    }
  }
  if (selected_node == nullptr) {
    throw std::runtime_error("node report references unknown node: " +
                             std::string(node_id));
  }

  boost::json::object report;
  constexpr std::string_view kContextFields[] = {
      "run_root",
      "run_id",
      "chain",
      "status",
      "ok",
      "started_at",
      "finished_at",
      "failed_at",
      "failure",
      "event_count",
      "event_counts",
      "metric_count",
      "topology",
      "topology_current_edges",
      "active_network_partitions",
      "topology_degraded_links",
      "topology_blocked_rules",
      "topology_groups_summary",
      "resource_profiles",
      "network_profiles",
  };
  for (const std::string_view field : kContextFields) {
    ThrowIfReportCancelled(stop_token);
    CopyField(full_report, field, &report);
  }
  report["operator_command_sequence"] = operator_command_sequence;
  report["node"] = *selected_node;
  ThrowIfReportCancelled(stop_token);
  return boost::json::serialize(report);
}

}  // namespace bbp
