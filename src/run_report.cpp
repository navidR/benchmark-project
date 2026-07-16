#include "bbp/run_report.h"

#include <algorithm>
#include <boost/json/array.hpp>
#include <boost/json/object.hpp>
#include <boost/json/parse.hpp>
#include <boost/json/serialize.hpp>
#include <boost/json/value.hpp>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <limits>
#include <map>
#include <optional>
#include <stdexcept>
#include <string>
#include <string_view>

#include "bbp/operator_command_status.h"
#include "bbp/simulation_command.h"
#include "bbp/simulation_event_kind.h"
#include "bbp/simulation_registry.h"
#include "bbp/simulator/node_runtime_lifecycle.h"
#include "bbp/util.h"

namespace bbp {
namespace {

constexpr std::size_t kMaximumNodeLogTailBytes = 256U * 1024U;
constexpr std::size_t kMaximumOperatorCommandSummaries = 256U;
constexpr std::size_t kMaximumScheduledBlockSummaries = 256U;
constexpr std::size_t kMaximumScheduledEventSummaries = 256U;
constexpr std::size_t kMaximumDirectionalPolicyVerifications = 256U;
constexpr std::size_t kMaximumTopologyEdgeSummaries = 256U;
constexpr std::size_t kMaximumProfileUpdateSummaries = 256U;

struct NodeReport {
  std::optional<std::uint64_t> index;
  std::string chain;
  std::string role;
  std::string last_error;
  std::uint64_t metric_samples = 0;
  std::optional<NodeRuntimeLifecycle> final_state;
  std::string unknown_final_state;
  boost::json::object last_metrics;
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
  kIncomplete,
};

std::string_view RunReportStatusName(RunReportStatus status) {
  switch (status) {
    case RunReportStatus::kFinished:
      return "finished";
    case RunReportStatus::kFailed:
      return "failed";
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

void LoadResolvedScenario(const std::filesystem::path& path,
                          boost::json::object* report) {
  if (!std::filesystem::exists(path)) {
    return;
  }
  boost::json::value value = boost::json::parse(ReadText(path));
  if (!value.is_object()) {
    throw std::runtime_error("resolved scenario is not a JSON object: " +
                             path.string());
  }
  const boost::json::object& scenario = value.as_object();
  CopyField(scenario, "run_id", report);
  CopyField(scenario, "chain", report);
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
  CopyField(scenario, "runtime_node_restarts", report);
  CopyField(scenario, "runtime_node_freezes", report);
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

std::string BuildRunReportJson(const std::filesystem::path& run_root) {
  if (!std::filesystem::is_directory(run_root)) {
    throw std::runtime_error("run root is not a directory: " +
                             run_root.string());
  }

  boost::json::object report;
  report["run_root"] = std::filesystem::absolute(run_root).string();
  LoadResolvedScenario(run_root / "resolved-scenario.json", &report);
  boost::json::array topology_current_edges;
  const boost::json::value* topology_initial_edges =
      report.if_contains("topology_initial_edges");
  if (topology_initial_edges != nullptr && topology_initial_edges->is_array()) {
    topology_current_edges = topology_initial_edges->as_array();
  }

  std::uint64_t event_count = 0;
  std::uint64_t metric_count = 0;
  std::uint64_t scheduled_block_count = 0;
  std::uint64_t scheduled_event_started_count = 0;
  std::uint64_t scheduled_event_completed_count = 0;
  std::uint64_t scheduled_event_failed_count = 0;
  bool run_started = false;
  bool run_finished = false;
  bool run_failed = false;
  std::string started_at;
  std::string finished_at;
  std::string failed_at;
  boost::json::value failure_detail;
  std::map<std::string, std::uint64_t> event_counts;
  std::map<std::string, NodeReport> nodes;
  boost::json::array generated_blocks;
  boost::json::array scheduled_blocks;
  boost::json::array scheduled_events_started;
  boost::json::array scheduled_events_completed;
  boost::json::array scheduled_events_failed;
  boost::json::array height_reached;
  boost::json::array height_waits;
  boost::json::array peer_waits;
  boost::json::array peer_connects;
  boost::json::array peer_disconnects;
  boost::json::array raw_transactions;
  boost::json::array transaction_visibility;
  boost::json::array transaction_confirmations;
  boost::json::array node_restarts;
  boost::json::array node_freezes;
  boost::json::array resource_updates;
  boost::json::array resource_profile_updates;
  boost::json::array network_profile_updates;
  boost::json::array profile_update_rollback_failures;
  boost::json::array network_condition_updates;
  boost::json::array network_blocks;
  boost::json::array network_unblocks;
  boost::json::array network_partitions;
  boost::json::array network_partition_heals;
  boost::json::array directional_network_policy_verifications;
  boost::json::array topology_edge_updates;
  boost::json::array topology_edge_rollback_failures;
  boost::json::array wallet_funding;
  boost::json::array wallet_transactions;
  boost::json::array operator_commands;
  std::map<std::uint64_t, WalletReport> wallets;

  ForEachJsonLine(
      run_root / "events.jsonl", [&](const boost::json::object& event) {
        ++event_count;
        const std::string event_name = OptionalStringField(event, "event");
        const std::string node_id = OptionalStringField(event, "node_id");
        ++event_counts[event_name];
        const std::optional<SimulationEventKind> event_kind =
            SimulationEventKindFromName(event_name);
        if (!event_kind) {
          return;
        }
        if (!node_id.empty() && node_id != "sim" &&
            IsNodeErrorEvent(*event_kind)) {
          nodes[node_id].last_error = NodeErrorText(event);
        }
        switch (*event_kind) {
          case SimulationEventKind::kRunStarted:
            run_started = true;
            started_at = OptionalStringField(event, "timestamp");
            break;
          case SimulationEventKind::kRunFinished:
            run_finished = true;
            finished_at = OptionalStringField(event, "timestamp");
            break;
          case SimulationEventKind::kRunFailed:
            run_failed = true;
            failed_at = OptionalStringField(event, "timestamp");
            failure_detail = ParseEventDetail(event);
            break;
          case SimulationEventKind::kState:
            if (!node_id.empty()) {
              RememberNodeState(OptionalStringField(event, "detail"),
                                &nodes[node_id]);
            }
            break;
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
            AppendEventSummary(event, &generated_blocks);
            break;
          case SimulationEventKind::kScheduledBlockProduced:
            ++scheduled_block_count;
            AppendScheduledBlockSummary(event, &scheduled_blocks);
            break;
          case SimulationEventKind::kScheduledEventStarted:
            ++scheduled_event_started_count;
            AppendScheduledEventSummary(event, &scheduled_events_started);
            break;
          case SimulationEventKind::kScheduledEventCompleted:
            ++scheduled_event_completed_count;
            AppendScheduledEventSummary(event, &scheduled_events_completed);
            break;
          case SimulationEventKind::kScheduledEventFailed:
            ++scheduled_event_failed_count;
            AppendScheduledEventSummary(event, &scheduled_events_failed);
            break;
          case SimulationEventKind::kHeightReached:
            AppendEventSummary(event, &height_reached);
            break;
          case SimulationEventKind::kHeightWaitReached:
            AppendEventSummary(event, &height_waits);
            break;
          case SimulationEventKind::kPeerCountReached:
            AppendEventSummary(event, &peer_waits);
            break;
          case SimulationEventKind::kPeerConnected:
            AppendEventSummary(event, &peer_connects);
            break;
          case SimulationEventKind::kPeerDisconnected:
            AppendEventSummary(event, &peer_disconnects);
            break;
          case SimulationEventKind::kRawTransactionSubmitted:
            AppendEventSummary(event, &raw_transactions);
            break;
          case SimulationEventKind::kTransactionVisible:
            AppendEventSummary(event, &transaction_visibility);
            break;
          case SimulationEventKind::kTransactionConfirmed:
            AppendEventSummary(event, &transaction_confirmations);
            break;
          case SimulationEventKind::kNodeRestarted:
            AppendEventSummary(event, &node_restarts);
            break;
          case SimulationEventKind::kNodeFreezeCompleted:
            AppendEventSummary(event, &node_freezes);
            break;
          case SimulationEventKind::kResourceLimitsUpdated:
            AppendEventSummary(event, &resource_updates);
            break;
          case SimulationEventKind::kResourceProfileUpdated:
            AppendBoundedProfileUpdateSummary(event, &resource_profile_updates);
            break;
          case SimulationEventKind::kNetworkProfileUpdated:
            AppendBoundedProfileUpdateSummary(event, &network_profile_updates);
            break;
          case SimulationEventKind::kProfileUpdateRollbackFailed:
            AppendBoundedProfileUpdateSummary(
                event, &profile_update_rollback_failures);
            break;
          case SimulationEventKind::kNetworkConditionUpdated:
            AppendEventSummary(event, &network_condition_updates);
            break;
          case SimulationEventKind::kNetworkBlockApplied:
            AppendEventSummary(event, &network_blocks);
            break;
          case SimulationEventKind::kNetworkBlockRemoved:
            AppendEventSummary(event, &network_unblocks);
            break;
          case SimulationEventKind::kNetworkPartitionApplied:
            AppendEventSummary(event, &network_partitions);
            break;
          case SimulationEventKind::kNetworkPartitionHealed:
            AppendEventSummary(event, &network_partition_heals);
            break;
          case SimulationEventKind::kDirectionalNetworkPoliciesVerified:
            AppendDirectionalPolicyVerification(
                event, &directional_network_policy_verifications);
            break;
          case SimulationEventKind::kTopologyEdgeUpdated:
            RememberTopologyEdgeUpdate(event, &topology_edge_updates,
                                       &topology_current_edges);
            break;
          case SimulationEventKind::kTopologyEdgeUpdateRollbackFailed:
            AppendBoundedTopologyEdgeSummary(event,
                                             &topology_edge_rollback_failures);
            break;
          case SimulationEventKind::kWalletAddressRequested:
          case SimulationEventKind::kWalletAddressCreated:
            RememberWalletAddressEvent(ParseEventDetail(event), &wallets);
            break;
          case SimulationEventKind::kWalletFunded: {
            boost::json::value detail = ParseEventDetail(event);
            RememberWalletFundingEvent(detail, &wallets);
            AppendEventSummary(event, &wallet_funding);
            break;
          }
          case SimulationEventKind::kWalletTransactionSubmitted: {
            boost::json::value detail = ParseEventDetail(event);
            RememberWalletTransactionEvent(detail, &wallets);
            AppendEventSummary(event, &wallet_transactions);
            break;
          }
          case SimulationEventKind::kOperatorCommandCompleted:
            AppendOperatorCommandSummary(
                event, OperatorCommandStatus::kCompleted, &operator_commands);
            ApplyBlockProductionPolicyEvent(event, &report);
            break;
          case SimulationEventKind::kOperatorCommandFailed:
            AppendOperatorCommandSummary(event, OperatorCommandStatus::kFailed,
                                         &operator_commands);
            break;
          default:
            break;
        }
      });

  ForEachJsonLine(
      run_root / "metrics.jsonl", [&](const boost::json::object& metric) {
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
        RememberNodeMetricSample(metric, &node);
      });

  ForEachJsonLine(run_root / "wallet-metrics.jsonl",
                  [&](const boost::json::object& metric) {
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
                  });

  const bool ok = run_started && run_finished && !run_failed;
  const RunReportStatus status =
      ok ? RunReportStatus::kFinished
         : (run_failed ? RunReportStatus::kFailed
                       : RunReportStatus::kIncomplete);
  SeedConfiguredNodes(report, &nodes);
  report["ok"] = ok;
  report["status"] = RunReportStatusName(status);
  if (!started_at.empty()) {
    report["started_at"] = started_at;
  }
  if (!finished_at.empty()) {
    report["finished_at"] = finished_at;
  }
  if (!failed_at.empty()) {
    report["failed_at"] = failed_at;
  }
  if (!failure_detail.is_null()) {
    report["failure"] = std::move(failure_detail);
  }
  report["event_count"] = event_count;
  report["metric_count"] = metric_count;
  report["event_counts"] = EventCountsJson(event_counts);
  report["generated_blocks"] = std::move(generated_blocks);
  report["scheduled_block_count"] = scheduled_block_count;
  report["scheduled_blocks"] = std::move(scheduled_blocks);
  report["scheduled_event_started_count"] = scheduled_event_started_count;
  report["scheduled_event_completed_count"] = scheduled_event_completed_count;
  report["scheduled_event_failed_count"] = scheduled_event_failed_count;
  report["scheduled_events_started"] = std::move(scheduled_events_started);
  report["scheduled_events_completed"] = std::move(scheduled_events_completed);
  report["scheduled_events_failed"] = std::move(scheduled_events_failed);
  report["height_reached"] = std::move(height_reached);
  report["height_waits"] = std::move(height_waits);
  report["peer_waits"] = std::move(peer_waits);
  report["peer_connects"] = std::move(peer_connects);
  report["peer_disconnects"] = std::move(peer_disconnects);
  report["raw_transactions"] = std::move(raw_transactions);
  report["transaction_visibility"] = std::move(transaction_visibility);
  report["transaction_confirmations"] = std::move(transaction_confirmations);
  report["node_restarts"] = std::move(node_restarts);
  report["node_freezes"] = std::move(node_freezes);
  report["resource_updates"] = std::move(resource_updates);
  report["resource_profile_updates"] = std::move(resource_profile_updates);
  report["network_profile_updates"] = std::move(network_profile_updates);
  report["profile_update_rollback_failures"] =
      std::move(profile_update_rollback_failures);
  report["network_condition_updates"] = std::move(network_condition_updates);
  report["network_blocks"] = std::move(network_blocks);
  report["network_unblocks"] = std::move(network_unblocks);
  report["network_partitions"] = std::move(network_partitions);
  report["network_partition_heals"] = std::move(network_partition_heals);
  report["directional_network_policy_verifications"] =
      std::move(directional_network_policy_verifications);
  report["topology_edge_updates"] = std::move(topology_edge_updates);
  report["topology_edge_rollback_failures"] =
      std::move(topology_edge_rollback_failures);
  report["topology_current_edges"] = std::move(topology_current_edges);
  report["wallet_funding"] = std::move(wallet_funding);
  report["wallet_transactions"] = std::move(wallet_transactions);
  report["operator_commands"] = std::move(operator_commands);
  report["wallets_summary"] = WalletsJson(wallets);
  report["nodes_summary"] = NodesJson(nodes);
  return boost::json::serialize(report);
}

}  // namespace bbp
