#include "benchmark_sim/run_report.h"

#include <boost/json/array.hpp>
#include <boost/json/object.hpp>
#include <boost/json/parse.hpp>
#include <boost/json/serialize.hpp>
#include <boost/json/value.hpp>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <map>
#include <optional>
#include <stdexcept>
#include <string>
#include <string_view>

#include "benchmark_sim/util.h"

namespace bsim {
namespace {

struct NodeReport {
  std::uint64_t metric_samples = 0;
  std::string final_state;
  boost::json::object last_metrics;
  boost::json::object log_tails;
};

std::string OptionalStringField(const boost::json::object& object,
                                std::string_view field) {
  const boost::json::value* value = object.if_contains(field);
  if (value == nullptr || !value->is_string()) {
    return "";
  }
  return std::string(value->as_string());
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

void CopySelectedMetricFields(const boost::json::object& source,
                              boost::json::object* target) {
  constexpr std::string_view kFields[] = {
      "height",
      "best_hash",
      "peer_count",
      "mempool_tx_count",
      "mempool_bytes",
      "generated_block_count",
      "restart_count",
      "initial_block_download",
      "difficulty",
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
      "io_read_bytes",
      "io_write_bytes",
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
      "network_has_stats",
      "network_rx_bytes",
      "network_tx_bytes",
      "network_rx_packets",
      "network_tx_packets",
      "network_rx_dropped",
      "network_tx_dropped",
      "network_rx_errors",
      "network_tx_errors",
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
  };
  for (std::string_view field : kFields) {
    CopyField(source, field, target);
  }
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
  CopyField(scenario, "isolated_network", report);
  CopyField(scenario, "sync_timeout_sec", report);
  CopyField(scenario, "workloads", report);
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
  for (const auto& [node_id, node] : nodes) {
    boost::json::object object;
    object["node_id"] = node_id;
    object["metric_samples"] = node.metric_samples;
    if (!node.final_state.empty()) {
      object["final_state"] = node.final_state;
    }
    object["last_metrics"] = node.last_metrics;
    if (!node.log_tails.empty()) {
      object["log_tails"] = node.log_tails;
    }
    array.push_back(std::move(object));
  }
  return array;
}

std::optional<std::string_view> LogTailKind(std::string_view event_name) {
  if (event_name == "stdout_tail") {
    return "stdout";
  }
  if (event_name == "stderr_tail") {
    return "stderr";
  }
  if (event_name == "daemon_log_tail") {
    return "daemon_log";
  }
  return std::nullopt;
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

}  // namespace

std::string BuildRunReportJson(const std::filesystem::path& run_root) {
  if (!std::filesystem::is_directory(run_root)) {
    throw std::runtime_error("run root is not a directory: " +
                             run_root.string());
  }

  boost::json::object report;
  report["run_root"] = std::filesystem::absolute(run_root).string();
  LoadResolvedScenario(run_root / "resolved-scenario.json", &report);

  std::uint64_t event_count = 0;
  std::uint64_t metric_count = 0;
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
  boost::json::array height_reached;
  boost::json::array height_waits;
  boost::json::array peer_waits;
  boost::json::array peer_connects;
  boost::json::array peer_disconnects;
  boost::json::array node_restarts;
  boost::json::array node_freezes;
  boost::json::array resource_updates;
  boost::json::array network_partitions;
  boost::json::array network_partition_heals;

  ForEachJsonLine(
      run_root / "events.jsonl", [&](const boost::json::object& event) {
        ++event_count;
        const std::string event_name = OptionalStringField(event, "event");
        const std::string node_id = OptionalStringField(event, "node_id");
        ++event_counts[event_name];
        if (event_name == "run_started") {
          run_started = true;
          started_at = OptionalStringField(event, "timestamp");
        } else if (event_name == "run_finished") {
          run_finished = true;
          finished_at = OptionalStringField(event, "timestamp");
        } else if (event_name == "run_failed") {
          run_failed = true;
          failed_at = OptionalStringField(event, "timestamp");
          failure_detail = ParseEventDetail(event);
        } else if (event_name == "state" && !node_id.empty()) {
          nodes[node_id].final_state = OptionalStringField(event, "detail");
        } else if (const std::optional<std::string_view> kind =
                       LogTailKind(event_name);
                   kind && !node_id.empty()) {
          nodes[node_id].log_tails[std::string(*kind)] =
              ParseEventDetail(event);
        } else if (event_name == "generated_blocks") {
          AppendEventSummary(event, &generated_blocks);
        } else if (event_name == "height_reached") {
          AppendEventSummary(event, &height_reached);
        } else if (event_name == "height_wait_reached") {
          AppendEventSummary(event, &height_waits);
        } else if (event_name == "peer_count_reached") {
          AppendEventSummary(event, &peer_waits);
        } else if (event_name == "peer_connected") {
          AppendEventSummary(event, &peer_connects);
        } else if (event_name == "peer_disconnected") {
          AppendEventSummary(event, &peer_disconnects);
        } else if (event_name == "node_restarted") {
          AppendEventSummary(event, &node_restarts);
        } else if (event_name == "node_freeze_completed") {
          AppendEventSummary(event, &node_freezes);
        } else if (event_name == "resource_limits_updated") {
          AppendEventSummary(event, &resource_updates);
        } else if (event_name == "network_partition_applied") {
          AppendEventSummary(event, &network_partitions);
        } else if (event_name == "network_partition_healed") {
          AppendEventSummary(event, &network_partition_heals);
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
        ++node.metric_samples;
        node.last_metrics = {};
        CopySelectedMetricFields(metric, &node.last_metrics);
      });

  const bool ok = run_started && run_finished && !run_failed;
  report["ok"] = ok;
  report["status"] = ok ? "finished" : (run_failed ? "failed" : "incomplete");
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
  report["height_reached"] = std::move(height_reached);
  report["height_waits"] = std::move(height_waits);
  report["peer_waits"] = std::move(peer_waits);
  report["peer_connects"] = std::move(peer_connects);
  report["peer_disconnects"] = std::move(peer_disconnects);
  report["node_restarts"] = std::move(node_restarts);
  report["node_freezes"] = std::move(node_freezes);
  report["resource_updates"] = std::move(resource_updates);
  report["network_partitions"] = std::move(network_partitions);
  report["network_partition_heals"] = std::move(network_partition_heals);
  report["nodes_summary"] = NodesJson(nodes);
  return boost::json::serialize(report);
}

}  // namespace bsim
