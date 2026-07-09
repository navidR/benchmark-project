#include "benchmark_sim/run_report.h"

#include "benchmark_sim/util.h"

#include <cstdint>
#include <filesystem>
#include <fstream>
#include <map>
#include <stdexcept>
#include <string>
#include <string_view>

#include <boost/json/array.hpp>
#include <boost/json/object.hpp>
#include <boost/json/parse.hpp>
#include <boost/json/serialize.hpp>
#include <boost/json/value.hpp>

namespace bsim {
namespace {

struct NodeReport {
  std::uint64_t metric_samples = 0;
  std::string final_state;
  boost::json::object last_metrics;
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
      "rpc_latency_ms",
      "chain_version",
      "chain_protocol_version",
      "chain_subversion",
      "qdisc_kind",
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
    throw std::runtime_error(path.string() + ":" +
                             std::to_string(line_number) +
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
    array.push_back(std::move(object));
  }
  return array;
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
  std::map<std::string, std::uint64_t> event_counts;
  std::map<std::string, NodeReport> nodes;

  ForEachJsonLine(run_root / "events.jsonl",
                  [&](const boost::json::object& event) {
                    ++event_count;
                    const std::string event_name =
                        OptionalStringField(event, "event");
                    const std::string node_id =
                        OptionalStringField(event, "node_id");
                    ++event_counts[event_name];
                    if (event_name == "run_started") {
                      run_started = true;
                    } else if (event_name == "run_finished") {
                      run_finished = true;
                    } else if (event_name == "run_failed") {
                      run_failed = true;
                    } else if (event_name == "state" && !node_id.empty()) {
                      nodes[node_id].final_state =
                          OptionalStringField(event, "detail");
                    }
                  });

  ForEachJsonLine(run_root / "metrics.jsonl",
                  [&](const boost::json::object& metric) {
                    ++metric_count;
                    const std::string node_id =
                        OptionalStringField(metric, "node_id");
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
  report["status"] =
      ok ? "finished" : (run_failed ? "failed" : "incomplete");
  report["event_count"] = event_count;
  report["metric_count"] = metric_count;
  report["event_counts"] = EventCountsJson(event_counts);
  report["nodes_summary"] = NodesJson(nodes);
  return boost::json::serialize(report);
}

}  // namespace bsim
