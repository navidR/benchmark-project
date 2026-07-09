#include "benchmark_sim/run_report.h"

#include "benchmark_sim/util.h"

#include <unistd.h>

#include <cstdint>
#include <filesystem>
#include <stdexcept>
#include <string>
#include <string_view>

#include <boost/json/array.hpp>
#include <boost/json/object.hpp>
#include <boost/json/parse.hpp>
#include <boost/test/unit_test.hpp>

namespace {

std::filesystem::path MakeTestDir(const std::string& name) {
  const std::filesystem::path dir =
      std::filesystem::temp_directory_path() /
      ("benchmark-sim-" + name + "-" + std::to_string(getpid()));
  std::filesystem::remove_all(dir);
  std::filesystem::create_directories(dir);
  return dir;
}

std::uint64_t JsonInteger(const boost::json::object& object,
                          std::string_view field) {
  const boost::json::value& value = object.at(field);
  if (value.is_uint64()) {
    return value.as_uint64();
  }
  if (value.is_int64() && value.as_int64() >= 0) {
    return static_cast<std::uint64_t>(value.as_int64());
  }
  throw std::runtime_error("expected non-negative JSON integer");
}

}  // namespace

BOOST_AUTO_TEST_CASE(run_report_summarizes_events_and_last_metrics) {
  const std::filesystem::path dir = MakeTestDir("run-report");
  bsim::WriteText(dir / "resolved-scenario.json",
                  "{\"run_id\":\"r1\",\"chain\":\"firo\",\"nodes\":1,"
                  "\"generate_blocks\":3,\"generate_node\":null,"
                  "\"isolated_network\":true,\"sync_timeout_sec\":null,"
                  "\"resources\":{\"memory_max_bytes\":1024},"
                  "\"default_network_condition\":{\"delay_ms\":2},"
                  "\"node_network_conditions\":[{\"node\":1,\"delay_ms\":3}],"
                  "\"runtime_node_resource_limits\":["
                  "{\"node\":1,\"pids_max\":128}],"
                  "\"runtime_node_restarts\":[{\"node\":1}],"
                  "\"workloads\":["
                  "{\"type\":\"block_generation\",\"node\":1,\"count\":1,"
                  "\"sync_timeout_sec\":45},"
                  "{\"type\":\"block_generation\",\"node\":2,\"count\":2,"
                  "\"sync_timeout_sec\":60}]}\n");
  bsim::AppendLine(
      dir / "events.jsonl",
      "{\"run_id\":\"r1\",\"node_id\":\"sim\",\"event\":\"run_started\"}");
  bsim::AppendLine(
      dir / "events.jsonl",
      "{\"run_id\":\"r1\",\"node_id\":\"firo-1\",\"event\":\"state\","
      "\"detail\":\"Running\"}");
  bsim::AppendLine(
      dir / "events.jsonl",
      "{\"run_id\":\"r1\",\"node_id\":\"firo-1\","
      "\"event\":\"daemon_log_tail\","
      "\"detail\":\"{\\\"kind\\\":\\\"daemon_log\\\","
      "\\\"start_offset\\\":0,\\\"next_offset\\\":4,"
      "\\\"truncated\\\":false,\\\"offset_reset\\\":false,"
      "\\\"text\\\":\\\"tail\\\"}\"}");
  bsim::AppendLine(
      dir / "events.jsonl",
      "{\"run_id\":\"r1\",\"node_id\":\"firo-1\","
      "\"event\":\"generated_blocks\","
      "\"detail\":\"{\\\"workload_index\\\":1,"
      "\\\"workload_count\\\":2,\\\"generator_node\\\":1,"
      "\\\"count\\\":1,\\\"start_height\\\":0,"
      "\\\"target_height\\\":1,\\\"reward_address\\\":\\\"a\\\","
      "\\\"hashes\\\":[\\\"abc\\\"]}\"}");
  bsim::AppendLine(
      dir / "events.jsonl",
      "{\"run_id\":\"r1\",\"node_id\":\"firo-1\","
      "\"event\":\"height_wait_reached\","
      "\"detail\":\"{\\\"workload_index\\\":2,"
      "\\\"workload_count\\\":2,\\\"node\\\":1,"
      "\\\"target_height\\\":2,\\\"observed_height\\\":2}\"}");
  bsim::AppendLine(
      dir / "events.jsonl",
      "{\"run_id\":\"r1\",\"node_id\":\"sim\",\"event\":\"run_finished\"}");
  bsim::AppendLine(
      dir / "metrics.jsonl",
      "{\"run_id\":\"r1\",\"node_id\":\"firo-1\",\"height\":1,"
      "\"generated_block_count\":0,\"qdisc_kind\":\"netem\","
      "\"qdisc_has_netem_options\":true,\"qdisc_netem_latency_us\":1000,"
      "\"qdisc_netem_reorder\":0}");
  bsim::AppendLine(
      dir / "metrics.jsonl",
      "{\"run_id\":\"r1\",\"node_id\":\"firo-1\",\"height\":2,"
      "\"initial_block_download\":false,\"difficulty\":1.25,"
      "\"mempool_tx_count\":3,\"mempool_bytes\":450,"
      "\"generated_block_count\":1,\"qdisc_kind\":\"tbf+netem\","
      "\"cpu_usage_usec\":10,\"memory_current\":20,"
      "\"memory_high_limit_bytes\":null,\"io_read_bytes\":30,"
      "\"pids_current\":2,\"oom_kill\":0,"
      "\"network_rx_bytes\":100,\"network_tx_bytes\":200,"
      "\"qdisc_bytes\":300,\"qdisc_drops\":1,"
      "\"qdisc_has_netem_options\":true,\"qdisc_netem_latency_us\":2000,"
      "\"qdisc_netem_jitter_us\":500,\"qdisc_netem_reorder\":429496,"
      "\"qdisc_netem_limit_packets\":1000,\"qdisc_has_tbf_options\":true,"
      "\"qdisc_tbf_rate_bytes_per_sec\":1250000,"
      "\"qdisc_tbf_limit_bytes\":125000}");

  const boost::json::value value =
      boost::json::parse(bsim::BuildRunReportJson(dir));
  const boost::json::object& report = value.as_object();

  BOOST_TEST(report.at("ok").as_bool());
  BOOST_TEST(report.at("status").as_string() == "finished");
  BOOST_TEST(JsonInteger(report, "event_count") == 6U);
  BOOST_TEST(JsonInteger(report, "metric_count") == 2U);
  BOOST_TEST(JsonInteger(report, "generate_blocks") == 3U);
  BOOST_TEST(report.at("generate_node").is_null());
  BOOST_TEST(report.at("sync_timeout_sec").is_null());
  const boost::json::array& workloads = report.at("workloads").as_array();
  BOOST_REQUIRE_EQUAL(workloads.size(), 2U);
  const boost::json::object& first_workload =
      workloads.front().as_object();
  BOOST_TEST(first_workload.at("type").as_string() == "block_generation");
  BOOST_TEST(JsonInteger(first_workload, "node") == 1U);
  BOOST_TEST(JsonInteger(first_workload, "count") == 1U);
  BOOST_TEST(JsonInteger(first_workload, "sync_timeout_sec") == 45U);
  BOOST_TEST(JsonInteger(report.at("resources").as_object(),
                         "memory_max_bytes") == 1024U);
  BOOST_TEST(JsonInteger(report.at("default_network_condition").as_object(),
                         "delay_ms") == 2U);
  BOOST_REQUIRE_EQUAL(report.at("node_network_conditions").as_array().size(),
                      1U);
  BOOST_REQUIRE_EQUAL(
      report.at("runtime_node_resource_limits").as_array().size(), 1U);
  BOOST_REQUIRE_EQUAL(report.at("runtime_node_restarts").as_array().size(),
                      1U);
  const boost::json::array& generated_blocks =
      report.at("generated_blocks").as_array();
  BOOST_REQUIRE_EQUAL(generated_blocks.size(), 1U);
  const boost::json::object& generated_block_event =
      generated_blocks.front().as_object();
  BOOST_TEST(generated_block_event.at("node_id").as_string() == "firo-1");
  const boost::json::object& generated_block_detail =
      generated_block_event.at("detail").as_object();
  BOOST_TEST(JsonInteger(generated_block_detail, "workload_index") == 1U);
  BOOST_TEST(JsonInteger(generated_block_detail, "workload_count") == 2U);
  BOOST_TEST(JsonInteger(generated_block_detail, "generator_node") == 1U);
  BOOST_TEST(JsonInteger(generated_block_detail, "target_height") == 1U);
  BOOST_REQUIRE_EQUAL(generated_block_detail.at("hashes").as_array().size(),
                      1U);
  const boost::json::array& height_waits =
      report.at("height_waits").as_array();
  BOOST_REQUIRE_EQUAL(height_waits.size(), 1U);
  const boost::json::object& height_wait =
      height_waits.front().as_object().at("detail").as_object();
  BOOST_TEST(JsonInteger(height_wait, "workload_index") == 2U);
  BOOST_TEST(JsonInteger(height_wait, "target_height") == 2U);
  BOOST_TEST(JsonInteger(height_wait, "observed_height") == 2U);
  const boost::json::array& nodes =
      report.at("nodes_summary").as_array();
  BOOST_REQUIRE_EQUAL(nodes.size(), 1U);
  const boost::json::object& node = nodes.front().as_object();
  BOOST_TEST(node.at("node_id").as_string() == "firo-1");
  BOOST_TEST(JsonInteger(node, "metric_samples") == 2U);
  BOOST_TEST(node.at("final_state").as_string() == "Running");
  const boost::json::object& last_metrics =
      node.at("last_metrics").as_object();
  BOOST_TEST(JsonInteger(last_metrics, "height") == 2U);
  BOOST_TEST(JsonInteger(last_metrics, "mempool_tx_count") == 3U);
  BOOST_TEST(JsonInteger(last_metrics, "mempool_bytes") == 450U);
  BOOST_TEST(JsonInteger(last_metrics, "generated_block_count") == 1U);
  BOOST_TEST(!last_metrics.at("initial_block_download").as_bool());
  BOOST_TEST(JsonInteger(last_metrics, "cpu_usage_usec") == 10U);
  BOOST_TEST(JsonInteger(last_metrics, "memory_current") == 20U);
  BOOST_TEST(last_metrics.at("memory_high_limit_bytes").is_null());
  BOOST_TEST(JsonInteger(last_metrics, "io_read_bytes") == 30U);
  BOOST_TEST(JsonInteger(last_metrics, "pids_current") == 2U);
  BOOST_TEST(JsonInteger(last_metrics, "oom_kill") == 0U);
  BOOST_TEST(JsonInteger(last_metrics, "network_rx_bytes") == 100U);
  BOOST_TEST(JsonInteger(last_metrics, "network_tx_bytes") == 200U);
  BOOST_TEST(last_metrics.at("qdisc_kind").as_string() == "tbf+netem");
  BOOST_TEST(JsonInteger(last_metrics, "qdisc_bytes") == 300U);
  BOOST_TEST(JsonInteger(last_metrics, "qdisc_drops") == 1U);
  BOOST_TEST(last_metrics.at("qdisc_has_netem_options").as_bool());
  BOOST_TEST(JsonInteger(last_metrics, "qdisc_netem_latency_us") == 2000U);
  BOOST_TEST(JsonInteger(last_metrics, "qdisc_netem_jitter_us") == 500U);
  BOOST_TEST(JsonInteger(last_metrics, "qdisc_netem_reorder") == 429496U);
  BOOST_TEST(JsonInteger(last_metrics, "qdisc_netem_limit_packets") == 1000U);
  BOOST_TEST(last_metrics.at("qdisc_has_tbf_options").as_bool());
  BOOST_TEST(JsonInteger(last_metrics, "qdisc_tbf_rate_bytes_per_sec") ==
             1250000U);
  const boost::json::object& log_tails = node.at("log_tails").as_object();
  const boost::json::object& daemon_log_tail =
      log_tails.at("daemon_log").as_object();
  BOOST_TEST(daemon_log_tail.at("text").as_string() == "tail");

  std::filesystem::remove_all(dir);
}
