#include <unistd.h>

#include <boost/json/array.hpp>
#include <boost/json/object.hpp>
#include <boost/json/parse.hpp>
#include <boost/json/serialize.hpp>
#include <boost/test/unit_test.hpp>
#include <cstdint>
#include <filesystem>
#include <stdexcept>
#include <string>
#include <string_view>

#include "bbp/run_report.h"
#include "bbp/util.h"

namespace {

std::filesystem::path MakeTestDir(const std::string& name) {
  const std::filesystem::path dir =
      std::filesystem::temp_directory_path() /
      ("bbp-" + name + "-" + std::to_string(getpid()));
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

std::uint64_t JsonIntegerValue(const boost::json::value& value) {
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
  bbp::WriteText(dir / "resolved-scenario.json",
                 "{\"run_id\":\"r1\",\"chain\":\"firo\",\"nodes\":1,"
                 "\"generate_blocks\":3,\"generate_node\":null,"
                 "\"isolated_network\":true,\"sync_timeout_sec\":null,"
                 "\"topology\":{\"node_count\":1,"
                 "\"wallet_node_count\":1,\"miner_node_count\":1,"
                 "\"allow_miner_wallet_overlap\":true,"
                 "\"wallet_nodes\":[1],\"miner_nodes\":[1]},"
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
                 "\"sync_timeout_sec\":60},"
                 "{\"type\":\"wait_for_peers\",\"node\":1,"
                 "\"peer_count\":1,\"timeout_sec\":30},"
                 "{\"type\":\"restart_node\",\"node\":1},"
                 "{\"type\":\"freeze_node\",\"node\":1,"
                 "\"duration_ms\":25},"
                 "{\"type\":\"update_resource_limits\",\"node\":1,"
                 "\"pids_max\":128}]}\n");
  bbp::AppendLine(dir / "events.jsonl",
                  "{\"run_id\":\"r1\",\"node_id\":\"sim\","
                  "\"timestamp\":\"2026-07-09T00:00:00Z\","
                  "\"event\":\"run_started\"}");
  bbp::AppendLine(
      dir / "events.jsonl",
      "{\"run_id\":\"r1\",\"node_id\":\"firo-1\",\"event\":\"state\","
      "\"detail\":\"Running\"}");
  bbp::AppendLine(dir / "events.jsonl",
                  "{\"run_id\":\"r1\",\"node_id\":\"firo-1\","
                  "\"event\":\"daemon_log_tail\","
                  "\"detail\":\"{\\\"kind\\\":\\\"daemon_log\\\","
                  "\\\"start_offset\\\":0,\\\"next_offset\\\":4,"
                  "\\\"truncated\\\":false,\\\"offset_reset\\\":false,"
                  "\\\"text\\\":\\\"tail\\\"}\"}");
  bbp::AppendLine(dir / "events.jsonl",
                  "{\"run_id\":\"r1\",\"node_id\":\"firo-1\","
                  "\"event\":\"daemon_log_tail\","
                  "\"detail\":\"{\\\"kind\\\":\\\"daemon_log\\\","
                  "\\\"start_offset\\\":4,\\\"next_offset\\\":9,"
                  "\\\"truncated\\\":false,\\\"offset_reset\\\":false,"
                  "\\\"text\\\":\\\" next\\\"}\"}");
  bbp::AppendLine(dir / "events.jsonl",
                  "{\"run_id\":\"r1\",\"node_id\":\"firo-1\","
                  "\"timestamp\":\"2026-07-09T00:00:01Z\","
                  "\"event\":\"generated_blocks\","
                  "\"detail\":\"{\\\"workload_index\\\":1,"
                  "\\\"workload_count\\\":6,\\\"generator_node\\\":1,"
                  "\\\"count\\\":1,\\\"start_height\\\":0,"
                  "\\\"target_height\\\":1,\\\"reward_address\\\":\\\"a\\\","
                  "\\\"hashes\\\":[\\\"abc\\\"]}\"}");
  bbp::AppendLine(dir / "events.jsonl",
                  "{\"run_id\":\"r1\",\"node_id\":\"firo-1\","
                  "\"event\":\"height_reached\",\"detail\":\"1\"}");
  bbp::AppendLine(dir / "events.jsonl",
                  "{\"run_id\":\"r1\",\"node_id\":\"firo-1\","
                  "\"timestamp\":\"2026-07-09T00:00:02Z\","
                  "\"event\":\"scheduled_block_produced\","
                  "\"detail\":\"{\\\"hashes\\\":[\\\"def\\\"]}\"}");
  bbp::AppendLine(dir / "events.jsonl",
                  "{\"run_id\":\"r1\",\"node_id\":\"firo-1\","
                  "\"event\":\"height_wait_reached\","
                  "\"detail\":\"{\\\"workload_index\\\":2,"
                  "\\\"workload_count\\\":6,\\\"node\\\":1,"
                  "\\\"target_height\\\":2,\\\"observed_height\\\":2}\"}");
  bbp::AppendLine(
      dir / "events.jsonl",
      "{\"run_id\":\"r1\",\"node_id\":\"firo-1\","
      "\"event\":\"peer_count_reached\","
      "\"detail\":\"{\\\"workload_index\\\":3,"
      "\\\"workload_count\\\":6,\\\"node\\\":1,"
      "\\\"target_peer_count\\\":1,\\\"observed_peer_count\\\":1}\"}");
  bbp::AppendLine(dir / "events.jsonl",
                  "{\"run_id\":\"r1\",\"node_id\":\"firo-1\","
                  "\"event\":\"node_restarted\","
                  "\"detail\":\"{\\\"workload_index\\\":4,"
                  "\\\"workload_count\\\":6,\\\"node\\\":1,"
                  "\\\"restart_count\\\":1}\"}");
  bbp::AppendLine(dir / "events.jsonl",
                  "{\"run_id\":\"r1\",\"node_id\":\"firo-1\","
                  "\"event\":\"node_freeze_completed\","
                  "\"detail\":\"{\\\"workload_index\\\":5,"
                  "\\\"workload_count\\\":6,\\\"node\\\":1,"
                  "\\\"duration_ms\\\":25}\"}");
  bbp::AppendLine(dir / "events.jsonl",
                  "{\"run_id\":\"r1\",\"node_id\":\"firo-1\","
                  "\"event\":\"resource_limits_updated\","
                  "\"detail\":\"{\\\"workload_index\\\":6,"
                  "\\\"workload_count\\\":6,\\\"node\\\":1,"
                  "\\\"requested\\\":{\\\"pids_max\\\":128},"
                  "\\\"previous\\\":{\\\"memory_high_bytes\\\":1024,"
                  "\\\"memory_max_bytes\\\":2048,"
                  "\\\"cpu_quota_us\\\":null,"
                  "\\\"cpu_period_us\\\":100000,"
                  "\\\"pids_max\\\":256},"
                  "\\\"current\\\":{\\\"memory_high_bytes\\\":1024,"
                  "\\\"memory_max_bytes\\\":2048,"
                  "\\\"cpu_quota_us\\\":null,"
                  "\\\"cpu_period_us\\\":100000,"
                  "\\\"pids_max\\\":128}}\"}");
  bbp::AppendLine(dir / "events.jsonl",
                  "{\"run_id\":\"r1\",\"node_id\":\"firo-1\","
                  "\"event\":\"wallet_address_created\","
                  "\"detail\":\"{\\\"wallet_index\\\":1,"
                  "\\\"node\\\":1,\\\"strategy\\\":\\\"driver_rpc\\\","
                  "\\\"mode\\\":\\\"public\\\","
                  "\\\"address\\\":\\\"addr1\\\"}\"}");
  bbp::AppendLine(dir / "events.jsonl",
                  "{\"run_id\":\"r1\",\"node_id\":\"firo-2\","
                  "\"event\":\"wallet_address_created\","
                  "\"detail\":\"{\\\"wallet_index\\\":2,"
                  "\\\"node\\\":2,\\\"strategy\\\":\\\"driver_rpc\\\","
                  "\\\"mode\\\":\\\"public\\\","
                  "\\\"address\\\":\\\"addr2\\\"}\"}");
  bbp::AppendLine(dir / "events.jsonl",
                  "{\"run_id\":\"r1\",\"node_id\":\"firo-1\","
                  "\"event\":\"wallet_funded\","
                  "\"detail\":\"{\\\"wallet_index\\\":1,"
                  "\\\"node\\\":1,\\\"address\\\":\\\"addr1\\\","
                  "\\\"miner_node\\\":1,"
                  "\\\"funding_strategy\\\":\\\"round_robin\\\","
                  "\\\"funding_threshold_satoshis\\\":100001000,"
                  "\\\"ready_balance_satoshis\\\":5000000000}\"}");
  bbp::AppendLine(dir / "events.jsonl",
                  "{\"run_id\":\"r1\",\"node_id\":\"firo-1\","
                  "\"event\":\"wallet_transaction_submitted\","
                  "\"detail\":\"{\\\"workload_index\\\":7,"
                  "\\\"workload_count\\\":7,"
                  "\\\"transaction_index\\\":1,"
                  "\\\"transaction_count\\\":1,"
                  "\\\"strategy\\\":\\\"random\\\","
                  "\\\"seed\\\":42,"
                  "\\\"sender_wallet_index\\\":1,"
                  "\\\"receiver_wallet_index\\\":2,"
                  "\\\"sender_node\\\":1,"
                  "\\\"receiver_node\\\":2,"
                  "\\\"sender_address\\\":\\\"addr1\\\","
                  "\\\"receiver_address\\\":\\\"addr2\\\","
                  "\\\"funding_miner_node\\\":1,"
                  "\\\"amount\\\":\\\"1.00000000\\\","
                  "\\\"amount_satoshis\\\":100000000,"
                  "\\\"txids\\\":[\\\"tx1\\\"],"
                  "\\\"mempool_size\\\":1}\"}");
  bbp::AppendLine(
      dir / "events.jsonl",
      "{\"run_id\":\"r1\",\"node_id\":\"firo-1\","
      "\"timestamp\":\"2026-07-09T00:00:01Z\","
      "\"event\":\"operator_command_failed\","
      "\"detail\":\"{\\\"sequence\\\":7,"
      "\\\"kind\\\":\\\"increase_log_verbosity\\\","
      "\\\"error\\\":\\\"Firo does not support runtime log verbosity "
      "adjustment functionality.\\\"}\"}");
  bbp::AppendLine(dir / "events.jsonl",
                  "{\"run_id\":\"r1\",\"node_id\":\"sim\","
                  "\"timestamp\":\"2026-07-09T00:00:02Z\","
                  "\"event\":\"run_finished\"}");
  bbp::AppendLine(
      dir / "metrics.jsonl",
      "{\"run_id\":\"r1\",\"node_id\":\"firo-1\",\"height\":1,"
      "\"timestamp_ms\":1000,"
      "\"generated_block_count\":0,\"qdisc_kind\":\"netem\","
      "\"network_rx_bytes\":25,\"network_tx_bytes\":50,"
      "\"qdisc_has_netem_options\":true,\"qdisc_netem_latency_us\":1000,"
      "\"qdisc_netem_reorder\":0}");
  bbp::AppendLine(
      dir / "metrics.jsonl",
      "{\"run_id\":\"r1\",\"node_id\":\"firo-1\",\"height\":2,"
      "\"timestamp_ms\":3000,"
      "\"initial_block_download\":false,\"difficulty\":1.25,"
      "\"mempool_tx_count\":3,\"mempool_bytes\":450,"
      "\"generated_block_count\":1,\"mined_transaction_count\":2,"
      "\"mined_transaction_count_complete\":true,"
      "\"qdisc_kind\":\"tbf+netem\","
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
  bbp::AppendLine(dir / "wallet-metrics.jsonl",
                  "{\"run_id\":\"r1\",\"timestamp_ms\":3000,\"wallet_index\":1,"
                  "\"node\":1,\"mode\":\"public\","
                  "\"available_balance_satoshis\":4900000000,"
                  "\"unconfirmed_balance_satoshis\":100000000,"
                  "\"immature_balance_satoshis\":0,\"transaction_count\":2,"
                  "\"transaction_history_truncated\":false,"
                  "\"transactions\":[{\"direction\":\"outgoing\","
                  "\"amount_satoshis\":-100000000,\"confirmations\":0,"
                  "\"timestamp\":2,\"txid\":\"tx1\"}]}");

  const boost::json::value value =
      boost::json::parse(bbp::BuildRunReportJson(dir));
  const boost::json::object& report = value.as_object();

  BOOST_TEST(report.at("ok").as_bool());
  BOOST_TEST(report.at("status").as_string() == "finished");
  BOOST_TEST(report.at("started_at").as_string() == "2026-07-09T00:00:00Z");
  BOOST_TEST(report.at("finished_at").as_string() == "2026-07-09T00:00:02Z");
  BOOST_TEST(JsonInteger(report, "event_count") == 18U);
  BOOST_TEST(JsonInteger(report, "metric_count") == 2U);
  BOOST_TEST(JsonInteger(report, "generate_blocks") == 3U);
  BOOST_TEST(report.at("generate_node").is_null());
  BOOST_TEST(report.at("sync_timeout_sec").is_null());
  const boost::json::object& topology = report.at("topology").as_object();
  BOOST_TEST(JsonInteger(topology, "node_count") == 1U);
  BOOST_TEST(JsonInteger(topology, "wallet_node_count") == 1U);
  BOOST_TEST(JsonInteger(topology, "miner_node_count") == 1U);
  BOOST_TEST(topology.at("allow_miner_wallet_overlap").as_bool());
  BOOST_REQUIRE_EQUAL(topology.at("wallet_nodes").as_array().size(), 1U);
  BOOST_TEST(JsonIntegerValue(topology.at("wallet_nodes").as_array().front()) ==
             1U);
  BOOST_REQUIRE_EQUAL(topology.at("miner_nodes").as_array().size(), 1U);
  BOOST_TEST(JsonIntegerValue(topology.at("miner_nodes").as_array().front()) ==
             1U);
  const boost::json::array& workloads = report.at("workloads").as_array();
  BOOST_REQUIRE_EQUAL(workloads.size(), 6U);
  const boost::json::object& first_workload = workloads.front().as_object();
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
  BOOST_REQUIRE_EQUAL(report.at("runtime_node_restarts").as_array().size(), 1U);
  const boost::json::array& operator_commands =
      report.at("operator_commands").as_array();
  BOOST_REQUIRE_EQUAL(operator_commands.size(), 1U);
  const boost::json::object& operator_command =
      operator_commands.front().as_object();
  BOOST_TEST(operator_command.at("status").as_string() == "failed");
  BOOST_TEST(operator_command.at("node_id").as_string() == "firo-1");
  const boost::json::object& operator_detail =
      operator_command.at("detail").as_object();
  BOOST_TEST(JsonInteger(operator_detail, "sequence") == 7U);
  BOOST_TEST(operator_detail.at("error").as_string() ==
             "Firo does not support runtime log verbosity adjustment "
             "functionality.");
  const boost::json::array& generated_blocks =
      report.at("generated_blocks").as_array();
  BOOST_REQUIRE_EQUAL(generated_blocks.size(), 1U);
  const boost::json::object& generated_block_event =
      generated_blocks.front().as_object();
  BOOST_TEST(generated_block_event.at("node_id").as_string() == "firo-1");
  BOOST_TEST(generated_block_event.at("timestamp").as_string() ==
             "2026-07-09T00:00:01Z");
  const boost::json::object& generated_block_detail =
      generated_block_event.at("detail").as_object();
  BOOST_TEST(JsonInteger(generated_block_detail, "workload_index") == 1U);
  BOOST_TEST(JsonInteger(generated_block_detail, "workload_count") == 6U);
  BOOST_TEST(JsonInteger(generated_block_detail, "generator_node") == 1U);
  BOOST_TEST(JsonInteger(generated_block_detail, "target_height") == 1U);
  BOOST_REQUIRE_EQUAL(generated_block_detail.at("hashes").as_array().size(),
                      1U);
  const boost::json::array& scheduled_blocks =
      report.at("scheduled_blocks").as_array();
  BOOST_REQUIRE_EQUAL(scheduled_blocks.size(), 1U);
  BOOST_TEST(scheduled_blocks.front()
                 .as_object()
                 .at("detail")
                 .as_object()
                 .at("hashes")
                 .as_array()
                 .front()
                 .as_string() == "def");
  const boost::json::array& height_reached =
      report.at("height_reached").as_array();
  BOOST_REQUIRE_EQUAL(height_reached.size(), 1U);
  const boost::json::object& reached_event = height_reached.front().as_object();
  BOOST_TEST(reached_event.at("node_id").as_string() == "firo-1");
  BOOST_TEST(reached_event.at("detail").as_int64() == 1);
  const boost::json::array& height_waits = report.at("height_waits").as_array();
  BOOST_REQUIRE_EQUAL(height_waits.size(), 1U);
  const boost::json::object& height_wait =
      height_waits.front().as_object().at("detail").as_object();
  BOOST_TEST(JsonInteger(height_wait, "workload_index") == 2U);
  BOOST_TEST(JsonInteger(height_wait, "target_height") == 2U);
  BOOST_TEST(JsonInteger(height_wait, "observed_height") == 2U);
  const boost::json::array& peer_waits = report.at("peer_waits").as_array();
  BOOST_REQUIRE_EQUAL(peer_waits.size(), 1U);
  const boost::json::object& peer_wait =
      peer_waits.front().as_object().at("detail").as_object();
  BOOST_TEST(JsonInteger(peer_wait, "workload_index") == 3U);
  BOOST_TEST(JsonInteger(peer_wait, "target_peer_count") == 1U);
  BOOST_TEST(JsonInteger(peer_wait, "observed_peer_count") == 1U);
  const boost::json::array& node_restarts =
      report.at("node_restarts").as_array();
  BOOST_REQUIRE_EQUAL(node_restarts.size(), 1U);
  const boost::json::object& node_restart =
      node_restarts.front().as_object().at("detail").as_object();
  BOOST_TEST(JsonInteger(node_restart, "workload_index") == 4U);
  BOOST_TEST(JsonInteger(node_restart, "node") == 1U);
  BOOST_TEST(JsonInteger(node_restart, "restart_count") == 1U);
  const boost::json::array& node_freezes = report.at("node_freezes").as_array();
  BOOST_REQUIRE_EQUAL(node_freezes.size(), 1U);
  const boost::json::object& node_freeze =
      node_freezes.front().as_object().at("detail").as_object();
  BOOST_TEST(JsonInteger(node_freeze, "workload_index") == 5U);
  BOOST_TEST(JsonInteger(node_freeze, "node") == 1U);
  BOOST_TEST(JsonInteger(node_freeze, "duration_ms") == 25U);
  const boost::json::array& resource_updates =
      report.at("resource_updates").as_array();
  BOOST_REQUIRE_EQUAL(resource_updates.size(), 1U);
  const boost::json::object& resource_update =
      resource_updates.front().as_object().at("detail").as_object();
  BOOST_TEST(JsonInteger(resource_update, "workload_index") == 6U);
  BOOST_TEST(JsonInteger(resource_update, "node") == 1U);
  BOOST_TEST(JsonInteger(resource_update.at("requested").as_object(),
                         "pids_max") == 128U);
  const boost::json::array& wallet_funding =
      report.at("wallet_funding").as_array();
  BOOST_REQUIRE_EQUAL(wallet_funding.size(), 1U);
  const boost::json::object& wallet_funding_detail =
      wallet_funding.front().as_object().at("detail").as_object();
  BOOST_TEST(JsonInteger(wallet_funding_detail, "wallet_index") == 1U);
  BOOST_TEST(JsonInteger(wallet_funding_detail, "miner_node") == 1U);
  BOOST_TEST(wallet_funding_detail.at("funding_strategy").as_string() ==
             "round_robin");
  const boost::json::array& wallet_transactions =
      report.at("wallet_transactions").as_array();
  BOOST_REQUIRE_EQUAL(wallet_transactions.size(), 1U);
  const boost::json::object& wallet_transaction =
      wallet_transactions.front().as_object().at("detail").as_object();
  BOOST_TEST(JsonInteger(wallet_transaction, "sender_wallet_index") == 1U);
  BOOST_TEST(JsonInteger(wallet_transaction, "receiver_wallet_index") == 2U);
  BOOST_TEST(wallet_transaction.at("strategy").as_string() == "random");
  BOOST_TEST(JsonInteger(wallet_transaction, "seed") == 42U);
  const boost::json::array& wallets = report.at("wallets_summary").as_array();
  BOOST_REQUIRE_EQUAL(wallets.size(), 2U);
  const boost::json::object& sender_wallet = wallets.front().as_object();
  BOOST_TEST(JsonInteger(sender_wallet, "wallet_index") == 1U);
  BOOST_TEST(JsonInteger(sender_wallet, "node") == 1U);
  BOOST_TEST(sender_wallet.at("address").as_string() == "addr1");
  BOOST_TEST(sender_wallet.at("strategy").as_string() == "driver_rpc");
  BOOST_TEST(sender_wallet.at("mode").as_string() == "public");
  BOOST_TEST(JsonInteger(sender_wallet, "transactions_sent") == 1U);
  BOOST_TEST(JsonInteger(sender_wallet, "transactions_received") == 0U);
  BOOST_TEST(JsonInteger(sender_wallet, "simulated_amount_sent_satoshis") ==
             100000000U);
  const boost::json::object& last_funding =
      sender_wallet.at("last_funding").as_object();
  BOOST_TEST(JsonInteger(last_funding, "miner_node") == 1U);
  BOOST_TEST(JsonInteger(last_funding, "ready_balance_satoshis") ==
             5000000000U);
  const boost::json::object& wallet_metrics =
      sender_wallet.at("last_metrics").as_object();
  BOOST_TEST(JsonInteger(wallet_metrics, "available_balance_satoshis") ==
             4900000000U);
  BOOST_TEST(JsonInteger(wallet_metrics, "transaction_count") == 2U);
  BOOST_REQUIRE_EQUAL(wallet_metrics.at("transactions").as_array().size(), 1U);
  BOOST_TEST(JsonInteger(sender_wallet.at("last_sent_transaction").as_object(),
                         "receiver_wallet_index") == 2U);
  const boost::json::object& receiver_wallet = wallets[1].as_object();
  BOOST_TEST(JsonInteger(receiver_wallet, "wallet_index") == 2U);
  BOOST_TEST(JsonInteger(receiver_wallet, "node") == 2U);
  BOOST_TEST(receiver_wallet.at("address").as_string() == "addr2");
  BOOST_TEST(JsonInteger(receiver_wallet, "transactions_sent") == 0U);
  BOOST_TEST(JsonInteger(receiver_wallet, "transactions_received") == 1U);
  BOOST_TEST(JsonInteger(receiver_wallet,
                         "simulated_amount_received_satoshis") == 100000000U);
  const boost::json::array& nodes = report.at("nodes_summary").as_array();
  BOOST_REQUIRE_EQUAL(nodes.size(), 1U);
  const boost::json::object& node = nodes.front().as_object();
  BOOST_TEST(node.at("node_id").as_string() == "firo-1");
  BOOST_TEST(JsonInteger(node, "metric_samples") == 2U);
  BOOST_TEST(node.at("final_state").as_string() == "Running");
  const boost::json::object& last_metrics = node.at("last_metrics").as_object();
  BOOST_TEST(JsonInteger(last_metrics, "height") == 2U);
  BOOST_TEST(JsonInteger(last_metrics, "mempool_tx_count") == 3U);
  BOOST_TEST(JsonInteger(last_metrics, "mempool_bytes") == 450U);
  BOOST_TEST(JsonInteger(last_metrics, "generated_block_count") == 1U);
  BOOST_TEST(JsonInteger(last_metrics, "mined_transaction_count") == 2U);
  BOOST_TEST(last_metrics.at("mined_transaction_count_complete").as_bool());
  BOOST_TEST(!last_metrics.at("initial_block_download").as_bool());
  BOOST_TEST(JsonInteger(last_metrics, "cpu_usage_usec") == 10U);
  BOOST_TEST(JsonInteger(last_metrics, "memory_current") == 20U);
  BOOST_TEST(last_metrics.at("memory_high_limit_bytes").is_null());
  BOOST_TEST(JsonInteger(last_metrics, "io_read_bytes") == 30U);
  BOOST_TEST(JsonInteger(last_metrics, "pids_current") == 2U);
  BOOST_TEST(JsonInteger(last_metrics, "oom_kill") == 0U);
  BOOST_TEST(JsonInteger(last_metrics, "network_rx_bytes") == 100U);
  BOOST_TEST(JsonInteger(last_metrics, "network_tx_bytes") == 200U);
  BOOST_TEST(JsonInteger(last_metrics, "network_uplink_bytes") == 100U);
  BOOST_TEST(JsonInteger(last_metrics, "network_downlink_bytes") == 200U);
  BOOST_TEST(JsonInteger(last_metrics, "network_uplink_bytes_per_sec") == 37U);
  BOOST_TEST(JsonInteger(last_metrics, "network_downlink_bytes_per_sec") ==
             75U);
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
  BOOST_TEST(daemon_log_tail.at("text").as_string() == "tail next");
  BOOST_TEST(JsonInteger(daemon_log_tail, "start_offset") == 0U);
  BOOST_TEST(JsonInteger(daemon_log_tail, "next_offset") == 9U);

  std::filesystem::remove_all(dir);
}

BOOST_AUTO_TEST_CASE(run_report_summarizes_network_partition_events) {
  const std::filesystem::path dir = MakeTestDir("run-report-partition");
  bbp::WriteText(dir / "resolved-scenario.json",
                 "{\"run_id\":\"r1\",\"chain\":\"firo\",\"nodes\":2,"
                 "\"isolated_network\":true,"
                 "\"workloads\":["
                 "{\"type\":\"partition_nodes\","
                 "\"group_a\":[1],\"group_b\":[2]},"
                 "{\"type\":\"heal_partition\","
                 "\"group_a\":[1],\"group_b\":[2]}]}\n");
  bbp::AppendLine(dir / "events.jsonl",
                  "{\"run_id\":\"r1\",\"node_id\":\"sim\","
                  "\"event\":\"run_started\"}");
  bbp::AppendLine(dir / "events.jsonl",
                  "{\"run_id\":\"r1\",\"node_id\":\"sim\","
                  "\"event\":\"network_partition_applied\","
                  "\"detail\":\"{\\\"workload_index\\\":1,"
                  "\\\"workload_count\\\":2,"
                  "\\\"group_a\\\":[1],\\\"group_b\\\":[2],"
                  "\\\"scope\\\":\\\"source_aware_group\\\","
                  "\\\"rules\\\":[]}\"}");
  bbp::AppendLine(dir / "events.jsonl",
                  "{\"run_id\":\"r1\",\"node_id\":\"sim\","
                  "\"event\":\"network_partition_healed\","
                  "\"detail\":\"{\\\"workload_index\\\":2,"
                  "\\\"workload_count\\\":2,"
                  "\\\"group_a\\\":[1],\\\"group_b\\\":[2],"
                  "\\\"scope\\\":\\\"source_aware_group\\\","
                  "\\\"rules\\\":[]}\"}");
  bbp::AppendLine(dir / "events.jsonl",
                  "{\"run_id\":\"r1\",\"node_id\":\"sim\","
                  "\"event\":\"run_finished\"}");

  const boost::json::value value =
      boost::json::parse(bbp::BuildRunReportJson(dir));
  const boost::json::object& report = value.as_object();

  BOOST_TEST(report.at("ok").as_bool());
  BOOST_TEST(JsonInteger(report, "event_count") == 4U);
  const boost::json::array& workloads = report.at("workloads").as_array();
  BOOST_REQUIRE_EQUAL(workloads.size(), 2U);
  BOOST_TEST(workloads.front().as_object().at("type").as_string() ==
             "partition_nodes");
  const boost::json::array& partitions =
      report.at("network_partitions").as_array();
  BOOST_REQUIRE_EQUAL(partitions.size(), 1U);
  const boost::json::object& partition_detail =
      partitions.front().as_object().at("detail").as_object();
  BOOST_TEST(JsonInteger(partition_detail, "workload_index") == 1U);
  BOOST_TEST(JsonInteger(partition_detail, "workload_count") == 2U);
  BOOST_REQUIRE_EQUAL(partition_detail.at("group_a").as_array().size(), 1U);
  const boost::json::array& heals =
      report.at("network_partition_heals").as_array();
  BOOST_REQUIRE_EQUAL(heals.size(), 1U);
  const boost::json::object& heal_detail =
      heals.front().as_object().at("detail").as_object();
  BOOST_TEST(JsonInteger(heal_detail, "workload_index") == 2U);

  std::filesystem::remove_all(dir);
}

BOOST_AUTO_TEST_CASE(run_report_summarizes_peer_churn_events) {
  const std::filesystem::path dir = MakeTestDir("run-report-peer-churn");
  bbp::WriteText(dir / "resolved-scenario.json",
                 "{\"run_id\":\"r1\",\"chain\":\"firo\",\"nodes\":2,"
                 "\"isolated_network\":true,"
                 "\"workloads\":["
                 "{\"type\":\"disconnect_peer\",\"node\":2,\"peer\":1,"
                 "\"timeout_sec\":5},"
                 "{\"type\":\"connect_peer\",\"node\":2,\"peer\":1,"
                 "\"timeout_sec\":5}]}\n");
  bbp::AppendLine(dir / "events.jsonl",
                  "{\"run_id\":\"r1\",\"node_id\":\"sim\","
                  "\"event\":\"run_started\"}");
  bbp::AppendLine(dir / "events.jsonl",
                  "{\"run_id\":\"r1\",\"node_id\":\"firo-2\","
                  "\"event\":\"peer_disconnected\","
                  "\"detail\":\"{\\\"workload_index\\\":1,"
                  "\\\"workload_count\\\":2,\\\"node\\\":2,"
                  "\\\"peer\\\":1,\\\"address\\\":\\\"10.210.1.2:18168\\\","
                  "\\\"before_peer_count\\\":1,"
                  "\\\"after_peer_count\\\":0,"
                  "\\\"connected_before\\\":true,"
                  "\\\"connected_after\\\":false}\"}");
  bbp::AppendLine(dir / "events.jsonl",
                  "{\"run_id\":\"r1\",\"node_id\":\"firo-2\","
                  "\"event\":\"peer_connected\","
                  "\"detail\":\"{\\\"workload_index\\\":2,"
                  "\\\"workload_count\\\":2,\\\"node\\\":2,"
                  "\\\"peer\\\":1,\\\"address\\\":\\\"10.210.1.2:18168\\\","
                  "\\\"before_peer_count\\\":0,"
                  "\\\"after_peer_count\\\":1,"
                  "\\\"connected_before\\\":false,"
                  "\\\"connected_after\\\":true,"
                  "\\\"timeout_sec\\\":5}\"}");
  bbp::AppendLine(dir / "events.jsonl",
                  "{\"run_id\":\"r1\",\"node_id\":\"sim\","
                  "\"event\":\"run_finished\"}");

  const boost::json::value value =
      boost::json::parse(bbp::BuildRunReportJson(dir));
  const boost::json::object& report = value.as_object();

  BOOST_TEST(report.at("ok").as_bool());
  const boost::json::array& workloads = report.at("workloads").as_array();
  BOOST_REQUIRE_EQUAL(workloads.size(), 2U);
  BOOST_TEST(workloads.front().as_object().at("type").as_string() ==
             "disconnect_peer");
  const boost::json::array& disconnects =
      report.at("peer_disconnects").as_array();
  BOOST_REQUIRE_EQUAL(disconnects.size(), 1U);
  const boost::json::object& disconnect_detail =
      disconnects.front().as_object().at("detail").as_object();
  BOOST_TEST(JsonInteger(disconnect_detail, "workload_index") == 1U);
  BOOST_TEST(JsonInteger(disconnect_detail, "node") == 2U);
  BOOST_TEST(JsonInteger(disconnect_detail, "peer") == 1U);
  BOOST_TEST(!disconnect_detail.at("connected_after").as_bool());
  const boost::json::array& connects = report.at("peer_connects").as_array();
  BOOST_REQUIRE_EQUAL(connects.size(), 1U);
  const boost::json::object& connect_detail =
      connects.front().as_object().at("detail").as_object();
  BOOST_TEST(JsonInteger(connect_detail, "workload_index") == 2U);
  BOOST_TEST(connect_detail.at("connected_after").as_bool());
  BOOST_TEST(JsonInteger(connect_detail, "timeout_sec") == 5U);

  std::filesystem::remove_all(dir);
}

BOOST_AUTO_TEST_CASE(run_report_summarizes_raw_transaction_events) {
  const std::filesystem::path dir = MakeTestDir("run-report-raw-tx");
  bbp::WriteText(
      dir / "resolved-scenario.json",
      "{\"run_id\":\"r1\",\"chain\":\"firo\",\"nodes\":1,"
      "\"workloads\":["
      "{\"type\":\"send_raw_transaction\",\"funding_node\":1,"
      "\"submit_node\":1,"
      "\"source_address\":\"TEDbE9M6woLAtvxKoFitLpFgeDHFicgTA2\","
      "\"source_private_key\":"
      "\"cTpB4YiyKiBcPxnefsDpbnDxFDffjqJob8wGCEDXxgQ7zQoMXJdH\","
      "\"destination_address\":\"TPxjJMGYU3jFz9zioYfGcq7w47ZGFW3Xbh\","
      "\"funding_blocks\":101,\"amount\":\"39.99000000\","
      "\"fee\":\"0.01000000\",\"timeout_sec\":30}]}\n");
  bbp::AppendLine(dir / "events.jsonl",
                  "{\"run_id\":\"r1\",\"node_id\":\"sim\","
                  "\"event\":\"run_started\"}");
  bbp::AppendLine(
      dir / "events.jsonl",
      "{\"run_id\":\"r1\",\"node_id\":\"firo-1\","
      "\"event\":\"raw_transaction_submitted\","
      "\"detail\":\"{\\\"workload_index\\\":1,"
      "\\\"workload_count\\\":1,\\\"funding_node\\\":1,"
      "\\\"submit_node\\\":1,"
      "\\\"source_address\\\":\\\"TEDbE9M6woLAtvxKoFitLpFgeDHFicgTA2\\\","
      "\\\"destination_address\\\":\\\"TPxjJMGYU3jFz9zioYfGcq7w47ZGFW3Xbh\\\","
      "\\\"funding_blocks\\\":101,\\\"funding_hash_count\\\":101,"
      "\\\"funding_start_height\\\":0,"
      "\\\"funding_target_height\\\":101,"
      "\\\"selected_utxo\\\":{\\\"txid\\\":\\\"abc\\\","
      "\\\"vout\\\":0,\\\"amount\\\":\\\"40.00000000\\\","
      "\\\"block_hash\\\":\\\"def\\\",\\\"confirmations\\\":101},"
      "\\\"amount\\\":\\\"39.99000000\\\","
      "\\\"fee\\\":\\\"0.01000000\\\","
      "\\\"change_amount\\\":\\\"0.00000000\\\","
      "\\\"txid\\\":\\\"tx123\\\",\\\"mempool_size\\\":1,"
      "\\\"timeout_sec\\\":30}\"}");
  bbp::AppendLine(dir / "events.jsonl",
                  "{\"run_id\":\"r1\",\"node_id\":\"sim\","
                  "\"event\":\"run_finished\"}");

  const boost::json::value value =
      boost::json::parse(bbp::BuildRunReportJson(dir));
  const boost::json::object& report = value.as_object();

  BOOST_TEST(report.at("ok").as_bool());
  const boost::json::array& workloads = report.at("workloads").as_array();
  BOOST_REQUIRE_EQUAL(workloads.size(), 1U);
  BOOST_TEST(workloads.front().as_object().at("type").as_string() ==
             "send_raw_transaction");
  const boost::json::array& transactions =
      report.at("raw_transactions").as_array();
  BOOST_REQUIRE_EQUAL(transactions.size(), 1U);
  const boost::json::object& transaction =
      transactions.front().as_object().at("detail").as_object();
  BOOST_TEST(JsonInteger(transaction, "workload_index") == 1U);
  BOOST_TEST(transaction.at("txid").as_string() == "tx123");
  BOOST_TEST(transaction.at("amount").as_string() == "39.99000000");
  BOOST_TEST(JsonInteger(transaction, "mempool_size") == 1U);
  const boost::json::object& utxo = transaction.at("selected_utxo").as_object();
  BOOST_TEST(utxo.at("amount").as_string() == "40.00000000");
  BOOST_TEST(JsonInteger(utxo, "confirmations") == 101U);

  std::filesystem::remove_all(dir);
}

BOOST_AUTO_TEST_CASE(run_report_exposes_failed_run_detail) {
  const std::filesystem::path dir = MakeTestDir("run-report-failed");
  bbp::AppendLine(dir / "events.jsonl",
                  "{\"run_id\":\"r1\",\"node_id\":\"sim\","
                  "\"timestamp\":\"2026-07-09T00:00:00Z\","
                  "\"event\":\"run_started\"}");
  bbp::AppendLine(dir / "events.jsonl",
                  "{\"run_id\":\"r1\",\"node_id\":\"sim\","
                  "\"timestamp\":\"2026-07-09T00:00:01Z\","
                  "\"event\":\"run_failed\","
                  "\"detail\":\"peer wait timed out\"}");

  const boost::json::value value =
      boost::json::parse(bbp::BuildRunReportJson(dir));
  const boost::json::object& report = value.as_object();

  BOOST_TEST(!report.at("ok").as_bool());
  BOOST_TEST(report.at("status").as_string() == "failed");
  BOOST_TEST(report.at("started_at").as_string() == "2026-07-09T00:00:00Z");
  BOOST_TEST(report.at("failed_at").as_string() == "2026-07-09T00:00:01Z");
  BOOST_TEST(report.at("failure").as_string() == "peer wait timed out");
  BOOST_TEST(JsonInteger(report, "event_count") == 2U);

  std::filesystem::remove_all(dir);
}

BOOST_AUTO_TEST_CASE(
    run_report_bounds_scheduled_blocks_and_applies_runtime_policy) {
  const std::filesystem::path dir = MakeTestDir("run-report-scheduled-blocks");
  boost::json::object initial_policy;
  initial_policy["enabled"] = true;
  initial_policy["native_mining"] = false;
  initial_policy["period_ms"] = 1000U;
  initial_policy["probability"] = 0.5;
  initial_policy["seed"] = 1U;
  boost::json::object scenario;
  scenario["run_id"] = "r1";
  scenario["nodes"] = 1U;
  scenario["block_production"] = std::move(initial_policy);
  bbp::WriteText(dir / "resolved-scenario.json",
                 boost::json::serialize(scenario) + "\n");
  bbp::AppendLine(dir / "events.jsonl",
                  R"({"run_id":"r1","node_id":"sim","event":"run_started"})");
  for (std::uint64_t index = 0U; index < 260U; ++index) {
    boost::json::array hashes;
    hashes.emplace_back("hash-" + std::to_string(index));
    boost::json::object detail;
    detail["hashes"] = std::move(hashes);
    boost::json::object event;
    event["run_id"] = "r1";
    event["node_id"] = "firo-1";
    event["event"] = "scheduled_block_produced";
    event["detail"] = boost::json::serialize(detail);
    bbp::AppendLine(dir / "events.jsonl", boost::json::serialize(event));
  }
  boost::json::object command_detail;
  command_detail["kind"] = "set_block_production_policy";
  command_detail["period_ms"] = 250U;
  command_detail["probability"] = 0.75;
  command_detail["seed"] = 9U;
  boost::json::object command_event;
  command_event["run_id"] = "r1";
  command_event["node_id"] = "sim";
  command_event["event"] = "operator_command_completed";
  command_event["detail"] = boost::json::serialize(command_detail);
  bbp::AppendLine(dir / "events.jsonl", boost::json::serialize(command_event));
  bbp::AppendLine(dir / "events.jsonl",
                  R"({"run_id":"r1","node_id":"sim","event":"run_finished"})");

  const boost::json::value value =
      boost::json::parse(bbp::BuildRunReportJson(dir));
  const boost::json::object& report = value.as_object();
  BOOST_TEST(JsonInteger(report, "scheduled_block_count") == 260U);
  const boost::json::array& blocks = report.at("scheduled_blocks").as_array();
  BOOST_REQUIRE_EQUAL(blocks.size(), 256U);
  BOOST_TEST(blocks.front()
                 .as_object()
                 .at("detail")
                 .as_object()
                 .at("hashes")
                 .as_array()
                 .front()
                 .as_string() == "hash-4");
  const boost::json::object& policy = report.at("block_production").as_object();
  BOOST_TEST(JsonInteger(policy, "period_ms") == 250U);
  BOOST_TEST(policy.at("probability").as_double() == 0.75);
  BOOST_TEST(JsonInteger(policy, "seed") == 9U);

  std::filesystem::remove_all(dir);
}
