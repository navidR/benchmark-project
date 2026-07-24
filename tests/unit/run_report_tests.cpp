#include <unistd.h>

#include <boost/json/array.hpp>
#include <boost/json/object.hpp>
#include <boost/json/parse.hpp>
#include <boost/json/serialize.hpp>
#include <boost/test/unit_test.hpp>
#include <chrono>
#include <cstdint>
#include <filesystem>
#include <iterator>
#include <limits>
#include <stdexcept>
#include <stop_token>
#include <string>
#include <string_view>

#include "bbp/run_report.h"
#include "bbp/simulation_event_kind.h"
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

void AppendDetailEvent(const std::filesystem::path& dir,
                       std::string_view event_name,
                       const boost::json::object& detail,
                       std::string_view node_id = "sim") {
  boost::json::object event;
  event["run_id"] = "load";
  event["node_id"] = node_id;
  event["event"] = event_name;
  event["detail"] = boost::json::serialize(detail);
  bbp::AppendLine(dir / "events.jsonl", boost::json::serialize(event));
}

boost::json::object TransactionLoadProgressDetail(
    std::uint64_t workload_index, std::uint64_t revision,
    std::uint64_t attempted, std::uint64_t submitted,
    std::uint64_t rejected = 0U, std::uint64_t timed_out = 0U,
    std::uint64_t backpressured = 0U, std::uint64_t dropped = 0U,
    std::uint64_t propagated = 0U, std::uint64_t confirmed = 0U,
    std::uint64_t failed = 0U, std::uint64_t cancelled = 0U) {
  boost::json::object detail;
  detail["workload_index"] = workload_index;
  detail["workload_count"] = 2U;
  detail["revision"] = revision;
  detail["attempted"] = attempted;
  detail["submitted"] = submitted;
  detail["rejected"] = rejected;
  detail["timed_out"] = timed_out;
  detail["backpressured"] = backpressured;
  detail["dropped"] = dropped;
  detail["propagated"] = propagated;
  detail["confirmed"] = confirmed;
  detail["failed"] = failed;
  detail["cancelled"] = cancelled;
  detail["accounting_invariants_hold"] = true;
  return detail;
}

}  // namespace

BOOST_AUTO_TEST_CASE(run_report_honors_pre_requested_cancellation) {
  const std::filesystem::path dir = MakeTestDir("cancelled-report");
  bbp::WriteText(dir / "resolved-scenario.json",
                 R"({"run_id":"cancelled","chain":"firo","nodes":1})");
  std::stop_source stop_source;
  stop_source.request_stop();

  BOOST_CHECK_THROW(bbp::BuildRunReport(dir, stop_source.get_token()),
                    std::runtime_error);
  std::filesystem::remove_all(dir);
}

BOOST_AUTO_TEST_CASE(run_report_rejects_records_from_a_different_run) {
  const std::filesystem::path dir = MakeTestDir("mismatched-run-record");
  bbp::WriteText(dir / "resolved-scenario.json",
                 R"({"run_id":"expected","chain":"firo","nodes":1})");
  bbp::AppendLine(
      dir / "events.jsonl",
      R"({"run_id":"other","node_id":"sim","event":"run_failed"})");

  BOOST_CHECK_EXCEPTION(
      bbp::BuildRunReport(dir), std::runtime_error,
      [](const std::runtime_error& error) {
        return std::string(error.what()).find(
                   "run_id does not match resolved scenario") !=
               std::string::npos;
      });
  std::filesystem::remove_all(dir);
}

BOOST_AUTO_TEST_CASE(
    run_report_applies_runtime_generation_as_one_complete_snapshot) {
  const std::filesystem::path dir = MakeTestDir("runtime-generation-report");
  bbp::WriteText(
      dir / "resolved-scenario.json",
      R"({"run_id":"generation","chain":"firo","nodes":0,"node_configs":[],"topology_initial_edges":[]})");
  const boost::json::object detail{
      {"generation", 2U},
      {"node_count", 2U},
      {"node_ids", boost::json::array{"firo-1", "firo-2"}},
      {"node_configs",
       boost::json::array{
           boost::json::object{{"index", 1U},
                               {"id", "firo-1"},
                               {"chain", "firo"},
                               {"role", "base"},
                               {"lifecycle", "running"}},
           boost::json::object{{"index", 2U},
                               {"id", "firo-2"},
                               {"chain", "firo"},
                               {"role", "miner"},
                               {"lifecycle", "running"}}}},
      {"topology", boost::json::object{{"type", "full_mesh"}}},
      {"topology_current_edges",
       boost::json::array{
           boost::json::object{{"from", 1U},
                               {"to", 2U},
                               {"band", 1U},
                               {"active", true},
                               {"condition", nullptr}},
           boost::json::object{{"from", 2U},
                               {"to", 1U},
                               {"band", 1U},
                               {"active", true},
                               {"condition", nullptr}}}},
      {"manifest_state", "live"}};
  bbp::AppendLine(
      dir / "events.jsonl",
      boost::json::serialize(boost::json::object{
          {"run_id", "generation"},
          {"node_id", "sim"},
          {"event", "runtime_generation_published"},
          {"detail", boost::json::serialize(detail)}}));

  const boost::json::object report = bbp::BuildRunReport(dir);
  BOOST_TEST(JsonInteger(report, "inventory_generation") == 2U);
  BOOST_TEST(JsonInteger(report, "nodes") == 2U);
  BOOST_TEST(report.at("node_ids").as_array() ==
             boost::json::array({"firo-1", "firo-2"}));
  BOOST_TEST(report.at("node_configs").as_array().size() == 2U);
  BOOST_TEST(report.at("topology_current_edges").as_array().size() == 2U);
  BOOST_TEST(report.at("nodes_summary").as_array().size() == 2U);
  BOOST_TEST(report.at("nodes_summary")
                 .as_array()
                 .back()
                 .as_object()
                 .at("role")
                 .as_string() == "miner");
  std::filesystem::remove_all(dir);
}

BOOST_AUTO_TEST_CASE(item10_prefixed_report_retention_reproducer) {
  const std::filesystem::path dir = MakeTestDir("item10-prefixed-report");
  bbp::WriteText(dir / "resolved-scenario.json",
                 R"({"run_id":"item10","chain":"firo","nodes":1})");
  constexpr std::uint64_t kProduced = 10'000U;
  for (std::uint64_t index = 0U; index < kProduced; ++index) {
    boost::json::object event;
    event["run_id"] = "item10";
    event["node_id"] = "firo-1";
    event["event"] = "generated_blocks";
    event["detail"] = std::to_string(index);
    bbp::AppendLine(dir / "events.jsonl", boost::json::serialize(event));
  }

  const auto refresh_started = std::chrono::steady_clock::now();
  bbp::IncrementalRunReport incremental(dir);
  const boost::json::object& report = incremental.Refresh();
  const auto refresh_elapsed =
      std::chrono::steady_clock::now() - refresh_started;
  BOOST_TEST(refresh_elapsed < std::chrono::seconds(5));
  const auto idle_refresh_started = std::chrono::steady_clock::now();
  static_cast<void>(incremental.Refresh(256U));
  const auto idle_refresh_elapsed =
      std::chrono::steady_clock::now() - idle_refresh_started;
  BOOST_TEST(idle_refresh_elapsed < std::chrono::seconds(1));
  BOOST_TEST(incremental.last_refresh_stats().event_records == 0U);
  BOOST_TEST(report.at("generated_blocks").as_array().size() <= 256U);
  BOOST_TEST(JsonInteger(report, "event_count") == kProduced);
  std::filesystem::remove_all(dir);
}

BOOST_AUTO_TEST_CASE(
    run_report_bounds_every_historical_summary_and_preserves_exact_totals) {
  const std::filesystem::path dir = MakeTestDir("bounded-all-summaries");
  bbp::WriteText(dir / "resolved-scenario.json",
                 R"({"run_id":"bounded","chain":"firo","nodes":1})");
  const std::vector<std::pair<bbp::SimulationEventKind, std::string_view>>
      histories = {
          {bbp::SimulationEventKind::kGeneratedBlocks, "generated_blocks"},
          {bbp::SimulationEventKind::kNodeStartDeadlineReached,
           "node_start_deadlines"},
          {bbp::SimulationEventKind::kNodeStopDeadlineReached,
           "node_stop_deadlines"},
          {bbp::SimulationEventKind::kProcessExited, "process_exits"},
          {bbp::SimulationEventKind::kRestartPolicyApplied,
           "restart_policy_actions"},
          {bbp::SimulationEventKind::kScheduledBlockProduced,
           "scheduled_blocks"},
          {bbp::SimulationEventKind::kScheduledEventStarted,
           "scheduled_events_started"},
          {bbp::SimulationEventKind::kScheduledEventCompleted,
           "scheduled_events_completed"},
          {bbp::SimulationEventKind::kScheduledEventFailed,
           "scheduled_events_failed"},
          {bbp::SimulationEventKind::kCheckpointRecorded, "checkpoints"},
          {bbp::SimulationEventKind::kHeightReached, "height_reached"},
          {bbp::SimulationEventKind::kHeightWaitReached, "height_waits"},
          {bbp::SimulationEventKind::kPeerCountReached, "peer_waits"},
          {bbp::SimulationEventKind::kPeerConnected, "peer_connects"},
          {bbp::SimulationEventKind::kPeerDisconnected, "peer_disconnects"},
          {bbp::SimulationEventKind::kRawTransactionSubmitted,
           "raw_transactions"},
          {bbp::SimulationEventKind::kTransactionVisible,
           "transaction_visibility"},
          {bbp::SimulationEventKind::kTransactionConfirmed,
           "transaction_confirmations"},
          {bbp::SimulationEventKind::kTransactionLoadAttempt,
           "transaction_load_attempts"},
          {bbp::SimulationEventKind::kTransactionLoadCompleted,
           "transaction_load_summaries"},
          {bbp::SimulationEventKind::kNodeRestarted, "node_restarts"},
          {bbp::SimulationEventKind::kNodeFreezeCompleted, "node_freezes"},
          {bbp::SimulationEventKind::kResourceLimitsUpdated,
           "resource_updates"},
          {bbp::SimulationEventKind::kResourceProfileUpdated,
           "resource_profile_updates"},
          {bbp::SimulationEventKind::kNetworkProfileUpdated,
           "network_profile_updates"},
          {bbp::SimulationEventKind::kProfileUpdateRollbackFailed,
           "profile_update_rollback_failures"},
          {bbp::SimulationEventKind::kNetworkConditionUpdated,
           "network_condition_updates"},
          {bbp::SimulationEventKind::kNetworkBlockApplied, "network_blocks"},
          {bbp::SimulationEventKind::kNetworkBlockRemoved, "network_unblocks"},
          {bbp::SimulationEventKind::kNetworkPartitionApplied,
           "network_partitions"},
          {bbp::SimulationEventKind::kNetworkPartitionHealed,
           "network_partition_heals"},
          {bbp::SimulationEventKind::kDirectionalNetworkPoliciesVerified,
           "directional_network_policy_verifications"},
          {bbp::SimulationEventKind::kTopologyEdgeUpdated,
           "topology_edge_updates"},
          {bbp::SimulationEventKind::kTopologyEdgeUpdateRollbackFailed,
           "topology_edge_rollback_failures"},
          {bbp::SimulationEventKind::kWalletFunded, "wallet_funding"},
          {bbp::SimulationEventKind::kWalletTransactionSubmitted,
           "wallet_transactions"},
          {bbp::SimulationEventKind::kOperatorCommandCompleted,
           "operator_commands"},
      };
  constexpr std::uint64_t kEventsPerKind = 300U;
  for (const auto& [kind, field] : histories) {
    static_cast<void>(field);
    for (std::uint64_t index = 0U; index < kEventsPerKind; ++index) {
      boost::json::object detail;
      detail["index"] = index;
      boost::json::object event;
      event["run_id"] = "bounded";
      event["node_id"] = "firo-1";
      event["event"] = bbp::SimulationEventKindName(kind);
      event["detail"] = boost::json::serialize(detail);
      bbp::AppendLine(dir / "events.jsonl", boost::json::serialize(event));
    }
  }

  const auto all_histories_started = std::chrono::steady_clock::now();
  const boost::json::object report = bbp::BuildRunReport(dir);
  const auto all_histories_elapsed =
      std::chrono::steady_clock::now() - all_histories_started;
  BOOST_TEST(all_histories_elapsed < std::chrono::seconds(5));
  BOOST_TEST(JsonInteger(report, "summary_retention_limit") ==
             bbp::kMaximumRunReportSummaryRecords);
  BOOST_TEST(JsonInteger(report, "event_count") ==
             histories.size() * kEventsPerKind);
  const boost::json::object& event_counts =
      report.at("event_counts").as_object();
  for (const auto& [kind, field] : histories) {
    const boost::json::array& retained = report.at(field).as_array();
    BOOST_TEST(retained.size() == bbp::kMaximumRunReportSummaryRecords);
    BOOST_TEST(JsonInteger(event_counts, bbp::SimulationEventKindName(kind)) ==
               kEventsPerKind);
    const boost::json::object& first = retained.front().as_object();
    BOOST_TEST(JsonInteger(first.at("detail").as_object(), "index") ==
               kEventsPerKind - bbp::kMaximumRunReportSummaryRecords);
    const boost::json::object& last = retained.back().as_object();
    BOOST_TEST(JsonInteger(last.at("detail").as_object(), "index") ==
               kEventsPerKind - 1U);
  }
  std::filesystem::remove_all(dir);
}

BOOST_AUTO_TEST_CASE(run_report_preserves_manual_operator_connection_command) {
  const std::filesystem::path dir =
      MakeTestDir("run-report-operator-connection");
  bbp::WriteText(dir / "resolved-scenario.json",
                 R"({"run_id":"r1","chain":"firo","nodes":1})");
  boost::json::object detail;
  detail["kind"] = "manual_firo_gui";
  detail["manual_launch"] = true;
  detail["discovery_disabled"] = true;
  detail["wallet_enabled"] = true;
  detail["network"] = "regtest";
  detail["executable"] = "/opt/firo/bin/firo-qt";
  detail["arguments"] =
      boost::json::array{"-regtest", "-datadir=/tmp/run/operator/firo-qt",
                         "-connect=10.77.0.2:19168", "-dnsseed=0", "-listen=0"};
  detail["argv"] = boost::json::array{"/opt/firo/bin/firo-qt",
                                      "-regtest",
                                      "-datadir=/tmp/run/operator/firo-qt",
                                      "-connect=10.77.0.2:19168",
                                      "-dnsseed=0",
                                      "-listen=0"};
  detail["command"] =
      "'/opt/firo/bin/firo-qt' '-regtest' "
      "'-datadir=/tmp/run/operator/firo-qt' "
      "'-connect=10.77.0.2:19168' '-dnsseed=0' '-listen=0'";
  detail["data_dir"] = "/tmp/run/operator/firo-qt";
  detail["peer_address"] = "10.77.0.2";
  detail["peer_port"] = 19168U;
  detail["peer_endpoint"] = "10.77.0.2:19168";
  boost::json::object event;
  event["timestamp"] = "2026-07-21T00:00:00Z";
  event["run_id"] = "r1";
  event["node_id"] = "firo-1";
  event["event"] = "operator_connection_command";
  event["detail"] = boost::json::serialize(detail);
  bbp::AppendLine(dir / "events.jsonl", boost::json::serialize(event));

  const boost::json::object report =
      boost::json::parse(bbp::BuildRunReportJson(dir)).as_object();
  const boost::json::object& connection =
      report.at("operator_connection_command").as_object();
  BOOST_TEST(connection.at("node_id").as_string() == "firo-1");
  BOOST_TEST(connection.at("timestamp").as_string() == "2026-07-21T00:00:00Z");
  BOOST_TEST(connection.at("command") == detail.at("command"));
  BOOST_TEST(connection.at("arguments") == detail.at("arguments"));
  BOOST_TEST(connection.at("argv") == detail.at("argv"));
  BOOST_TEST(connection.at("data_dir") == detail.at("data_dir"));
  BOOST_TEST(connection.at("peer_endpoint") == detail.at("peer_endpoint"));
  BOOST_TEST(connection.at("manual_launch").as_bool());
  BOOST_TEST(connection.at("discovery_disabled").as_bool());
  BOOST_TEST(connection.at("wallet_enabled").as_bool());
  std::filesystem::remove_all(dir);
}

BOOST_AUTO_TEST_CASE(run_report_normalizes_process_control_schema) {
  const std::filesystem::path canonical =
      MakeTestDir("run-report-process-canonical");
  bbp::WriteText(
      canonical / "resolved-scenario.json",
      R"({"run_id":"canonical","chain":"firo","nodes":1,"process":{"runtime_node_restarts":[{"node":1}],"runtime_node_freezes":[{"node":1,"duration_ms":10}]}})");
  const boost::json::object canonical_report =
      boost::json::parse(bbp::BuildRunReportJson(canonical)).as_object();
  const boost::json::object& process =
      canonical_report.at("process").as_object();
  BOOST_TEST(process.at("runtime_node_restarts") ==
             canonical_report.at("runtime_node_restarts"));
  BOOST_TEST(process.at("runtime_node_freezes") ==
             canonical_report.at("runtime_node_freezes"));

  const std::filesystem::path legacy = MakeTestDir("run-report-process-legacy");
  bbp::WriteText(
      legacy / "resolved-scenario.json",
      R"({"run_id":"legacy","chain":"firo","nodes":1,"runtime_node_restarts":[{"node":1}],"runtime_node_freezes":[{"node":1,"duration_ms":20}]})");
  const boost::json::object legacy_report =
      boost::json::parse(bbp::BuildRunReportJson(legacy)).as_object();
  const boost::json::object& normalized =
      legacy_report.at("process").as_object();
  BOOST_TEST(normalized.at("runtime_node_restarts") ==
             legacy_report.at("runtime_node_restarts"));
  BOOST_TEST(normalized.at("runtime_node_freezes") ==
             legacy_report.at("runtime_node_freezes"));

  const std::filesystem::path conflicting =
      MakeTestDir("run-report-process-conflicting");
  bbp::WriteText(
      conflicting / "resolved-scenario.json",
      R"({"run_id":"conflicting","chain":"firo","nodes":1,"process":{"runtime_node_restarts":[{"node":1}]},"runtime_node_restarts":[]})");
  BOOST_CHECK_THROW(bbp::BuildRunReportJson(conflicting), std::runtime_error);
}

BOOST_AUTO_TEST_CASE(run_report_preserves_canonical_chain_registry) {
  const std::filesystem::path dir = MakeTestDir("run-report-chain-registry");
  bbp::WriteText(
      dir / "resolved-scenario.json",
      R"({"run_id":"registry","chain":"firo","chains":{"firo":{"driver":"firo","default_binary":"/opt/firod"}},"nodes":1})");

  const boost::json::object report =
      boost::json::parse(bbp::BuildRunReportJson(dir)).as_object();
  const boost::json::object& definition =
      report.at("chains").as_object().at("firo").as_object();
  BOOST_TEST(definition.at("driver").as_string() == "firo");
  BOOST_TEST(definition.at("default_binary").as_string() == "/opt/firod");
}

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
                 "\"resource_profiles\":{\"small\":{\"pids_max\":128}},"
                 "\"network_profiles\":{\"normal\":{\"delay_ms\":2}},"
                 "\"node_configs\":[{\"index\":1,\"id\":\"custom-1\","
                 "\"role\":\"miner\",\"resources\":{\"profile\":\"small\"},"
                 "\"network\":{\"profile\":\"normal\"}}],"
                 "\"resources\":{\"memory_max_bytes\":1024},"
                 "\"default_network_condition\":{\"delay_ms\":2},"
                 "\"node_network_conditions\":[{\"node\":1,\"delay_ms\":3}],"
                 "\"runtime_node_resource_limits\":["
                 "{\"node\":1,\"pids_max\":128}],"
                 "\"process\":{\"runtime_node_restarts\":[{\"node\":1}],"
                 "\"runtime_node_freezes\":["
                 "{\"node\":1,\"duration_ms\":25}]},"
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
                  "\\\"operator_command_sequence\\\":8,"
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
      "{\"run_id\":\"r1\",\"node_id\":\"firo-2\","
      "\"timestamp\":\"2026-07-09T00:00:01Z\","
      "\"event\":\"transaction_visible\","
      "\"detail\":\"{\\\"txid\\\":\\\"tx1\\\","
      "\\\"submission_kind\\\":\\\"wallet_transaction_submitted\\\","
      "\\\"node\\\":2,\\\"state\\\":\\\"mempool\\\","
      "\\\"observed_height\\\":2,\\\"mempool_size\\\":1}\"}");
  bbp::AppendLine(
      dir / "events.jsonl",
      "{\"run_id\":\"r1\",\"node_id\":\"firo-2\","
      "\"timestamp\":\"2026-07-09T00:00:02Z\","
      "\"event\":\"transaction_confirmed\","
      "\"detail\":\"{\\\"txid\\\":\\\"tx1\\\","
      "\\\"submission_kind\\\":\\\"wallet_transaction_submitted\\\","
      "\\\"node\\\":2,\\\"state\\\":\\\"confirmed\\\","
      "\\\"observed_height\\\":3,\\\"mempool_size\\\":0,"
      "\\\"block_hash\\\":\\\"block3\\\","
      "\\\"confirmation_height\\\":3,\\\"confirmations\\\":1}\"}");
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
      "\"cpu_usage_usec\":0,\"cpu_throttled_usec\":0,"
      "\"memory_current\":10,\"io_read_bytes\":0,"
      "\"io_write_bytes\":0,\"mempool_tx_count\":0,"
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
      "\"cpu_usage_usec\":10,\"cpu_throttled_usec\":2,"
      "\"cpu_weight\":250,\"io_weight\":300,\"rpc_latency_ms\":7,"
      "\"memory_current\":20,\"memory_stat\":{\"anon\":19},"
      "\"memory_high_limit_bytes\":null,\"io_read_bytes\":30,"
      "\"io_write_bytes\":40,"
      "\"perf_counter_names\":[\"cycles\",\"task-clock\"],"
      "\"perf_counter_target_kind\":\"node\","
      "\"perf_counter_target_id\":\"firo-1\","
      "\"perf_counter_target_pid\":123,"
      "\"perf_counter_attached_pid\":123,"
      "\"perf_counter_process_generation\":1,"
      "\"perf_counter_cgroup_path\":null,"
      "\"perf_counter_cpus\":[],"
      "\"perf_counters_available\":true,"
      "\"perf_counter_error_kind\":null,\"perf_counter_error\":null,"
      "\"perf_counters\":[{\"name\":\"cycles\","
      "\"raw_value\":100,\"scaled_value\":125,"
      "\"time_enabled_ns\":1000,\"time_running_ns\":800,"
      "\"multiplexed\":true,\"scaled\":true,"
      "\"scaled_overflow\":false}],"
      "\"io_read_operations\":4,\"io_write_operations\":5,"
      "\"io_discard_bytes\":6,\"io_discard_operations\":7,"
      "\"io_max\":[{\"device\":\"252:1\","
      "\"read_bytes_per_sec\":1048576}],"
      "\"pids_current\":2,\"oom_kill\":0,"
      "\"network_rx_bytes\":100,\"network_tx_bytes\":200,"
      "\"network_condition\":{\"bandwidth_mbps\":20,"
      "\"delay_ms\":80,\"jitter_ms\":10,"
      "\"loss_basis_points\":11,\"duplicate_basis_points\":12,"
      "\"corrupt_basis_points\":13,\"reorder_basis_points\":14,"
      "\"limit_packets\":900},"
      "\"network_filter_policy_count\":1,"
      "\"network_filter_policies_with_stats\":1,"
      "\"network_filter_match_bytes\":180,"
      "\"network_filter_match_packets\":3,"
      "\"network_filter_drop_packets\":3,"
      "\"network_policy_counters\":[{\"kind\":\"ipv4_tcp_drop\","
      "\"handle\":1001,\"src_address\":null,\"src_port\":43120,"
      "\"dst_address\":\"198.51.100.7\",\"dst_port\":18168,"
      "\"has_stats\":true,\"match_bytes\":180,"
      "\"match_packets\":3,\"drop_packets\":3}],"
      "\"network_active_block_rules\":[{"
      "\"kind\":\"ipv4_tcp_drop\",\"handle\":1001,"
      "\"src_address\":null,\"src_port\":43120,"
      "\"dst_address\":\"198.51.100.7\","
      "\"dst_port\":18168,\"has_stats\":true,"
      "\"match_bytes\":180,\"match_packets\":3,"
      "\"drop_packets\":3}],"
      "\"directional_network_policy_count\":1,"
      "\"directional_network_policies_with_filter_stats\":1,"
      "\"directional_network_filter_match_bytes\":900,"
      "\"directional_network_filter_match_packets\":9,"
      "\"directional_network_qdisc_count\":2,"
      "\"directional_network_qdiscs_with_stats\":2,"
      "\"directional_network_qdisc_bytes\":900,"
      "\"directional_network_qdisc_packets\":9,"
      "\"directional_network_qdisc_drops\":2,"
      "\"directional_network_qdisc_overlimits\":4,"
      "\"directional_network_qdisc_qlen\":1,"
      "\"directional_network_qdisc_backlog\":64,"
      "\"directional_network_qdisc_requeues\":3,"
      "\"directional_network_policy_counters\":[{"
      "\"band\":1,\"destination_address\":\"198.51.100.8\","
      "\"filter_handle\":3137339393,\"filter_has_stats\":true,"
      "\"filter_match_bytes\":900,\"filter_match_packets\":9,"
      "\"qdisc_bytes\":900,\"qdisc_packets\":9,"
      "\"qdisc_drops\":2,\"qdisc_overlimits\":4,"
      "\"qdisc_qlen\":1,\"qdisc_backlog\":64,"
      "\"qdisc_requeues\":3,\"qdiscs\":[{\"kind\":\"tbf\"},"
      "{\"kind\":\"netem\"}]}],"
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
  BOOST_TEST(JsonInteger(report, "event_count") == 20U);
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
  BOOST_TEST(
      JsonInteger(
          report.at("resource_profiles").as_object().at("small").as_object(),
          "pids_max") == 128U);
  BOOST_TEST(
      JsonInteger(
          report.at("network_profiles").as_object().at("normal").as_object(),
          "delay_ms") == 2U);
  const boost::json::object& node_config =
      report.at("node_configs").as_array().front().as_object();
  BOOST_TEST(node_config.at("id").as_string() == "custom-1");
  BOOST_TEST(
      node_config.at("resources").as_object().at("profile").as_string() ==
      "small");
  BOOST_REQUIRE_EQUAL(report.at("node_network_conditions").as_array().size(),
                      1U);
  BOOST_REQUIRE_EQUAL(
      report.at("runtime_node_resource_limits").as_array().size(), 1U);
  BOOST_REQUIRE_EQUAL(report.at("runtime_node_restarts").as_array().size(), 1U);
  BOOST_REQUIRE_EQUAL(report.at("runtime_node_freezes").as_array().size(), 1U);
  const boost::json::object& process = report.at("process").as_object();
  BOOST_REQUIRE_EQUAL(process.at("runtime_node_restarts").as_array().size(),
                      1U);
  BOOST_REQUIRE_EQUAL(process.at("runtime_node_freezes").as_array().size(), 1U);
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
  BOOST_TEST(JsonInteger(resource_update, "operator_command_sequence") == 8U);
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
  const boost::json::array& transaction_visibility =
      report.at("transaction_visibility").as_array();
  BOOST_REQUIRE_EQUAL(transaction_visibility.size(), 1U);
  const boost::json::object& visible =
      transaction_visibility.front().as_object();
  BOOST_TEST(visible.at("node_id").as_string() == "firo-2");
  BOOST_TEST(visible.at("detail").as_object().at("txid").as_string() == "tx1");
  BOOST_TEST(visible.at("detail").as_object().at("state").as_string() ==
             "mempool");
  const boost::json::array& transaction_confirmations =
      report.at("transaction_confirmations").as_array();
  BOOST_REQUIRE_EQUAL(transaction_confirmations.size(), 1U);
  const boost::json::object& confirmed =
      transaction_confirmations.front().as_object().at("detail").as_object();
  BOOST_TEST(confirmed.at("block_hash").as_string() == "block3");
  BOOST_TEST(JsonInteger(confirmed, "confirmation_height") == 3U);
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
  const boost::json::array& metrics_history =
      node.at("metrics_history").as_array();
  BOOST_REQUIRE_EQUAL(metrics_history.size(), 2U);
  const boost::json::object& first_metric = metrics_history.front().as_object();
  BOOST_TEST(JsonInteger(first_metric, "timestamp_ms") == 1000U);
  BOOST_TEST(first_metric.at("cpu_percent").is_null());
  BOOST_TEST(first_metric.at("network_downlink_bytes_per_sec").is_null());
  const boost::json::object& second_metric = metrics_history.back().as_object();
  BOOST_TEST(JsonInteger(second_metric, "timestamp_ms") == 3000U);
  BOOST_TEST(JsonInteger(second_metric, "memory_current") == 20U);
  BOOST_TEST(JsonInteger(second_metric, "io_read_bytes_per_sec") == 15U);
  BOOST_TEST(JsonInteger(second_metric, "io_write_bytes_per_sec") == 20U);
  BOOST_TEST(JsonInteger(second_metric, "network_downlink_bytes_per_sec") ==
             75U);
  BOOST_TEST(JsonInteger(second_metric, "network_uplink_bytes_per_sec") == 37U);
  BOOST_TEST(second_metric.at("cpu_percent").as_double() == 0.0005,
             boost::test_tools::tolerance(0.000001));
  BOOST_TEST(second_metric.at("cpu_throttled_percent").as_double() == 0.0001,
             boost::test_tools::tolerance(0.000001));
  BOOST_TEST(JsonInteger(second_metric, "height") == 2U);
  BOOST_TEST(JsonInteger(second_metric, "mempool_tx_count") == 3U);
  BOOST_TEST(JsonInteger(second_metric, "rpc_latency_ms") == 7U);
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
  BOOST_TEST(last_metrics.at("perf_counters_available").as_bool());
  BOOST_TEST(last_metrics.at("perf_counter_target_kind").as_string() == "node");
  BOOST_TEST(last_metrics.at("perf_counter_target_id").as_string() == "firo-1");
  BOOST_TEST(JsonInteger(last_metrics, "perf_counter_target_pid") == 123U);
  BOOST_TEST(JsonInteger(last_metrics, "perf_counter_attached_pid") == 123U);
  BOOST_TEST(JsonInteger(last_metrics, "perf_counter_process_generation") ==
             1U);
  BOOST_TEST(last_metrics.at("perf_counter_cgroup_path").is_null());
  BOOST_TEST(last_metrics.at("perf_counter_cpus").as_array().empty());
  BOOST_REQUIRE_EQUAL(last_metrics.at("perf_counter_names").as_array().size(),
                      2U);
  BOOST_TEST(last_metrics.at("perf_counter_error_kind").is_null());
  BOOST_REQUIRE_EQUAL(last_metrics.at("perf_counters").as_array().size(), 1U);
  const boost::json::object& perf_counter =
      last_metrics.at("perf_counters").as_array()[0].as_object();
  BOOST_TEST(perf_counter.at("name").as_string() == "cycles");
  BOOST_TEST(JsonInteger(perf_counter, "raw_value") == 100U);
  BOOST_TEST(JsonInteger(perf_counter, "scaled_value") == 125U);
  BOOST_TEST(JsonInteger(perf_counter, "time_enabled_ns") == 1000U);
  BOOST_TEST(JsonInteger(perf_counter, "time_running_ns") == 800U);
  BOOST_TEST(perf_counter.at("multiplexed").as_bool());
  BOOST_TEST(perf_counter.at("scaled").as_bool());
  BOOST_TEST(!perf_counter.at("scaled_overflow").as_bool());
  BOOST_TEST(JsonInteger(last_metrics, "cpu_weight") == 250U);
  BOOST_TEST(JsonInteger(last_metrics, "io_weight") == 300U);
  BOOST_TEST(JsonInteger(last_metrics, "io_read_operations") == 4U);
  BOOST_TEST(JsonInteger(last_metrics, "io_write_operations") == 5U);
  BOOST_TEST(JsonInteger(last_metrics, "io_discard_bytes") == 6U);
  BOOST_TEST(JsonInteger(last_metrics, "io_discard_operations") == 7U);
  BOOST_TEST(JsonInteger(last_metrics.at("memory_stat").as_object(), "anon") ==
             19U);
  BOOST_REQUIRE_EQUAL(last_metrics.at("io_max").as_array().size(), 1U);
  BOOST_TEST(last_metrics.at("io_max")
                 .as_array()[0]
                 .as_object()
                 .at("device")
                 .as_string() == "252:1");
  BOOST_TEST(JsonInteger(last_metrics, "pids_current") == 2U);
  BOOST_TEST(JsonInteger(last_metrics, "oom_kill") == 0U);
  BOOST_TEST(JsonInteger(last_metrics, "network_rx_bytes") == 100U);
  BOOST_TEST(JsonInteger(last_metrics, "network_tx_bytes") == 200U);
  const boost::json::object& network_condition =
      last_metrics.at("network_condition").as_object();
  BOOST_TEST(JsonInteger(network_condition, "bandwidth_mbps") == 20U);
  BOOST_TEST(JsonInteger(network_condition, "delay_ms") == 80U);
  BOOST_TEST(JsonInteger(network_condition, "jitter_ms") == 10U);
  BOOST_TEST(JsonInteger(network_condition, "loss_basis_points") == 11U);
  BOOST_TEST(JsonInteger(network_condition, "duplicate_basis_points") == 12U);
  BOOST_TEST(JsonInteger(network_condition, "corrupt_basis_points") == 13U);
  BOOST_TEST(JsonInteger(network_condition, "reorder_basis_points") == 14U);
  BOOST_TEST(JsonInteger(network_condition, "limit_packets") == 900U);
  BOOST_TEST(JsonInteger(last_metrics, "network_filter_policy_count") == 1U);
  BOOST_TEST(JsonInteger(last_metrics, "network_filter_policies_with_stats") ==
             1U);
  BOOST_TEST(JsonInteger(last_metrics, "network_filter_match_bytes") == 180U);
  BOOST_TEST(JsonInteger(last_metrics, "network_filter_match_packets") == 3U);
  BOOST_TEST(JsonInteger(last_metrics, "network_filter_drop_packets") == 3U);
  const boost::json::array& policy_counters =
      last_metrics.at("network_policy_counters").as_array();
  BOOST_REQUIRE_EQUAL(policy_counters.size(), 1U);
  const boost::json::object& policy_counter = policy_counters[0].as_object();
  BOOST_TEST(JsonInteger(policy_counter, "handle") == 1001U);
  BOOST_TEST(policy_counter.at("src_address").is_null());
  BOOST_TEST(JsonInteger(policy_counter, "src_port") == 43120U);
  BOOST_TEST(policy_counter.at("dst_address").as_string() == "198.51.100.7");
  BOOST_TEST(JsonInteger(policy_counter, "dst_port") == 18168U);
  BOOST_TEST(policy_counter.at("has_stats").as_bool());
  BOOST_TEST(JsonInteger(policy_counter, "drop_packets") == 3U);
  BOOST_REQUIRE_EQUAL(
      last_metrics.at("network_active_block_rules").as_array().size(), 1U);
  BOOST_TEST(JsonInteger(last_metrics.at("network_active_block_rules")
                             .as_array()
                             .front()
                             .as_object(),
                         "src_port") == 43120U);
  BOOST_TEST(JsonInteger(last_metrics, "directional_network_policy_count") ==
             1U);
  BOOST_TEST(JsonInteger(last_metrics,
                         "directional_network_policies_with_filter_stats") ==
             1U);
  BOOST_TEST(JsonInteger(last_metrics,
                         "directional_network_filter_match_bytes") == 900U);
  BOOST_TEST(JsonInteger(last_metrics,
                         "directional_network_filter_match_packets") == 9U);
  BOOST_TEST(JsonInteger(last_metrics, "directional_network_qdisc_count") ==
             2U);
  BOOST_TEST(
      JsonInteger(last_metrics, "directional_network_qdiscs_with_stats") == 2U);
  BOOST_TEST(JsonInteger(last_metrics, "directional_network_qdisc_bytes") ==
             900U);
  BOOST_TEST(JsonInteger(last_metrics, "directional_network_qdisc_packets") ==
             9U);
  BOOST_TEST(JsonInteger(last_metrics, "directional_network_qdisc_drops") ==
             2U);
  BOOST_TEST(
      JsonInteger(last_metrics, "directional_network_qdisc_overlimits") == 4U);
  BOOST_TEST(JsonInteger(last_metrics, "directional_network_qdisc_qlen") == 1U);
  BOOST_TEST(JsonInteger(last_metrics, "directional_network_qdisc_backlog") ==
             64U);
  BOOST_TEST(JsonInteger(last_metrics, "directional_network_qdisc_requeues") ==
             3U);
  const boost::json::array& directional_counters =
      last_metrics.at("directional_network_policy_counters").as_array();
  BOOST_REQUIRE_EQUAL(directional_counters.size(), 1U);
  const boost::json::object& directional_counter =
      directional_counters.front().as_object();
  BOOST_TEST(JsonInteger(directional_counter, "band") == 1U);
  BOOST_TEST(directional_counter.at("destination_address").as_string() ==
             "198.51.100.8");
  BOOST_TEST(JsonInteger(directional_counter, "qdisc_packets") == 9U);
  BOOST_REQUIRE_EQUAL(directional_counter.at("qdiscs").as_array().size(), 2U);
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

BOOST_AUTO_TEST_CASE(run_report_bounds_metric_history_per_node) {
  const std::filesystem::path dir = MakeTestDir("run-report-metric-history");
  bbp::WriteText(dir / "resolved-scenario.json",
                 R"({"run_id":"r1","chain":"firo","nodes":1})");
  for (std::uint64_t sample = 1U; sample <= 125U; ++sample) {
    boost::json::object metric;
    metric["run_id"] = "r1";
    metric["node_id"] = "firo-1";
    metric["timestamp_ms"] = sample * 1000U;
    metric["height"] = sample;
    metric["memory_current"] = sample * 1024U;
    bbp::AppendLine(dir / "metrics.jsonl", boost::json::serialize(metric));
  }

  const boost::json::object report =
      boost::json::parse(bbp::BuildRunReportJson(dir)).as_object();
  const boost::json::object& node =
      report.at("nodes_summary").as_array().front().as_object();
  BOOST_TEST(JsonInteger(node, "metric_samples") == 125U);
  const boost::json::array& history = node.at("metrics_history").as_array();
  BOOST_REQUIRE_EQUAL(history.size(), 120U);
  BOOST_TEST(JsonInteger(history.front().as_object(), "timestamp_ms") == 6000U);
  BOOST_TEST(JsonInteger(history.front().as_object(), "height") == 6U);
  BOOST_TEST(JsonInteger(history.back().as_object(), "timestamp_ms") ==
             125000U);
  BOOST_TEST(JsonInteger(history.back().as_object(), "height") == 125U);

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
                  "{\"run_id\":\"r1\",\"node_id\":\"firo-1\","
                  "\"event\":\"network_condition_updated\","
                  "\"detail\":\"{\\\"bandwidth_mbps\\\":20,"
                  "\\\"operator_command_sequence\\\":7}\"}");
  bbp::AppendLine(dir / "events.jsonl",
                  "{\"run_id\":\"r1\",\"node_id\":\"firo-1\","
                  "\"event\":\"network_block_applied\","
                  "\"detail\":\"{\\\"handle\\\":77,"
                  "\\\"present_after\\\":true}\"}");
  bbp::AppendLine(dir / "events.jsonl",
                  "{\"run_id\":\"r1\",\"node_id\":\"firo-1\","
                  "\"event\":\"network_block_removed\","
                  "\"detail\":\"{\\\"handle\\\":77,"
                  "\\\"present_after\\\":false}\"}");
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
  BOOST_TEST(JsonInteger(report, "event_count") == 7U);
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
  const boost::json::array& condition_updates =
      report.at("network_condition_updates").as_array();
  BOOST_REQUIRE_EQUAL(condition_updates.size(), 1U);
  BOOST_TEST(JsonInteger(
                 condition_updates.front().as_object().at("detail").as_object(),
                 "operator_command_sequence") == 7U);
  const boost::json::array& blocks = report.at("network_blocks").as_array();
  BOOST_REQUIRE_EQUAL(blocks.size(), 1U);
  BOOST_TEST(JsonInteger(blocks.front().as_object().at("detail").as_object(),
                         "handle") == 77U);
  const boost::json::array& unblocks = report.at("network_unblocks").as_array();
  BOOST_REQUIRE_EQUAL(unblocks.size(), 1U);
  BOOST_TEST(!unblocks.front()
                  .as_object()
                  .at("detail")
                  .as_object()
                  .at("present_after")
                  .as_bool());

  std::filesystem::remove_all(dir);
}

BOOST_AUTO_TEST_CASE(
    run_report_preserves_topology_conditions_and_policy_verification) {
  const std::filesystem::path dir = MakeTestDir("edge-condition");
  boost::json::object condition;
  condition["from"] = 1U;
  condition["to"] = 2U;
  condition["bandwidth_mbps"] = 9U;
  condition["delay_ms"] = 40U;
  condition["jitter_ms"] = 3U;
  condition["loss_basis_points"] = 5U;
  condition["duplicate_basis_points"] = 6U;
  condition["corrupt_basis_points"] = 7U;
  condition["reorder_basis_points"] = 8U;
  condition["limit_packets"] = 777U;
  boost::json::array resolved_edges;
  resolved_edges.push_back(condition);
  boost::json::object topology;
  topology["type"] = "custom_edge_list";
  topology["resolved_edges"] = std::move(resolved_edges);
  boost::json::object scenario;
  scenario["run_id"] = "r1";
  scenario["nodes"] = 2U;
  scenario["topology"] = std::move(topology);
  bbp::WriteText(dir / "resolved-scenario.json",
                 boost::json::serialize(scenario) + "\n");

  boost::json::object policy_condition = condition;
  policy_condition.erase("from");
  policy_condition.erase("to");
  boost::json::object policy;
  policy["band"] = 1U;
  policy["destination_address"] = "10.210.1.6";
  policy["condition"] = std::move(policy_condition);
  boost::json::array policies;
  policies.push_back(std::move(policy));
  boost::json::object detail;
  detail["source_node"] = 1U;
  detail["peer_if"] = "bbptest1p";
  detail["verified"] = true;
  detail["policies"] = std::move(policies);
  boost::json::object event;
  event["timestamp"] = "2026-07-16T00:00:00Z";
  event["run_id"] = "r1";
  event["node_id"] = "node-001";
  event["event"] = "directional_network_policies_verified";
  event["detail"] = boost::json::serialize(detail);
  bbp::WriteText(dir / "events.jsonl", boost::json::serialize(event) + "\n");
  bbp::WriteText(dir / "metrics.jsonl", "");

  const boost::json::object report =
      boost::json::parse(bbp::BuildRunReportJson(dir)).as_object();
  const auto& report_edges =
      report.at("topology").as_object().at("resolved_edges").as_array();
  BOOST_REQUIRE_EQUAL(report_edges.size(), 1U);
  BOOST_TEST(JsonInteger(report_edges[0].as_object(), "delay_ms") == 40U);
  const auto& verifications =
      report.at("directional_network_policy_verifications").as_array();
  BOOST_REQUIRE_EQUAL(verifications.size(), 1U);
  const auto& reported_detail =
      verifications[0].as_object().at("detail").as_object();
  BOOST_TEST(reported_detail.at("verified").as_bool());
  BOOST_TEST(
      JsonInteger(reported_detail.at("policies").as_array()[0].as_object(),
                  "band") == 1U);

  std::filesystem::remove_all(dir);
}

BOOST_AUTO_TEST_CASE(run_report_reduces_successful_topology_edge_updates) {
  const std::filesystem::path dir = MakeTestDir("topology-edge-updates");
  boost::json::object initial_first;
  initial_first["from"] = 1U;
  initial_first["to"] = 2U;
  initial_first["band"] = 1U;
  initial_first["active"] = true;
  initial_first["condition"] = nullptr;
  boost::json::object initial_second;
  initial_second["from"] = 1U;
  initial_second["to"] = 3U;
  initial_second["band"] = 2U;
  initial_second["active"] = false;
  initial_second["condition"] = nullptr;
  boost::json::array initial_edges;
  initial_edges.push_back(initial_first);
  initial_edges.push_back(initial_second);
  boost::json::object scenario;
  scenario["run_id"] = "r1";
  scenario["nodes"] = 3U;
  scenario["topology_initial_edges"] = initial_edges;
  bbp::WriteText(dir / "resolved-scenario.json",
                 boost::json::serialize(scenario) + "\n");

  bbp::AppendLine(dir / "events.jsonl",
                  "{\"run_id\":\"r1\",\"node_id\":\"sim\","
                  "\"event\":\"run_started\"}");
  boost::json::object conditioned = initial_first;
  boost::json::object condition;
  condition["bandwidth_mbps"] = 12U;
  condition["delay_ms"] = 35U;
  conditioned["condition"] = condition;
  boost::json::object condition_detail;
  condition_detail["action"] = "set_edge_condition";
  condition_detail["from"] = 1U;
  condition_detail["to"] = 2U;
  condition_detail["previous"] = initial_first;
  condition_detail["current"] = conditioned;
  condition_detail["kernel_verified"] = true;
  condition_detail["peer_verified"] = true;
  boost::json::object condition_event;
  condition_event["run_id"] = "r1";
  condition_event["node_id"] = "firo-1";
  condition_event["event"] = "topology_edge_updated";
  condition_event["detail"] = boost::json::serialize(condition_detail);
  bbp::AppendLine(dir / "events.jsonl",
                  boost::json::serialize(condition_event));

  boost::json::object inactive = conditioned;
  inactive["active"] = false;
  boost::json::object deactivate_detail;
  deactivate_detail["action"] = "deactivate_edge";
  deactivate_detail["from"] = 1U;
  deactivate_detail["to"] = 2U;
  deactivate_detail["previous"] = conditioned;
  deactivate_detail["current"] = inactive;
  deactivate_detail["kernel_verified"] = true;
  deactivate_detail["peer_verified"] = true;
  boost::json::object deactivate_event;
  deactivate_event["run_id"] = "r1";
  deactivate_event["node_id"] = "firo-1";
  deactivate_event["event"] = "topology_edge_updated";
  deactivate_event["detail"] = boost::json::serialize(deactivate_detail);
  bbp::AppendLine(dir / "events.jsonl",
                  boost::json::serialize(deactivate_event));
  bbp::AppendLine(
      dir / "events.jsonl",
      "{\"run_id\":\"r1\",\"node_id\":\"firo-1\","
      "\"event\":\"topology_edge_update_rollback_failed\","
      "\"detail\":\"{\\\"original_error\\\":\\\"rpc failed\\\"}\"}");
  bbp::AppendLine(dir / "events.jsonl",
                  "{\"run_id\":\"r1\",\"node_id\":\"sim\","
                  "\"event\":\"run_finished\"}");
  bbp::WriteText(dir / "metrics.jsonl", "");

  const boost::json::object report =
      boost::json::parse(bbp::BuildRunReportJson(dir)).as_object();
  BOOST_REQUIRE_EQUAL(report.at("topology_edge_updates").as_array().size(), 2U);
  BOOST_REQUIRE_EQUAL(
      report.at("topology_edge_rollback_failures").as_array().size(), 1U);
  const boost::json::array& current =
      report.at("topology_current_edges").as_array();
  BOOST_REQUIRE_EQUAL(current.size(), 2U);
  BOOST_TEST(JsonInteger(current[0].as_object(), "band") == 1U);
  BOOST_TEST(!current[0].as_object().at("active").as_bool());
  BOOST_TEST(JsonInteger(current[0].as_object().at("condition").as_object(),
                         "delay_ms") == 35U);
  BOOST_TEST(JsonInteger(current[1].as_object(), "band") == 2U);
  BOOST_TEST(!current[1].as_object().at("active").as_bool());

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

BOOST_AUTO_TEST_CASE(run_report_preserves_cancelled_run_status) {
  const std::filesystem::path dir = MakeTestDir("run-report-cancelled");
  bbp::AppendLine(
      dir / "events.jsonl",
      R"({"run_id":"r1","node_id":"sim","timestamp":"2026-07-19T12:00:00Z","event":"run_started"})");
  bbp::AppendLine(
      dir / "events.jsonl",
      R"({"run_id":"r1","node_id":"sim","timestamp":"2026-07-19T12:00:01Z","event":"run_cancelled"})");
  bbp::AppendLine(
      dir / "events.jsonl",
      R"({"run_id":"r1","node_id":"sim","timestamp":"2026-07-19T12:00:01Z","event":"transaction_load_completed","detail":"{\"attempted\":7,\"submitted\":1,\"rejected\":0,\"timed_out\":0,\"backpressured\":0,\"dropped\":5,\"cancelled\":1,\"failed\":0,\"propagated\":1,\"confirmed\":0,\"accounting_invariants_hold\":true}"})");
  bbp::AppendLine(
      dir / "events.jsonl",
      R"({"run_id":"r1","node_id":"sim","timestamp":"2026-07-19T12:00:02Z","event":"run_finished"})");

  boost::json::object report =
      boost::json::parse(bbp::BuildRunReportJson(dir)).as_object();
  BOOST_TEST(!report.at("ok").as_bool());
  BOOST_TEST(report.at("status").as_string() == "cancelled");
  BOOST_TEST(report.at("cancelled_at").as_string() == "2026-07-19T12:00:01Z");
  BOOST_TEST(report.at("finished_at").as_string() == "2026-07-19T12:00:02Z");
  const boost::json::array& load_summaries =
      report.at("transaction_load_summaries").as_array();
  BOOST_REQUIRE_EQUAL(load_summaries.size(), 1U);
  const boost::json::object& load_summary =
      load_summaries.front().as_object().at("detail").as_object();
  BOOST_TEST(JsonInteger(load_summary, "dropped") == 5U);
  BOOST_TEST(JsonInteger(load_summary, "cancelled") == 1U);

  bbp::AppendLine(
      dir / "events.jsonl",
      R"({"run_id":"r1","node_id":"sim","timestamp":"2026-07-19T12:00:03Z","event":"run_failed","detail":"cleanup failed"})");
  report = boost::json::parse(bbp::BuildRunReportJson(dir)).as_object();
  BOOST_TEST(!report.at("ok").as_bool());
  BOOST_TEST(report.at("status").as_string() == "failed");
  BOOST_TEST(report.at("failure").as_string() == "cleanup failed");

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

BOOST_AUTO_TEST_CASE(run_report_exposes_scheduled_event_lifecycle) {
  const std::filesystem::path dir = MakeTestDir("run-report-scheduled-events");
  bbp::WriteText(
      dir / "resolved-scenario.json",
      R"({"run_id":"r1","simulation":{"name":"scaled","seed":7,"duration":"100ms","duration_ms":100,"wall_duration_ms":50,"time_scale":2.0},"nodes":1,"events":[{"sequence":1,"at":"10ms","at_ms":10,"action":"restart_node","node":1}]})"
      "\n");
  bbp::AppendLine(dir / "events.jsonl",
                  R"({"run_id":"r1","node_id":"sim","event":"run_started"})");
  bbp::AppendLine(
      dir / "events.jsonl",
      R"({"run_id":"r1","node_id":"sim","event":"scheduled_event_started","detail":"{\"sequence\":1,\"action\":\"restart_node\",\"scheduled_at_ms\":10,\"started_at_ms\":12,\"lateness_ms\":2}"})");
  bbp::AppendLine(
      dir / "events.jsonl",
      R"({"run_id":"r1","node_id":"sim","event":"scheduled_event_completed","detail":"{\"sequence\":1,\"action\":\"restart_node\",\"duration_ms\":5}"})");
  bbp::AppendLine(
      dir / "events.jsonl",
      R"({"run_id":"r1","node_id":"sim","event":"scheduled_event_failed","detail":"{\"sequence\":2,\"action\":\"freeze_node\",\"error\":\"failed\"}"})");
  bbp::AppendLine(
      dir / "events.jsonl",
      R"({"run_id":"r1","node_id":"sim","event":"checkpoint_recorded","detail":"{\"workload_index\":1,\"workload_count\":1,\"name\":\"after-heal\",\"node_metric_samples\":1,\"wallet_metric_samples\":0,\"total_metric_samples\":1}"})");
  bbp::AppendLine(
      dir / "events.jsonl",
      R"({"run_id":"r1","node_id":"sim","event":"simulation_duration_reached","detail":"{\"duration_ms\":100,\"wall_duration_ms\":50,\"time_scale\":2.0,\"stop_requested_at_ms\":50,\"elapsed_wall_ms\":50}"})");

  const boost::json::value value =
      boost::json::parse(bbp::BuildRunReportJson(dir));
  const boost::json::object& report = value.as_object();

  BOOST_REQUIRE_EQUAL(report.at("events").as_array().size(), 1U);
  BOOST_TEST(report.at("simulation").as_object().at("name").as_string() ==
             "scaled");
  BOOST_TEST(JsonInteger(report, "scheduled_event_started_count") == 1U);
  BOOST_TEST(JsonInteger(report, "scheduled_event_completed_count") == 1U);
  BOOST_TEST(JsonInteger(report, "scheduled_event_failed_count") == 1U);
  BOOST_TEST(JsonInteger(report, "checkpoint_count") == 1U);
  BOOST_TEST(report.at("simulation_duration_reached").as_bool());
  BOOST_TEST(JsonInteger(report.at("simulation_duration").as_object(),
                         "wall_duration_ms") == 50U);
  BOOST_TEST(JsonInteger(report.at("simulation_duration").as_object(),
                         "stop_requested_at_ms") == 50U);
  const boost::json::object& checkpoint = report.at("checkpoints")
                                              .as_array()
                                              .front()
                                              .as_object()
                                              .at("detail")
                                              .as_object();
  BOOST_TEST(checkpoint.at("name").as_string() == "after-heal");
  BOOST_TEST(JsonInteger(checkpoint, "total_metric_samples") == 1U);
  const boost::json::object& started = report.at("scheduled_events_started")
                                           .as_array()
                                           .front()
                                           .as_object()
                                           .at("detail")
                                           .as_object();
  BOOST_TEST(JsonInteger(started, "sequence") == 1U);
  BOOST_TEST(JsonInteger(started, "lateness_ms") == 2U);
  BOOST_TEST(report.at("scheduled_events_failed")
                 .as_array()
                 .front()
                 .as_object()
                 .at("detail")
                 .as_object()
                 .at("error")
                 .as_string() == "failed");

  std::filesystem::remove_all(dir);
}

BOOST_AUTO_TEST_CASE(run_report_reduces_profile_updates_and_active_profiles) {
  const std::filesystem::path dir = MakeTestDir("run-report-profiles");
  bbp::WriteText(
      dir / "resolved-scenario.json",
      R"({"run_id":"r1","nodes":1,"resource_profiles":{"large":{"pids_max":512}},"network_profiles":{"degraded":{"delay_ms":200}},"events":[{"sequence":1,"at":"10ms","at_ms":10,"action":"set_resource_profile","nodes":["firo-a"],"profile":"large"},{"sequence":2,"at":"20ms","at_ms":20,"action":"set_network_profile","nodes":["firo-a"],"profile":"degraded"}]})"
      "\n");
  bbp::AppendLine(dir / "events.jsonl",
                  R"({"run_id":"r1","node_id":"sim","event":"run_started"})");
  bbp::AppendLine(
      dir / "events.jsonl",
      R"({"run_id":"r1","node_id":"firo-a","timestamp":"2026-07-16T00:00:01Z","event":"resource_profile_updated","detail":"{\"workload_index\":1,\"workload_count\":2,\"node\":1,\"profile\":\"large\",\"previous_profile\":\"small\",\"previous\":{\"pids_max\":256},\"current\":{\"pids_max\":512},\"kernel_verified\":true}"})");
  bbp::AppendLine(
      dir / "events.jsonl",
      R"({"run_id":"r1","node_id":"firo-a","timestamp":"2026-07-16T00:00:02Z","event":"network_profile_updated","detail":"{\"workload_index\":2,\"workload_count\":2,\"node\":1,\"profile\":\"degraded\",\"previous_profile\":\"normal\",\"previous\":{\"delay_ms\":20},\"current\":{\"delay_ms\":200},\"qdisc\":{\"kind\":\"tbf+netem\"},\"kernel_verified\":true}"})");
  bbp::AppendLine(
      dir / "events.jsonl",
      R"({"run_id":"r1","node_id":"sim","timestamp":"2026-07-16T00:00:03Z","event":"profile_update_rollback_failed","detail":"{\"action\":\"set_network_profile\",\"profile\":\"degraded\",\"original_error\":\"apply failed\",\"rollback_errors\":[\"firo-a: restore failed\"]}"})");
  bbp::AppendLine(
      dir / "metrics.jsonl",
      R"({"run_id":"r1","node_id":"firo-a","timestamp_ms":1000,"active_resource_profile":"large","active_network_profile":"degraded"})");

  const boost::json::value report_value =
      boost::json::parse(bbp::BuildRunReportJson(dir));
  const boost::json::object& report = report_value.as_object();
  const boost::json::object& resource_action =
      report.at("events").as_array().front().as_object();
  BOOST_TEST(resource_action.at("action").as_string() ==
             "set_resource_profile");
  BOOST_TEST(resource_action.at("profile").as_string() == "large");
  BOOST_TEST(resource_action.at("nodes").as_array().front().as_string() ==
             "firo-a");
  const boost::json::object& resource_update =
      report.at("resource_profile_updates")
          .as_array()
          .front()
          .as_object()
          .at("detail")
          .as_object();
  BOOST_TEST(resource_update.at("previous_profile").as_string() == "small");
  BOOST_TEST(resource_update.at("profile").as_string() == "large");
  BOOST_TEST(JsonInteger(resource_update.at("current").as_object(),
                         "pids_max") == 512U);
  const boost::json::object& network_update =
      report.at("network_profile_updates")
          .as_array()
          .front()
          .as_object()
          .at("detail")
          .as_object();
  BOOST_TEST(network_update.at("previous_profile").as_string() == "normal");
  BOOST_TEST(network_update.at("profile").as_string() == "degraded");
  BOOST_TEST(network_update.at("qdisc").as_object().at("kind").as_string() ==
             "tbf+netem");
  BOOST_REQUIRE_EQUAL(
      report.at("profile_update_rollback_failures").as_array().size(), 1U);
  const boost::json::object& last_metrics = report.at("nodes_summary")
                                                .as_array()
                                                .front()
                                                .as_object()
                                                .at("last_metrics")
                                                .as_object();
  BOOST_TEST(last_metrics.at("active_resource_profile").as_string() == "large");
  BOOST_TEST(last_metrics.at("active_network_profile").as_string() ==
             "degraded");

  std::filesystem::remove_all(dir);
}

BOOST_AUTO_TEST_CASE(run_report_orders_nodes_and_derives_resource_rates) {
  const std::filesystem::path dir = MakeTestDir("run-report-node-rates");
  bbp::WriteText(
      dir / "resolved-scenario.json",
      R"({"run_id":"r1","chain":"firo","nodes":10,"node_configs":[{"index":10,"id":"firo-10","chain":"firo","role":"base"},{"index":2,"id":"firo-2","chain":"firo","role":"miner"}]})"
      "\n");
  bbp::AppendLine(
      dir / "events.jsonl",
      R"({"run_id":"r1","node_id":"firo-10","event":"state","detail":"Running"})");
  bbp::AppendLine(
      dir / "events.jsonl",
      R"({"run_id":"r1","node_id":"firo-2","event":"state","detail":"Running"})");
  bbp::AppendLine(
      dir / "events.jsonl",
      R"({"run_id":"r1","node_id":"firo-2","event":"metrics_node_unavailable","detail":"{\"sample\":3,\"error\":\"RPC timeout\"}"})");
  bbp::AppendLine(
      dir / "metrics.jsonl",
      R"({"run_id":"r1","node_id":"firo-10","timestamp_ms":1000,"cpu_usage_usec":1,"io_read_bytes":1,"network_rx_bytes":1,"network_tx_bytes":1})");
  bbp::AppendLine(
      dir / "metrics.jsonl",
      R"({"run_id":"r1","node_id":"firo-2","timestamp_ms":1000,"cpu_usage_usec":100000,"cpu_throttled_usec":1000,"io_read_bytes":1000,"io_write_bytes":2000,"network_rx_bytes":3000,"network_tx_bytes":4000})");
  bbp::AppendLine(
      dir / "metrics.jsonl",
      R"({"run_id":"r1","node_id":"firo-2","timestamp_ms":3000,"cpu_usage_usec":1100000,"cpu_throttled_usec":101000,"io_read_bytes":5000,"io_write_bytes":10000,"network_rx_bytes":7000,"network_tx_bytes":10000,"network_rx_dropped":1,"network_tx_dropped":2,"qdisc_drops":3,"network_filter_drop_packets":4,"directional_network_qdisc_drops":5})");

  const boost::json::value report_value =
      boost::json::parse(bbp::BuildRunReportJson(dir));
  const boost::json::object& report = report_value.as_object();
  const boost::json::array& nodes = report.at("nodes_summary").as_array();
  BOOST_REQUIRE_EQUAL(nodes.size(), 2U);
  BOOST_TEST(nodes[0].as_object().at("node_id").as_string() == "firo-2");
  BOOST_TEST(JsonInteger(nodes[0].as_object(), "node_index") == 2U);
  BOOST_TEST(nodes[0].as_object().at("chain").as_string() == "firo");
  BOOST_TEST(nodes[0].as_object().at("role").as_string() == "miner");
  BOOST_TEST(nodes[1].as_object().at("node_id").as_string() == "firo-10");
  BOOST_TEST(JsonInteger(nodes[1].as_object(), "node_index") == 10U);
  BOOST_TEST(nodes[0].as_object().at("last_error").as_string() ==
             "metrics_node_unavailable: RPC timeout");

  const boost::json::object& metrics =
      nodes[0].as_object().at("last_metrics").as_object();
  BOOST_TEST(metrics.at("cpu_percent").as_double() == 50.0);
  BOOST_TEST(metrics.at("cpu_throttled_percent").as_double() == 5.0);
  BOOST_TEST(JsonInteger(metrics, "io_read_bytes_per_sec") == 2000U);
  BOOST_TEST(JsonInteger(metrics, "io_write_bytes_per_sec") == 4000U);
  BOOST_TEST(JsonInteger(metrics, "network_uplink_bytes_per_sec") == 2000U);
  BOOST_TEST(JsonInteger(metrics, "network_downlink_bytes_per_sec") == 3000U);
  BOOST_TEST(JsonInteger(metrics, "network_drop_count") == 15U);
  const boost::json::object& first_only_metrics =
      nodes[1].as_object().at("last_metrics").as_object();
  BOOST_TEST(first_only_metrics.at("cpu_percent").is_null());
  BOOST_TEST(first_only_metrics.at("io_read_bytes_per_sec").is_null());
  BOOST_TEST(first_only_metrics.at("network_uplink_bytes_per_sec").is_null());

  std::filesystem::remove_all(dir);
}

BOOST_AUTO_TEST_CASE(
    run_report_retains_process_exit_history_and_clears_recovered_errors) {
  const std::filesystem::path dir =
      MakeTestDir("run-report-lifecycle-exit-recovery");
  bbp::WriteText(
      dir / "resolved-scenario.json",
      R"({"run_id":"r1","chain":"firo","nodes":2,"node_configs":[{"index":1,"id":"firo-recovered","chain":"firo","role":"base"},{"index":2,"id":"firo-failed","chain":"firo","role":"base"}]})"
      "\n");
  bbp::AppendLine(
      dir / "events.jsonl",
      R"({"run_id":"r1","node_id":"firo-recovered","event":"process_exited","detail":"{\"running\":false,\"pid\":101,\"raw_status\":9,\"kind\":\"signal\",\"signal\":9}"})");
  bbp::AppendLine(
      dir / "events.jsonl",
      R"({"run_id":"r1","node_id":"firo-recovered","event":"process_exited_before_rpc_ready","detail":"{\"running\":false,\"pid\":102,\"raw_status\":768,\"kind\":\"exit\",\"exit_code\":3}"})");
  bbp::AppendLine(
      dir / "events.jsonl",
      R"({"run_id":"r1","node_id":"firo-recovered","event":"rpc_ready"})");
  bbp::AppendLine(
      dir / "events.jsonl",
      R"({"run_id":"r1","node_id":"firo-failed","event":"process_exited_before_rpc_ready","detail":"{\"running\":false,\"pid\":201,\"raw_status\":1024,\"kind\":\"exit\",\"exit_code\":4}"})");

  const boost::json::object report =
      boost::json::parse(bbp::BuildRunReportJson(dir)).as_object();
  const boost::json::array& exits = report.at("process_exits").as_array();
  BOOST_REQUIRE_EQUAL(exits.size(), 3U);
  BOOST_TEST(JsonInteger(exits[0].as_object().at("detail").as_object(),
                         "raw_status") == 9U);
  BOOST_TEST(JsonInteger(exits[1].as_object().at("detail").as_object(),
                         "raw_status") == 768U);
  BOOST_TEST(JsonInteger(exits[2].as_object().at("detail").as_object(),
                         "raw_status") == 1024U);

  const boost::json::array& nodes = report.at("nodes_summary").as_array();
  BOOST_REQUIRE_EQUAL(nodes.size(), 2U);
  BOOST_TEST(nodes[0].as_object().at("node_id").as_string() ==
             "firo-recovered");
  BOOST_TEST(nodes[0].as_object().at("last_error").is_null());
  BOOST_TEST(nodes[1].as_object().at("node_id").as_string() == "firo-failed");
  BOOST_TEST(nodes[1].as_object().at("last_error").as_string() ==
             "process_exited_before_rpc_ready: "
             "{\"running\":false,\"pid\":201,\"raw_status\":1024,"
             "\"kind\":\"exit\",\"exit_code\":4}");

  std::filesystem::remove_all(dir);
}

BOOST_AUTO_TEST_CASE(run_report_builds_topology_groups_and_node_exports) {
  const std::filesystem::path dir = MakeTestDir("run-report-topology-groups");
  bbp::WriteText(
      dir / "resolved-scenario.json",
      R"({"run_id":"r1","chain":"firo","nodes":3,"topology":{"type":"partitioned_groups","groups":[[1,2],[3]]},"topology_initial_edges":[{"from":1,"to":2,"band":1,"active":true,"condition":{"bandwidth_mbps":5,"delay_ms":10}},{"from":2,"to":1,"band":1,"active":true,"condition":null}],"node_configs":[{"index":1,"id":"firo-1","chain":"firo","role":"miner"},{"index":2,"id":"firo-2","chain":"firo","role":"base"},{"index":3,"id":"firo-3","chain":"firo","role":"wallet"}]})"
      "\n");
  for (const std::string_view node_id : {"firo-1", "firo-2", "firo-3"}) {
    bbp::AppendLine(dir / "events.jsonl",
                    "{\"run_id\":\"r1\",\"node_id\":\"" + std::string(node_id) +
                        "\",\"event\":\"state\",\"detail\":\"Running\"}");
  }
  bbp::AppendLine(
      dir / "events.jsonl",
      R"({"run_id":"r1","node_id":"sim","event":"network_partition_applied","detail":"{\"group_a\":[1],\"group_b\":[3],\"rules\":[]}"})");
  bbp::AppendLine(
      dir / "events.jsonl",
      R"({"run_id":"r1","node_id":"sim","event":"network_partition_healed","detail":"{\"group_a\":[3],\"group_b\":[1],\"rules\":[]}"})");
  bbp::AppendLine(
      dir / "events.jsonl",
      R"({"run_id":"r1","node_id":"sim","event":"network_partition_applied","detail":"{\"group_a\":[2],\"group_b\":[3],\"rules\":[]}"})");
  bbp::AppendLine(
      dir / "metrics.jsonl",
      R"({"run_id":"r1","node_id":"firo-1","timestamp_ms":1000,"network_rx_bytes":100,"network_tx_bytes":200})");
  bbp::AppendLine(
      dir / "metrics.jsonl",
      R"({"run_id":"r1","node_id":"firo-1","timestamp_ms":2000,"network_rx_bytes":300,"network_tx_bytes":600,"network_condition":{"bandwidth_mbps":0,"delay_ms":80},"network_active_block_rules":[{"handle":77,"dst_address":"10.0.0.2","dst_port":18168}],"perf_counter_target_kind":"group","perf_counter_target_id":"topology-1","perf_counters_available":true,"perf_counter_names":["cycles"],"perf_counters":[{"name":"cycles","raw_value":18446744073709551610,"scaled_value":18446744073709551613,"time_enabled_ns":18446744073709551614,"time_running_ns":100,"multiplexed":true,"scaled":true,"scaled_overflow":false}]})");
  bbp::AppendLine(
      dir / "metrics.jsonl",
      R"({"run_id":"r1","node_id":"firo-2","timestamp_ms":1000,"network_rx_bytes":50,"network_tx_bytes":100})");
  bbp::AppendLine(
      dir / "metrics.jsonl",
      R"({"run_id":"r1","node_id":"firo-2","timestamp_ms":2000,"network_rx_bytes":150,"network_tx_bytes":300,"perf_counter_target_kind":"group","perf_counter_target_id":"topology-1","perf_counters_available":true,"perf_counter_names":["cycles"],"perf_counters":[{"name":"cycles","raw_value":10,"scaled_value":10,"time_enabled_ns":10,"time_running_ns":20,"multiplexed":false,"scaled":false,"scaled_overflow":false}]})");
  bbp::AppendLine(
      dir / "metrics.jsonl",
      R"({"run_id":"r1","node_id":"firo-3","timestamp_ms":1000,"network_rx_bytes":10,"network_tx_bytes":20})");
  bbp::AppendLine(
      dir / "metrics.jsonl",
      R"({"run_id":"r1","node_id":"firo-3","timestamp_ms":2000,"network_rx_bytes":20,"network_tx_bytes":40})");

  const boost::json::object report =
      boost::json::parse(bbp::BuildRunReportJson(dir)).as_object();
  BOOST_REQUIRE_EQUAL(report.at("active_network_partitions").as_array().size(),
                      1U);
  BOOST_REQUIRE_EQUAL(report.at("topology_degraded_links").as_array().size(),
                      2U);
  BOOST_REQUIRE_EQUAL(report.at("topology_blocked_rules").as_array().size(),
                      1U);
  const boost::json::array& groups =
      report.at("topology_groups_summary").as_array();
  BOOST_REQUIRE_EQUAL(groups.size(), 6U);
  BOOST_TEST(groups[0].as_object().at("group").as_string() == "all");
  BOOST_TEST(JsonInteger(groups[0].as_object(), "active_partition_count") ==
             1U);
  BOOST_TEST(JsonInteger(groups[0].as_object(), "degraded_link_count") == 2U);
  BOOST_TEST(JsonInteger(groups[0].as_object(), "blocked_rule_count") == 1U);
  const boost::json::object& topology_one = groups[1].as_object();
  BOOST_TEST(topology_one.at("group").as_string() == "topology-1");
  BOOST_TEST(JsonInteger(topology_one, "node_count") == 2U);
  BOOST_TEST(JsonInteger(topology_one, "network_downlink_bytes") == 900U);
  BOOST_TEST(JsonInteger(topology_one, "network_uplink_bytes") == 450U);
  BOOST_TEST(JsonInteger(topology_one, "network_downlink_bytes_per_sec") ==
             600U);
  BOOST_TEST(JsonInteger(topology_one, "network_uplink_bytes_per_sec") == 300U);
  BOOST_TEST(JsonInteger(topology_one, "active_partition_count") == 1U);
  BOOST_TEST(JsonInteger(topology_one, "degraded_link_count") == 2U);
  BOOST_TEST(JsonInteger(topology_one, "blocked_rule_count") == 1U);
  BOOST_TEST(topology_one.at("perf_counters_available").as_bool());
  BOOST_REQUIRE_EQUAL(topology_one.at("perf_counter_names").as_array().size(),
                      1U);
  const boost::json::object& group_counter =
      topology_one.at("perf_counters").as_array().front().as_object();
  BOOST_TEST(JsonInteger(group_counter, "raw_value") ==
             std::numeric_limits<std::uint64_t>::max());
  BOOST_TEST(JsonInteger(group_counter, "scaled_value") ==
             std::numeric_limits<std::uint64_t>::max());
  BOOST_TEST(JsonInteger(group_counter, "time_enabled_ns") ==
             std::numeric_limits<std::uint64_t>::max());
  BOOST_TEST(JsonInteger(group_counter, "time_running_ns") == 120U);
  BOOST_TEST(group_counter.at("multiplexed").as_bool());
  BOOST_TEST(group_counter.at("scaled").as_bool());
  BOOST_TEST(group_counter.at("scaled_overflow").as_bool());
  BOOST_TEST(group_counter.at("aggregation_overflow").as_bool());
  BOOST_TEST(topology_one.at("perf_counter_aggregation_overflow").as_bool());

  const boost::json::object node_report =
      boost::json::parse(bbp::BuildNodeReportJson(dir, "firo-2", 9U))
          .as_object();
  BOOST_TEST(JsonInteger(node_report, "operator_command_sequence") == 9U);
  BOOST_REQUIRE(node_report.at("event_counts").is_object());
  BOOST_TEST(JsonInteger(node_report.at("event_counts").as_object(), "state") ==
             3U);
  BOOST_TEST(node_report.at("node").as_object().at("node_id").as_string() ==
             "firo-2");
  BOOST_REQUIRE_EQUAL(
      node_report.at("topology_groups_summary").as_array().size(), 6U);
  BOOST_CHECK_THROW(bbp::BuildNodeReportJson(dir, "missing", 10U),
                    std::runtime_error);

  std::filesystem::remove_all(dir);
}

BOOST_AUTO_TEST_CASE(
    run_report_aggregates_distributed_wallet_transaction_amounts) {
  const std::filesystem::path dir =
      MakeTestDir("run-report-wallet-distributions");
  bbp::WriteText(
      dir / "resolved-scenario.json",
      R"({"run_id":"r1","nodes":2,"workloads":[{"type":"wallet_transactions","strategy":"fanout","sender_wallets":[1],"transaction_count":2,"amount":{"distribution":"uniform","min":"0.00000011","max":"0.00000019"},"interval":{"distribution":"uniform","min":"5ms","max":"9ms"},"fee":"0.00000001","seed":7}]})"
      "\n");
  bbp::AppendLine(
      dir / "events.jsonl",
      R"({"run_id":"r1","node_id":"firo-1","event":"wallet_address_created","detail":"{\"wallet_index\":1,\"node\":1,\"strategy\":\"driver_rpc\",\"mode\":\"public\",\"address\":\"addr1\"}"})");
  bbp::AppendLine(
      dir / "events.jsonl",
      R"({"run_id":"r1","node_id":"firo-2","event":"wallet_address_created","detail":"{\"wallet_index\":2,\"node\":2,\"strategy\":\"driver_rpc\",\"mode\":\"public\",\"address\":\"addr2\"}"})");
  bbp::AppendLine(
      dir / "events.jsonl",
      R"({"run_id":"r1","node_id":"firo-1","event":"wallet_transaction_submitted","detail":"{\"transaction_index\":1,\"transaction_count\":2,\"strategy\":\"fanout\",\"seed\":7,\"sender_wallet_index\":1,\"receiver_wallet_index\":2,\"sender_node\":1,\"receiver_node\":2,\"amount_satoshis\":11,\"interval_before_ms\":0,\"amount_distribution\":{\"distribution\":\"uniform\",\"min_satoshis\":11,\"max_satoshis\":19}}"})");
  bbp::AppendLine(
      dir / "events.jsonl",
      R"({"run_id":"r1","node_id":"firo-1","event":"wallet_transaction_submitted","detail":"{\"transaction_index\":2,\"transaction_count\":2,\"strategy\":\"fanout\",\"seed\":7,\"sender_wallet_index\":1,\"receiver_wallet_index\":2,\"sender_node\":1,\"receiver_node\":2,\"amount_satoshis\":19,\"interval_before_ms\":7,\"amount_distribution\":{\"distribution\":\"uniform\",\"min_satoshis\":11,\"max_satoshis\":19}}"})");

  const boost::json::value report_value =
      boost::json::parse(bbp::BuildRunReportJson(dir));
  const boost::json::object& report = report_value.as_object();
  const boost::json::array& transactions =
      report.at("wallet_transactions").as_array();
  BOOST_REQUIRE_EQUAL(transactions.size(), 2U);
  const boost::json::object& second =
      transactions[1].as_object().at("detail").as_object();
  BOOST_TEST(second.at("strategy").as_string() == "fanout");
  BOOST_TEST(JsonInteger(second, "amount_satoshis") == 19U);
  BOOST_TEST(JsonInteger(second, "interval_before_ms") == 7U);
  BOOST_TEST(second.at("amount_distribution")
                 .as_object()
                 .at("distribution")
                 .as_string() == "uniform");

  const boost::json::array& wallets = report.at("wallets_summary").as_array();
  BOOST_REQUIRE_EQUAL(wallets.size(), 2U);
  BOOST_TEST(JsonInteger(wallets[0].as_object(), "transactions_sent") == 2U);
  BOOST_TEST(JsonInteger(wallets[0].as_object(),
                         "simulated_amount_sent_satoshis") == 30U);
  BOOST_TEST(JsonInteger(wallets[1].as_object(), "transactions_received") ==
             2U);
  BOOST_TEST(JsonInteger(wallets[1].as_object(),
                         "simulated_amount_received_satoshis") == 30U);

  const boost::json::object& workload =
      report.at("workloads").as_array().front().as_object();
  BOOST_TEST(workload.at("strategy").as_string() == "fanout");
  BOOST_TEST(
      JsonIntegerValue(workload.at("sender_wallets").as_array().front()) == 1U);
  BOOST_TEST(workload.at("amount").as_object().at("distribution").as_string() ==
             "uniform");
  BOOST_TEST(workload.at("interval").as_object().at("max").as_string() ==
             "9ms");

  std::filesystem::remove_all(dir);
}

BOOST_AUTO_TEST_CASE(
    run_report_aggregates_operator_wallet_send_identity_and_amounts) {
  const std::filesystem::path dir =
      MakeTestDir("run-report-operator-wallet-send");
  bbp::WriteText(
      dir / "resolved-scenario.json",
      R"({"run_id":"r1","chain":"firo","nodes":2,"node_configs":[{"index":1,"id":"firo-wallet-a","chain":"firo","role":"wallet"},{"index":2,"id":"firo-wallet-b","chain":"firo","role":"wallet"}]})"
      "\n");
  const auto append_event = [&](std::string_view node_id,
                                std::string_view event_name,
                                const boost::json::object& detail) {
    boost::json::object event;
    event["run_id"] = "r1";
    event["node_id"] = node_id;
    event["event"] = event_name;
    event["detail"] = boost::json::serialize(detail);
    bbp::AppendLine(dir / "events.jsonl", boost::json::serialize(event));
  };

  bbp::AppendLine(dir / "events.jsonl",
                  R"({"run_id":"r1","node_id":"sim","event":"run_started"})");
  append_event("firo-wallet-a", "wallet_address_created",
               {{"wallet_index", 1U},
                {"node", 1U},
                {"strategy", "driver_rpc"},
                {"mode", "public"},
                {"address", "addr1"}});
  append_event("firo-wallet-b", "wallet_address_created",
               {{"wallet_index", 2U},
                {"node", 2U},
                {"strategy", "driver_rpc"},
                {"mode", "public"},
                {"address", "addr2"}});

  boost::json::object wallet_send;
  wallet_send["sender_wallet_index"] = 1U;
  wallet_send["receiver_wallet_index"] = 2U;
  wallet_send["amount"] = "0.10000000";
  wallet_send["amount_satoshis"] = 10000000U;
  wallet_send["fee"] = "0.00001000";
  wallet_send["fee_satoshis"] = 1000U;
  wallet_send["timeout_sec"] = 45U;
  boost::json::object command_detail;
  command_detail["sequence"] = 1U;
  command_detail["kind"] = "send_wallet_transaction";
  command_detail["wallet_send"] = wallet_send;
  command_detail["confirmed"] = true;
  append_event("firo-wallet-a", "operator_command_started", command_detail);

  boost::json::object transaction_detail;
  transaction_detail["submission_kind"] = "operator_wallet_send";
  transaction_detail["operator_command_sequence"] = 1U;
  transaction_detail["sender_wallet_index"] = 1U;
  transaction_detail["receiver_wallet_index"] = 2U;
  transaction_detail["sender_node"] = 1U;
  transaction_detail["receiver_node"] = 2U;
  transaction_detail["sender_address"] = "addr1";
  transaction_detail["receiver_address"] = "addr2";
  transaction_detail["amount"] = "0.10000000";
  transaction_detail["amount_satoshis"] = 10000000U;
  transaction_detail["fee"] = "0.00001000";
  transaction_detail["fee_satoshis"] = 1000U;
  transaction_detail["timeout_sec"] = 45U;
  transaction_detail["txids"] = boost::json::array{"operator-txid"};
  append_event("firo-wallet-a", "wallet_transaction_submitted",
               transaction_detail);
  append_event("firo-wallet-a", "operator_command_completed", command_detail);
  bbp::AppendLine(dir / "events.jsonl",
                  R"({"run_id":"r1","node_id":"sim","event":"run_finished"})");

  const boost::json::object report =
      boost::json::parse(bbp::BuildRunReportJson(dir)).as_object();
  BOOST_TEST(report.at("ok").as_bool());
  const boost::json::array& wallets = report.at("wallets_summary").as_array();
  BOOST_REQUIRE_EQUAL(wallets.size(), 2U);
  BOOST_TEST(JsonInteger(wallets[0].as_object(), "wallet_index") == 1U);
  BOOST_TEST(JsonInteger(wallets[0].as_object(), "transactions_sent") == 1U);
  BOOST_TEST(JsonInteger(wallets[0].as_object(),
                         "simulated_amount_sent_satoshis") == 10000000U);
  BOOST_TEST(JsonInteger(wallets[1].as_object(), "wallet_index") == 2U);
  BOOST_TEST(JsonInteger(wallets[1].as_object(), "transactions_received") ==
             1U);
  BOOST_TEST(JsonInteger(wallets[1].as_object(),
                         "simulated_amount_received_satoshis") == 10000000U);

  const boost::json::array& transactions =
      report.at("wallet_transactions").as_array();
  BOOST_REQUIRE_EQUAL(transactions.size(), 1U);
  const boost::json::object& submitted =
      transactions.front().as_object().at("detail").as_object();
  BOOST_TEST(submitted.at("submission_kind").as_string() ==
             "operator_wallet_send");
  BOOST_TEST(JsonInteger(submitted, "operator_command_sequence") == 1U);
  BOOST_TEST(JsonInteger(submitted, "sender_wallet_index") == 1U);
  BOOST_TEST(JsonInteger(submitted, "receiver_wallet_index") == 2U);
  BOOST_TEST(JsonInteger(submitted, "amount_satoshis") == 10000000U);
  BOOST_TEST(submitted.at("txids").as_array().front().as_string() ==
             "operator-txid");

  const boost::json::array& commands =
      report.at("operator_commands").as_array();
  BOOST_REQUIRE_EQUAL(commands.size(), 1U);
  const boost::json::object& command = commands.front().as_object();
  BOOST_TEST(command.at("status").as_string() == "completed");
  const boost::json::object& stored_detail = command.at("detail").as_object();
  BOOST_TEST(stored_detail.at("kind").as_string() == "send_wallet_transaction");
  BOOST_TEST(JsonInteger(stored_detail, "sequence") == 1U);
  const boost::json::object& stored_send =
      stored_detail.at("wallet_send").as_object();
  BOOST_TEST(JsonInteger(stored_send, "sender_wallet_index") == 1U);
  BOOST_TEST(JsonInteger(stored_send, "receiver_wallet_index") == 2U);
  BOOST_TEST(JsonInteger(stored_send, "amount_satoshis") == 10000000U);
  BOOST_TEST(JsonInteger(stored_send, "fee_satoshis") == 1000U);
  BOOST_TEST(JsonInteger(stored_send, "timeout_sec") == 45U);
  BOOST_TEST(stored_detail.at("confirmed").as_bool());

  std::filesystem::remove_all(dir);
}

BOOST_AUTO_TEST_CASE(
    incremental_run_report_bounds_refresh_work_and_matches_full_report) {
  const std::filesystem::path dir = MakeTestDir("run-report-incremental");
  bbp::WriteText(
      dir / "resolved-scenario.json",
      R"({"run_id":"incremental","chain":"firo","nodes":1,"node_configs":[{"index":1,"id":"firo-1","chain":"firo","role":"wallet"}]})");

  constexpr std::uint64_t kInitialRecords = 1024U;
  for (std::uint64_t index = 0; index < kInitialRecords; ++index) {
    bbp::AppendLine(
        dir / "events.jsonl",
        R"({"run_id":"incremental","node_id":"sim","event":"unrecognized_test_event","detail":")" +
            std::to_string(index) + R"("})");
    bbp::AppendLine(
        dir / "metrics.jsonl",
        R"({"run_id":"incremental","node_id":"firo-1","node_index":1,"chain":"firo","role":"wallet","timestamp_ms":)" +
            std::to_string(1000U + index) + R"(,"cpu_usage_usec":)" +
            std::to_string(index) + R"(,"network_rx_bytes":)" +
            std::to_string(index * 2U) + R"(,"network_tx_bytes":)" +
            std::to_string(index * 3U) + "}");
  }
  bbp::AppendLine(
      dir / "wallet-metrics.jsonl",
      R"({"run_id":"incremental","wallet_index":1,"node":1,"mode":"public","available_balance_satoshis":100})");

  bbp::IncrementalRunReport incremental(dir);
  constexpr std::size_t kBatchSize = 64U;
  const boost::json::object* report = nullptr;
  std::uint64_t refresh_count = 0U;
  do {
    report = &incremental.Refresh(kBatchSize);
    const bbp::RunReportRefreshStats& stats = incremental.last_refresh_stats();
    BOOST_TEST(stats.event_records <= kBatchSize);
    BOOST_TEST(stats.metric_records <= kBatchSize);
    BOOST_TEST(stats.wallet_metric_records <= kBatchSize);
    ++refresh_count;
  } while (incremental.last_refresh_stats().has_backlog);

  BOOST_REQUIRE(report != nullptr);
  BOOST_TEST(refresh_count == kInitialRecords / kBatchSize);
  BOOST_TEST(JsonInteger(*report, "event_count") == kInitialRecords);
  BOOST_TEST(JsonInteger(*report, "metric_count") == kInitialRecords);

  bbp::AppendLine(
      dir / "events.jsonl",
      R"({"run_id":"incremental","node_id":"sim","event":"run_finished","timestamp":"2026-07-17T12:00:00Z"})");
  bbp::AppendLine(
      dir / "metrics.jsonl",
      R"({"run_id":"incremental","node_id":"firo-1","node_index":1,"chain":"firo","role":"wallet","timestamp_ms":3000,"cpu_usage_usec":2000,"network_rx_bytes":4000,"network_tx_bytes":6000})");
  bbp::AppendLine(
      dir / "wallet-metrics.jsonl",
      R"({"run_id":"incremental","wallet_index":1,"node":1,"mode":"public","available_balance_satoshis":200})");

  const boost::json::object& updated = incremental.Refresh(kBatchSize);
  const bbp::RunReportRefreshStats& update_stats =
      incremental.last_refresh_stats();
  BOOST_TEST(update_stats.event_records == 1U);
  BOOST_TEST(update_stats.metric_records == 1U);
  BOOST_TEST(update_stats.wallet_metric_records == 1U);
  BOOST_TEST(!update_stats.has_backlog);
  BOOST_TEST(JsonInteger(updated, "event_count") == kInitialRecords + 1U);
  BOOST_TEST(JsonInteger(updated, "metric_count") == kInitialRecords + 1U);

  const boost::json::object full = bbp::BuildRunReport(dir);
  BOOST_TEST(updated == full);
  std::filesystem::remove_all(dir);
}

BOOST_AUTO_TEST_CASE(
    run_report_bounds_transaction_load_details_and_preserves_totals) {
  const std::filesystem::path dir = MakeTestDir("run-report-transaction-load");
  bbp::WriteText(dir / "resolved-scenario.json",
                 R"({"run_id":"load","chain":"firo","nodes":2})");

  for (std::uint64_t index = 1U; index <= 300U; ++index) {
    boost::json::object attempt_detail;
    attempt_detail["transaction_index"] = index;
    attempt_detail["outcome"] = "backpressured";
    attempt_detail["dropped"] = true;
    boost::json::object attempt_event;
    attempt_event["run_id"] = "load";
    attempt_event["node_id"] = "firo-1";
    attempt_event["event"] = "transaction_load_attempt";
    attempt_event["detail"] = boost::json::serialize(attempt_detail);
    bbp::AppendLine(dir / "events.jsonl",
                    boost::json::serialize(attempt_event));

    boost::json::object transaction_detail;
    transaction_detail["transaction_index"] = index;
    transaction_detail["sender_wallet_index"] = 1U;
    transaction_detail["receiver_wallet_index"] = 2U;
    transaction_detail["amount_satoshis"] = 1U;
    boost::json::object transaction_event;
    transaction_event["run_id"] = "load";
    transaction_event["node_id"] = "firo-1";
    transaction_event["event"] = "wallet_transaction_submitted";
    transaction_event["detail"] = boost::json::serialize(transaction_detail);
    bbp::AppendLine(dir / "events.jsonl",
                    boost::json::serialize(transaction_event));
  }

  boost::json::object summary_detail;
  summary_detail["attempted"] = 300U;
  summary_detail["submitted"] = 0U;
  summary_detail["rejected"] = 0U;
  summary_detail["timed_out"] = 0U;
  summary_detail["backpressured"] = 300U;
  summary_detail["dropped"] = 300U;
  summary_detail["cancelled"] = 0U;
  summary_detail["propagated"] = 0U;
  summary_detail["confirmed"] = 0U;
  summary_detail["failed"] = 0U;
  summary_detail["accounting_invariants_hold"] = true;
  boost::json::object summary_event;
  summary_event["run_id"] = "load";
  summary_event["node_id"] = "sim";
  summary_event["event"] = "transaction_load_completed";
  summary_event["detail"] = boost::json::serialize(summary_detail);
  bbp::AppendLine(dir / "events.jsonl", boost::json::serialize(summary_event));

  const boost::json::object report = bbp::BuildRunReport(dir);
  BOOST_TEST(JsonInteger(report, "transaction_load_attempt_count") == 300U);
  BOOST_TEST(JsonInteger(report, "wallet_transaction_count") == 300U);
  BOOST_TEST(JsonInteger(report, "transaction_load_completed_count") == 1U);
  const boost::json::array& attempts =
      report.at("transaction_load_attempts").as_array();
  const boost::json::array& transactions =
      report.at("wallet_transactions").as_array();
  BOOST_REQUIRE_EQUAL(attempts.size(), 256U);
  BOOST_REQUIRE_EQUAL(transactions.size(), 256U);
  BOOST_TEST(JsonInteger(attempts.front().as_object().at("detail").as_object(),
                         "transaction_index") == 45U);
  BOOST_TEST(
      JsonInteger(transactions.front().as_object().at("detail").as_object(),
                  "transaction_index") == 45U);
  const boost::json::array& summaries =
      report.at("transaction_load_summaries").as_array();
  BOOST_REQUIRE_EQUAL(summaries.size(), 1U);
  const boost::json::object& stored_summary =
      summaries.front().as_object().at("detail").as_object();
  BOOST_TEST(JsonInteger(stored_summary, "attempted") == 300U);
  BOOST_TEST(JsonInteger(stored_summary, "backpressured") == 300U);
  BOOST_TEST(stored_summary.at("accounting_invariants_hold").as_bool());
  std::filesystem::remove_all(dir);
}

BOOST_AUTO_TEST_CASE(
    run_report_exposes_exact_active_one_node_load_without_completion) {
  const std::filesystem::path dir = MakeTestDir("run-report-live-load");
  bbp::WriteText(dir / "resolved-scenario.json",
                 R"({"run_id":"load","chain":"firo","nodes":1})");

  boost::json::object unrelated_visibility;
  unrelated_visibility["workload_index"] = 2U;
  unrelated_visibility["submission_kind"] = "raw_transaction_submitted";
  unrelated_visibility["txid"] = "not-a-load";
  AppendDetailEvent(dir, "transaction_visible", unrelated_visibility, "firo-1");

  constexpr std::string_view kOutcomes[] = {
      "submitted",     "submitted", "rejected", "timed_out",
      "backpressured", "dropped",   "failed",   "cancelled",
  };
  for (std::uint64_t index = 0U; index < std::size(kOutcomes); ++index) {
    boost::json::object detail;
    detail["workload_index"] = 1U;
    detail["workload_count"] = 1U;
    detail["transaction_index"] = index + 1U;
    detail["outcome"] = kOutcomes[index];
    AppendDetailEvent(dir, "transaction_load_attempt", detail, "firo-1");
  }
  for (std::uint64_t index = 1U; index <= 2U; ++index) {
    boost::json::object detail;
    detail["workload_index"] = 1U;
    detail["workload_count"] = 1U;
    detail["transaction_index"] = index;
    detail["txid"] = "tx-" + std::to_string(index);
    AppendDetailEvent(dir, "transaction_visible", detail, "firo-1");
  }
  boost::json::object confirmed_detail;
  confirmed_detail["workload_index"] = 1U;
  confirmed_detail["workload_count"] = 1U;
  confirmed_detail["transaction_index"] = 1U;
  confirmed_detail["txid"] = "tx-1";
  AppendDetailEvent(dir, "transaction_confirmed", confirmed_detail, "firo-1");

  const boost::json::object report = bbp::BuildRunReport(dir);
  const boost::json::array& loads =
      report.at("transaction_load_live").as_array();
  BOOST_REQUIRE_EQUAL(loads.size(), 1U);
  const boost::json::object& load = loads.front().as_object();
  BOOST_TEST(JsonInteger(load, "workload_index") == 1U);
  BOOST_TEST(JsonInteger(load, "attempted") == 8U);
  BOOST_TEST(JsonInteger(load, "submitted") == 2U);
  BOOST_TEST(JsonInteger(load, "rejected") == 1U);
  BOOST_TEST(JsonInteger(load, "timed_out") == 1U);
  BOOST_TEST(JsonInteger(load, "backpressured") == 1U);
  BOOST_TEST(JsonInteger(load, "dropped") == 1U);
  BOOST_TEST(JsonInteger(load, "propagated") == 2U);
  BOOST_TEST(JsonInteger(load, "confirmed") == 1U);
  BOOST_TEST(JsonInteger(load, "failed") == 1U);
  BOOST_TEST(JsonInteger(load, "cancelled") == 1U);
  BOOST_TEST(!load.at("authoritative").as_bool());
  BOOST_TEST(!load.at("completed").as_bool());
  BOOST_TEST(report.at("transaction_load_summaries").as_array().empty());
  std::filesystem::remove_all(dir);
}

BOOST_AUTO_TEST_CASE(
    run_report_keeps_exact_revisioned_load_after_attempt_detail_rollover) {
  const std::filesystem::path dir = MakeTestDir("run-report-load-rollover");
  bbp::WriteText(dir / "resolved-scenario.json",
                 R"({"run_id":"load","chain":"firo","nodes":2})");
  constexpr std::uint64_t kAttempts = 300U;
  for (std::uint64_t index = 1U; index <= kAttempts; ++index) {
    AppendDetailEvent(
        dir, "transaction_load_progress",
        TransactionLoadProgressDetail(1U, index, index, 0U, 0U, 0U, index));
    boost::json::object attempt;
    attempt["workload_index"] = 1U;
    attempt["workload_count"] = 1U;
    attempt["transaction_index"] = index;
    attempt["outcome"] = "backpressured";
    AppendDetailEvent(dir, "transaction_load_attempt", attempt, "firo-1");
  }

  const boost::json::object report = bbp::BuildRunReport(dir);
  BOOST_TEST(report.at("transaction_load_attempts").as_array().size() ==
             bbp::kMaximumRunReportSummaryRecords);
  const boost::json::array& loads =
      report.at("transaction_load_live").as_array();
  BOOST_REQUIRE_EQUAL(loads.size(), 1U);
  const boost::json::object& load = loads.front().as_object();
  BOOST_TEST(JsonInteger(load, "revision") == kAttempts);
  BOOST_TEST(JsonInteger(load, "attempted") == kAttempts);
  BOOST_TEST(JsonInteger(load, "backpressured") == kAttempts);
  BOOST_TEST(load.at("authoritative").as_bool());
  BOOST_TEST(!load.at("completed").as_bool());
  std::filesystem::remove_all(dir);
}

BOOST_AUTO_TEST_CASE(
    run_report_attributes_loads_rejects_stale_progress_and_reconciles_completion) {
  const std::filesystem::path dir = MakeTestDir("run-report-load-attribution");
  bbp::WriteText(dir / "resolved-scenario.json",
                 R"({"run_id":"load","chain":"firo","nodes":2})");
  AppendDetailEvent(dir, "transaction_load_progress",
                    TransactionLoadProgressDetail(1U, 2U, 2U, 1U, 1U));
  AppendDetailEvent(dir, "transaction_load_progress",
                    TransactionLoadProgressDetail(2U, 1U, 1U, 0U, 0U, 0U, 0U,
                                                  0U, 0U, 0U, 1U));
  AppendDetailEvent(dir, "transaction_load_progress",
                    TransactionLoadProgressDetail(1U, 1U, 1U, 1U));
  AppendDetailEvent(dir, "transaction_load_completed",
                    TransactionLoadProgressDetail(1U, 2U, 2U, 1U, 1U));

  const boost::json::object report = bbp::BuildRunReport(dir);
  const boost::json::array& loads =
      report.at("transaction_load_live").as_array();
  BOOST_REQUIRE_EQUAL(loads.size(), 2U);
  const boost::json::object& workload_two = loads.front().as_object();
  const boost::json::object& workload_one = loads.back().as_object();
  BOOST_TEST(JsonInteger(workload_two, "workload_index") == 2U);
  BOOST_TEST(JsonInteger(workload_two, "failed") == 1U);
  BOOST_TEST(!workload_two.at("completed").as_bool());
  BOOST_TEST(JsonInteger(workload_one, "workload_index") == 1U);
  BOOST_TEST(JsonInteger(workload_one, "revision") == 2U);
  BOOST_TEST(JsonInteger(workload_one, "attempted") == 2U);
  BOOST_TEST(JsonInteger(workload_one, "submitted") == 1U);
  BOOST_TEST(JsonInteger(workload_one, "rejected") == 1U);
  BOOST_TEST(workload_one.at("completed").as_bool());
  BOOST_TEST(JsonInteger(report, "transaction_load_completed_count") == 1U);
  std::filesystem::remove_all(dir);
}

BOOST_AUTO_TEST_CASE(
    incremental_load_progress_is_bounded_responsive_and_matches_full_report) {
  const std::filesystem::path dir = MakeTestDir("run-report-load-backlog");
  bbp::WriteText(dir / "resolved-scenario.json",
                 R"({"run_id":"load","chain":"firo","nodes":2})");
  constexpr std::uint64_t kProgressEvents = 1024U;
  for (std::uint64_t revision = 1U; revision <= kProgressEvents; ++revision) {
    AppendDetailEvent(dir, "transaction_load_progress",
                      TransactionLoadProgressDetail(1U, revision, revision, 0U,
                                                    0U, 0U, revision));
  }

  bbp::IncrementalRunReport incremental(dir);
  const auto started = std::chrono::steady_clock::now();
  const boost::json::object* incremental_report = nullptr;
  std::uint64_t refreshes = 0U;
  do {
    const auto refresh_started = std::chrono::steady_clock::now();
    incremental_report = &incremental.Refresh(256U);
    const auto refresh_us =
        std::chrono::duration_cast<std::chrono::microseconds>(
            std::chrono::steady_clock::now() - refresh_started);
    BOOST_TEST(refresh_us.count() < 1'000'000);
    BOOST_TEST(incremental.last_refresh_stats().event_records <= 256U);
    ++refreshes;
  } while (incremental.last_refresh_stats().has_backlog);
  const auto total_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
      std::chrono::steady_clock::now() - started);
  BOOST_TEST(total_ms.count() < 5'000);
  BOOST_TEST(refreshes == 4U);
  BOOST_REQUIRE(incremental_report != nullptr);
  const boost::json::object exhaustive = bbp::BuildRunReport(dir);
  BOOST_TEST(*incremental_report == exhaustive);
  const boost::json::object& load =
      incremental_report->at("transaction_load_live")
          .as_array()
          .front()
          .as_object();
  BOOST_TEST(JsonInteger(load, "revision") == kProgressEvents);
  BOOST_TEST(JsonInteger(load, "attempted") == kProgressEvents);
  std::filesystem::remove_all(dir);
}

BOOST_AUTO_TEST_CASE(run_report_bounds_live_transaction_load_workloads) {
  const std::filesystem::path dir = MakeTestDir("run-report-load-bound");
  bbp::WriteText(dir / "resolved-scenario.json",
                 R"({"run_id":"load","chain":"firo","nodes":1})");
  constexpr std::uint64_t kWorkloads = 300U;
  for (std::uint64_t workload = 1U; workload <= kWorkloads; ++workload) {
    AppendDetailEvent(dir, "transaction_load_progress",
                      TransactionLoadProgressDetail(workload, 1U, 1U, 0U, 0U,
                                                    0U, 0U, 0U, 0U, 0U, 1U));
  }
  const boost::json::object report = bbp::BuildRunReport(dir);
  const boost::json::array& loads =
      report.at("transaction_load_live").as_array();
  BOOST_REQUIRE_EQUAL(loads.size(), bbp::kMaximumRunReportSummaryRecords);
  BOOST_TEST(JsonInteger(loads.front().as_object(), "workload_index") == 45U);
  BOOST_TEST(JsonInteger(loads.back().as_object(), "workload_index") ==
             kWorkloads);
  std::filesystem::remove_all(dir);
}

BOOST_AUTO_TEST_CASE(
    run_report_preserves_equal_fanout_caps_and_per_transfer_rate_slots) {
  const std::filesystem::path dir = MakeTestDir("run-report-fanout-rate");
  bbp::WriteText(dir / "resolved-scenario.json",
                 R"({"run_id":"fanout-rate","chain":"firo","nodes":4})");

  for (std::uint64_t index = 0U; index < 3U; ++index) {
    boost::json::object detail;
    detail["transaction_index"] = index + 1U;
    detail["strategy"] = "equal_fanout";
    detail["amount_satoshis"] = 1'000U;
    detail["scheduled_simulation_elapsed_ms"] = index * 500U;
    detail["scheduled_wall_elapsed_ms"] = index * 500U;
    detail["outcome"] = "submitted";
    boost::json::object event;
    event["run_id"] = "fanout-rate";
    event["node_id"] = "firo-1";
    event["event"] = "transaction_load_attempt";
    event["detail"] = boost::json::serialize(detail);
    bbp::AppendLine(dir / "events.jsonl", boost::json::serialize(event));
  }

  const boost::json::object report = bbp::BuildRunReport(dir);
  const boost::json::array& attempts =
      report.at("transaction_load_attempts").as_array();
  BOOST_REQUIRE_EQUAL(attempts.size(), 3U);
  for (std::size_t index = 0U; index < attempts.size(); ++index) {
    const boost::json::object& detail =
        attempts[index].as_object().at("detail").as_object();
    BOOST_TEST(JsonInteger(detail, "amount_satoshis") == 1'000U);
    BOOST_TEST(JsonInteger(detail, "scheduled_simulation_elapsed_ms") ==
               index * 500U);
    BOOST_TEST(JsonInteger(detail, "scheduled_wall_elapsed_ms") ==
               index * 500U);
  }
  std::filesystem::remove_all(dir);
}

BOOST_AUTO_TEST_CASE(incremental_run_report_waits_for_complete_jsonl_lines) {
  const std::filesystem::path dir = MakeTestDir("run-report-partial-line");
  bbp::WriteText(dir / "resolved-scenario.json",
                 R"({"run_id":"partial-line","nodes":0})");
  bbp::WriteText(
      dir / "events.jsonl",
      R"({"run_id":"partial-line","node_id":"sim","event":"run_started"})");

  bbp::IncrementalRunReport incremental(dir);
  const boost::json::object& incomplete = incremental.Refresh(16U);
  BOOST_TEST(JsonInteger(incomplete, "event_count") == 0U);
  BOOST_TEST(incremental.last_refresh_stats().event_records == 0U);
  BOOST_TEST(incremental.last_refresh_stats().has_backlog);

  bbp::AppendLine(dir / "events.jsonl", "");
  const boost::json::object& complete = incremental.Refresh(16U);
  BOOST_TEST(JsonInteger(complete, "event_count") == 1U);
  BOOST_TEST(incremental.last_refresh_stats().event_records == 1U);
  BOOST_TEST(!incremental.last_refresh_stats().has_backlog);
  std::filesystem::remove_all(dir);
}

BOOST_AUTO_TEST_CASE(incremental_run_report_replays_atomically_replaced_input) {
  const std::filesystem::path dir = MakeTestDir("run-report-replaced-input");
  bbp::WriteText(dir / "resolved-scenario.json",
                 R"({"run_id":"replaced-input","nodes":0})");
  bbp::AppendLine(
      dir / "events.jsonl",
      R"({"run_id":"replaced-input","node_id":"sim","event":"run_started"})");

  bbp::IncrementalRunReport incremental(dir);
  BOOST_TEST(JsonInteger(incremental.Refresh(), "event_count") == 1U);

  const std::filesystem::path replacement = dir / "replacement.jsonl";
  bbp::AppendLine(
      replacement,
      R"({"run_id":"replaced-input","node_id":"sim","event":"run_started","timestamp":"2026-07-17T12:00:00Z"})");
  bbp::AppendLine(
      replacement,
      R"({"run_id":"replaced-input","node_id":"sim","event":"run_finished","timestamp":"2026-07-17T12:00:01Z"})");
  std::filesystem::rename(replacement, dir / "events.jsonl");

  const boost::json::object& replayed = incremental.Refresh();
  BOOST_TEST(JsonInteger(replayed, "event_count") == 2U);
  BOOST_TEST(bbp::JsonString(replayed, "status") == "finished");
  std::filesystem::remove_all(dir);
}

BOOST_AUTO_TEST_CASE(incremental_run_report_does_not_skip_invalid_jsonl) {
  const std::filesystem::path dir = MakeTestDir("run-report-invalid-jsonl");
  bbp::WriteText(dir / "resolved-scenario.json",
                 R"({"run_id":"invalid-jsonl","nodes":0})");
  bbp::AppendLine(dir / "events.jsonl", "not-json");

  bbp::IncrementalRunReport incremental(dir);
  BOOST_CHECK_THROW(incremental.Refresh(), std::exception);
  BOOST_CHECK_THROW(incremental.Refresh(), std::exception);
  std::filesystem::remove_all(dir);
}
