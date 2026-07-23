#include <unistd.h>

#include <atomic>
#include <boost/json/object.hpp>
#include <boost/json/serialize.hpp>
#include <boost/test/unit_test.hpp>
#include <chrono>
#include <filesystem>
#include <stdexcept>
#include <string>
#include <thread>

#include "bbp/mcp_dispatcher.h"
#include "bbp/mcp_live_application.h"
#include "bbp/mcp_run_evidence.h"
#include "bbp/run_ownership.h"
#include "bbp/scenario_service.h"
#include "bbp/simulation_command_queue.h"
#include "bbp/util.h"

namespace bbp {
namespace {

using namespace std::chrono_literals;

class LiveApplicationDirectory {
 public:
  LiveApplicationDirectory() {
    path_ = std::filesystem::temp_directory_path() /
            ("bbp-mcp-live-application-" + std::to_string(getpid()));
    std::filesystem::remove_all(path_);
    std::filesystem::create_directories(path_);
  }

  ~LiveApplicationDirectory() { std::filesystem::remove_all(path_); }

  const std::filesystem::path& path() const { return path_; }

 private:
  std::filesystem::path path_;
};

boost::json::object LiveScenario() {
  return boost::json::object{
      {"chain", "firo"},
      {"chain_daemon", "/bin/true"},
      {"run_id", "live-application"},
      {"nodes", 1U},
      {"block_production", boost::json::object{{"enabled", false}}}};
}

boost::json::object Invoke(McpDispatcher* dispatcher, std::string_view tool,
                           boost::json::object arguments) {
  boost::json::value result = dispatcher->ToolHandler()(
      tool, arguments, "live-session", std::stop_token{});
  BOOST_REQUIRE(result.is_object());
  return result.as_object();
}

boost::json::object WaitForTerminal(McpDispatcher* dispatcher,
                                    const boost::json::object& submitted) {
  const std::string operation_id(submitted.at("operation_id").as_string());
  const auto deadline = std::chrono::steady_clock::now() + 2s;
  while (std::chrono::steady_clock::now() < deadline) {
    boost::json::object snapshot =
        Invoke(dispatcher, "operation.get",
               boost::json::object{{"operation_id", operation_id}});
    const std::string state(snapshot.at("state").as_string());
    if (state == "succeeded" || state == "failed" || state == "cancelled") {
      return snapshot;
    }
    std::this_thread::sleep_for(1ms);
  }
  throw std::runtime_error("MCP live application operation did not finish");
}

SimulationCommand WaitForQueuedCommand(SimulationCommandQueue* queue) {
  const auto deadline = std::chrono::steady_clock::now() + 2s;
  while (std::chrono::steady_clock::now() < deadline) {
    if (std::optional<SimulationCommand> command = queue->TryPop()) {
      return std::move(*command);
    }
    std::this_thread::sleep_for(1ms);
  }
  throw std::runtime_error("MCP live command was not queued");
}

}  // namespace

BOOST_AUTO_TEST_CASE(
    mcp_live_application_reads_real_report_and_waits_for_real_command_outcome) {
  LiveApplicationDirectory temporary;
  const boost::json::object scenario = LiveScenario();
  const Options options = ParseAndValidateScenario(scenario);
  WriteText(temporary.path() / "resolved-scenario.json",
            boost::json::serialize(ResolveScenario(scenario)) + "\n");
  AppendLine(
      temporary.path() / "events.jsonl",
      R"({"run_id":"live-application","node_id":"sim","event":"run_started"})");

  SimulationCommandQueue queue;
  std::atomic<bool> stop_requested = false;
  McpLiveApplication application(McpLiveApplication::Config{
      .run_id = "live-application",
      .run_root = temporary.path(),
      .options = &options,
      .command_queue = &queue,
      .request_run_stop = [&] { stop_requested.store(true); }});
  McpDispatcher dispatcher({}, application.OperationFactory(),
                           application.ResourceReader());
  dispatcher.SessionHandler()("live-session", true, {});

  const boost::json::value reports = application.ResourceReader()(
      McpInformationFamily::kReports, "live-session", std::stop_token{});
  BOOST_REQUIRE(reports.is_object());
  BOOST_TEST(reports.as_object().at("run_id").as_string() ==
             "live-application");
  BOOST_TEST(reports.as_object()
                 .at("data")
                 .as_object()
                 .at("event_count")
                 .as_uint64() == 1U);

  const boost::json::object report_submitted =
      Invoke(&dispatcher, "run.report",
             boost::json::object{{"run_id", "live-application"}});
  const boost::json::object report_terminal =
      WaitForTerminal(&dispatcher, report_submitted);
  BOOST_TEST(report_terminal.at("state").as_string() == "succeeded");
  BOOST_TEST(report_terminal.at("terminal_result")
                 .as_object()
                 .at("items")
                 .as_array()
                 .front()
                 .as_object()
                 .at("data")
                 .as_object()
                 .at("run_id")
                 .as_string() == "live-application");

  const boost::json::object command_arguments{
      {"run_id", "live-application"},
      {"command", boost::json::object{{"kind", "increase_log_verbosity"},
                                      {"node", "firo-1"}}}};
  const boost::json::object command_submitted =
      Invoke(&dispatcher, "simulation.command", command_arguments);
  const SimulationCommand command = WaitForQueuedCommand(&queue);
  application.RecordCommandOutcome(command, std::nullopt);
  const boost::json::object command_terminal =
      WaitForTerminal(&dispatcher, command_submitted);
  BOOST_TEST(command_terminal.at("state").as_string() == "succeeded");
  BOOST_TEST(command_terminal.at("terminal_result")
                 .as_object()
                 .at("command_id")
                 .as_string() == "command-1");

  const boost::json::object failed_submitted =
      Invoke(&dispatcher, "simulation.command", command_arguments);
  const SimulationCommand failed = WaitForQueuedCommand(&queue);
  application.RecordCommandOutcome(failed, "production command failure");
  const boost::json::object failed_terminal =
      WaitForTerminal(&dispatcher, failed_submitted);
  BOOST_TEST(failed_terminal.at("state").as_string() == "failed");
  BOOST_TEST(failed_terminal.at("terminal_error")
                 .as_object()
                 .at("message")
                 .as_string() ==
             "simulation command #2 failed: production command failure");

  const boost::json::object stop_submitted =
      Invoke(&dispatcher, "run.stop",
             boost::json::object{{"run_id", "live-application"}});
  const boost::json::object stop_terminal =
      WaitForTerminal(&dispatcher, stop_submitted);
  BOOST_TEST(stop_terminal.at("state").as_string() == "succeeded");
  BOOST_TEST(stop_requested.load());

  dispatcher.Shutdown();
  application.Shutdown();
}

BOOST_AUTO_TEST_CASE(
    mcp_live_application_pages_owned_evidence_logs_and_safe_artifacts) {
  LiveApplicationDirectory temporary;
  const boost::json::object scenario = LiveScenario();
  const Options options = ParseAndValidateScenario(scenario);
  const RunOwnership ownership =
      CreateRunOwnership("live-application", temporary.path());
  WriteRunOwnershipMarker(ownership);
  WriteText(temporary.path() / "resolved-scenario.json",
            boost::json::serialize(ResolveScenario(scenario)) + "\n");
  AppendLine(
      temporary.path() / "events.jsonl",
      R"({"timestamp":"2026-07-22T12:00:00Z","run_id":"live-application","node_id":"sim","event":"run_started","detail":""})");
  AppendLine(
      temporary.path() / "events.jsonl",
      R"({"timestamp":"2026-07-22T12:00:01Z","run_id":"live-application","node_id":"firo-1","event":"daemon_log_tail","detail":"{\"kind\":\"daemon_log\",\"text\":\"ready\\n\"}"})");
  AppendLine(
      temporary.path() / "events.jsonl",
      R"({"timestamp":"2026-07-22T12:00:02Z","run_id":"live-application","node_id":"firo-1","event":"operator_command_failed","detail":"expected failure"})");
  AppendLine(
      temporary.path() / "metrics.jsonl",
      R"({"timestamp_ms":1784721603000,"run_id":"live-application","node_id":"firo-1","height":7})");
  WriteText(temporary.path() / "simulator.log", "abcdefgh");
  std::filesystem::create_directories(temporary.path() / "nodes" / "firo-1" /
                                      "data");
  WriteText(temporary.path() / "nodes" / "firo-1" / "data" / ".cookie",
            "secret");
  std::filesystem::create_directories(temporary.path() / "mcp");
  WriteText(temporary.path() / "mcp" / "token", "must-not-be-listed");
  std::filesystem::create_symlink(
      "/etc/passwd", temporary.path() / "nodes" / "firo-1" / "escape.log");

  SimulationCommandQueue queue;
  McpLiveApplication application(
      McpLiveApplication::Config{.run_id = "live-application",
                                 .run_root = temporary.path(),
                                 .options = &options,
                                 .command_queue = &queue,
                                 .request_run_stop = [] {}});
  McpDispatcher dispatcher({}, application.OperationFactory(),
                           application.ResourceReader());
  dispatcher.SessionHandler()("live-session", true, {});

  const boost::json::object evidence_terminal = WaitForTerminal(
      &dispatcher,
      Invoke(&dispatcher, "evidence.query",
             boost::json::object{{"run_id", "live-application"},
                                 {"families", boost::json::array{"events"}},
                                 {"limit", 2U}}));
  BOOST_TEST(evidence_terminal.at("state").as_string() == "succeeded");
  const boost::json::object& first_page =
      evidence_terminal.at("terminal_result").as_object();
  BOOST_REQUIRE_EQUAL(first_page.at("items").as_array().size(), 2U);
  BOOST_TEST(first_page.at("truncated").as_bool());
  BOOST_TEST(first_page.at("next_cursor").as_string().size() < 256U);
  BOOST_TEST(first_page.at("items")
                 .as_array()[1]
                 .as_object()
                 .at("data")
                 .as_object()
                 .at("detail")
                 .as_object()
                 .at("text")
                 .as_string() == "ready\n");

  const boost::json::object second_terminal = WaitForTerminal(
      &dispatcher,
      Invoke(&dispatcher, "evidence.query",
             boost::json::object{{"run_id", "live-application"},
                                 {"families", boost::json::array{"events"}},
                                 {"cursor", first_page.at("next_cursor")},
                                 {"limit", 2U}}));
  const boost::json::object& second_page =
      second_terminal.at("terminal_result").as_object();
  BOOST_REQUIRE_EQUAL(second_page.at("items").as_array().size(), 1U);
  BOOST_TEST(!second_page.at("truncated").as_bool());
  BOOST_TEST(second_page.at("items")
                 .as_array()[0]
                 .as_object()
                 .at("kind")
                 .as_string() == "operator_command_failed");

  const boost::json::object log_terminal = WaitForTerminal(
      &dispatcher,
      Invoke(&dispatcher, "log.query",
             boost::json::object{{"run_id", "live-application"},
                                 {"node_ids", boost::json::array{"firo-1"}}}));
  const boost::json::object& log_page =
      log_terminal.at("terminal_result").as_object();
  BOOST_REQUIRE_EQUAL(log_page.at("items").as_array().size(), 1U);
  BOOST_TEST(
      log_page.at("items").as_array()[0].as_object().at("kind").as_string() ==
      "daemon_log_tail");

  const boost::json::object follow_submitted =
      Invoke(&dispatcher, "log.follow",
             boost::json::object{{"run_id", "live-application"},
                                 {"node_ids", boost::json::array{"firo-1"}},
                                 {"cursor", log_page.at("next_cursor")}});
  std::this_thread::sleep_for(50ms);
  AppendLine(
      temporary.path() / "events.jsonl",
      R"({"timestamp":"2026-07-22T12:00:04Z","run_id":"live-application","node_id":"firo-1","event":"daemon_log_tail","detail":"{\"kind\":\"daemon_log\",\"text\":\"new line\\n\"}"})");
  const boost::json::object follow_terminal =
      WaitForTerminal(&dispatcher, follow_submitted);
  BOOST_TEST(follow_terminal.at("state").as_string() == "succeeded");
  const boost::json::object& follow_page =
      follow_terminal.at("terminal_result").as_object();
  BOOST_REQUIRE_EQUAL(follow_page.at("items").as_array().size(), 1U);
  BOOST_TEST(follow_page.at("items")
                 .as_array()[0]
                 .as_object()
                 .at("data")
                 .as_object()
                 .at("detail")
                 .as_object()
                 .at("text")
                 .as_string() == "new line\n");

  const boost::json::object cancellable_follow =
      Invoke(&dispatcher, "log.follow",
             boost::json::object{{"run_id", "live-application"},
                                 {"node_ids", boost::json::array{"firo-1"}},
                                 {"cursor", follow_page.at("next_cursor")}});
  std::this_thread::sleep_for(50ms);
  const auto cancellation_started = std::chrono::steady_clock::now();
  const boost::json::object cancellation =
      Invoke(&dispatcher, "operation.cancel",
             boost::json::object{
                 {"operation_id", cancellable_follow.at("operation_id")}});
  BOOST_TEST(cancellation.at("cancel_requested").as_bool());
  const boost::json::object cancelled_terminal =
      WaitForTerminal(&dispatcher, cancellable_follow);
  BOOST_TEST(cancelled_terminal.at("state").as_string() == "cancelled");
  BOOST_CHECK(std::chrono::steady_clock::now() - cancellation_started < 500ms);

  const boost::json::value inventory_value = application.ResourceReader()(
      McpInformationFamily::kArtifacts, "live-session", std::stop_token{});
  const boost::json::array& entries = inventory_value.as_object()
                                          .at("data")
                                          .as_object()
                                          .at("entries")
                                          .as_array();
  std::string simulator_log_id;
  bool cookie_was_unreadable = false;
  bool symlink_was_unreadable = false;
  bool credential_was_listed = false;
  for (const boost::json::value& value : entries) {
    const boost::json::object& entry = value.as_object();
    const std::string path(entry.at("relative_path").as_string());
    if (path == "simulator.log") {
      simulator_log_id = std::string(entry.at("artifact_id").as_string());
      BOOST_TEST(entry.at("readable").as_bool());
    } else if (path.ends_with("/.cookie")) {
      cookie_was_unreadable = !entry.at("readable").as_bool();
    } else if (path.ends_with("/escape.log")) {
      symlink_was_unreadable = !entry.at("readable").as_bool();
    } else if (path.starts_with("mcp/")) {
      credential_was_listed = true;
    }
  }
  BOOST_REQUIRE(!simulator_log_id.empty());
  BOOST_TEST(cookie_was_unreadable);
  BOOST_TEST(symlink_was_unreadable);
  BOOST_TEST(!credential_was_listed);

  const boost::json::object artifact_terminal = WaitForTerminal(
      &dispatcher, Invoke(&dispatcher, "artifact.read",
                          boost::json::object{{"run_id", "live-application"},
                                              {"artifact_id", simulator_log_id},
                                              {"limit", 4U}}));
  const boost::json::object& artifact =
      artifact_terminal.at("terminal_result").as_object();
  BOOST_TEST(artifact.at("content").as_string() == "YWJjZA==");
  BOOST_TEST(artifact.at("next_offset").as_uint64() == 4U);
  BOOST_TEST(!artifact.at("eof").as_bool());

  dispatcher.Shutdown();
  application.Shutdown();
}

BOOST_AUTO_TEST_CASE(
    mcp_run_evidence_cursor_resumes_shared_sources_and_later_appends) {
  LiveApplicationDirectory temporary;
  const RunOwnership ownership =
      CreateRunOwnership("live-application", temporary.path());
  WriteRunOwnershipMarker(ownership);
  AppendLine(
      temporary.path() / "events.jsonl",
      R"({"timestamp":"2026-07-22T12:00:00Z","run_id":"live-application","node_id":"firo-1","event":"run_started","detail":""})");
  AppendLine(
      temporary.path() / "metrics.jsonl",
      R"({"timestamp_ms":1784721601000,"run_id":"live-application","node_id":"firo-1","height":7})");

  McpRunEvidenceQuery query;
  query.families = {McpInformationFamily::kEvents,
                    McpInformationFamily::kMetrics};
  query.limit = 2U;
  const boost::json::object first =
      QueryMcpRunEvidence("live-application", temporary.path(), query);
  BOOST_REQUIRE_EQUAL(first.at("items").as_array().size(), 2U);
  BOOST_TEST(first.at("next_cursor").as_string().size() < 256U);

  McpRunEvidenceQuery mismatched = query;
  mismatched.families = {McpInformationFamily::kMetrics,
                         McpInformationFamily::kEvents};
  mismatched.cursor = std::string(first.at("next_cursor").as_string());
  BOOST_CHECK_THROW(
      QueryMcpRunEvidence("live-application", temporary.path(), mismatched),
      std::invalid_argument);

  AppendLine(
      temporary.path() / "events.jsonl",
      R"({"timestamp":"2026-07-22T12:00:02Z","run_id":"live-application","node_id":"firo-1","event":"run_completed","detail":""})");
  query.cursor = std::string(first.at("next_cursor").as_string());
  const boost::json::object second =
      QueryMcpRunEvidence("live-application", temporary.path(), query);
  BOOST_REQUIRE_EQUAL(second.at("items").as_array().size(), 1U);
  BOOST_TEST(second.at("items")
                 .as_array()
                 .front()
                 .as_object()
                 .at("family")
                 .as_string() == "events");
  BOOST_TEST(second.at("items")
                 .as_array()
                 .front()
                 .as_object()
                 .at("kind")
                 .as_string() == "run_completed");
  BOOST_TEST(!second.at("truncated").as_bool());
}

BOOST_AUTO_TEST_CASE(
    mcp_run_evidence_cursor_resumes_pending_family_and_snapshots_once) {
  LiveApplicationDirectory temporary;
  const RunOwnership ownership =
      CreateRunOwnership("live-application", temporary.path());
  WriteRunOwnershipMarker(ownership);
  AppendLine(
      temporary.path() / "events.jsonl",
      R"({"timestamp":"2026-07-22T12:00:00Z","run_id":"live-application","node_id":"firo-1","event":"operator_command_failed","detail":"expected"})");

  McpRunEvidenceQuery shared_source;
  shared_source.families = {McpInformationFamily::kEvents,
                            McpInformationFamily::kErrors};
  shared_source.limit = 1U;
  const boost::json::object events =
      QueryMcpRunEvidence("live-application", temporary.path(), shared_source);
  BOOST_REQUIRE_EQUAL(events.at("items").as_array().size(), 1U);
  BOOST_TEST(events.at("items")
                 .as_array()
                 .front()
                 .as_object()
                 .at("family")
                 .as_string() == "events");

  shared_source.cursor = std::string(events.at("next_cursor").as_string());
  const boost::json::object errors =
      QueryMcpRunEvidence("live-application", temporary.path(), shared_source);
  BOOST_REQUIRE_EQUAL(errors.at("items").as_array().size(), 1U);
  BOOST_TEST(errors.at("items")
                 .as_array()
                 .front()
                 .as_object()
                 .at("family")
                 .as_string() == "errors");

  McpRunEvidenceQuery snapshot;
  snapshot.families = {McpInformationFamily::kCapabilities};
  snapshot.limit = 1U;
  const boost::json::object snapshot_page =
      QueryMcpRunEvidence("live-application", temporary.path(), snapshot);
  BOOST_REQUIRE_EQUAL(snapshot_page.at("items").as_array().size(), 1U);
  snapshot.cursor = std::string(snapshot_page.at("next_cursor").as_string());
  const boost::json::object exhausted =
      QueryMcpRunEvidence("live-application", temporary.path(), snapshot);
  BOOST_TEST(exhausted.at("items").as_array().empty());
  BOOST_TEST(!exhausted.at("truncated").as_bool());
}

BOOST_AUTO_TEST_CASE(
    mcp_run_artifact_inventory_surfaces_cancellation_and_entry_bound) {
  LiveApplicationDirectory temporary;
  const RunOwnership ownership =
      CreateRunOwnership("live-application", temporary.path());
  WriteRunOwnershipMarker(ownership);

  std::stop_source cancelled;
  cancelled.request_stop();
  McpRunEvidenceQuery query;
  query.families = {McpInformationFamily::kEvents};
  BOOST_CHECK_THROW(QueryMcpRunEvidence("live-application", temporary.path(),
                                        query, cancelled.get_token()),
                    McpOperationCancelled);
  BOOST_CHECK_THROW(
      BuildMcpRunArtifactInventory("live-application", temporary.path(),
                                   cancelled.get_token()),
      McpOperationCancelled);

  std::filesystem::create_directories(temporary.path() / "mcp");
  WriteText(temporary.path() / "mcp" / "token", "excluded");
  for (std::size_t index = 0U; index < 4097U; ++index) {
    WriteText(temporary.path() / ("artifact-" + std::to_string(index) + ".log"),
              "bounded");
  }
  const boost::json::object inventory =
      BuildMcpRunArtifactInventory("live-application", temporary.path());
  BOOST_TEST(inventory.at("truncated").as_bool());
  BOOST_TEST(inventory.at("entries").as_array().size() == 4096U);
  for (const boost::json::value& value : inventory.at("entries").as_array()) {
    BOOST_TEST(!std::string(value.as_object().at("relative_path").as_string())
                    .starts_with("mcp"));
  }
}

}  // namespace bbp
