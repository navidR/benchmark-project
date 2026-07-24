#include <unistd.h>

#include <algorithm>
#include <array>
#include <atomic>
#include <boost/json/object.hpp>
#include <boost/json/serialize.hpp>
#include <boost/test/unit_test.hpp>
#include <chrono>
#include <filesystem>
#include <future>
#include <memory>
#include <stdexcept>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include "bbp/mcp_dispatcher.h"
#include "bbp/mcp_live_application.h"
#include "bbp/mcp_run_evidence.h"
#include "bbp/run_ownership.h"
#include "bbp/scenario_service.h"
#include "bbp/simulation_cancelled.h"
#include "bbp/simulation_command_processor.h"
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

class AtomicReleaseGuard {
 public:
  explicit AtomicReleaseGuard(std::atomic_bool* release) : release_(release) {}
  ~AtomicReleaseGuard() { release_->store(true, std::memory_order_release); }

  AtomicReleaseGuard(const AtomicReleaseGuard&) = delete;
  AtomicReleaseGuard& operator=(const AtomicReleaseGuard&) = delete;

 private:
  std::atomic_bool* release_;
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

McpOperationSnapshot WaitForTerminal(McpOperationService* service,
                                     std::string_view operation_id) {
  const std::optional<McpOperationSnapshot> terminal =
      service->WaitForOperation("live-session", operation_id, 2s);
  BOOST_REQUIRE(static_cast<bool>(terminal));
  return *terminal;
}

SimulationCommandOutcome CommandOutcome(
    SimulationCommandOutcomeState state,
    std::optional<std::string> error = std::nullopt,
    std::optional<std::string> node_lifecycle = std::nullopt,
    SimulationCommandCancellationCause cause =
        SimulationCommandCancellationCause::kNone) {
  return SimulationCommandOutcome{.state = state,
                                  .cancellation_cause = cause,
                                  .error = std::move(error),
                                  .node_lifecycle = std::move(node_lifecycle)};
}

}  // namespace

BOOST_AUTO_TEST_CASE(
    mcp_live_application_reads_real_report_and_waits_for_real_command_outcome) {
  LiveApplicationDirectory temporary;
  const boost::json::object scenario = LiveScenario();
  const auto options =
      std::make_shared<Options>(ParseAndValidateScenario(scenario));
  WriteText(temporary.path() / "resolved-scenario.json",
            boost::json::serialize(ResolveScenario(scenario)) + "\n");
  AppendLine(
      temporary.path() / "events.jsonl",
      R"({"run_id":"live-application","node_id":"sim","event":"run_started"})");

  auto queue = std::make_shared<SimulationCommandQueue>();
  std::atomic<bool> stop_requested = false;
  McpDispatcher* evidence_dispatcher = nullptr;
  McpLiveApplication application(McpLiveApplication::Config{
      .run_id = "live-application",
      .run_root = temporary.path(),
      .retained_run = std::nullopt,
      .options = options,
      .command_queue = queue,
      .request_run_stop = [&] { stop_requested.store(true); },
      .run_started = {},
      .run_stopping = {},
      .run_stopped = {},
      .publish_evidence =
          [&](McpEvidenceRecord record) {
            BOOST_REQUIRE(evidence_dispatcher != nullptr);
            evidence_dispatcher->Publish(std::move(record));
          },
      .close_run_subscriptions =
          [&](std::string_view run_id) {
            BOOST_REQUIRE(evidence_dispatcher != nullptr);
            evidence_dispatcher->CloseRunSubscriptions(run_id);
          }});
  const boost::json::value starting_registry = application.ResourceReader()(
      McpInformationFamily::kRunRegistry, "live-session", std::stop_token{});
  BOOST_TEST(starting_registry.as_object()
                 .at("data")
                 .as_array()
                 .front()
                 .as_object()
                 .at("state")
                 .as_string() == "starting");
  McpDispatcher dispatcher({}, application.OperationFactory(),
                           application.ResourceReader());
  evidence_dispatcher = &dispatcher;
  dispatcher.SessionHandler()("live-session", true, {});
  const boost::json::object subscription =
      Invoke(&dispatcher, "subscription.create",
             boost::json::object{
                 {"run_id", "live-application"},
                 {"families", boost::json::array{"lifecycle", "events"}}});
  application.MarkRunStarted();
  const boost::json::object started_page =
      Invoke(&dispatcher, "subscription.poll",
             boost::json::object{
                 {"subscription_id", subscription.at("subscription_id")},
                 {"cursor", "0"},
                 {"limit", 8U}});
  BOOST_REQUIRE_EQUAL(started_page.at("items").as_array().size(), 1U);
  const boost::json::object& started_evidence =
      started_page.at("items").as_array().front().as_object();
  BOOST_TEST(started_evidence.at("run_id").as_string() == "live-application");
  BOOST_TEST(started_evidence.at("kind").as_string() == "run_started");

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
  const SimulationCommand command = WaitForQueuedCommand(queue.get());
  application.RecordCommandOutcome(
      command, CommandOutcome(SimulationCommandOutcomeState::kSucceeded));
  const boost::json::object command_terminal =
      WaitForTerminal(&dispatcher, command_submitted);
  BOOST_TEST(command_terminal.at("state").as_string() == "succeeded");
  BOOST_TEST(command_terminal.at("terminal_result")
                 .as_object()
                 .at("command_id")
                 .as_string() == "command-1");

  const boost::json::object failed_submitted =
      Invoke(&dispatcher, "simulation.command", command_arguments);
  const SimulationCommand failed = WaitForQueuedCommand(queue.get());
  application.RecordCommandOutcome(
      failed, CommandOutcome(SimulationCommandOutcomeState::kFailed,
                             "production command failure"));
  const boost::json::object failed_terminal =
      WaitForTerminal(&dispatcher, failed_submitted);
  BOOST_TEST(failed_terminal.at("state").as_string() == "failed");
  BOOST_TEST(failed_terminal.at("terminal_error")
                 .as_object()
                 .at("message")
                 .as_string() ==
             "simulation command #2 failed: production command failure");

  const boost::json::object cancellable_submitted =
      Invoke(&dispatcher, "simulation.command", command_arguments);
  const SimulationCommand cancellable = WaitForQueuedCommand(queue.get());
  const auto cancellation_started = std::chrono::steady_clock::now();
  const boost::json::object cancellation =
      Invoke(&dispatcher, "operation.cancel",
             boost::json::object{
                 {"operation_id", cancellable_submitted.at("operation_id")}});
  BOOST_TEST(cancellation.at("cancel_requested").as_bool());
  BOOST_TEST(cancellation.at("state").as_string() == "cancelling");
  BOOST_REQUIRE(cancellable.operation_control);
  BOOST_TEST(cancellable.operation_control->stop_source.stop_requested());
  application.RecordCommandOutcome(
      cancellable,
      CommandOutcome(SimulationCommandOutcomeState::kCancelled,
                     "simulation stop requested", std::nullopt,
                     SimulationCommandCancellationCause::kClientCancel));
  const boost::json::object cancelled_terminal =
      WaitForTerminal(&dispatcher, cancellable_submitted);
  BOOST_TEST(cancelled_terminal.at("state").as_string() == "cancelled");
  BOOST_CHECK(std::chrono::steady_clock::now() - cancellation_started < 500ms);

  const SimulationCommandQueueStats queue_before_invalid_node = queue->Stats();
  BOOST_CHECK_THROW(Invoke(&dispatcher, "node.stop",
                           boost::json::object{{"run_id", "live-application"},
                                               {"node_id", "bad/node"},
                                               {"timeout_sec", 30U}}),
                    std::invalid_argument);
  const SimulationCommandQueueStats queue_after_invalid_node = queue->Stats();
  BOOST_TEST(queue_after_invalid_node.size == queue_before_invalid_node.size);
  BOOST_TEST(queue_after_invalid_node.maximum_size ==
             queue_before_invalid_node.maximum_size);
  BOOST_TEST(queue_after_invalid_node.rejected ==
             queue_before_invalid_node.rejected);

  const std::array typed_node_operations{
      std::pair{"node.stop", SimulationCommandKind::kStopNode},
      std::pair{"node.kill", SimulationCommandKind::kKillNode},
      std::pair{"node.restart", SimulationCommandKind::kRestartNode},
  };
  const std::array expected_actions{"stop", "kill", "restart"};
  const std::array expected_states{"stopped", "killed", "running"};
  for (std::size_t index = 0U; index < typed_node_operations.size(); ++index) {
    const auto& [tool, expected_kind] = typed_node_operations[index];
    const boost::json::object submitted =
        Invoke(&dispatcher, tool,
               boost::json::object{{"run_id", "live-application"},
                                   {"node_id", "firo-1"},
                                   {"timeout_sec", 30U}});
    const SimulationCommand typed_command = WaitForQueuedCommand(queue.get());
    BOOST_CHECK(typed_command.kind == expected_kind);
    BOOST_TEST(typed_command.node_id == "firo-1");
    BOOST_TEST(typed_command.confirmed);
    BOOST_REQUIRE(typed_command.operation_control);
    BOOST_TEST(!typed_command.operation_control->stop_source.stop_requested());
    application.RecordCommandOutcome(
        typed_command, CommandOutcome(SimulationCommandOutcomeState::kSucceeded,
                                      std::nullopt, expected_states[index]));
    const boost::json::object terminal =
        WaitForTerminal(&dispatcher, submitted);
    BOOST_TEST(terminal.at("state").as_string() == "succeeded");
    const boost::json::object& mutation =
        terminal.at("terminal_result").as_object();
    BOOST_TEST(mutation.at("result_family").as_string() == "mutation");
    BOOST_TEST(mutation.at("run_id").as_string() == "live-application");
    BOOST_TEST(mutation.at("added_node_ids").as_array().empty());
    BOOST_TEST(mutation.at("removed_node_ids").as_array().empty());
    BOOST_TEST(
        mutation.at("affected_node_ids").as_array().front().as_string() ==
        "firo-1");
    BOOST_TEST(mutation.at("action").as_string() ==
               "node." + std::string(expected_actions[index]));
    BOOST_TEST(mutation.at("state").as_string() == expected_states[index]);
    BOOST_TEST(std::string(mutation.at("command_id").as_string())
                   .starts_with("command-"));
    BOOST_TEST(!mutation.at("unchanged").as_bool());
  }

  const boost::json::object typed_cancellable =
      Invoke(&dispatcher, "node.restart",
             boost::json::object{{"run_id", "live-application"},
                                 {"node_id", "firo-1"},
                                 {"timeout_sec", 30U}});
  const SimulationCommand cancelled_node_command =
      WaitForQueuedCommand(queue.get());
  BOOST_REQUIRE(cancelled_node_command.operation_control);
  static_cast<void>(
      Invoke(&dispatcher, "operation.cancel",
             boost::json::object{
                 {"operation_id", typed_cancellable.at("operation_id")}}));
  const boost::json::object typed_cancelling =
      Invoke(&dispatcher, "operation.get",
             boost::json::object{
                 {"operation_id", typed_cancellable.at("operation_id")}});
  BOOST_TEST(typed_cancelling.at("state").as_string() == "cancelling");
  BOOST_TEST(
      cancelled_node_command.operation_control->stop_source.stop_requested());
  application.RecordCommandOutcome(
      cancelled_node_command,
      CommandOutcome(SimulationCommandOutcomeState::kCancelled,
                     "simulation stop requested", "running",
                     SimulationCommandCancellationCause::kClientCancel));
  const boost::json::object typed_cancelled_terminal =
      WaitForTerminal(&dispatcher, typed_cancellable);
  BOOST_TEST(typed_cancelled_terminal.at("state").as_string() == "cancelled");

  const auto timeout_started = std::chrono::steady_clock::now();
  const boost::json::object typed_timeout =
      Invoke(&dispatcher, "node.stop",
             boost::json::object{{"run_id", "live-application"},
                                 {"node_id", "firo-1"},
                                 {"timeout_sec", 1U}});
  const SimulationCommand timed_out_node_command =
      WaitForQueuedCommand(queue.get());
  BOOST_REQUIRE(timed_out_node_command.operation_control);
  const auto cancellation_deadline = std::chrono::steady_clock::now() + 900ms;
  while (
      !timed_out_node_command.operation_control->stop_source.stop_requested() &&
      std::chrono::steady_clock::now() < cancellation_deadline) {
    std::this_thread::sleep_for(1ms);
  }
  BOOST_TEST(
      timed_out_node_command.operation_control->stop_source.stop_requested());
  const boost::json::object timing_out = Invoke(
      &dispatcher, "operation.get",
      boost::json::object{{"operation_id", typed_timeout.at("operation_id")}});
  BOOST_TEST(timing_out.at("state").as_string() == "running");
  application.RecordCommandOutcome(
      timed_out_node_command,
      CommandOutcome(SimulationCommandOutcomeState::kTimedOut,
                     "simulation stop requested", "running",
                     SimulationCommandCancellationCause::kDeadline));
  const boost::json::object typed_timeout_terminal =
      WaitForTerminal(&dispatcher, typed_timeout);
  BOOST_TEST(typed_timeout_terminal.at("state").as_string() == "failed");
  BOOST_TEST(typed_timeout_terminal.at("terminal_error")
                 .as_object()
                 .at("code")
                 .as_string() == "node_operation_timeout");
  BOOST_CHECK(std::chrono::steady_clock::now() - timeout_started < 1500ms);

  const boost::json::object stop_submitted =
      Invoke(&dispatcher, "run.stop",
             boost::json::object{{"run_id", "live-application"}});
  const boost::json::object stop_terminal =
      WaitForTerminal(&dispatcher, stop_submitted);
  BOOST_TEST(stop_terminal.at("state").as_string() == "succeeded");
  BOOST_TEST(stop_requested.load());
  const boost::json::object rejected_submitted =
      Invoke(&dispatcher, "simulation.command", command_arguments);
  const boost::json::object rejected_terminal =
      WaitForTerminal(&dispatcher, rejected_submitted);
  BOOST_TEST(rejected_terminal.at("state").as_string() == "failed");
  BOOST_TEST(rejected_terminal.at("terminal_error")
                 .as_object()
                 .at("code")
                 .as_string() == "run_not_active");
  const boost::json::value stopping_registry = application.ResourceReader()(
      McpInformationFamily::kRunRegistry, "live-session", std::stop_token{});
  BOOST_TEST(stopping_registry.as_object()
                 .at("data")
                 .as_array()
                 .front()
                 .as_object()
                 .at("state")
                 .as_string() == "stopping");
  application.MarkRunStopped();
  const boost::json::value stopped_registry = application.ResourceReader()(
      McpInformationFamily::kRunRegistry, "live-session", std::stop_token{});
  BOOST_TEST(stopped_registry.as_object()
                 .at("data")
                 .as_array()
                 .front()
                 .as_object()
                 .at("state")
                 .as_string() == "stopped");
  const boost::json::object stopped_page =
      Invoke(&dispatcher, "subscription.poll",
             boost::json::object{
                 {"subscription_id", subscription.at("subscription_id")},
                 {"cursor", "0"},
                 {"limit", 64U}});
  BOOST_TEST(!stopped_page.at("active").as_bool());
  const boost::json::array& lifecycle_items =
      stopped_page.at("items").as_array();
  BOOST_REQUIRE(lifecycle_items.size() >= 3U);
  BOOST_TEST(lifecycle_items.back().as_object().at("kind").as_string() ==
             "run_stopped");

  dispatcher.Shutdown();
  application.Shutdown();
}

BOOST_AUTO_TEST_CASE(
    mcp_node_lifecycle_terminal_waits_for_command_owner_reconciliation) {
  LiveApplicationDirectory temporary;
  const auto options =
      std::make_shared<Options>(ParseAndValidateScenario(LiveScenario()));
  auto queue = std::make_shared<SimulationCommandQueue>();
  std::atomic_bool run_stop_requested = false;
  McpLiveApplication application(McpLiveApplication::Config{
      .run_id = "live-application",
      .run_root = temporary.path(),
      .retained_run = std::nullopt,
      .options = options,
      .command_queue = queue,
      .request_run_stop = [&] { run_stop_requested = true; },
      .run_started = {},
      .run_stopping = {},
      .run_stopped = {}});
  application.MarkRunStarted();
  std::atomic_uint32_t owner_started = 0U;
  std::atomic_uint32_t owner_reconciled = 0U;
  std::atomic_uint32_t failure_reports = 0U;
  std::atomic_bool hold_owner = false;
  std::atomic_bool release_owner = false;
  SimulationCommandProcessor processor(
      *queue,
      [&](const SimulationCommand& command) {
        if (!command.operation_control) {
          throw std::runtime_error("MCP command stop source is missing");
        }
        owner_started.fetch_add(1U, std::memory_order_release);
        const std::stop_token command_stop_token =
            command.operation_control->stop_source.get_token();
        while (!command_stop_token.stop_requested()) {
          std::this_thread::sleep_for(1ms);
        }
        while (hold_owner.load(std::memory_order_acquire) &&
               !release_owner.load(std::memory_order_acquire)) {
          std::this_thread::sleep_for(1ms);
        }
        std::this_thread::sleep_for(25ms);
        owner_reconciled.fetch_add(1U, std::memory_order_release);
        throw SimulationCancelled();
      },
      [&](const SimulationCommand&, std::string_view) {
        failure_reports.fetch_add(1U, std::memory_order_release);
      },
      [&](const SimulationCommand& command,
          const SimulationCommandOutcome& outcome) {
        SimulationCommandOutcome authoritative = outcome;
        authoritative.node_lifecycle = "running";
        application.RecordCommandOutcome(command, authoritative);
      });
  processor.Start();
  McpDispatcher dispatcher({}, application.OperationFactory(),
                           application.ResourceReader());
  AtomicReleaseGuard release_guard(&release_owner);
  dispatcher.SessionHandler()("live-session", true, {});

  const boost::json::object cancellable =
      Invoke(&dispatcher, "node.kill",
             boost::json::object{{"run_id", "live-application"},
                                 {"node_id", "firo-1"},
                                 {"timeout_sec", 30U}});
  const auto owner_start_deadline = std::chrono::steady_clock::now() + 1s;
  while (owner_started.load(std::memory_order_acquire) != 1U &&
         std::chrono::steady_clock::now() < owner_start_deadline) {
    std::this_thread::sleep_for(1ms);
  }
  BOOST_TEST(owner_started.load(std::memory_order_acquire) == 1U);
  const boost::json::object cancelling = Invoke(
      &dispatcher, "operation.cancel",
      boost::json::object{{"operation_id", cancellable.at("operation_id")}});
  BOOST_TEST(cancelling.at("state").as_string() == "cancelling");
  const boost::json::object cancelled =
      WaitForTerminal(&dispatcher, cancellable);
  BOOST_TEST(cancelled.at("state").as_string() == "cancelled");
  BOOST_TEST(owner_reconciled.load(std::memory_order_acquire) == 1U);
  BOOST_TEST(failure_reports.load(std::memory_order_acquire) == 0U);
  const boost::json::array& cancelled_diagnostics =
      cancelled.at("terminal_error").as_object().at("diagnostics").as_array();
  BOOST_REQUIRE_EQUAL(cancelled_diagnostics.size(), 1U);
  BOOST_TEST(
      cancelled_diagnostics.front().as_object().at("node_id").as_string() ==
      "firo-1");
  BOOST_TEST(
      cancelled_diagnostics.front().as_object().at("action").as_string() ==
      "node.kill");
  BOOST_TEST(
      cancelled_diagnostics.front().as_object().at("state").as_string() ==
      "running");

  const auto timeout_started = std::chrono::steady_clock::now();
  const boost::json::object timing_out =
      Invoke(&dispatcher, "node.restart",
             boost::json::object{{"run_id", "live-application"},
                                 {"node_id", "firo-1"},
                                 {"timeout_sec", 1U}});
  const boost::json::object timed_out =
      WaitForTerminal(&dispatcher, timing_out);
  const auto timeout_elapsed =
      std::chrono::steady_clock::now() - timeout_started;
  BOOST_TEST(timed_out.at("state").as_string() == "failed");
  BOOST_TEST(
      timed_out.at("terminal_error").as_object().at("code").as_string() ==
      "node_operation_timeout");
  BOOST_TEST(owner_started.load(std::memory_order_acquire) == 2U);
  BOOST_TEST(owner_reconciled.load(std::memory_order_acquire) == 2U);
  BOOST_TEST(failure_reports.load(std::memory_order_acquire) == 0U);
  BOOST_CHECK(timeout_elapsed < 1200ms);

  hold_owner.store(true, std::memory_order_release);
  const boost::json::object unconfirmed =
      Invoke(&dispatcher, "node.stop",
             boost::json::object{{"run_id", "live-application"},
                                 {"node_id", "firo-1"},
                                 {"timeout_sec", 30U}});
  const auto third_owner_deadline = std::chrono::steady_clock::now() + 1s;
  while (owner_started.load(std::memory_order_acquire) != 3U &&
         std::chrono::steady_clock::now() < third_owner_deadline) {
    std::this_thread::sleep_for(1ms);
  }
  BOOST_TEST(owner_started.load(std::memory_order_acquire) == 3U);
  const auto unconfirmed_started = std::chrono::steady_clock::now();
  static_cast<void>(Invoke(
      &dispatcher, "operation.cancel",
      boost::json::object{{"operation_id", unconfirmed.at("operation_id")}}));
  const boost::json::object unconfirmed_terminal =
      WaitForTerminal(&dispatcher, unconfirmed);
  BOOST_TEST(unconfirmed_terminal.at("state").as_string() == "failed");
  BOOST_TEST(unconfirmed_terminal.at("terminal_error")
                 .as_object()
                 .at("code")
                 .as_string() == "node_outcome_unconfirmed");
  BOOST_CHECK(std::chrono::steady_clock::now() - unconfirmed_started < 500ms);
  BOOST_TEST(run_stop_requested.load(std::memory_order_acquire));
  release_owner.store(true, std::memory_order_release);
  const auto third_reconciled_deadline = std::chrono::steady_clock::now() + 1s;
  while (owner_reconciled.load(std::memory_order_acquire) != 3U &&
         std::chrono::steady_clock::now() < third_reconciled_deadline) {
    std::this_thread::sleep_for(1ms);
  }
  BOOST_TEST(owner_reconciled.load(std::memory_order_acquire) == 3U);
  BOOST_TEST(failure_reports.load(std::memory_order_acquire) == 0U);
  const boost::json::object immutable_terminal = Invoke(
      &dispatcher, "operation.get",
      boost::json::object{{"operation_id", unconfirmed.at("operation_id")}});
  BOOST_TEST(immutable_terminal.at("state").as_string() == "failed");
  BOOST_TEST(immutable_terminal.at("terminal_error")
                 .as_object()
                 .at("code")
                 .as_string() == "node_outcome_unconfirmed");

  hold_owner.store(false, std::memory_order_release);
  release_owner.store(false, std::memory_order_release);
  const boost::json::object shutdown_operation =
      Invoke(&dispatcher, "node.kill",
             boost::json::object{{"run_id", "live-application"},
                                 {"node_id", "firo-1"},
                                 {"timeout_sec", 30U}});
  const auto fourth_owner_deadline = std::chrono::steady_clock::now() + 1s;
  while (owner_started.load(std::memory_order_acquire) != 4U &&
         std::chrono::steady_clock::now() < fourth_owner_deadline) {
    std::this_thread::sleep_for(1ms);
  }
  BOOST_TEST(owner_started.load(std::memory_order_acquire) == 4U);
  const boost::json::object stop_run =
      Invoke(&dispatcher, "run.stop",
             boost::json::object{{"run_id", "live-application"}});
  BOOST_TEST(WaitForTerminal(&dispatcher, stop_run).at("state").as_string() ==
             "succeeded");
  const boost::json::object shutdown_terminal =
      WaitForTerminal(&dispatcher, shutdown_operation);
  BOOST_TEST(shutdown_terminal.at("state").as_string() == "cancelled");
  const boost::json::array& shutdown_diagnostics =
      shutdown_terminal.at("terminal_error")
          .as_object()
          .at("diagnostics")
          .as_array();
  BOOST_REQUIRE_EQUAL(shutdown_diagnostics.size(), 1U);
  BOOST_TEST(shutdown_diagnostics.front().as_object().at("code").as_string() ==
             "application_shutdown");
  BOOST_TEST(owner_reconciled.load(std::memory_order_acquire) == 4U);
  BOOST_TEST(failure_reports.load(std::memory_order_acquire) == 0U);

  processor.Stop();
  dispatcher.Shutdown();
  application.Shutdown();
}

BOOST_AUTO_TEST_CASE(
    mcp_prebuilt_command_is_rejected_after_run_stopping_is_published) {
  LiveApplicationDirectory temporary;
  const auto options =
      std::make_shared<Options>(ParseAndValidateScenario(LiveScenario()));
  auto queue = std::make_shared<SimulationCommandQueue>();
  McpLiveApplication application(
      McpLiveApplication::Config{.run_id = "live-application",
                                 .run_root = temporary.path(),
                                 .retained_run = std::nullopt,
                                 .options = options,
                                 .command_queue = queue,
                                 .request_run_stop = [] {},
                                 .run_started = {},
                                 .run_stopping = {},
                                 .run_stopped = {}});
  application.MarkRunStarted();

  McpOperationPlan stale_plan = application.OperationFactory()(
      McpOperationKind::kInvokeRuntimeCommand,
      boost::json::object{
          {"run_id", "live-application"},
          {"command", boost::json::object{{"kind", "increase_log_verbosity"},
                                          {"node", "firo-1"}}}},
      "live-session");
  application.MarkRunStopping();

  McpOperationService service;
  service.RegisterSession("live-session");
  const McpOperationSnapshot submitted =
      service.Submit("live-session", McpOperationKind::kInvokeRuntimeCommand,
                     stale_plan.progress_total, std::move(stale_plan.executor));
  const McpOperationSnapshot terminal =
      WaitForTerminal(&service, submitted.operation_id);
  BOOST_CHECK(terminal.state == McpOperationState::kFailed);
  BOOST_REQUIRE(terminal.error.has_value());
  BOOST_TEST(terminal.error->code == "run_not_active");
  const SimulationCommandQueueStats stats = queue->Stats();
  BOOST_TEST(stats.size == 0U);
  BOOST_TEST(stats.maximum_size == 0U);
  BOOST_TEST(!queue->TryPop().has_value());

  service.Shutdown();
  application.Shutdown();
}

BOOST_AUTO_TEST_CASE(
    mcp_shutdown_cancels_and_drains_an_admitted_old_run_request) {
  LiveApplicationDirectory temporary;
  const boost::json::object scenario = LiveScenario();
  const auto options =
      std::make_shared<Options>(ParseAndValidateScenario(scenario));
  const RunOwnership ownership =
      CreateRunOwnership("live-application", temporary.path());
  WriteRunOwnershipMarker(ownership);
  WriteText(temporary.path() / "resolved-scenario.json",
            boost::json::serialize(ResolveScenario(scenario)) + "\n");
  AppendLine(
      temporary.path() / "events.jsonl",
      R"({"run_id":"live-application","node_id":"firo-1","event":"run_started"})");

  std::promise<void> request_admitted_promise;
  std::future<void> request_admitted = request_admitted_promise.get_future();
  std::promise<void> release_request_promise;
  const std::shared_future<void> release_request =
      release_request_promise.get_future().share();
  auto queue = std::make_shared<SimulationCommandQueue>();
  McpLiveApplication application(
      McpLiveApplication::Config{.run_id = "live-application",
                                 .run_root = temporary.path(),
                                 .retained_run = std::nullopt,
                                 .options = options,
                                 .command_queue = queue,
                                 .request_run_stop = [] {},
                                 .run_started = {},
                                 .run_stopping = {},
                                 .run_stopped = {},
                                 .request_admitted_test_hook =
                                     [&] {
                                       request_admitted_promise.set_value();
                                       release_request.wait();
                                     }});
  application.MarkRunStarted();
  McpDispatcher dispatcher({}, application.OperationFactory(),
                           application.ResourceReader());
  dispatcher.SessionHandler()("live-session", true, {});

  const boost::json::object submitted =
      Invoke(&dispatcher, "log.follow",
             boost::json::object{{"run_id", "live-application"},
                                 {"node_ids", boost::json::array{"firo-1"}}});
  if (request_admitted.wait_for(2s) != std::future_status::ready) {
    release_request_promise.set_value();
    dispatcher.Shutdown();
    application.Shutdown();
    BOOST_FAIL("old-run request was not admitted");
  }

  std::future<void> shutdown =
      std::async(std::launch::async, [&] { application.Shutdown(); });
  BOOST_CHECK(shutdown.wait_for(20ms) == std::future_status::timeout);
  release_request_promise.set_value();
  BOOST_REQUIRE(shutdown.wait_for(500ms) == std::future_status::ready);
  shutdown.get();

  const boost::json::object terminal = WaitForTerminal(&dispatcher, submitted);
  BOOST_TEST(terminal.at("state").as_string() == "cancelled");
  BOOST_TEST(terminal.if_contains("terminal_result") == nullptr);

  std::filesystem::remove_all(temporary.path());
  std::filesystem::create_directories(temporary.path());
  AppendLine(
      temporary.path() / "events.jsonl",
      R"({"run_id":"live-application","node_id":"replacement","event":"replacement_run_sentinel"})");
  BOOST_TEST(terminal.if_contains("terminal_result") == nullptr);

  dispatcher.Shutdown();
}

BOOST_AUTO_TEST_CASE(
    mcp_retained_application_is_truthful_read_only_and_uses_persisted_status) {
  LiveApplicationDirectory temporary;
  const boost::json::object scenario = LiveScenario();
  WriteText(temporary.path() / "resolved-scenario.json",
            boost::json::serialize(ResolveScenario(scenario)) + "\n");
  AppendLine(
      temporary.path() / "events.jsonl",
      R"({"run_id":"live-application","node_id":"sim","event":"run_started"})");
  AppendLine(
      temporary.path() / "events.jsonl",
      R"({"run_id":"live-application","node_id":"sim","event":"run_failed","detail":"expected retained failure"})");

  McpLiveApplication application(McpLiveApplication::Config{
      .run_id = "live-application",
      .run_root = temporary.path(),
      .retained_run =
          McpLiveApplication::RetainedRun{
              .chain = "firo", .node_count = 1U, .state = "failed"},
      .options = {},
      .command_queue = {},
      .request_run_stop = {},
      .run_started = {},
      .run_stopping = {},
      .run_stopped = {}});
  BOOST_TEST(application.read_only());
  const std::vector<McpOperationKind> supported =
      application.SupportedOperations();
  BOOST_CHECK(std::find(supported.begin(), supported.end(),
                        McpOperationKind::kReportRun) != supported.end());
  BOOST_CHECK(std::find(supported.begin(), supported.end(),
                        McpOperationKind::kStopRun) == supported.end());
  BOOST_CHECK(std::find(supported.begin(), supported.end(),
                        McpOperationKind::kReadArtifact) == supported.end());
  BOOST_CHECK(std::find(supported.begin(), supported.end(),
                        McpOperationKind::kCreateSubscription) ==
              supported.end());
  const std::vector<McpInformationFamily> supported_information =
      application.SupportedInformationFamilies();
  BOOST_CHECK(std::find(supported_information.begin(),
                        supported_information.end(),
                        McpInformationFamily::kArtifacts) ==
              supported_information.end());
  try {
    static_cast<void>(
        application.ResourceReader()(McpInformationFamily::kArtifacts,
                                     "retained-session", std::stop_token{}));
    BOOST_FAIL("unowned retained artifacts were exposed");
  } catch (const McpOperationFailure& failure) {
    BOOST_TEST(failure.code() == "resource_unavailable");
  }

  const boost::json::object registry =
      application
          .ResourceReader()(McpInformationFamily::kRunRegistry,
                            "retained-session", std::stop_token{})
          .as_object();
  BOOST_TEST(registry.at("data")
                 .as_array()
                 .front()
                 .as_object()
                 .at("state")
                 .as_string() == "failed");

  const boost::json::object capabilities =
      application
          .ResourceReader()(McpInformationFamily::kCapabilities,
                            "retained-session", std::stop_token{})
          .as_object()
          .at("data")
          .as_object();
  BOOST_TEST(capabilities.at("access_mode").as_string() == "read_only");
  BOOST_TEST(capabilities.at("operations").as_array().size() ==
             supported.size());

  const boost::json::object notifications =
      application
          .ResourceReader()(McpInformationFamily::kNotifications,
                            "retained-session", std::stop_token{})
          .as_object()
          .at("data")
          .as_object();
  BOOST_TEST(notifications.at("transport").as_string() == "MCP SSE GET stream");
  const boost::json::array& notification_methods =
      notifications.at("methods").as_array();
  BOOST_REQUIRE_EQUAL(notification_methods.size(), 1U);
  BOOST_TEST(notification_methods[0].as_string() ==
             kMcpOperationUpdatedNotification);
  const boost::json::object& notification_schemas =
      notifications.at("schemas").as_object();
  BOOST_REQUIRE_EQUAL(notification_schemas.size(),
                      notification_methods.size());
  BOOST_TEST(!notification_schemas.contains(
      kMcpSubscriptionUpdatedNotification));
  BOOST_TEST(notifications.if_contains("available_through") == nullptr);

  try {
    static_cast<void>(application.OperationFactory()(
        McpOperationKind::kReportRun,
        boost::json::object{{"run_id", "live-application"},
                            {"include_artifacts", true}},
        "retained-session"));
    BOOST_FAIL("unowned retained artifacts were included in a report");
  } catch (const McpOperationFailure& failure) {
    BOOST_TEST(failure.code() == "artifact_unavailable");
  }

  try {
    static_cast<void>(application.OperationFactory()(
        McpOperationKind::kStopRun,
        boost::json::object{{"run_id", "live-application"}},
        "retained-session"));
    BOOST_FAIL("retained mutation was accepted");
  } catch (const McpOperationFailure& failure) {
    BOOST_TEST(failure.code() == "read_only_run");
  }

  McpDispatcher dispatcher({}, application.OperationFactory(),
                           application.ResourceReader());
  dispatcher.SessionHandler()("live-session", true, {});
  const boost::json::object report_terminal = WaitForTerminal(
      &dispatcher, Invoke(&dispatcher, "run.report",
                          boost::json::object{{"run_id", "live-application"}}));
  BOOST_TEST(report_terminal.at("state").as_string() == "succeeded");
  BOOST_TEST(report_terminal.at("terminal_result")
                 .as_object()
                 .at("items")
                 .as_array()
                 .front()
                 .as_object()
                 .at("data")
                 .as_object()
                 .at("status")
                 .as_string() == "failed");
  dispatcher.Shutdown();
  application.Shutdown();
}

BOOST_AUTO_TEST_CASE(
    mcp_live_application_pages_owned_evidence_logs_and_safe_artifacts) {
  LiveApplicationDirectory temporary;
  const boost::json::object scenario = LiveScenario();
  const auto options =
      std::make_shared<Options>(ParseAndValidateScenario(scenario));
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

  auto queue = std::make_shared<SimulationCommandQueue>();
  McpLiveApplication application(
      McpLiveApplication::Config{.run_id = "live-application",
                                 .run_root = temporary.path(),
                                 .retained_run = std::nullopt,
                                 .options = options,
                                 .command_queue = queue,
                                 .request_run_stop = [] {},
                                 .run_started = {},
                                 .run_stopping = {},
                                 .run_stopped = {}});
  application.MarkRunStarted();
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

BOOST_AUTO_TEST_CASE(mcp_evidence_rejects_records_from_a_different_run) {
  LiveApplicationDirectory temporary;
  const RunOwnership ownership =
      CreateRunOwnership("live-application", temporary.path());
  WriteRunOwnershipMarker(ownership);
  AppendLine(temporary.path() / "events.jsonl",
             R"({"run_id":"other","node_id":"sim","event":"run_failed"})");
  McpRunEvidenceQuery query;
  query.families = {McpInformationFamily::kEvents};

  BOOST_CHECK_EXCEPTION(
      QueryMcpRunEvidence("live-application", temporary.path(), query),
      std::runtime_error, [](const std::runtime_error& error) {
        return std::string(error.what())
                   .find("record run_id does not match the selected run") !=
               std::string::npos;
      });
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
