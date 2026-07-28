#include <unistd.h>

#include <algorithm>
#include <array>
#include <atomic>
#include <boost/json/object.hpp>
#include <boost/json/serialize.hpp>
#include <boost/test/unit_test.hpp>
#include <chrono>
#include <cstdint>
#include <filesystem>
#include <future>
#include <limits>
#include <memory>
#include <mutex>
#include <stdexcept>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include "bbp/drivers/chain_driver_registry.h"
#ifdef BBP_FIRO_GUI_LAUNCHER
#include "bbp/drivers/firo_gui_launcher.h"
#endif
#include "bbp/mcp_dispatcher.h"
#include "bbp/mcp_live_application.h"
#include "bbp/mcp_run_evidence.h"
#include "bbp/operator_connection.h"
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

McpLiveNodeInventorySnapshot InitialInventory(const Options& options) {
  McpLiveNodeInventorySnapshot result{.generation = 1U,
                                      .node_ids = options.node_ids};
  if (result.node_ids.empty()) {
    const std::string& prefix =
        ChainDriverSpecFor(options.chain).node_id_prefix;
    result.node_ids.reserve(options.nodes);
    for (std::uint32_t index = 0U; index < options.nodes; ++index) {
      result.node_ids.push_back(prefix + "-" + std::to_string(index + 1U));
    }
  }
  return result;
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
                                  .node_lifecycle = std::move(node_lifecycle),
                                  .added_node_ids = {},
                                  .removed_node_ids = {},
                                  .inventory_generation = std::nullopt,
                                  .final_node_count = std::nullopt};
}

SimulationCommandOutcome NodeAddOutcome(std::vector<std::string> node_ids,
                                        std::uint64_t inventory_generation,
                                        std::uint32_t final_node_count) {
  return SimulationCommandOutcome{
      .state = SimulationCommandOutcomeState::kSucceeded,
      .cancellation_cause = SimulationCommandCancellationCause::kNone,
      .error = std::nullopt,
      .node_lifecycle = std::nullopt,
      .added_node_ids = std::move(node_ids),
      .removed_node_ids = {},
      .inventory_generation = inventory_generation,
      .final_node_count = final_node_count};
}

SimulationCommandOutcome NodeRemoveOutcome(std::vector<std::string> node_ids,
                                           std::uint64_t inventory_generation,
                                           std::uint32_t final_node_count) {
  return SimulationCommandOutcome{
      .state = SimulationCommandOutcomeState::kSucceeded,
      .cancellation_cause = SimulationCommandCancellationCause::kNone,
      .error = std::nullopt,
      .node_lifecycle = std::nullopt,
      .added_node_ids = {},
      .removed_node_ids = std::move(node_ids),
      .inventory_generation = inventory_generation,
      .final_node_count = final_node_count};
}

SimulationCommandOutcome NodeReplaceOutcome(std::uint64_t inventory_generation,
                                            std::uint32_t final_node_count) {
  return SimulationCommandOutcome{
      .state = SimulationCommandOutcomeState::kSucceeded,
      .cancellation_cause = SimulationCommandCancellationCause::kNone,
      .error = std::nullopt,
      .node_lifecycle = "running",
      .added_node_ids = {},
      .removed_node_ids = {},
      .inventory_generation = inventory_generation,
      .final_node_count = final_node_count};
}

void MarkNodeAddCommitted(const SimulationCommand& command,
                          std::uint64_t initial_generation,
                          std::vector<std::string> initial_node_ids) {
  BOOST_REQUIRE(command.operation_control);
  BOOST_TEST(command.operation_control->RecordInitialInventory(
      initial_generation, std::move(initial_node_ids)));
  BOOST_TEST(command.operation_control->TryBeginCommit());
  command.operation_control->MarkCommitted();
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
      .node_inventory_snapshot =
          [options] { return InitialInventory(*options); },
      .publication_mutex = {},
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

  const boost::json::object tui_outcome_subscription =
      Invoke(&dispatcher, "subscription.create",
             boost::json::object{{"run_id", "live-application"},
                                 {"families", boost::json::array{"events"}}});
  SimulationCommand tui_command;
  tui_command.sequence = 99U;
  tui_command.kind = SimulationCommandKind::kIncreaseLogVerbosity;
  tui_command.node_id = "firo-1";
  tui_command.confirmed = true;
  application.RecordCommandOutcome(
      tui_command, CommandOutcome(SimulationCommandOutcomeState::kSucceeded));
  const boost::json::object tui_outcome_page = Invoke(
      &dispatcher, "subscription.poll",
      boost::json::object{
          {"subscription_id", tui_outcome_subscription.at("subscription_id")},
          {"cursor", "0"},
          {"limit", 8U}});
  BOOST_REQUIRE_EQUAL(tui_outcome_page.at("items").as_array().size(), 1U);
  const boost::json::object& tui_outcome =
      tui_outcome_page.at("items").as_array().front().as_object();
  BOOST_TEST(tui_outcome.at("kind").as_string() == "command_outcome");
  BOOST_TEST(tui_outcome.at("data").as_object().at("command_id").as_string() ==
             "command-99");
  BOOST_TEST(tui_outcome.at("data").as_object().at("state").as_string() ==
             "succeeded");

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
    mcp_node_add_reports_progress_exact_inventory_and_generic_parity) {
  LiveApplicationDirectory temporary;
  const auto options =
      std::make_shared<Options>(ParseAndValidateScenario(LiveScenario()));
  auto queue = std::make_shared<SimulationCommandQueue>();
  std::atomic_uint32_t run_stop_requests = 0U;
  std::atomic_uint64_t inventory_reads = 0U;
  std::mutex inventory_mutex;
  std::mutex evidence_mutex;
  std::vector<McpEvidenceRecord> evidence;
  McpLiveNodeInventorySnapshot inventory = InitialInventory(*options);
  const auto publish_inventory = [&](std::uint64_t generation,
                                     std::vector<std::string> node_ids) {
    std::lock_guard<std::mutex> lock(inventory_mutex);
    inventory.generation = generation;
    inventory.node_ids = std::move(node_ids);
  };
  McpLiveApplication application(McpLiveApplication::Config{
      .run_id = "live-application",
      .run_root = temporary.path(),
      .retained_run = std::nullopt,
      .options = options,
      .command_queue = queue,
      .node_inventory_snapshot =
          [&] {
            inventory_reads.fetch_add(1U, std::memory_order_relaxed);
            std::lock_guard<std::mutex> lock(inventory_mutex);
            return inventory;
          },
      .publication_mutex = {},
      .request_run_stop = [&] { ++run_stop_requests; },
      .run_started = {},
      .run_stopping = {},
      .run_stopped = {},
      .publish_evidence =
          [&](McpEvidenceRecord record) {
            std::lock_guard<std::mutex> lock(evidence_mutex);
            evidence.push_back(std::move(record));
          },
      .close_run_subscriptions = [](std::string_view) {}});
  application.MarkRunStarted();
  McpDispatcher dispatcher({}, application.OperationFactory(),
                           application.ResourceReader());
  dispatcher.SessionHandler()("live-session", true, {});

  const boost::json::object direct_submitted = Invoke(
      &dispatcher, "node.add",
      boost::json::object{
          {"run_id", "live-application"},
          {"request",
           boost::json::object{
               {"chain", "firo"},
               {"count", 2U},
               {"node_ids", boost::json::array{"added-a", "added-b"}}}}});
  const SimulationCommand direct_command = WaitForQueuedCommand(queue.get());
  BOOST_REQUIRE(direct_command.operation_control);
  for (std::uint64_t phase = 1U; phase < kSimulationNodeAddProgressTotal;
       ++phase) {
    BOOST_TEST(direct_command.operation_control->ReportProgress(phase));
    const auto deadline = std::chrono::steady_clock::now() + 500ms;
    std::uint64_t observed = 0U;
    while (std::chrono::steady_clock::now() < deadline) {
      const boost::json::object snapshot =
          Invoke(&dispatcher, "operation.get",
                 boost::json::object{
                     {"operation_id", direct_submitted.at("operation_id")}});
      observed = snapshot.at("progress_completed").as_uint64();
      if (observed >= phase) {
        break;
      }
      std::this_thread::sleep_for(1ms);
    }
    BOOST_TEST(observed == phase);
  }
  BOOST_TEST(!direct_command.operation_control->ReportProgress(3U));
  BOOST_TEST(!direct_command.operation_control->ReportProgress(
      kSimulationNodeAddProgressTotal + 1U));
  BOOST_TEST(direct_command.operation_control->progress_completed.load(
                 std::memory_order_acquire) ==
             kSimulationNodeAddProgressTotal - 1U);
  BOOST_TEST(direct_command.operation_control->ReportProgress(
      kSimulationNodeAddProgressTotal));
  publish_inventory(2U, {"firo-1", "added-a", "added-b"});
  MarkNodeAddCommitted(direct_command, 1U, {"firo-1"});
  application.RecordCommandOutcome(
      direct_command, NodeAddOutcome({"added-a", "added-b"}, 2U, 3U));
  {
    std::lock_guard<std::mutex> lock(evidence_mutex);
    BOOST_REQUIRE(!evidence.empty());
    const McpEvidenceRecord& outcome_evidence = evidence.back();
    BOOST_REQUIRE(outcome_evidence.data);
    BOOST_TEST(outcome_evidence.kind.value_or("") == "command_outcome");
    BOOST_TEST(outcome_evidence.data->as_object()
                   .at("inventory_generation")
                   .as_uint64() == 2U);
    BOOST_TEST(
        outcome_evidence.data->as_object().at("final_node_count").as_uint64() ==
        3U);
  }
  const boost::json::object direct_terminal =
      WaitForTerminal(&dispatcher, direct_submitted);
  BOOST_TEST(direct_terminal.at("state").as_string() == "succeeded");
  BOOST_TEST(direct_terminal.at("progress_completed").as_uint64() ==
             kSimulationNodeAddProgressTotal);
  const boost::json::object& direct_result =
      direct_terminal.at("terminal_result").as_object();
  BOOST_TEST(direct_result.at("result_family").as_string() == "mutation");
  BOOST_TEST(direct_result.at("action").as_string() == "node.add");
  BOOST_TEST(direct_result.at("added_node_ids").as_array().size() == 2U);
  BOOST_TEST(direct_result.at("affected_node_ids").as_array() ==
             direct_result.at("added_node_ids").as_array());
  BOOST_TEST(direct_result.at("removed_node_ids").as_array().empty());
  BOOST_TEST(direct_result.at("inventory_generation").as_uint64() == 2U);
  BOOST_TEST(direct_result.at("final_node_count").as_uint64() == 3U);
  BOOST_TEST(!direct_result.at("unchanged").as_bool());

  const boost::json::object generic_submitted =
      Invoke(&dispatcher, "simulation.command",
             boost::json::object{
                 {"run_id", "live-application"},
                 {"command",
                  boost::json::object{
                      {"kind", "add_nodes"},
                      {"node_add",
                       boost::json::object{
                           {"chain", "firo"},
                           {"count", 1U},
                           {"node_ids", boost::json::array{"added-c"}}}}}}});
  const SimulationCommand generic_command = WaitForQueuedCommand(queue.get());
  BOOST_REQUIRE(generic_command.operation_control);
  BOOST_TEST(generic_command.operation_control->ReportProgress(
      kSimulationNodeAddProgressTotal));
  publish_inventory(3U, {"firo-1", "added-a", "added-b", "added-c"});
  MarkNodeAddCommitted(generic_command, 2U, {"firo-1", "added-a", "added-b"});
  application.RecordCommandOutcome(generic_command,
                                   NodeAddOutcome({"added-c"}, 3U, 4U));
  const boost::json::object generic_terminal =
      WaitForTerminal(&dispatcher, generic_submitted);
  BOOST_TEST(generic_terminal.at("state").as_string() == "succeeded");
  const boost::json::object& generic_result =
      generic_terminal.at("terminal_result").as_object();
  BOOST_TEST(generic_result.at("result_family").as_string() ==
             "runtime_command");
  BOOST_TEST(generic_result.at("action").as_string() == "node.add");
  BOOST_TEST(
      generic_result.at("added_node_ids").as_array().front().as_string() ==
      "added-c");
  BOOST_TEST(generic_result.at("final_node_count").as_uint64() == 4U);
  BOOST_TEST(generic_result.at("inventory_generation").as_uint64() == 3U);

  const boost::json::object capabilities =
      application
          .ResourceReader()(McpInformationFamily::kCapabilities, "live-session",
                            std::stop_token{})
          .as_object()
          .at("data")
          .as_object()
          .at("current_run")
          .as_object();
  BOOST_TEST(capabilities.at("node_count").as_uint64() == 4U);
  BOOST_TEST(capabilities.at("node_capacity").as_uint64() == 16U);
  BOOST_TEST(capabilities.at("chain_node_maximum").as_uint64() == 16U);
  BOOST_TEST(capabilities.at("available_node_capacity").as_uint64() == 12U);
  const std::uint64_t registry_reads_before =
      inventory_reads.load(std::memory_order_relaxed);
  const boost::json::value run_registry = application.ResourceReader()(
      McpInformationFamily::kRunRegistry, "live-session", std::stop_token{});
  const boost::json::object& run_registry_entry =
      run_registry.as_object().at("data").as_array().front().as_object();
  BOOST_TEST(inventory_reads.load(std::memory_order_relaxed) ==
             registry_reads_before + 1U);
  BOOST_TEST(run_registry_entry.at("node_count").as_uint64() == 4U);
  BOOST_TEST(run_registry_entry.at("available_node_capacity").as_uint64() ==
             12U);

  const boost::json::object over_capacity = WaitForTerminal(
      &dispatcher,
      Invoke(&dispatcher, "node.add",
             boost::json::object{
                 {"run_id", "live-application"},
                 {"request",
                  boost::json::object{{"chain", "firo"}, {"count", 13U}}}}));
  BOOST_TEST(over_capacity.at("state").as_string() == "failed");
  const boost::json::object& capacity_error =
      over_capacity.at("terminal_error").as_object();
  BOOST_TEST(capacity_error.at("code").as_string() == "node_capacity_exceeded");
  const boost::json::object& capacity_diagnostic =
      capacity_error.at("diagnostics").as_array().front().as_object();
  BOOST_TEST(capacity_diagnostic.at("requested_count").as_uint64() == 13U);
  BOOST_TEST(capacity_diagnostic.at("current_node_count").as_uint64() == 4U);
  BOOST_TEST(capacity_diagnostic.at("node_capacity").as_uint64() == 16U);
  BOOST_TEST(capacity_diagnostic.at("available_node_capacity").as_uint64() ==
             12U);
  BOOST_TEST(!queue->TryPop().has_value());
  const boost::json::object generic_over_capacity = WaitForTerminal(
      &dispatcher,
      Invoke(&dispatcher, "simulation.command",
             boost::json::object{
                 {"run_id", "live-application"},
                 {"command",
                  boost::json::object{
                      {"kind", "add_nodes"},
                      {"node_add", boost::json::object{{"chain", "firo"},
                                                       {"count", 13U}}}}}}));
  BOOST_TEST(generic_over_capacity.at("state").as_string() == "failed");
  const boost::json::object& generic_capacity_error =
      generic_over_capacity.at("terminal_error").as_object();
  BOOST_TEST(generic_capacity_error.at("code").as_string() ==
             "node_capacity_exceeded");
  BOOST_TEST(generic_capacity_error.at("diagnostics")
                 .as_array()
                 .front()
                 .as_object()
                 .at("available_node_capacity")
                 .as_uint64() == 12U);
  BOOST_TEST(!queue->TryPop().has_value());

  const boost::json::object unavailable_submitted = Invoke(
      &dispatcher, "node.add",
      boost::json::object{
          {"run_id", "live-application"},
          {"request", boost::json::object{{"chain", "firo"}, {"count", 1U}}}});
  const SimulationCommand unavailable_command =
      WaitForQueuedCommand(queue.get());
  BOOST_REQUIRE(unavailable_command.operation_control);
  unavailable_command.operation_control->RecordNodeResourceFailure(
      SimulationNodeResourceFailure{
          .resource_kind = "tcp_port",
          .node_id = "firo-5",
          .address = "0.0.0.0",
          .port = 18172U,
          .purpose = "P2P",
          .mutation_started = false,
      });
  application.RecordCommandOutcome(
      unavailable_command,
      CommandOutcome(SimulationCommandOutcomeState::kFailed,
                     "node-add P2P endpoint is unavailable"));
  const boost::json::object unavailable_terminal =
      WaitForTerminal(&dispatcher, unavailable_submitted);
  BOOST_TEST(unavailable_terminal.at("state").as_string() == "failed");
  const boost::json::object& unavailable_error =
      unavailable_terminal.at("terminal_error").as_object();
  BOOST_TEST(unavailable_error.at("code").as_string() ==
             "node_resource_unavailable");
  const boost::json::object& unavailable_diagnostic =
      unavailable_error.at("diagnostics").as_array().front().as_object();
  BOOST_TEST(unavailable_diagnostic.at("resource_kind").as_string() ==
             "tcp_port");
  BOOST_TEST(unavailable_diagnostic.at("node_id").as_string() == "firo-5");
  BOOST_TEST(unavailable_diagnostic.at("address").as_string() == "0.0.0.0");
  BOOST_TEST(unavailable_diagnostic.at("port").as_uint64() == 18172U);
  BOOST_TEST(unavailable_diagnostic.at("purpose").as_string() == "P2P");
  BOOST_TEST(!unavailable_diagnostic.at("mutation_started").as_bool());

  const auto require_unconfirmed =
      [&](boost::json::object request, std::vector<std::string> outcome_ids,
          std::uint64_t generation, std::uint32_t final_count) {
        const boost::json::object submitted =
            Invoke(&dispatcher, "node.add",
                   boost::json::object{{"run_id", "live-application"},
                                       {"request", std::move(request)}});
        const SimulationCommand command = WaitForQueuedCommand(queue.get());
        MarkNodeAddCommitted(command, 3U,
                             {"firo-1", "added-a", "added-b", "added-c"});
        application.RecordCommandOutcome(
            command,
            NodeAddOutcome(std::move(outcome_ids), generation, final_count));
        const boost::json::object terminal =
            WaitForTerminal(&dispatcher, submitted);
        BOOST_TEST(terminal.at("state").as_string() == "failed");
        BOOST_TEST(
            terminal.at("terminal_error").as_object().at("code").as_string() ==
            "node_outcome_unconfirmed");
        BOOST_TEST(application.current_node_count() == 4U);
      };
  require_unconfirmed(boost::json::object{{"chain", "firo"}, {"count", 1U}},
                      {"stale-generation"}, 3U, 5U);
  require_unconfirmed(boost::json::object{{"chain", "firo"}, {"count", 1U}},
                      {"skipped-generation"}, 5U, 5U);
  require_unconfirmed(boost::json::object{{"chain", "firo"}, {"count", 1U}},
                      {"bad/id"}, 4U, 5U);
  require_unconfirmed(boost::json::object{{"chain", "firo"}, {"count", 2U}},
                      {"duplicate", "duplicate"}, 4U, 6U);
  require_unconfirmed(
      boost::json::object{{"chain", "firo"},
                          {"count", 1U},
                          {"node_ids", boost::json::array{"requested"}}},
      {"different"}, 4U, 5U);
  BOOST_TEST(run_stop_requests.load(std::memory_order_acquire) >= 5U);

  dispatcher.Shutdown();
  application.Shutdown();
}

BOOST_AUTO_TEST_CASE(
    mcp_node_add_reconciliation_terminalizes_inventory_failures_and_preserves_existing_ids) {
  LiveApplicationDirectory temporary;
  const auto options =
      std::make_shared<Options>(ParseAndValidateScenario(LiveScenario()));
  auto queue = std::make_shared<SimulationCommandQueue>();
  std::mutex inventory_mutex;
  McpLiveNodeInventorySnapshot inventory = InitialInventory(*options);
  bool inventory_read_fails = false;
  std::atomic_uint32_t run_stop_requests = 0U;
  const auto publish_inventory = [&](std::uint64_t generation,
                                     std::vector<std::string> node_ids) {
    std::lock_guard<std::mutex> lock(inventory_mutex);
    inventory.generation = generation;
    inventory.node_ids = std::move(node_ids);
  };
  McpLiveApplication application(McpLiveApplication::Config{
      .run_id = "live-application",
      .run_root = temporary.path(),
      .retained_run = std::nullopt,
      .options = options,
      .command_queue = queue,
      .node_inventory_snapshot =
          [&] {
            std::lock_guard<std::mutex> lock(inventory_mutex);
            if (inventory_read_fails) {
              throw std::runtime_error("expected inventory read failure");
            }
            return inventory;
          },
      .publication_mutex = {},
      .request_run_stop = [&] { ++run_stop_requests; },
      .run_started = {},
      .run_stopping = {},
      .run_stopped = {}});
  application.MarkRunStarted();
  McpDispatcher dispatcher({}, application.OperationFactory(),
                           application.ResourceReader());
  dispatcher.SessionHandler()("live-session", true, {});

  const boost::json::object unreadable_submitted = Invoke(
      &dispatcher, "node.add",
      boost::json::object{
          {"run_id", "live-application"},
          {"request",
           boost::json::object{{"chain", "firo"},
                               {"count", 1U},
                               {"node_ids", boost::json::array{"firo-2"}}}}});
  const SimulationCommand unreadable_command =
      WaitForQueuedCommand(queue.get());
  MarkNodeAddCommitted(unreadable_command, 1U, {"firo-1"});
  publish_inventory(2U, {"firo-1", "firo-2"});
  {
    std::lock_guard<std::mutex> lock(inventory_mutex);
    inventory_read_fails = true;
  }
  application.RecordCommandOutcome(unreadable_command,
                                   NodeAddOutcome({"firo-2"}, 2U, 2U));
  {
    std::lock_guard<std::mutex> lock(inventory_mutex);
    inventory_read_fails = false;
  }
  const boost::json::object unreadable_terminal =
      WaitForTerminal(&dispatcher, unreadable_submitted);
  BOOST_TEST(unreadable_terminal.at("state").as_string() == "failed");
  BOOST_TEST(unreadable_terminal.at("terminal_error")
                 .as_object()
                 .at("code")
                 .as_string() == "node_outcome_unconfirmed");

  const boost::json::object cancelled_submitted = Invoke(
      &dispatcher, "node.add",
      boost::json::object{
          {"run_id", "live-application"},
          {"request", boost::json::object{{"chain", "firo"}, {"count", 1U}}}});
  const SimulationCommand cancelled_command = WaitForQueuedCommand(queue.get());
  BOOST_REQUIRE(cancelled_command.operation_control);
  BOOST_TEST(cancelled_command.operation_control->RequestCancellation(
      SimulationCommandCancellationCause::kClientCancel));
  application.RecordCommandOutcome(
      cancelled_command,
      CommandOutcome(SimulationCommandOutcomeState::kFailed,
                     "failure published after cancellation won"));
  const boost::json::object cancelled_terminal =
      WaitForTerminal(&dispatcher, cancelled_submitted);
  BOOST_TEST(cancelled_terminal.at("state").as_string() == "failed");
  BOOST_TEST(cancelled_terminal.at("terminal_error")
                 .as_object()
                 .at("code")
                 .as_string() == "node_outcome_unconfirmed");

  const boost::json::object replaced_submitted = Invoke(
      &dispatcher, "node.add",
      boost::json::object{
          {"run_id", "live-application"},
          {"request",
           boost::json::object{{"chain", "firo"},
                               {"count", 1U},
                               {"node_ids", boost::json::array{"firo-3"}}}}});
  const SimulationCommand replaced_command = WaitForQueuedCommand(queue.get());
  MarkNodeAddCommitted(replaced_command, 2U, {"firo-1", "firo-2"});
  publish_inventory(3U, {"replacement", "firo-2", "firo-3"});
  application.RecordCommandOutcome(replaced_command,
                                   NodeAddOutcome({"firo-3"}, 3U, 3U));
  const boost::json::object replaced_terminal =
      WaitForTerminal(&dispatcher, replaced_submitted);
  BOOST_TEST(replaced_terminal.at("state").as_string() == "failed");
  BOOST_TEST(replaced_terminal.at("terminal_error")
                 .as_object()
                 .at("code")
                 .as_string() == "node_outcome_unconfirmed");
  BOOST_TEST(run_stop_requests.load(std::memory_order_acquire) >= 3U);

  dispatcher.Shutdown();
  application.Shutdown();
}

BOOST_AUTO_TEST_CASE(
    mcp_node_replace_has_direct_generic_progress_failure_and_inventory_parity) {
  LiveApplicationDirectory temporary;
  boost::json::object scenario = LiveScenario();
  scenario["nodes"] = 2U;
  scenario["node_capacity"] = 2U;
  const auto options =
      std::make_shared<Options>(ParseAndValidateScenario(scenario));
  auto queue = std::make_shared<SimulationCommandQueue>();
  std::mutex inventory_mutex;
  McpLiveNodeInventorySnapshot inventory = InitialInventory(*options);
  std::atomic_uint32_t run_stop_requests = 0U;
  const auto publish_inventory = [&](std::uint64_t generation,
                                     std::vector<std::string> node_ids) {
    std::lock_guard<std::mutex> lock(inventory_mutex);
    inventory.generation = generation;
    inventory.node_ids = std::move(node_ids);
  };
  McpLiveApplication application(McpLiveApplication::Config{
      .run_id = "live-application",
      .run_root = temporary.path(),
      .retained_run = std::nullopt,
      .options = options,
      .command_queue = queue,
      .node_inventory_snapshot =
          [&] {
            std::lock_guard<std::mutex> lock(inventory_mutex);
            return inventory;
          },
      .publication_mutex = {},
      .request_run_stop = [&] { ++run_stop_requests; },
      .run_started = {},
      .run_stopping = {},
      .run_stopped = {}});
  application.MarkRunStarted();
  McpDispatcher dispatcher({}, application.OperationFactory(),
                           application.ResourceReader());
  dispatcher.SessionHandler()("live-session", true, {});

  const boost::json::object replacement{
      {"chain", "firo"},
      {"count", 1U},
      {"node_ids", boost::json::array{"firo-2"}},
      {"binary", "/bin/true"},
      {"ready_timeout_sec", 11U},
      {"sync_timeout_sec", 13U}};
  const boost::json::object direct_submitted =
      Invoke(&dispatcher, "node.replace",
             boost::json::object{{"run_id", "live-application"},
                                 {"node_id", "firo-2"},
                                 {"replacement", replacement}});
  const SimulationCommand direct_command = WaitForQueuedCommand(queue.get());
  BOOST_CHECK(direct_command.kind == SimulationCommandKind::kReplaceNode);
  BOOST_TEST(direct_command.node_id == "firo-2");
  BOOST_REQUIRE(direct_command.node_replace);
  BOOST_TEST(direct_command.node_replace->count == 1U);
  BOOST_REQUIRE(direct_command.operation_control);
  for (std::uint64_t phase = 1U; phase <= kSimulationNodeAddProgressTotal;
       ++phase) {
    BOOST_TEST(direct_command.operation_control->ReportProgress(phase));
  }
  publish_inventory(2U, {"firo-1", "firo-2"});
  MarkNodeAddCommitted(direct_command, 1U, {"firo-1", "firo-2"});
  application.RecordCommandOutcome(direct_command, NodeReplaceOutcome(2U, 2U));
  const boost::json::object direct_terminal =
      WaitForTerminal(&dispatcher, direct_submitted);
  BOOST_TEST(direct_terminal.at("state").as_string() == "succeeded");
  BOOST_TEST(direct_terminal.at("progress_total").as_uint64() ==
             kSimulationNodeAddProgressTotal);
  BOOST_TEST(direct_terminal.at("progress_completed").as_uint64() ==
             kSimulationNodeAddProgressTotal);
  const boost::json::object& direct_result =
      direct_terminal.at("terminal_result").as_object();
  BOOST_TEST(direct_result.at("result_family").as_string() == "mutation");
  BOOST_TEST(direct_result.at("action").as_string() == "node.replace");
  BOOST_TEST(direct_result.at("state").as_string() == "running");
  BOOST_TEST(direct_result.at("added_node_ids").as_array().empty());
  BOOST_TEST(direct_result.at("removed_node_ids").as_array().empty());
  BOOST_TEST(
      direct_result.at("affected_node_ids").as_array().front().as_string() ==
      "firo-2");
  BOOST_TEST(direct_result.at("inventory_generation").as_uint64() == 2U);
  BOOST_TEST(direct_result.at("final_node_count").as_uint64() == 2U);

  const boost::json::object generic_submitted = Invoke(
      &dispatcher, "simulation.command",
      boost::json::object{
          {"run_id", "live-application"},
          {"command", boost::json::object{{"kind", "replace_node"},
                                          {"node", "firo-2"},
                                          {"node_replace", replacement}}}});
  const SimulationCommand generic_command = WaitForQueuedCommand(queue.get());
  BOOST_CHECK(generic_command.kind == SimulationCommandKind::kReplaceNode);
  publish_inventory(3U, {"firo-1", "firo-2"});
  MarkNodeAddCommitted(generic_command, 2U, {"firo-1", "firo-2"});
  BOOST_TEST(generic_command.operation_control->ReportProgress(
      kSimulationNodeAddProgressTotal));
  application.RecordCommandOutcome(generic_command, NodeReplaceOutcome(3U, 2U));
  const boost::json::object generic_terminal =
      WaitForTerminal(&dispatcher, generic_submitted);
  BOOST_TEST(generic_terminal.at("state").as_string() == "succeeded");
  const boost::json::object& generic_result =
      generic_terminal.at("terminal_result").as_object();
  BOOST_TEST(generic_result.at("result_family").as_string() ==
             "runtime_command");
  BOOST_TEST(generic_result.at("action").as_string() == "node.replace");
  BOOST_TEST(
      generic_result.at("affected_node_ids").as_array().front().as_string() ==
      "firo-2");
  BOOST_TEST(generic_result.at("inventory_generation").as_uint64() == 3U);
  BOOST_TEST(generic_result.at("final_node_count").as_uint64() == 2U);

  const boost::json::object failed_submitted =
      Invoke(&dispatcher, "node.replace",
             boost::json::object{{"run_id", "live-application"},
                                 {"node_id", "firo-2"},
                                 {"replacement", replacement}});
  const SimulationCommand failed_command = WaitForQueuedCommand(queue.get());
  application.RecordCommandOutcome(
      failed_command, CommandOutcome(SimulationCommandOutcomeState::kFailed,
                                     "candidate readiness failed", "running"));
  const boost::json::object failed_terminal =
      WaitForTerminal(&dispatcher, failed_submitted);
  BOOST_TEST(failed_terminal.at("state").as_string() == "failed");
  BOOST_TEST(
      failed_terminal.at("terminal_error").as_object().at("code").as_string() ==
      "node_replace_failed");

  const boost::json::object cancelled_submitted =
      Invoke(&dispatcher, "node.replace",
             boost::json::object{{"run_id", "live-application"},
                                 {"node_id", "firo-2"},
                                 {"replacement", replacement}});
  const SimulationCommand cancelled_command = WaitForQueuedCommand(queue.get());
  BOOST_REQUIRE(cancelled_command.operation_control);
  BOOST_TEST(cancelled_command.operation_control->RequestCancellation(
      SimulationCommandCancellationCause::kClientCancel));
  application.RecordCommandOutcome(
      cancelled_command,
      CommandOutcome(SimulationCommandOutcomeState::kCancelled, std::nullopt,
                     "running",
                     SimulationCommandCancellationCause::kClientCancel));
  const boost::json::object cancelled_terminal =
      WaitForTerminal(&dispatcher, cancelled_submitted);
  BOOST_TEST(cancelled_terminal.at("state").as_string() == "cancelled");
  BOOST_TEST(run_stop_requests.load(std::memory_order_acquire) == 0U);

  const boost::json::object unconfirmed_submitted =
      Invoke(&dispatcher, "node.replace",
             boost::json::object{{"run_id", "live-application"},
                                 {"node_id", "firo-2"},
                                 {"replacement", replacement}});
  const SimulationCommand unconfirmed_command =
      WaitForQueuedCommand(queue.get());
  MarkNodeAddCommitted(unconfirmed_command, 3U, {"firo-1", "firo-2"});
  publish_inventory(4U, {"replacement", "firo-2"});
  application.RecordCommandOutcome(unconfirmed_command,
                                   NodeReplaceOutcome(4U, 2U));
  const boost::json::object unconfirmed_terminal =
      WaitForTerminal(&dispatcher, unconfirmed_submitted);
  BOOST_TEST(unconfirmed_terminal.at("state").as_string() == "failed");
  BOOST_TEST(unconfirmed_terminal.at("terminal_error")
                 .as_object()
                 .at("code")
                 .as_string() == "node_outcome_unconfirmed");
  BOOST_TEST(run_stop_requests.load(std::memory_order_acquire) >= 1U);

  dispatcher.Shutdown();
  application.Shutdown();
}

BOOST_AUTO_TEST_CASE(
    mcp_node_remove_has_direct_generic_cancellation_and_inventory_parity) {
  LiveApplicationDirectory temporary;
  boost::json::object scenario = LiveScenario();
  scenario["nodes"] = 4U;
  const auto options =
      std::make_shared<Options>(ParseAndValidateScenario(scenario));
  auto queue = std::make_shared<SimulationCommandQueue>();
  std::mutex inventory_mutex;
  McpLiveNodeInventorySnapshot inventory = InitialInventory(*options);
  std::atomic_uint32_t run_stop_requests = 0U;
  const auto publish_inventory = [&](std::uint64_t generation,
                                     std::vector<std::string> node_ids) {
    std::lock_guard<std::mutex> lock(inventory_mutex);
    inventory.generation = generation;
    inventory.node_ids = std::move(node_ids);
  };
  McpLiveApplication application(McpLiveApplication::Config{
      .run_id = "live-application",
      .run_root = temporary.path(),
      .retained_run = std::nullopt,
      .options = options,
      .command_queue = queue,
      .node_inventory_snapshot =
          [&] {
            std::lock_guard<std::mutex> lock(inventory_mutex);
            return inventory;
          },
      .publication_mutex = {},
      .request_run_stop = [&] { ++run_stop_requests; },
      .run_started = {},
      .run_stopping = {},
      .run_stopped = {}});
  application.MarkRunStarted();
  McpDispatcher dispatcher({}, application.OperationFactory(),
                           application.ResourceReader());
  dispatcher.SessionHandler()("live-session", true, {});

  const boost::json::object direct_submitted = Invoke(
      &dispatcher, "node.remove",
      boost::json::object{{"run_id", "live-application"},
                          {"node_ids", boost::json::array{"firo-2", "firo-4"}},
                          {"timeout_sec", 45U}});
  const SimulationCommand direct_command = WaitForQueuedCommand(queue.get());
  BOOST_CHECK(direct_command.kind == SimulationCommandKind::kRemoveNodes);
  BOOST_REQUIRE(direct_command.node_remove);
  BOOST_TEST(direct_command.node_remove->node_ids ==
                 std::vector<std::string>({"firo-2", "firo-4"}),
             boost::test_tools::per_element());
  BOOST_TEST(direct_command.node_remove->timeout_sec == 45U);
  publish_inventory(2U, {"firo-1", "firo-3"});
  MarkNodeAddCommitted(direct_command, 1U,
                       {"firo-1", "firo-2", "firo-3", "firo-4"});
  BOOST_TEST(!direct_command.operation_control->RequestCancellation(
      SimulationCommandCancellationCause::kClientCancel));
  BOOST_TEST(direct_command.operation_control->ReportProgress(
      kSimulationNodeAddProgressTotal));
  application.RecordCommandOutcome(
      direct_command, NodeRemoveOutcome({"firo-2", "firo-4"}, 2U, 2U));
  const boost::json::object direct_terminal =
      WaitForTerminal(&dispatcher, direct_submitted);
  BOOST_TEST(direct_terminal.at("state").as_string() == "succeeded");
  const boost::json::object& direct_result =
      direct_terminal.at("terminal_result").as_object();
  BOOST_TEST(direct_result.at("result_family").as_string() == "mutation");
  BOOST_TEST(direct_result.at("action").as_string() == "node.remove");
  BOOST_TEST(direct_result.at("added_node_ids").as_array().empty());
  BOOST_TEST(direct_result.at("removed_node_ids").as_array().size() == 2U);
  BOOST_TEST(direct_result.at("affected_node_ids").as_array() ==
             direct_result.at("removed_node_ids").as_array());
  BOOST_TEST(direct_result.at("inventory_generation").as_uint64() == 2U);
  BOOST_TEST(direct_result.at("final_node_count").as_uint64() == 2U);

  const boost::json::object generic_submitted = Invoke(
      &dispatcher, "simulation.command",
      boost::json::object{
          {"run_id", "live-application"},
          {"command",
           boost::json::object{
               {"kind", "remove_nodes"},
               {"node_remove",
                boost::json::object{{"node_ids", boost::json::array{"firo-1"}},
                                    {"timeout_sec", 39U}}}}}});
  const SimulationCommand generic_command = WaitForQueuedCommand(queue.get());
  BOOST_REQUIRE(generic_command.node_remove);
  BOOST_TEST(generic_command.node_remove->timeout_sec == 39U);
  publish_inventory(3U, {"firo-3"});
  MarkNodeAddCommitted(generic_command, 2U, {"firo-1", "firo-3"});
  BOOST_TEST(generic_command.operation_control->ReportProgress(
      kSimulationNodeAddProgressTotal));
  application.RecordCommandOutcome(generic_command,
                                   NodeRemoveOutcome({"firo-1"}, 3U, 1U));
  const boost::json::object generic_terminal =
      WaitForTerminal(&dispatcher, generic_submitted);
  BOOST_TEST(generic_terminal.at("state").as_string() == "succeeded");
  const boost::json::object& generic_result =
      generic_terminal.at("terminal_result").as_object();
  BOOST_TEST(generic_result.at("result_family").as_string() ==
             "runtime_command");
  BOOST_TEST(generic_result.at("action").as_string() == "node.remove");
  BOOST_TEST(
      generic_result.at("removed_node_ids").as_array().front().as_string() ==
      "firo-1");
  BOOST_TEST(generic_result.at("final_node_count").as_uint64() == 1U);
  BOOST_TEST(application.current_node_count() == 1U);

  const boost::json::object cancelled_submitted =
      Invoke(&dispatcher, "node.remove",
             boost::json::object{{"run_id", "live-application"},
                                 {"node_ids", boost::json::array{"firo-3"}}});
  const SimulationCommand cancelled_command = WaitForQueuedCommand(queue.get());
  BOOST_REQUIRE(cancelled_command.operation_control);
  BOOST_TEST(cancelled_command.operation_control->RequestCancellation(
      SimulationCommandCancellationCause::kClientCancel));
  application.RecordCommandOutcome(
      cancelled_command,
      CommandOutcome(SimulationCommandOutcomeState::kCancelled, std::nullopt,
                     std::nullopt,
                     SimulationCommandCancellationCause::kClientCancel));
  const boost::json::object cancelled_terminal =
      WaitForTerminal(&dispatcher, cancelled_submitted);
  BOOST_TEST(cancelled_terminal.at("state").as_string() == "cancelled");
  BOOST_TEST(application.current_node_count() == 1U);
  BOOST_TEST(run_stop_requests.load(std::memory_order_acquire) == 0U);

  const boost::json::object empty_submitted =
      Invoke(&dispatcher, "node.remove",
             boost::json::object{{"run_id", "live-application"},
                                 {"node_ids", boost::json::array{"firo-3"}}});
  const SimulationCommand empty_command = WaitForQueuedCommand(queue.get());
  publish_inventory(4U, {});
  MarkNodeAddCommitted(empty_command, 3U, {"firo-3"});
  BOOST_TEST(empty_command.operation_control->ReportProgress(
      kSimulationNodeAddProgressTotal));
  application.RecordCommandOutcome(empty_command,
                                   NodeRemoveOutcome({"firo-3"}, 4U, 0U));
  const boost::json::object empty_terminal =
      WaitForTerminal(&dispatcher, empty_submitted);
  BOOST_TEST(empty_terminal.at("state").as_string() == "succeeded");
  const boost::json::object& empty_result =
      empty_terminal.at("terminal_result").as_object();
  BOOST_TEST(empty_result.at("removed_node_ids").as_array().size() == 1U);
  BOOST_TEST(empty_result.at("final_node_count").as_uint64() == 0U);
  BOOST_TEST(application.current_node_count() == 0U);

  dispatcher.Shutdown();
  application.Shutdown();
}

BOOST_AUTO_TEST_CASE(
    mcp_node_remove_terminalizes_invalid_generation_count_and_identity) {
  LiveApplicationDirectory temporary;
  boost::json::object scenario = LiveScenario();
  scenario["nodes"] = 3U;
  const auto options =
      std::make_shared<Options>(ParseAndValidateScenario(scenario));
  auto queue = std::make_shared<SimulationCommandQueue>();
  std::mutex inventory_mutex;
  McpLiveNodeInventorySnapshot inventory = InitialInventory(*options);
  std::atomic_uint32_t run_stop_requests = 0U;
  const auto publish_inventory = [&](std::uint64_t generation,
                                     std::vector<std::string> node_ids) {
    std::lock_guard<std::mutex> lock(inventory_mutex);
    inventory.generation = generation;
    inventory.node_ids = std::move(node_ids);
  };
  McpLiveApplication application(McpLiveApplication::Config{
      .run_id = "live-application",
      .run_root = temporary.path(),
      .retained_run = std::nullopt,
      .options = options,
      .command_queue = queue,
      .node_inventory_snapshot =
          [&] {
            std::lock_guard<std::mutex> lock(inventory_mutex);
            return inventory;
          },
      .publication_mutex = {},
      .request_run_stop = [&] { ++run_stop_requests; },
      .run_started = {},
      .run_stopping = {},
      .run_stopped = {}});
  application.MarkRunStarted();
  McpDispatcher dispatcher({}, application.OperationFactory(),
                           application.ResourceReader());
  dispatcher.SessionHandler()("live-session", true, {});

  const auto require_unconfirmed =
      [&](std::vector<std::string> published_ids,
          std::uint64_t published_generation, std::uint64_t outcome_generation,
          std::uint32_t final_count,
          std::vector<std::string> outcome_removed_ids) {
        publish_inventory(1U, {"firo-1", "firo-2", "firo-3"});
        const boost::json::object submitted = Invoke(
            &dispatcher, "node.remove",
            boost::json::object{{"run_id", "live-application"},
                                {"node_ids", boost::json::array{"firo-2"}}});
        const SimulationCommand command = WaitForQueuedCommand(queue.get());
        MarkNodeAddCommitted(command, 1U, {"firo-1", "firo-2", "firo-3"});
        publish_inventory(published_generation, std::move(published_ids));
        application.RecordCommandOutcome(
            command, NodeRemoveOutcome(std::move(outcome_removed_ids),
                                       outcome_generation, final_count));
        const boost::json::object terminal =
            WaitForTerminal(&dispatcher, submitted);
        BOOST_TEST(terminal.at("state").as_string() == "failed");
        BOOST_TEST(
            terminal.at("terminal_error").as_object().at("code").as_string() ==
            "node_outcome_unconfirmed");
      };
  require_unconfirmed({"firo-1", "firo-3"}, 1U, 1U, 2U, {"firo-2"});
  require_unconfirmed({"firo-1", "firo-3"}, 2U, 2U, 1U, {"firo-2"});
  require_unconfirmed({"replacement", "firo-3"}, 2U, 2U, 2U, {"firo-2"});
  require_unconfirmed({"firo-1", "firo-3"}, 2U, 2U, 2U, {"firo-1"});
  BOOST_TEST(run_stop_requests.load(std::memory_order_acquire) >= 4U);

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
      .node_inventory_snapshot =
          [options] { return InitialInventory(*options); },
      .publication_mutex = {},
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
        return SimulationCommandOutcome{};
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
  McpLiveApplication application(McpLiveApplication::Config{
      .run_id = "live-application",
      .run_root = temporary.path(),
      .retained_run = std::nullopt,
      .options = options,
      .command_queue = queue,
      .node_inventory_snapshot =
          [options] { return InitialInventory(*options); },
      .publication_mutex = {},
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
  McpLiveApplication application(McpLiveApplication::Config{
      .run_id = "live-application",
      .run_root = temporary.path(),
      .retained_run = std::nullopt,
      .options = options,
      .command_queue = queue,
      .node_inventory_snapshot =
          [options] { return InitialInventory(*options); },
      .publication_mutex = {},
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

  std::stop_source shutdown_cancellation;
  std::future<void> cancelled_shutdown = std::async(std::launch::async, [&] {
    application.Shutdown(shutdown_cancellation.get_token());
  });
  BOOST_CHECK(cancelled_shutdown.wait_for(20ms) == std::future_status::timeout);
  shutdown_cancellation.request_stop();
  const std::future_status cancelled_status =
      cancelled_shutdown.wait_for(500ms);
  if (cancelled_status != std::future_status::ready) {
    release_request_promise.set_value();
  }
  BOOST_REQUIRE(cancelled_status == std::future_status::ready);
  BOOST_CHECK_THROW(cancelled_shutdown.get(), McpOperationCancelled);

  std::future<void> bounded_shutdown = std::async(std::launch::async, [&] {
    application.Shutdown(std::chrono::steady_clock::now() + 20ms, {});
  });
  const std::future_status bounded_status = bounded_shutdown.wait_for(500ms);
  release_request_promise.set_value();
  BOOST_REQUIRE(bounded_status == std::future_status::ready);
  BOOST_CHECK_THROW(bounded_shutdown.get(), std::runtime_error);
  application.Shutdown(std::chrono::steady_clock::now() + 500ms, {});

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
      .publication_mutex = {},
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
  BOOST_REQUIRE_EQUAL(notification_schemas.size(), notification_methods.size());
  BOOST_TEST(
      !notification_schemas.contains(kMcpSubscriptionUpdatedNotification));
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
  McpLiveApplication application(McpLiveApplication::Config{
      .run_id = "live-application",
      .run_root = temporary.path(),
      .retained_run = std::nullopt,
      .options = options,
      .command_queue = queue,
      .node_inventory_snapshot =
          [options] { return InitialInventory(*options); },
      .publication_mutex = {},
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
  BOOST_REQUIRE_MESSAGE(
      evidence_terminal.at("state").as_string() == "succeeded",
      boost::json::serialize(evidence_terminal));
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

BOOST_AUTO_TEST_CASE(
    mcp_live_wallet_workload_operations_route_one_stable_lifecycle) {
  LiveApplicationDirectory temporary;
  const auto options =
      std::make_shared<Options>(ParseAndValidateScenario(LiveScenario()));
  WriteText(temporary.path() / "resolved-scenario.json",
            boost::json::serialize(ResolveScenario(LiveScenario())) + "\n");
  auto queue = std::make_shared<SimulationCommandQueue>();
  McpLiveApplication application(McpLiveApplication::Config{
      .run_id = "live-application",
      .run_root = temporary.path(),
      .retained_run = std::nullopt,
      .options = options,
      .command_queue = queue,
      .node_inventory_snapshot =
          [options] { return InitialInventory(*options); },
      .publication_mutex = {},
      .request_run_stop = [] {},
      .run_started = {},
      .run_stopping = {},
      .run_stopped = {},
      .publish_evidence = {},
      .close_run_subscriptions = {}});
  const std::vector<McpOperationKind> advertised_before_service =
      application.SupportedOperations();
  for (const McpOperationKind operation :
       {McpOperationKind::kStartWorkload, McpOperationKind::kInspectWorkload,
        McpOperationKind::kReconfigureWorkload,
        McpOperationKind::kPauseWorkload, McpOperationKind::kResumeWorkload,
        McpOperationKind::kStopWorkload}) {
    const bool advertised =
        std::find(advertised_before_service.begin(),
                  advertised_before_service.end(),
                  operation) != advertised_before_service.end();
    BOOST_TEST(advertised);
  }

  std::mutex state_mutex;
  std::string state = "running";
  std::string terminal_outcome = "none";
  std::uint64_t revision = 1U;
  std::uint64_t submitted = 8U;
  const boost::json::object configuration{{"type", "wallet_transactions"},
                                          {"strategy", "random_bruteforce"},
                                          {"retained_balance_percentage", 80.0},
                                          {"transaction_rate", 2.0},
                                          {"amount", "1.00000000"},
                                          {"fee", "0.00001000"}};
  const auto snapshot = [&] {
    boost::json::object accounting{{"planned", submitted},
                                   {"accepted", submitted},
                                   {"attempted", submitted},
                                   {"submitted", submitted},
                                   {"propagated", submitted},
                                   {"confirmed", submitted - 1U},
                                   {"rejected", 0U},
                                   {"timed_out", 0U},
                                   {"backpressured", 0U},
                                   {"dropped", 0U},
                                   {"failed", 0U},
                                   {"retried", 0U},
                                   {"cancelled", 0U},
                                   {"outstanding", 1U},
                                   {"in_flight", 0U},
                                   {"reserved_atomic_units", 800U},
                                   {"released_atomic_units", 800U}};
    return boost::json::object{{"workload_id", "wallet-workload-1"},
                               {"state", state},
                               {"terminal_outcome", terminal_outcome},
                               {"configuration_revision", revision},
                               {"configuration", configuration},
                               {"accounting", std::move(accounting)}};
  };
  auto service = std::make_shared<McpLiveWorkloadService>();
  service->operation = [&](McpOperationKind kind, const boost::json::object&,
                           std::stop_token stop_token) {
    if (stop_token.stop_requested()) {
      throw McpOperationCancelled();
    }
    std::lock_guard<std::mutex> lock(state_mutex);
    switch (kind) {
      case McpOperationKind::kStartWorkload:
      case McpOperationKind::kInspectWorkload:
        break;
      case McpOperationKind::kReconfigureWorkload:
        ++revision;
        submitted += 1U;
        break;
      case McpOperationKind::kPauseWorkload:
        state = "paused";
        break;
      case McpOperationKind::kResumeWorkload:
        state = "running";
        break;
      case McpOperationKind::kStopWorkload:
        state = "stopped";
        terminal_outcome = "stopped";
        break;
      default:
        throw std::logic_error("unexpected workload operation");
    }
    return snapshot();
  };
  service->read = [&](bool history, std::stop_token) {
    std::lock_guard<std::mutex> lock(state_mutex);
    const bool terminal = state == "stopped";
    if (history != terminal) {
      return boost::json::value(boost::json::array{});
    }
    return boost::json::value(boost::json::array{snapshot()});
  };
  application.SetWorkloadService(service);
  application.MarkRunStarted();
  McpDispatcher dispatcher({}, application.OperationFactory(),
                           application.ResourceReader());
  dispatcher.SessionHandler()("live-session", true, {});

  const auto invoke_workload = [&](std::string_view tool,
                                   boost::json::object arguments) {
    arguments["run_id"] = "live-application";
    const boost::json::object submitted_operation =
        Invoke(&dispatcher, tool, std::move(arguments));
    const boost::json::object terminal =
        WaitForTerminal(&dispatcher, submitted_operation);
    BOOST_TEST(terminal.at("state").as_string() == "succeeded");
    return terminal.at("terminal_result").as_object();
  };
  const boost::json::object started =
      invoke_workload("workload.start",
                      boost::json::object{{"workload_id", "wallet-workload-1"},
                                          {"workload", configuration}});
  BOOST_TEST(started.at("workload_id").as_string() == "wallet-workload-1");
  BOOST_TEST(
      started.at("accounting").as_object().at("outstanding").as_uint64() == 1U);

  const boost::json::object inspected = invoke_workload(
      "workload.inspect",
      boost::json::object{{"workload_id", "wallet-workload-1"}});
  BOOST_TEST(inspected.at("state").as_string() == "running");
  const boost::json::object paused =
      invoke_workload("workload.pause",
                      boost::json::object{{"workload_id", "wallet-workload-1"},
                                          {"timeout_sec", 1U}});
  BOOST_TEST(paused.at("state").as_string() == "paused");
  const boost::json::object resumed =
      invoke_workload("workload.resume",
                      boost::json::object{{"workload_id", "wallet-workload-1"},
                                          {"timeout_sec", 1U}});
  BOOST_TEST(resumed.at("state").as_string() == "running");
  const boost::json::object reconfigured =
      invoke_workload("workload.reconfigure",
                      boost::json::object{{"workload_id", "wallet-workload-1"},
                                          {"workload", configuration}});
  BOOST_TEST(reconfigured.at("configuration_revision").as_uint64() == 2U);
  BOOST_TEST(reconfigured.at("workload_id").as_string() == "wallet-workload-1");
  const boost::json::object stopped = invoke_workload(
      "workload.stop", boost::json::object{{"workload_id", "wallet-workload-1"},
                                           {"policy", "cancel"},
                                           {"timeout_sec", 1U}});
  BOOST_TEST(stopped.at("state").as_string() == "stopped");
  BOOST_TEST(stopped.at("terminal_outcome").as_string() == "stopped");

  const boost::json::value active = application.ResourceReader()(
      McpInformationFamily::kWorkloads, "live-session", {});
  BOOST_TEST(active.as_object().at("data").as_array().empty());
  const boost::json::value history = application.ResourceReader()(
      McpInformationFamily::kWorkloadHistory, "live-session", {});
  BOOST_REQUIRE_EQUAL(history.as_object().at("data").as_array().size(), 1U);
  BOOST_TEST(history.as_object()
                 .at("data")
                 .as_array()
                 .front()
                 .as_object()
                 .at("workload_id")
                 .as_string() == "wallet-workload-1");
  dispatcher.Shutdown();
  application.Shutdown();
}

BOOST_AUTO_TEST_CASE(
    mcp_live_wallet_lifecycle_routes_authoritative_role_mutation_service) {
  LiveApplicationDirectory temporary;
  const auto options =
      std::make_shared<Options>(ParseAndValidateScenario(LiveScenario()));
  auto queue = std::make_shared<SimulationCommandQueue>();
  McpLiveApplication application(McpLiveApplication::Config{
      .run_id = "live-application",
      .run_root = temporary.path(),
      .retained_run = std::nullopt,
      .options = options,
      .command_queue = queue,
      .node_inventory_snapshot =
          [options] { return InitialInventory(*options); },
      .publication_mutex = {},
      .request_run_stop = [] {},
      .run_started = {},
      .run_stopping = {},
      .run_stopped = {},
      .publish_evidence = {},
      .close_run_subscriptions = {}});
  const std::vector<McpOperationKind> supported =
      application.SupportedOperations();
  BOOST_CHECK(std::find(supported.begin(), supported.end(),
                        McpOperationKind::kAddWallet) != supported.end());
  BOOST_CHECK(std::find(supported.begin(), supported.end(),
                        McpOperationKind::kRemoveWallet) != supported.end());

  auto service = std::make_shared<McpLiveRoleService>();
  service->operation = [](McpOperationKind kind,
                          const boost::json::object& arguments,
                          std::stop_token stop_token) {
    BOOST_CHECK(kind == McpOperationKind::kAddWallet ||
                kind == McpOperationKind::kRemoveWallet);
    if (stop_token.stop_requested()) {
      throw McpOperationCancelled();
    }
    if (kind == McpOperationKind::kRemoveWallet) {
      boost::json::array node_ids;
      if (const boost::json::value* plural =
              arguments.if_contains("node_ids")) {
        node_ids = plural->as_array();
        BOOST_TEST(arguments.at("timeout_sec").as_uint64() == 30U);
      } else {
        node_ids.emplace_back(arguments.at("node_id"));
      }
      boost::json::array wallets;
      for (std::size_t index = 0U; index < node_ids.size(); ++index) {
        wallets.emplace_back(boost::json::object{
            {"wallet_index", index + 1U},
            {"node", index + 1U},
            {"node_id", node_ids[index]},
            {"mode", "public"},
            {"address", "wallet-address-" + std::to_string(index + 1U)},
            {"funding_address",
             "wallet-address-" + std::to_string(index + 1U)}});
      }
      return boost::json::object{
          {"added_node_ids", boost::json::array{}},
          {"removed_node_ids", boost::json::array{}},
          {"affected_node_ids", node_ids},
          {"action", "wallet.remove"},
          {"state", "removed"},
          {"unchanged", false},
          {"wallets", std::move(wallets)},
          {"inventory_generation", 1U},
          {"final_node_count", node_ids.size()},
          {"wallet_generation", 3U},
          {"final_wallet_count", 0U},
          {"final_wallet_node_count", 0U},
      };
    }
    BOOST_TEST(arguments.at("count").as_uint64() == 1U);
    BOOST_TEST(arguments.at("mode").as_string() == "public");
    const boost::json::value* node_id = arguments.if_contains("node_id");
    if (node_id != nullptr && node_id->as_string() == "firo-cancel") {
      throw SimulationCancelled();
    }
    const bool create_node = arguments.contains("create_node");
    return boost::json::object{
        {"added_node_ids",
         create_node ? boost::json::array{"firo-2"} : boost::json::array{}},
        {"removed_node_ids", boost::json::array{}},
        {"affected_node_ids",
         boost::json::array{create_node ? "firo-2" : "firo-1"}},
        {"action", "wallet.add"},
        {"state", "ready"},
        {"unchanged", false},
        {"wallets", boost::json::array{boost::json::object{
                        {"wallet_index", 1U},
                        {"node", create_node ? 2U : 1U},
                        {"node_id", create_node ? "firo-2" : "firo-1"},
                        {"mode", "public"},
                        {"address", "wallet-address"},
                        {"funding_address", "wallet-address"}}}},
        {"inventory_generation", create_node ? 2U : 1U},
        {"final_node_count", create_node ? 2U : 1U},
        {"wallet_generation", 2U},
        {"final_wallet_count", 1U},
        {"final_wallet_node_count", 1U},
    };
  };
  application.SetRoleService(service);
  application.MarkRunStarted();
  McpDispatcher dispatcher({}, application.OperationFactory(),
                           application.ResourceReader());
  dispatcher.SessionHandler()("live-session", true, {});

  const boost::json::object submitted =
      Invoke(&dispatcher, "wallet.add",
             boost::json::object{{"run_id", "live-application"},
                                 {"node_id", "firo-1"},
                                 {"count", 1U},
                                 {"mode", "public"}});
  const boost::json::object terminal = WaitForTerminal(&dispatcher, submitted);
  BOOST_TEST(terminal.at("state").as_string() == "succeeded");
  const boost::json::object& result =
      terminal.at("terminal_result").as_object();
  BOOST_TEST(result.at("result_family").as_string() == "mutation");
  BOOST_TEST(result.at("action").as_string() == "wallet.add");
  BOOST_TEST(result.at("state").as_string() == "ready");
  BOOST_TEST(result.at("wallet_generation").as_uint64() == 2U);
  BOOST_TEST(result.at("wallets").as_array().size() == 1U);

  const boost::json::object removed = Invoke(
      &dispatcher, "wallet.remove",
      boost::json::object{{"run_id", "live-application"},
                          {"node_ids", boost::json::array{"firo-1", "firo-2"}},
                          {"timeout_sec", 30U}});
  const boost::json::object removed_terminal =
      WaitForTerminal(&dispatcher, removed);
  BOOST_TEST(removed_terminal.at("state").as_string() == "succeeded");
  const boost::json::object& removed_result =
      removed_terminal.at("terminal_result").as_object();
  BOOST_TEST(removed_result.at("result_family").as_string() == "mutation");
  BOOST_TEST(removed_result.at("action").as_string() == "wallet.remove");
  BOOST_TEST(removed_result.at("state").as_string() == "removed");
  const boost::json::array removed_wallet_nodes{"firo-1", "firo-2"};
  BOOST_TEST(removed_result.at("affected_node_ids").as_array() ==
             removed_wallet_nodes);
  BOOST_TEST(removed_result.at("wallets").as_array().size() == 2U);
  BOOST_TEST(removed_result.at("final_wallet_count").as_uint64() == 0U);

  const boost::json::object legacy_removed = WaitForTerminal(
      &dispatcher, Invoke(&dispatcher, "wallet.remove",
                          boost::json::object{{"run_id", "live-application"},
                                              {"node_id", "firo-1"}}));
  BOOST_TEST(legacy_removed.at("state").as_string() == "succeeded");

  const boost::json::object created = Invoke(
      &dispatcher, "wallet.add",
      boost::json::object{{"run_id", "live-application"},
                          {"count", 1U},
                          {"mode", "public"},
                          {"create_node", boost::json::object{{"chain", "firo"},
                                                              {"count", 1U}}}});
  const boost::json::object created_terminal =
      WaitForTerminal(&dispatcher, created);
  BOOST_TEST(created_terminal.at("state").as_string() == "succeeded");
  const boost::json::object& created_result =
      created_terminal.at("terminal_result").as_object();
  BOOST_TEST(created_result.at("added_node_ids").as_array() ==
             boost::json::array{"firo-2"});
  BOOST_TEST(created_result.at("inventory_generation").as_uint64() == 2U);
  BOOST_TEST(created_result.at("final_node_count").as_uint64() == 2U);

  const boost::json::object cancelled =
      Invoke(&dispatcher, "wallet.add",
             boost::json::object{{"run_id", "live-application"},
                                 {"node_id", "firo-cancel"},
                                 {"count", 1U},
                                 {"mode", "public"}});
  BOOST_TEST(WaitForTerminal(&dispatcher, cancelled).at("state").as_string() ==
             "cancelled");
}

BOOST_AUTO_TEST_CASE(
    mcp_live_role_assign_delegates_one_typed_batch_and_normalizes_results) {
  LiveApplicationDirectory temporary;
  const auto options =
      std::make_shared<Options>(ParseAndValidateScenario(LiveScenario()));
  auto queue = std::make_shared<SimulationCommandQueue>();
  bool stop_requested = false;
  McpLiveApplication application(McpLiveApplication::Config{
      .run_id = "live-application",
      .run_root = temporary.path(),
      .retained_run = std::nullopt,
      .options = options,
      .command_queue = queue,
      .node_inventory_snapshot =
          [options] { return InitialInventory(*options); },
      .publication_mutex = {},
      .request_run_stop = [&stop_requested] { stop_requested = true; },
      .run_started = {},
      .run_stopping = {},
      .run_stopped = {},
      .publish_evidence = {},
      .close_run_subscriptions = {}});
  const std::vector<McpOperationKind> supported =
      application.SupportedOperations();
  BOOST_CHECK(std::find(supported.begin(), supported.end(),
                        McpOperationKind::kAssignRole) != supported.end());

  std::size_t service_calls = 0U;
  auto service = std::make_shared<McpLiveRoleService>();
  service->operation = [&service_calls](McpOperationKind kind,
                                        const boost::json::object& arguments,
                                        std::stop_token stop_token) {
    ++service_calls;
    BOOST_TEST(arguments.at("run_id").as_string() == "live-application");
    const boost::json::array& node_ids = arguments.at("node_ids").as_array();
    BOOST_TEST(arguments.at("count").as_uint64() == node_ids.size());
    if (stop_token.stop_requested()) {
      throw McpOperationCancelled();
    }
    if (node_ids.front().as_string() == "firo-cancel") {
      throw SimulationCancelled();
    }
    if (node_ids.front().as_string() == "firo-secret") {
      BOOST_CHECK(kind == McpOperationKind::kAddMasternode);
      return boost::json::object{
          {"node_ids", node_ids},
          {"assigned_roles", boost::json::array{"masternode"}},
          {"removed_roles", boost::json::array{}},
          {"action", "masternode.add"},
          {"state", "ready"},
          {"created_node_ids", boost::json::array{}},
          {"role_generation", 11U},
          {"final_masternode_count", 1U},
          {"masternodes", boost::json::array{boost::json::object{
                              {"node", 1U},
                              {"node_id", "firo-secret"},
                              {"funding_wallet_node_id", "firo-wallet"},
                              {"pro_tx_hash", "protx-secret"},
                              {"service", "10.77.0.3:18168"},
                              {"collateral_address", "collateral"},
                              {"owner_address", "owner"},
                              {"operator_public_key", "operator-public"},
                              {"operator_secret_key", "delegated-secret"},
                              {"voting_address", "voting"},
                              {"payout_address", "payout"},
                              {"collateral_hash", "collateral-hash"},
                              {"collateral_index", 0U},
                              {"state", "READY"},
                              {"status", "ready"}}}},
          {"inventory_generation", 4U},
          {"final_node_count", 2U},
      };
    }
    if (kind == McpOperationKind::kAddWallet) {
      BOOST_TEST(arguments.at("mode").as_string() == "public");
      boost::json::array wallets;
      for (std::size_t index = 0U; index < node_ids.size(); ++index) {
        wallets.emplace_back(boost::json::object{
            {"wallet_index", index + 1U},
            {"node", index + 1U},
            {"node_id", node_ids[index]},
            {"mode", "public"},
            {"address", "wallet-address-" + std::to_string(index + 1U)},
            {"funding_address",
             "funding-address-" + std::to_string(index + 1U)}});
      }
      return boost::json::object{
          {"added_node_ids", boost::json::array{}},
          {"removed_node_ids", boost::json::array{}},
          {"affected_node_ids", node_ids},
          {"action", "wallet.add"},
          {"state", "ready"},
          {"unchanged", false},
          {"wallets", std::move(wallets)},
          {"inventory_generation", 4U},
          {"final_node_count", 2U},
          {"wallet_generation", 8U},
          {"final_wallet_count", 3U},
          {"final_wallet_node_count", 2U},
      };
    }
    if (kind == McpOperationKind::kAddMiner) {
      return boost::json::object{
          {"node_ids", node_ids},
          {"assigned_roles", boost::json::array{"miner"}},
          {"removed_roles", boost::json::array{}},
          {"action", "miner.add"},
          {"state", "ready"},
          {"created_node_ids", boost::json::array{}},
          {"role_generation", 9U},
          {"final_miner_count", 2U},
          {"inventory_generation", 4U},
          {"final_node_count", 2U},
      };
    }
    BOOST_REQUIRE(kind == McpOperationKind::kAddMasternode);
    BOOST_TEST(arguments.at("funding_wallet_id").as_string() == "firo-wallet");
    boost::json::array masternodes;
    for (std::size_t index = 0U; index < node_ids.size(); ++index) {
      masternodes.emplace_back(boost::json::object{
          {"node", index + 1U},
          {"node_id", node_ids[index]},
          {"funding_wallet_node_id", "firo-wallet"},
          {"pro_tx_hash", "protx-" + std::to_string(index + 1U)},
          {"service", "10.77.0.2:18168"},
          {"collateral_address", "collateral"},
          {"owner_address", "owner"},
          {"operator_public_key", "operator-public"},
          {"voting_address", "voting"},
          {"payout_address", "payout"},
          {"collateral_hash", "collateral-hash"},
          {"collateral_index", index},
          {"state", "READY"},
          {"status", "ready"}});
    }
    return boost::json::object{
        {"node_ids", node_ids},
        {"assigned_roles", boost::json::array{"masternode"}},
        {"removed_roles", boost::json::array{}},
        {"action", "masternode.add"},
        {"state", "ready"},
        {"created_node_ids", boost::json::array{}},
        {"role_generation", 10U},
        {"final_masternode_count", node_ids.size()},
        {"masternodes", std::move(masternodes)},
        {"inventory_generation", 4U},
        {"final_node_count", 2U},
    };
  };
  application.SetRoleService(service);
  application.MarkRunStarted();
  McpDispatcher dispatcher({}, application.OperationFactory(),
                           application.ResourceReader());
  dispatcher.SessionHandler()("live-session", true, {});

  const auto assign = [&](boost::json::array node_ids, std::string_view role,
                          boost::json::object role_fields) {
    role_fields["run_id"] = "live-application";
    role_fields["node_ids"] = std::move(node_ids);
    role_fields["roles"] = boost::json::array{role};
    return WaitForTerminal(&dispatcher, Invoke(&dispatcher, "role.assign",
                                               std::move(role_fields)));
  };
  const boost::json::array two_nodes{"_firo-1", "-firo-2"};

  const boost::json::object wallet =
      assign(two_nodes, "wallet", boost::json::object{{"mode", "public"}});
  BOOST_TEST(wallet.at("state").as_string() == "succeeded");
  const boost::json::object& wallet_result =
      wallet.at("terminal_result").as_object();
  BOOST_TEST(wallet_result.at("result_family").as_string() == "role_mutation");
  BOOST_TEST(wallet_result.at("action").as_string() == "role.assign");
  BOOST_TEST(wallet_result.at("assigned_roles").as_array() ==
             boost::json::array{"wallet"});
  BOOST_TEST(wallet_result.at("node_ids").as_array() == two_nodes);
  BOOST_TEST(wallet_result.at("role_generation").as_uint64() == 8U);
  BOOST_TEST(wallet_result.at("final_wallet_count").as_uint64() == 3U);
  BOOST_TEST(wallet_result.at("final_wallet_node_count").as_uint64() == 2U);
  BOOST_TEST(wallet_result.at("wallets").as_array().size() == 2U);

  const boost::json::object miner =
      assign(two_nodes, "miner", boost::json::object{});
  const boost::json::object& miner_result =
      miner.at("terminal_result").as_object();
  BOOST_TEST(miner.at("state").as_string() == "succeeded");
  BOOST_TEST(miner_result.at("action").as_string() == "role.assign");
  BOOST_TEST(miner_result.at("final_miner_count").as_uint64() == 2U);
  BOOST_TEST(miner_result.at("created_node_ids").as_array().empty());

  const boost::json::object masternode =
      assign(two_nodes, "masternode",
             boost::json::object{{"funding_wallet_id", "firo-wallet"}});
  const boost::json::object& masternode_result =
      masternode.at("terminal_result").as_object();
  BOOST_TEST(masternode.at("state").as_string() == "succeeded");
  BOOST_TEST(masternode_result.at("action").as_string() == "role.assign");
  BOOST_TEST(masternode_result.at("final_masternode_count").as_uint64() == 2U);
  BOOST_TEST(masternode_result.at("masternodes").as_array().size() == 2U);
  BOOST_TEST(
      boost::json::serialize(masternode_result).find("operator_secret") ==
      std::string::npos);

  const boost::json::object cancelled =
      assign(boost::json::array{"firo-cancel"}, "miner", boost::json::object{});
  BOOST_TEST(cancelled.at("state").as_string() == "cancelled");
  BOOST_TEST(service_calls == 4U);
  BOOST_TEST(stop_requested == false);

  BOOST_CHECK_THROW(
      Invoke(&dispatcher, "role.assign",
             boost::json::object{{"run_id", "live-application"},
                                 {"node_ids", boost::json::array{"firo-1"}},
                                 {"roles", boost::json::array{"base"}}}),
      std::invalid_argument);
  BOOST_CHECK_THROW(Invoke(&dispatcher, "role.assign",
                           boost::json::object{
                               {"run_id", "live-application"},
                               {"node_ids", boost::json::array{"firo-1"}},
                               {"roles", boost::json::array{"wallet", "miner"}},
                               {"mode", "public"}}),
                    std::invalid_argument);
  BOOST_CHECK_THROW(
      Invoke(&dispatcher, "role.assign",
             boost::json::object{{"run_id", "live-application"},
                                 {"node_ids", boost::json::array{"firo-1"}},
                                 {"roles", boost::json::array{"miner"}},
                                 {"mode", "public"}}),
      std::invalid_argument);
  const boost::json::object secret_rejected =
      assign(boost::json::array{"firo-secret"}, "masternode",
             boost::json::object{{"funding_wallet_id", "firo-wallet"}});
  BOOST_TEST(secret_rejected.at("state").as_string() == "failed");
  BOOST_TEST(
      secret_rejected.at("terminal_error").as_object().at("code").as_string() ==
      "role_assign_outcome_unconfirmed");
  BOOST_TEST(boost::json::serialize(secret_rejected).find("delegated-secret") ==
             std::string::npos);
  BOOST_TEST(service_calls == 5U);
  BOOST_TEST(stop_requested);
  dispatcher.Shutdown();
  application.Shutdown();
}

BOOST_AUTO_TEST_CASE(mcp_live_role_assign_rejects_delegated_schema_overflow) {
  using RoleOperation = decltype(McpLiveRoleService{}.operation);
  const auto reject_delegated =
      [](boost::json::array node_ids, std::string_view role,
         boost::json::object role_fields, RoleOperation operation) {
        LiveApplicationDirectory temporary;
        const auto options =
            std::make_shared<Options>(ParseAndValidateScenario(LiveScenario()));
        bool stop_requested = false;
        McpLiveApplication application(McpLiveApplication::Config{
            .run_id = "live-application",
            .run_root = temporary.path(),
            .retained_run = std::nullopt,
            .options = options,
            .command_queue = std::make_shared<SimulationCommandQueue>(),
            .node_inventory_snapshot =
                [options] { return InitialInventory(*options); },
            .publication_mutex = {},
            .request_run_stop = [&stop_requested] { stop_requested = true; },
            .run_started = {},
            .run_stopping = {},
            .run_stopped = {},
            .publish_evidence = {},
            .close_run_subscriptions = {}});
        auto service = std::make_shared<McpLiveRoleService>();
        service->operation = std::move(operation);
        application.SetRoleService(service);
        application.MarkRunStarted();
        McpDispatcher dispatcher({}, application.OperationFactory(),
                                 application.ResourceReader());
        dispatcher.SessionHandler()("live-session", true, {});

        role_fields["run_id"] = "live-application";
        role_fields["node_ids"] = std::move(node_ids);
        role_fields["roles"] = boost::json::array{role};
        const boost::json::object terminal = WaitForTerminal(
            &dispatcher,
            Invoke(&dispatcher, "role.assign", std::move(role_fields)));
        BOOST_TEST(terminal.at("state").as_string() == "failed");
        BOOST_TEST(
            terminal.at("terminal_error").as_object().at("code").as_string() ==
            "role_assign_outcome_unconfirmed");
        BOOST_TEST(stop_requested);
        dispatcher.Shutdown();
        application.Shutdown();
      };

  reject_delegated(
      boost::json::array{"firo-count-overflow"}, "miner", boost::json::object{},
      [](McpOperationKind kind, const boost::json::object& arguments,
         std::stop_token stop_token) {
        BOOST_CHECK(kind == McpOperationKind::kAddMiner);
        BOOST_TEST(!stop_token.stop_requested());
        return boost::json::object{
            {"node_ids", arguments.at("node_ids")},
            {"assigned_roles", boost::json::array{"miner"}},
            {"removed_roles", boost::json::array{}},
            {"action", "miner.add"},
            {"state", "ready"},
            {"created_node_ids", boost::json::array{}},
            {"role_generation", 2U},
            {"final_miner_count", 1U},
            {"inventory_generation", 1U},
            {"final_node_count",
             static_cast<std::uint64_t>(
                 std::numeric_limits<std::uint32_t>::max()) +
                 1U},
        };
      });
  reject_delegated(
      boost::json::array{"firo-text-overflow"}, "wallet",
      boost::json::object{{"mode", "public"}},
      [](McpOperationKind kind, const boost::json::object& arguments,
         std::stop_token stop_token) {
        BOOST_CHECK(kind == McpOperationKind::kAddWallet);
        BOOST_TEST(!stop_token.stop_requested());
        return boost::json::object{
            {"added_node_ids", boost::json::array{}},
            {"removed_node_ids", boost::json::array{}},
            {"affected_node_ids", arguments.at("node_ids")},
            {"action", "wallet.add"},
            {"state", "ready"},
            {"unchanged", false},
            {"wallets",
             boost::json::array{boost::json::object{
                 {"wallet_index", 1U},
                 {"node", 1U},
                 {"node_id", "firo-text-overflow"},
                 {"mode", "public"},
                 {"address",
                  std::string(kMcpMaximumEvidenceTextBytes + 1U, 'a')},
                 {"funding_address", "funding-address"}}}},
            {"inventory_generation", 1U},
            {"final_node_count", 1U},
            {"wallet_generation", 2U},
            {"final_wallet_count", 1U},
            {"final_wallet_node_count", 1U},
        };
      });
}

BOOST_AUTO_TEST_CASE(
    mcp_live_role_remove_delegates_one_typed_batch_and_normalizes_results) {
  LiveApplicationDirectory temporary;
  const auto options =
      std::make_shared<Options>(ParseAndValidateScenario(LiveScenario()));
  bool stop_requested = false;
  McpLiveApplication application(McpLiveApplication::Config{
      .run_id = "live-application",
      .run_root = temporary.path(),
      .retained_run = std::nullopt,
      .options = options,
      .command_queue = std::make_shared<SimulationCommandQueue>(),
      .node_inventory_snapshot =
          [options] { return InitialInventory(*options); },
      .publication_mutex = {},
      .request_run_stop = [&stop_requested] { stop_requested = true; },
      .run_started = {},
      .run_stopping = {},
      .run_stopped = {},
      .publish_evidence = {},
      .close_run_subscriptions = {}});
  const std::vector<McpOperationKind> supported =
      application.SupportedOperations();
  BOOST_CHECK(std::find(supported.begin(), supported.end(),
                        McpOperationKind::kRemoveRole) != supported.end());

  std::size_t service_calls = 0U;
  auto service = std::make_shared<McpLiveRoleService>();
  service->operation = [&service_calls](McpOperationKind kind,
                                        const boost::json::object& arguments,
                                        std::stop_token stop_token) {
    ++service_calls;
    BOOST_TEST(arguments.at("run_id").as_string() == "live-application");
    BOOST_TEST(arguments.if_contains("roles") == nullptr);
    BOOST_TEST(arguments.if_contains("count") == nullptr);
    BOOST_TEST(arguments.at("timeout_sec").as_uint64() == 45U);
    const boost::json::array& node_ids = arguments.at("node_ids").as_array();
    if (stop_token.stop_requested()) {
      throw McpOperationCancelled();
    }
    if (node_ids.front().as_string() == "firo-cancel") {
      throw SimulationCancelled();
    }

    if (kind == McpOperationKind::kRemoveWallet) {
      boost::json::array wallets;
      for (std::size_t index = 0U; index < node_ids.size(); ++index) {
        wallets.emplace_back(boost::json::object{
            {"wallet_index", index + 1U},
            {"node", index + 1U},
            {"node_id", node_ids[index]},
            {"mode", index == 0U ? "public" : "private"},
            {"address", "wallet-address-" + std::to_string(index + 1U)},
            {"funding_address",
             "funding-address-" + std::to_string(index + 1U)}});
      }
      return boost::json::object{
          {"added_node_ids", boost::json::array{}},
          {"removed_node_ids", boost::json::array{}},
          {"affected_node_ids", node_ids},
          {"action", "wallet.remove"},
          {"state", "removed"},
          {"unchanged", false},
          {"wallets", std::move(wallets)},
          {"inventory_generation", 4U},
          {"final_node_count", 2U},
          {"wallet_generation", 12U},
          {"final_wallet_count", 0U},
          {"final_wallet_node_count", 0U},
      };
    }
    if (kind == McpOperationKind::kRemoveMiner) {
      return boost::json::object{
          {"node_ids", node_ids},
          {"assigned_roles", boost::json::array{}},
          {"removed_roles", boost::json::array{"miner"}},
          {"action", "miner.remove"},
          {"state", "removed"},
          {"created_node_ids", boost::json::array{}},
          {"role_generation", 13U},
          {"final_miner_count", 0U},
          {"inventory_generation", 4U},
          {"final_node_count", 2U},
      };
    }
    BOOST_REQUIRE(kind == McpOperationKind::kRemoveMasternode);
    boost::json::array masternodes;
    for (std::size_t index = 0U; index < node_ids.size(); ++index) {
      boost::json::object identity{
          {"node", index + 1U},
          {"node_id", node_ids[index]},
          {"funding_wallet_node_id", "firo-wallet"},
          {"pro_tx_hash", "protx-" + std::to_string(index + 1U)},
          {"service", "10.77.0.2:18168"},
          {"collateral_address", "collateral"},
          {"owner_address", "owner"},
          {"operator_public_key", "operator-public"},
          {"voting_address", "voting"},
          {"payout_address", "payout"},
          {"collateral_hash", "collateral-hash"},
          {"collateral_index", index},
          {"state", "REVOKED"},
          {"status", "revoked"},
      };
      if (node_ids.front().as_string() == "firo-secret") {
        identity["operator_secret_key"] = "delegated-secret";
      }
      masternodes.emplace_back(std::move(identity));
    }
    return boost::json::object{
        {"node_ids", node_ids},
        {"assigned_roles", boost::json::array{}},
        {"removed_roles", boost::json::array{"masternode"}},
        {"action", "masternode.remove"},
        {"state", "removed"},
        {"created_node_ids", boost::json::array{}},
        {"role_generation", 14U},
        {"final_masternode_count", 0U},
        {"masternodes", std::move(masternodes)},
        {"inventory_generation", 4U},
        {"final_node_count", 2U},
    };
  };
  application.SetRoleService(service);
  application.MarkRunStarted();
  McpDispatcher dispatcher({}, application.OperationFactory(),
                           application.ResourceReader());
  dispatcher.SessionHandler()("live-session", true, {});

  const auto remove = [&](boost::json::array node_ids, std::string_view role) {
    return WaitForTerminal(
        &dispatcher,
        Invoke(&dispatcher, "role.remove",
               boost::json::object{{"run_id", "live-application"},
                                   {"node_ids", std::move(node_ids)},
                                   {"roles", boost::json::array{role}},
                                   {"timeout_sec", 45U}}));
  };
  const boost::json::array two_nodes{"_firo-1", "-firo-2"};

  const boost::json::object wallet = remove(two_nodes, "wallet");
  BOOST_TEST(wallet.at("state").as_string() == "succeeded");
  const boost::json::object& wallet_result =
      wallet.at("terminal_result").as_object();
  BOOST_TEST(wallet_result.at("result_family").as_string() == "role_mutation");
  BOOST_TEST(wallet_result.at("action").as_string() == "role.remove");
  BOOST_TEST(wallet_result.at("state").as_string() == "removed");
  BOOST_TEST(wallet_result.at("node_ids").as_array() == two_nodes);
  BOOST_TEST(wallet_result.at("assigned_roles").as_array().empty());
  BOOST_TEST(wallet_result.at("removed_roles").as_array() ==
             boost::json::array{"wallet"});
  BOOST_TEST(wallet_result.at("wallets").as_array().size() == 2U);
  BOOST_TEST(wallet_result.at("final_wallet_count").as_uint64() == 0U);

  const boost::json::object miner = remove(two_nodes, "miner");
  const boost::json::object& miner_result =
      miner.at("terminal_result").as_object();
  BOOST_TEST(miner.at("state").as_string() == "succeeded");
  BOOST_TEST(miner_result.at("action").as_string() == "role.remove");
  BOOST_TEST(miner_result.at("removed_roles").as_array() ==
             boost::json::array{"miner"});
  BOOST_TEST(miner_result.at("final_miner_count").as_uint64() == 0U);
  BOOST_TEST(miner_result.at("created_node_ids").as_array().empty());

  const boost::json::object masternode = remove(two_nodes, "masternode");
  const boost::json::object& masternode_result =
      masternode.at("terminal_result").as_object();
  BOOST_TEST(masternode.at("state").as_string() == "succeeded");
  BOOST_TEST(masternode_result.at("action").as_string() == "role.remove");
  BOOST_TEST(masternode_result.at("removed_roles").as_array() ==
             boost::json::array{"masternode"});
  BOOST_TEST(masternode_result.at("final_masternode_count").as_uint64() == 0U);
  BOOST_TEST(masternode_result.at("masternodes").as_array().size() == 2U);
  BOOST_TEST(
      boost::json::serialize(masternode_result).find("operator_secret") ==
      std::string::npos);

  const boost::json::object cancelled =
      remove(boost::json::array{"firo-cancel"}, "miner");
  BOOST_TEST(cancelled.at("state").as_string() == "cancelled");
  BOOST_TEST(service_calls == 4U);
  BOOST_TEST(stop_requested == false);

  BOOST_CHECK_THROW(
      Invoke(&dispatcher, "role.remove",
             boost::json::object{{"run_id", "live-application"},
                                 {"node_ids", boost::json::array{"firo-1"}},
                                 {"roles", boost::json::array{"base"}}}),
      std::invalid_argument);
  BOOST_CHECK_THROW(
      Invoke(&dispatcher, "role.remove",
             boost::json::object{
                 {"run_id", "live-application"},
                 {"node_ids", boost::json::array{"firo-1"}},
                 {"roles", boost::json::array{"wallet", "miner"}}}),
      std::invalid_argument);
  BOOST_TEST(service_calls == 4U);

  const boost::json::object secret_rejected =
      remove(boost::json::array{"firo-secret"}, "masternode");
  BOOST_TEST(secret_rejected.at("state").as_string() == "failed");
  BOOST_TEST(
      secret_rejected.at("terminal_error").as_object().at("code").as_string() ==
      "role_remove_outcome_unconfirmed");
  BOOST_TEST(boost::json::serialize(secret_rejected).find("delegated-secret") ==
             std::string::npos);
  BOOST_TEST(service_calls == 5U);
  BOOST_TEST(stop_requested);
  dispatcher.Shutdown();
  application.Shutdown();
}

BOOST_AUTO_TEST_CASE(
    mcp_live_role_remove_rejects_impossible_delegated_evidence) {
  using RoleOperation = decltype(McpLiveRoleService{}.operation);
  const auto reject_delegated = [](std::string_view role,
                                   RoleOperation operation) {
    LiveApplicationDirectory temporary;
    const auto options =
        std::make_shared<Options>(ParseAndValidateScenario(LiveScenario()));
    bool stop_requested = false;
    McpLiveApplication application(McpLiveApplication::Config{
        .run_id = "live-application",
        .run_root = temporary.path(),
        .retained_run = std::nullopt,
        .options = options,
        .command_queue = std::make_shared<SimulationCommandQueue>(),
        .node_inventory_snapshot =
            [options] { return InitialInventory(*options); },
        .publication_mutex = {},
        .request_run_stop = [&stop_requested] { stop_requested = true; },
        .run_started = {},
        .run_stopping = {},
        .run_stopped = {},
        .publish_evidence = {},
        .close_run_subscriptions = {}});
    auto service = std::make_shared<McpLiveRoleService>();
    service->operation = std::move(operation);
    application.SetRoleService(service);
    application.MarkRunStarted();
    McpDispatcher dispatcher({}, application.OperationFactory(),
                             application.ResourceReader());
    dispatcher.SessionHandler()("live-session", true, {});

    const boost::json::object terminal = WaitForTerminal(
        &dispatcher,
        Invoke(&dispatcher, "role.remove",
               boost::json::object{{"run_id", "live-application"},
                                   {"node_ids", boost::json::array{"firo-1"}},
                                   {"roles", boost::json::array{role}}}));
    BOOST_TEST(terminal.at("state").as_string() == "failed");
    BOOST_TEST(
        terminal.at("terminal_error").as_object().at("code").as_string() ==
        "role_remove_outcome_unconfirmed");
    BOOST_TEST(stop_requested);
    dispatcher.Shutdown();
    application.Shutdown();
  };

  reject_delegated(
      "miner", [](McpOperationKind kind, const boost::json::object& arguments,
                  std::stop_token stop_token) {
        BOOST_CHECK(kind == McpOperationKind::kRemoveMiner);
        BOOST_TEST(!stop_token.stop_requested());
        return boost::json::object{
            {"node_ids", arguments.at("node_ids")},
            {"assigned_roles", boost::json::array{}},
            {"removed_roles", boost::json::array{"miner"}},
            {"action", "miner.remove"},
            {"state", "removed"},
            {"created_node_ids", boost::json::array{}},
            {"role_generation", 2U},
            {"final_miner_count", 1U},
            {"inventory_generation", 1U},
            {"final_node_count", 1U},
        };
      });
  reject_delegated("masternode", [](McpOperationKind kind,
                                    const boost::json::object& arguments,
                                    std::stop_token stop_token) {
    BOOST_CHECK(kind == McpOperationKind::kRemoveMasternode);
    BOOST_TEST(!stop_token.stop_requested());
    return boost::json::object{
        {"node_ids", arguments.at("node_ids")},
        {"assigned_roles", boost::json::array{}},
        {"removed_roles", boost::json::array{"masternode"}},
        {"action", "masternode.remove"},
        {"state", "removed"},
        {"created_node_ids", boost::json::array{}},
        {"role_generation", 2U},
        {"final_masternode_count", 0U},
        {"masternodes", boost::json::array{boost::json::object{
                            {"node", 1U},
                            {"node_id", "firo-1"},
                            {"funding_wallet_node_id", "firo-wallet"},
                            {"pro_tx_hash", "protx"},
                            {"service", "10.77.0.2:18168"},
                            {"collateral_address", "collateral"},
                            {"owner_address", "owner"},
                            {"operator_public_key", "operator-public"},
                            {"voting_address", "voting"},
                            {"payout_address", "payout"},
                            {"collateral_hash", "collateral-hash"},
                            {"collateral_index", 0U},
                            {"state", "READY"},
                            {"status", "enabled"}}}},
        {"inventory_generation", 1U},
        {"final_node_count", 1U},
    };
  });
}

BOOST_AUTO_TEST_CASE(
    mcp_live_miner_lifecycle_routes_role_results_and_cancellation) {
  LiveApplicationDirectory temporary;
  const auto options =
      std::make_shared<Options>(ParseAndValidateScenario(LiveScenario()));
  auto queue = std::make_shared<SimulationCommandQueue>();
  McpLiveApplication application(McpLiveApplication::Config{
      .run_id = "live-application",
      .run_root = temporary.path(),
      .retained_run = std::nullopt,
      .options = options,
      .command_queue = queue,
      .node_inventory_snapshot =
          [options] { return InitialInventory(*options); },
      .publication_mutex = {},
      .request_run_stop = [] {},
      .run_started = {},
      .run_stopping = {},
      .run_stopped = {},
      .publish_evidence = {},
      .close_run_subscriptions = {}});
  const std::vector<McpOperationKind> supported =
      application.SupportedOperations();
  BOOST_CHECK(std::find(supported.begin(), supported.end(),
                        McpOperationKind::kAddMiner) != supported.end());
  BOOST_CHECK(std::find(supported.begin(), supported.end(),
                        McpOperationKind::kRemoveMiner) != supported.end());

  auto service = std::make_shared<McpLiveRoleService>();
  service->operation = [](McpOperationKind kind,
                          const boost::json::object& arguments,
                          std::stop_token stop_token) {
    BOOST_CHECK(kind == McpOperationKind::kAddMiner ||
                kind == McpOperationKind::kRemoveMiner);
    if (stop_token.stop_requested()) {
      throw McpOperationCancelled();
    }
    if (kind == McpOperationKind::kRemoveMiner) {
      BOOST_TEST(arguments.at("node_ids").as_array() ==
                 boost::json::array{"firo-1"});
      return boost::json::object{
          {"node_ids", boost::json::array{"firo-1"}},
          {"assigned_roles", boost::json::array{}},
          {"removed_roles", boost::json::array{"miner"}},
          {"action", "miner.remove"},
          {"state", "removed"},
          {"created_node_ids", boost::json::array{}},
          {"role_generation", 3U},
          {"final_miner_count", 0U},
          {"inventory_generation", 1U},
          {"final_node_count", 1U},
      };
    }
    BOOST_TEST(arguments.at("count").as_uint64() == 1U);
    if (const boost::json::value* node_ids = arguments.if_contains("node_ids");
        node_ids != nullptr &&
        node_ids->as_array().front().as_string() == "firo-cancel") {
      throw SimulationCancelled();
    }
    const bool created = arguments.contains("create_nodes");
    return boost::json::object{
        {"node_ids", boost::json::array{created ? "firo-2" : "firo-1"}},
        {"assigned_roles", boost::json::array{"miner"}},
        {"removed_roles", boost::json::array{}},
        {"action", "miner.add"},
        {"state", "ready"},
        {"created_node_ids",
         created ? boost::json::array{"firo-2"} : boost::json::array{}},
        {"role_generation", 2U},
        {"final_miner_count", 1U},
        {"inventory_generation", created ? 2U : 1U},
        {"final_node_count", created ? 2U : 1U},
    };
  };
  application.SetRoleService(service);
  application.MarkRunStarted();
  McpDispatcher dispatcher({}, application.OperationFactory(),
                           application.ResourceReader());
  dispatcher.SessionHandler()("live-session", true, {});

  const boost::json::object submitted =
      Invoke(&dispatcher, "miner.add",
             boost::json::object{{"run_id", "live-application"},
                                 {"node_ids", boost::json::array{"firo-1"}},
                                 {"count", 1U}});
  const boost::json::object terminal = WaitForTerminal(&dispatcher, submitted);
  BOOST_TEST(terminal.at("state").as_string() == "succeeded");
  const boost::json::object& result =
      terminal.at("terminal_result").as_object();
  BOOST_TEST(result.at("result_family").as_string() == "role_mutation");
  BOOST_TEST(result.at("action").as_string() == "miner.add");
  BOOST_TEST(result.at("node_ids").as_array() == boost::json::array{"firo-1"});
  BOOST_TEST(result.at("role_generation").as_uint64() == 2U);

  const boost::json::object removed =
      Invoke(&dispatcher, "miner.remove",
             boost::json::object{{"run_id", "live-application"},
                                 {"node_ids", boost::json::array{"firo-1"}}});
  const boost::json::object removed_terminal =
      WaitForTerminal(&dispatcher, removed);
  BOOST_TEST(removed_terminal.at("state").as_string() == "succeeded");
  const boost::json::object& removed_result =
      removed_terminal.at("terminal_result").as_object();
  BOOST_TEST(removed_result.at("result_family").as_string() == "role_mutation");
  BOOST_TEST(removed_result.at("action").as_string() == "miner.remove");
  BOOST_TEST(removed_result.at("state").as_string() == "removed");
  BOOST_TEST(removed_result.at("removed_roles").as_array() ==
             boost::json::array{"miner"});
  BOOST_TEST(removed_result.at("final_miner_count").as_uint64() == 0U);

  const boost::json::object created =
      Invoke(&dispatcher, "miner.add",
             boost::json::object{
                 {"run_id", "live-application"},
                 {"count", 1U},
                 {"create_nodes",
                  boost::json::object{{"chain", "firo"}, {"count", 1U}}}});
  const boost::json::object created_terminal =
      WaitForTerminal(&dispatcher, created);
  BOOST_TEST(created_terminal.at("state").as_string() == "succeeded");
  BOOST_TEST(created_terminal.at("terminal_result")
                 .as_object()
                 .at("created_node_ids")
                 .as_array() == boost::json::array{"firo-2"});

  const boost::json::object cancelled = Invoke(
      &dispatcher, "miner.add",
      boost::json::object{{"run_id", "live-application"},
                          {"node_ids", boost::json::array{"firo-cancel"}},
                          {"count", 1U}});
  BOOST_TEST(WaitForTerminal(&dispatcher, cancelled).at("state").as_string() ==
             "cancelled");
  dispatcher.Shutdown();
  application.Shutdown();
}

BOOST_AUTO_TEST_CASE(
    mcp_live_masternode_lifecycle_routes_redacted_role_results) {
  LiveApplicationDirectory temporary;
  const auto options =
      std::make_shared<Options>(ParseAndValidateScenario(LiveScenario()));
  auto queue = std::make_shared<SimulationCommandQueue>();
  McpLiveApplication application(McpLiveApplication::Config{
      .run_id = "live-application",
      .run_root = temporary.path(),
      .retained_run = std::nullopt,
      .options = options,
      .command_queue = queue,
      .node_inventory_snapshot =
          [options] { return InitialInventory(*options); },
      .publication_mutex = {},
      .request_run_stop = [] {},
      .run_started = {},
      .run_stopping = {},
      .run_stopped = {},
      .publish_evidence = {},
      .close_run_subscriptions = {}});
  const std::vector<McpOperationKind> supported =
      application.SupportedOperations();
  for (const McpOperationKind operation :
       {McpOperationKind::kAddMasternode, McpOperationKind::kRemoveMasternode,
        McpOperationKind::kRestartMasternode}) {
    BOOST_CHECK(std::find(supported.begin(), supported.end(), operation) !=
                supported.end());
  }

  auto service = std::make_shared<McpLiveRoleService>();
  service->operation = [](McpOperationKind kind,
                          const boost::json::object& arguments,
                          std::stop_token stop_token) {
    if (stop_token.stop_requested()) {
      throw McpOperationCancelled();
    }
    const std::string action(McpOperationKindName(kind));
    const boost::json::array& node_ids = arguments.at("node_ids").as_array();
    if (node_ids.front().as_string() == "firo-cancel") {
      throw SimulationCancelled();
    }
    const bool adding = kind == McpOperationKind::kAddMasternode;
    const bool removing = kind == McpOperationKind::kRemoveMasternode;
    if (adding) {
      BOOST_TEST(arguments.at("count").as_uint64() == 1U);
      BOOST_TEST(arguments.at("funding_wallet_id").as_string() ==
                 "firo-wallet");
    }
    boost::json::object identity{
        {"node", 1U},
        {"node_id", "firo-1"},
        {"funding_wallet_node_id", "firo-wallet"},
        {"pro_tx_hash", "protx"},
        {"service", "10.77.0.2:18168"},
        {"collateral_address", "collateral"},
        {"owner_address", "owner"},
        {"operator_public_key", "operator-public"},
        {"voting_address", "voting"},
        {"payout_address", "payout"},
        {"collateral_hash", "collateral-hash"},
        {"collateral_index", 1U},
        {"state", removing ? "REVOKED" : "READY"},
        {"status", removing ? "revoked" : "ready"},
    };
    return boost::json::object{
        {"node_ids", boost::json::array{"firo-1"}},
        {"assigned_roles",
         adding ? boost::json::array{"masternode"} : boost::json::array{}},
        {"removed_roles",
         removing ? boost::json::array{"masternode"} : boost::json::array{}},
        {"action", action},
        {"state", removing ? "removed" : "ready"},
        {"created_node_ids", boost::json::array{}},
        {"role_generation", removing ? 3U : 2U},
        {"final_masternode_count", removing ? 0U : 1U},
        {"masternodes", boost::json::array{std::move(identity)}},
        {"inventory_generation", 1U},
        {"final_node_count", 1U},
    };
  };
  application.SetRoleService(service);
  application.MarkRunStarted();
  McpDispatcher dispatcher({}, application.OperationFactory(),
                           application.ResourceReader());
  dispatcher.SessionHandler()("live-session", true, {});

  const auto invoke = [&](std::string_view operation,
                          boost::json::object arguments) {
    arguments["run_id"] = "live-application";
    const boost::json::object submitted =
        Invoke(&dispatcher, operation, std::move(arguments));
    return WaitForTerminal(&dispatcher, submitted);
  };
  const boost::json::object added =
      invoke("masternode.add",
             boost::json::object{{"node_ids", boost::json::array{"firo-1"}},
                                 {"count", 1U},
                                 {"funding_wallet_id", "firo-wallet"}});
  BOOST_TEST(added.at("state").as_string() == "succeeded");
  const boost::json::object& add_result =
      added.at("terminal_result").as_object();
  BOOST_TEST(add_result.at("result_family").as_string() == "role_mutation");
  BOOST_TEST(add_result.at("action").as_string() == "masternode.add");
  BOOST_TEST(add_result.at("final_masternode_count").as_uint64() == 1U);
  BOOST_TEST(boost::json::serialize(add_result).find("operator_secret") ==
             std::string::npos);

  const boost::json::object restarted =
      invoke("masternode.restart",
             boost::json::object{{"node_ids", boost::json::array{"firo-1"}}});
  BOOST_TEST(restarted.at("state").as_string() == "succeeded");
  BOOST_TEST(restarted.at("terminal_result")
                 .as_object()
                 .at("role_generation")
                 .as_uint64() == 2U);

  const boost::json::object removed =
      invoke("masternode.remove",
             boost::json::object{{"node_ids", boost::json::array{"firo-1"}}});
  BOOST_TEST(removed.at("state").as_string() == "succeeded");
  BOOST_TEST(removed.at("terminal_result")
                 .as_object()
                 .at("removed_roles")
                 .as_array() == boost::json::array{"masternode"});

  const boost::json::object cancelled = invoke(
      "masternode.restart",
      boost::json::object{{"node_ids", boost::json::array{"firo-cancel"}}});
  BOOST_TEST(cancelled.at("state").as_string() == "cancelled");
  dispatcher.Shutdown();
  application.Shutdown();
}

#ifdef BBP_FIRO_GUI_LAUNCHER
BOOST_AUTO_TEST_CASE(
    mcp_live_firo_qt_launcher_binds_report_replaces_and_cleans_up) {
  LiveApplicationDirectory temporary;
  boost::json::object scenario = LiveScenario();
  scenario["nodes"] = 2U;
  const auto options =
      std::make_shared<Options>(ParseAndValidateScenario(scenario));
  WriteText(temporary.path() / "resolved-scenario.json",
            boost::json::serialize(ResolveScenario(scenario)) + "\n");
  AppendLine(
      temporary.path() / "events.jsonl",
      R"({"run_id":"live-application","node_id":"sim","event":"run_started"})");
  OperatorConnectionCommand connection;
  connection.executable = "/opt/firo/firo-qt";
  connection.data_dir = "/tmp/bbp-operator/firo-qt";
  connection.peer_address = "127.0.0.1";
  connection.peer_port = 18444U;
  connection.arguments = {
      "-regtest",
      "-datadir=" + connection.data_dir.string(),
      "-connect=127.0.0.1:18444",
      "-dns=0",
      "-dnsseed=0",
      "-forcednsseed=0",
      "-maxconnections=1",
      "-listen=0",
      "-discover=0",
      "-listenonion=0",
      "-torsetup=0",
      "-upnp=0",
  };
  const std::string operator_command = connection.ShellCommand();
  boost::json::array arguments;
  boost::json::array argv{connection.executable.string()};
  for (const std::string& argument : connection.arguments) {
    arguments.emplace_back(argument);
    argv.emplace_back(argument);
  }
  const boost::json::object detail{
      {"kind", "manual_firo_gui"},
      {"manual_launch", true},
      {"discovery_disabled", true},
      {"wallet_enabled", true},
      {"network", "regtest"},
      {"executable", connection.executable.string()},
      {"arguments", std::move(arguments)},
      {"argv", std::move(argv)},
      {"command", operator_command},
      {"data_dir", connection.data_dir.string()},
      {"peer_address", connection.peer_address},
      {"peer_port", connection.peer_port},
      {"peer_endpoint", "127.0.0.1:18444"},
  };
  AppendLine(temporary.path() / "events.jsonl",
             boost::json::serialize(boost::json::object{
                 {"run_id", "live-application"},
                 {"node_id", "_firo"},
                 {"event", "operator_connection_command"},
                 {"timestamp", "2026-07-27T00:00:00Z"},
                 {"detail", boost::json::serialize(detail)},
             }));

  auto queue = std::make_shared<SimulationCommandQueue>();
  std::unique_ptr<ChainDriver> launcher_driver = CreateDefaultChainDriver();
  auto launcher_service = launcher_driver->CreateOperatorConnectionLauncher(
      [connection](std::string_view node_id, std::stop_token) {
        if (node_id != "_firo") {
          throw std::runtime_error(
              "requested node has no authoritative Firo-Qt command");
        }
        return OperatorConnectionLauncherAuthority{
            .inventory_generation = 1U,
            .node_id = "_firo",
            .command = connection,
        };
      });
  McpLiveApplication application(McpLiveApplication::Config{
      .run_id = "live-application",
      .run_root = temporary.path(),
      .retained_run = std::nullopt,
      .options = options,
      .command_queue = queue,
      .operator_connection_launcher = launcher_service,
      .node_inventory_snapshot =
          [] {
            return McpLiveNodeInventorySnapshot{
                .generation = 1U, .node_ids = {"_firo", "firo-2"}};
          },
      .publication_mutex = std::make_shared<std::timed_mutex>(),
      .request_run_stop = [] {},
      .run_started = {},
      .run_stopping = {},
      .run_stopped = {}});
  application.MarkRunStarted();
  McpDispatcher dispatcher({}, application.OperationFactory(),
                           application.ResourceReader());
  dispatcher.SessionHandler()("live-session", true, {});

  const boost::json::object mismatched = WaitForTerminal(
      &dispatcher, Invoke(&dispatcher, "local.firo_qt_launcher",
                          boost::json::object{{"run_id", "live-application"},
                                              {"node_id", "firo-2"}}));
  BOOST_TEST(mismatched.at("state").as_string() == "failed");
  BOOST_TEST(!launcher_service->Snapshot().has_value());

  const auto create_launcher = [&] {
    return WaitForTerminal(
        &dispatcher, Invoke(&dispatcher, "local.firo_qt_launcher",
                            boost::json::object{{"run_id", "live-application"},
                                                {"node_id", "_firo"}}));
  };
  const boost::json::object first_operation = create_launcher();
  BOOST_TEST(first_operation.at("state").as_string() == "succeeded");
  const boost::json::object& first =
      first_operation.at("terminal_result").as_object();
  BOOST_TEST(first.size() == 10U);
  BOOST_TEST(first.at("result_family").as_string() == "mutation");
  BOOST_TEST(first.at("run_id").as_string() == "live-application");
  BOOST_TEST(first.at("added_node_ids").as_array().empty());
  BOOST_TEST(first.at("removed_node_ids").as_array().empty());
  const boost::json::array expected_affected{"_firo"};
  BOOST_TEST(first.at("affected_node_ids").as_array() == expected_affected);
  BOOST_TEST(first.at("action").as_string() == "local.firo_qt_launcher");
  BOOST_TEST(first.at("state").as_string() == "ready");
  BOOST_TEST(!first.at("unchanged").as_bool());
  BOOST_TEST(first.at("operator_command").as_string() == operator_command);
  const std::filesystem::path first_path(
      std::string(first.at("launcher_path").as_string()));
  BOOST_TEST(ReadText(first_path) ==
             "#!/bin/bash\nexec " + operator_command + "\n");
  const std::filesystem::perms permissions =
      std::filesystem::status(first_path).permissions();
  BOOST_CHECK((permissions & std::filesystem::perms::owner_all) ==
              std::filesystem::perms::owner_all);
  BOOST_CHECK((permissions & (std::filesystem::perms::group_all |
                              std::filesystem::perms::others_all)) ==
              std::filesystem::perms::none);
  const boost::json::object second_operation = create_launcher();
  BOOST_TEST(second_operation.at("state").as_string() == "succeeded");
  const std::filesystem::path second_path(
      std::string(second_operation.at("terminal_result")
                      .as_object()
                      .at("launcher_path")
                      .as_string()));
  BOOST_TEST(second_path != first_path);
  BOOST_TEST(!std::filesystem::exists(first_path));
  BOOST_TEST(std::filesystem::exists(second_path));

  SetFiroQtLauncherCleanupTestHook(
      [](FiroQtLauncherCleanupTestPhase phase, const std::filesystem::path&,
         const std::optional<std::filesystem::path>&) {
        if (phase ==
            FiroQtLauncherCleanupTestPhase::kAfterPublicIdentityCheck) {
          throw std::runtime_error("injected launcher cleanup failure");
        }
      });
  BOOST_CHECK_THROW(application.MarkRunStopped(), std::runtime_error);
  SetFiroQtLauncherCleanupTestHook({});
  BOOST_TEST(std::filesystem::exists(second_path));
  application.MarkRunStopped();
  BOOST_TEST(!std::filesystem::exists(second_path));
  BOOST_TEST(!launcher_service->Snapshot().has_value());

  boost::json::object uncertainty_report{
      {"chain", "firo"},
      {"operator_connection_command", detail},
  };
  boost::json::object& uncertainty_command =
      uncertainty_report.at("operator_connection_command").as_object();
  uncertainty_command["node_id"] = "_firo";
  uncertainty_command["timestamp"] = "2026-07-27T00:00:00Z";
  auto uncertain_launcher_service =
      launcher_driver->CreateOperatorConnectionLauncher(
          [connection](std::string_view node_id, std::stop_token) {
            if (node_id != "_firo") {
              throw std::runtime_error(
                  "requested node has no authoritative Firo-Qt command");
            }
            return OperatorConnectionLauncherAuthority{
                .inventory_generation = 1U,
                .node_id = "_firo",
                .command = connection,
            };
          });
  const OperatorConnectionLauncherSnapshot uncertain_launcher =
      uncertain_launcher_service->ReplaceFromReport(uncertainty_report,
                                                    "_firo");
  BOOST_REQUIRE(std::filesystem::remove(uncertain_launcher.launcher_path));
  WriteText(uncertain_launcher.launcher_path, "foreign launcher\n");
  McpLiveApplication uncertain_application(McpLiveApplication::Config{
      .run_id = "live-application",
      .run_root = temporary.path(),
      .retained_run = std::nullopt,
      .options = options,
      .command_queue = std::make_shared<SimulationCommandQueue>(),
      .operator_connection_launcher = uncertain_launcher_service,
      .node_inventory_snapshot =
          [] {
            return McpLiveNodeInventorySnapshot{
                .generation = 1U, .node_ids = {"_firo", "firo-2"}};
          },
      .publication_mutex = std::make_shared<std::timed_mutex>(),
      .request_run_stop = [] {},
      .run_started = {},
      .run_stopping = {},
      .run_stopped = {}});
  uncertain_application.MarkRunStarted();
  try {
    uncertain_application.MarkRunStopped();
    BOOST_FAIL("unverified launcher cleanup unexpectedly succeeded");
  } catch (const McpOperationFailure& error) {
    BOOST_TEST(error.code() == "run_cleanup_unverified");
    BOOST_TEST(!error.retryable());
  }
  BOOST_TEST(ReadText(uncertain_launcher.launcher_path) ==
             "foreign launcher\n");
  BOOST_TEST(std::filesystem::is_directory(temporary.path()));
  BOOST_REQUIRE(std::filesystem::remove(uncertain_launcher.launcher_path));

  dispatcher.Shutdown();
  application.Shutdown();
}
#endif

}  // namespace bbp
