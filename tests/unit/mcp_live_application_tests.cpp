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
  dispatcher.SessionHandler()("live-session", true);

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

}  // namespace bbp
