#include <boost/json/object.hpp>
#include <boost/test/unit_test.hpp>
#include <chrono>
#include <memory>
#include <mutex>
#include <optional>
#include <stdexcept>
#include <string>
#include <thread>

#include "bbp/mcp_dispatcher.h"
#include "bbp/mcp_host_application.h"
#include "bbp/mcp_live_application.h"

namespace bbp {
namespace {

using namespace std::chrono_literals;

boost::json::object Invoke(McpDispatcher* dispatcher, std::string_view tool,
                           boost::json::object arguments) {
  boost::json::value result = dispatcher->ToolHandler()(
      tool, arguments, "host-session", std::stop_token{});
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
  throw std::runtime_error("MCP host operation did not finish");
}

}  // namespace

BOOST_AUTO_TEST_CASE(
    mcp_host_application_routes_runs_without_replacing_the_host) {
  std::mutex run_mutex;
  std::optional<McpHostedRunSnapshot> current_run;
  McpHostApplication application(McpHostApplication::Config{
      .host_id = "editor-host",
      .snapshot_run =
          [&] {
            std::lock_guard<std::mutex> lock(run_mutex);
            return current_run;
          },
      .launch_run =
          [&](const boost::json::object& scenario, std::stop_token) {
            BOOST_TEST(scenario.at("run_id").as_string() == "launched-run");
            std::lock_guard<std::mutex> lock(run_mutex);
            current_run = McpHostedRunSnapshot{.generation = 1U,
                                               .run_id = "launched-run",
                                               .state = "active",
                                               .chain = "firo",
                                               .node_count = 3U,
                                               .application = {}};
            return McpRunLifecycleResult{
                .run_id = "launched-run", .state = "active", .node_count = 3U};
          },
      .stop_run =
          [&](std::string_view run_id, std::chrono::seconds timeout,
              std::stop_token) {
            BOOST_TEST(run_id == "launched-run");
            BOOST_TEST(timeout == 30s);
            std::lock_guard<std::mutex> lock(run_mutex);
            current_run.reset();
            return McpRunLifecycleResult{
                .run_id = "launched-run", .state = "stopped", .node_count = 3U};
          }});
  McpDispatcher dispatcher({}, application.OperationFactory(),
                           application.ResourceReader());
  dispatcher.SessionHandler()("host-session", true, {});

  const boost::json::object capabilities =
      application
          .ResourceReader()(McpInformationFamily::kCapabilities, "host-session",
                            std::stop_token{})
          .as_object();
  BOOST_TEST(capabilities.at("host_id").as_string() == "editor-host");
  BOOST_TEST(capabilities.at("run_id").is_null());
  BOOST_TEST(capabilities.at("data").as_object().at("lifetime").as_string() ==
             "bbp_process");

  const boost::json::object launch = WaitForTerminal(
      &dispatcher,
      Invoke(
          &dispatcher, "run.launch",
          boost::json::object{
              {"scenario", boost::json::object{{"run_id", "launched-run"}}}}));
  BOOST_TEST(launch.at("state").as_string() == "succeeded");
  BOOST_TEST(launch.at("terminal_result").as_object().at("state").as_string() ==
             "active");

  const boost::json::object stop = WaitForTerminal(
      &dispatcher, Invoke(&dispatcher, "run.stop",
                          boost::json::object{{"run_id", "launched-run"}}));
  BOOST_TEST(stop.at("state").as_string() == "succeeded");
  BOOST_TEST(stop.at("terminal_result").as_object().at("state").as_string() ==
             "stopped");

  dispatcher.SessionHandler()("host-session", false, {});
  application.Shutdown();
  BOOST_CHECK_THROW(
      application.ResourceReader()(McpInformationFamily::kCapabilities,
                                   "host-session", std::stop_token{}),
      std::runtime_error);
}

BOOST_AUTO_TEST_CASE(mcp_host_application_rejects_run_work_while_starting) {
  const auto live_application =
      std::make_shared<McpLiveApplication>(McpLiveApplication::Config{
          .run_id = "starting-run",
          .run_root = "/tmp/bbp-mcp-host-starting-run",
          .retained_run =
              McpLiveApplication::RetainedRun{
                  .chain = "firo", .node_count = 1U, .state = "starting"},
          .options = {},
          .command_queue = {},
          .request_run_stop = {},
          .run_started = {},
          .run_stopping = {},
          .run_stopped = {}});
  McpHostApplication application(McpHostApplication::Config{
      .host_id = "editor-host",
      .snapshot_run =
          [live_application] {
            return McpHostedRunSnapshot{.generation = 1U,
                                        .run_id = "starting-run",
                                        .state = "starting",
                                        .chain = "firo",
                                        .node_count = 1U,
                                        .application = live_application};
          },
      .launch_run = [](const boost::json::object&,
                       std::stop_token) { return McpRunLifecycleResult{}; },
      .stop_run = [](std::string_view, std::chrono::seconds,
                     std::stop_token) { return McpRunLifecycleResult{}; }});
  McpDispatcher dispatcher({}, application.OperationFactory(),
                           application.ResourceReader());
  dispatcher.SessionHandler()("host-session", true, {});

  BOOST_CHECK_THROW(Invoke(&dispatcher, "run.report",
                           boost::json::object{{"run_id", "starting-run"}}),
                    McpOperationFailure);
  BOOST_CHECK_THROW(
      application.ResourceReader()(McpInformationFamily::kReports,
                                   "host-session", std::stop_token{}),
      McpOperationFailure);

  dispatcher.SessionHandler()("host-session", false, {});
}

}  // namespace bbp
