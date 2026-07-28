#include <algorithm>
#include <boost/json/array.hpp>
#include <boost/json/object.hpp>
#include <boost/test/unit_test.hpp>
#include <chrono>
#include <memory>
#include <mutex>
#include <optional>
#include <set>
#include <stdexcept>
#include <string>
#include <thread>

#include "bbp/mcp_dispatcher.h"
#include "bbp/mcp_host_application.h"
#include "bbp/mcp_live_application.h"
#include "bbp/simulation_command_queue.h"
#include "bbp/simulator/options.h"

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
  std::size_t cleanup_calls = 0U;
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
          },
      .clean_run =
          [&](std::string_view run_id, std::chrono::seconds timeout,
              bool remove_retained_artifacts, std::stop_token stop_token) {
            BOOST_TEST(!stop_token.stop_requested());
            if (cleanup_calls == 0U) {
              BOOST_TEST(run_id == "_retained-default");
              BOOST_TEST(timeout == 30s);
              BOOST_TEST(!remove_retained_artifacts);
            } else {
              BOOST_TEST(cleanup_calls == 1U);
              BOOST_TEST(run_id == "retained-remove");
              BOOST_TEST(timeout == 3600s);
              BOOST_TEST(remove_retained_artifacts);
            }
            ++cleanup_calls;
            return McpRunCleanupResult{
                .run_id = std::string(run_id),
                .verified_owned = true,
                .processes_remaining = 0U,
                .network_resources_remaining = 0U,
                .cgroups_remaining = 0U,
                .credentials_remaining = 0U,
                .complete = true,
            };
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
  const std::vector<McpOperationKind> supported =
      application.SupportedOperations();
  BOOST_CHECK(std::find(supported.begin(), supported.end(),
                        McpOperationKind::kCleanRun) != supported.end());
#ifdef BBP_FIRO_GUI_LAUNCHER
  BOOST_CHECK(std::find(supported.begin(), supported.end(),
                        McpOperationKind::kCreateFiroQtLauncher) !=
              supported.end());
#else
  std::string unavailable_operation = "local";
  unavailable_operation += '.';
  unavailable_operation += "firo_qt_launcher";
  BOOST_CHECK(std::none_of(
      supported.begin(), supported.end(), [&](McpOperationKind operation) {
        return McpOperationKindName(operation) == unavailable_operation;
      }));
#endif
  for (const McpOperationKind operation :
       {McpOperationKind::kStopNode, McpOperationKind::kKillNode,
        McpOperationKind::kRestartNode, McpOperationKind::kAddWallet,
        McpOperationKind::kRemoveWallet, McpOperationKind::kAssignRole,
        McpOperationKind::kRemoveRole, McpOperationKind::kAddMiner,
        McpOperationKind::kRemoveMiner, McpOperationKind::kAddMasternode,
        McpOperationKind::kRemoveMasternode,
        McpOperationKind::kRestartMasternode, McpOperationKind::kStartWorkload,
        McpOperationKind::kInspectWorkload,
        McpOperationKind::kReconfigureWorkload,
        McpOperationKind::kPauseWorkload, McpOperationKind::kResumeWorkload,
        McpOperationKind::kStopWorkload}) {
    BOOST_CHECK(std::find(supported.begin(), supported.end(), operation) !=
                supported.end());
  }

  try {
    static_cast<void>(
        Invoke(&dispatcher, "workload.inspect",
               boost::json::object{{"run_id", "missing-run"},
                                   {"workload_id", "wallet-workload-1"}}));
    BOOST_FAIL("workload inspection without a run must fail");
  } catch (const McpOperationFailure& error) {
    BOOST_TEST(error.code() == "run_not_active");
  }

  const boost::json::object default_cleanup = WaitForTerminal(
      &dispatcher,
      Invoke(&dispatcher, "run.clean",
             boost::json::object{{"run_id", "_retained-default"}}));
  BOOST_TEST(default_cleanup.at("state").as_string() == "succeeded");
  const boost::json::object& default_cleanup_result =
      default_cleanup.at("terminal_result").as_object();
  BOOST_TEST(default_cleanup_result.at("result_family").as_string() ==
             "cleanup");
  BOOST_TEST(default_cleanup_result.at("run_id").as_string() ==
             "_retained-default");
  BOOST_TEST(default_cleanup_result.at("verified_owned").as_bool());
  BOOST_TEST(default_cleanup_result.at("processes_remaining").as_uint64() ==
             0U);
  BOOST_TEST(
      default_cleanup_result.at("network_resources_remaining").as_uint64() ==
      0U);
  BOOST_TEST(default_cleanup_result.at("cgroups_remaining").as_uint64() == 0U);
  BOOST_TEST(default_cleanup_result.at("credentials_remaining").as_uint64() ==
             0U);
  BOOST_TEST(default_cleanup_result.at("complete").as_bool());
  BOOST_CHECK_THROW(Invoke(&dispatcher, "run.clean",
                           boost::json::object{{"run_id", "retained.invalid"}}),
                    std::invalid_argument);
  BOOST_CHECK_THROW(
      Invoke(&dispatcher, "run.clean",
             boost::json::object{{"run_id", "retained-zero-timeout"},
                                 {"timeout_sec", 0U}}),
      std::invalid_argument);
  BOOST_CHECK_THROW(
      Invoke(&dispatcher, "run.clean",
             boost::json::object{{"run_id", "retained-long-timeout"},
                                 {"timeout_sec", 3601U}}),
      std::invalid_argument);

  const boost::json::object launch = WaitForTerminal(
      &dispatcher,
      Invoke(
          &dispatcher, "run.launch",
          boost::json::object{
              {"scenario", boost::json::object{{"run_id", "launched-run"}}}}));
  BOOST_TEST(launch.at("state").as_string() == "succeeded");
  BOOST_TEST(launch.at("terminal_result").as_object().at("state").as_string() ==
             "active");

  const boost::json::object removing_cleanup = WaitForTerminal(
      &dispatcher,
      Invoke(&dispatcher, "run.clean",
             boost::json::object{{"run_id", "retained-remove"},
                                 {"timeout_sec", 3600U},
                                 {"remove_retained_artifacts", true}}));
  BOOST_TEST(removing_cleanup.at("state").as_string() == "succeeded");
  const boost::json::object& removing_cleanup_result =
      removing_cleanup.at("terminal_result").as_object();
  BOOST_TEST(removing_cleanup_result.at("run_id").as_string() ==
             "retained-remove");
  BOOST_TEST(removing_cleanup_result.at("complete").as_bool());
  BOOST_TEST(cleanup_calls == 2U);

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

BOOST_AUTO_TEST_CASE(
    mcp_host_application_rejects_inexact_cleanup_callback_results) {
  std::size_t cleanup_calls = 0U;
  McpHostApplication application(McpHostApplication::Config{
      .host_id = "editor-host",
      .snapshot_run = [] { return std::optional<McpHostedRunSnapshot>{}; },
      .launch_run = [](const boost::json::object&,
                       std::stop_token) { return McpRunLifecycleResult{}; },
      .stop_run = [](std::string_view, std::chrono::seconds,
                     std::stop_token) { return McpRunLifecycleResult{}; },
      .clean_run =
          [&](std::string_view run_id, std::chrono::seconds, bool,
              std::stop_token) {
            McpRunCleanupResult result{
                .run_id = std::string(run_id),
                .verified_owned = true,
                .processes_remaining = 0U,
                .network_resources_remaining = 0U,
                .cgroups_remaining = 0U,
                .credentials_remaining = 0U,
                .complete = true,
            };
            switch (cleanup_calls++) {
              case 0U:
                result.run_id = "different-run";
                break;
              case 1U:
                result.verified_owned = false;
                break;
              case 2U:
                result.processes_remaining = 1U;
                break;
              case 3U:
                result.complete = false;
                break;
              default:
                BOOST_FAIL("unexpected cleanup callback invocation");
            }
            return result;
          }});
  McpDispatcher dispatcher({}, application.OperationFactory(),
                           application.ResourceReader());
  dispatcher.SessionHandler()("host-session", true, {});

  for (const std::string_view run_id :
       {"callback-id", "callback-ownership", "callback-residual",
        "callback-incomplete"}) {
    const boost::json::object terminal = WaitForTerminal(
        &dispatcher, Invoke(&dispatcher, "run.clean",
                            boost::json::object{{"run_id", run_id}}));
    BOOST_TEST(terminal.at("state").as_string() == "failed");
    BOOST_TEST(terminal.if_contains("terminal_result") == nullptr);
    const boost::json::object& error =
        terminal.at("terminal_error").as_object();
    BOOST_TEST(error.at("code").as_string() == "invalid_result_shape");
    BOOST_TEST(!error.at("retryable").as_bool());
  }
  BOOST_TEST(cleanup_calls == 4U);

  dispatcher.SessionHandler()("host-session", false, {});
  application.Shutdown();
}

BOOST_AUTO_TEST_CASE(mcp_host_application_delegates_generic_role_mutations) {
  const auto options = std::make_shared<Options>();
  options->node_capacity = 1U;
  const auto live_application =
      std::make_shared<McpLiveApplication>(McpLiveApplication::Config{
          .run_id = "role-run",
          .run_root = "/tmp/bbp-mcp-host-role-run",
          .retained_run = {},
          .options = options,
          .command_queue = std::make_shared<SimulationCommandQueue>(),
          .node_inventory_snapshot =
              [] {
                return McpLiveNodeInventorySnapshot{.generation = 1U,
                                                    .node_ids = {"firo-1"}};
              },
          .publication_mutex = std::make_shared<std::timed_mutex>(),
          .request_run_stop = [] {},
          .run_started = {},
          .run_stopping = {},
          .run_stopped = {}});
  std::set<McpOperationKind> delegated;
  auto role_service = std::make_shared<McpLiveRoleService>();
  role_service->operation = [&delegated](McpOperationKind kind,
                                         const boost::json::object& arguments,
                                         std::stop_token stop_token) {
    BOOST_CHECK(kind == McpOperationKind::kAddMiner ||
                kind == McpOperationKind::kRemoveMiner);
    BOOST_TEST(arguments.at("run_id").as_string() == "role-run");
    BOOST_TEST(arguments.at("node_ids").as_array() ==
               boost::json::array{"firo-1"});
    BOOST_TEST(arguments.if_contains("roles") == nullptr);
    BOOST_TEST(arguments.at("timeout_sec").as_uint64() == 30U);
    BOOST_TEST(!stop_token.stop_requested());
    delegated.insert(kind);
    if (kind == McpOperationKind::kRemoveMiner) {
      BOOST_TEST(arguments.if_contains("count") == nullptr);
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
    return boost::json::object{
        {"node_ids", boost::json::array{"firo-1"}},
        {"assigned_roles", boost::json::array{"miner"}},
        {"removed_roles", boost::json::array{}},
        {"action", "miner.add"},
        {"state", "ready"},
        {"created_node_ids", boost::json::array{}},
        {"role_generation", 2U},
        {"final_miner_count", 1U},
        {"inventory_generation", 1U},
        {"final_node_count", 1U},
    };
  };
  live_application->SetRoleService(role_service);
  live_application->MarkRunStarted();

  McpHostApplication application(McpHostApplication::Config{
      .host_id = "editor-host",
      .snapshot_run =
          [live_application] {
            return McpHostedRunSnapshot{.generation = 1U,
                                        .run_id = "role-run",
                                        .state = "active",
                                        .chain = "firo",
                                        .node_count = 1U,
                                        .application = live_application};
          },
      .launch_run = [](const boost::json::object&,
                       std::stop_token) { return McpRunLifecycleResult{}; },
      .stop_run = [](std::string_view, std::chrono::seconds,
                     std::stop_token) { return McpRunLifecycleResult{}; },
      .clean_run = [](std::string_view, std::chrono::seconds, bool,
                      std::stop_token) { return McpRunCleanupResult{}; }});
  const std::vector<McpOperationKind> supported =
      application.SupportedOperations();
  BOOST_CHECK(std::find(supported.begin(), supported.end(),
                        McpOperationKind::kAssignRole) != supported.end());
  BOOST_CHECK(std::find(supported.begin(), supported.end(),
                        McpOperationKind::kRemoveRole) != supported.end());

  McpDispatcher dispatcher({}, application.OperationFactory(),
                           application.ResourceReader());
  dispatcher.SessionHandler()("host-session", true, {});
  const boost::json::object terminal = WaitForTerminal(
      &dispatcher,
      Invoke(&dispatcher, "role.assign",
             boost::json::object{{"run_id", "role-run"},
                                 {"node_ids", boost::json::array{"firo-1"}},
                                 {"roles", boost::json::array{"miner"}},
                                 {"timeout_sec", 30U}}));
  BOOST_TEST(terminal.at("state").as_string() == "succeeded");
  BOOST_CHECK(delegated.contains(McpOperationKind::kAddMiner));
  const boost::json::object& result =
      terminal.at("terminal_result").as_object();
  BOOST_TEST(result.at("result_family").as_string() == "role_mutation");
  BOOST_TEST(result.at("action").as_string() == "role.assign");
  BOOST_TEST(result.at("assigned_roles").as_array() ==
             boost::json::array{"miner"});

  const boost::json::object removed = WaitForTerminal(
      &dispatcher,
      Invoke(&dispatcher, "role.remove",
             boost::json::object{{"run_id", "role-run"},
                                 {"node_ids", boost::json::array{"firo-1"}},
                                 {"roles", boost::json::array{"miner"}},
                                 {"timeout_sec", 30U}}));
  BOOST_TEST(removed.at("state").as_string() == "succeeded");
  BOOST_CHECK(delegated.contains(McpOperationKind::kRemoveMiner));
  const boost::json::object& removed_result =
      removed.at("terminal_result").as_object();
  BOOST_TEST(removed_result.at("result_family").as_string() == "role_mutation");
  BOOST_TEST(removed_result.at("action").as_string() == "role.remove");
  BOOST_TEST(removed_result.at("removed_roles").as_array() ==
             boost::json::array{"miner"});

  dispatcher.SessionHandler()("host-session", false, {});
  application.Shutdown();
  live_application->Shutdown();
}

BOOST_AUTO_TEST_CASE(mcp_host_application_rejects_run_work_while_starting) {
  std::string run_state = "starting";
  const auto live_application =
      std::make_shared<McpLiveApplication>(McpLiveApplication::Config{
          .run_id = "starting-run",
          .run_root = "/tmp/bbp-mcp-host-starting-run",
          .retained_run =
              McpLiveApplication::RetainedRun{
                  .chain = "firo", .node_count = 1U, .state = "starting"},
          .options = {},
          .command_queue = {},
          .publication_mutex = {},
          .request_run_stop = {},
          .run_started = {},
          .run_stopping = {},
          .run_stopped = {}});
  std::shared_ptr<McpLiveApplication> current_application = live_application;
  McpHostApplication application(McpHostApplication::Config{
      .host_id = "editor-host",
      .snapshot_run =
          [&current_application, &run_state] {
            return McpHostedRunSnapshot{.generation = 1U,
                                        .run_id = "starting-run",
                                        .state = run_state,
                                        .chain = "firo",
                                        .node_count = 1U,
                                        .application = current_application};
          },
      .launch_run = [](const boost::json::object&,
                       std::stop_token) { return McpRunLifecycleResult{}; },
      .stop_run = [](std::string_view, std::chrono::seconds,
                     std::stop_token) { return McpRunLifecycleResult{}; },
      .clean_run = [](std::string_view, std::chrono::seconds, bool,
                      std::stop_token) { return McpRunCleanupResult{}; }});
  McpDispatcher dispatcher({}, application.OperationFactory(),
                           application.ResourceReader());
  dispatcher.SessionHandler()("host-session", true, {});

  try {
    static_cast<void>(
        Invoke(&dispatcher, "workload.inspect",
               boost::json::object{{"run_id", "starting-run"},
                                   {"workload_id", "wallet-workload-1"}}));
    BOOST_FAIL("workload inspection while starting must fail");
  } catch (const McpOperationFailure& error) {
    BOOST_TEST(error.code() == "run_not_ready");
  }
  BOOST_CHECK_THROW(Invoke(&dispatcher, "run.report",
                           boost::json::object{{"run_id", "starting-run"}}),
                    McpOperationFailure);
  BOOST_CHECK_THROW(
      application.ResourceReader()(McpInformationFamily::kReports,
                                   "host-session", std::stop_token{}),
      McpOperationFailure);

  run_state = "active";
  const auto options = std::make_shared<Options>();
  options->node_capacity = 1U;
  current_application =
      std::make_shared<McpLiveApplication>(McpLiveApplication::Config{
          .run_id = "starting-run",
          .run_root = "/tmp/bbp-mcp-host-starting-run",
          .retained_run = {},
          .options = options,
          .command_queue = std::make_shared<SimulationCommandQueue>(),
          .node_inventory_snapshot =
              [] {
                return McpLiveNodeInventorySnapshot{.generation = 1U,
                                                    .node_ids = {"firo-1"}};
              },
          .publication_mutex = std::make_shared<std::timed_mutex>(),
          .request_run_stop = [] {},
          .run_started = {},
          .run_stopping = {},
          .run_stopped = {}});
  current_application->MarkRunStarted();
  try {
    static_cast<void>(
        Invoke(&dispatcher, "workload.inspect",
               boost::json::object{{"run_id", "starting-run"},
                                   {"workload_id", "wallet-workload-1"}}));
    BOOST_FAIL("workload inspection without a service must fail");
  } catch (const McpOperationFailure& error) {
    BOOST_TEST(error.code() == "workload_service_unavailable");
  }

  dispatcher.SessionHandler()("host-session", false, {});
}

}  // namespace bbp
