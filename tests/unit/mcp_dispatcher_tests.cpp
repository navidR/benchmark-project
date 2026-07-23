#include <atomic>
#include <boost/json/array.hpp>
#include <boost/json/object.hpp>
#include <boost/test/unit_test.hpp>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <stdexcept>
#include <string>
#include <thread>

#include "bbp/mcp_dispatcher.h"

namespace bbp {
namespace {

using namespace std::chrono_literals;

boost::json::object MinimalDispatcherScenario() {
  return boost::json::object{
      {"chain", "firo"},
      {"chain_daemon", "/bin/true"},
      {"run_id", "mcp-dispatcher-test"},
      {"nodes", 1U},
      {"block_production", boost::json::object{{"enabled", false}}}};
}

boost::json::object Invoke(McpDispatcher* dispatcher, std::string_view tool,
                           boost::json::object arguments,
                           std::string_view session = "session-a") {
  boost::json::value result =
      dispatcher->ToolHandler()(tool, arguments, session, std::stop_token{});
  BOOST_REQUIRE(result.is_object());
  return result.as_object();
}

boost::json::object WaitForTerminal(McpDispatcher* dispatcher,
                                    std::string operation_id) {
  const auto deadline = std::chrono::steady_clock::now() + 2s;
  while (std::chrono::steady_clock::now() < deadline) {
    boost::json::object result =
        Invoke(dispatcher, "operation.get",
               boost::json::object{{"operation_id", operation_id}});
    const std::string state(result.at("state").as_string());
    if (state == "succeeded" || state == "failed" || state == "cancelled") {
      return result;
    }
    std::this_thread::sleep_for(1ms);
  }
  throw std::runtime_error("MCP dispatcher operation did not finish");
}

}  // namespace

BOOST_AUTO_TEST_CASE(
    mcp_dispatcher_uses_production_scenario_validation_and_async_results) {
  McpDispatcher dispatcher;
  dispatcher.SessionHandler()("session-a", true, {});

  boost::json::object invalid = MinimalDispatcherScenario();
  invalid["simulation"] = boost::json::object{{"unexpected", true}};
  const boost::json::object submitted =
      Invoke(&dispatcher, "scenario.validate",
             boost::json::object{{"scenario", std::move(invalid)}});
  BOOST_TEST(submitted.at("state").as_string() == "queued");
  const boost::json::object terminal = WaitForTerminal(
      &dispatcher, std::string(submitted.at("operation_id").as_string()));
  BOOST_TEST(terminal.at("state").as_string() == "succeeded");
  const boost::json::object& validation =
      terminal.at("terminal_result").as_object();
  BOOST_TEST(validation.at("result_family").as_string() == "validation");
  BOOST_TEST(validation.at("valid").as_bool() == false);
  BOOST_REQUIRE_EQUAL(validation.at("diagnostics").as_array().size(), 1U);
  BOOST_TEST(validation.at("diagnostics")
                 .as_array()
                 .front()
                 .as_object()
                 .at("message")
                 .as_string() ==
             "scenario simulation has unsupported field: unexpected");

  const boost::json::object resolved =
      Invoke(&dispatcher, "scenario.resolve",
             boost::json::object{{"scenario", MinimalDispatcherScenario()}});
  const boost::json::object resolved_terminal = WaitForTerminal(
      &dispatcher, std::string(resolved.at("operation_id").as_string()));
  BOOST_TEST(resolved_terminal.at("terminal_result")
                 .as_object()
                 .at("scenario")
                 .as_object()
                 .at("node_configs")
                 .as_array()
                 .front()
                 .as_object()
                 .at("id")
                 .as_string() == "firo-1");

  BOOST_CHECK_THROW(
      Invoke(&dispatcher, "scenario.validate",
             boost::json::object{{"scenario", MinimalDispatcherScenario()},
                                 {"unexpected", true}}),
      std::invalid_argument);
  dispatcher.SessionHandler()("session-a", false, {});
  BOOST_TEST(dispatcher.Stats().sessions == 0U);
}

BOOST_AUTO_TEST_CASE(
    mcp_dispatcher_routes_typed_application_work_and_cleans_session_state) {
  std::atomic<bool> started = false;
  McpDispatcher dispatcher({}, [&](McpOperationKind kind,
                                   const boost::json::object& arguments,
                                   std::string_view session_id) {
    BOOST_CHECK(kind == McpOperationKind::kReportRun);
    BOOST_TEST(arguments.at("run_id").as_string() == "run-a");
    BOOST_TEST(session_id == "session-a");
    return McpOperationPlan{.progress_total = 2U,
                            .executor = [&](McpOperationContext& context) {
                              started.store(true, std::memory_order_release);
                              context.ReportProgress(1U);
                              while (!context.stop_requested()) {
                                std::this_thread::sleep_for(1ms);
                              }
                              context.ThrowIfCancelled();
                              return McpTypedResult{};
                            }};
  });
  dispatcher.SessionHandler()("session-a", true, {});
  const boost::json::object submitted = Invoke(
      &dispatcher, "run.report", boost::json::object{{"run_id", "run-a"}});
  const auto deadline = std::chrono::steady_clock::now() + 1s;
  while (!started.load(std::memory_order_acquire) &&
         std::chrono::steady_clock::now() < deadline) {
    std::this_thread::sleep_for(1ms);
  }
  BOOST_REQUIRE(started.load(std::memory_order_acquire));
  dispatcher.SessionHandler()("session-a", false, {});
  const McpOperationServiceStats stats = dispatcher.Stats();
  BOOST_TEST(stats.sessions == 0U);
  BOOST_TEST(stats.active_operations == 0U);
  BOOST_TEST(stats.cancelled_operations == 1U);
  BOOST_CHECK_THROW(Invoke(&dispatcher, "operation.get",
                           boost::json::object{
                               {"operation_id", submitted.at("operation_id")}}),
                    std::runtime_error);
}

BOOST_AUTO_TEST_CASE(
    mcp_dispatcher_session_close_drains_a_synchronous_resource_read) {
  std::atomic<bool> read_started = false;
  std::atomic<bool> cancellation_seen = false;
  std::atomic<bool> read_finished = false;
  McpDispatcher dispatcher(
      {}, {},
      [&](McpInformationFamily family, std::string_view session_id,
          std::stop_token stop_token) {
        BOOST_CHECK(family == McpInformationFamily::kLogs);
        BOOST_TEST(session_id == "session-a");
        read_started.store(true, std::memory_order_release);
        const auto deadline = std::chrono::steady_clock::now() + 2s;
        while (!stop_token.stop_requested() &&
               std::chrono::steady_clock::now() < deadline) {
          std::this_thread::yield();
        }
        cancellation_seen.store(stop_token.stop_requested(),
                                std::memory_order_release);
        read_finished.store(true, std::memory_order_release);
        return boost::json::object{};
      });
  dispatcher.SessionHandler()("session-a", true, {});
  const McpResourceHandler read_resource = dispatcher.ResourceHandler();
  std::jthread reader([&] {
    static_cast<void>(
        read_resource("bbp:///logs", "session-a", std::stop_token{}));
  });
  const auto start_deadline = std::chrono::steady_clock::now() + 2s;
  while (!read_started.load(std::memory_order_acquire) &&
         std::chrono::steady_clock::now() < start_deadline) {
    std::this_thread::yield();
  }

  BOOST_REQUIRE(read_started.load(std::memory_order_acquire));
  dispatcher.SessionHandler()("session-a", false, {});
  BOOST_TEST(cancellation_seen.load(std::memory_order_acquire));
  BOOST_TEST(read_finished.load(std::memory_order_acquire));
  reader.join();
  BOOST_TEST(dispatcher.Stats().sessions == 0U);
}

BOOST_AUTO_TEST_CASE(
    mcp_dispatcher_subscriptions_filter_and_forward_bounded_notifications) {
  McpDispatcher dispatcher;
  std::atomic<std::size_t> delivered = 0U;
  dispatcher.SetNotificationHandler(
      [&](const McpServiceNotification&) { delivered.fetch_add(1U); });
  dispatcher.SessionHandler()("session-a", true, {});
  const boost::json::object subscription =
      Invoke(&dispatcher, "subscription.create",
             boost::json::object{{"run_id", "run-a"},
                                 {"families", boost::json::array{"metrics"}},
                                 {"node_ids", boost::json::array{"node-1"}}});
  dispatcher.Publish(McpEvidenceRecord{.family = McpInformationFamily::kEvents,
                                       .sequence = 0U,
                                       .timestamp_ms = 1U,
                                       .node_id = "node-1",
                                       .kind = "ignored",
                                       .message = std::nullopt,
                                       .artifact_id = std::nullopt,
                                       .data = std::nullopt});
  dispatcher.Publish(McpEvidenceRecord{.family = McpInformationFamily::kMetrics,
                                       .sequence = 0U,
                                       .timestamp_ms = 2U,
                                       .node_id = "node-1",
                                       .kind = "sample",
                                       .message = "ready",
                                       .artifact_id = std::nullopt,
                                       .data = std::nullopt});
  const boost::json::object page =
      Invoke(&dispatcher, "subscription.poll",
             boost::json::object{
                 {"subscription_id", subscription.at("subscription_id")},
                 {"cursor", "0"},
                 {"limit", 4U}});
  BOOST_REQUIRE_EQUAL(page.at("items").as_array().size(), 1U);
  BOOST_TEST(page.at("items")
                 .as_array()
                 .front()
                 .as_object()
                 .at("message")
                 .as_string() == "ready");
  BOOST_TEST(delivered.load() == 1U);
  dispatcher.SessionHandler()("session-a", false, {});
}

}  // namespace bbp
