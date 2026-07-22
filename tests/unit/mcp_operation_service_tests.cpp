#include <algorithm>
#include <atomic>
#include <boost/json/array.hpp>
#include <boost/json/object.hpp>
#include <boost/test/unit_test.hpp>
#include <chrono>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <mutex>
#include <set>
#include <stop_token>
#include <string>
#include <thread>
#include <vector>

#include "bbp/mcp_operation_service.h"

namespace bbp {
namespace {

using namespace std::chrono_literals;

McpOperationServiceConfig TestConfig(std::size_t sessions = 4U,
                                     std::size_t tasks = 8U,
                                     std::size_t subscriptions = 4U,
                                     std::size_t notifications = 8U,
                                     std::size_t retained_operations = 16U,
                                     std::size_t page = 8U) {
  return McpOperationServiceConfig{
      .maximum_sessions = sessions,
      .maximum_tasks_per_session = tasks,
      .maximum_subscriptions_per_session = subscriptions,
      .maximum_notifications_per_subscription = notifications,
      .maximum_retained_operations = retained_operations,
      .maximum_poll_items = page};
}

McpTypedResult ValidationResult() {
  return McpTypedResult{
      .family = McpResultFamily::kValidation,
      .value = boost::json::object{{"result_family", "validation"},
                                   {"valid", true},
                                   {"diagnostics", boost::json::array{}}}};
}

McpOperationSnapshot WaitForTerminal(McpOperationService* service,
                                     std::string_view session_id,
                                     std::string_view operation_id,
                                     std::chrono::milliseconds timeout = 2s) {
  const std::optional<McpOperationSnapshot> terminal =
      service->WaitForOperation(session_id, operation_id, timeout);
  BOOST_REQUIRE(static_cast<bool>(terminal));
  return *terminal;
}

void WaitUntilState(McpOperationService* service, std::string_view session_id,
                    std::string_view operation_id, McpOperationState state) {
  const auto deadline = std::chrono::steady_clock::now() + 2s;
  while (std::chrono::steady_clock::now() < deadline) {
    if (service->GetOperation(session_id, operation_id).state == state) {
      return;
    }
    std::this_thread::sleep_for(1ms);
  }
  BOOST_FAIL("MCP operation did not reach the expected state");
}

McpOperationExecutor CancellableExecutor(std::atomic<bool>* started = nullptr) {
  return [started](McpOperationContext& context) {
    if (started != nullptr) {
      started->store(true, std::memory_order_release);
    }
    while (!context.stop_requested()) {
      std::this_thread::sleep_for(1ms);
    }
    context.ThrowIfCancelled();
    return ValidationResult();
  };
}

McpEvidenceRecord Evidence(McpInformationFamily family, std::string node_id,
                           std::string message) {
  return McpEvidenceRecord{.family = family,
                           .sequence = 0U,
                           .timestamp_ms = 100U,
                           .node_id = std::move(node_id),
                           .kind = "sample",
                           .message = std::move(message),
                           .artifact_id = std::nullopt};
}

}  // namespace

BOOST_AUTO_TEST_CASE(mcp_operation_ids_are_unique_under_concurrent_submission) {
  McpOperationService service(TestConfig(2U, 32U, 2U, 2U, 32U, 2U));
  service.RegisterSession("session-a");
  std::mutex ids_mutex;
  std::vector<std::string> ids;
  std::vector<std::jthread> submitters;
  constexpr std::size_t kSubmissionCount = 24U;
  for (std::size_t index = 0U; index < kSubmissionCount; ++index) {
    submitters.emplace_back([&] {
      const McpOperationSnapshot operation = service.Submit(
          "session-a", McpOperationKind::kValidateScenario, 1U,
          [](McpOperationContext&) { return ValidationResult(); });
      std::lock_guard<std::mutex> lock(ids_mutex);
      ids.push_back(operation.operation_id);
    });
  }
  for (std::jthread& submitter : submitters) {
    submitter.join();
  }
  BOOST_REQUIRE_EQUAL(ids.size(), kSubmissionCount);
  const std::set<std::string> unique(ids.begin(), ids.end());
  BOOST_TEST(unique.size() == kSubmissionCount);
  for (const std::string& id : ids) {
    const McpOperationSnapshot terminal =
        WaitForTerminal(&service, "session-a", id);
    BOOST_CHECK(terminal.state == McpOperationState::kSucceeded);
    BOOST_TEST(terminal.progress.completed == terminal.progress.total);
  }
  const McpOperationServiceStats stats = service.Stats();
  BOOST_TEST(stats.submitted_operations == kSubmissionCount);
  BOOST_TEST(stats.succeeded_operations == kSubmissionCount);
  BOOST_TEST(stats.active_operations == 0U);
  BOOST_TEST(stats.retained_operations == kSubmissionCount);
}

BOOST_AUTO_TEST_CASE(
    mcp_operation_capacity_rejects_per_session_and_all_active_history) {
  McpOperationService service(TestConfig(3U, 1U, 1U, 1U, 2U, 1U));
  service.RegisterSession("session-a");
  service.RegisterSession("session-b");
  service.RegisterSession("session-c");
  const McpOperationSnapshot first =
      service.Submit("session-a", McpOperationKind::kValidateScenario, 1U,
                     CancellableExecutor());
  WaitUntilState(&service, "session-a", first.operation_id,
                 McpOperationState::kRunning);
  BOOST_CHECK_THROW(
      service.Submit("session-a", McpOperationKind::kValidateScenario, 1U,
                     CancellableExecutor()),
      std::runtime_error);

  const McpOperationSnapshot second =
      service.Submit("session-b", McpOperationKind::kValidateScenario, 1U,
                     CancellableExecutor());
  WaitUntilState(&service, "session-b", second.operation_id,
                 McpOperationState::kRunning);
  BOOST_CHECK_THROW(
      service.Submit("session-c", McpOperationKind::kValidateScenario, 1U,
                     CancellableExecutor()),
      std::runtime_error);
  BOOST_TEST(service.Stats().retained_operations == 2U);
  BOOST_TEST(service.Stats().active_operations == 2U);

  service.CancelOperation("session-a", first.operation_id);
  service.CancelOperation("session-b", second.operation_id);
  BOOST_CHECK(
      WaitForTerminal(&service, "session-a", first.operation_id).state ==
      McpOperationState::kCancelled);
  BOOST_CHECK(
      WaitForTerminal(&service, "session-b", second.operation_id).state ==
      McpOperationState::kCancelled);
}

BOOST_AUTO_TEST_CASE(mcp_operation_history_evicts_oldest_terminal_only) {
  McpOperationService service(TestConfig(1U, 2U, 1U, 1U, 2U, 1U));
  service.RegisterSession("session-a");
  const auto submit_success = [&] {
    return service.Submit(
        "session-a", McpOperationKind::kValidateScenario, 1U,
        [](McpOperationContext&) { return ValidationResult(); });
  };
  const McpOperationSnapshot first = submit_success();
  WaitForTerminal(&service, "session-a", first.operation_id);
  const McpOperationSnapshot second = submit_success();
  WaitForTerminal(&service, "session-a", second.operation_id);
  const McpOperationSnapshot third = submit_success();
  WaitForTerminal(&service, "session-a", third.operation_id);

  BOOST_CHECK_THROW(service.GetOperation("session-a", first.operation_id),
                    std::runtime_error);
  BOOST_CHECK(service.GetOperation("session-a", second.operation_id).state ==
              McpOperationState::kSucceeded);
  BOOST_CHECK(service.GetOperation("session-a", third.operation_id).state ==
              McpOperationState::kSucceeded);
  BOOST_TEST(service.Stats().retained_operations == 2U);
  BOOST_TEST(service.Stats().evicted_operations == 1U);
}

BOOST_AUTO_TEST_CASE(
    mcp_operation_progress_is_monotonic_bounded_and_terminal_is_immutable) {
  std::atomic<bool> rejected_regression = false;
  std::atomic<bool> rejected_overflow = false;
  McpOperationService service;
  service.RegisterSession("session-a");
  const McpOperationSnapshot submitted = service.Submit(
      "session-a", McpOperationKind::kValidateScenario, 4U,
      [&](McpOperationContext& context) {
        context.ReportProgress(2U);
        try {
          context.ReportProgress(1U);
        } catch (const std::invalid_argument&) {
          rejected_regression.store(true, std::memory_order_release);
        }
        try {
          context.ReportProgress(5U);
        } catch (const std::invalid_argument&) {
          rejected_overflow.store(true, std::memory_order_release);
        }
        context.ReportProgress(4U);
        return ValidationResult();
      });
  const McpOperationSnapshot terminal =
      WaitForTerminal(&service, "session-a", submitted.operation_id);
  BOOST_TEST(rejected_regression.load(std::memory_order_acquire));
  BOOST_TEST(rejected_overflow.load(std::memory_order_acquire));
  BOOST_CHECK(terminal.state == McpOperationState::kSucceeded);
  BOOST_TEST(terminal.progress.completed == 4U);
  const boost::json::object terminal_json = McpOperationSnapshotJson(terminal);
  BOOST_TEST(terminal_json.at("terminal_result")
                 .as_object()
                 .at("result_family")
                 .as_string() == "validation");
  BOOST_TEST(!terminal_json.contains("terminal_error"));
  const McpOperationCancellation cancellation =
      service.CancelOperation("session-a", submitted.operation_id);
  BOOST_TEST(cancellation.already_terminal);
  BOOST_TEST(!cancellation.request_accepted);
  BOOST_TEST(cancellation.operation.sequence == terminal.sequence);
}

BOOST_AUTO_TEST_CASE(mcp_operation_cancellation_is_bounded_and_truthful) {
  McpOperationService service;
  service.RegisterSession("session-a");
  std::atomic<bool> started = false;
  const McpOperationSnapshot submitted =
      service.Submit("session-a", McpOperationKind::kValidateScenario, 10U,
                     CancellableExecutor(&started));
  WaitUntilState(&service, "session-a", submitted.operation_id,
                 McpOperationState::kRunning);
  const auto start_deadline = std::chrono::steady_clock::now() + 500ms;
  while (!started.load(std::memory_order_acquire) &&
         std::chrono::steady_clock::now() < start_deadline) {
    std::this_thread::yield();
  }
  BOOST_TEST(started.load(std::memory_order_acquire));

  const auto began = std::chrono::steady_clock::now();
  const McpOperationCancellation cancellation =
      service.CancelOperation("session-a", submitted.operation_id);
  BOOST_TEST(cancellation.request_accepted);
  BOOST_TEST(!cancellation.already_terminal);
  BOOST_TEST(cancellation.operation.cancel_requested);
  BOOST_CHECK(cancellation.operation.state == McpOperationState::kCancelling);
  const McpOperationSnapshot terminal =
      WaitForTerminal(&service, "session-a", submitted.operation_id, 500ms);
  BOOST_CHECK(std::chrono::steady_clock::now() - began < 500ms);
  BOOST_CHECK(terminal.state == McpOperationState::kCancelled);
  BOOST_REQUIRE(static_cast<bool>(terminal.error));
  BOOST_TEST(terminal.error->code == "cancelled");
  BOOST_CHECK(terminal.terminal_result_family == McpResultFamily::kError);
  BOOST_TEST(service.CancelOperation("session-a", submitted.operation_id)
                 .already_terminal);
}

BOOST_AUTO_TEST_CASE(mcp_operation_failure_retains_typed_bounded_error) {
  McpOperationService service;
  service.RegisterSession("session-a");
  const McpOperationSnapshot submitted = service.Submit(
      "session-a", McpOperationKind::kValidateScenario, 2U,
      [](McpOperationContext& context) -> McpTypedResult {
        context.ReportProgress(1U);
        throw McpOperationFailure(
            "rpc_unavailable", "daemon RPC did not become ready", true,
            boost::json::array{boost::json::object{
                {"code", "rpc_timeout"},
                {"message", "readiness deadline expired"}}});
      });
  const McpOperationSnapshot terminal =
      WaitForTerminal(&service, "session-a", submitted.operation_id);
  BOOST_CHECK(terminal.state == McpOperationState::kFailed);
  BOOST_TEST(terminal.progress.completed == 1U);
  BOOST_REQUIRE(static_cast<bool>(terminal.error));
  BOOST_TEST(terminal.error->code == "rpc_unavailable");
  BOOST_TEST(terminal.error->retryable);
  BOOST_TEST(terminal.error->diagnostics.size() == 1U);
  const boost::json::object snapshot_json = McpOperationSnapshotJson(terminal);
  BOOST_TEST(
      snapshot_json.at("terminal_error").as_object().at("code").as_string() ==
      "rpc_unavailable");
  BOOST_TEST(!snapshot_json.contains("terminal_result"));
  const boost::json::object json =
      McpOperationErrorJson(*terminal.error, terminal.operation_id);
  BOOST_TEST(json.at("result_family").as_string() == "error");
  BOOST_TEST(json.at("operation_id").as_string() == terminal.operation_id);
}

BOOST_AUTO_TEST_CASE(mcp_operation_rejects_wrong_typed_terminal_result) {
  McpOperationService service;
  service.RegisterSession("session-a");
  const McpOperationSnapshot submitted = service.Submit(
      "session-a", McpOperationKind::kValidateScenario, 1U,
      [](McpOperationContext&) {
        return McpTypedResult{
            .family = McpResultFamily::kScenario,
            .value = boost::json::object{{"result_family", "scenario"}}};
      });
  const McpOperationSnapshot terminal =
      WaitForTerminal(&service, "session-a", submitted.operation_id);
  BOOST_CHECK(terminal.state == McpOperationState::kFailed);
  BOOST_REQUIRE(static_cast<bool>(terminal.error));
  BOOST_TEST(terminal.error->code == "invalid_result_family");
}

BOOST_AUTO_TEST_CASE(
    mcp_operation_rejects_oversized_results_and_subscription_selection) {
  McpOperationService service;
  service.RegisterSession("session-a");
  const McpOperationSnapshot submitted = service.Submit(
      "session-a", McpOperationKind::kValidateScenario, 1U,
      [](McpOperationContext&) {
        return McpTypedResult{
            .family = McpResultFamily::kValidation,
            .value = boost::json::object{
                {"result_family", "validation"},
                {"payload",
                 std::string(kMcpMaximumRetainedResultBytes + 1U, 'x')}}};
      });
  const McpOperationSnapshot terminal =
      WaitForTerminal(&service, "session-a", submitted.operation_id);
  BOOST_CHECK(terminal.state == McpOperationState::kFailed);
  BOOST_REQUIRE(static_cast<bool>(terminal.error));
  BOOST_TEST(terminal.error->code == "result_too_large");

  McpSubscriptionRequest request{.families = {McpInformationFamily::kMetrics},
                                 .node_ids = std::vector<std::string>(
                                     kMcpMaximumSelectionItems + 1U, "node-1"),
                                 .cursor = 0U};
  BOOST_CHECK_THROW(service.CreateSubscription("session-a", std::move(request)),
                    std::invalid_argument);
}

BOOST_AUTO_TEST_CASE(mcp_operation_enforces_session_ownership_and_capacity) {
  McpOperationService service(TestConfig(2U));
  service.RegisterSession("session-a");
  service.RegisterSession("session-b");
  BOOST_CHECK_THROW(service.RegisterSession("session-c"), std::runtime_error);
  const McpOperationSnapshot submitted =
      service.Submit("session-a", McpOperationKind::kValidateScenario, 1U,
                     [](McpOperationContext&) { return ValidationResult(); });
  WaitForTerminal(&service, "session-a", submitted.operation_id);
  BOOST_CHECK_THROW(service.GetOperation("session-b", submitted.operation_id),
                    std::runtime_error);
  BOOST_CHECK_THROW(
      service.CancelOperation("session-b", submitted.operation_id),
      std::runtime_error);
}

BOOST_AUTO_TEST_CASE(mcp_operation_session_removal_cancels_owned_state) {
  McpOperationService service(TestConfig());
  service.RegisterSession("session-a");
  const McpSubscriptionSnapshot subscription = service.CreateSubscription(
      "session-a",
      McpSubscriptionRequest{.families = {McpInformationFamily::kMetrics},
                             .node_ids = {},
                             .cursor = 0U});
  const McpOperationSnapshot submitted =
      service.Submit("session-a", McpOperationKind::kValidateScenario, 1U,
                     CancellableExecutor());
  WaitUntilState(&service, "session-a", submitted.operation_id,
                 McpOperationState::kRunning);
  const auto began = std::chrono::steady_clock::now();
  BOOST_TEST(service.RemoveSession("session-a", 500ms));
  BOOST_CHECK(std::chrono::steady_clock::now() - began < 500ms);
  const McpOperationServiceStats stats = service.Stats();
  BOOST_TEST(stats.sessions == 0U);
  BOOST_TEST(stats.active_operations == 0U);
  BOOST_TEST(stats.active_workers == 0U);
  BOOST_TEST(stats.active_subscriptions == 0U);
  BOOST_TEST(stats.retained_subscriptions == 0U);
  BOOST_CHECK_THROW(service.PollSubscription(
                        "session-a", subscription.subscription_id, 0U, 1U),
                    std::runtime_error);
}

BOOST_AUTO_TEST_CASE(mcp_operation_shutdown_requests_stop_and_joins_workers) {
  McpOperationService service(TestConfig(2U, 8U));
  service.RegisterSession("session-a");
  for (std::size_t index = 0U; index < 8U; ++index) {
    service.Submit("session-a", McpOperationKind::kValidateScenario, 1U,
                   CancellableExecutor());
  }
  const auto deadline = std::chrono::steady_clock::now() + 2s;
  while (service.Stats().active_operations != 8U &&
         std::chrono::steady_clock::now() < deadline) {
    std::this_thread::sleep_for(1ms);
  }
  BOOST_REQUIRE(service.Stats().active_operations == 8U);
  const auto began = std::chrono::steady_clock::now();
  service.Shutdown();
  BOOST_CHECK(std::chrono::steady_clock::now() - began < 500ms);
  const McpOperationServiceStats stats = service.Stats();
  BOOST_TEST(!stats.accepting);
  BOOST_TEST(stats.sessions == 0U);
  BOOST_TEST(stats.active_operations == 0U);
  BOOST_TEST(stats.active_workers == 0U);
  BOOST_TEST(stats.active_subscriptions == 0U);
  BOOST_CHECK_THROW(service.RegisterSession("session-b"), std::runtime_error);
  service.Shutdown();
}

BOOST_AUTO_TEST_CASE(mcp_operation_callbacks_never_run_under_service_lock) {
  McpOperationService* service_pointer = nullptr;
  std::atomic<std::size_t> notifications = 0U;
  std::atomic<bool> callback_failed = false;
  McpOperationService service(TestConfig(), [&](const McpServiceNotification&) {
    if (service_pointer == nullptr) {
      callback_failed.store(true, std::memory_order_release);
      return;
    }
    static_cast<void>(service_pointer->Stats());
    notifications.fetch_add(1U, std::memory_order_relaxed);
  });
  service_pointer = &service;
  service.RegisterSession("session-a");
  const McpOperationSnapshot submitted =
      service.Submit("session-a", McpOperationKind::kValidateScenario, 2U,
                     [&](McpOperationContext& context) {
                       static_cast<void>(service.Stats());
                       context.ReportProgress(1U);
                       return ValidationResult();
                     });
  BOOST_CHECK(
      WaitForTerminal(&service, "session-a", submitted.operation_id).state ==
      McpOperationState::kSucceeded);
  service.CreateSubscription(
      "session-a",
      McpSubscriptionRequest{.families = {McpInformationFamily::kMetrics},
                             .node_ids = {},
                             .cursor = 0U});
  service.Publish(Evidence(McpInformationFamily::kMetrics, "node-1", "sample"));
  BOOST_TEST(!callback_failed.load(std::memory_order_acquire));
  BOOST_TEST(notifications.load(std::memory_order_relaxed) >= 4U);
}

BOOST_AUTO_TEST_CASE(mcp_subscription_capacity_and_cancellation_are_bounded) {
  McpOperationService service(TestConfig(2U, 1U, 2U, 2U, 2U, 2U));
  service.RegisterSession("session-a");
  service.RegisterSession("session-b");
  const McpSubscriptionRequest request{
      .families = {McpInformationFamily::kMetrics},
      .node_ids = {},
      .cursor = 0U};
  const McpSubscriptionSnapshot first =
      service.CreateSubscription("session-a", request);
  service.CreateSubscription("session-a", request);
  BOOST_CHECK_THROW(service.CreateSubscription("session-a", request),
                    std::runtime_error);
  BOOST_CHECK_THROW(
      service.CancelSubscription("session-b", first.subscription_id),
      std::runtime_error);

  BOOST_TEST(
      !service.CancelSubscription("session-a", first.subscription_id).active);
  BOOST_TEST(
      !service.CancelSubscription("session-a", first.subscription_id).active);
  const McpSubscriptionSnapshot replacement =
      service.CreateSubscription("session-a", request);
  BOOST_TEST(replacement.active);
  BOOST_TEST(service.Stats().retained_subscriptions == 2U);
  BOOST_TEST(service.Stats().active_subscriptions == 2U);
  BOOST_CHECK_THROW(
      service.CancelSubscription("session-a", first.subscription_id),
      std::runtime_error);
}

BOOST_AUTO_TEST_CASE(
    mcp_subscription_filters_bounds_queue_and_reports_exact_drops) {
  McpOperationService service(TestConfig(1U, 1U, 2U, 2U, 2U, 2U));
  service.RegisterSession("session-a");
  const McpSubscriptionSnapshot subscription = service.CreateSubscription(
      "session-a",
      McpSubscriptionRequest{.families = {McpInformationFamily::kMetrics},
                             .node_ids = {"node-1"},
                             .cursor = 0U});
  service.Publish(Evidence(McpInformationFamily::kEvents, "node-1", "wrong"));
  service.Publish(
      Evidence(McpInformationFamily::kMetrics, "node-2", "wrong-node"));
  service.Publish(Evidence(McpInformationFamily::kMetrics, "node-1", "one"));
  service.Publish(Evidence(McpInformationFamily::kMetrics, "node-1", "two"));
  service.Publish(Evidence(McpInformationFamily::kMetrics, "node-1", "three"));

  const McpSubscriptionSnapshot page = service.PollSubscription(
      "session-a", subscription.subscription_id, 0U, 2U);
  BOOST_REQUIRE_EQUAL(page.items.size(), 2U);
  BOOST_TEST(page.items[0].sequence == 2U);
  BOOST_TEST(page.items[0].message.value() == "two");
  BOOST_TEST(page.items[1].sequence == 3U);
  BOOST_TEST(page.next_cursor == 3U);
  BOOST_TEST(page.dropped == 1U);
  BOOST_TEST(service.Stats().notifications_published == 3U);
  BOOST_TEST(service.Stats().notifications_dropped == 1U);

  const McpSubscriptionSnapshot resumed = service.PollSubscription(
      "session-a", subscription.subscription_id, 2U, 2U);
  BOOST_REQUIRE_EQUAL(resumed.items.size(), 1U);
  BOOST_TEST(resumed.items.front().sequence == 3U);
  BOOST_TEST(resumed.next_cursor == 3U);
  const boost::json::object json = McpSubscriptionSnapshotJson(resumed);
  BOOST_TEST(json.at("result_family").as_string() == "subscription");
  BOOST_TEST(json.at("next_cursor").as_string() == "3");
}

BOOST_AUTO_TEST_CASE(mcp_subscription_poll_wakes_on_publish_and_cancel) {
  McpOperationService service;
  service.RegisterSession("session-a");
  const McpSubscriptionSnapshot subscription = service.CreateSubscription(
      "session-a",
      McpSubscriptionRequest{.families = {McpInformationFamily::kMetrics},
                             .node_ids = {},
                             .cursor = 0U});
  std::optional<McpSubscriptionSnapshot> polled;
  std::jthread poller([&] {
    polled = service.PollSubscription("session-a", subscription.subscription_id,
                                      0U, 8U, 1s);
  });
  std::this_thread::sleep_for(20ms);
  service.Publish(Evidence(McpInformationFamily::kMetrics, "node-1", "sample"));
  poller.join();
  BOOST_REQUIRE(static_cast<bool>(polled));
  BOOST_REQUIRE_EQUAL(polled->items.size(), 1U);

  std::optional<McpSubscriptionSnapshot> cancelled_poll;
  std::jthread cancelled_poller([&] {
    cancelled_poll = service.PollSubscription(
        "session-a", subscription.subscription_id, 1U, 8U, 1s);
  });
  std::this_thread::sleep_for(20ms);
  service.CancelSubscription("session-a", subscription.subscription_id);
  cancelled_poller.join();
  BOOST_REQUIRE(static_cast<bool>(cancelled_poll));
  BOOST_TEST(!cancelled_poll->active);
  BOOST_TEST(cancelled_poll->items.empty());
}

BOOST_AUTO_TEST_CASE(mcp_subscription_concurrent_publish_poll_cancel_is_safe) {
  McpOperationService service(TestConfig(1U, 1U, 4U, 8U, 2U, 8U));
  service.RegisterSession("session-a");
  const McpSubscriptionSnapshot subscription = service.CreateSubscription(
      "session-a",
      McpSubscriptionRequest{.families = {McpInformationFamily::kMetrics},
                             .node_ids = {},
                             .cursor = 0U});
  std::atomic<bool> start = false;
  std::vector<std::jthread> publishers;
  for (std::size_t worker = 0U; worker < 4U; ++worker) {
    publishers.emplace_back([&, worker] {
      while (!start.load(std::memory_order_acquire)) {
        std::this_thread::yield();
      }
      for (std::size_t index = 0U; index < 100U; ++index) {
        service.Publish(Evidence(
            McpInformationFamily::kMetrics, "node-1",
            "sample-" + std::to_string(worker) + "-" + std::to_string(index)));
      }
    });
  }
  std::jthread poller([&] {
    while (!start.load(std::memory_order_acquire)) {
      std::this_thread::yield();
    }
    std::uint64_t cursor = 0U;
    for (std::size_t index = 0U; index < 50U; ++index) {
      const McpSubscriptionSnapshot page = service.PollSubscription(
          "session-a", subscription.subscription_id, cursor, 8U, 5ms);
      cursor = page.next_cursor;
    }
  });
  start.store(true, std::memory_order_release);
  for (std::jthread& publisher : publishers) {
    publisher.join();
  }
  poller.join();
  const McpSubscriptionSnapshot cancelled =
      service.CancelSubscription("session-a", subscription.subscription_id);
  BOOST_TEST(!cancelled.active);
  BOOST_TEST(cancelled.items.size() <= 8U);
  const McpOperationServiceStats stats = service.Stats();
  BOOST_TEST(stats.notifications_published == 400U);
  BOOST_TEST(stats.notifications_dropped == 392U);
}

BOOST_AUTO_TEST_CASE(mcp_operation_registry_result_mapping_remains_exhaustive) {
  for (std::size_t index = 0U;
       index < static_cast<std::size_t>(McpOperationKind::kCount); ++index) {
    const McpOperationKind kind = static_cast<McpOperationKind>(index);
    BOOST_TEST(McpOperationKindName(kind).empty() == false);
    BOOST_TEST(McpResultFamilyName(McpOperationResultFamily(kind)).empty() ==
               false);
  }
  BOOST_CHECK_THROW(McpOperationStateName(static_cast<McpOperationState>(1000)),
                    std::logic_error);
}

}  // namespace bbp
