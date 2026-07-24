#include <algorithm>
#include <atomic>
#include <boost/json/array.hpp>
#include <boost/json/object.hpp>
#include <boost/test/unit_test.hpp>
#include <chrono>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <memory>
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

McpOperationExecutor StatsReentrantStopExecutor(
    McpOperationService* service, std::atomic<bool>* callback_ready,
    std::atomic<bool>* callback_completed) {
  return [service, callback_ready,
          callback_completed](McpOperationContext& context) {
    std::stop_callback on_stop(context.stop_token(),
                               [service, callback_completed] {
                                 static_cast<void>(service->Stats());
                                 callback_completed->store(
                                     true, std::memory_order_release);
                               });
    callback_ready->store(true, std::memory_order_release);
    while (!context.stop_requested()) {
      std::this_thread::yield();
    }
    context.ThrowIfCancelled();
    return ValidationResult();
  };
}

McpEvidenceRecord Evidence(McpInformationFamily family, std::string node_id,
                           std::string message,
                           std::string run_id = "run-a") {
  return McpEvidenceRecord{.run_id = std::move(run_id),
                           .family = family,
                           .sequence = 0U,
                           .timestamp_ms = 100U,
                           .node_id = std::move(node_id),
                           .kind = "sample",
                           .message = std::move(message),
                           .artifact_id = std::nullopt,
                           .data = std::nullopt};
}

class SetAtomicBoolOnExit {
 public:
  explicit SetAtomicBoolOnExit(std::atomic<bool>* value) : value_(value) {}
  SetAtomicBoolOnExit(const SetAtomicBoolOnExit&) = delete;
  SetAtomicBoolOnExit& operator=(const SetAtomicBoolOnExit&) = delete;
  ~SetAtomicBoolOnExit() { value_->store(true, std::memory_order_release); }

 private:
  std::atomic<bool>* value_;
};

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
    mcp_operation_contains_non_standard_executor_and_callback_exceptions) {
  std::atomic<std::size_t> callback_invocations = 0U;
  McpOperationService service(TestConfig(), [&](const McpServiceNotification&) {
    callback_invocations.fetch_add(1U, std::memory_order_relaxed);
    throw 7;
  });
  service.RegisterSession("session-a");
  const McpOperationSnapshot submitted =
      service.Submit("session-a", McpOperationKind::kValidateScenario, 1U,
                     [](McpOperationContext&) -> McpTypedResult { throw 11; });

  const McpOperationSnapshot terminal =
      WaitForTerminal(&service, "session-a", submitted.operation_id);
  BOOST_CHECK(terminal.state == McpOperationState::kFailed);
  BOOST_REQUIRE(static_cast<bool>(terminal.error));
  BOOST_TEST(terminal.error->code == "operation_failed");
  BOOST_TEST(terminal.error->message ==
             "operation failed with a non-standard exception");

  const auto notification_deadline = std::chrono::steady_clock::now() + 2s;
  while (service.Stats().notification_handler_failures != 2U &&
         std::chrono::steady_clock::now() < notification_deadline) {
    std::this_thread::yield();
  }
  BOOST_REQUIRE(service.Stats().notification_handler_failures == 2U);
  service.Shutdown();
  const McpOperationServiceStats stats = service.Stats();
  BOOST_TEST(callback_invocations.load(std::memory_order_relaxed) == 3U);
  BOOST_TEST(stats.notification_handler_failures == 3U);
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

  McpSubscriptionRequest request{.run_id = "run-a",
                                 .families = {McpInformationFamily::kMetrics},
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
      McpSubscriptionRequest{.run_id = "run-a",
                             .families = {McpInformationFamily::kMetrics},
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

BOOST_AUTO_TEST_CASE(
    mcp_operation_terminal_callback_removes_session_after_waiters_drain) {
  McpOperationService* service_pointer = nullptr;
  std::atomic<bool> finish = false;
  std::atomic<bool> removal_started = false;
  std::atomic<bool> removal_completed = false;
  std::atomic<bool> removal_succeeded = false;
  std::atomic<std::size_t> waiter_failures = 0U;
  McpOperationService service(
      TestConfig(), [&](const McpServiceNotification& notification) {
        if (notification.method != kMcpOperationUpdatedNotification ||
            notification.params.as_object().at("state").as_string() !=
                "succeeded" ||
            removal_started.exchange(true, std::memory_order_acq_rel)) {
          return;
        }
        try {
          removal_succeeded.store(
              service_pointer->RemoveSession("session-a", 1s),
              std::memory_order_release);
        } catch (...) {
          waiter_failures.fetch_add(1U, std::memory_order_relaxed);
        }
        removal_completed.store(true, std::memory_order_release);
      });
  service_pointer = &service;
  service.RegisterSession("session-a");
  const McpSubscriptionSnapshot subscription = service.CreateSubscription(
      "session-a",
      McpSubscriptionRequest{.run_id = "run-a",
                             .families = {McpInformationFamily::kMetrics},
                             .node_ids = {},
                             .cursor = 0U});
  const McpOperationSnapshot submitted =
      service.Submit("session-a", McpOperationKind::kValidateScenario, 1U,
                     [&](McpOperationContext&) {
                       while (!finish.load(std::memory_order_acquire)) {
                         std::this_thread::yield();
                       }
                       return ValidationResult();
                     });
  WaitUntilState(&service, "session-a", submitted.operation_id,
                 McpOperationState::kRunning);

  constexpr std::size_t kOperationWaiters = 4U;
  std::atomic<std::size_t> waiters_started = 0U;
  std::vector<std::optional<McpOperationSnapshot>> waited(kOperationWaiters);
  std::vector<std::jthread> waiters;
  for (std::size_t index = 0U; index < kOperationWaiters; ++index) {
    waiters.emplace_back([&, index] {
      waiters_started.fetch_add(1U, std::memory_order_release);
      try {
        waited[index] =
            service.WaitForOperation("session-a", submitted.operation_id, 1s);
      } catch (...) {
        waiter_failures.fetch_add(1U, std::memory_order_relaxed);
      }
    });
  }
  std::optional<McpSubscriptionSnapshot> polled;
  std::jthread poller([&] {
    waiters_started.fetch_add(1U, std::memory_order_release);
    try {
      polled = service.PollSubscription(
          "session-a", subscription.subscription_id, 0U, 1U, 1s);
    } catch (...) {
      waiter_failures.fetch_add(1U, std::memory_order_relaxed);
    }
  });

  const auto waiter_deadline = std::chrono::steady_clock::now() + 2s;
  while (waiters_started.load(std::memory_order_acquire) !=
             kOperationWaiters + 1U &&
         std::chrono::steady_clock::now() < waiter_deadline) {
    std::this_thread::yield();
  }
  std::this_thread::sleep_for(20ms);
  finish.store(true, std::memory_order_release);
  for (std::jthread& waiter : waiters) {
    waiter.join();
  }
  poller.join();

  const auto removal_deadline = std::chrono::steady_clock::now() + 2s;
  while (!removal_completed.load(std::memory_order_acquire) &&
         std::chrono::steady_clock::now() < removal_deadline) {
    std::this_thread::yield();
  }
  service.Shutdown();

  BOOST_TEST(waiters_started.load(std::memory_order_acquire) ==
             kOperationWaiters + 1U);
  BOOST_TEST(removal_completed.load(std::memory_order_acquire));
  BOOST_TEST(removal_succeeded.load(std::memory_order_acquire));
  BOOST_TEST(waiter_failures.load(std::memory_order_relaxed) == 0U);
  for (const auto& result : waited) {
    BOOST_REQUIRE(static_cast<bool>(result));
    BOOST_CHECK(result->state == McpOperationState::kSucceeded);
  }
  BOOST_REQUIRE(static_cast<bool>(polled));
  BOOST_TEST(!polled->active);
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

BOOST_AUTO_TEST_CASE(mcp_operation_terminal_callback_can_shutdown_reentrantly) {
  McpOperationService* service_pointer = nullptr;
  std::atomic<bool> shutdown_started = false;
  std::atomic<bool> shutdown_returned = false;
  McpOperationService service(
      TestConfig(), [&](const McpServiceNotification& notification) {
        if (notification.method != kMcpOperationUpdatedNotification ||
            notification.params.as_object().at("state").as_string() !=
                "succeeded" ||
            shutdown_started.exchange(true, std::memory_order_acq_rel)) {
          return;
        }
        service_pointer->Shutdown();
        shutdown_returned.store(true, std::memory_order_release);
      });
  service_pointer = &service;
  service.RegisterSession("session-a");
  static_cast<void>(
      service.Submit("session-a", McpOperationKind::kValidateScenario, 1U,
                     [](McpOperationContext&) { return ValidationResult(); }));

  const auto deadline = std::chrono::steady_clock::now() + 2s;
  while (!shutdown_returned.load(std::memory_order_acquire) &&
         std::chrono::steady_clock::now() < deadline) {
    std::this_thread::yield();
  }
  service.Shutdown();

  BOOST_TEST(shutdown_started.load(std::memory_order_acquire));
  BOOST_TEST(shutdown_returned.load(std::memory_order_acquire));
  const McpOperationServiceStats stats = service.Stats();
  BOOST_TEST(!stats.accepting);
  BOOST_TEST(stats.sessions == 0U);
  BOOST_TEST(stats.active_workers == 0U);
}

BOOST_AUTO_TEST_CASE(
    mcp_session_request_shutdown_defers_completion_until_lease_exit) {
  std::atomic<bool> stop_callback_completed = false;
  std::chrono::steady_clock::duration shutdown_duration{};
  std::size_t sessions_before_lease_exit = 0U;
  McpOperationService service;
  service.RegisterSession("session-a");

  static_cast<void>(service.ExecuteSessionRequest(
      "session-a", [&](std::stop_token stop_token) {
        std::stop_callback on_stop(stop_token, [&] {
          static_cast<void>(service.Stats());
          stop_callback_completed.store(true, std::memory_order_release);
        });
        const auto began = std::chrono::steady_clock::now();
        service.Shutdown();
        shutdown_duration = std::chrono::steady_clock::now() - began;
        sessions_before_lease_exit = service.Stats().sessions;
        return boost::json::object{};
      }));

  BOOST_CHECK(shutdown_duration < 500ms);
  BOOST_TEST(stop_callback_completed.load(std::memory_order_acquire));
  BOOST_TEST(sessions_before_lease_exit == 1U);
  const McpOperationServiceStats stats = service.Stats();
  BOOST_TEST(!stats.accepting);
  BOOST_TEST(stats.sessions == 0U);
  BOOST_TEST(stats.active_operations == 0U);
  BOOST_TEST(stats.active_workers == 0U);
}

BOOST_AUTO_TEST_CASE(mcp_operation_concurrent_shutdown_waits_for_worker_owner) {
  std::atomic<bool> stop_seen = false;
  std::atomic<bool> release_worker = false;
  std::atomic<bool> first_returned = false;
  std::atomic<bool> second_entered = false;
  std::atomic<bool> second_returned = false;
  McpOperationService service;
  service.RegisterSession("session-a");
  const McpOperationSnapshot submitted =
      service.Submit("session-a", McpOperationKind::kValidateScenario, 1U,
                     [&](McpOperationContext& context) {
                       while (!context.stop_requested()) {
                         std::this_thread::yield();
                       }
                       stop_seen.store(true, std::memory_order_release);
                       while (!release_worker.load(std::memory_order_acquire)) {
                         std::this_thread::yield();
                       }
                       context.ThrowIfCancelled();
                       return ValidationResult();
                     });
  WaitUntilState(&service, "session-a", submitted.operation_id,
                 McpOperationState::kRunning);

  std::jthread first_shutdown([&] {
    service.Shutdown();
    first_returned.store(true, std::memory_order_release);
  });
  const auto stop_deadline = std::chrono::steady_clock::now() + 2s;
  while (!stop_seen.load(std::memory_order_acquire) &&
         std::chrono::steady_clock::now() < stop_deadline) {
    std::this_thread::yield();
  }
  std::jthread second_shutdown([&] {
    second_entered.store(true, std::memory_order_release);
    service.Shutdown();
    second_returned.store(true, std::memory_order_release);
  });
  const auto second_deadline = std::chrono::steady_clock::now() + 2s;
  while (!second_entered.load(std::memory_order_acquire) &&
         std::chrono::steady_clock::now() < second_deadline) {
    std::this_thread::yield();
  }
  std::this_thread::sleep_for(20ms);
  const bool second_returned_while_worker_active =
      second_returned.load(std::memory_order_acquire);
  const std::size_t sessions_while_worker_active = service.Stats().sessions;

  release_worker.store(true, std::memory_order_release);
  first_shutdown.join();
  second_shutdown.join();

  BOOST_TEST(stop_seen.load(std::memory_order_acquire));
  BOOST_TEST(!second_returned_while_worker_active);
  BOOST_TEST(sessions_while_worker_active == 1U);
  BOOST_TEST(first_returned.load(std::memory_order_acquire));
  BOOST_TEST(second_returned.load(std::memory_order_acquire));
  const McpOperationServiceStats stats = service.Stats();
  BOOST_TEST(stats.sessions == 0U);
  BOOST_TEST(stats.active_operations == 0U);
  BOOST_TEST(stats.active_workers == 0U);
}

BOOST_AUTO_TEST_CASE(
    mcp_session_removal_cancels_and_waits_for_synchronous_requests) {
  std::atomic<bool> request_started = false;
  std::atomic<bool> cancellation_seen = false;
  std::atomic<bool> stop_callback_completed = false;
  std::atomic<bool> request_finished = false;
  McpOperationService service;
  service.RegisterSession("session-a");

  std::jthread request([&] {
    static_cast<void>(service.ExecuteSessionRequest(
        "session-a", [&](std::stop_token stop_token) {
          std::stop_callback on_stop(stop_token, [&] {
            static_cast<void>(service.Stats());
            stop_callback_completed.store(true, std::memory_order_release);
          });
          request_started.store(true, std::memory_order_release);
          const auto deadline = std::chrono::steady_clock::now() + 2s;
          while (!stop_token.stop_requested() &&
                 std::chrono::steady_clock::now() < deadline) {
            std::this_thread::yield();
          }
          cancellation_seen.store(stop_token.stop_requested(),
                                  std::memory_order_release);
          request_finished.store(true, std::memory_order_release);
          return boost::json::object{};
        }));
  });
  const auto start_deadline = std::chrono::steady_clock::now() + 2s;
  while (!request_started.load(std::memory_order_acquire) &&
         std::chrono::steady_clock::now() < start_deadline) {
    std::this_thread::yield();
  }

  BOOST_REQUIRE(request_started.load(std::memory_order_acquire));
  BOOST_TEST(service.RemoveSession("session-a", 1s));
  BOOST_TEST(cancellation_seen.load(std::memory_order_acquire));
  BOOST_TEST(stop_callback_completed.load(std::memory_order_acquire));
  BOOST_TEST(request_finished.load(std::memory_order_acquire));
  request.join();
  BOOST_TEST(service.Stats().sessions == 0U);
}

BOOST_AUTO_TEST_CASE(
    mcp_worker_stop_callbacks_reenter_stats_during_removal_and_shutdown) {
  std::atomic<bool> removal_callback_ready = false;
  std::atomic<bool> removal_callback_completed = false;
  std::atomic<bool> shutdown_callback_ready = false;
  std::atomic<bool> shutdown_callback_completed = false;
  McpOperationService service;
  service.RegisterSession("session-remove");
  static_cast<void>(service.Submit(
      "session-remove", McpOperationKind::kValidateScenario, 1U,
      StatsReentrantStopExecutor(&service, &removal_callback_ready,
                                 &removal_callback_completed)));
  const auto removal_ready_deadline = std::chrono::steady_clock::now() + 2s;
  while (!removal_callback_ready.load(std::memory_order_acquire) &&
         std::chrono::steady_clock::now() < removal_ready_deadline) {
    std::this_thread::yield();
  }
  BOOST_REQUIRE(removal_callback_ready.load(std::memory_order_acquire));

  const auto removal_began = std::chrono::steady_clock::now();
  BOOST_TEST(service.RemoveSession("session-remove", 1s));
  BOOST_CHECK(std::chrono::steady_clock::now() - removal_began < 500ms);
  BOOST_TEST(removal_callback_completed.load(std::memory_order_acquire));

  service.RegisterSession("session-shutdown");
  static_cast<void>(service.Submit(
      "session-shutdown", McpOperationKind::kValidateScenario, 1U,
      StatsReentrantStopExecutor(&service, &shutdown_callback_ready,
                                 &shutdown_callback_completed)));
  const auto shutdown_ready_deadline = std::chrono::steady_clock::now() + 2s;
  while (!shutdown_callback_ready.load(std::memory_order_acquire) &&
         std::chrono::steady_clock::now() < shutdown_ready_deadline) {
    std::this_thread::yield();
  }
  BOOST_REQUIRE(shutdown_callback_ready.load(std::memory_order_acquire));

  const auto shutdown_began = std::chrono::steady_clock::now();
  service.Shutdown();
  BOOST_CHECK(std::chrono::steady_clock::now() - shutdown_began < 500ms);
  BOOST_TEST(shutdown_callback_completed.load(std::memory_order_acquire));
  const McpOperationServiceStats stats = service.Stats();
  BOOST_TEST(stats.sessions == 0U);
  BOOST_TEST(stats.active_operations == 0U);
  BOOST_TEST(stats.active_workers == 0U);
}

BOOST_AUTO_TEST_CASE(mcp_cancel_stop_callback_can_reenter_stats) {
  std::atomic<bool> callback_ready = false;
  std::atomic<bool> callback_completed = false;
  McpOperationService service;
  service.RegisterSession("session-a");
  const McpOperationSnapshot submitted = service.Submit(
      "session-a", McpOperationKind::kValidateScenario, 1U,
      StatsReentrantStopExecutor(&service, &callback_ready,
                                 &callback_completed));
  const auto ready_deadline = std::chrono::steady_clock::now() + 2s;
  while (!callback_ready.load(std::memory_order_acquire) &&
         std::chrono::steady_clock::now() < ready_deadline) {
    std::this_thread::yield();
  }
  BOOST_REQUIRE(callback_ready.load(std::memory_order_acquire));

  const auto began = std::chrono::steady_clock::now();
  const McpOperationCancellation cancellation =
      service.CancelOperation("session-a", submitted.operation_id);
  BOOST_CHECK(std::chrono::steady_clock::now() - began < 500ms);
  BOOST_TEST(cancellation.request_accepted);
  BOOST_TEST(callback_completed.load(std::memory_order_acquire));
  BOOST_CHECK(
      WaitForTerminal(&service, "session-a", submitted.operation_id).state ==
      McpOperationState::kCancelled);
  service.Shutdown();
  const McpOperationServiceStats stats = service.Stats();
  BOOST_TEST(stats.sessions == 0U);
  BOOST_TEST(stats.active_operations == 0U);
  BOOST_TEST(stats.active_workers == 0U);
}

BOOST_AUTO_TEST_CASE(
    mcp_cancel_notifications_never_regress_after_terminal_delivery) {
  using BlockingStopCallback = std::stop_callback<std::function<void()>>;
  std::atomic<bool> executor_ready = false;
  std::atomic<bool> terminal_delivered = false;
  std::shared_ptr<BlockingStopCallback> blocking_stop_callback;
  std::mutex delivered_mutex;
  std::vector<std::uint64_t> delivered_sequences;
  std::vector<std::string> delivered_states;
  McpOperationService service(
      TestConfig(), [&](const McpServiceNotification& notification) {
        if (notification.method != kMcpOperationUpdatedNotification) {
          return;
        }
        const boost::json::object& params = notification.params.as_object();
        {
          std::lock_guard<std::mutex> lock(delivered_mutex);
          delivered_sequences.push_back(params.at("sequence").as_uint64());
          delivered_states.emplace_back(params.at("state").as_string());
        }
        if (params.at("state").as_string() == "cancelled") {
          terminal_delivered.store(true, std::memory_order_release);
        }
      });
  service.RegisterSession("session-a");
  const McpOperationSnapshot submitted = service.Submit(
      "session-a", McpOperationKind::kValidateScenario, 1U,
      [&](McpOperationContext& context) {
        blocking_stop_callback = std::make_shared<BlockingStopCallback>(
            context.stop_token(), std::function<void()>([&] {
              while (!terminal_delivered.load(std::memory_order_acquire)) {
                std::this_thread::yield();
              }
            }));
        executor_ready.store(true, std::memory_order_release);
        while (!context.stop_requested()) {
          std::this_thread::yield();
        }
        context.ThrowIfCancelled();
        return ValidationResult();
      });
  const auto ready_deadline = std::chrono::steady_clock::now() + 2s;
  while (!executor_ready.load(std::memory_order_acquire) &&
         std::chrono::steady_clock::now() < ready_deadline) {
    std::this_thread::yield();
  }
  BOOST_REQUIRE(executor_ready.load(std::memory_order_acquire));

  const McpOperationCancellation cancellation =
      service.CancelOperation("session-a", submitted.operation_id);
  blocking_stop_callback.reset();
  BOOST_TEST(cancellation.request_accepted);
  BOOST_CHECK(
      WaitForTerminal(&service, "session-a", submitted.operation_id).state ==
      McpOperationState::kCancelled);
  service.Shutdown();

  std::lock_guard<std::mutex> lock(delivered_mutex);
  BOOST_REQUIRE(!delivered_sequences.empty());
  BOOST_REQUIRE(delivered_sequences.size() == delivered_states.size());
  for (std::size_t index = 1U; index < delivered_sequences.size(); ++index) {
    BOOST_TEST(delivered_sequences[index - 1U] < delivered_sequences[index]);
  }
  BOOST_TEST(delivered_states.back() == "cancelled");
}

BOOST_AUTO_TEST_CASE(
    mcp_terminal_notification_is_delivered_before_capacity_one_eviction) {
  std::atomic<bool> terminal_delivery_started = false;
  std::atomic<bool> release_terminal_delivery = false;
  std::atomic<bool> terminal_handler_returning = false;
  McpOperationService service(
      TestConfig(1U, 1U, 1U, 1U, 1U, 1U),
      [&](const McpServiceNotification& notification) {
        const boost::json::object& params = notification.params.as_object();
        if (notification.method != kMcpOperationUpdatedNotification ||
            params.at("state").as_string() != "succeeded") {
          return;
        }
        terminal_delivery_started.store(true, std::memory_order_release);
        while (!release_terminal_delivery.load(std::memory_order_acquire)) {
          std::this_thread::yield();
        }
        terminal_handler_returning.store(true, std::memory_order_release);
      });
  SetAtomicBoolOnExit release_terminal_delivery_on_exit{
      &release_terminal_delivery};
  service.RegisterSession("session-a");
  const McpOperationSnapshot first = service.Submit(
      "session-a", McpOperationKind::kValidateScenario, 1U,
      [](McpOperationContext&) { return ValidationResult(); });
  BOOST_CHECK(WaitForTerminal(&service, "session-a", first.operation_id).state ==
              McpOperationState::kSucceeded);
  BOOST_REQUIRE(terminal_delivery_started.load(std::memory_order_acquire));
  BOOST_CHECK_THROW(
      service.Submit("session-a", McpOperationKind::kValidateScenario, 1U,
                     [](McpOperationContext&) { return ValidationResult(); }),
      std::runtime_error);

  release_terminal_delivery.store(true, std::memory_order_release);
  const auto deadline = std::chrono::steady_clock::now() + 2s;
  while (!terminal_handler_returning.load(std::memory_order_acquire) &&
         std::chrono::steady_clock::now() < deadline) {
    std::this_thread::yield();
  }
  BOOST_REQUIRE(terminal_handler_returning.load(std::memory_order_acquire));
  std::optional<McpOperationSnapshot> replacement;
  const auto eviction_deadline = std::chrono::steady_clock::now() + 2s;
  while (!replacement &&
         std::chrono::steady_clock::now() < eviction_deadline) {
    try {
      replacement = service.Submit(
          "session-a", McpOperationKind::kValidateScenario, 1U,
          [](McpOperationContext&) { return ValidationResult(); });
    } catch (const std::runtime_error&) {
      std::this_thread::yield();
    }
  }
  BOOST_REQUIRE(static_cast<bool>(replacement));
  BOOST_CHECK(WaitForTerminal(&service, "session-a", replacement->operation_id)
                  .state == McpOperationState::kSucceeded);
}

BOOST_AUTO_TEST_CASE(
    mcp_failed_terminal_notification_keeps_reserved_slot_at_queue_bound) {
  std::atomic<bool> first_terminal_delivery_started = false;
  std::atomic<bool> release_first_terminal_delivery = false;
  std::atomic<std::size_t> subscription_deliveries = 0U;
  std::mutex terminal_sequences_mutex;
  std::vector<std::uint64_t> terminal_sequences;
  McpOperationService service(
      TestConfig(1U, 2U, 1U, 1U, 2U, 1U),
      [&](const McpServiceNotification& notification) {
        if (notification.method == kMcpSubscriptionUpdatedNotification) {
          subscription_deliveries.fetch_add(1U, std::memory_order_relaxed);
          return;
        }
        if (notification.method != kMcpOperationUpdatedNotification) {
          return;
        }
        const boost::json::object& params = notification.params.as_object();
        if (params.at("state").as_string() != "succeeded") {
          return;
        }
        bool first_attempt = false;
        {
          std::lock_guard<std::mutex> lock(terminal_sequences_mutex);
          first_attempt = terminal_sequences.empty();
          terminal_sequences.push_back(params.at("sequence").as_uint64());
        }
        if (first_attempt) {
          first_terminal_delivery_started.store(true,
                                                std::memory_order_release);
          while (!release_first_terminal_delivery.load(
              std::memory_order_acquire)) {
            std::this_thread::yield();
          }
          throw std::runtime_error("terminal notification delivery failed");
        }
      });
  SetAtomicBoolOnExit release_terminal_delivery_on_exit{
      &release_first_terminal_delivery};
  service.RegisterSession("session-a");
  service.CreateSubscription(
      "session-a",
      McpSubscriptionRequest{.run_id = "run-a",
                             .families = {McpInformationFamily::kMetrics},
                             .node_ids = {},
                             .cursor = 0U});

  const McpOperationSnapshot first = service.Submit(
      "session-a", McpOperationKind::kValidateScenario, 1U,
      [](McpOperationContext&) { return ValidationResult(); });
  const auto delivery_deadline = std::chrono::steady_clock::now() + 2s;
  while (!first_terminal_delivery_started.load(std::memory_order_acquire) &&
         std::chrono::steady_clock::now() < delivery_deadline) {
    std::this_thread::yield();
  }
  if (!first_terminal_delivery_started.load(std::memory_order_acquire)) {
    release_first_terminal_delivery.store(true, std::memory_order_release);
  }
  BOOST_REQUIRE(
      first_terminal_delivery_started.load(std::memory_order_acquire));
  BOOST_CHECK(service.GetOperation("session-a", first.operation_id).state ==
              McpOperationState::kSucceeded);

  const McpOperationSnapshot second = service.Submit(
      "session-a", McpOperationKind::kValidateScenario, 1U,
      [](McpOperationContext&) { return ValidationResult(); });
  BOOST_CHECK(WaitForTerminal(&service, "session-a", second.operation_id).state ==
              McpOperationState::kSucceeded);

  // Two operation keys and one subscription key exhaust maximum_pending.
  service.Publish(Evidence(McpInformationFamily::kMetrics, "node-1", "fill"));
  BOOST_TEST(subscription_deliveries.load(std::memory_order_relaxed) == 0U);

  release_first_terminal_delivery.store(true, std::memory_order_release);
  const auto failure_deadline = std::chrono::steady_clock::now() + 2s;
  while (service.Stats().notification_handler_failures != 1U &&
         std::chrono::steady_clock::now() < failure_deadline) {
    std::this_thread::yield();
  }
  BOOST_REQUIRE(service.Stats().notification_handler_failures == 1U);

  BOOST_CHECK_THROW(
      service.Submit("session-a", McpOperationKind::kValidateScenario, 1U,
                     [](McpOperationContext&) { return ValidationResult(); }),
      std::runtime_error);
  BOOST_CHECK(service.GetOperation("session-a", first.operation_id).state ==
              McpOperationState::kSucceeded);
  BOOST_CHECK(service.GetOperation("session-a", second.operation_id).state ==
              McpOperationState::kSucceeded);
  BOOST_TEST(subscription_deliveries.load(std::memory_order_relaxed) == 0U);
  {
    std::lock_guard<std::mutex> lock(terminal_sequences_mutex);
    BOOST_REQUIRE(terminal_sequences.size() == 1U);
  }

  service.Publish(Evidence(McpInformationFamily::kMetrics, "node-1", "retry"));
  BOOST_TEST(subscription_deliveries.load(std::memory_order_relaxed) == 1U);
  BOOST_TEST(service.Stats().notification_handler_failures == 1U);
  {
    std::lock_guard<std::mutex> lock(terminal_sequences_mutex);
    BOOST_REQUIRE(terminal_sequences.size() == 3U);
    BOOST_TEST(terminal_sequences.front() == terminal_sequences.back());
    BOOST_TEST(terminal_sequences[1] != terminal_sequences.front());
  }

  const McpOperationSnapshot replacement = service.Submit(
      "session-a", McpOperationKind::kValidateScenario, 1U,
      [](McpOperationContext&) { return ValidationResult(); });
  BOOST_CHECK_THROW(service.GetOperation("session-a", first.operation_id),
                    std::runtime_error);
  BOOST_CHECK(service.GetOperation("session-a", second.operation_id).state ==
              McpOperationState::kSucceeded);
  BOOST_CHECK(
      WaitForTerminal(&service, "session-a", replacement.operation_id).state ==
      McpOperationState::kSucceeded);
}

BOOST_AUTO_TEST_CASE(
    mcp_failed_terminal_notification_retries_after_reentrant_shutdown) {
  McpOperationService* service_pointer = nullptr;
  std::atomic<bool> first_shutdown_returned = false;
  std::atomic<std::size_t> terminal_attempts = 0U;
  std::mutex terminal_sequences_mutex;
  std::vector<std::uint64_t> terminal_sequences;
  McpOperationService service(
      TestConfig(), [&](const McpServiceNotification& notification) {
        if (notification.method != kMcpOperationUpdatedNotification ||
            notification.params.as_object().at("state").as_string() !=
                "succeeded") {
          return;
        }
        {
          std::lock_guard<std::mutex> lock(terminal_sequences_mutex);
          terminal_sequences.push_back(
              notification.params.as_object().at("sequence").as_uint64());
        }
        const std::size_t attempt =
            terminal_attempts.fetch_add(1U, std::memory_order_acq_rel) + 1U;
        if (attempt == 1U) {
          service_pointer->Shutdown();
          first_shutdown_returned.store(true, std::memory_order_release);
          throw std::runtime_error(
              "terminal delivery failed after reentrant shutdown");
        }
      });
  service_pointer = &service;
  service.RegisterSession("session-a");
  static_cast<void>(service.Submit(
      "session-a", McpOperationKind::kValidateScenario, 1U,
      [](McpOperationContext&) { return ValidationResult(); }));

  const auto deadline = std::chrono::steady_clock::now() + 2s;
  while (!first_shutdown_returned.load(std::memory_order_acquire) &&
         std::chrono::steady_clock::now() < deadline) {
    std::this_thread::yield();
  }
  BOOST_REQUIRE(first_shutdown_returned.load(std::memory_order_acquire));

  service.Shutdown();
  BOOST_TEST(terminal_attempts.load(std::memory_order_acquire) == 2U);
  BOOST_TEST(service.Stats().notification_handler_failures == 1U);
  std::lock_guard<std::mutex> lock(terminal_sequences_mutex);
  BOOST_REQUIRE(terminal_sequences.size() == 2U);
  BOOST_TEST(terminal_sequences.front() == terminal_sequences.back());
}

BOOST_AUTO_TEST_CASE(mcp_operation_callbacks_never_run_under_service_lock) {
  McpOperationService* service_pointer = nullptr;
  std::atomic<std::size_t> notifications = 0U;
  std::atomic<bool> callback_failed = false;
  std::atomic<bool> terminal_delivery_started = false;
  std::atomic<bool> release_terminal_delivery = false;
  std::atomic<bool> subscription_delivered = false;
  McpOperationService service(
      TestConfig(), [&](const McpServiceNotification& notification) {
        if (service_pointer == nullptr) {
          callback_failed.store(true, std::memory_order_release);
          return;
        }
        static_cast<void>(service_pointer->Stats());
        notifications.fetch_add(1U, std::memory_order_relaxed);
        if (notification.method == kMcpOperationUpdatedNotification &&
            notification.params.as_object().at("state").as_string() ==
                "succeeded") {
          terminal_delivery_started.store(true, std::memory_order_release);
          while (!release_terminal_delivery.load(std::memory_order_acquire)) {
            std::this_thread::yield();
          }
        } else if (notification.method ==
                   kMcpSubscriptionUpdatedNotification) {
          subscription_delivered.store(true, std::memory_order_release);
        }
      });
  SetAtomicBoolOnExit release_terminal_delivery_on_exit{
      &release_terminal_delivery};
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
  const auto deadline = std::chrono::steady_clock::now() + 2s;
  while (!terminal_delivery_started.load(std::memory_order_acquire) &&
         std::chrono::steady_clock::now() < deadline) {
    std::this_thread::yield();
  }
  if (!terminal_delivery_started.load(std::memory_order_acquire)) {
    release_terminal_delivery.store(true, std::memory_order_release);
  }
  BOOST_REQUIRE(terminal_delivery_started.load(std::memory_order_acquire));
  service.CreateSubscription(
      "session-a",
      McpSubscriptionRequest{.run_id = "run-a",
                             .families = {McpInformationFamily::kMetrics},
                             .node_ids = {},
                             .cursor = 0U});
  service.Publish(Evidence(McpInformationFamily::kMetrics, "node-1", "sample"));
  BOOST_TEST(!callback_failed.load(std::memory_order_acquire));
  BOOST_TEST(notifications.load(std::memory_order_relaxed) == 3U);
  BOOST_TEST(!subscription_delivered.load(std::memory_order_acquire));

  release_terminal_delivery.store(true, std::memory_order_release);
  const auto subscription_deadline =
      std::chrono::steady_clock::now() + 2s;
  while (!subscription_delivered.load(std::memory_order_acquire) &&
         std::chrono::steady_clock::now() < subscription_deadline) {
    std::this_thread::yield();
  }
  BOOST_TEST(subscription_delivered.load(std::memory_order_acquire));
  BOOST_TEST(notifications.load(std::memory_order_relaxed) == 4U);
}

BOOST_AUTO_TEST_CASE(mcp_subscription_capacity_and_cancellation_are_bounded) {
  McpOperationService service(TestConfig(2U, 1U, 2U, 2U, 2U, 2U));
  service.RegisterSession("session-a");
  service.RegisterSession("session-b");
  const McpSubscriptionRequest request{
      .run_id = "run-a",
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
      McpSubscriptionRequest{.run_id = "run-a",
                             .families = {McpInformationFamily::kMetrics},
                             .node_ids = {"node-1"},
                             .cursor = 0U});
  service.Publish(Evidence(McpInformationFamily::kEvents, "node-1", "wrong"));
  service.Publish(
      Evidence(McpInformationFamily::kMetrics, "node-2", "wrong-node"));
  service.Publish(Evidence(McpInformationFamily::kMetrics, "node-1",
                           "wrong-run", "run-b"));
  service.Publish(Evidence(McpInformationFamily::kMetrics, "node-1", "one"));
  service.Publish(Evidence(McpInformationFamily::kMetrics, "node-1", "two"));
  service.Publish(Evidence(McpInformationFamily::kMetrics, "node-1", "three"));

  const McpSubscriptionSnapshot page = service.PollSubscription(
      "session-a", subscription.subscription_id, 0U, 2U);
  BOOST_REQUIRE_EQUAL(page.items.size(), 2U);
  BOOST_TEST(page.run_id == "run-a");
  BOOST_TEST(page.items[0].sequence == 2U);
  BOOST_TEST(page.items[0].run_id == "run-a");
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
  BOOST_TEST(json.at("run_id").as_string() == "run-a");
  BOOST_TEST(json.at("next_cursor").as_string() == "3");
}

BOOST_AUTO_TEST_CASE(mcp_subscription_poll_wakes_on_publish_and_cancel) {
  McpOperationService service;
  service.RegisterSession("session-a");
  const McpSubscriptionSnapshot subscription = service.CreateSubscription(
      "session-a",
      McpSubscriptionRequest{.run_id = "run-a",
                             .families = {McpInformationFamily::kMetrics},
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
      McpSubscriptionRequest{.run_id = "run-a",
                             .families = {McpInformationFamily::kMetrics},
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
