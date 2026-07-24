#pragma once

#include <boost/json/array.hpp>
#include <boost/json/object.hpp>
#include <boost/json/value.hpp>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <memory>
#include <optional>
#include <stdexcept>
#include <stop_token>
#include <string>
#include <string_view>
#include <vector>

#include "bbp/mcp_registry.h"

namespace bbp {

enum class McpOperationState {
  kQueued,
  kRunning,
  kCancelling,
  kCancelled,
  kSucceeded,
  kFailed,
};

std::string_view McpOperationStateName(McpOperationState state);
bool IsTerminalMcpOperationState(McpOperationState state);
void ValidateMcpIdentifier(std::string_view value, std::string_view label);

struct McpOperationProgress {
  std::uint64_t completed = 0U;
  std::uint64_t total = 0U;
};

struct McpTypedResult {
  McpResultFamily family = McpResultFamily::kError;
  boost::json::object value;
};

struct McpOperationError {
  std::string code;
  std::string message;
  bool retryable = false;
  boost::json::array diagnostics;
};

struct McpOperationSnapshot {
  std::string operation_id;
  std::string session_id;
  McpOperationKind kind = McpOperationKind::kCount;
  McpOperationState state = McpOperationState::kQueued;
  McpOperationProgress progress;
  bool cancel_requested = false;
  McpResultFamily terminal_result_family = McpResultFamily::kError;
  std::optional<McpTypedResult> result;
  std::optional<McpOperationError> error;
  std::uint64_t sequence = 0U;
};

struct McpOperationCancellation {
  McpOperationSnapshot operation;
  bool request_accepted = false;
  bool already_terminal = false;
};

class McpOperationCancelled : public std::runtime_error {
 public:
  McpOperationCancelled();
  McpOperationCancelled(std::string message, boost::json::array diagnostics);

  const boost::json::array& diagnostics() const noexcept;

 private:
  boost::json::array diagnostics_;
};

class McpOperationFailure : public std::runtime_error {
 public:
  McpOperationFailure(std::string code, std::string message, bool retryable,
                      boost::json::array diagnostics = {});

  const std::string& code() const noexcept;
  bool retryable() const noexcept;
  const boost::json::array& diagnostics() const noexcept;

 private:
  std::string code_;
  bool retryable_ = false;
  boost::json::array diagnostics_;
};

class McpOperationContext {
 public:
  std::stop_token stop_token() const;
  bool stop_requested() const;
  void ThrowIfCancelled() const;
  void ReportProgress(std::uint64_t completed) const;

 private:
  friend class McpOperationService;
  McpOperationContext(std::stop_token stop_token,
                      std::function<void(std::uint64_t)> progress_handler);

  std::stop_token stop_token_;
  std::function<void(std::uint64_t)> progress_handler_;
};

using McpOperationExecutor =
    std::function<McpTypedResult(McpOperationContext&)>;
using McpSessionRequestHandler =
    std::function<boost::json::value(std::stop_token)>;

struct McpEvidenceRecord {
  std::string run_id;
  McpInformationFamily family = McpInformationFamily::kCount;
  std::uint64_t sequence = 0U;
  std::uint64_t timestamp_ms = 0U;
  std::optional<std::string> node_id;
  std::optional<std::string> kind;
  std::optional<std::string> message;
  std::optional<std::string> artifact_id;
  std::optional<boost::json::value> data;
};

struct McpSubscriptionRequest {
  std::string run_id;
  std::vector<McpInformationFamily> families;
  std::vector<std::string> node_ids;
  std::uint64_t cursor = 0U;
};

struct McpSubscriptionSnapshot {
  std::string subscription_id;
  std::string session_id;
  std::string run_id;
  std::vector<McpEvidenceRecord> items;
  std::uint64_t next_cursor = 0U;
  std::uint64_t dropped = 0U;
  bool active = false;
};

struct McpServiceNotification {
  std::string session_id;
  std::string method;
  boost::json::value params;
};

using McpServiceNotificationHandler =
    std::function<void(const McpServiceNotification&)>;

struct McpOperationServiceConfig {
  std::size_t maximum_sessions = kMcpMaximumSessions;
  std::size_t maximum_tasks_per_session = kMcpMaximumTasksPerSession;
  std::size_t maximum_subscriptions_per_session =
      kMcpMaximumSubscriptionsPerSession;
  std::size_t maximum_notifications_per_subscription =
      kMcpMaximumNotificationsPerSession;
  std::size_t maximum_retained_operations = kMcpMaximumRetainedOperations;
  std::size_t maximum_poll_items = kMcpListPageSize;
};

struct McpOperationServiceStats {
  bool accepting = false;
  std::size_t sessions = 0U;
  std::size_t active_operations = 0U;
  std::size_t retained_operations = 0U;
  std::size_t active_workers = 0U;
  std::size_t active_subscriptions = 0U;
  std::size_t retained_subscriptions = 0U;
  std::size_t maximum_active_operations = 0U;
  std::uint64_t submitted_operations = 0U;
  std::uint64_t succeeded_operations = 0U;
  std::uint64_t failed_operations = 0U;
  std::uint64_t cancelled_operations = 0U;
  std::uint64_t evicted_operations = 0U;
  std::uint64_t notifications_published = 0U;
  std::uint64_t notifications_dropped = 0U;
  std::uint64_t notification_handler_failures = 0U;
};

class McpOperationService {
 public:
  explicit McpOperationService(
      McpOperationServiceConfig config = {},
      McpServiceNotificationHandler notification_handler = {});
  ~McpOperationService();

  McpOperationService(const McpOperationService&) = delete;
  McpOperationService& operator=(const McpOperationService&) = delete;

  void RegisterSession(std::string session_id);
  bool RemoveSession(std::string_view session_id,
                     std::chrono::milliseconds timeout);
  boost::json::value ExecuteSessionRequest(std::string_view session_id,
                                           McpSessionRequestHandler handler,
                                           std::stop_token stop_token = {});

  McpOperationSnapshot Submit(std::string_view session_id,
                              McpOperationKind kind,
                              std::uint64_t progress_total,
                              McpOperationExecutor executor);
  McpOperationSnapshot GetOperation(std::string_view session_id,
                                    std::string_view operation_id) const;
  McpOperationCancellation CancelOperation(std::string_view session_id,
                                           std::string_view operation_id);
  std::optional<McpOperationSnapshot> WaitForOperation(
      std::string_view session_id, std::string_view operation_id,
      std::chrono::milliseconds timeout, std::stop_token stop_token = {}) const;

  McpSubscriptionSnapshot CreateSubscription(std::string_view session_id,
                                             McpSubscriptionRequest request);
  McpSubscriptionSnapshot PollSubscription(
      std::string_view session_id, std::string_view subscription_id,
      std::uint64_t after_sequence, std::size_t limit,
      std::chrono::milliseconds timeout = std::chrono::milliseconds::zero(),
      std::stop_token stop_token = {}) const;
  McpSubscriptionSnapshot CancelSubscription(std::string_view session_id,
                                             std::string_view subscription_id);
  void CloseRunSubscriptions(std::string_view run_id);
  void Publish(McpEvidenceRecord record);

  McpOperationServiceStats Stats() const;
  void Shutdown();

 private:
  struct Impl;
  std::unique_ptr<Impl> impl_;
};

boost::json::object McpOperationSnapshotJson(
    const McpOperationSnapshot& snapshot);
boost::json::object McpOperationErrorJson(const McpOperationError& error,
                                          std::string_view operation_id = {});
boost::json::object McpEvidenceRecordJson(const McpEvidenceRecord& record);
boost::json::object McpSubscriptionSnapshotJson(
    const McpSubscriptionSnapshot& snapshot);

}  // namespace bbp
