#include "bbp/mcp_operation_service.h"

#include <sys/random.h>

#include <algorithm>
#include <array>
#include <boost/json/serialize.hpp>
#include <cerrno>
#include <chrono>
#include <condition_variable>
#include <cstring>
#include <deque>
#include <limits>
#include <map>
#include <mutex>
#include <stdexcept>
#include <thread>
#include <utility>

namespace bbp {
namespace {

constexpr std::size_t kMaximumIdentifierBytes = 128U;
thread_local const void* g_mcp_operation_worker = nullptr;

struct McpSessionRequestFrame {
  const void* implementation;
  const McpSessionRequestFrame* previous;
};

thread_local const McpSessionRequestFrame* g_mcp_session_request = nullptr;

bool CurrentThreadHasSessionRequest(const void* implementation) {
  for (const McpSessionRequestFrame* frame = g_mcp_session_request;
       frame != nullptr; frame = frame->previous) {
    if (frame->implementation == implementation) {
      return true;
    }
  }
  return false;
}

std::string RandomId(std::string_view prefix) {
  std::array<unsigned char, 16U> bytes{};
  std::size_t offset = 0U;
  while (offset < bytes.size()) {
    const ssize_t received =
        getrandom(bytes.data() + offset, bytes.size() - offset, 0U);
    if (received < 0) {
      if (errno == EINTR) {
        continue;
      }
      throw std::runtime_error("getrandom for MCP operation failed: " +
                               std::string(std::strerror(errno)));
    }
    if (received == 0) {
      throw std::runtime_error(
          "getrandom for MCP operation identifier made no progress");
    }
    offset += static_cast<std::size_t>(received);
  }
  constexpr std::array<char, 16U> kHex = {'0', '1', '2', '3', '4', '5',
                                          '6', '7', '8', '9', 'a', 'b',
                                          'c', 'd', 'e', 'f'};
  std::string result(prefix);
  result.reserve(prefix.size() + bytes.size() * 2U);
  for (const unsigned char byte : bytes) {
    result.push_back(kHex[byte >> 4U]);
    result.push_back(kHex[byte & 0x0fU]);
  }
  return result;
}

void ValidateIdentifierImpl(std::string_view value, std::string_view label) {
  if (value.empty() || value.size() > kMaximumIdentifierBytes) {
    throw std::invalid_argument(std::string(label) +
                                " must contain 1..128 bytes");
  }
  for (std::size_t index = 0U; index < value.size(); ++index) {
    const char character = value[index];
    const bool alphanumeric = (character >= 'a' && character <= 'z') ||
                              (character >= 'A' && character <= 'Z') ||
                              (character >= '0' && character <= '9');
    const bool valid = (character >= 'a' && character <= 'z') ||
                       (character >= 'A' && character <= 'Z') ||
                       (character >= '0' && character <= '9') ||
                       character == '_' || character == '.' || character == '-';
    if (!valid || (index == 0U && !alphanumeric)) {
      throw std::invalid_argument(std::string(label) +
                                  " contains an invalid character");
    }
  }
}

void ValidateOperationKind(McpOperationKind kind) {
  if (kind == McpOperationKind::kCount) {
    throw std::invalid_argument("MCP operation kind is invalid");
  }
  static_cast<void>(McpOperationKindName(kind));
}

void ValidateInformationFamily(McpInformationFamily family) {
  if (family == McpInformationFamily::kCount) {
    throw std::invalid_argument("MCP information family is invalid");
  }
  static_cast<void>(McpInformationFamilyName(family));
}

void ValidateTypedResult(McpOperationKind kind, const McpTypedResult& result) {
  const McpResultFamily expected = McpOperationResultFamily(kind);
  if (result.family != expected) {
    throw McpOperationFailure(
        "invalid_result_family",
        "operation returned result family " +
            std::string(McpResultFamilyName(result.family)) + " but expected " +
            std::string(McpResultFamilyName(expected)),
        false);
  }
  const boost::json::value* declared =
      result.value.if_contains("result_family");
  if (declared == nullptr || !declared->is_string() ||
      declared->as_string() != McpResultFamilyName(result.family)) {
    throw McpOperationFailure(
        "invalid_result_shape",
        "operation result must declare its typed result_family", false);
  }
  if (boost::json::serialize(result.value).size() >
      kMcpMaximumRetainedResultBytes) {
    throw McpOperationFailure(
        "result_too_large",
        "operation result exceeds the retained result byte limit", false);
  }
}

std::string BoundedErrorMessage(std::string_view message) {
  if (message.size() <= kMcpMaximumEvidenceTextBytes) {
    return std::string(message);
  }
  return std::string(message.substr(0U, kMcpMaximumEvidenceTextBytes));
}

void ValidateError(McpOperationError* error) {
  try {
    ValidateMcpIdentifier(error->code, "MCP operation error code");
  } catch (const std::invalid_argument&) {
    error->code = "invalid_error_code";
  }
  error->message = BoundedErrorMessage(error->message);
  if (error->message.empty()) {
    error->message = "operation failed without a diagnostic message";
  }
  bool diagnostics_valid =
      error->diagnostics.size() <= kMcpMaximumSelectionItems;
  for (const boost::json::value& value : error->diagnostics) {
    if (!diagnostics_valid || !value.is_object()) {
      diagnostics_valid = false;
      break;
    }
    const boost::json::object& diagnostic = value.as_object();
    for (const auto& member : diagnostic) {
      if (member.key() != "code" && member.key() != "message" &&
          member.key() != "path" && member.key() != "recoverable" &&
          member.key() != "node_id" && member.key() != "action" &&
          member.key() != "state" && member.key() != "command_id") {
        diagnostics_valid = false;
        break;
      }
    }
    const boost::json::value* code = diagnostic.if_contains("code");
    const boost::json::value* message = diagnostic.if_contains("message");
    if (!diagnostics_valid || code == nullptr || !code->is_string() ||
        message == nullptr || !message->is_string() ||
        message->as_string().empty() ||
        message->as_string().size() > kMcpMaximumEvidenceTextBytes) {
      diagnostics_valid = false;
      break;
    }
    try {
      ValidateMcpIdentifier(code->as_string(), "MCP diagnostic code");
    } catch (const std::invalid_argument&) {
      diagnostics_valid = false;
      break;
    }
    const boost::json::value* path = diagnostic.if_contains("path");
    const boost::json::value* recoverable =
        diagnostic.if_contains("recoverable");
    if ((path != nullptr &&
         (!path->is_string() ||
          path->as_string().size() > kMcpMaximumEvidenceTextBytes)) ||
        (recoverable != nullptr && !recoverable->is_bool())) {
      diagnostics_valid = false;
      break;
    }
    for (const std::string_view identifier :
         {"node_id", "action", "state", "command_id"}) {
      const boost::json::value* member = diagnostic.if_contains(identifier);
      if (member == nullptr) {
        continue;
      }
      if (!member->is_string()) {
        diagnostics_valid = false;
        break;
      }
      try {
        ValidateMcpIdentifier(member->as_string(), identifier);
      } catch (const std::invalid_argument&) {
        diagnostics_valid = false;
        break;
      }
    }
  }
  if (!diagnostics_valid) {
    error->code = "invalid_error_evidence";
    error->message =
        "operation returned diagnostics outside the typed retained bound";
    error->retryable = false;
    error->diagnostics.clear();
  }
  boost::json::object serialized = McpOperationErrorJson(*error);
  if (boost::json::serialize(serialized).size() >
      kMcpMaximumRetainedResultBytes) {
    error->code = "error_evidence_too_large";
    error->message =
        "operation error evidence exceeded the retained byte limit";
    error->retryable = false;
    error->diagnostics.clear();
  }
}

bool ContainsFamily(const std::vector<McpInformationFamily>& families,
                    McpInformationFamily family) {
  return std::find(families.begin(), families.end(), family) != families.end();
}

bool ContainsNode(const std::vector<std::string>& node_ids,
                  const std::optional<std::string>& node_id) {
  return node_ids.empty() ||
         (node_id && std::find(node_ids.begin(), node_ids.end(), *node_id) !=
                         node_ids.end());
}

void ValidateEvidenceRecord(const McpEvidenceRecord& record) {
  ValidateInformationFamily(record.family);
  const auto validate_optional_identifier = [](const auto& value,
                                               std::string_view label) {
    if (value) {
      ValidateMcpIdentifier(*value, label);
    }
  };
  validate_optional_identifier(record.node_id, "evidence node id");
  validate_optional_identifier(record.kind, "evidence kind");
  validate_optional_identifier(record.artifact_id, "evidence artifact id");
  if (record.message && record.message->size() > kMcpMaximumEvidenceTextBytes) {
    throw std::invalid_argument("evidence message exceeds retained byte limit");
  }
  if (record.data && boost::json::serialize(*record.data).size() >
                         kMcpMaximumRetainedResultBytes) {
    throw std::invalid_argument("evidence data exceeds retained byte limit");
  }
}

McpServiceNotification OperationNotification(
    const McpOperationSnapshot& snapshot) {
  boost::json::object params{{"progressToken", snapshot.operation_id},
                             {"progress", snapshot.progress.completed},
                             {"total", snapshot.progress.total},
                             {"message", McpOperationStateName(snapshot.state)},
                             {"operation", McpOperationKindName(snapshot.kind)},
                             {"sequence", snapshot.sequence}};
  return McpServiceNotification{.session_id = snapshot.session_id,
                                .method = "notifications/progress",
                                .params = std::move(params)};
}

}  // namespace

void ValidateMcpIdentifier(std::string_view value, std::string_view label) {
  ValidateIdentifierImpl(value, label);
}

std::string_view McpOperationStateName(McpOperationState state) {
  switch (state) {
    case McpOperationState::kQueued:
      return "queued";
    case McpOperationState::kRunning:
      return "running";
    case McpOperationState::kCancelling:
      return "cancelling";
    case McpOperationState::kCancelled:
      return "cancelled";
    case McpOperationState::kSucceeded:
      return "succeeded";
    case McpOperationState::kFailed:
      return "failed";
  }
  throw std::logic_error("unknown MCP operation state");
}

bool IsTerminalMcpOperationState(McpOperationState state) {
  return state == McpOperationState::kCancelled ||
         state == McpOperationState::kSucceeded ||
         state == McpOperationState::kFailed;
}

McpOperationCancelled::McpOperationCancelled()
    : std::runtime_error("MCP operation cancelled") {}

McpOperationCancelled::McpOperationCancelled(std::string message,
                                             boost::json::array diagnostics)
    : std::runtime_error(std::move(message)),
      diagnostics_(std::move(diagnostics)) {}

const boost::json::array& McpOperationCancelled::diagnostics() const noexcept {
  return diagnostics_;
}

McpOperationFailure::McpOperationFailure(std::string code, std::string message,
                                         bool retryable,
                                         boost::json::array diagnostics)
    : std::runtime_error(std::move(message)),
      code_(std::move(code)),
      retryable_(retryable),
      diagnostics_(std::move(diagnostics)) {}

const std::string& McpOperationFailure::code() const noexcept { return code_; }

bool McpOperationFailure::retryable() const noexcept { return retryable_; }

const boost::json::array& McpOperationFailure::diagnostics() const noexcept {
  return diagnostics_;
}

McpOperationContext::McpOperationContext(
    std::stop_token stop_token,
    std::function<void(std::uint64_t)> progress_handler)
    : stop_token_(stop_token), progress_handler_(std::move(progress_handler)) {}

std::stop_token McpOperationContext::stop_token() const { return stop_token_; }

bool McpOperationContext::stop_requested() const {
  return stop_token_.stop_requested();
}

void McpOperationContext::ThrowIfCancelled() const {
  if (stop_requested()) {
    throw McpOperationCancelled();
  }
}

void McpOperationContext::ReportProgress(std::uint64_t completed) const {
  progress_handler_(completed);
}

struct McpOperationService::Impl {
  enum class ShutdownPhase {
    kRunning,
    kStopping,
    kComplete,
  };

  struct OperationRecord {
    McpOperationSnapshot snapshot;
    std::uint64_t admission_sequence = 0U;
    std::optional<std::jthread> worker;
    std::size_t waiters = 0U;
    bool worker_exited = false;
  };

  struct SubscriptionRecord {
    std::string id;
    std::vector<McpInformationFamily> families;
    std::vector<std::string> node_ids;
    std::deque<McpEvidenceRecord> notifications;
    std::uint64_t next_sequence = 1U;
    std::uint64_t dropped = 0U;
    bool active = true;
    std::uint64_t admission_sequence = 0U;
    std::size_t waiters = 0U;
  };

  struct SessionRecord {
    std::size_t active_operations = 0U;
    std::size_t waiters = 0U;
    std::stop_source request_stop_source;
    bool closing = false;
    std::map<std::string, SubscriptionRecord, std::less<>> subscriptions;
  };

  struct WaiterPin {
    WaiterPin(Impl* implementation, SessionRecord* session_record,
              std::size_t* target_waiters)
        : implementation(implementation),
          session_record(session_record),
          target_waiters(target_waiters) {
      ++session_record->waiters;
      ++*target_waiters;
    }

    ~WaiterPin() {
      --*target_waiters;
      --session_record->waiters;
      implementation->state_changed.notify_all();
    }

    WaiterPin(const WaiterPin&) = delete;
    WaiterPin& operator=(const WaiterPin&) = delete;

    Impl* implementation;
    SessionRecord* session_record;
    std::size_t* target_waiters;
  };

  explicit Impl(McpOperationServiceConfig config_value,
                McpServiceNotificationHandler notification_handler_value)
      : config(std::move(config_value)),
        notification_handler(std::move(notification_handler_value)) {
    ValidateConfig();
    stats.accepting = true;
  }

  ~Impl() { Shutdown(); }

  void ValidateConfig() const {
    const auto require_limit = [](std::size_t value, std::size_t maximum,
                                  std::string_view label) {
      if (value == 0U || value > maximum) {
        throw std::invalid_argument(std::string(label) +
                                    " must be within its advertised bound");
      }
    };
    require_limit(config.maximum_sessions, kMcpMaximumSessions,
                  "MCP session capacity");
    require_limit(config.maximum_tasks_per_session, kMcpMaximumTasksPerSession,
                  "MCP task capacity");
    require_limit(config.maximum_subscriptions_per_session,
                  kMcpMaximumSubscriptionsPerSession,
                  "MCP subscription capacity");
    require_limit(config.maximum_notifications_per_subscription,
                  kMcpMaximumNotificationsPerSession,
                  "MCP notification capacity");
    require_limit(config.maximum_retained_operations,
                  kMcpMaximumRetainedOperations,
                  "MCP retained operation capacity");
    require_limit(config.maximum_poll_items, kMcpListPageSize,
                  "MCP subscription page capacity");
  }

  SessionRecord& RequireSessionLocked(std::string_view session_id) {
    const auto session = sessions.find(session_id);
    if (session == sessions.end()) {
      throw std::runtime_error("unknown MCP operation session");
    }
    return session->second;
  }

  const SessionRecord& RequireSessionLocked(std::string_view session_id) const {
    const auto session = sessions.find(session_id);
    if (session == sessions.end()) {
      throw std::runtime_error("unknown MCP operation session");
    }
    return session->second;
  }

  OperationRecord& RequireOperationLocked(std::string_view session_id,
                                          std::string_view operation_id) {
    static_cast<void>(RequireSessionLocked(session_id));
    const auto operation = operations.find(operation_id);
    if (operation == operations.end() ||
        operation->second.snapshot.session_id != session_id) {
      throw std::runtime_error("unknown MCP operation for this session");
    }
    return operation->second;
  }

  const OperationRecord& RequireOperationLocked(
      std::string_view session_id, std::string_view operation_id) const {
    static_cast<void>(RequireSessionLocked(session_id));
    const auto operation = operations.find(operation_id);
    if (operation == operations.end() ||
        operation->second.snapshot.session_id != session_id) {
      throw std::runtime_error("unknown MCP operation for this session");
    }
    return operation->second;
  }

  SubscriptionRecord& RequireSubscriptionLocked(
      std::string_view session_id, std::string_view subscription_id) {
    SessionRecord& session = RequireSessionLocked(session_id);
    const auto subscription = session.subscriptions.find(subscription_id);
    if (subscription == session.subscriptions.end()) {
      throw std::runtime_error("unknown MCP subscription for this session");
    }
    return subscription->second;
  }

  const SubscriptionRecord& RequireSubscriptionLocked(
      std::string_view session_id, std::string_view subscription_id) const {
    const SessionRecord& session = RequireSessionLocked(session_id);
    const auto subscription = session.subscriptions.find(subscription_id);
    if (subscription == session.subscriptions.end()) {
      throw std::runtime_error("unknown MCP subscription for this session");
    }
    return subscription->second;
  }

  std::string UniqueOperationIdLocked() const {
    std::string id;
    do {
      id = RandomId("op-");
    } while (operations.contains(id));
    return id;
  }

  std::string UniqueSubscriptionIdLocked() const {
    std::string id;
    bool collision = false;
    do {
      id = RandomId("sub-");
      collision = false;
      for (const auto& [session_id, session] : sessions) {
        static_cast<void>(session_id);
        if (session.subscriptions.contains(id)) {
          collision = true;
          break;
        }
      }
    } while (collision);
    return id;
  }

  void IncrementSequenceLocked(McpOperationSnapshot* snapshot) {
    if (next_state_sequence == std::numeric_limits<std::uint64_t>::max()) {
      throw std::overflow_error("MCP operation state sequence overflow");
    }
    snapshot->sequence = next_state_sequence++;
  }

  bool CurrentThreadIsWorker() const { return g_mcp_operation_worker == this; }

  bool CurrentThreadIsSessionRequest() const {
    return CurrentThreadHasSessionRequest(this);
  }

  void CollectDeferredWorkersLocked(std::vector<std::jthread>* workers) {
    const std::thread::id current_thread = std::this_thread::get_id();
    for (auto worker = deferred_workers.begin();
         worker != deferred_workers.end();) {
      if (worker->get_id() == current_thread) {
        ++worker;
        continue;
      }
      workers->push_back(std::move(*worker));
      worker = deferred_workers.erase(worker);
    }
  }

  void TakeWorkerLocked(std::optional<std::jthread>* worker,
                        std::vector<std::jthread>* workers) {
    if (!*worker) {
      return;
    }
    if ((*worker)->get_id() == std::this_thread::get_id()) {
      deferred_workers.push_back(std::move(**worker));
    } else {
      workers->push_back(std::move(**worker));
    }
    worker->reset();
  }

  void CollectFinishedWorkersLocked(std::vector<std::jthread>* workers) {
    CollectDeferredWorkersLocked(workers);
    for (auto& [id, operation] : operations) {
      static_cast<void>(id);
      if (IsTerminalMcpOperationState(operation.snapshot.state) &&
          operation.worker_exited) {
        TakeWorkerLocked(&operation.worker, workers);
      }
    }
  }

  void EvictTerminalOperationLocked(std::vector<std::jthread>* workers) {
    auto oldest = operations.end();
    for (auto operation = operations.begin(); operation != operations.end();
         ++operation) {
      if (!IsTerminalMcpOperationState(operation->second.snapshot.state) ||
          operation->second.waiters != 0U) {
        continue;
      }
      if (oldest == operations.end() || operation->second.admission_sequence <
                                            oldest->second.admission_sequence) {
        oldest = operation;
      }
    }
    if (oldest == operations.end()) {
      throw std::runtime_error(
          "MCP retained operation capacity contains only active work");
    }
    TakeWorkerLocked(&oldest->second.worker, workers);
    operations.erase(oldest);
    ++stats.evicted_operations;
  }

  void Deliver(std::vector<McpServiceNotification> notifications) {
    if (!notification_handler) {
      return;
    }
    for (const McpServiceNotification& notification : notifications) {
      try {
        notification_handler(notification);
      } catch (...) {
        std::lock_guard<std::mutex> lock(mutex);
        ++stats.notification_handler_failures;
      }
    }
  }

  void NotifyOperation(const McpOperationSnapshot& snapshot) {
    Deliver({OperationNotification(snapshot)});
  }

  void ReportProgress(std::string_view operation_id, std::uint64_t completed) {
    McpOperationSnapshot snapshot;
    {
      std::lock_guard<std::mutex> lock(mutex);
      const auto operation = operations.find(operation_id);
      if (operation == operations.end()) {
        throw std::runtime_error("MCP operation progress target was evicted");
      }
      McpOperationSnapshot& target = operation->second.snapshot;
      if (IsTerminalMcpOperationState(target.state)) {
        throw std::logic_error("MCP operation progress is terminal");
      }
      if (completed < target.progress.completed) {
        throw std::invalid_argument("MCP operation progress cannot regress");
      }
      if (completed > target.progress.total) {
        throw std::invalid_argument(
            "MCP operation progress cannot exceed its total");
      }
      if (completed == target.progress.completed) {
        return;
      }
      target.progress.completed = completed;
      IncrementSequenceLocked(&target);
      snapshot = target;
    }
    state_changed.notify_all();
    NotifyOperation(snapshot);
  }

  void CompleteLocked(OperationRecord* operation, McpOperationState state,
                      std::optional<McpTypedResult> result,
                      std::optional<McpOperationError> error) {
    if (IsTerminalMcpOperationState(operation->snapshot.state)) {
      throw std::logic_error("MCP operation terminal state is immutable");
    }
    operation->snapshot.state = state;
    operation->snapshot.result = std::move(result);
    operation->snapshot.error = std::move(error);
    if (state == McpOperationState::kSucceeded) {
      operation->snapshot.progress.completed =
          operation->snapshot.progress.total;
      operation->snapshot.terminal_result_family =
          operation->snapshot.result->family;
      ++stats.succeeded_operations;
    } else {
      operation->snapshot.terminal_result_family = McpResultFamily::kError;
      if (state == McpOperationState::kCancelled) {
        ++stats.cancelled_operations;
      } else {
        ++stats.failed_operations;
      }
    }
    IncrementSequenceLocked(&operation->snapshot);
    SessionRecord& session =
        RequireSessionLocked(operation->snapshot.session_id);
    if (session.active_operations == 0U || stats.active_operations == 0U) {
      throw std::logic_error("MCP active operation accounting underflow");
    }
    --session.active_operations;
    --stats.active_operations;
  }

  void RunOperation(std::string operation_id, McpOperationExecutor executor,
                    std::stop_token stop_token) {
    bool execute = false;
    McpOperationSnapshot snapshot;
    {
      std::lock_guard<std::mutex> lock(mutex);
      const auto found = operations.find(operation_id);
      if (found == operations.end()) {
        return;
      }
      OperationRecord& operation = found->second;
      if (operation.snapshot.cancel_requested || stop_token.stop_requested()) {
        operation.snapshot.cancel_requested = true;
        McpOperationError error{.code = "cancelled",
                                .message = "operation cancelled before start",
                                .retryable = false,
                                .diagnostics = {}};
        CompleteLocked(&operation, McpOperationState::kCancelled, std::nullopt,
                       std::move(error));
      } else {
        operation.snapshot.state = McpOperationState::kRunning;
        IncrementSequenceLocked(&operation.snapshot);
        execute = true;
      }
      snapshot = operation.snapshot;
    }
    state_changed.notify_all();
    NotifyOperation(snapshot);
    if (!execute) {
      return;
    }

    std::optional<McpTypedResult> result;
    std::optional<McpOperationError> error;
    McpOperationState terminal_state = McpOperationState::kFailed;
    try {
      McpOperationContext context(
          stop_token, [this, operation_id](std::uint64_t completed) {
            ReportProgress(operation_id, completed);
          });
      result = executor(context);
      McpOperationKind kind = McpOperationKind::kCount;
      {
        std::lock_guard<std::mutex> lock(mutex);
        const auto found = operations.find(operation_id);
        if (found == operations.end()) {
          throw std::runtime_error("MCP operation was unexpectedly evicted");
        }
        kind = found->second.snapshot.kind;
      }
      ValidateTypedResult(kind, *result);
      terminal_state = McpOperationState::kSucceeded;
    } catch (const McpOperationCancelled& cancelled) {
      error = McpOperationError{.code = "cancelled",
                                .message = cancelled.what(),
                                .retryable = false,
                                .diagnostics = cancelled.diagnostics()};
      terminal_state = McpOperationState::kCancelled;
    } catch (const McpOperationFailure& failure) {
      error = McpOperationError{.code = failure.code(),
                                .message = failure.what(),
                                .retryable = failure.retryable(),
                                .diagnostics = failure.diagnostics()};
      terminal_state = McpOperationState::kFailed;
    } catch (const std::exception& failure) {
      error = McpOperationError{.code = "operation_failed",
                                .message = BoundedErrorMessage(failure.what()),
                                .retryable = false,
                                .diagnostics = {}};
      terminal_state = McpOperationState::kFailed;
    } catch (...) {
      error = McpOperationError{
          .code = "operation_failed",
          .message = "operation failed with a non-standard exception",
          .retryable = false,
          .diagnostics = {}};
      terminal_state = McpOperationState::kFailed;
    }
    if (error) {
      ValidateError(&*error);
      result.reset();
    }

    {
      std::lock_guard<std::mutex> lock(mutex);
      const auto found = operations.find(operation_id);
      if (found == operations.end()) {
        return;
      }
      OperationRecord& operation = found->second;
      if (terminal_state == McpOperationState::kCancelled) {
        operation.snapshot.cancel_requested = true;
      }
      CompleteLocked(&operation, terminal_state, std::move(result),
                     std::move(error));
      snapshot = operation.snapshot;
    }
    state_changed.notify_all();
    NotifyOperation(snapshot);
  }

  bool SessionWaitersDrainedLocked() const {
    return std::all_of(
        sessions.begin(), sessions.end(),
        [](const auto& session) { return session.second.waiters == 0U; });
  }

  void FinalizeShutdownLocked() {
    for (auto& [session_id, session] : sessions) {
      static_cast<void>(session_id);
      session.subscriptions.clear();
    }
    sessions.clear();
    stats.sessions = 0U;
    shutdown_phase = ShutdownPhase::kComplete;
  }

  void FailUnexpectedOperation(std::string_view operation_id) noexcept {
    try {
      McpOperationSnapshot snapshot;
      bool notify = false;
      {
        std::lock_guard<std::mutex> lock(mutex);
        const auto found = operations.find(operation_id);
        if (found != operations.end() &&
            !IsTerminalMcpOperationState(found->second.snapshot.state)) {
          McpOperationError error{
              .code = "operation_failed",
              .message = "operation worker failed at its thread boundary",
              .retryable = false,
              .diagnostics = {}};
          CompleteLocked(&found->second, McpOperationState::kFailed,
                         std::nullopt, std::move(error));
          snapshot = found->second.snapshot;
          notify = true;
        }
      }
      state_changed.notify_all();
      if (notify) {
        NotifyOperation(snapshot);
      }
    } catch (...) {
    }
  }

  void WorkerExited(std::string_view operation_id) noexcept {
    try {
      std::unique_lock<std::mutex> lock(mutex);
      const auto found = operations.find(operation_id);
      if (found != operations.end()) {
        found->second.worker_exited = true;
      }
      if (shutdown_phase == ShutdownPhase::kStopping &&
          shutdown_owner == std::this_thread::get_id() &&
          shutdown_workers_joined) {
        state_changed.wait(lock,
                           [this] { return SessionWaitersDrainedLocked(); });
        FinalizeShutdownLocked();
      }
      lock.unlock();
      state_changed.notify_all();
    } catch (...) {
    }
  }

  void WorkerMain(std::string operation_id, McpOperationExecutor executor,
                  std::stop_token stop_token) noexcept {
    const void* previous_worker = g_mcp_operation_worker;
    g_mcp_operation_worker = this;
    try {
      RunOperation(operation_id, std::move(executor), stop_token);
    } catch (...) {
      FailUnexpectedOperation(operation_id);
    }
    WorkerExited(operation_id);
    g_mcp_operation_worker = previous_worker;
  }

  McpSubscriptionSnapshot SubscriptionSnapshotLocked(
      std::string_view session_id, const SubscriptionRecord& subscription,
      std::uint64_t after_sequence, std::size_t limit) const {
    McpSubscriptionSnapshot snapshot{.subscription_id = subscription.id,
                                     .session_id = std::string(session_id),
                                     .items = {},
                                     .next_cursor = after_sequence,
                                     .dropped = subscription.dropped,
                                     .active = subscription.active};
    snapshot.items.reserve(limit);
    for (const McpEvidenceRecord& item : subscription.notifications) {
      if (item.sequence <= after_sequence) {
        continue;
      }
      snapshot.items.push_back(item);
      snapshot.next_cursor = item.sequence;
      if (snapshot.items.size() == limit) {
        break;
      }
    }
    return snapshot;
  }

  void Shutdown() {
    std::vector<std::jthread> workers;
    std::vector<std::stop_source> stop_sources;
    std::vector<McpServiceNotification> notifications;
    const auto join_workers = [](std::vector<std::jthread>* joinable_workers) {
      for (std::jthread& worker : *joinable_workers) {
        worker.request_stop();
        if (worker.joinable()) {
          worker.join();
        }
      }
    };
    {
      std::unique_lock<std::mutex> lock(mutex);
      if (shutdown_phase == ShutdownPhase::kComplete) {
        CollectFinishedWorkersLocked(&workers);
        lock.unlock();
        join_workers(&workers);
        return;
      }
      if (shutdown_phase == ShutdownPhase::kStopping) {
        if (shutdown_owner == std::this_thread::get_id() ||
            CurrentThreadIsWorker() || CurrentThreadIsSessionRequest()) {
          return;
        }
        state_changed.wait(lock, [this] {
          return shutdown_phase == ShutdownPhase::kComplete;
        });
        CollectFinishedWorkersLocked(&workers);
        lock.unlock();
        join_workers(&workers);
        return;
      }

      shutdown_phase = ShutdownPhase::kStopping;
      shutdown_owner = std::this_thread::get_id();
      shutdown_owner_is_worker = CurrentThreadIsWorker();
      shutdown_owner_is_request = CurrentThreadIsSessionRequest();
      stats.accepting = false;
      CollectDeferredWorkersLocked(&workers);
      for (auto& [id, operation] : operations) {
        static_cast<void>(id);
        if (!IsTerminalMcpOperationState(operation.snapshot.state)) {
          if (!operation.snapshot.cancel_requested) {
            operation.snapshot.cancel_requested = true;
            operation.snapshot.state = McpOperationState::kCancelling;
            IncrementSequenceLocked(&operation.snapshot);
            notifications.push_back(OperationNotification(operation.snapshot));
          }
          if (operation.worker) {
            stop_sources.push_back(operation.worker->get_stop_source());
          }
        }
        if (!operation.worker ||
            operation.worker->get_id() != std::this_thread::get_id()) {
          TakeWorkerLocked(&operation.worker, &workers);
        }
      }
      for (auto& [session_id, session] : sessions) {
        static_cast<void>(session_id);
        stop_sources.push_back(session.request_stop_source);
        for (auto& [subscription_id, subscription] : session.subscriptions) {
          static_cast<void>(subscription_id);
          subscription.active = false;
        }
      }
    }
    for (std::stop_source& stop_source : stop_sources) {
      stop_source.request_stop();
    }
    state_changed.notify_all();
    Deliver(std::move(notifications));
    join_workers(&workers);

    bool completed = false;
    {
      std::unique_lock<std::mutex> lock(mutex);
      shutdown_workers_joined = true;
      if (!shutdown_owner_is_worker && !shutdown_owner_is_request) {
        state_changed.wait(lock,
                           [this] { return SessionWaitersDrainedLocked(); });
        FinalizeShutdownLocked();
        completed = true;
      }
    }
    if (completed) {
      state_changed.notify_all();
    }
  }

  McpOperationServiceConfig config;
  McpServiceNotificationHandler notification_handler;
  mutable std::mutex mutex;
  mutable std::condition_variable_any state_changed;
  std::map<std::string, SessionRecord, std::less<>> sessions;
  std::map<std::string, OperationRecord, std::less<>> operations;
  std::vector<std::jthread> deferred_workers;
  McpOperationServiceStats stats;
  std::uint64_t next_state_sequence = 1U;
  std::uint64_t next_admission_sequence = 1U;
  ShutdownPhase shutdown_phase = ShutdownPhase::kRunning;
  std::thread::id shutdown_owner;
  bool shutdown_owner_is_worker = false;
  bool shutdown_owner_is_request = false;
  bool shutdown_workers_joined = false;
};

McpOperationService::McpOperationService(
    McpOperationServiceConfig config,
    McpServiceNotificationHandler notification_handler)
    : impl_(std::make_unique<Impl>(std::move(config),
                                   std::move(notification_handler))) {}

McpOperationService::~McpOperationService() = default;

void McpOperationService::RegisterSession(std::string session_id) {
  ValidateMcpIdentifier(session_id, "MCP session id");
  std::lock_guard<std::mutex> lock(impl_->mutex);
  if (!impl_->stats.accepting) {
    throw std::runtime_error("MCP operation service is shutting down");
  }
  if (impl_->sessions.contains(session_id)) {
    throw std::runtime_error("MCP operation session is already registered");
  }
  if (impl_->sessions.size() >= impl_->config.maximum_sessions) {
    throw std::runtime_error("MCP operation session capacity reached");
  }
  impl_->sessions.emplace(std::move(session_id), Impl::SessionRecord{});
  impl_->stats.sessions = impl_->sessions.size();
}

bool McpOperationService::RemoveSession(std::string_view session_id,
                                        std::chrono::milliseconds timeout) {
  if (timeout < std::chrono::milliseconds::zero()) {
    throw std::invalid_argument(
        "MCP session removal timeout cannot be negative");
  }
  std::vector<McpServiceNotification> notifications;
  std::vector<std::stop_source> stop_sources;
  std::vector<std::jthread> workers;
  std::unique_lock<std::mutex> lock(impl_->mutex);
  Impl::SessionRecord& session = impl_->RequireSessionLocked(session_id);
  session.closing = true;
  stop_sources.push_back(session.request_stop_source);
  for (auto& [id, operation] : impl_->operations) {
    static_cast<void>(id);
    if (operation.snapshot.session_id != session_id ||
        IsTerminalMcpOperationState(operation.snapshot.state)) {
      continue;
    }
    if (!operation.snapshot.cancel_requested) {
      operation.snapshot.cancel_requested = true;
      operation.snapshot.state = McpOperationState::kCancelling;
      impl_->IncrementSequenceLocked(&operation.snapshot);
      notifications.push_back(OperationNotification(operation.snapshot));
    }
    if (operation.worker) {
      stop_sources.push_back(operation.worker->get_stop_source());
    }
  }
  for (auto& [id, subscription] : session.subscriptions) {
    static_cast<void>(id);
    subscription.active = false;
  }
  lock.unlock();
  for (std::stop_source& stop_source : stop_sources) {
    stop_source.request_stop();
  }
  impl_->state_changed.notify_all();
  impl_->Deliver(std::move(notifications));
  lock.lock();
  const auto deadline = std::chrono::steady_clock::now() + timeout;
  const bool stopped = impl_->state_changed.wait_until(lock, deadline, [&] {
    const auto found = impl_->sessions.find(session_id);
    return found == impl_->sessions.end() ||
           (found->second.active_operations == 0U &&
            found->second.waiters == 0U);
  });
  if (!stopped) {
    return false;
  }
  for (auto& [id, operation] : impl_->operations) {
    static_cast<void>(id);
    if (operation.snapshot.session_id == session_id && operation.worker) {
      impl_->TakeWorkerLocked(&operation.worker, &workers);
    }
  }
  impl_->sessions.erase(std::string(session_id));
  impl_->stats.sessions = impl_->sessions.size();
  lock.unlock();
  impl_->state_changed.notify_all();
  for (std::jthread& worker : workers) {
    if (worker.joinable()) {
      worker.join();
    }
  }
  return true;
}

boost::json::value McpOperationService::ExecuteSessionRequest(
    std::string_view session_id, McpSessionRequestHandler handler,
    std::stop_token stop_token) {
  if (!handler) {
    throw std::invalid_argument("MCP session request handler must be callable");
  }
  std::stop_source combined_stop_source;
  std::stop_token session_stop_token;
  {
    std::lock_guard<std::mutex> lock(impl_->mutex);
    if (!impl_->stats.accepting) {
      throw std::runtime_error("MCP operation service is shutting down");
    }
    Impl::SessionRecord& session = impl_->RequireSessionLocked(session_id);
    if (session.closing) {
      throw std::runtime_error("MCP operation session is closing");
    }
    ++session.waiters;
    session_stop_token = session.request_stop_source.get_token();
  }

  std::stop_callback stop_on_request(
      stop_token, [&] { combined_stop_source.request_stop(); });
  std::stop_callback stop_on_session(
      session_stop_token, [&] { combined_stop_source.request_stop(); });
  const auto release_request = [&] {
    {
      std::lock_guard<std::mutex> lock(impl_->mutex);
      const auto session = impl_->sessions.find(session_id);
      if (session == impl_->sessions.end() || session->second.waiters == 0U) {
        throw std::logic_error(
            "MCP session request accounting is inconsistent");
      }
      --session->second.waiters;
      if (impl_->shutdown_phase == Impl::ShutdownPhase::kStopping &&
          impl_->shutdown_owner_is_request &&
          !impl_->shutdown_owner_is_worker && impl_->shutdown_workers_joined &&
          impl_->SessionWaitersDrainedLocked()) {
        impl_->FinalizeShutdownLocked();
      }
    }
    impl_->state_changed.notify_all();
  };

  const McpSessionRequestFrame request_frame{impl_.get(),
                                             g_mcp_session_request};
  g_mcp_session_request = &request_frame;
  boost::json::value result;
  try {
    result = handler(combined_stop_source.get_token());
  } catch (...) {
    g_mcp_session_request = request_frame.previous;
    release_request();
    throw;
  }
  g_mcp_session_request = request_frame.previous;
  release_request();
  return result;
}

McpOperationSnapshot McpOperationService::Submit(
    std::string_view session_id, McpOperationKind kind,
    std::uint64_t progress_total, McpOperationExecutor executor) {
  ValidateOperationKind(kind);
  if (progress_total == 0U) {
    throw std::invalid_argument(
        "MCP operation progress total must be positive");
  }
  if (!executor) {
    throw std::invalid_argument("MCP operation executor must be callable");
  }
  std::vector<std::jthread> finished_workers;
  McpOperationSnapshot initial;
  {
    std::lock_guard<std::mutex> lock(impl_->mutex);
    if (!impl_->stats.accepting) {
      throw std::runtime_error("MCP operation service is shutting down");
    }
    Impl::SessionRecord& session = impl_->RequireSessionLocked(session_id);
    if (session.closing) {
      throw std::runtime_error("MCP operation session is closing");
    }
    if (session.active_operations >= impl_->config.maximum_tasks_per_session) {
      throw std::runtime_error("MCP per-session task capacity reached");
    }
    if (impl_->next_admission_sequence ==
        std::numeric_limits<std::uint64_t>::max()) {
      throw std::overflow_error("MCP operation admission sequence overflow");
    }
    impl_->CollectFinishedWorkersLocked(&finished_workers);
    if (impl_->operations.size() == impl_->config.maximum_retained_operations) {
      impl_->EvictTerminalOperationLocked(&finished_workers);
    }
    const std::string operation_id = impl_->UniqueOperationIdLocked();
    Impl::OperationRecord record;
    record.snapshot = McpOperationSnapshot{
        .operation_id = operation_id,
        .session_id = std::string(session_id),
        .kind = kind,
        .state = McpOperationState::kQueued,
        .progress = {.completed = 0U, .total = progress_total},
        .cancel_requested = false,
        .terminal_result_family = McpOperationResultFamily(kind),
        .result = std::nullopt,
        .error = std::nullopt,
        .sequence = 0U};
    impl_->IncrementSequenceLocked(&record.snapshot);
    record.admission_sequence = impl_->next_admission_sequence++;
    auto [operation, inserted] =
        impl_->operations.emplace(operation_id, std::move(record));
    if (!inserted) {
      throw std::logic_error("MCP operation identifier collision");
    }
    ++session.active_operations;
    ++impl_->stats.active_operations;
    ++impl_->stats.submitted_operations;
    impl_->stats.maximum_active_operations = std::max(
        impl_->stats.maximum_active_operations, impl_->stats.active_operations);
    initial = operation->second.snapshot;
    try {
      operation->second.worker.emplace(
          [implementation = impl_.get(), operation_id,
           executor = std::move(executor)](std::stop_token stop_token) mutable {
            implementation->WorkerMain(operation_id, std::move(executor),
                                       stop_token);
          });
    } catch (...) {
      --session.active_operations;
      --impl_->stats.active_operations;
      --impl_->stats.submitted_operations;
      impl_->operations.erase(operation);
      throw;
    }
  }
  for (std::jthread& worker : finished_workers) {
    if (worker.joinable()) {
      worker.join();
    }
  }
  return initial;
}

McpOperationSnapshot McpOperationService::GetOperation(
    std::string_view session_id, std::string_view operation_id) const {
  std::lock_guard<std::mutex> lock(impl_->mutex);
  return impl_->RequireOperationLocked(session_id, operation_id).snapshot;
}

McpOperationCancellation McpOperationService::CancelOperation(
    std::string_view session_id, std::string_view operation_id) {
  McpOperationCancellation cancellation;
  std::optional<std::stop_source> stop_source;
  bool notify = false;
  {
    std::lock_guard<std::mutex> lock(impl_->mutex);
    Impl::OperationRecord& operation =
        impl_->RequireOperationLocked(session_id, operation_id);
    if (IsTerminalMcpOperationState(operation.snapshot.state)) {
      cancellation.already_terminal = true;
    } else if (!operation.snapshot.cancel_requested) {
      operation.snapshot.cancel_requested = true;
      operation.snapshot.state = McpOperationState::kCancelling;
      impl_->IncrementSequenceLocked(&operation.snapshot);
      if (operation.worker) {
        stop_source = operation.worker->get_stop_source();
      }
      cancellation.request_accepted = true;
      notify = true;
    }
    cancellation.operation = operation.snapshot;
  }
  if (stop_source) {
    stop_source->request_stop();
  }
  impl_->state_changed.notify_all();
  if (notify) {
    impl_->NotifyOperation(cancellation.operation);
  }
  return cancellation;
}

std::optional<McpOperationSnapshot> McpOperationService::WaitForOperation(
    std::string_view session_id, std::string_view operation_id,
    std::chrono::milliseconds timeout, std::stop_token stop_token) const {
  if (timeout < std::chrono::milliseconds::zero()) {
    throw std::invalid_argument(
        "MCP operation wait timeout cannot be negative");
  }
  std::unique_lock<std::mutex> lock(impl_->mutex);
  Impl::SessionRecord& session = impl_->RequireSessionLocked(session_id);
  Impl::OperationRecord& operation =
      impl_->RequireOperationLocked(session_id, operation_id);
  Impl::WaiterPin waiter(impl_.get(), &session, &operation.waiters);
  const auto terminal = [&] {
    return IsTerminalMcpOperationState(
        impl_->RequireOperationLocked(session_id, operation_id).snapshot.state);
  };
  if (!terminal()) {
    const auto deadline = std::chrono::steady_clock::now() + timeout;
    if (!impl_->state_changed.wait_until(lock, stop_token, deadline,
                                         terminal)) {
      return std::nullopt;
    }
  }
  return impl_->RequireOperationLocked(session_id, operation_id).snapshot;
}

McpSubscriptionSnapshot McpOperationService::CreateSubscription(
    std::string_view session_id, McpSubscriptionRequest request) {
  if (request.families.empty()) {
    throw std::invalid_argument(
        "MCP subscription requires an information family");
  }
  if (request.families.size() >
      static_cast<std::size_t>(McpInformationFamily::kCount)) {
    throw std::invalid_argument(
        "MCP subscription has too many information families");
  }
  for (const McpInformationFamily family : request.families) {
    ValidateInformationFamily(family);
  }
  std::sort(request.families.begin(), request.families.end());
  if (std::adjacent_find(request.families.begin(), request.families.end()) !=
      request.families.end()) {
    throw std::invalid_argument(
        "MCP subscription information families must be unique");
  }
  if (request.node_ids.size() > kMcpMaximumSelectionItems) {
    throw std::invalid_argument(
        "MCP subscription node selection exceeds its retained bound");
  }
  for (const std::string& node_id : request.node_ids) {
    ValidateMcpIdentifier(node_id, "MCP subscription node id");
  }
  std::sort(request.node_ids.begin(), request.node_ids.end());
  if (std::adjacent_find(request.node_ids.begin(), request.node_ids.end()) !=
      request.node_ids.end()) {
    throw std::invalid_argument("MCP subscription node ids must be unique");
  }
  if (request.cursor == std::numeric_limits<std::uint64_t>::max()) {
    throw std::invalid_argument("MCP subscription cursor cannot advance");
  }

  std::lock_guard<std::mutex> lock(impl_->mutex);
  if (!impl_->stats.accepting) {
    throw std::runtime_error("MCP operation service is shutting down");
  }
  Impl::SessionRecord& session = impl_->RequireSessionLocked(session_id);
  if (session.closing) {
    throw std::runtime_error("MCP operation session is closing");
  }
  if (impl_->next_admission_sequence ==
      std::numeric_limits<std::uint64_t>::max()) {
    throw std::overflow_error("MCP subscription admission sequence overflow");
  }
  if (session.subscriptions.size() ==
      impl_->config.maximum_subscriptions_per_session) {
    auto oldest = session.subscriptions.end();
    for (auto subscription = session.subscriptions.begin();
         subscription != session.subscriptions.end(); ++subscription) {
      if (subscription->second.active || subscription->second.waiters != 0U) {
        continue;
      }
      if (oldest == session.subscriptions.end() ||
          subscription->second.admission_sequence <
              oldest->second.admission_sequence) {
        oldest = subscription;
      }
    }
    if (oldest == session.subscriptions.end()) {
      throw std::runtime_error("MCP per-session subscription capacity reached");
    }
    session.subscriptions.erase(oldest);
  }
  const std::string subscription_id = impl_->UniqueSubscriptionIdLocked();
  Impl::SubscriptionRecord subscription{
      .id = subscription_id,
      .families = std::move(request.families),
      .node_ids = std::move(request.node_ids),
      .notifications = {},
      .next_sequence = request.cursor + 1U,
      .dropped = 0U,
      .active = true,
      .admission_sequence = impl_->next_admission_sequence++,
      .waiters = 0U};
  auto [inserted, was_inserted] =
      session.subscriptions.emplace(subscription_id, std::move(subscription));
  if (!was_inserted) {
    throw std::logic_error("MCP subscription identifier collision");
  }
  ++impl_->stats.active_subscriptions;
  ++impl_->stats.retained_subscriptions;
  return impl_->SubscriptionSnapshotLocked(session_id, inserted->second,
                                           request.cursor,
                                           impl_->config.maximum_poll_items);
}

McpSubscriptionSnapshot McpOperationService::PollSubscription(
    std::string_view session_id, std::string_view subscription_id,
    std::uint64_t after_sequence, std::size_t limit,
    std::chrono::milliseconds timeout, std::stop_token stop_token) const {
  if (limit == 0U || limit > impl_->config.maximum_poll_items) {
    throw std::invalid_argument("MCP subscription poll limit is out of range");
  }
  if (timeout < std::chrono::milliseconds::zero()) {
    throw std::invalid_argument("MCP subscription poll timeout is negative");
  }
  std::unique_lock<std::mutex> lock(impl_->mutex);
  Impl::SessionRecord& session = impl_->RequireSessionLocked(session_id);
  Impl::SubscriptionRecord& subscription =
      impl_->RequireSubscriptionLocked(session_id, subscription_id);
  Impl::WaiterPin waiter(impl_.get(), &session, &subscription.waiters);
  const auto ready = [&] {
    const Impl::SubscriptionRecord& subscription =
        impl_->RequireSubscriptionLocked(session_id, subscription_id);
    return !subscription.active ||
           (!subscription.notifications.empty() &&
            subscription.notifications.back().sequence > after_sequence) ||
           !impl_->stats.accepting;
  };
  if (!ready() && timeout > std::chrono::milliseconds::zero()) {
    const auto deadline = std::chrono::steady_clock::now() + timeout;
    static_cast<void>(
        impl_->state_changed.wait_until(lock, stop_token, deadline, ready));
  }
  return impl_->SubscriptionSnapshotLocked(
      session_id, impl_->RequireSubscriptionLocked(session_id, subscription_id),
      after_sequence, limit);
}

McpSubscriptionSnapshot McpOperationService::CancelSubscription(
    std::string_view session_id, std::string_view subscription_id) {
  McpSubscriptionSnapshot snapshot;
  {
    std::lock_guard<std::mutex> lock(impl_->mutex);
    Impl::SubscriptionRecord& subscription =
        impl_->RequireSubscriptionLocked(session_id, subscription_id);
    if (subscription.active) {
      subscription.active = false;
      if (impl_->stats.active_subscriptions == 0U) {
        throw std::logic_error("MCP active subscription accounting underflow");
      }
      --impl_->stats.active_subscriptions;
    }
    const std::uint64_t cursor =
        subscription.next_sequence == 0U ? 0U : subscription.next_sequence - 1U;
    snapshot = impl_->SubscriptionSnapshotLocked(
        session_id, subscription, cursor, impl_->config.maximum_poll_items);
  }
  impl_->state_changed.notify_all();
  return snapshot;
}

void McpOperationService::Publish(McpEvidenceRecord record) {
  ValidateEvidenceRecord(record);
  std::vector<McpServiceNotification> notifications;
  {
    std::lock_guard<std::mutex> lock(impl_->mutex);
    if (!impl_->stats.accepting) {
      throw std::runtime_error("MCP operation service is shutting down");
    }
    for (const auto& [session_id, session] : impl_->sessions) {
      for (const auto& [subscription_id, subscription] :
           session.subscriptions) {
        static_cast<void>(subscription_id);
        if (subscription.active &&
            ContainsFamily(subscription.families, record.family) &&
            ContainsNode(subscription.node_ids, record.node_id) &&
            subscription.next_sequence ==
                std::numeric_limits<std::uint64_t>::max()) {
          throw std::overflow_error("MCP subscription sequence overflow");
        }
      }
      static_cast<void>(session_id);
    }
    for (auto& [session_id, session] : impl_->sessions) {
      for (auto& [subscription_id, subscription] : session.subscriptions) {
        if (!subscription.active ||
            !ContainsFamily(subscription.families, record.family) ||
            !ContainsNode(subscription.node_ids, record.node_id)) {
          continue;
        }
        McpEvidenceRecord retained = record;
        retained.sequence = subscription.next_sequence++;
        if (subscription.notifications.size() ==
            impl_->config.maximum_notifications_per_subscription) {
          subscription.notifications.pop_front();
          ++subscription.dropped;
          ++impl_->stats.notifications_dropped;
        }
        subscription.notifications.push_back(retained);
        ++impl_->stats.notifications_published;
        notifications.push_back(McpServiceNotification{
            .session_id = session_id,
            .method = "notifications/resources/updated",
            .params = boost::json::object{
                {"uri", "bbp:///notifications"},
                {"subscription_id", subscription_id},
                {"item", McpEvidenceRecordJson(retained)}}});
      }
    }
  }
  impl_->state_changed.notify_all();
  impl_->Deliver(std::move(notifications));
}

McpOperationServiceStats McpOperationService::Stats() const {
  std::lock_guard<std::mutex> lock(impl_->mutex);
  McpOperationServiceStats result = impl_->stats;
  result.sessions = impl_->sessions.size();
  result.retained_operations = impl_->operations.size();
  result.active_workers = 0U;
  result.retained_subscriptions = 0U;
  result.active_subscriptions = 0U;
  for (const auto& [id, operation] : impl_->operations) {
    static_cast<void>(id);
    if (operation.worker &&
        !IsTerminalMcpOperationState(operation.snapshot.state)) {
      ++result.active_workers;
    }
  }
  for (const auto& [session_id, session] : impl_->sessions) {
    static_cast<void>(session_id);
    result.retained_subscriptions += session.subscriptions.size();
    for (const auto& [subscription_id, subscription] : session.subscriptions) {
      static_cast<void>(subscription_id);
      if (subscription.active) {
        ++result.active_subscriptions;
      }
    }
  }
  return result;
}

void McpOperationService::Shutdown() { impl_->Shutdown(); }

boost::json::object McpOperationSnapshotJson(
    const McpOperationSnapshot& snapshot) {
  boost::json::object result{
      {"result_family", McpResultFamilyName(McpResultFamily::kOperation)},
      {"operation_id", snapshot.operation_id},
      {"operation", McpOperationKindName(snapshot.kind)},
      {"state", McpOperationStateName(snapshot.state)},
      {"progress_completed", snapshot.progress.completed},
      {"progress_total", snapshot.progress.total},
      {"cancel_requested", snapshot.cancel_requested},
      {"terminal_result_family",
       McpResultFamilyName(snapshot.terminal_result_family)}};
  if (snapshot.result) {
    result["terminal_result"] = snapshot.result->value;
  }
  if (snapshot.error) {
    result["terminal_error"] =
        McpOperationErrorJson(*snapshot.error, snapshot.operation_id);
  }
  return result;
}

boost::json::object McpOperationErrorJson(const McpOperationError& error,
                                          std::string_view operation_id) {
  boost::json::object result{
      {"result_family", McpResultFamilyName(McpResultFamily::kError)},
      {"code", error.code},
      {"message", error.message},
      {"retryable", error.retryable},
      {"diagnostics", error.diagnostics}};
  if (!operation_id.empty()) {
    result["operation_id"] = operation_id;
  }
  return result;
}

boost::json::object McpEvidenceRecordJson(const McpEvidenceRecord& record) {
  boost::json::object result{
      {"family", McpInformationFamilyName(record.family)},
      {"sequence", record.sequence},
      {"timestamp_ms", record.timestamp_ms}};
  if (record.node_id) {
    result["node_id"] = *record.node_id;
  }
  if (record.kind) {
    result["kind"] = *record.kind;
  }
  if (record.message) {
    result["message"] = *record.message;
  }
  if (record.artifact_id) {
    result["artifact_id"] = *record.artifact_id;
  }
  if (record.data) {
    result["data"] = *record.data;
  }
  return result;
}

boost::json::object McpSubscriptionSnapshotJson(
    const McpSubscriptionSnapshot& snapshot) {
  boost::json::array items;
  items.reserve(snapshot.items.size());
  for (const McpEvidenceRecord& item : snapshot.items) {
    items.emplace_back(McpEvidenceRecordJson(item));
  }
  return boost::json::object{
      {"result_family", McpResultFamilyName(McpResultFamily::kSubscription)},
      {"subscription_id", snapshot.subscription_id},
      {"items", std::move(items)},
      {"next_cursor", std::to_string(snapshot.next_cursor)},
      {"dropped", snapshot.dropped},
      {"active", snapshot.active}};
}

}  // namespace bbp
