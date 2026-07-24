#include "bbp/mcp_live_application.h"

#include <algorithm>
#include <array>
#include <boost/json/array.hpp>
#include <boost/json/parse.hpp>
#include <boost/json/serialize.hpp>
#include <chrono>
#include <cstdint>
#include <exception>
#include <filesystem>
#include <initializer_list>
#include <limits>
#include <set>
#include <span>
#include <stdexcept>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include "bbp/chain_kind.h"
#include "bbp/drivers/chain_driver_registry.h"
#include "bbp/logging.h"
#include "bbp/mcp_registry.h"
#include "bbp/mcp_run_evidence.h"
#include "bbp/run_report.h"
#include "bbp/scenario_service.h"
#include "bbp/simulation_command_queue.h"
#include "bbp/simulator/options.h"
#include "bbp/util.h"

namespace bbp {
namespace {

constexpr std::array kLiveOperations = {
    McpOperationKind::kValidateScenario,
    McpOperationKind::kResolveScenario,
    McpOperationKind::kStopRun,
    McpOperationKind::kReportRun,
    McpOperationKind::kInvokeRuntimeCommand,
    McpOperationKind::kAddNode,
    McpOperationKind::kStopNode,
    McpOperationKind::kKillNode,
    McpOperationKind::kRestartNode,
    McpOperationKind::kQueryEvidence,
    McpOperationKind::kQueryLogs,
    McpOperationKind::kFollowLogs,
    McpOperationKind::kReadArtifact,
    McpOperationKind::kGetOperation,
    McpOperationKind::kCancelOperation,
    McpOperationKind::kCreateSubscription,
    McpOperationKind::kPollSubscription,
    McpOperationKind::kCancelSubscription,
};

constexpr std::array kRetainedOperations = {
    McpOperationKind::kValidateScenario, McpOperationKind::kResolveScenario,
    McpOperationKind::kReportRun,        McpOperationKind::kGetOperation,
    McpOperationKind::kCancelOperation,
};

constexpr std::array kOwnedRunOperations = {
    McpOperationKind::kQueryEvidence, McpOperationKind::kQueryLogs,
    McpOperationKind::kFollowLogs, McpOperationKind::kReadArtifact};

class CombinedStopToken {
  struct ForwardStop {
    std::stop_source* source;

    void operator()() const noexcept { source->request_stop(); }
  };

 public:
  CombinedStopToken(std::stop_token operation, std::stop_token application)
      : operation_callback_(operation, ForwardStop{&source_}),
        application_callback_(application, ForwardStop{&source_}) {}

  std::stop_token token() const { return source_.get_token(); }

 private:
  std::stop_source source_;
  std::stop_callback<ForwardStop> operation_callback_;
  std::stop_callback<ForwardStop> application_callback_;
};

constexpr std::uint64_t kMaximumNodeOperationTimeoutSeconds = 3600U;

bool IsSafeNodeAddIdentifier(std::string_view value) {
  return !value.empty() && value.size() <= 32U &&
         std::all_of(value.begin(), value.end(), [](char character) {
           return (character >= 'a' && character <= 'z') ||
                  (character >= 'A' && character <= 'Z') ||
                  (character >= '0' && character <= '9') || character == '-' ||
                  character == '_';
         });
}

std::optional<std::string> NodeAddOutcomeError(
    const SimulationCommand& command, const SimulationCommandOutcome& outcome) {
  if (!command.node_add) {
    return "successful node-add outcome has no request";
  }
  if (outcome.added_node_ids.empty() ||
      outcome.added_node_ids.size() != command.node_add->count) {
    return "successful node-add outcome has the wrong node-id count";
  }
  std::set<std::string> unique;
  for (const std::string& node_id : outcome.added_node_ids) {
    if (!IsSafeNodeAddIdentifier(node_id) || !unique.insert(node_id).second) {
      return "successful node-add outcome has an unsafe or duplicate node id";
    }
  }
  if (!command.node_add->node_ids.empty() &&
      outcome.added_node_ids != command.node_add->node_ids) {
    return "successful node-add outcome differs from its explicit node ids";
  }
  if (!outcome.inventory_generation || *outcome.inventory_generation == 0U ||
      !outcome.final_node_count ||
      *outcome.final_node_count < outcome.added_node_ids.size()) {
    return "successful node-add outcome omitted its inventory generation or "
           "final node count";
  }
  return std::nullopt;
}

bool IsNodeCapacityFailure(std::string_view message) {
  return message == "node.add request exceeds the configured node capacity" ||
         message == "node-add request exceeds the configured node capacity";
}

boost::json::array NodeCapacityDiagnostics(std::uint64_t requested,
                                           std::uint32_t current,
                                           std::uint32_t capacity) {
  return boost::json::array{boost::json::object{
      {"code", "node_capacity_exceeded"},
      {"message", "the requested node batch exceeds available capacity"},
      {"path", "request.count"},
      {"requested_count", requested},
      {"current_node_count", current},
      {"node_capacity", capacity},
      {"available_node_capacity",
       current <= capacity ? capacity - current : 0U},
  }};
}

void ThrowIfCancelled(std::stop_token stop_token) {
  if (stop_token.stop_requested()) {
    throw McpOperationCancelled();
  }
}

const boost::json::value& RequireMember(const boost::json::object& object,
                                        std::string_view name) {
  const boost::json::value* value = object.if_contains(name);
  if (value == nullptr) {
    throw std::invalid_argument("missing MCP application argument: " +
                                std::string(name));
  }
  return *value;
}

const boost::json::object& RequireObject(const boost::json::object& object,
                                         std::string_view name) {
  const boost::json::value& value = RequireMember(object, name);
  if (!value.is_object()) {
    throw std::invalid_argument("MCP application argument " +
                                std::string(name) + " must be an object");
  }
  return value.as_object();
}

std::string RequireString(const boost::json::object& object,
                          std::string_view name) {
  const boost::json::value& value = RequireMember(object, name);
  if (!value.is_string() || value.as_string().empty()) {
    throw std::invalid_argument("MCP application argument " +
                                std::string(name) +
                                " must be a non-empty string");
  }
  return std::string(value.as_string());
}

std::uint64_t OptionalUnsigned(const boost::json::object& object,
                               std::string_view name, std::uint64_t fallback) {
  const boost::json::value* value = object.if_contains(name);
  if (value == nullptr) {
    return fallback;
  }
  if (value->is_uint64()) {
    return value->as_uint64();
  }
  if (value->is_int64() && value->as_int64() >= 0) {
    return static_cast<std::uint64_t>(value->as_int64());
  }
  throw std::invalid_argument("MCP application argument " + std::string(name) +
                              " must be an unsigned integer");
}

std::optional<std::uint32_t> Uint32Value(const boost::json::value& value) {
  if (value.is_uint64() &&
      value.as_uint64() <= std::numeric_limits<std::uint32_t>::max()) {
    return static_cast<std::uint32_t>(value.as_uint64());
  }
  if (value.is_int64() && value.as_int64() >= 0 &&
      static_cast<std::uint64_t>(value.as_int64()) <=
          std::numeric_limits<std::uint32_t>::max()) {
    return static_cast<std::uint32_t>(value.as_int64());
  }
  return std::nullopt;
}

bool OptionalBoolean(const boost::json::object& object, std::string_view name,
                     bool fallback) {
  const boost::json::value* value = object.if_contains(name);
  if (value == nullptr) {
    return fallback;
  }
  if (!value->is_bool()) {
    throw std::invalid_argument("MCP application argument " +
                                std::string(name) + " must be a boolean");
  }
  return value->as_bool();
}

std::string OptionalCursor(const boost::json::object& object,
                           std::string_view name) {
  const boost::json::value* value = object.if_contains(name);
  if (value == nullptr) {
    return {};
  }
  if (!value->is_string() || value->as_string().empty()) {
    throw std::invalid_argument("MCP application argument " +
                                std::string(name) +
                                " must be a non-empty cursor string");
  }
  if (value->as_string().size() > 256U) {
    throw std::invalid_argument("MCP application argument " +
                                std::string(name) +
                                " exceeds the cursor byte bound");
  }
  return std::string(value->as_string());
}

std::vector<std::string> OptionalStringArray(const boost::json::object& object,
                                             std::string_view name,
                                             bool required = false) {
  const boost::json::value* value = object.if_contains(name);
  if (value == nullptr) {
    if (required) {
      throw std::invalid_argument("missing MCP application argument: " +
                                  std::string(name));
    }
    return {};
  }
  if (!value->is_array() || (required && value->as_array().empty())) {
    throw std::invalid_argument("MCP application argument " +
                                std::string(name) +
                                " must be a non-empty array");
  }
  std::vector<std::string> result;
  result.reserve(value->as_array().size());
  std::set<std::string, std::less<>> unique;
  for (const boost::json::value& item : value->as_array()) {
    if (!item.is_string() || item.as_string().empty()) {
      throw std::invalid_argument("MCP application argument " +
                                  std::string(name) +
                                  " must contain non-empty strings");
    }
    std::string text(item.as_string());
    if (!unique.insert(text).second) {
      throw std::invalid_argument("MCP application argument " +
                                  std::string(name) +
                                  " must contain unique strings");
    }
    result.push_back(std::move(text));
  }
  return result;
}

std::vector<McpInformationFamily> RequireFamilies(
    const boost::json::object& object) {
  const std::vector<std::string> names =
      OptionalStringArray(object, "families", true);
  std::vector<McpInformationFamily> families;
  families.reserve(names.size());
  const std::span<const McpNamedCapability> registry =
      McpInformationFamilyRegistry();
  for (const std::string& name : names) {
    const auto found = std::find_if(
        registry.begin(), registry.end(),
        [&](const McpNamedCapability& entry) { return entry.name == name; });
    if (found == registry.end()) {
      throw std::invalid_argument("unknown MCP evidence family: " + name);
    }
    families.push_back(static_cast<McpInformationFamily>(
        static_cast<std::size_t>(found - registry.begin())));
  }
  return families;
}

std::uint64_t EpochMilliseconds() {
  const auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(
      std::chrono::system_clock::now().time_since_epoch());
  if (elapsed.count() < 0) {
    return 0U;
  }
  return static_cast<std::uint64_t>(elapsed.count());
}

std::string_view CommandOutcomeStateName(SimulationCommandOutcomeState state) {
  switch (state) {
    case SimulationCommandOutcomeState::kSucceeded:
      return "succeeded";
    case SimulationCommandOutcomeState::kFailed:
      return "failed";
    case SimulationCommandOutcomeState::kCancelled:
      return "cancelled";
    case SimulationCommandOutcomeState::kTimedOut:
      return "timed_out";
    case SimulationCommandOutcomeState::kOutcomeUnconfirmed:
      return "outcome_unconfirmed";
  }
  throw std::logic_error("unknown simulation command outcome state");
}

std::string_view CommandCancellationCauseName(
    SimulationCommandCancellationCause cause) {
  switch (cause) {
    case SimulationCommandCancellationCause::kNone:
      return "none";
    case SimulationCommandCancellationCause::kClientCancel:
      return "client_cancel";
    case SimulationCommandCancellationCause::kDeadline:
      return "deadline";
    case SimulationCommandCancellationCause::kApplicationShutdown:
      return "application_shutdown";
  }
  throw std::logic_error("unknown simulation command cancellation cause");
}

boost::json::object SelectReportFields(
    const boost::json::object& report,
    std::initializer_list<std::string_view> fields) {
  boost::json::object selected;
  for (const std::string_view field : fields) {
    if (const boost::json::value* value = report.if_contains(field)) {
      selected[field] = *value;
    }
  }
  return selected;
}

boost::json::object ResourceEnvelope(McpInformationFamily family,
                                     std::string_view run_id,
                                     boost::json::value data) {
  return boost::json::object{
      {"family", McpInformationFamilyName(family)},
      {"run_id", run_id},
      {"timestamp_ms", EpochMilliseconds()},
      {"data", std::move(data)},
  };
}

boost::json::object BuildSchemaDocument(
    std::span<const McpOperationKind> selected_operations,
    std::span<const McpInformationFamily> selected_information_families) {
  boost::json::object operation_schemas;
  const std::span<const McpNamedCapability> operations = McpOperationRegistry();
  for (const McpOperationKind kind : selected_operations) {
    const std::size_t index = static_cast<std::size_t>(kind);
    if (index >= operations.size()) {
      throw std::logic_error("unknown MCP operation kind");
    }
    operation_schemas[operations[index].name] = boost::json::object{
        {"input",
         BuildMcpOperationInputSchema(kind, selected_information_families)},
        {"output", BuildMcpOperationOutputSchema(kind, selected_operations)},
    };
  }
  return boost::json::object{
      {"scenario", BuildMcpScenarioSchema()},
      {"simulation_command", BuildMcpSimulationCommandSchema()},
      {"operations", std::move(operation_schemas)},
      {"resources", BuildMcpResourceRegistry(selected_information_families)},
  };
}

boost::json::value ReadJsonObjectFile(const std::filesystem::path& path,
                                      std::stop_token stop_token) {
  if (stop_token.stop_requested()) {
    throw McpOperationCancelled();
  }
  boost::json::value value;
  try {
    value = boost::json::parse(
        ReadText(path, kMcpMaximumRetainedResultBytes, stop_token));
  } catch (...) {
    if (stop_token.stop_requested()) {
      throw McpOperationCancelled();
    }
    throw;
  }
  if (stop_token.stop_requested()) {
    throw McpOperationCancelled();
  }
  if (!value.is_object()) {
    throw std::runtime_error("MCP run document is not a JSON object: " +
                             path.string());
  }
  return value;
}

void RequireDocumentRunIdentity(const boost::json::object& document,
                                std::string_view run_id,
                                std::string_view document_kind) {
  const boost::json::value* actual = document.if_contains("run_id");
  if (actual == nullptr || !actual->is_string() ||
      actual->as_string() != run_id) {
    throw std::runtime_error("MCP " + std::string(document_kind) +
                             " identity does not match the selected run");
  }
}

boost::json::object EvidencePage(std::string_view run_id,
                                 McpInformationFamily family,
                                 boost::json::value data) {
  boost::json::object record{{"family", McpInformationFamilyName(family)},
                             {"sequence", 1U},
                             {"timestamp_ms", EpochMilliseconds()},
                             {"data", std::move(data)}};
  return boost::json::object{{"result_family", "evidence_page"},
                             {"run_id", run_id},
                             {"items", boost::json::array{std::move(record)}},
                             {"next_cursor", "1"},
                             {"truncated", false}};
}

McpRunEvidenceQuery ParseEvidenceQuery(const boost::json::object& arguments,
                                       bool logs_only) {
  McpRunEvidenceQuery query;
  query.families =
      logs_only
          ? std::vector<McpInformationFamily>{McpInformationFamily::kLogHistory}
          : RequireFamilies(arguments);
  query.node_ids = OptionalStringArray(arguments, "node_ids", logs_only);
  const std::uint64_t start = OptionalUnsigned(arguments, "start_sequence", 0U);
  query.cursor = OptionalCursor(arguments, "cursor");
  if (query.cursor.empty()) {
    query.start_sequence = start;
  }
  const std::uint64_t limit =
      OptionalUnsigned(arguments, "limit", kMcpListPageSize);
  if (limit == 0U || limit > kMcpListPageSize ||
      limit > std::numeric_limits<std::size_t>::max()) {
    throw std::invalid_argument("MCP evidence limit is out of range");
  }
  query.limit = static_cast<std::size_t>(limit);
  if (arguments.if_contains("end_sequence") != nullptr) {
    query.end_sequence = OptionalUnsigned(arguments, "end_sequence", 0U);
    if (*query.end_sequence < start) {
      throw std::invalid_argument(
          "MCP log end_sequence precedes start_sequence");
    }
  }
  return query;
}

}  // namespace

McpLiveApplication::McpLiveApplication(Config config)
    : config_(std::move(config)) {
  if (config_.run_id.empty()) {
    throw std::invalid_argument("MCP live application requires a run id");
  }
  if (config_.run_root.empty()) {
    throw std::invalid_argument("MCP live application requires a run root");
  }
  if (config_.retained_run) {
    if (config_.retained_run->chain.empty() ||
        config_.retained_run->state.empty()) {
      throw std::invalid_argument(
          "MCP retained application requires persisted run metadata");
    }
    if (config_.options != nullptr || config_.command_queue != nullptr ||
        config_.node_inventory_snapshot || config_.request_run_stop ||
        config_.run_started || config_.run_stopping || config_.run_stopped ||
        config_.publish_evidence || config_.close_run_subscriptions) {
      throw std::invalid_argument(
          "MCP retained application cannot own live run controls");
    }
    return;
  }
  if (config_.options == nullptr) {
    throw std::invalid_argument("MCP live application requires run options");
  }
  if (config_.command_queue == nullptr) {
    throw std::invalid_argument(
        "MCP live application requires a simulation command queue");
  }
  if (!config_.node_inventory_snapshot) {
    throw std::invalid_argument(
        "MCP live application requires an authoritative node inventory");
  }
  if (!config_.request_run_stop) {
    throw std::invalid_argument(
        "MCP live application requires a run stop callback");
  }
  if (static_cast<bool>(config_.publish_evidence) !=
      static_cast<bool>(config_.close_run_subscriptions)) {
    throw std::invalid_argument(
        "MCP live evidence publisher and subscription closer must be paired");
  }
}

McpLiveApplication::~McpLiveApplication() { Shutdown(); }

McpLiveApplication::ActiveRequest::ActiveRequest(
    McpLiveApplication* application)
    : application_(application) {
  application_->BeginRequest();
}

McpLiveApplication::ActiveRequest::~ActiveRequest() {
  application_->EndRequest();
}

McpApplicationOperationFactory McpLiveApplication::OperationFactory() {
  return [this](McpOperationKind kind, const boost::json::object& arguments,
                std::string_view session_id) {
    McpOperationPlan plan = BuildOperation(kind, arguments, session_id);
    if (plan.executor) {
      McpOperationExecutor executor = std::move(plan.executor);
      plan.executor = [this, executor = std::move(executor)](
                          McpOperationContext& context) mutable {
        ActiveRequest request(this);
        McpTypedResult result = executor(context);
        ThrowIfCancelled(request_stop_source_.get_token());
        return result;
      };
    }
    return plan;
  };
}

McpApplicationResourceReader McpLiveApplication::ResourceReader() {
  return [this](McpInformationFamily family, std::string_view session_id,
                std::stop_token stop_token) {
    ActiveRequest request(this);
    CombinedStopToken cancellation(stop_token,
                                   request_stop_source_.get_token());
    return ReadResource(family, session_id, cancellation.token());
  };
}

void McpLiveApplication::BeginRequest() {
#ifdef BBP_ENABLE_TEST_HOOKS
  std::function<void()> request_admitted_test_hook;
#endif
  {
    std::lock_guard<std::mutex> lock(mutex_);
    if (shutdown_) {
      throw McpOperationCancelled();
    }
    ++active_requests_;
#ifdef BBP_ENABLE_TEST_HOOKS
    request_admitted_test_hook = config_.request_admitted_test_hook;
#endif
  }
#ifdef BBP_ENABLE_TEST_HOOKS
  try {
    if (request_admitted_test_hook) {
      request_admitted_test_hook();
    }
  } catch (...) {
    EndRequest();
    throw;
  }
#endif
}

void McpLiveApplication::EndRequest() {
  std::lock_guard<std::mutex> lock(mutex_);
  if (active_requests_ == 0U) {
    std::terminate();
  }
  --active_requests_;
  if (active_requests_ == 0U) {
    requests_drained_.notify_all();
  }
}

std::vector<McpOperationKind> McpLiveApplication::SupportedOperations() const {
  if (!config_.retained_run) {
    return {kLiveOperations.begin(), kLiveOperations.end()};
  }
  std::vector<McpOperationKind> operations{kRetainedOperations.begin(),
                                           kRetainedOperations.end()};
  if (config_.retained_run->has_owned_artifacts) {
    operations.insert(operations.end(), kOwnedRunOperations.begin(),
                      kOwnedRunOperations.end());
  }
  return operations;
}

std::vector<McpInformationFamily>
McpLiveApplication::SupportedInformationFamilies() const {
  std::vector<McpInformationFamily> information_families;
  information_families.reserve(
      static_cast<std::size_t>(McpInformationFamily::kCount));
  for (std::size_t index = 0U;
       index < static_cast<std::size_t>(McpInformationFamily::kCount);
       ++index) {
    const McpInformationFamily family =
        static_cast<McpInformationFamily>(index);
    if (config_.retained_run && !config_.retained_run->has_owned_artifacts &&
        (family == McpInformationFamily::kArtifacts ||
         family == McpInformationFamily::kArtifactContents)) {
      continue;
    }
    information_families.push_back(family);
  }
  return information_families;
}

bool McpLiveApplication::read_only() const {
  return config_.retained_run.has_value();
}

McpOperationPlan McpLiveApplication::BuildOperation(
    McpOperationKind kind, const boost::json::object& arguments,
    std::string_view session_id) {
  static_cast<void>(session_id);
  const std::vector<McpOperationKind> supported = SupportedOperations();
  if (std::find(supported.begin(), supported.end(), kind) == supported.end()) {
    throw McpOperationFailure(
        config_.retained_run ? "read_only_run" : "operation_unavailable",
        config_.retained_run
            ? "the retained run is read-only and cannot execute this operation"
            : "the operation is unavailable in the current live run",
        false);
  }
  if (kind == McpOperationKind::kCreateSubscription) {
    RequireRun(arguments);
    std::lock_guard<std::mutex> lock(mutex_);
    if (shutdown_ || run_stopped_) {
      throw McpOperationFailure(
          "run_not_active",
          "subscriptions require a managed run that has not terminated", false);
    }
    return {};
  }
  if (kind != McpOperationKind::kStopRun &&
      kind != McpOperationKind::kReportRun &&
      kind != McpOperationKind::kInvokeRuntimeCommand &&
      kind != McpOperationKind::kAddNode &&
      kind != McpOperationKind::kStopNode &&
      kind != McpOperationKind::kKillNode &&
      kind != McpOperationKind::kRestartNode &&
      kind != McpOperationKind::kQueryEvidence &&
      kind != McpOperationKind::kQueryLogs &&
      kind != McpOperationKind::kFollowLogs &&
      kind != McpOperationKind::kReadArtifact) {
    return {};
  }
  RequireRun(arguments);
  {
    std::lock_guard<std::mutex> lock(mutex_);
    if (shutdown_) {
      throw std::runtime_error("MCP live application is shutting down");
    }
    if (!config_.retained_run && kind != McpOperationKind::kStopRun &&
        !run_started_ && !run_stopped_) {
      throw McpOperationFailure("run_not_ready",
                                "the managed run is still starting", true);
    }
  }

  if (kind == McpOperationKind::kStopRun) {
    return McpOperationPlan{
        .progress_total = 1U, .executor = [this](McpOperationContext& context) {
          context.ThrowIfCancelled();
          {
            std::lock_guard<std::mutex> lock(mutex_);
            if (shutdown_ || stop_requested_ || run_stopped_) {
              throw McpOperationFailure("run_not_active",
                                        "the managed run is no longer active",
                                        false);
            }
            stop_requested_ = true;
            command_outcome_ready_.notify_all();
          }
          run_stop_source_.request_stop();
          config_.request_run_stop();
          return McpTypedResult{
              .family = McpResultFamily::kRunLifecycle,
              .value = boost::json::object{{"result_family", "run_lifecycle"},
                                           {"run_id", config_.run_id},
                                           {"state", "stopping"},
                                           {"node_count", NodeCount()}}};
        }};
  }

  if (kind == McpOperationKind::kReportRun) {
    const std::vector<std::string> node_ids =
        OptionalStringArray(arguments, "node_ids");
    const bool include_artifacts =
        OptionalBoolean(arguments, "include_artifacts", false);
    if (include_artifacts && config_.retained_run &&
        !config_.retained_run->has_owned_artifacts) {
      throw McpOperationFailure(
          "artifact_unavailable",
          "the retained run has no verified owned artifacts", false);
    }
    return McpOperationPlan{
        .progress_total = 1U,
        .executor = [this, node_ids,
                     include_artifacts](McpOperationContext& context) {
          CombinedStopToken cancellation(context.stop_token(),
                                         request_stop_source_.get_token());
          const std::stop_token stop_token = cancellation.token();
          ThrowIfCancelled(stop_token);
          std::unique_lock<std::timed_mutex> publication_lock =
              AcquirePublicationLock(stop_token);
          boost::json::value report;
          if (node_ids.empty()) {
            report = ReportSnapshot(stop_token, true);
          } else {
            boost::json::array node_reports;
            node_reports.reserve(node_ids.size());
            for (const std::string& node_id : node_ids) {
              ThrowIfCancelled(stop_token);
              boost::json::value node_report;
              try {
                node_report = boost::json::parse(BuildNodeReportJson(
                    config_.run_root, node_id, 0U, stop_token));
              } catch (...) {
                ThrowIfCancelled(stop_token);
                throw;
              }
              if (!node_report.is_object()) {
                throw std::runtime_error(
                    "MCP node report is not a JSON object");
              }
              RequireDocumentRunIdentity(node_report.as_object(),
                                         config_.run_id, "node report");
              node_reports.push_back(node_report);
            }
            report =
                boost::json::object{{"run_id", config_.run_id},
                                    {"node_reports", std::move(node_reports)}};
          }
          if (include_artifacts) {
            report.as_object()["artifacts"] = BuildMcpRunArtifactInventory(
                config_.run_id, config_.run_root, stop_token);
          }
          ThrowIfCancelled(stop_token);
          return McpTypedResult{
              .family = McpResultFamily::kEvidencePage,
              .value =
                  EvidencePage(config_.run_id, McpInformationFamily::kReports,
                               std::move(report))};
        }};
  }

  if (kind == McpOperationKind::kQueryEvidence ||
      kind == McpOperationKind::kQueryLogs ||
      kind == McpOperationKind::kFollowLogs) {
    McpRunEvidenceQuery query = ParseEvidenceQuery(
        arguments, kind == McpOperationKind::kQueryLogs ||
                       kind == McpOperationKind::kFollowLogs);
    return McpOperationPlan{
        .progress_total = 1U,
        .executor = [this, query = std::move(query),
                     follow = kind == McpOperationKind::kFollowLogs](
                        McpOperationContext& context) mutable {
          CombinedStopToken cancellation(context.stop_token(),
                                         request_stop_source_.get_token());
          const std::stop_token stop_token = cancellation.token();
          constexpr auto kFollowWait = std::chrono::seconds(1);
          constexpr auto kFollowPoll = std::chrono::milliseconds(25);
          const auto deadline = std::chrono::steady_clock::now() + kFollowWait;
          while (true) {
            ThrowIfCancelled(stop_token);
            std::unique_lock<std::timed_mutex> publication_lock =
                AcquirePublicationLock(stop_token);
            boost::json::object page = QueryMcpRunEvidence(
                config_.run_id, config_.run_root, query, stop_token);
            if (publication_lock.owns_lock()) {
              publication_lock.unlock();
            }
            ThrowIfCancelled(stop_token);
            if (!follow || !page.at("items").as_array().empty() ||
                std::chrono::steady_clock::now() >= deadline) {
              return McpTypedResult{.family = McpResultFamily::kEvidencePage,
                                    .value = std::move(page)};
            }
            query.cursor = std::string(page.at("next_cursor").as_string());
            query.start_sequence = 0U;
            std::this_thread::sleep_for(kFollowPoll);
          }
        }};
  }

  if (kind == McpOperationKind::kReadArtifact) {
    const std::string artifact_id = RequireString(arguments, "artifact_id");
    const std::uint64_t offset = OptionalUnsigned(arguments, "offset", 0U);
    const std::uint64_t limit =
        OptionalUnsigned(arguments, "limit", 64U * 1024U);
    if (limit == 0U || limit > (1U << 20U) ||
        limit > std::numeric_limits<std::size_t>::max()) {
      throw std::invalid_argument("MCP artifact limit is out of range");
    }
    return McpOperationPlan{
        .progress_total = 1U,
        .executor = [this, artifact_id, offset,
                     limit = static_cast<std::size_t>(limit)](
                        McpOperationContext& context) {
          CombinedStopToken cancellation(context.stop_token(),
                                         request_stop_source_.get_token());
          const std::stop_token stop_token = cancellation.token();
          ThrowIfCancelled(stop_token);
          std::unique_lock<std::timed_mutex> publication_lock =
              AcquirePublicationLock(stop_token);
          return McpTypedResult{.family = McpResultFamily::kArtifactContent,
                                .value = ReadMcpRunArtifact(
                                    config_.run_id, config_.run_root,
                                    artifact_id, offset, limit, stop_token)};
        }};
  }

  const bool typed_node_operation = kind == McpOperationKind::kStopNode ||
                                    kind == McpOperationKind::kKillNode ||
                                    kind == McpOperationKind::kRestartNode;
  const bool direct_node_add_operation = kind == McpOperationKind::kAddNode;
  bool node_add_operation = direct_node_add_operation;
  std::optional<std::chrono::steady_clock::duration> command_timeout;
  SimulationCommand command;
  const auto node_capacity_failure_plan =
      [](const boost::json::object& request,
         const Options& validation_options) -> McpOperationPlan {
    const std::uint64_t requested_count =
        OptionalUnsigned(request, "count", 0U);
    const std::uint32_t current_count = validation_options.nodes;
    const std::uint32_t capacity = validation_options.node_capacity;
    return McpOperationPlan{
        .progress_total = kSimulationNodeAddProgressTotal,
        .executor = [requested_count, current_count,
                     capacity](McpOperationContext&) -> McpTypedResult {
          throw McpOperationFailure(
              "node_capacity_exceeded",
              "node.add request exceeds the configured node capacity", false,
              NodeCapacityDiagnostics(requested_count, current_count,
                                      capacity));
        }};
  };
  if (node_add_operation) {
    Options validation_options = *config_.options;
    McpLiveNodeInventorySnapshot inventory = LiveNodeInventory();
    validation_options.nodes =
        static_cast<std::uint32_t>(inventory.node_ids.size());
    validation_options.node_ids = std::move(inventory.node_ids);
    command.kind = SimulationCommandKind::kAddNodes;
    command.node_id = "sim";
    const boost::json::object& request = RequireObject(arguments, "request");
    try {
      command.node_add =
          ParseAndValidateSimulationNodeAddRequest(request, validation_options);
    } catch (const std::runtime_error& error) {
      if (!IsNodeCapacityFailure(error.what())) {
        throw;
      }
      return node_capacity_failure_plan(request, validation_options);
    }
    command.confirmed = true;
  } else if (typed_node_operation) {
    command.node_id = RequireString(arguments, "node_id");
    ValidateMcpIdentifier(command.node_id, "MCP node operation node_id");
    const std::uint64_t timeout_seconds =
        OptionalUnsigned(arguments, "timeout_sec", 30U);
    if (timeout_seconds == 0U ||
        timeout_seconds > kMaximumNodeOperationTimeoutSeconds ||
        timeout_seconds >
            static_cast<std::uint64_t>(
                std::numeric_limits<std::chrono::seconds::rep>::max())) {
      throw std::invalid_argument(
          "MCP node operation timeout_sec must be in 1..3600");
    }
    command.kind = kind == McpOperationKind::kStopNode
                       ? SimulationCommandKind::kStopNode
                   : kind == McpOperationKind::kKillNode
                       ? SimulationCommandKind::kKillNode
                       : SimulationCommandKind::kRestartNode;
    command.confirmed = true;
    command_timeout = std::chrono::seconds(timeout_seconds);
  } else {
    Options validation_options = *config_.options;
    McpLiveNodeInventorySnapshot inventory = LiveNodeInventory();
    validation_options.nodes =
        static_cast<std::uint32_t>(inventory.node_ids.size());
    validation_options.node_ids = std::move(inventory.node_ids);
    const boost::json::object& command_request =
        RequireObject(arguments, "command");
    try {
      command =
          ParseAndValidateSimulationCommand(command_request, validation_options);
    } catch (const std::runtime_error& error) {
      const boost::json::value* command_kind =
          command_request.if_contains("kind");
      const boost::json::value* node_add =
          command_request.if_contains("node_add");
      if (!IsNodeCapacityFailure(error.what()) || command_kind == nullptr ||
          !command_kind->is_string() ||
          command_kind->as_string() != "add_nodes" || node_add == nullptr ||
          !node_add->is_object()) {
        throw;
      }
      return node_capacity_failure_plan(node_add->as_object(),
                                        validation_options);
    }
    node_add_operation = command.kind == SimulationCommandKind::kAddNodes;
  }
  return McpOperationPlan{
      .progress_total =
          node_add_operation ? kSimulationNodeAddProgressTotal : 1U,
      .executor = [this, command = std::move(command), command_timeout,
                   typed_node_operation, node_add_operation,
                   direct_node_add_operation](
                      McpOperationContext& context) mutable {
        {
          std::lock_guard<std::mutex> lock(mutex_);
          if (shutdown_ || stop_requested_ || run_stopped_) {
            throw McpOperationFailure(
                "run_not_active", "the managed run is no longer active", false);
          }
        }
        CombinedStopToken cancellation(context.stop_token(),
                                       run_stop_source_.get_token());
        const std::stop_token stop_token = cancellation.token();
        const auto operation_control =
            std::make_shared<SimulationCommandControl>();
        std::stop_callback stop_command_on_client_cancellation(
            context.stop_token(), [operation_control] {
              operation_control->RequestCancellation(
                  SimulationCommandCancellationCause::kClientCancel);
            });
        std::stop_callback stop_command_on_application_shutdown(
            run_stop_source_.get_token(), [operation_control] {
              operation_control->RequestCancellation(
                  SimulationCommandCancellationCause::kApplicationShutdown);
            });
        ThrowIfCancelled(stop_token);
        const SimulationCommandKind command_kind = command.kind;
        const std::uint32_t expected_added_node_count =
            command.node_add ? command.node_add->count : 0U;
        const std::string command_node_id = command.node_id;
        const std::string command_action =
            command_kind == SimulationCommandKind::kStopNode   ? "node.stop"
            : command_kind == SimulationCommandKind::kKillNode ? "node.kill"
            : command_kind == SimulationCommandKind::kRestartNode
                ? "node.restart"
            : command_kind == SimulationCommandKind::kAddNodes
                ? "node.add"
                : std::string(SimulationCommandKindName(command_kind));
        const auto operation_started = std::chrono::steady_clock::now();
        const std::optional<std::chrono::steady_clock::time_point>
            terminal_deadline =
                command_timeout
                    ? std::optional<std::chrono::steady_clock::time_point>(
                          operation_started + *command_timeout)
                    : std::nullopt;
        const std::optional<std::chrono::steady_clock::time_point>
            cancellation_deadline =
                terminal_deadline
                    ? std::optional<std::chrono::steady_clock::time_point>(
                          *terminal_deadline -
                          kSimulationCommandCancellationReconciliation)
                    : std::nullopt;
        operation_control->absolute_deadline = terminal_deadline;
        command.operation_control = operation_control;
        const std::uint64_t sequence = SubmitCommand(std::move(command));
        const std::chrono::steady_clock::duration reconciliation_bound =
            node_add_operation ? kSimulationNodeAddCancellationReconciliation
                               : kSimulationCommandCancellationReconciliation;
        const SimulationCommandOutcome outcome = [&] {
          try {
            return WaitForCommand(
                sequence, stop_token, operation_control, cancellation_deadline,
                terminal_deadline, reconciliation_bound,
                node_add_operation ? &context : nullptr);
          } catch (...) {
            DetachPendingCommand(sequence);
            throw;
          }
        }();
        if (outcome.state == SimulationCommandOutcomeState::kCancelled) {
          if (typed_node_operation) {
            const bool application_shutdown =
                outcome.cancellation_cause ==
                SimulationCommandCancellationCause::kApplicationShutdown;
            const std::string message =
                application_shutdown
                    ? "the application shut down after reconciling the node "
                      "operation"
                    : "the client cancelled the node operation after its "
                      "authoritative state was reconciled";
            throw McpOperationCancelled(
                message,
                boost::json::array{boost::json::object{
                    {"code", application_shutdown ? "application_shutdown"
                                                  : "node_operation_cancelled"},
                    {"message", message},
                    {"path", "command-" + std::to_string(sequence)},
                    {"node_id", command_node_id},
                    {"action", command_action},
                    {"state", outcome.node_lifecycle.value_or("indeterminate")},
                    {"command_id", "command-" + std::to_string(sequence)},
                    {"recoverable", false}}});
          }
          throw McpOperationCancelled();
        }
        if (outcome.state == SimulationCommandOutcomeState::kTimedOut) {
          const std::string timeout_message =
              command_kind == SimulationCommandKind::kRestartNode
                  ? "node.restart timed out during phase " +
                        std::string(SimulationNodeRestartPhaseName(
                            operation_control->restart_phase.load(
                                std::memory_order_acquire))) +
                        " before authoritative lifecycle reconciliation"
                  : command_action +
                        " timed out before authoritative lifecycle "
                        "reconciliation";
          throw McpOperationFailure(
              "node_operation_timeout",
              "node operation command #" + std::to_string(sequence) +
                  " exceeded timeout_sec: " + timeout_message,
              false,
              boost::json::array{boost::json::object{
                  {"code", "node_operation_timeout"},
                  {"message", timeout_message},
                  {"path", "command-" + std::to_string(sequence)},
                  {"node_id", command_node_id},
                  {"action", command_action},
                  {"state", outcome.node_lifecycle.value_or("indeterminate")},
                  {"command_id", "command-" + std::to_string(sequence)},
                  {"recoverable", false}}});
        }
        if (outcome.state ==
            SimulationCommandOutcomeState::kOutcomeUnconfirmed) {
          config_.request_run_stop();
          const bool node_operation =
              typed_node_operation || node_add_operation;
          throw McpOperationFailure(
              node_operation ? "node_outcome_unconfirmed"
                             : "command_outcome_unconfirmed",
              (node_operation ? "node operation command #"
                              : "simulation command #") +
                  std::to_string(sequence) +
                  " did not publish authoritative state within its "
                  "cancellation bound",
              false,
              boost::json::array{boost::json::object{
                  {"code", node_operation ? "node_outcome_unconfirmed"
                                          : "command_outcome_unconfirmed"},
                  {"message",
                   node_operation
                       ? "the selected node state is indeterminate; run stop "
                         "was requested and blind retry is unsafe"
                       : "the command state is indeterminate; run stop was "
                         "requested and blind retry is unsafe"},
                  {"path", command_node_id},
                  {"node_id", command_node_id},
                  {"action", command_action},
                  {"state", outcome.node_lifecycle.value_or("indeterminate")},
                  {"command_id", "command-" + std::to_string(sequence)},
                  {"recoverable", false}}});
        }
        if (outcome.state == SimulationCommandOutcomeState::kFailed) {
          const std::string error =
              outcome.error.value_or("command failed without an error");
          const bool capacity_failure =
              node_add_operation && IsNodeCapacityFailure(error);
          const std::optional<SimulationNodeResourceFailure> resource_failure =
              node_add_operation ? operation_control->NodeResourceFailure()
                                 : std::nullopt;
          const bool node_operation =
              typed_node_operation || node_add_operation;
          const std::string code = capacity_failure ? "node_capacity_exceeded"
                                   : resource_failure
                                       ? "node_resource_unavailable"
                                   : node_add_operation ? "node_add_failed"
                                   : typed_node_operation
                                       ? "node_operation_failed"
                                       : "simulation_command_failed";
          boost::json::array diagnostics;
          if (capacity_failure) {
            diagnostics =
                NodeCapacityDiagnostics(expected_added_node_count, NodeCount(),
                                        config_.options->node_capacity);
          } else if (resource_failure) {
            diagnostics.emplace_back(boost::json::object{
                {"code", code},
                {"message", error},
                {"path", "request"},
                {"resource_kind", resource_failure->resource_kind},
                {"node_id", resource_failure->node_id},
                {"address", resource_failure->address},
                {"port", resource_failure->port},
                {"purpose", resource_failure->purpose},
                {"mutation_started", resource_failure->mutation_started},
                {"action", command_action},
                {"command_id", "command-" + std::to_string(sequence)},
                {"recoverable", true}});
          } else {
            diagnostics.emplace_back(boost::json::object{
                {"code", code},
                {"message", error},
                {"path", "command-" + std::to_string(sequence)},
                {"node_id", command_node_id},
                {"action", command_action},
                {"state", outcome.node_lifecycle.value_or("indeterminate")},
                {"command_id", "command-" + std::to_string(sequence)},
                {"recoverable", false}});
          }
          throw McpOperationFailure(code,
                                    (node_operation ? "node operation command #"
                                                    : "simulation command #") +
                                        std::to_string(sequence) +
                                        " failed: " + error,
                                    false, std::move(diagnostics));
        }
        if (outcome.state != SimulationCommandOutcomeState::kSucceeded) {
          throw std::logic_error("unknown simulation command outcome state");
        }
        if (typed_node_operation) {
          if (!outcome.node_lifecycle) {
            throw McpOperationFailure(
                "node_outcome_unconfirmed",
                "successful node operation omitted authoritative lifecycle "
                "state",
                false);
          }
          return McpTypedResult{
              .family = McpResultFamily::kMutation,
              .value = boost::json::object{
                  {"result_family", "mutation"},
                  {"run_id", config_.run_id},
                  {"added_node_ids", boost::json::array{}},
                  {"removed_node_ids", boost::json::array{}},
                  {"affected_node_ids", boost::json::array{command_node_id}},
                  {"action", command_action},
                  {"state", *outcome.node_lifecycle},
                  {"command_id", "command-" + std::to_string(sequence)},
                  {"unchanged", false}}};
        }
        if (node_add_operation) {
          if (outcome.added_node_ids.size() != expected_added_node_count ||
              outcome.added_node_ids.empty()) {
            config_.request_run_stop();
            throw McpOperationFailure(
                "node_outcome_unconfirmed",
                "successful node.add omitted its exact authoritative node "
                "identity set",
                false);
          }
          boost::json::array added_node_ids;
          boost::json::array affected_node_ids;
          added_node_ids.reserve(outcome.added_node_ids.size());
          affected_node_ids.reserve(outcome.added_node_ids.size());
          for (const std::string& node_id : outcome.added_node_ids) {
            added_node_ids.emplace_back(node_id);
            affected_node_ids.emplace_back(node_id);
          }
          if (!direct_node_add_operation) {
            return McpTypedResult{
                .family = McpResultFamily::kRuntimeCommand,
                .value = boost::json::object{
                    {"result_family", "runtime_command"},
                    {"run_id", config_.run_id},
                    {"command_id", "command-" + std::to_string(sequence)},
                    {"accepted", true},
                    {"state", "succeeded"},
                    {"action", "node.add"},
                    {"added_node_ids", std::move(added_node_ids)},
                    {"affected_node_ids", std::move(affected_node_ids)},
                    {"inventory_generation", *outcome.inventory_generation},
                    {"final_node_count", *outcome.final_node_count}}};
          }
          return McpTypedResult{
              .family = McpResultFamily::kMutation,
              .value = boost::json::object{
                  {"result_family", "mutation"},
                  {"run_id", config_.run_id},
                  {"added_node_ids", std::move(added_node_ids)},
                  {"removed_node_ids", boost::json::array{}},
                  {"affected_node_ids", std::move(affected_node_ids)},
                  {"action", "node.add"},
                  {"command_id", "command-" + std::to_string(sequence)},
                  {"inventory_generation", *outcome.inventory_generation},
                  {"final_node_count", *outcome.final_node_count},
                  {"unchanged", false}}};
        }
        return McpTypedResult{
            .family = McpResultFamily::kRuntimeCommand,
            .value = boost::json::object{
                {"result_family", "runtime_command"},
                {"run_id", config_.run_id},
                {"command_id", "command-" + std::to_string(sequence)},
                {"accepted", true},
                {"state", "succeeded"}}};
      }};
}

boost::json::value McpLiveApplication::ReadResource(
    McpInformationFamily family, std::string_view session_id,
    std::stop_token stop_token) {
  static_cast<void>(session_id);
  if (stop_token.stop_requested()) {
    throw McpOperationCancelled();
  }
  {
    std::lock_guard<std::mutex> lock(mutex_);
    if (shutdown_) {
      throw std::runtime_error("MCP live application is shutting down");
    }
  }
  const std::vector<McpInformationFamily> supported =
      SupportedInformationFamilies();
  if (std::find(supported.begin(), supported.end(), family) ==
      supported.end()) {
    throw McpOperationFailure(
        "resource_unavailable",
        "the requested resource is unavailable in the current endpoint", false);
  }

  if (family == McpInformationFamily::kCapabilities) {
    if (config_.retained_run) {
      static_cast<void>(ReportSnapshot(stop_token));
    }
    const std::vector<McpOperationKind> selected = SupportedOperations();
    const std::vector<McpInformationFamily> information_families =
        SupportedInformationFamilies();
    boost::json::object capabilities =
        BuildMcpCapabilityDocument(selected, information_families);
    capabilities["access_mode"] = read_only() ? "read_only" : "read_write";
    const std::uint32_t node_count = NodeCount();
    const ChainKind chain = config_.retained_run
                                ? ParseChainKind(config_.retained_run->chain)
                                : config_.options->chain;
    const std::uint32_t node_capacity =
        config_.retained_run ? node_count : config_.options->node_capacity;
    capabilities["current_run"] = boost::json::object{
        {"run_id", config_.run_id},
        {"chain", CurrentChain()},
        {"state", RunState(node_count)},
        {"node_count", node_count},
        {"node_capacity", node_capacity},
        {"chain_node_maximum", ChainDriverSpecFor(chain).max_nodes},
        {"available_node_capacity", config_.retained_run ? 0U
                                    : node_count <= node_capacity
                                        ? node_capacity - node_count
                                        : 0U}};
    return ResourceEnvelope(family, config_.run_id, std::move(capabilities));
  }
  if (family == McpInformationFamily::kSchemas) {
    const std::vector<McpOperationKind> selected = SupportedOperations();
    const std::vector<McpInformationFamily> information_families =
        SupportedInformationFamilies();
    return ResourceEnvelope(
        family, config_.run_id,
        BuildSchemaDocument(selected, information_families));
  }
  if (family == McpInformationFamily::kResolvedScenario) {
    boost::json::value scenario = ReadJsonObjectFile(
        config_.run_root / "resolved-scenario.json", stop_token);
    RequireDocumentRunIdentity(scenario.as_object(), config_.run_id,
                               "resolved scenario");
    return ResourceEnvelope(family, config_.run_id, std::move(scenario));
  }
  if (family == McpInformationFamily::kSourceScenario) {
    const std::filesystem::path source =
        config_.run_root / "source-scenario.json";
    if (std::filesystem::exists(source)) {
      boost::json::value scenario = ReadJsonObjectFile(source, stop_token);
      RequireDocumentRunIdentity(scenario.as_object(), config_.run_id,
                                 "source scenario");
      return ResourceEnvelope(family, config_.run_id, std::move(scenario));
    }
    return ResourceEnvelope(
        family, config_.run_id,
        boost::json::object{
            {"available", false},
            {"reason", "this run did not retain a source scenario document"}});
  }
  if (family == McpInformationFamily::kRunRegistry) {
    if (config_.retained_run) {
      static_cast<void>(ReportSnapshot(stop_token));
    }
    const std::uint32_t node_count = NodeCount();
    const std::uint32_t node_capacity =
        config_.retained_run ? node_count : config_.options->node_capacity;
    return ResourceEnvelope(
        family, config_.run_id,
        boost::json::array{boost::json::object{
            {"run_id", config_.run_id},
            {"state", RunState(node_count)},
            {"chain", CurrentChain()},
            {"node_count", node_count},
            {"node_capacity", node_capacity},
            {"chain_node_maximum",
             ChainDriverSpecFor(
                 config_.retained_run
                     ? ParseChainKind(config_.retained_run->chain)
                     : config_.options->chain)
                 .max_nodes},
            {"available_node_capacity",
             config_.retained_run ? 0U
             : node_count <= node_capacity
                 ? node_capacity - node_count
                 : 0U}}});
  }

  boost::json::object report = ReportSnapshot(stop_token);
  boost::json::value data;
  switch (family) {
    case McpInformationFamily::kLifecycle:
      data = boost::json::object{
          {"run_state", RunState()},
          {"nodes", report.if_contains("nodes_summary")
                        ? *report.if_contains("nodes_summary")
                        : boost::json::value(boost::json::array{})}};
      break;
    case McpInformationFamily::kNodes:
      data = SelectReportFields(report, {"node_configs", "nodes_summary"});
      break;
    case McpInformationFamily::kRoles:
      data = SelectReportFields(
          report, {"node_configs", "wallets_summary", "block_production"});
      break;
    case McpInformationFamily::kPeers:
      data = SelectReportFields(report, {"nodes_summary", "peer_connects",
                                         "peer_disconnects", "peer_waits"});
      break;
    case McpInformationFamily::kTopology:
      data = SelectReportFields(
          report, {"topology_initial_edges", "topology_current_edges",
                   "topology_groups_summary", "active_network_partitions",
                   "topology_blocked_rules", "topology_degraded_links"});
      break;
    case McpInformationFamily::kWallets:
    case McpInformationFamily::kBalances:
    case McpInformationFamily::kWalletMetrics:
      data = SelectReportFields(
          report, {"wallets_summary", "wallet_funding", "wallet_transactions"});
      break;
    case McpInformationFamily::kMining:
      data = SelectReportFields(report, {"block_production", "generated_blocks",
                                         "scheduled_blocks", "nodes_summary"});
      break;
    case McpInformationFamily::kTransactionLoad:
      data = SelectReportFields(
          report, {"transaction_load_live", "transaction_load_attempts",
                   "transaction_load_summaries", "transaction_visibility",
                   "transaction_confirmations"});
      break;
    case McpInformationFamily::kWorkloads:
    case McpInformationFamily::kWorkloadHistory:
      data = SelectReportFields(
          report, {"workloads", "scheduled_events_started",
                   "scheduled_events_completed", "scheduled_events_failed"});
      break;
    case McpInformationFamily::kProcesses:
    case McpInformationFamily::kNamespaces:
    case McpInformationFamily::kInterfaces:
    case McpInformationFamily::kQdiscs:
    case McpInformationFamily::kCgroups:
      data = SelectReportFields(report, {"nodes_summary"});
      break;
    case McpInformationFamily::kResourceState:
      data = SelectReportFields(
          report, {"resources", "nodes_summary", "resource_updates",
                   "resource_profile_updates"});
      break;
    case McpInformationFamily::kCleanupState:
      data = boost::json::object{
          {"state", RunState()},
          {"run_root_present", std::filesystem::exists(config_.run_root)}};
      break;
    case McpInformationFamily::kMetrics:
      data = SelectReportFields(report, {"metric_count", "nodes_summary"});
      break;
    case McpInformationFamily::kInstrumentation:
    case McpInformationFamily::kMeasurements:
    case McpInformationFamily::kMeasurementHistory:
    case McpInformationFamily::kComparisons:
      data = SelectReportFields(report, {"nodes_summary"});
      break;
    case McpInformationFamily::kEvents:
      data = SelectReportFields(
          report, {"event_count", "event_counts", "scheduled_events_started",
                   "scheduled_events_completed", "scheduled_events_failed"});
      break;
    case McpInformationFamily::kLogs:
    case McpInformationFamily::kLogHistory:
      data = SelectReportFields(report, {"nodes_summary"});
      break;
    case McpInformationFamily::kRpcFailures:
    case McpInformationFamily::kErrors:
      data = SelectReportFields(report,
                                {"nodes_summary", "scheduled_events_failed",
                                 "profile_update_rollback_failures",
                                 "topology_edge_rollback_failures"});
      break;
    case McpInformationFamily::kCommandHistory:
      data = SelectReportFields(report, {"operator_commands"});
      break;
    case McpInformationFamily::kReports:
      data = std::move(report);
      break;
    case McpInformationFamily::kGeneratedCommands:
      data = SelectReportFields(report, {"operator_connection_command"});
      break;
    case McpInformationFamily::kArtifacts:
      data = BuildMcpRunArtifactInventory(config_.run_id, config_.run_root,
                                          stop_token);
      break;
    case McpInformationFamily::kArtifactContents:
      data = boost::json::object{{"available_through", "artifact.read"}};
      break;
    case McpInformationFamily::kProgress:
    case McpInformationFamily::kOperations:
      data = boost::json::object{
          {"available_through",
           boost::json::array{"operation.get", "operation.cancel"}}};
      break;
    case McpInformationFamily::kNotifications:
      data = BuildMcpNotificationDiscovery(SupportedOperations());
      break;
    case McpInformationFamily::kCapabilities:
    case McpInformationFamily::kSchemas:
    case McpInformationFamily::kSourceScenario:
    case McpInformationFamily::kResolvedScenario:
    case McpInformationFamily::kRunRegistry:
    case McpInformationFamily::kCount:
      throw std::logic_error("invalid MCP live resource dispatch");
  }
  if (stop_token.stop_requested()) {
    throw McpOperationCancelled();
  }
  return ResourceEnvelope(family, config_.run_id, std::move(data));
}

void McpLiveApplication::RequireRun(
    const boost::json::object& arguments) const {
  const std::string run_id = RequireString(arguments, "run_id");
  if (run_id != config_.run_id) {
    throw McpOperationFailure("run_not_found",
                              "the requested run is not owned by this live "
                              "application: " +
                                  run_id,
                              false);
  }
}

std::uint64_t McpLiveApplication::SubmitCommand(SimulationCommand command) {
  std::lock_guard<std::mutex> lock(mutex_);
  if (shutdown_ || stop_requested_) {
    throw McpOperationFailure("run_not_active", "the managed run is stopping",
                              false);
  }
  if (pending_commands_.size() >= kMcpMaximumRetainedOperations) {
    throw McpOperationFailure(
        "command_capacity_reached",
        "MCP command correlation capacity reached its explicit 256-entry "
        "bound",
        true);
  }
  const std::uint64_t sequence =
      config_.command_queue->PushRuntimeCommand(std::move(command));
  const auto [entry, inserted] =
      pending_commands_.emplace(sequence, PendingCommand{});
  static_cast<void>(entry);
  if (!inserted) {
    throw std::logic_error("duplicate MCP simulation command sequence");
  }
  return sequence;
}

void McpLiveApplication::DetachPendingCommand(
    std::uint64_t sequence) noexcept {
  try {
    std::lock_guard<std::mutex> lock(mutex_);
    const auto pending = pending_commands_.find(sequence);
    if (pending == pending_commands_.end()) {
      return;
    }
    if (pending->second.completed) {
      pending_commands_.erase(pending);
    } else {
      pending->second.detached = true;
    }
    command_outcome_ready_.notify_all();
  } catch (...) {
  }
}

SimulationCommandOutcome McpLiveApplication::WaitForCommand(
    std::uint64_t sequence, std::stop_token stop_token,
    const std::shared_ptr<SimulationCommandControl>& operation_control,
    std::optional<std::chrono::steady_clock::time_point> cancellation_deadline,
    std::optional<std::chrono::steady_clock::time_point> terminal_deadline,
    std::chrono::steady_clock::duration reconciliation_bound,
    McpOperationContext* progress_context) {
  if (reconciliation_bound <= std::chrono::steady_clock::duration::zero()) {
    throw std::invalid_argument(
        "simulation command reconciliation bound must be positive");
  }
  std::stop_callback wake_on_cancellation(
      stop_token, [this] { command_outcome_ready_.notify_all(); });
  std::unique_lock<std::mutex> lock(mutex_);
  const auto stop_or_terminal = [&] {
    const auto pending = pending_commands_.find(sequence);
    return stop_token.stop_requested() || stop_requested_ || run_stopped_ ||
           shutdown_ || pending == pending_commands_.end() ||
           pending->second.completed;
  };
  bool cancellation_deadline_reached = false;
  if (progress_context != nullptr) {
    std::uint64_t last_progress = 0U;
    while (!stop_or_terminal()) {
      const auto poll_deadline =
          std::chrono::steady_clock::now() + std::chrono::milliseconds(50);
      const auto wait_deadline =
          cancellation_deadline
              ? std::min(poll_deadline, *cancellation_deadline)
              : poll_deadline;
      static_cast<void>(command_outcome_ready_.wait_until(lock, wait_deadline,
                                                          stop_or_terminal));
      const std::uint64_t progress =
          operation_control->progress_completed.load(std::memory_order_acquire);
      if (progress > last_progress) {
        lock.unlock();
        progress_context->ReportProgress(progress);
        lock.lock();
        last_progress = progress;
      }
      if (cancellation_deadline &&
          std::chrono::steady_clock::now() >= *cancellation_deadline &&
          !stop_or_terminal()) {
        cancellation_deadline_reached = true;
        break;
      }
    }
  } else if (cancellation_deadline) {
    cancellation_deadline_reached = !command_outcome_ready_.wait_until(
        lock, *cancellation_deadline, stop_or_terminal);
  } else {
    command_outcome_ready_.wait(lock, stop_or_terminal);
  }
  auto pending = pending_commands_.find(sequence);
  if (pending == pending_commands_.end()) {
    throw std::logic_error("MCP simulation command outcome was lost");
  }
  if (pending->second.completed) {
    if (!pending->second.outcome) {
      throw std::logic_error("MCP simulation command outcome is missing");
    }
    SimulationCommandOutcome outcome = std::move(*pending->second.outcome);
    pending_commands_.erase(pending);
    return outcome;
  }
  const bool cancelled = stop_token.stop_requested();
  const bool run_ended = stop_requested_ || run_stopped_ || shutdown_;
  if (!cancelled && !run_ended && !cancellation_deadline_reached) {
    throw std::logic_error("MCP simulation command wait ended unexpectedly");
  }
  lock.unlock();
  if (cancellation_deadline_reached) {
    operation_control->RequestCancellation(
        SimulationCommandCancellationCause::kDeadline);
  } else if (run_ended) {
    operation_control->RequestCancellation(
        SimulationCommandCancellationCause::kApplicationShutdown);
  } else {
    operation_control->RequestCancellation(
        SimulationCommandCancellationCause::kClientCancel);
  }
  lock.lock();
  auto drain_deadline = std::chrono::steady_clock::now() + reconciliation_bound;
  if (terminal_deadline) {
    drain_deadline = std::min(drain_deadline, *terminal_deadline);
  }
  const bool owner_reconciled =
      command_outcome_ready_.wait_until(lock, drain_deadline, [&] {
        const auto outcome = pending_commands_.find(sequence);
        return outcome == pending_commands_.end() || outcome->second.completed;
      });
  pending = pending_commands_.find(sequence);
  if (pending == pending_commands_.end()) {
    throw std::logic_error("MCP simulation command outcome was lost");
  }
  if (!owner_reconciled || !pending->second.completed) {
    pending->second.detached = true;
    lock.unlock();
    config_.request_run_stop();
    return SimulationCommandOutcome{
        .state = SimulationCommandOutcomeState::kOutcomeUnconfirmed,
        .cancellation_cause = operation_control->CancellationCause(),
        .error =
            "the simulator owner exceeded its declared cancellation "
            "reconciliation bound",
        .node_lifecycle = std::nullopt,
        .added_node_ids = {},
        .inventory_generation = std::nullopt,
        .final_node_count = std::nullopt,
    };
  }
  if (!pending->second.outcome) {
    throw std::logic_error("MCP simulation command outcome is missing");
  }
  SimulationCommandOutcome outcome = std::move(*pending->second.outcome);
  pending_commands_.erase(pending);
  return outcome;
}

void McpLiveApplication::RecordCommandOutcome(
    const SimulationCommand& command, const SimulationCommandOutcome& outcome) {
  SimulationCommandOutcome validated_outcome = outcome;
  const auto mark_outcome_unconfirmed = [&](std::string error) {
    validated_outcome.state =
        SimulationCommandOutcomeState::kOutcomeUnconfirmed;
    validated_outcome.error = std::move(error);
    try {
      config_.request_run_stop();
    } catch (...) {
    }
  };
  if (command.kind == SimulationCommandKind::kAddNodes) {
    try {
      const std::optional<SimulationCommandCommitPhase> commit_phase =
          command.operation_control
              ? std::optional<SimulationCommandCommitPhase>(
                    command.operation_control->CommitPhase())
              : std::nullopt;
      const bool phase_mismatch =
          !commit_phase ||
          (outcome.state == SimulationCommandOutcomeState::kSucceeded &&
           *commit_phase != SimulationCommandCommitPhase::kCommitted) ||
          ((outcome.state == SimulationCommandOutcomeState::kCancelled ||
            outcome.state == SimulationCommandOutcomeState::kTimedOut) &&
           *commit_phase != SimulationCommandCommitPhase::kCancelled) ||
          (outcome.state == SimulationCommandOutcomeState::kFailed &&
           *commit_phase != SimulationCommandCommitPhase::kOpen);
      if (phase_mismatch) {
        mark_outcome_unconfirmed(
            "node-add outcome does not match its authoritative commit phase");
      } else if (outcome.state ==
                 SimulationCommandOutcomeState::kSucceeded) {
        const std::optional<std::string> validation_error =
            NodeAddOutcomeError(command, outcome);
        const std::optional<std::uint64_t> initial_generation =
            command.operation_control->InitialInventoryGeneration();
        const std::optional<std::vector<std::string>> initial_node_ids =
            command.operation_control->InitialInventoryNodeIds();
        if (validation_error) {
          mark_outcome_unconfirmed(*validation_error);
        } else if (!initial_generation ||
                   *initial_generation ==
                       std::numeric_limits<std::uint64_t>::max() ||
                   *initial_generation + 1U !=
                       *outcome.inventory_generation) {
          mark_outcome_unconfirmed(
              "successful node-add outcome reused or skipped its "
              "authoritative inventory generation");
        } else if (!initial_node_ids) {
          mark_outcome_unconfirmed(
              "successful node-add outcome omitted its authoritative initial "
              "node identities");
        } else {
          const McpLiveNodeInventorySnapshot inventory = LiveNodeInventory();
          const bool generation_mismatch =
              *outcome.inventory_generation != inventory.generation;
          const bool count_mismatch =
              inventory.node_ids.size() != *outcome.final_node_count ||
              *outcome.final_node_count > config_.options->node_capacity;
          const std::size_t expected_count =
              initial_node_ids->size() + outcome.added_node_ids.size();
          const bool identity_mismatch =
              count_mismatch || expected_count != inventory.node_ids.size() ||
              !std::equal(initial_node_ids->begin(), initial_node_ids->end(),
                          inventory.node_ids.begin()) ||
              !std::equal(
                  outcome.added_node_ids.begin(),
                  outcome.added_node_ids.end(),
                  inventory.node_ids.begin() +
                      static_cast<std::vector<std::string>::difference_type>(
                          initial_node_ids->size()));
          if (generation_mismatch) {
            mark_outcome_unconfirmed(
                "successful node-add outcome does not match the authoritative "
                "inventory generation");
          } else if (count_mismatch || identity_mismatch) {
            mark_outcome_unconfirmed(
                "successful node-add outcome does not preserve and append the "
                "authoritative node identities");
          }
        }
      }
    } catch (const std::exception& error) {
      mark_outcome_unconfirmed(
          "node-add outcome reconciliation failed: " +
          std::string(error.what()));
    } catch (...) {
      mark_outcome_unconfirmed(
          "node-add outcome reconciliation failed with an unknown exception");
    }
  }
  std::optional<SimulationCommandOutcome> published_outcome =
      validated_outcome;
  {
    std::lock_guard<std::mutex> lock(mutex_);
    const auto pending = pending_commands_.find(command.sequence);
    if (pending == pending_commands_.end()) {
      // TUI and scheduled commands have no MCP-local waiter, but their shared
      // authoritative outcomes are still public evidence.
    } else if (pending->second.detached) {
      pending_commands_.erase(pending);
      published_outcome = validated_outcome;
    } else if (pending->second.completed) {
      pending->second.outcome = SimulationCommandOutcome{
          .state = SimulationCommandOutcomeState::kFailed,
          .cancellation_cause = SimulationCommandCancellationCause::kNone,
          .error =
              "simulation command processor published more than one outcome",
          .node_lifecycle = std::nullopt,
          .added_node_ids = {},
          .inventory_generation = std::nullopt,
          .final_node_count = std::nullopt,
      };
      published_outcome = pending->second.outcome;
    } else {
      pending->second.completed = true;
      pending->second.outcome = validated_outcome;
      published_outcome = validated_outcome;
    }
    command_outcome_ready_.notify_all();
  }
  boost::json::object data{
      {"command_id", "command-" + std::to_string(command.sequence)},
      {"action", SimulationCommandKindName(command.kind)},
      {"state", CommandOutcomeStateName(published_outcome->state)},
      {"cancellation_cause",
       CommandCancellationCauseName(published_outcome->cancellation_cause)}};
  if (published_outcome->error) {
    data["error"] = *published_outcome->error;
  }
  if (published_outcome->node_lifecycle) {
    data["node_lifecycle"] = *published_outcome->node_lifecycle;
  }
  if (!published_outcome->added_node_ids.empty()) {
    boost::json::array added_node_ids;
    for (const std::string& node_id : published_outcome->added_node_ids) {
      added_node_ids.emplace_back(node_id);
    }
    data["added_node_ids"] = std::move(added_node_ids);
  }
  if (published_outcome->inventory_generation) {
    data["inventory_generation"] = *published_outcome->inventory_generation;
  }
  if (published_outcome->final_node_count) {
    data["final_node_count"] = *published_outcome->final_node_count;
  }
  PublishEvidence(McpInformationFamily::kEvents, "command_outcome",
                  "simulation command reached an authoritative outcome",
                  command.node_id.empty()
                      ? std::nullopt
                      : std::optional<std::string>(command.node_id),
                  std::move(data));
}

boost::json::object McpLiveApplication::ReportSnapshot(
    std::stop_token stop_token, bool publication_locked) {
  if (stop_token.stop_requested()) {
    throw McpOperationCancelled();
  }
  std::unique_lock<std::mutex> lock(report_mutex_, std::defer_lock);
  while (!lock.try_lock()) {
    if (stop_token.stop_requested()) {
      throw McpOperationCancelled();
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
  }
  if (stop_token.stop_requested()) {
    throw McpOperationCancelled();
  }
  std::unique_lock<std::timed_mutex> publication_lock;
  if (!publication_locked) {
    publication_lock = AcquirePublicationLock(stop_token);
  }
  boost::json::object report;
  try {
    report = BuildRunReport(config_.run_root, stop_token);
  } catch (...) {
    if (stop_token.stop_requested()) {
      throw McpOperationCancelled();
    }
    throw;
  }
  if (stop_token.stop_requested()) {
    throw McpOperationCancelled();
  }
  const boost::json::value* run_id = report.if_contains("run_id");
  if (run_id == nullptr || !run_id->is_string() ||
      run_id->as_string() != config_.run_id) {
    throw std::runtime_error(
        "MCP report identity does not match the selected run");
  }
  if (config_.retained_run) {
    const boost::json::value* chain = report.if_contains("chain");
    const boost::json::value* state = report.if_contains("status");
    const boost::json::value* nodes = report.if_contains("nodes");
    const std::optional<std::uint32_t> node_count =
        nodes == nullptr ? std::nullopt : Uint32Value(*nodes);
    if (chain == nullptr || !chain->is_string() ||
        chain->as_string() != config_.retained_run->chain || state == nullptr ||
        !state->is_string() || !node_count) {
      throw std::runtime_error("MCP retained report metadata is inconsistent");
    }
    std::lock_guard<std::mutex> state_lock(mutex_);
    config_.retained_run->state = std::string(state->as_string());
    config_.retained_run->node_count = *node_count;
  }
  return report;
}

std::string McpLiveApplication::RunState(
    std::optional<std::uint32_t> live_node_count) const {
  std::lock_guard<std::mutex> lock(mutex_);
  if (config_.retained_run) {
    return config_.retained_run->state;
  }
  if (run_stopped_ || shutdown_) {
    return "stopped";
  }
  if (stop_requested_) {
    return "stopping";
  }
  if (!run_started_) {
    return "starting";
  }
  const std::uint32_t node_count =
      live_node_count ? *live_node_count : NodeCount();
  return node_count == 0U ? "empty" : "active";
}

std::string McpLiveApplication::CurrentChain() const {
  if (config_.retained_run) {
    return config_.retained_run->chain;
  }
  return std::string(ChainKindName(config_.options->chain));
}

std::uint32_t McpLiveApplication::NodeCount() const {
  if (config_.retained_run) {
    std::lock_guard<std::mutex> lock(mutex_);
    return config_.retained_run->node_count;
  }
  const std::size_t count = LiveNodeInventory().node_ids.size();
  if (count > std::numeric_limits<std::uint32_t>::max()) {
    throw std::overflow_error("MCP live node inventory exceeds uint32");
  }
  return static_cast<std::uint32_t>(count);
}

std::uint32_t McpLiveApplication::current_node_count() const {
  return NodeCount();
}

McpLiveNodeInventorySnapshot McpLiveApplication::LiveNodeInventory() const {
  if (config_.retained_run || !config_.node_inventory_snapshot) {
    throw std::logic_error("authoritative live node inventory is unavailable");
  }
  std::unique_lock<std::timed_mutex> publication_lock =
      AcquirePublicationLock({});
  McpLiveNodeInventorySnapshot snapshot = config_.node_inventory_snapshot();
  if (snapshot.node_ids.size() > config_.options->node_capacity ||
      snapshot.node_ids.size() > std::numeric_limits<std::uint32_t>::max()) {
    throw std::logic_error(
        "authoritative live node inventory exceeds configured capacity");
  }
  std::set<std::string> unique;
  for (const std::string& node_id : snapshot.node_ids) {
    if (!IsSafeNodeAddIdentifier(node_id) || !unique.insert(node_id).second) {
      throw std::logic_error(
          "authoritative live node inventory has an invalid identity");
    }
  }
  return snapshot;
}

std::unique_lock<std::timed_mutex>
McpLiveApplication::AcquirePublicationLock(std::stop_token stop_token) const {
  if (!config_.publication_mutex) {
    return {};
  }
  std::unique_lock<std::timed_mutex> lock(*config_.publication_mutex,
                                          std::defer_lock);
  while (!lock.try_lock_for(std::chrono::milliseconds(10))) {
    if (stop_token.stop_requested()) {
      throw McpOperationCancelled();
    }
  }
  if (stop_token.stop_requested()) {
    throw McpOperationCancelled();
  }
  return lock;
}

void McpLiveApplication::MarkRunStarted() {
  bool notify = false;
  {
    std::lock_guard<std::mutex> lock(mutex_);
    if (config_.retained_run || stop_requested_ || run_stopped_ || shutdown_) {
      return;
    }
    if (!run_started_) {
      run_started_ = true;
      notify = true;
    }
  }
  if (notify && config_.run_started) {
    config_.run_started();
  }
  if (notify) {
    PublishEvidence(McpInformationFamily::kLifecycle, "run_started",
                    "managed run entered its active lifecycle", std::nullopt,
                    boost::json::object{{"state", RunState()}});
  }
}

void McpLiveApplication::MarkRunStopping() {
  bool notify = false;
  {
    std::lock_guard<std::mutex> lock(mutex_);
    if (config_.retained_run || run_stopped_ || shutdown_) {
      return;
    }
    if (!stop_requested_) {
      stop_requested_ = true;
      notify = true;
    }
    command_outcome_ready_.notify_all();
  }
  run_stop_source_.request_stop();
  if (notify && config_.run_stopping) {
    config_.run_stopping();
  }
  if (notify) {
    PublishEvidence(McpInformationFamily::kLifecycle, "run_stopping",
                    "managed run began bounded shutdown", std::nullopt,
                    boost::json::object{{"state", "stopping"}});
  }
}

void McpLiveApplication::MarkRunStopped() {
  bool notify = false;
  {
    std::lock_guard<std::mutex> lock(mutex_);
    if (config_.retained_run) {
      return;
    }
    stop_requested_ = true;
    if (!run_stopped_) {
      run_stopped_ = true;
      notify = true;
    }
    command_outcome_ready_.notify_all();
  }
  run_stop_source_.request_stop();
  if (notify && config_.run_stopped) {
    config_.run_stopped();
  }
  if (notify) {
    PublishEvidence(McpInformationFamily::kLifecycle, "run_stopped",
                    "managed run reached its terminal lifecycle", std::nullopt,
                    boost::json::object{{"state", "stopped"}});
    CloseRunSubscriptions();
  }
}

void McpLiveApplication::PublishEvidence(
    McpInformationFamily family, std::string kind, std::string message,
    std::optional<std::string> node_id,
    std::optional<boost::json::value> data) const noexcept {
  if (!config_.publish_evidence) {
    return;
  }
  try {
    config_.publish_evidence(McpEvidenceRecord{
        .run_id = config_.run_id,
        .family = family,
        .sequence = 0U,
        .timestamp_ms = EpochMilliseconds(),
        .node_id = std::move(node_id),
        .kind = std::move(kind),
        .message = std::move(message),
        .artifact_id = std::nullopt,
        .data = std::move(data),
    });
  } catch (const std::exception& error) {
    BBP_LOG(error) << "MCP evidence publication failed for run "
                   << config_.run_id << ": " << error.what();
  } catch (...) {
    BBP_LOG(error) << "MCP evidence publication failed for run "
                   << config_.run_id << " with a non-standard exception";
  }
}

void McpLiveApplication::CloseRunSubscriptions() const noexcept {
  if (!config_.close_run_subscriptions) {
    return;
  }
  try {
    config_.close_run_subscriptions(config_.run_id);
  } catch (const std::exception& error) {
    BBP_LOG(error) << "MCP subscription closure failed for run "
                   << config_.run_id << ": " << error.what();
  } catch (...) {
    BBP_LOG(error) << "MCP subscription closure failed for run "
                   << config_.run_id << " with a non-standard exception";
  }
}

void McpLiveApplication::Shutdown() {
  {
    std::lock_guard<std::mutex> lock(mutex_);
    shutdown_ = true;
    stop_requested_ = true;
    command_outcome_ready_.notify_all();
  }
  request_stop_source_.request_stop();
  run_stop_source_.request_stop();
  std::unique_lock<std::mutex> lock(mutex_);
  requests_drained_.wait(lock, [this] { return active_requests_ == 0U; });
}

}  // namespace bbp
