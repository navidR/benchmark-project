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
    McpOperationKind::kStopNode,
    McpOperationKind::kKillNode,
    McpOperationKind::kRestartNode,
    McpOperationKind::kQueryEvidence,
    McpOperationKind::kQueryLogs,
    McpOperationKind::kFollowLogs,
    McpOperationKind::kReadArtifact,
    McpOperationKind::kGetOperation,
    McpOperationKind::kCancelOperation,
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
        config_.request_run_stop || config_.run_started ||
        config_.run_stopping || config_.run_stopped) {
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
  if (!config_.request_run_stop) {
    throw std::invalid_argument(
        "MCP live application requires a run stop callback");
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
  if (kind != McpOperationKind::kStopRun &&
      kind != McpOperationKind::kReportRun &&
      kind != McpOperationKind::kInvokeRuntimeCommand &&
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
          }
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
          boost::json::value report;
          if (node_ids.empty()) {
            report = ReportSnapshot(stop_token);
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
            boost::json::object page = QueryMcpRunEvidence(
                config_.run_id, config_.run_root, query, stop_token);
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
          return McpTypedResult{.family = McpResultFamily::kArtifactContent,
                                .value = ReadMcpRunArtifact(
                                    config_.run_id, config_.run_root,
                                    artifact_id, offset, limit, stop_token)};
        }};
  }

  const bool typed_node_operation = kind == McpOperationKind::kStopNode ||
                                    kind == McpOperationKind::kKillNode ||
                                    kind == McpOperationKind::kRestartNode;
  std::optional<std::chrono::steady_clock::duration> command_timeout;
  SimulationCommand command;
  if (typed_node_operation) {
    command.node_id = RequireString(arguments, "node_id");
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
    command = ParseAndValidateSimulationCommand(
        RequireObject(arguments, "command"), *config_.options);
  }
  return McpOperationPlan{
      .progress_total = 1U,
      .executor = [this, command = std::move(command), command_timeout,
                   typed_node_operation](McpOperationContext& context) mutable {
        CombinedStopToken cancellation(context.stop_token(),
                                       request_stop_source_.get_token());
        const std::stop_token stop_token = cancellation.token();
        const auto operation_stop_source = std::make_shared<std::stop_source>();
        std::stop_callback stop_command_on_cancellation(
            stop_token,
            [operation_stop_source] { operation_stop_source->request_stop(); });
        ThrowIfCancelled(stop_token);
        command.operation_stop_source = operation_stop_source;
        const std::uint64_t sequence = SubmitCommand(std::move(command));
        const std::optional<std::chrono::steady_clock::time_point> deadline =
            command_timeout
                ? std::optional<std::chrono::steady_clock::time_point>(
                      std::chrono::steady_clock::now() + *command_timeout)
                : std::nullopt;
        const std::optional<std::string> error = WaitForCommand(
            sequence, stop_token, operation_stop_source, deadline);
        if (error) {
          throw McpOperationFailure(
              typed_node_operation ? "node_operation_failed"
                                   : "simulation_command_failed",
              (typed_node_operation ? "node operation command #"
                                    : "simulation command #") +
                  std::to_string(sequence) + " failed: " + *error,
              false,
              boost::json::array{boost::json::object{
                  {"code", typed_node_operation ? "node_operation_failed"
                                                : "simulation_command_failed"},
                  {"message", *error},
                  {"path", "command-" + std::to_string(sequence)},
                  {"recoverable", false}}});
        }
        if (typed_node_operation) {
          return McpTypedResult{.family = McpResultFamily::kMutation,
                                .value = boost::json::object{
                                    {"result_family", "mutation"},
                                    {"run_id", config_.run_id},
                                    {"added_node_ids", boost::json::array{}},
                                    {"removed_node_ids", boost::json::array{}},
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
    capabilities["current_run"] =
        boost::json::object{{"run_id", config_.run_id},
                            {"chain", CurrentChain()},
                            {"state", RunState()},
                            {"node_count", NodeCount()}};
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
    return ResourceEnvelope(
        family, config_.run_id,
        boost::json::array{boost::json::object{{"run_id", config_.run_id},
                                               {"state", RunState()},
                                               {"chain", CurrentChain()},
                                               {"node_count", NodeCount()}}});
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
      data = boost::json::object{
          {"transport", "MCP SSE GET stream"},
          {"methods", boost::json::array{"notifications/progress"}}};
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

std::optional<std::string> McpLiveApplication::WaitForCommand(
    std::uint64_t sequence, std::stop_token stop_token,
    const std::shared_ptr<std::stop_source>& operation_stop_source,
    std::optional<std::chrono::steady_clock::time_point> deadline) {
  std::stop_callback wake_on_cancellation(
      stop_token, [this] { command_outcome_ready_.notify_all(); });
  std::unique_lock<std::mutex> lock(mutex_);
  const auto terminal = [&] {
    const auto pending = pending_commands_.find(sequence);
    return stop_token.stop_requested() || stop_requested_ || run_stopped_ ||
           shutdown_ || pending == pending_commands_.end() ||
           pending->second.completed;
  };
  bool deadline_reached = false;
  if (deadline) {
    deadline_reached =
        !command_outcome_ready_.wait_until(lock, *deadline, terminal);
  } else {
    command_outcome_ready_.wait(lock, terminal);
  }
  const auto pending = pending_commands_.find(sequence);
  if (pending == pending_commands_.end()) {
    throw std::logic_error("MCP simulation command outcome was lost");
  }
  if (pending->second.completed) {
    std::optional<std::string> error = std::move(pending->second.error);
    pending_commands_.erase(pending);
    return error;
  }
  const bool cancelled = stop_token.stop_requested();
  const bool run_ended = stop_requested_ || run_stopped_ || shutdown_;
  pending_commands_.erase(pending);
  lock.unlock();
  operation_stop_source->request_stop();
  if (cancelled) {
    throw McpOperationCancelled();
  }
  if (deadline_reached) {
    throw McpOperationFailure(
        "node_operation_timeout",
        "node operation command #" + std::to_string(sequence) +
            " did not produce a terminal outcome before timeout_sec",
        false,
        boost::json::array{boost::json::object{
            {"code", "node_operation_timeout"},
            {"message",
             "the admitted node command was cancelled at its "
             "wall-clock deadline"},
            {"path", "command-" + std::to_string(sequence)},
            {"recoverable", false}}});
  }
  if (run_ended) {
    throw McpOperationFailure("run_stopped_before_command_outcome",
                              "the run stopped before the command produced a "
                              "terminal outcome",
                              false);
  }
  throw std::logic_error("MCP simulation command wait ended unexpectedly");
}

void McpLiveApplication::RecordCommandOutcome(
    const SimulationCommand& command, std::optional<std::string_view> error) {
  std::lock_guard<std::mutex> lock(mutex_);
  const auto pending = pending_commands_.find(command.sequence);
  if (pending == pending_commands_.end()) {
    return;
  }
  if (pending->second.completed) {
    pending->second.error =
        "simulation command processor published more than one outcome";
  } else {
    pending->second.completed = true;
    if (error) {
      pending->second.error = std::string(*error);
    }
  }
  command_outcome_ready_.notify_all();
}

boost::json::object McpLiveApplication::ReportSnapshot(
    std::stop_token stop_token) {
  if (stop_token.stop_requested()) {
    throw McpOperationCancelled();
  }
  std::unique_lock<std::timed_mutex> lock(report_mutex_, std::defer_lock);
  while (!lock.try_lock_for(std::chrono::milliseconds(10))) {
    if (stop_token.stop_requested()) {
      throw McpOperationCancelled();
    }
  }
  if (stop_token.stop_requested()) {
    throw McpOperationCancelled();
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

std::string McpLiveApplication::RunState() const {
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
  return config_.options->nodes == 0U ? "empty" : "active";
}

std::string McpLiveApplication::CurrentChain() const {
  if (config_.retained_run) {
    return config_.retained_run->chain;
  }
  return std::string(ChainKindName(config_.options->chain));
}

std::uint32_t McpLiveApplication::NodeCount() const {
  std::lock_guard<std::mutex> lock(mutex_);
  if (config_.retained_run) {
    return config_.retained_run->node_count;
  }
  return config_.options->nodes;
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
  if (notify && config_.run_stopping) {
    config_.run_stopping();
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
  if (notify && config_.run_stopped) {
    config_.run_stopped();
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
  std::unique_lock<std::mutex> lock(mutex_);
  requests_drained_.wait(lock, [this] { return active_requests_ == 0U; });
}

}  // namespace bbp
