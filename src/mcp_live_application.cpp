#include "bbp/mcp_live_application.h"

#include <boost/json/array.hpp>
#include <boost/json/parse.hpp>
#include <boost/json/serialize.hpp>
#include <chrono>
#include <cstdint>
#include <filesystem>
#include <initializer_list>
#include <stdexcept>
#include <string>
#include <utility>

#include "bbp/chain_kind.h"
#include "bbp/mcp_registry.h"
#include "bbp/run_report.h"
#include "bbp/scenario_service.h"
#include "bbp/simulation_command_queue.h"
#include "bbp/simulator/options.h"
#include "bbp/util.h"

namespace bbp {
namespace {

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

boost::json::object BuildSchemaDocument() {
  boost::json::object operation_schemas;
  const std::span<const McpNamedCapability> operations = McpOperationRegistry();
  for (std::size_t index = 0U; index < operations.size(); ++index) {
    const auto kind = static_cast<McpOperationKind>(index);
    operation_schemas[operations[index].name] = boost::json::object{
        {"input", BuildMcpOperationInputSchema(kind)},
        {"output", BuildMcpOperationOutputSchema(kind)},
    };
  }
  return boost::json::object{
      {"scenario", BuildMcpScenarioSchema()},
      {"simulation_command", BuildMcpSimulationCommandSchema()},
      {"operations", std::move(operation_schemas)},
      {"resources", BuildMcpResourceRegistry()},
  };
}

boost::json::value ReadJsonObjectFile(const std::filesystem::path& path) {
  const boost::json::value value = boost::json::parse(ReadText(path));
  if (!value.is_object()) {
    throw std::runtime_error("MCP run document is not a JSON object: " +
                             path.string());
  }
  return value;
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

}  // namespace

McpLiveApplication::McpLiveApplication(Config config)
    : config_(std::move(config)) {
  if (config_.run_id.empty()) {
    throw std::invalid_argument("MCP live application requires a run id");
  }
  if (config_.run_root.empty()) {
    throw std::invalid_argument("MCP live application requires a run root");
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

McpApplicationOperationFactory McpLiveApplication::OperationFactory() {
  return [this](McpOperationKind kind, const boost::json::object& arguments,
                std::string_view session_id) {
    return BuildOperation(kind, arguments, session_id);
  };
}

McpApplicationResourceReader McpLiveApplication::ResourceReader() {
  return [this](McpInformationFamily family, std::string_view session_id,
                std::stop_token stop_token) {
    return ReadResource(family, session_id, stop_token);
  };
}

McpOperationPlan McpLiveApplication::BuildOperation(
    McpOperationKind kind, const boost::json::object& arguments,
    std::string_view session_id) {
  static_cast<void>(session_id);
  if (kind != McpOperationKind::kStopRun &&
      kind != McpOperationKind::kReportRun &&
      kind != McpOperationKind::kInvokeRuntimeCommand) {
    return {};
  }
  RequireRun(arguments);
  {
    std::lock_guard<std::mutex> lock(mutex_);
    if (shutdown_) {
      throw std::runtime_error("MCP live application is shutting down");
    }
  }

  if (kind == McpOperationKind::kStopRun) {
    return McpOperationPlan{
        .progress_total = 1U, .executor = [this](McpOperationContext& context) {
          context.ThrowIfCancelled();
          {
            std::lock_guard<std::mutex> lock(mutex_);
            if (shutdown_) {
              throw McpOperationFailure("run_not_active",
                                        "the managed run is no longer active",
                                        false);
            }
            stop_requested_ = true;
          }
          config_.request_run_stop();
          return McpTypedResult{.family = McpResultFamily::kRunLifecycle,
                                .value = boost::json::object{
                                    {"result_family", "run_lifecycle"},
                                    {"run_id", config_.run_id},
                                    {"state", "stopping"},
                                    {"node_count", config_.options->nodes}}};
        }};
  }

  if (kind == McpOperationKind::kReportRun) {
    return McpOperationPlan{
        .progress_total = 1U, .executor = [this](McpOperationContext& context) {
          context.ThrowIfCancelled();
          boost::json::object report = ReportSnapshot(context.stop_token());
          return McpTypedResult{
              .family = McpResultFamily::kEvidencePage,
              .value =
                  EvidencePage(config_.run_id, McpInformationFamily::kReports,
                               std::move(report))};
        }};
  }

  SimulationCommand command = ParseAndValidateSimulationCommand(
      RequireObject(arguments, "command"), *config_.options);
  return McpOperationPlan{
      .progress_total = 1U,
      .executor = [this, command = std::move(command)](
                      McpOperationContext& context) mutable {
        context.ThrowIfCancelled();
        const std::uint64_t sequence = SubmitCommand(std::move(command));
        const std::optional<std::string> error = WaitForCommand(sequence);
        if (error) {
          throw McpOperationFailure(
              "simulation_command_failed",
              "simulation command #" + std::to_string(sequence) +
                  " failed: " + *error,
              false,
              boost::json::array{boost::json::object{
                  {"code", "simulation_command_failed"},
                  {"message", *error},
                  {"path", "command-" + std::to_string(sequence)},
                  {"recoverable", false}}});
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
    throw std::runtime_error("MCP resource read was cancelled");
  }
  {
    std::lock_guard<std::mutex> lock(mutex_);
    if (shutdown_) {
      throw std::runtime_error("MCP live application is shutting down");
    }
  }

  if (family == McpInformationFamily::kCapabilities) {
    boost::json::object capabilities = BuildMcpCapabilityDocument();
    capabilities["current_run"] =
        boost::json::object{{"run_id", config_.run_id},
                            {"chain", ChainKindName(config_.options->chain)},
                            {"state", RunState()},
                            {"node_count", config_.options->nodes}};
    return ResourceEnvelope(family, config_.run_id, std::move(capabilities));
  }
  if (family == McpInformationFamily::kSchemas) {
    return ResourceEnvelope(family, config_.run_id, BuildSchemaDocument());
  }
  if (family == McpInformationFamily::kResolvedScenario) {
    return ResourceEnvelope(
        family, config_.run_id,
        ReadJsonObjectFile(config_.run_root / "resolved-scenario.json"));
  }
  if (family == McpInformationFamily::kSourceScenario) {
    const std::filesystem::path source =
        config_.run_root / "source-scenario.json";
    if (std::filesystem::exists(source)) {
      return ResourceEnvelope(family, config_.run_id,
                              ReadJsonObjectFile(source));
    }
    return ResourceEnvelope(
        family, config_.run_id,
        boost::json::object{
            {"available", false},
            {"reason", "this run did not retain a source scenario document"}});
  }
  if (family == McpInformationFamily::kRunRegistry) {
    return ResourceEnvelope(
        family, config_.run_id,
        boost::json::array{boost::json::object{
            {"run_id", config_.run_id},
            {"state", RunState()},
            {"chain", ChainKindName(config_.options->chain)},
            {"node_count", config_.options->nodes}}});
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
      data = boost::json::object{{"run_root", config_.run_root.string()},
                                 {"safe_contents_require_artifact_read", true}};
      break;
    case McpInformationFamily::kArtifactContents:
      data = boost::json::object{{"available_through", "artifact.read"}};
      break;
    case McpInformationFamily::kProgress:
    case McpInformationFamily::kOperations:
    case McpInformationFamily::kNotifications:
      data = boost::json::object{
          {"available_through", "operation and subscription tools"}};
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
    throw std::runtime_error("MCP resource read was cancelled");
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
    std::uint64_t sequence) {
  std::unique_lock<std::mutex> lock(mutex_);
  command_outcome_ready_.wait(lock, [&] {
    const auto pending = pending_commands_.find(sequence);
    return shutdown_ || pending == pending_commands_.end() ||
           pending->second.completed;
  });
  const auto pending = pending_commands_.find(sequence);
  if (pending == pending_commands_.end()) {
    throw std::logic_error("MCP simulation command outcome was lost");
  }
  if (!pending->second.completed) {
    pending_commands_.erase(pending);
    throw McpOperationFailure("run_stopped_before_command_outcome",
                              "the run stopped before the command produced a "
                              "terminal outcome",
                              false);
  }
  std::optional<std::string> error = std::move(pending->second.error);
  pending_commands_.erase(pending);
  return error;
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
  std::lock_guard<std::mutex> lock(report_mutex_);
  boost::json::object report = BuildRunReport(config_.run_root);
  if (stop_token.stop_requested()) {
    throw McpOperationCancelled();
  }
  return report;
}

std::string McpLiveApplication::RunState() const {
  std::lock_guard<std::mutex> lock(mutex_);
  if (shutdown_ || stop_requested_) {
    return "stopping";
  }
  return config_.options->nodes == 0U ? "empty" : "active";
}

void McpLiveApplication::Shutdown() {
  std::lock_guard<std::mutex> lock(mutex_);
  if (shutdown_) {
    return;
  }
  shutdown_ = true;
  command_outcome_ready_.notify_all();
}

}  // namespace bbp
