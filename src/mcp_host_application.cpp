#include "bbp/mcp_host_application.h"

#include <algorithm>
#include <array>
#include <boost/json/array.hpp>
#include <boost/json/value.hpp>
#include <limits>
#include <span>
#include <stdexcept>
#include <utility>

#include "bbp/drivers/chain_driver_registry.h"
#include "bbp/mcp_live_application.h"
#include "bbp/mcp_registry.h"

namespace bbp {
namespace {

constexpr std::array kHostOperations = {
    McpOperationKind::kValidateScenario,
    McpOperationKind::kResolveScenario,
    McpOperationKind::kLaunchRun,
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

const boost::json::value& RequireMember(const boost::json::object& object,
                                        std::string_view name) {
  const boost::json::value* value = object.if_contains(name);
  if (value == nullptr) {
    throw std::invalid_argument("missing MCP host argument: " +
                                std::string(name));
  }
  return *value;
}

const boost::json::object& RequireObject(const boost::json::object& object,
                                         std::string_view name) {
  const boost::json::value& value = RequireMember(object, name);
  if (!value.is_object()) {
    throw std::invalid_argument("MCP host argument " + std::string(name) +
                                " must be an object");
  }
  return value.as_object();
}

std::string RequireString(const boost::json::object& object,
                          std::string_view name) {
  const boost::json::value& value = RequireMember(object, name);
  if (!value.is_string() || value.as_string().empty()) {
    throw std::invalid_argument("MCP host argument " + std::string(name) +
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
  throw std::invalid_argument("MCP host argument " + std::string(name) +
                              " must be an unsigned integer");
}

boost::json::object RunLifecycleJson(const McpRunLifecycleResult& result) {
  return boost::json::object{{"result_family", "run_lifecycle"},
                             {"run_id", result.run_id},
                             {"state", result.state},
                             {"node_count", result.node_count}};
}

boost::json::object RunSnapshotJson(const McpHostedRunSnapshot& snapshot) {
  return boost::json::object{
      {"generation", snapshot.generation},
      {"run_id", snapshot.run_id},
      {"state", snapshot.state},
      {"chain", snapshot.chain},
      {"node_count", snapshot.node_count},
      {"node_capacity", snapshot.node_capacity},
      {"chain_node_maximum", snapshot.chain_node_maximum},
      {"available_node_capacity", snapshot.available_node_capacity}};
}

boost::json::object ResourceEnvelope(std::string_view host_id,
                                     const McpHostedRunSnapshot* run,
                                     McpInformationFamily family,
                                     boost::json::value data) {
  boost::json::object result{
      {"family", McpInformationFamilyName(family)},
      {"host_id", host_id},
      {"data", std::move(data)},
  };
  result["run_id"] = run == nullptr ? boost::json::value(nullptr)
                                    : boost::json::value(run->run_id);
  return result;
}

boost::json::object BuildSchemaDocument(
    std::span<const McpOperationKind> operations,
    std::span<const McpInformationFamily> information_families) {
  boost::json::object operation_schemas;
  for (const McpOperationKind operation : operations) {
    operation_schemas[McpOperationKindName(operation)] = boost::json::object{
        {"input",
         BuildMcpOperationInputSchema(operation, information_families)},
        {"output", BuildMcpOperationOutputSchema(operation, operations)}};
  }
  return boost::json::object{
      {"scenario", BuildMcpScenarioSchema()},
      {"simulation_command", BuildMcpSimulationCommandSchema()},
      {"operations", std::move(operation_schemas)},
      {"resources", BuildMcpResourceRegistry(information_families)},
  };
}

}  // namespace

McpHostApplication::McpHostApplication(Config config)
    : config_(std::move(config)) {
  if (config_.host_id.empty()) {
    throw std::invalid_argument("MCP host application requires a host id");
  }
  if (!config_.snapshot_run || !config_.launch_run || !config_.stop_run) {
    throw std::invalid_argument(
        "MCP host application requires lifecycle callbacks");
  }
}

McpHostApplication::~McpHostApplication() { Shutdown(); }

McpApplicationOperationFactory McpHostApplication::OperationFactory() {
  return [this](McpOperationKind kind, const boost::json::object& arguments,
                std::string_view session_id) {
    return BuildOperation(kind, arguments, session_id);
  };
}

McpApplicationResourceReader McpHostApplication::ResourceReader() {
  return [this](McpInformationFamily family, std::string_view session_id,
                std::stop_token stop_token) {
    return ReadResource(family, session_id, stop_token);
  };
}

std::vector<McpOperationKind> McpHostApplication::SupportedOperations() const {
  return {kHostOperations.begin(), kHostOperations.end()};
}

std::vector<McpInformationFamily>
McpHostApplication::SupportedInformationFamilies() const {
  std::vector<McpInformationFamily> result;
  result.reserve(static_cast<std::size_t>(McpInformationFamily::kCount));
  for (std::size_t index = 0U;
       index < static_cast<std::size_t>(McpInformationFamily::kCount);
       ++index) {
    result.push_back(static_cast<McpInformationFamily>(index));
  }
  return result;
}

McpOperationPlan McpHostApplication::BuildOperation(
    McpOperationKind kind, const boost::json::object& arguments,
    std::string_view session_id) {
  RequireRunning();
  const std::vector<McpOperationKind> supported = SupportedOperations();
  if (std::find(supported.begin(), supported.end(), kind) == supported.end()) {
    throw McpOperationFailure(
        "operation_unavailable",
        "the operation is unavailable in the current BBP host", false);
  }

  if (kind == McpOperationKind::kLaunchRun) {
    const boost::json::object scenario = RequireObject(arguments, "scenario");
    return McpOperationPlan{
        .progress_total = 1U,
        .executor = [this, scenario](McpOperationContext& context) {
          context.ThrowIfCancelled();
          return McpTypedResult{.family = McpResultFamily::kRunLifecycle,
                                .value = RunLifecycleJson(config_.launch_run(
                                    scenario, context.stop_token()))};
        }};
  }

  if (kind == McpOperationKind::kStopRun) {
    const std::string run_id = RequireString(arguments, "run_id");
    const std::uint64_t timeout_seconds =
        OptionalUnsigned(arguments, "timeout_sec", 30U);
    if (timeout_seconds == 0U || timeout_seconds > 3600U ||
        timeout_seconds >
            static_cast<std::uint64_t>(
                std::numeric_limits<std::chrono::seconds::rep>::max())) {
      throw std::invalid_argument(
          "MCP run.stop timeout_sec must be in 1..3600");
    }
    return McpOperationPlan{
        .progress_total = 1U,
        .executor = [this, run_id,
                     timeout = std::chrono::seconds(timeout_seconds)](
                        McpOperationContext& context) {
          context.ThrowIfCancelled();
          return McpTypedResult{.family = McpResultFamily::kRunLifecycle,
                                .value = RunLifecycleJson(config_.stop_run(
                                    run_id, timeout, context.stop_token()))};
        }};
  }

  const std::optional<McpHostedRunSnapshot> run = config_.snapshot_run();
  if (!run || !run->application) {
    throw McpOperationFailure("run_not_active",
                              "this operation requires an active managed run",
                              true);
  }
  if (run->state == "starting") {
    throw McpOperationFailure("run_not_ready",
                              "the managed run is still starting", true);
  }
  McpOperationPlan delegated =
      run->application->OperationFactory()(kind, arguments, session_id);
  if (delegated.executor) {
    McpOperationExecutor executor = std::move(delegated.executor);
    const std::shared_ptr<McpLiveApplication> keep_alive = run->application;
    delegated.executor = [keep_alive, executor = std::move(executor)](
                             McpOperationContext& context) mutable {
      static_cast<void>(keep_alive);
      return executor(context);
    };
  }
  return delegated;
}

boost::json::value McpHostApplication::ReadResource(
    McpInformationFamily family, std::string_view session_id,
    std::stop_token stop_token) {
  RequireRunning();
  if (stop_token.stop_requested()) {
    throw McpOperationCancelled();
  }
  const std::optional<McpHostedRunSnapshot> run = config_.snapshot_run();
  const std::vector<McpOperationKind> operations = SupportedOperations();
  const std::vector<McpInformationFamily> information_families =
      SupportedInformationFamilies();

  if (family == McpInformationFamily::kCapabilities) {
    boost::json::object capabilities =
        BuildMcpCapabilityDocument(operations, information_families);
    capabilities["access_mode"] = "read_write";
    capabilities["lifetime"] = "bbp_process";
    boost::json::object chain_limits;
    for (std::size_t index = 0U;
         index < static_cast<std::size_t>(ChainKind::kCount); ++index) {
      const auto chain = static_cast<ChainKind>(index);
      chain_limits[ChainKindName(chain)] = ChainDriverSpecFor(chain).max_nodes;
    }
    capabilities["chain_node_maximums"] = std::move(chain_limits);
    capabilities["current_run"] =
        run ? boost::json::value(RunSnapshotJson(*run))
            : boost::json::value(nullptr);
    return ResourceEnvelope(config_.host_id, run ? &*run : nullptr, family,
                            std::move(capabilities));
  }
  if (family == McpInformationFamily::kSchemas) {
    return ResourceEnvelope(
        config_.host_id, run ? &*run : nullptr, family,
        BuildSchemaDocument(operations, information_families));
  }
  if (family == McpInformationFamily::kRunRegistry) {
    boost::json::array runs;
    if (run) {
      runs.emplace_back(RunSnapshotJson(*run));
    }
    return ResourceEnvelope(config_.host_id, run ? &*run : nullptr, family,
                            std::move(runs));
  }
  if (family == McpInformationFamily::kLifecycle && !run) {
    return ResourceEnvelope(
        config_.host_id, nullptr, family,
        boost::json::object{{"run_state", "empty"},
                            {"nodes", boost::json::array{}}});
  }
  if (family == McpInformationFamily::kProgress ||
      family == McpInformationFamily::kOperations) {
    return ResourceEnvelope(
        config_.host_id, run ? &*run : nullptr, family,
        boost::json::object{
            {"available_through",
             boost::json::array{"operation.get", "operation.cancel"}}});
  }
  if (family == McpInformationFamily::kNotifications) {
    return ResourceEnvelope(config_.host_id, run ? &*run : nullptr, family,
                            BuildMcpNotificationDiscovery(operations));
  }

  if (!run || !run->application) {
    throw McpOperationFailure(
        "run_not_active",
        "this information resource requires an active managed run", true);
  }
  if (run->state == "starting") {
    throw McpOperationFailure("run_not_ready",
                              "the managed run is still starting", true);
  }
  return run->application->ResourceReader()(family, session_id, stop_token);
}

void McpHostApplication::RequireRunning() const {
  std::lock_guard<std::mutex> lock(mutex_);
  if (shutdown_) {
    throw std::runtime_error("MCP host application is shutting down");
  }
}

void McpHostApplication::Shutdown() {
  std::lock_guard<std::mutex> lock(mutex_);
  shutdown_ = true;
}

}  // namespace bbp
