#include "bbp/mcp_registry.h"

#include <array>
#include <boost/json/string.hpp>
#include <stdexcept>
#include <string>
#include <type_traits>
#include <vector>

#include "bbp/chain_kind.h"
#include "bbp/scenario_fields.h"
#include "bbp/simulation_command.h"
#include "bbp/simulation_event_kind.h"
#include "bbp/simulator/workload_kind.h"
#include "bbp/tui_command_parser.h"

namespace bbp {
namespace {

template <typename Enum>
constexpr std::size_t EnumCount(Enum count) {
  return static_cast<std::size_t>(count);
}

constexpr std::array<McpNamedCapability, EnumCount(McpOperationKind::kCount)>
    kOperations = {
        {{"scenario.validate", "Validate a complete typed scenario"},
         {"scenario.resolve", "Resolve defaults into a canonical scenario"},
         {"run.launch", "Launch a validated scenario"},
         {"run.stop", "Stop an active managed run"},
         {"run.clean", "Clean a verified owned run"},
         {"run.replay", "Replay a retained source scenario"},
         {"run.report", "Build an exact retained or live report"},
         {"simulation.command", "Invoke a registered runtime command"},
         {"local.firo_qt_launcher", "Create the owned native Firo-Qt launcher"},
         {"node.add", "Transactionally add and synchronize a base node"},
         {"node.remove", "Gracefully remove an owned node and its resources"},
         {"node.stop", "Gracefully stop a selected owned node"},
         {"node.kill", "Force-kill a selected owned node"},
         {"node.restart", "Restart a selected owned node"},
         {"node.replace", "Transactionally replace a selected owned node"},
         {"wallet.add", "Transactionally initialize and register a wallet"},
         {"wallet.remove", "Remove a registered wallet role"},
         {"role.assign", "Assign a compatible overlapping node role"},
         {"role.remove", "Remove an assigned node role"},
         {"miner.add", "Transactionally add or activate a miner"},
         {"miner.remove", "Deactivate and remove a miner role"},
         {"masternode.add",
          "Fund, register, configure and activate a masternode"},
         {"masternode.remove", "Deactivate and remove a masternode role"},
         {"masternode.restart", "Restart and verify a masternode role"},
         {"workload.start", "Start a bounded registered workload"},
         {"workload.reconfigure",
          "Reconfigure a live workload at an accounting boundary"},
         {"workload.pause", "Pause a live workload"},
         {"workload.resume", "Resume a paused workload"},
         {"workload.stop", "Stop a workload with bounded cancellation"},
         {"instrumentation.start", "Start a typed measurement window"},
         {"instrumentation.reconfigure",
          "Reconfigure active measurement targets and counters"},
         {"instrumentation.stop", "Stop an active measurement window"},
         {"evidence.query", "Query bounded structured run evidence"},
         {"log.query", "Query a bounded historical log range"},
         {"log.follow", "Follow bounded new log records"},
         {"artifact.read", "Read bounded safe run-owned artifact content"},
         {"operation.get", "Read stable operation state and progress"},
         {"operation.cancel", "Request bounded operation cancellation"},
         {"subscription.create", "Create a bounded information subscription"},
         {"subscription.poll", "Poll bounded subscription notifications"},
         {"subscription.cancel", "Cancel a subscription"}}};

constexpr std::array<McpNamedCapability,
                     EnumCount(McpInformationFamily::kCount)>
    kInformationFamilies = {
        {{"capabilities", "Typed BBP and chain capabilities"},
         {"schemas", "Authoritative input and result schemas"},
         {"source_scenario", "Original typed scenario"},
         {"resolved_scenario", "Canonical resolved scenario"},
         {"run_registry", "Known live and retained runs"},
         {"lifecycle", "Run and node lifecycle state"},
         {"nodes", "Node identities and configurations"},
         {"roles", "Node role assignments"},
         {"peers", "Observed and intended peers"},
         {"topology", "Logical and physical topology"},
         {"wallets", "Redacted wallet registry"},
         {"balances", "Exact atomic-unit balances"},
         {"mining", "Mining configuration and state"},
         {"transaction_load", "Exact transaction-load progress and outcomes"},
         {"workloads", "Registered live workload state"},
         {"workload_history", "Retained workload transitions and outcomes"},
         {"processes", "Owned process state"},
         {"namespaces", "Owned namespace state"},
         {"interfaces", "Owned interface and address state"},
         {"qdiscs", "Owned qdisc and filter state"},
         {"cgroups", "Owned cgroup state"},
         {"resource_state", "CPU, memory, IO and PID limits"},
         {"cleanup_state", "Truthful cleanup progress and read-back"},
         {"metrics", "Typed node metrics"},
         {"wallet_metrics", "Typed wallet metrics"},
         {"instrumentation", "Instrumentation targets, counters and windows"},
         {"measurements", "Current typed instrumentation measurements"},
         {"measurement_history", "Bounded synchronized measurement history"},
         {"comparisons", "Time-aligned selected and healthy-peer comparisons"},
         {"events", "Typed simulation events"},
         {"logs", "Bounded redacted logs"},
         {"log_history", "Bounded ranged and followable log history"},
         {"rpc_failures", "Recent structured daemon RPC failures"},
         {"errors", "Structured failures"},
         {"command_history", "Runtime command admission and terminal outcomes"},
         {"reports", "Exact run and node reports"},
         {"generated_commands", "Safe generated operator commands"},
         {"artifacts", "Run-owned artifact inventory"},
         {"artifact_contents", "Safe bounded run-owned artifact contents"},
         {"progress", "Long-operation progress"},
         {"operations", "Stable long-operation terminal state"},
         {"notifications", "Bounded live subscription notifications"}}};

constexpr std::array<McpNamedCapability, EnumCount(McpResultFamily::kCount)>
    kResultFamilies = {
        {{"validation", "Scenario validation result and diagnostics"},
         {"scenario", "Source or canonical resolved scenario"},
         {"run_lifecycle", "Run identity and lifecycle transition"},
         {"runtime_command", "Runtime command admission and outcome"},
         {"mutation", "Transactional node or wallet mutation outcome"},
         {"role_mutation", "Transactional role mutation outcome"},
         {"workload", "Workload identity, state and exact accounting"},
         {"instrumentation", "Instrumentation identity and measurement state"},
         {"evidence_page", "Cursor-bounded structured evidence page"},
         {"artifact_content", "Bounded safe artifact bytes and metadata"},
         {"operation", "Stable long-operation progress and terminal outcome"},
         {"subscription",
          "Subscription identity and bounded notification page"},
         {"cleanup", "Ownership-verified cleanup and kernel read-back"},
         {"error", "Typed structured failure with retained evidence"}}};

const std::vector<std::string> kScenarioMembers = BuildScenarioMemberRegistry();
const std::vector<std::string_view> kScenarioMemberViews = [] {
  std::vector<std::string_view> views;
  views.reserve(kScenarioMembers.size());
  for (const std::string& member : kScenarioMembers) {
    views.push_back(member);
  }
  return views;
}();

static_assert(kOperations.size() == EnumCount(McpOperationKind::kCount));
static_assert(kInformationFamilies.size() ==
              EnumCount(McpInformationFamily::kCount));
static_assert(kResultFamilies.size() == EnumCount(McpResultFamily::kCount));

boost::json::array StringArray(std::span<const std::string_view> values) {
  boost::json::array result;
  result.reserve(values.size());
  for (const std::string_view value : values) {
    result.emplace_back(value);
  }
  return result;
}

boost::json::array NamedOperationArray(
    std::span<const McpOperationKind> operations) {
  boost::json::array result;
  result.reserve(operations.size());
  for (const McpOperationKind operation : operations) {
    const std::size_t index = EnumCount(operation);
    if (index >= kOperations.size()) {
      throw std::logic_error("unknown MCP operation kind");
    }
    result.emplace_back(
        boost::json::object{{"name", kOperations[index].name},
                            {"description", kOperations[index].description}});
  }
  return result;
}

boost::json::array NamedInformationFamilyArray(
    std::span<const McpInformationFamily> information_families) {
  boost::json::array result;
  result.reserve(information_families.size());
  for (const McpInformationFamily family : information_families) {
    const std::size_t index = EnumCount(family);
    if (index >= kInformationFamilies.size()) {
      throw std::logic_error("unknown MCP information family");
    }
    result.emplace_back(boost::json::object{
        {"name", kInformationFamilies[index].name},
        {"description", kInformationFamilies[index].description}});
  }
  return result;
}

boost::json::array NamedResultFamilyArray(
    std::span<const McpOperationKind> operations) {
  std::array<bool, EnumCount(McpResultFamily::kCount)> selected{};
  for (const McpOperationKind operation : operations) {
    selected[EnumCount(McpOperationResultFamily(operation))] = true;
  }
  if (!operations.empty()) {
    selected[EnumCount(McpResultFamily::kOperation)] = true;
    selected[EnumCount(McpResultFamily::kError)] = true;
  }
  boost::json::array result;
  for (std::size_t index = 0U; index < selected.size(); ++index) {
    if (!selected[index]) {
      continue;
    }
    result.emplace_back(boost::json::object{
        {"name", kResultFamilies[index].name},
        {"description", kResultFamilies[index].description}});
  }
  return result;
}

template <typename Enum, typename Name>
boost::json::array EnumNames(Enum count, Name name) {
  boost::json::array values;
  const std::size_t size = EnumCount(count);
  values.reserve(size);
  for (std::size_t index = 0U; index < size; ++index) {
    values.emplace_back(name(static_cast<Enum>(index)));
  }
  return values;
}

}  // namespace

std::string_view McpOperationKindName(McpOperationKind kind) {
  const std::size_t index = EnumCount(kind);
  if (index >= kOperations.size()) {
    throw std::logic_error("unknown MCP operation kind");
  }
  return kOperations[index].name;
}

std::string_view McpInformationFamilyName(McpInformationFamily family) {
  const std::size_t index = EnumCount(family);
  if (index >= kInformationFamilies.size()) {
    throw std::logic_error("unknown MCP information family");
  }
  return kInformationFamilies[index].name;
}

std::string_view McpResultFamilyName(McpResultFamily family) {
  const std::size_t index = EnumCount(family);
  if (index >= kResultFamilies.size()) {
    throw std::logic_error("unknown MCP result family");
  }
  return kResultFamilies[index].name;
}

std::span<const McpNamedCapability> McpOperationRegistry() {
  return kOperations;
}

std::span<const McpNamedCapability> McpInformationFamilyRegistry() {
  return kInformationFamilies;
}

std::span<const McpNamedCapability> McpResultFamilyRegistry() {
  return kResultFamilies;
}

std::span<const std::string_view> McpScenarioMemberRegistry() {
  return kScenarioMemberViews;
}

boost::json::array BuildMcpResourceRegistry() {
  std::array<McpInformationFamily,
             EnumCount(McpInformationFamily::kCount)>
      information_families{};
  for (std::size_t index = 0U; index < information_families.size(); ++index) {
    information_families[index] =
        static_cast<McpInformationFamily>(index);
  }
  return BuildMcpResourceRegistry(information_families);
}

boost::json::array BuildMcpResourceRegistry(
    std::span<const McpInformationFamily> information_families) {
  boost::json::array resources;
  resources.reserve(information_families.size());
  for (const McpInformationFamily family : information_families) {
    const std::size_t index = EnumCount(family);
    if (index >= kInformationFamilies.size()) {
      throw std::logic_error("unknown MCP information family");
    }
    const McpNamedCapability& descriptor = kInformationFamilies[index];
    resources.emplace_back(
        boost::json::object{{"uri", "bbp:///" + std::string(descriptor.name)},
                            {"name", descriptor.name},
                            {"description", descriptor.description},
                            {"mimeType", "application/json"}});
  }
  return resources;
}

boost::json::object BuildMcpCapabilityDocument(
    std::span<const McpOperationKind> operations,
    std::span<const McpInformationFamily> information_families) {
  boost::json::object limits{
      {"list_page_size", kMcpListPageSize},
      {"sessions", kMcpMaximumSessions},
      {"tasks_per_session", kMcpMaximumTasksPerSession},
      {"subscriptions_per_session", kMcpMaximumSubscriptionsPerSession},
      {"notifications_per_session", kMcpMaximumNotificationsPerSession},
      {"retained_operations", kMcpMaximumRetainedOperations},
      {"retained_result_bytes", kMcpMaximumRetainedResultBytes},
      {"evidence_text_bytes", kMcpMaximumEvidenceTextBytes},
      {"selection_items", kMcpMaximumSelectionItems}};
  boost::json::object document;
  document["protocol_version"] = kMcpProtocolVersion;
  document["transport"] = "streamable_http";
  document["authentication"] = "bearer";
  document["chains"] = EnumNames(
      ChainKind::kCount, [](ChainKind kind) { return ChainKindName(kind); });
  document["workloads"] =
      EnumNames(WorkloadKind::kCount,
                [](WorkloadKind kind) { return WorkloadKindName(kind); });
  document["runtime_commands"] =
      EnumNames(SimulationCommandKind::kCount, [](SimulationCommandKind kind) {
        return SimulationCommandKindName(kind);
      });
  document["events"] = EnumNames(
      SimulationEventKind::kCount,
      [](SimulationEventKind kind) { return SimulationEventKindName(kind); });
  document["tui_commands"] = StringArray(TuiCommandParser::CommandNames());
  document["tui_local_actions"] = EnumNames(
      TuiLocalAction::kCount,
      [](TuiLocalAction action) { return TuiLocalActionName(action); });
  document["operations"] = NamedOperationArray(operations);
  document["information_families"] =
      NamedInformationFamilyArray(information_families);
  document["result_families"] = NamedResultFamilyArray(operations);
  document["scenario_schema"] = BuildMcpScenarioSchema();
  document["limits"] = std::move(limits);
  return document;
}

boost::json::object BuildMcpCapabilityDocument(
    std::span<const McpOperationKind> operations) {
  std::array<McpInformationFamily,
             EnumCount(McpInformationFamily::kCount)>
      information_families{};
  for (std::size_t index = 0U; index < information_families.size(); ++index) {
    information_families[index] =
        static_cast<McpInformationFamily>(index);
  }
  return BuildMcpCapabilityDocument(operations, information_families);
}

boost::json::object BuildMcpCapabilityDocument() {
  std::array<McpOperationKind, EnumCount(McpOperationKind::kCount)>
      operations{};
  for (std::size_t index = 0U; index < operations.size(); ++index) {
    operations[index] = static_cast<McpOperationKind>(index);
  }
  return BuildMcpCapabilityDocument(operations);
}

}  // namespace bbp
