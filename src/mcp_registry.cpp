#include "bbp/mcp_registry.h"

#include <array>
#include <boost/json/string.hpp>
#include <stdexcept>
#include <string>
#include <type_traits>

#include "bbp/chain_kind.h"
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

constexpr auto kScenarioMembers = std::to_array<std::string_view>(
    {"simulation",
     "simulation.name",
     "simulation.seed",
     "simulation.duration",
     "simulation.time_scale",
     "simulation.cleanup_policy",
     "simulation.privilege_mode",
     "simulation.log_retention_policy",
     "simulation.metrics_interval",
     "simulation.tick_interval",
     "simulation.output_dir",
     "simulation.tui_refresh_interval",
     "chains",
     "chains.*.driver",
     "chains.*.default_binary",
     "chain",
     "chain_daemon",
     "firod",
     "bitcoind",
     "monerod",
     "output_dir",
     "run_id",
     "topology",
     "topology.node_count",
     "topology.wallet_nodes",
     "topology.miner_nodes",
     "topology.allow_miner_wallet_overlap",
     "topology.wallet_initialization",
     "topology.wallet_initialization.strategy",
     "topology.wallet_initialization.mode",
     "topology.peer_topology",
     "topology.peer_topology.kind",
     "topology.peer_topology.seed",
     "topology.peer_topology.edge_probability",
     "topology.peer_topology.attachment_edges",
     "topology.peer_topology.groups",
     "topology.peer_topology.regions",
     "topology.peer_topology.latency_matrix_ms",
     "topology.edges",
     "topology.edges[].from",
     "topology.edges[].to",
     "topology.edges[].bidirectional",
     "topology.edges[].active",
     "topology.edges[].latency_ms",
     "topology.edges[].bandwidth_mbps",
     "topology.edges[].delay_ms",
     "topology.edges[].jitter_ms",
     "topology.edges[].loss_basis_points",
     "topology.edges[].loss_percent",
     "topology.edges[].duplicate_basis_points",
     "topology.edges[].corrupt_basis_points",
     "topology.edges[].reorder_basis_points",
     "topology.edges[].limit_packets",
     "topology.peer_connectivity",
     "topology.peer_connectivity[].node",
     "topology.peer_connectivity[].all_peers",
     "topology.peer_connectivity[].min_peer_count",
     "topology.peer_connectivity[].max_peer_count",
     "nodes",
     "node_count",
     "nodes[].id",
     "nodes[].chain",
     "nodes[].binary",
     "nodes[].data_dir",
     "nodes[].chain_config",
     "nodes[].chain_config.network",
     "nodes[].chain_config.extra_args",
     "nodes[].wallet",
     "nodes[].wallet.enabled",
     "nodes[].wallet.mode",
     "nodes[].wallet.initialization_strategy",
     "nodes[].start_time",
     "nodes[].stop_time",
     "nodes[].restart_policy",
     "nodes[].role",
     "nodes[].resources",
     "nodes[].network",
     "block_production",
     "block_production.enabled",
     "block_production.native_mining",
     "block_production.period_ms",
     "block_production.probability",
     "block_production.seed",
     "block_production.difficulty",
     "generate_node",
     "ready_timeout_sec",
     "sync_timeout_sec",
     "metrics_sample_count",
     "metrics_interval_ms",
     "isolated_network",
     "workloads",
     "workloads[].type",
     "events",
     "events[].at",
     "events[].action",
     "resources",
     "resources.memory_high_bytes",
     "resources.memory_max_bytes",
     "resources.cpu_quota_us",
     "resources.cpu_period_us",
     "resources.cpu_weight",
     "resources.io_weight",
     "resources.io_max",
     "resources.pids_max",
     "resources.runtime_node_limits",
     "resource_profiles",
     "process",
     "process.runtime_node_restarts",
     "process.runtime_node_freezes",
     "network",
     "network.isolated",
     "network.default_condition",
     "network.node_conditions",
     "network.runtime_node_conditions",
     "network.runtime_node_blocks",
     "network.runtime_node_unblocks",
     "network.runtime_partitions",
     "network.runtime_partition_heals",
     "network_profiles",
     "generate_blocks",
     "network_condition.bandwidth_mbps",
     "network_condition.delay_ms",
     "network_condition.jitter_ms",
     "network_condition.loss_basis_points",
     "network_condition.loss_percent",
     "network_condition.duplicate_basis_points",
     "network_condition.corrupt_basis_points",
     "network_condition.reorder_basis_points",
     "network_condition.limit_packets",
     "distribution.kind",
     "distribution.value",
     "distribution.minimum",
     "distribution.maximum",
     "io_max[].device",
     "io_max[].read_bytes_per_sec",
     "io_max[].write_bytes_per_sec",
     "io_max[].read_operations_per_sec",
     "io_max[].write_operations_per_sec"});

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

boost::json::array NamedArray(std::span<const McpNamedCapability> values) {
  boost::json::array result;
  result.reserve(values.size());
  for (const McpNamedCapability& value : values) {
    result.emplace_back(boost::json::object{
        {"name", value.name}, {"description", value.description}});
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

boost::json::object StringEnumSchema(boost::json::array values) {
  return boost::json::object{{"type", "string"}, {"enum", std::move(values)}};
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
  return kScenarioMembers;
}

boost::json::object BuildMcpScenarioSchema() {
  boost::json::object properties;
  properties["chain"] = StringEnumSchema(EnumNames(
      ChainKind::kCount, [](ChainKind kind) { return ChainKindName(kind); }));
  properties["simulation"] = boost::json::object{{"type", "object"}};
  properties["chains"] = boost::json::object{{"type", "object"}};
  properties["topology"] = boost::json::object{{"type", "object"}};
  properties["nodes"] = boost::json::object{{"type", "array"}};
  properties["workloads"] = boost::json::object{{"type", "array"}};
  properties["events"] = boost::json::object{{"type", "array"}};
  properties["resources"] = boost::json::object{{"type", "object"}};
  properties["network"] = boost::json::object{{"type", "object"}};
  boost::json::object schema{
      {"$schema", "https://json-schema.org/draft/2020-12/schema"},
      {"type", "object"},
      {"properties", std::move(properties)},
      {"additionalProperties", false}};
  schema["x-bbp-members"] = StringArray(kScenarioMembers);
  schema["x-bbp-workload-kinds"] =
      EnumNames(WorkloadKind::kCount,
                [](WorkloadKind kind) { return WorkloadKindName(kind); });
  return schema;
}

boost::json::array BuildMcpToolRegistry() {
  boost::json::array tools;
  tools.reserve(kOperations.size());
  for (const McpNamedCapability& operation : kOperations) {
    boost::json::object input_schema{
        {"$schema", "https://json-schema.org/draft/2020-12/schema"},
        {"type", "object"}};
    if (operation.name == "simulation.command") {
      boost::json::object properties;
      properties["kind"] = StringEnumSchema(EnumNames(
          SimulationCommandKind::kCount, [](SimulationCommandKind kind) {
            return SimulationCommandKindName(kind);
          }));
      input_schema["properties"] = std::move(properties);
      input_schema["required"] = boost::json::array{"kind"};
    } else if (operation.name == "scenario.validate" ||
               operation.name == "scenario.resolve" ||
               operation.name == "run.launch") {
      input_schema["properties"] =
          boost::json::object{{"scenario", BuildMcpScenarioSchema()}};
      input_schema["required"] = boost::json::array{"scenario"};
    }
    input_schema["additionalProperties"] = true;
    tools.emplace_back(boost::json::object{
        {"name", operation.name},
        {"description", operation.description},
        {"inputSchema", std::move(input_schema)},
        {"execution", boost::json::object{{"taskSupport", "optional"}}}});
  }
  return tools;
}

boost::json::array BuildMcpResourceRegistry() {
  boost::json::array resources;
  resources.reserve(kInformationFamilies.size());
  for (const McpNamedCapability& family : kInformationFamilies) {
    resources.emplace_back(
        boost::json::object{{"uri", "bbp:///" + std::string(family.name)},
                            {"name", family.name},
                            {"description", family.description},
                            {"mimeType", "application/json"}});
  }
  return resources;
}

boost::json::object BuildMcpCapabilityDocument() {
  boost::json::object limits{
      {"list_page_size", kMcpListPageSize},
      {"sessions", kMcpMaximumSessions},
      {"tasks_per_session", kMcpMaximumTasksPerSession},
      {"subscriptions_per_session", kMcpMaximumSubscriptionsPerSession},
      {"notifications_per_session", kMcpMaximumNotificationsPerSession},
      {"retained_operations", kMcpMaximumRetainedOperations}};
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
  document["operations"] = NamedArray(kOperations);
  document["information_families"] = NamedArray(kInformationFamilies);
  document["result_families"] = NamedArray(kResultFamilies);
  document["scenario_schema"] = BuildMcpScenarioSchema();
  document["limits"] = std::move(limits);
  return document;
}

}  // namespace bbp
