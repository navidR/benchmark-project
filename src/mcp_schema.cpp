#include <array>
#include <boost/json/array.hpp>
#include <boost/json/object.hpp>
#include <cstddef>
#include <cstdint>
#include <initializer_list>
#include <limits>
#include <span>
#include <stdexcept>
#include <string>
#include <string_view>
#include <utility>

#include "bbp/chain_kind.h"
#include "bbp/default_peer_topology.h"
#include "bbp/drivers/chain_driver_registry.h"
#include "bbp/mcp_registry.h"
#include "bbp/node_lifecycle_policy.h"
#include "bbp/perf_counter.h"
#include "bbp/scenario_fields.h"
#include "bbp/simulation_command.h"
#include "bbp/simulation_partition.h"
#include "bbp/simulation_policy.h"
#include "bbp/simulator/wallet_funding_strategy.h"
#include "bbp/simulator/wallet_transactions_workload.h"
#include "bbp/simulator/wallet_transfer_strategy.h"
#include "bbp/simulator/workload_kind.h"

namespace bbp {
namespace {

constexpr std::string_view kJsonSchemaDraft =
    "https://json-schema.org/draft/2020-12/schema";
constexpr std::uint64_t kMaximumUint32 =
    std::numeric_limits<std::uint32_t>::max();
constexpr std::uint64_t kMaximumSafeCollection = 10000U;
constexpr std::size_t kMaximumTextLength = 1U << 20U;

constexpr std::array kPerfCounterKinds{
    PerfCounterKind::kCycles,
    PerfCounterKind::kInstructions,
    PerfCounterKind::kCacheReferences,
    PerfCounterKind::kCacheMisses,
    PerfCounterKind::kBranchInstructions,
    PerfCounterKind::kBranchMisses,
    PerfCounterKind::kContextSwitches,
    PerfCounterKind::kPageFaults,
    PerfCounterKind::kTaskClock,
};
constexpr std::array kPerfCounterTargetKinds{
    PerfCounterTargetKind::kNode,
    PerfCounterTargetKind::kWallet,
    PerfCounterTargetKind::kGroup,
    PerfCounterTargetKind::kCgroup,
};
constexpr std::array kSimulationPartitionScopes{
    SimulationPartitionScope::kNodePair,
    SimulationPartitionScope::kPartitionGroup,
    SimulationPartitionScope::kRegion,
    SimulationPartitionScope::kRole,
};

boost::json::object TypeSchema(std::string_view type) {
  return boost::json::object{{"type", type}};
}

boost::json::object StringSchema(std::size_t minimum_length = 0U) {
  boost::json::object schema{{"type", "string"},
                             {"maxLength", kMaximumTextLength}};
  if (minimum_length != 0U) {
    schema["minLength"] = minimum_length;
  }
  return schema;
}

boost::json::object IdentifierSchema() {
  return boost::json::object{{"type", "string"},
                             {"minLength", 1U},
                             {"maxLength", 128U},
                             {"pattern", "^[A-Za-z0-9][A-Za-z0-9_.-]*$"}};
}

boost::json::object IntegerSchema(std::uint64_t minimum = 0U,
                                  std::uint64_t maximum = kMaximumUint32) {
  return boost::json::object{
      {"type", "integer"}, {"minimum", minimum}, {"maximum", maximum}};
}

boost::json::object Uint64Schema(std::uint64_t minimum = 0U) {
  return boost::json::object{
      {"type", "integer"},
      {"minimum", minimum},
      {"maximum", std::numeric_limits<std::uint64_t>::max()}};
}

boost::json::object NumberSchema(double minimum = 0.0) {
  return boost::json::object{{"type", "number"}, {"minimum", minimum}};
}

boost::json::object ConstStringSchema(std::string_view value) {
  return boost::json::object{{"type", "string"}, {"const", value}};
}

boost::json::object StringEnumSchema(boost::json::array values) {
  return boost::json::object{{"type", "string"}, {"enum", std::move(values)}};
}

template <typename Enum, typename Name>
boost::json::array EnumNames(Enum count, Name name) {
  boost::json::array values;
  const std::size_t size = static_cast<std::size_t>(count);
  values.reserve(size);
  for (std::size_t index = 0U; index < size; ++index) {
    values.emplace_back(name(static_cast<Enum>(index)));
  }
  return values;
}

template <typename Enum, std::size_t Size, typename Name>
boost::json::array EnumNames(const std::array<Enum, Size>& values, Name name) {
  boost::json::array names;
  names.reserve(values.size());
  for (const Enum value : values) {
    names.emplace_back(name(value));
  }
  return names;
}

boost::json::array Required(std::initializer_list<std::string_view> fields) {
  boost::json::array required;
  required.reserve(fields.size());
  for (const std::string_view field : fields) {
    required.emplace_back(field);
  }
  return required;
}

boost::json::object ClosedObject(boost::json::object properties,
                                 boost::json::array required = {}) {
  boost::json::object schema{{"type", "object"},
                             {"properties", std::move(properties)},
                             {"additionalProperties", false}};
  if (!required.empty()) {
    schema["required"] = std::move(required);
  }
  return schema;
}

boost::json::object ArraySchema(
    boost::json::object items, std::size_t minimum_items = 0U,
    std::size_t maximum_items = kMaximumSafeCollection,
    bool unique_items = false) {
  boost::json::object schema{{"type", "array"},
                             {"items", std::move(items)},
                             {"maxItems", maximum_items}};
  if (minimum_items != 0U) {
    schema["minItems"] = minimum_items;
  }
  if (unique_items) {
    schema["uniqueItems"] = true;
  }
  return schema;
}

boost::json::object OneOf(std::initializer_list<boost::json::object> choices) {
  boost::json::array one_of;
  one_of.reserve(choices.size());
  for (const boost::json::object& choice : choices) {
    one_of.push_back(choice);
  }
  return boost::json::object{{"oneOf", std::move(one_of)}};
}

boost::json::object Nullable(boost::json::object schema) {
  return OneOf({std::move(schema), TypeSchema("null")});
}

boost::json::object NodeSelectorSchema() {
  return OneOf({IdentifierSchema(), IntegerSchema(1U)});
}

boost::json::object NodeSelectorArraySchema(std::size_t minimum = 1U) {
  return ArraySchema(NodeSelectorSchema(), minimum, kMaximumSafeCollection,
                     true);
}

boost::json::object Fixed8AmountSchema() {
  return OneOf(
      {Uint64Schema(), NumberSchema(),
       boost::json::object{{"type", "string"},
                           {"minLength", 1U},
                           {"maxLength", 64U},
                           {"pattern", "^[0-9]+(?:\\.[0-9]{0,8}0*)?$"}}});
}

boost::json::object DurationSchema() {
  return boost::json::object{
      {"type", "string"}, {"minLength", 2U}, {"maxLength", 64U}};
}

boost::json::object DistributionSchema() {
  boost::json::object properties;
  properties["distribution"] =
      StringEnumSchema(boost::json::array{"fixed", "uniform"});
  properties["min"] = Fixed8AmountSchema();
  properties["max"] = Fixed8AmountSchema();
  return ClosedObject(std::move(properties),
                      Required({"distribution", "min", "max"}));
}

boost::json::object DistributedAmountSchema() {
  return OneOf({Fixed8AmountSchema(), DistributionSchema()});
}

boost::json::object DistributedDurationSchema() {
  boost::json::object properties;
  properties["distribution"] =
      StringEnumSchema(boost::json::array{"fixed", "uniform"});
  properties["min"] = DurationSchema();
  properties["max"] = DurationSchema();
  return OneOf({DurationSchema(),
                ClosedObject(std::move(properties),
                             Required({"distribution", "min", "max"}))});
}

boost::json::object IoLimitSchema() {
  return BuildMcpScenarioObjectSchema(ScenarioObjectKind::kIoLimit);
}

boost::json::object GenericFieldSchema(std::string_view field) {
  if (field == "enabled" || field == "native_mining" || field == "all_peers" ||
      field == "bidirectional" || field == "active" || field == "isolated" ||
      field == "allow_miner_wallet_overlap" || field == "limit_packets" ||
      field == "generate_blocks" || field == "isolated_network") {
    return TypeSchema("boolean");
  }
  if (field == "probability" || field == "difficulty" ||
      field == "time_scale" || field == "transaction_rate" ||
      field == "bandwidth_mbps" || field == "loss_percent") {
    return NumberSchema();
  }
  if (field == "amount" || field == "fee" || field == "funding_threshold" ||
      field == "min" || field == "max") {
    return Fixed8AmountSchema();
  }
  if (field == "duration" || field == "interval" || field == "at" ||
      field == "start_time" || field == "stop_time" ||
      field == "metrics_interval" || field == "tick_interval" ||
      field == "tui_refresh_interval") {
    return DurationSchema();
  }
  if (field == "nodes" || field == "node_ids" || field == "sender_wallets" ||
      field == "receiver_wallets" || field == "wallets") {
    return NodeSelectorArraySchema();
  }
  if (field == "node" || field == "peer" || field == "funding_node" ||
      field == "submit_node" || field == "from" || field == "to" ||
      field == "center_node" || field == "generate_node" ||
      field == "peer_node_id") {
    return NodeSelectorSchema();
  }
  if (field == "wallet_nodes" || field == "miner_nodes" || field == "group_a" ||
      field == "group_b") {
    return ArraySchema(IntegerSchema(1U), 1U, kMaximumSafeCollection, true);
  }
  if (field == "groups" || field == "regions") {
    return ArraySchema(
        ArraySchema(IntegerSchema(1U), 1U, kMaximumSafeCollection, true), 1U);
  }
  if (field == "group_ids") {
    return ArraySchema(IdentifierSchema(), 1U, kMaximumSafeCollection, true);
  }
  if (field == "latency_matrix_ms") {
    return ArraySchema(ArraySchema(Nullable(IntegerSchema()), 1U), 1U);
  }
  if (field == "extra_args") {
    return ArraySchema(StringSchema(1U));
  }
  if (field == "io_max") {
    return ArraySchema(IoLimitSchema());
  }
  if (field == "perf_counters") {
    return ArraySchema(
        StringEnumSchema(EnumNames(
            kPerfCounterKinds,
            [](PerfCounterKind kind) { return PerfCounterKindName(kind); })),
        1U, kPerfCounterKinds.size(), true);
  }
  if (field == "count" || field == "node_count" ||
      field == "wallet_node_count" || field == "miner_node_count" ||
      field == "average_degree" || field == "attachment_count" ||
      field == "minimum_peer_count" || field == "maximum_peer_count" ||
      field == "block_count" || field == "funding_blocks" ||
      field == "funding_blocks_per_wallet" || field == "transaction_count" ||
      field == "concurrency" || field == "queue_capacity" ||
      field == "sender_wallet_index" || field == "receiver_wallet_index" ||
      field == "from_region" || field == "to_region" || field == "src_port" ||
      field == "dst_port" || field == "handle" || field == "duration_ms" ||
      field == "timeout_sec" || field == "sync_timeout_sec" ||
      field == "ready_timeout_sec" || field == "metrics_sample_count" ||
      field == "metrics_interval_ms" || field == "latency_ms" ||
      field == "delay_ms" || field == "jitter_ms" ||
      field == "loss_basis_points" || field == "duplicate_basis_points" ||
      field == "corrupt_basis_points" || field == "reorder_basis_points" ||
      field == "min_peer_count" || field == "max_peer_count") {
    return IntegerSchema();
  }
  if (field == "height" || field == "peer_count" || field == "period_ms" ||
      field == "readiness_confirmations" || field == "seed" ||
      field == "memory_high_bytes" || field == "memory_max_bytes" ||
      field == "cpu_quota_us" || field == "cpu_period_us" ||
      field == "cpu_weight" || field == "io_weight" || field == "pids_max" ||
      field == "read_bytes_per_sec" || field == "write_bytes_per_sec" ||
      field == "read_operations_per_sec" ||
      field == "write_operations_per_sec") {
    return Uint64Schema();
  }
  if (field == "id" || field == "name" || field == "profile" ||
      field == "run_id") {
    return IdentifierSchema();
  }
  if (field == "cleanup_policy") {
    return StringEnumSchema(
        boost::json::array{CleanupPolicyName(CleanupPolicy::kAutomatic),
                           CleanupPolicyName(CleanupPolicy::kRetainCgroups)});
  }
  if (field == "privilege_mode") {
    return ConstStringSchema(PrivilegeModeName(PrivilegeMode::kDirect));
  }
  if (field == "log_retention_policy") {
    return ConstStringSchema(
        LogRetentionPolicyName(LogRetentionPolicy::kPreserve));
  }
  if (field == "restart_policy") {
    return StringEnumSchema(
        boost::json::array{NodeRestartPolicyName(NodeRestartPolicy::kNever),
                           NodeRestartPolicyName(NodeRestartPolicy::kOnFailure),
                           NodeRestartPolicyName(NodeRestartPolicy::kAlways)});
  }
  if (field == "network") {
    return ConstStringSchema("regtest");
  }
  if (field == "distribution") {
    return StringEnumSchema(boost::json::array{"fixed", "uniform"});
  }
  if (field == "funding_strategy") {
    return StringEnumSchema(boost::json::array{"round_robin", "random"});
  }
  if (field == "strategy") {
    return StringEnumSchema(
        boost::json::array{"driver_rpc", "round_robin", "random", "fanout",
                           "hotspot", "random_bruteforce", "equal_fanout"});
  }
  if (field == "mode") {
    return StringEnumSchema(boost::json::array{"public", "private"});
  }
  if (field == "fee_policy") {
    return ConstStringSchema("fixed");
  }
  if (field == "type" || field == "kind" || field == "scope" ||
      field == "role" || field == "driver" || field == "chain") {
    return StringSchema(1U);
  }
  if (field == "simulation" || field == "chains" || field == "topology" ||
      field == "block_production" || field == "workloads" ||
      field == "events" || field == "resources" ||
      field == "resource_profiles" || field == "process" ||
      field == "network_profiles" || field == "chain_config" ||
      field == "wallet" || field == "initialization" ||
      field == "wallet_initialization" || field == "peer_connectivity" ||
      field == "edges" || field == "region_edges" ||
      field == "runtime_node_limits" || field == "default_condition" ||
      field == "node_conditions" || field == "runtime_node_conditions" ||
      field == "runtime_node_blocks" || field == "runtime_node_unblocks" ||
      field == "runtime_partitions" || field == "runtime_partition_heals" ||
      field == "runtime_node_restarts" || field == "runtime_node_freezes" ||
      field == "resource_limits" || field == "network_condition" ||
      field == "network_flow" || field == "partition" ||
      field == "perf_target" || field == "wallet_send") {
    return TypeSchema("object");
  }
  if (field == "chain_daemon" || field == "output_dir" ||
      field == "default_binary" || field == "binary" || field == "data_dir" ||
      field == "device" || field == "src_address" || field == "dst_address" ||
      field == "source_address" || field == "source_private_key" ||
      field == "destination_address" || field == "private_key" ||
      field == "address") {
    return StringSchema();
  }
  if (field == "memory_high" || field == "memory_max" || field == "cpu_quota" ||
      field == "cpu_max") {
    return StringSchema(1U);
  }
  throw std::logic_error("missing MCP schema for scenario field: " +
                         std::string(field));
}

boost::json::object PropertiesForFields(
    std::span<const std::string_view> fields) {
  boost::json::object properties;
  for (const std::string_view field : fields) {
    properties[field] = GenericFieldSchema(field);
  }
  return properties;
}

boost::json::object NetworkConditionSchema() {
  return BuildMcpScenarioObjectSchema(ScenarioObjectKind::kNetworkCondition);
}

boost::json::object ResourceLimitsSchema() {
  return BuildMcpScenarioObjectSchema(ScenarioObjectKind::kResourceLimits);
}

boost::json::object PatternMapSchema(boost::json::object value_schema) {
  boost::json::object patterns;
  patterns["^[A-Za-z0-9][A-Za-z0-9_.-]*$"] = std::move(value_schema);
  return boost::json::object{{"type", "object"},
                             {"patternProperties", std::move(patterns)},
                             {"additionalProperties", false}};
}

boost::json::object TopologySchema() {
  boost::json::array variants;
  variants.reserve(static_cast<std::size_t>(PeerTopologyKind::kCount));
  for (std::size_t index = 0U;
       index < static_cast<std::size_t>(PeerTopologyKind::kCount); ++index) {
    const auto kind = static_cast<PeerTopologyKind>(index);
    boost::json::object properties =
        PropertiesForFields(ScenarioTopologyCommonFields());
    properties["type"] = ConstStringSchema(PeerTopologyKindName(kind));
    properties["wallet_initialization"] =
        BuildMcpScenarioObjectSchema(ScenarioObjectKind::kWalletInitialization);
    properties["peer_connectivity"] = ArraySchema(
        BuildMcpScenarioObjectSchema(ScenarioObjectKind::kPeerConnectivity));
    for (const std::string_view field : ScenarioTopologyKindFields(kind)) {
      properties[field] = GenericFieldSchema(field);
    }
    if (kind == PeerTopologyKind::kCustomEdgeList) {
      properties["edges"] = ArraySchema(
          BuildMcpScenarioObjectSchema(ScenarioObjectKind::kTopologyEdge));
    } else if (kind == PeerTopologyKind::kInternetLikeRegionGraph) {
      properties["region_edges"] = ArraySchema(BuildMcpScenarioObjectSchema(
          ScenarioObjectKind::kTopologyRegionEdge));
    }
    variants.push_back(
        ClosedObject(std::move(properties), kind == PeerTopologyKind::kFullMesh
                                                ? boost::json::array{}
                                                : Required({"type"})));
  }
  return boost::json::object{{"oneOf", std::move(variants)}};
}

boost::json::object WorkloadVariant(WorkloadKind kind,
                                    std::string_view discriminator,
                                    bool scheduled) {
  boost::json::object properties =
      PropertiesForFields(ScenarioWorkloadFields(kind));
  properties[discriminator] = ConstStringSchema(WorkloadKindName(kind));
  boost::json::array required;
  required.emplace_back(discriminator);
  if (scheduled) {
    properties["at"] = DurationSchema();
    required.emplace_back("at");
  }
  const auto require = [&](std::initializer_list<std::string_view> fields) {
    for (const std::string_view field : fields) {
      required.emplace_back(field);
    }
  };
  switch (kind) {
    case WorkloadKind::kBlockGeneration:
      require({"count"});
      break;
    case WorkloadKind::kWaitUntilHeight:
      require({"height"});
      break;
    case WorkloadKind::kWaitForPeers:
      require({"peer_count"});
      break;
    case WorkloadKind::kConnectPeer:
    case WorkloadKind::kDisconnectPeer:
      require({"peer"});
      break;
    case WorkloadKind::kRestartNode:
      break;
    case WorkloadKind::kFreezeNode:
      require({"duration_ms"});
      break;
    case WorkloadKind::kUpdateResourceLimits:
      break;
    case WorkloadKind::kSetResourceProfile:
    case WorkloadKind::kSetNetworkProfile:
      properties["nodes"] = OneOf(
          {ArraySchema(IdentifierSchema(), 1U, kMaximumSafeCollection, true),
           boost::json::object{
               {"type", "string"},
               {"pattern", "^role:(?:base|node|wallet|miner)$"}}});
      require({"nodes", "profile"});
      break;
    case WorkloadKind::kResourcePressure:
      require({"duration_ms"});
      break;
    case WorkloadKind::kSetNetworkCondition:
      break;
    case WorkloadKind::kBlockNetworkFlow:
    case WorkloadKind::kUnblockNetworkFlow:
      break;
    case WorkloadKind::kPartitionNodes:
    case WorkloadKind::kHealPartition:
      require({"group_a", "group_b"});
      break;
    case WorkloadKind::kSetEdgeCondition:
    case WorkloadKind::kActivateEdge:
    case WorkloadKind::kDeactivateEdge:
    case WorkloadKind::kRestoreEdge:
      require({"from", "to"});
      break;
    case WorkloadKind::kSendRawTransaction:
      require({"source_address", "source_private_key", "destination_address",
               "amount", "fee"});
      break;
    case WorkloadKind::kWalletTransactions:
      properties["funding_strategy"] =
          StringEnumSchema(boost::json::array{"round_robin", "random"});
      properties["strategy"] = StringEnumSchema(
          boost::json::array{"round_robin", "random", "fanout", "hotspot",
                             "random_bruteforce", "equal_fanout"});
      properties["mode"] =
          StringEnumSchema(boost::json::array{"public", "private"});
      properties["fee_policy"] = ConstStringSchema("fixed");
      properties["amount"] = DistributedAmountSchema();
      properties["interval"] = DistributedDurationSchema();
      properties["fee"] = Fixed8AmountSchema();
      properties["funding_threshold"] = Fixed8AmountSchema();
      properties["sender_wallets"] =
          ArraySchema(IntegerSchema(1U), 1U, kMaximumSafeCollection, true);
      properties["receiver_wallets"] =
          ArraySchema(IntegerSchema(1U), 1U, kMaximumSafeCollection, true);
      properties["wallets"] =
          ArraySchema(IntegerSchema(1U), 1U, kMaximumSafeCollection, true);
      require({"amount", "fee"});
      break;
    case WorkloadKind::kCheckpoint:
      break;
    case WorkloadKind::kCount:
      throw std::logic_error("unknown workload kind");
  }
  return ClosedObject(std::move(properties), std::move(required));
}

boost::json::object CommandVariant(SimulationCommandKind kind,
                                   std::string_view discriminator,
                                   bool scheduled) {
  boost::json::object properties =
      PropertiesForFields(ScenarioCommandFields(kind));
  if (ScenarioCommandFieldAllowed(kind, "node")) {
    properties["node"] = NodeSelectorSchema();
  }
  properties[discriminator] =
      ConstStringSchema(SimulationCommandKindName(kind));
  boost::json::array required;
  required.emplace_back(discriminator);
  if (scheduled) {
    properties["at"] = DurationSchema();
    required.emplace_back("at");
  }
  if (ScenarioCommandFieldAllowed(kind, "node")) {
    required.emplace_back("node");
  }
  if (properties.contains("resource_limits")) {
    properties["resource_limits"] = ResourceLimitsSchema();
  }
  if (properties.contains("network_condition")) {
    properties["network_condition"] = NetworkConditionSchema();
  }
  if (properties.contains("network_flow")) {
    properties["network_flow"] =
        BuildMcpScenarioObjectSchema(ScenarioObjectKind::kNetworkFlow);
  }
  if (properties.contains("partition")) {
    properties["partition"] =
        BuildMcpScenarioObjectSchema(ScenarioObjectKind::kSimulationPartition);
  }
  if (properties.contains("perf_target")) {
    properties["perf_target"] =
        BuildMcpScenarioObjectSchema(ScenarioObjectKind::kPerfTarget);
  }
  if (properties.contains("wallet_send")) {
    properties["wallet_send"] =
        BuildMcpScenarioObjectSchema(ScenarioObjectKind::kWalletSend);
  }
  for (const std::string_view field : ScenarioCommandFields(kind)) {
    required.emplace_back(field);
  }
  return ClosedObject(std::move(properties), std::move(required));
}

boost::json::object ScheduledEventSchema() {
  boost::json::array variants;
  variants.reserve(static_cast<std::size_t>(WorkloadKind::kCount) +
                   static_cast<std::size_t>(SimulationCommandKind::kCount));
  for (std::size_t index = 0U;
       index < static_cast<std::size_t>(WorkloadKind::kCount); ++index) {
    variants.push_back(
        WorkloadVariant(static_cast<WorkloadKind>(index), "action", true));
  }
  for (std::size_t index = 0U;
       index < static_cast<std::size_t>(SimulationCommandKind::kCount);
       ++index) {
    variants.push_back(CommandVariant(static_cast<SimulationCommandKind>(index),
                                      "action", true));
  }
  return boost::json::object{{"oneOf", std::move(variants)}};
}

boost::json::object ChainSchema() {
  return StringEnumSchema(EnumNames(
      ChainKind::kCount, [](ChainKind chain) { return ChainKindName(chain); }));
}

boost::json::object RoleSchema() {
  return StringEnumSchema(
      boost::json::array{"base", "wallet", "miner", "masternode"});
}

boost::json::object RunStateSchema() {
  return StringEnumSchema(boost::json::array{"empty", "starting", "active",
                                             "stopping", "stopped", "failed",
                                             "cleaning", "clean"});
}

boost::json::object OperationStateSchema() {
  return StringEnumSchema(boost::json::array{
      "queued", "running", "cancelling", "cancelled", "succeeded", "failed"});
}

boost::json::object WorkloadStateSchema() {
  return StringEnumSchema(boost::json::array{"starting", "running", "paused",
                                             "stopping", "stopped", "failed"});
}

boost::json::object InformationFamilySchema() {
  return StringEnumSchema(
      EnumNames(McpInformationFamily::kCount, [](McpInformationFamily family) {
        return McpInformationFamilyName(family);
      }));
}

boost::json::object ResultFamilyNameSchema() {
  return StringEnumSchema(EnumNames(
      McpResultFamily::kCount,
      [](McpResultFamily family) { return McpResultFamilyName(family); }));
}

boost::json::object BoundedLimitSchema() {
  return IntegerSchema(1U, kMcpListPageSize);
}

boost::json::object CursorSchema() {
  return boost::json::object{{"type", "string"}, {"maxLength", 256U}};
}

boost::json::object DiagnosticSchema() {
  boost::json::object properties;
  properties["code"] = IdentifierSchema();
  properties["message"] = StringSchema(1U);
  properties["path"] = StringSchema();
  properties["recoverable"] = TypeSchema("boolean");
  return ClosedObject(std::move(properties), Required({"code", "message"}));
}

boost::json::object NodeMutationConfigSchema() {
  boost::json::object properties;
  properties["chain"] = ChainSchema();
  properties["count"] = IntegerSchema(1U, kMaximumSafeCollection);
  properties["node_ids"] =
      ArraySchema(IdentifierSchema(), 1U, kMaximumSafeCollection, true);
  properties["binary"] = StringSchema(1U);
  properties["topology"] = TopologySchema();
  properties["resources"] = ResourceLimitsSchema();
  properties["network"] = NetworkConditionSchema();
  properties["ready_timeout_sec"] = IntegerSchema(1U);
  properties["sync_timeout_sec"] = IntegerSchema(1U);
  return ClosedObject(std::move(properties), Required({"chain", "count"}));
}

boost::json::object PerfCounterArraySchema() {
  return ArraySchema(
      StringEnumSchema(EnumNames(
          kPerfCounterKinds,
          [](PerfCounterKind kind) { return PerfCounterKindName(kind); })),
      1U, kPerfCounterKinds.size(), true);
}

boost::json::object InstrumentationTargetSchema() {
  boost::json::object properties;
  properties["kind"] = StringEnumSchema(
      EnumNames(kPerfCounterTargetKinds, [](PerfCounterTargetKind kind) {
        return PerfCounterTargetKindName(kind);
      }));
  properties["id"] = IdentifierSchema();
  properties["node_ids"] =
      ArraySchema(IdentifierSchema(), 1U, kMaximumSafeCollection, true);
  return ClosedObject(std::move(properties),
                      Required({"kind", "id", "node_ids"}));
}

boost::json::object EvidenceRecordSchema() {
  boost::json::object properties;
  properties["family"] = InformationFamilySchema();
  properties["sequence"] = Uint64Schema();
  properties["timestamp_ms"] = Uint64Schema();
  properties["node_id"] = IdentifierSchema();
  properties["kind"] = IdentifierSchema();
  properties["message"] = StringSchema();
  properties["artifact_id"] = IdentifierSchema();
  return ClosedObject(std::move(properties),
                      Required({"family", "sequence", "timestamp_ms"}));
}

boost::json::object ExactAccountingSchema() {
  boost::json::object properties;
  for (const std::string_view field :
       {"planned", "admitted", "attempted", "submitted", "confirmed", "failed",
        "cancelled", "in_flight", "reserved_atomic_units",
        "released_atomic_units"}) {
    properties[field] = Uint64Schema();
  }
  return ClosedObject(
      std::move(properties),
      Required({"planned", "admitted", "attempted", "submitted", "confirmed",
                "failed", "cancelled", "in_flight", "reserved_atomic_units",
                "released_atomic_units"}));
}

boost::json::object AddDraft(boost::json::object schema) {
  schema["$schema"] = kJsonSchemaDraft;
  return schema;
}

}  // namespace

boost::json::object BuildMcpScenarioObjectSchema(ScenarioObjectKind kind) {
  boost::json::object properties =
      PropertiesForFields(ScenarioObjectFields(kind));
  boost::json::array required;
  switch (kind) {
    case ScenarioObjectKind::kRoot:
      break;
    case ScenarioObjectKind::kSimulation:
      break;
    case ScenarioObjectKind::kChainDefinition:
      properties["driver"] =
          StringEnumSchema(EnumNames(ChainKind::kCount, [](ChainKind chain) {
            return ChainKindName(chain);
          }));
      required = Required({"driver", "default_binary"});
      break;
    case ScenarioObjectKind::kNode:
      properties["chain"] =
          StringEnumSchema(EnumNames(ChainKind::kCount, [](ChainKind chain) {
            return ChainKindName(chain);
          }));
      properties["role"] = StringEnumSchema(boost::json::array{
          "base", "node", "wallet", "miner", "wallet_miner"});
      properties["chain_config"] =
          BuildMcpScenarioObjectSchema(ScenarioObjectKind::kNodeChainConfig);
      properties["wallet"] =
          BuildMcpScenarioObjectSchema(ScenarioObjectKind::kNodeWallet);
      properties["resources"] =
          BuildMcpScenarioObjectSchema(ScenarioObjectKind::kNodeProfile);
      properties["network"] =
          BuildMcpScenarioObjectSchema(ScenarioObjectKind::kNodeProfile);
      required = Required({"id", "chain", "role"});
      break;
    case ScenarioObjectKind::kNodeWallet:
      properties["initialization"] = BuildMcpScenarioObjectSchema(
          ScenarioObjectKind::kWalletInitialization);
      break;
    case ScenarioObjectKind::kWalletInitialization:
      properties["strategy"] = ConstStringSchema("driver_rpc");
      properties["mode"] =
          StringEnumSchema(boost::json::array{"public", "private"});
      break;
    case ScenarioObjectKind::kNodeChainConfig:
      properties["network"] = ConstStringSchema("regtest");
      break;
    case ScenarioObjectKind::kNodeProfile:
      required = Required({"profile"});
      break;
    case ScenarioObjectKind::kPeerConnectivity:
      required = Required({"node"});
      break;
    case ScenarioObjectKind::kTopologyEdge:
      required = Required({"from", "to"});
      break;
    case ScenarioObjectKind::kTopologyRegionEdge:
      required = Required({"from_region", "to_region"});
      break;
    case ScenarioObjectKind::kDistribution:
      properties["distribution"] =
          StringEnumSchema(boost::json::array{"fixed", "uniform"});
      required = Required({"distribution", "min", "max"});
      break;
    case ScenarioObjectKind::kIoLimit:
      properties["read_bytes_per_sec"] = Nullable(Uint64Schema(1U));
      properties["write_bytes_per_sec"] = Nullable(Uint64Schema(1U));
      properties["read_operations_per_sec"] = Nullable(Uint64Schema(1U));
      properties["write_operations_per_sec"] = Nullable(Uint64Schema(1U));
      required =
          Required({"device", "read_bytes_per_sec", "write_bytes_per_sec",
                    "read_operations_per_sec", "write_operations_per_sec"});
      break;
    case ScenarioObjectKind::kResourceLimits:
      properties["cpu_quota_us"] = Nullable(Uint64Schema());
      break;
    case ScenarioObjectKind::kResourceProfile:
      properties["cpu_quota_us"] = Nullable(Uint64Schema());
      break;
    case ScenarioObjectKind::kRuntimeResourceLimits:
      properties["cpu_quota_us"] = Nullable(Uint64Schema());
      required = Required({"node"});
      break;
    case ScenarioObjectKind::kNetworkCondition:
      break;
    case ScenarioObjectKind::kNodeNetworkCondition:
      required = Required({"node"});
      break;
    case ScenarioObjectKind::kNetworkBlockRule:
      required = Required({"node", "dst_address", "dst_port"});
      break;
    case ScenarioObjectKind::kNetworkFlow:
      break;
    case ScenarioObjectKind::kNetworkPartition:
      required = Required({"group_a", "group_b"});
      break;
    case ScenarioObjectKind::kSimulationPartitionGroup:
      properties["node_ids"] = NodeSelectorArraySchema();
      properties["group_ids"] =
          ArraySchema(IdentifierSchema(), 1U, kMaximumSafeCollection, true);
      required = Required({"group_ids", "node_ids"});
      break;
    case ScenarioObjectKind::kSimulationPartition:
      properties["scope"] = StringEnumSchema(EnumNames(
          kSimulationPartitionScopes, [](SimulationPartitionScope scope) {
            return SimulationPartitionScopeName(scope);
          }));
      properties["group_a"] = BuildMcpScenarioObjectSchema(
          ScenarioObjectKind::kSimulationPartitionGroup);
      properties["group_b"] = BuildMcpScenarioObjectSchema(
          ScenarioObjectKind::kSimulationPartitionGroup);
      required = Required({"scope", "group_a", "group_b"});
      break;
    case ScenarioObjectKind::kPerfTarget:
      properties["kind"] = StringEnumSchema(
          EnumNames(kPerfCounterTargetKinds, [](PerfCounterTargetKind target) {
            return PerfCounterTargetKindName(target);
          }));
      properties["node_ids"] = NodeSelectorArraySchema();
      required = Required({"kind", "id", "node_ids"});
      break;
    case ScenarioObjectKind::kWalletSend:
      properties["amount"] = Fixed8AmountSchema();
      properties["fee"] = Fixed8AmountSchema();
      required = Required({"sender_wallet_index", "receiver_wallet_index",
                           "amount", "fee", "timeout_sec"});
      break;
    case ScenarioObjectKind::kBlockProduction:
      properties["difficulty"] = Nullable(NumberSchema());
      break;
    case ScenarioObjectKind::kResources:
      properties["cpu_quota_us"] = Nullable(Uint64Schema());
      properties["runtime_node_limits"] =
          ArraySchema(BuildMcpScenarioObjectSchema(
              ScenarioObjectKind::kRuntimeResourceLimits));
      break;
    case ScenarioObjectKind::kNetwork:
      properties["default_condition"] = NetworkConditionSchema();
      properties["node_conditions"] = ArraySchema(BuildMcpScenarioObjectSchema(
          ScenarioObjectKind::kNodeNetworkCondition));
      properties["runtime_node_conditions"] =
          ArraySchema(BuildMcpScenarioObjectSchema(
              ScenarioObjectKind::kNodeNetworkCondition));
      properties["runtime_node_blocks"] = ArraySchema(
          BuildMcpScenarioObjectSchema(ScenarioObjectKind::kNetworkBlockRule));
      properties["runtime_node_unblocks"] = ArraySchema(
          BuildMcpScenarioObjectSchema(ScenarioObjectKind::kNetworkBlockRule));
      properties["runtime_partitions"] = ArraySchema(
          BuildMcpScenarioObjectSchema(ScenarioObjectKind::kNetworkPartition));
      properties["runtime_partition_heals"] = ArraySchema(
          BuildMcpScenarioObjectSchema(ScenarioObjectKind::kNetworkPartition));
      break;
    case ScenarioObjectKind::kProcess:
      properties["runtime_node_restarts"] = ArraySchema(
          BuildMcpScenarioObjectSchema(ScenarioObjectKind::kProcessRestart));
      properties["runtime_node_freezes"] = ArraySchema(
          BuildMcpScenarioObjectSchema(ScenarioObjectKind::kProcessFreeze));
      break;
    case ScenarioObjectKind::kProcessRestart:
      required = Required({"node"});
      break;
    case ScenarioObjectKind::kProcessFreeze:
      required = Required({"node", "duration_ms"});
      break;
    case ScenarioObjectKind::kCount:
      throw std::logic_error("unknown scenario object kind");
  }
  return ClosedObject(std::move(properties), std::move(required));
}

boost::json::object BuildMcpWorkloadSchema() {
  boost::json::array variants;
  variants.reserve(static_cast<std::size_t>(WorkloadKind::kCount));
  for (std::size_t index = 0U;
       index < static_cast<std::size_t>(WorkloadKind::kCount); ++index) {
    variants.push_back(
        WorkloadVariant(static_cast<WorkloadKind>(index), "type", false));
  }
  return AddDraft(boost::json::object{{"oneOf", std::move(variants)}});
}

boost::json::object BuildMcpSimulationCommandSchema() {
  boost::json::array variants;
  variants.reserve(static_cast<std::size_t>(SimulationCommandKind::kCount));
  for (std::size_t index = 0U;
       index < static_cast<std::size_t>(SimulationCommandKind::kCount);
       ++index) {
    variants.push_back(CommandVariant(static_cast<SimulationCommandKind>(index),
                                      "kind", false));
  }
  return AddDraft(boost::json::object{{"oneOf", std::move(variants)}});
}

boost::json::object BuildMcpScenarioSchema() {
  boost::json::object schema =
      BuildMcpScenarioObjectSchema(ScenarioObjectKind::kRoot);
  boost::json::object& properties = schema.at("properties").as_object();
  properties["chain"] = StringEnumSchema(EnumNames(
      ChainKind::kCount, [](ChainKind chain) { return ChainKindName(chain); }));
  properties["simulation"] =
      BuildMcpScenarioObjectSchema(ScenarioObjectKind::kSimulation);
  boost::json::object chain_definitions;
  for (std::size_t index = 0U;
       index < static_cast<std::size_t>(ChainKind::kCount); ++index) {
    const auto chain = static_cast<ChainKind>(index);
    boost::json::object definition =
        BuildMcpScenarioObjectSchema(ScenarioObjectKind::kChainDefinition);
    definition.at("properties").as_object()["driver"] =
        ConstStringSchema(ChainKindName(chain));
    chain_definitions[ChainKindName(chain)] = std::move(definition);
  }
  properties["chains"] =
      boost::json::object{{"type", "object"},
                          {"properties", std::move(chain_definitions)},
                          {"minProperties", 1U},
                          {"additionalProperties", false}};
  properties["topology"] = TopologySchema();
  properties["nodes"] =
      OneOf({IntegerSchema(1U),
             ArraySchema(
                 BuildMcpScenarioObjectSchema(ScenarioObjectKind::kNode), 1U)});
  properties["block_production"] =
      BuildMcpScenarioObjectSchema(ScenarioObjectKind::kBlockProduction);
  properties["workloads"] = ArraySchema(BuildMcpWorkloadSchema());
  properties["events"] = ArraySchema(ScheduledEventSchema());
  properties["resources"] =
      BuildMcpScenarioObjectSchema(ScenarioObjectKind::kResources);
  properties["resource_profiles"] = PatternMapSchema(
      BuildMcpScenarioObjectSchema(ScenarioObjectKind::kResourceProfile));
  properties["process"] =
      BuildMcpScenarioObjectSchema(ScenarioObjectKind::kProcess);
  properties["network"] =
      BuildMcpScenarioObjectSchema(ScenarioObjectKind::kNetwork);
  properties["network_profiles"] = PatternMapSchema(NetworkConditionSchema());
  for (std::size_t index = 0U;
       index < static_cast<std::size_t>(ChainKind::kCount); ++index) {
    const auto chain = static_cast<ChainKind>(index);
    properties[ChainDriverSpecFor(chain).daemon_scenario_field] =
        StringSchema(1U);
  }
  boost::json::array daemon_alias_rules;
  const auto append_alias_rule = [&](boost::json::object condition,
                                     ChainKind active_chain) {
    boost::json::array forbidden;
    const std::string_view active_alias =
        ChainDriverSpecFor(active_chain).daemon_scenario_field;
    for (std::size_t index = 0U;
         index < static_cast<std::size_t>(ChainKind::kCount); ++index) {
      const auto chain = static_cast<ChainKind>(index);
      if (chain != active_chain) {
        forbidden.emplace_back(boost::json::object{
            {"required",
             Required({ChainDriverSpecFor(chain).daemon_scenario_field})}});
      }
    }
    forbidden.emplace_back(boost::json::object{
        {"required", Required({"chain_daemon", active_alias})}});
    daemon_alias_rules.emplace_back(boost::json::object{
        {"if", std::move(condition)},
        {"then", boost::json::object{
                     {"not", boost::json::object{{"anyOf", forbidden}}}}}});
  };
  for (std::size_t index = 0U;
       index < static_cast<std::size_t>(ChainKind::kCount); ++index) {
    const auto chain = static_cast<ChainKind>(index);
    append_alias_rule(
        boost::json::object{
            {"properties",
             boost::json::object{
                 {"chain", ConstStringSchema(ChainKindName(chain))}}},
            {"required", Required({"chain"})}},
        chain);

    boost::json::object inferred_chains{
        {"required", Required({ChainKindName(chain)})}, {"maxProperties", 1U}};
    append_alias_rule(
        boost::json::object{
            {"not", boost::json::object{{"required", Required({"chain"})}}},
            {"properties",
             boost::json::object{{"chains", std::move(inferred_chains)}}},
            {"required", Required({"chains"})}},
        chain);
  }
  append_alias_rule(
      boost::json::object{
          {"not",
           boost::json::object{
               {"anyOf",
                boost::json::array{
                    boost::json::object{{"required", Required({"chain"})}},
                    boost::json::object{
                        {"required", Required({"chains"})}}}}}}},
      ChainKind::kFiro);
  boost::json::array legacy_daemon_presence;
  legacy_daemon_presence.emplace_back(
      boost::json::object{{"required", Required({"chain_daemon"})}});
  for (std::size_t index = 0U;
       index < static_cast<std::size_t>(ChainKind::kCount); ++index) {
    legacy_daemon_presence.emplace_back(boost::json::object{
        {"required", Required({ChainDriverSpecFor(static_cast<ChainKind>(index))
                                   .daemon_scenario_field})}});
  }
  daemon_alias_rules.emplace_back(boost::json::object{
      {"not", boost::json::object{
                  {"allOf",
                   boost::json::array{
                       boost::json::object{{"required", Required({"chains"})}},
                       boost::json::object{
                           {"anyOf", std::move(legacy_daemon_presence)}}}}}}});
  schema["allOf"] = std::move(daemon_alias_rules);
  schema["$schema"] = kJsonSchemaDraft;
  schema["x-bbp-members"] = boost::json::array();
  boost::json::array& members = schema.at("x-bbp-members").as_array();
  for (const std::string_view member : McpScenarioMemberRegistry()) {
    members.emplace_back(member);
  }
  schema["x-bbp-workload-kinds"] =
      EnumNames(WorkloadKind::kCount,
                [](WorkloadKind kind) { return WorkloadKindName(kind); });
  return schema;
}

McpResultFamily McpOperationResultFamily(McpOperationKind operation) {
  switch (operation) {
    case McpOperationKind::kValidateScenario:
      return McpResultFamily::kValidation;
    case McpOperationKind::kResolveScenario:
      return McpResultFamily::kScenario;
    case McpOperationKind::kLaunchRun:
    case McpOperationKind::kStopRun:
    case McpOperationKind::kReplayRun:
      return McpResultFamily::kRunLifecycle;
    case McpOperationKind::kCleanRun:
      return McpResultFamily::kCleanup;
    case McpOperationKind::kReportRun:
    case McpOperationKind::kQueryEvidence:
    case McpOperationKind::kQueryLogs:
    case McpOperationKind::kFollowLogs:
      return McpResultFamily::kEvidencePage;
    case McpOperationKind::kInvokeRuntimeCommand:
      return McpResultFamily::kRuntimeCommand;
    case McpOperationKind::kCreateFiroQtLauncher:
    case McpOperationKind::kAddNode:
    case McpOperationKind::kRemoveNode:
    case McpOperationKind::kStopNode:
    case McpOperationKind::kKillNode:
    case McpOperationKind::kRestartNode:
    case McpOperationKind::kReplaceNode:
    case McpOperationKind::kAddWallet:
    case McpOperationKind::kRemoveWallet:
      return McpResultFamily::kMutation;
    case McpOperationKind::kAssignRole:
    case McpOperationKind::kRemoveRole:
    case McpOperationKind::kAddMiner:
    case McpOperationKind::kRemoveMiner:
    case McpOperationKind::kAddMasternode:
    case McpOperationKind::kRemoveMasternode:
    case McpOperationKind::kRestartMasternode:
      return McpResultFamily::kRoleMutation;
    case McpOperationKind::kStartWorkload:
    case McpOperationKind::kReconfigureWorkload:
    case McpOperationKind::kPauseWorkload:
    case McpOperationKind::kResumeWorkload:
    case McpOperationKind::kStopWorkload:
      return McpResultFamily::kWorkload;
    case McpOperationKind::kStartInstrumentation:
    case McpOperationKind::kReconfigureInstrumentation:
    case McpOperationKind::kStopInstrumentation:
      return McpResultFamily::kInstrumentation;
    case McpOperationKind::kReadArtifact:
      return McpResultFamily::kArtifactContent;
    case McpOperationKind::kGetOperation:
    case McpOperationKind::kCancelOperation:
      return McpResultFamily::kOperation;
    case McpOperationKind::kCreateSubscription:
    case McpOperationKind::kPollSubscription:
    case McpOperationKind::kCancelSubscription:
      return McpResultFamily::kSubscription;
    case McpOperationKind::kCount:
      break;
  }
  throw std::logic_error("unknown MCP operation kind");
}

boost::json::object BuildMcpOperationInputSchema(McpOperationKind operation) {
  boost::json::object properties;
  boost::json::array required;
  const auto add_run = [&] {
    properties["run_id"] = IdentifierSchema();
    required.emplace_back("run_id");
  };
  const auto add_node = [&] {
    properties["node_id"] = IdentifierSchema();
    required.emplace_back("node_id");
  };
  const auto add_timeout = [&] {
    properties["timeout_sec"] = IntegerSchema(1U);
  };
  switch (operation) {
    case McpOperationKind::kValidateScenario:
    case McpOperationKind::kResolveScenario:
    case McpOperationKind::kLaunchRun:
      properties["scenario"] = BuildMcpScenarioSchema();
      required.emplace_back("scenario");
      break;
    case McpOperationKind::kStopRun:
      add_run();
      add_timeout();
      break;
    case McpOperationKind::kCleanRun:
      add_run();
      add_timeout();
      properties["remove_retained_artifacts"] = TypeSchema("boolean");
      break;
    case McpOperationKind::kReplayRun:
      properties["source_run_id"] = IdentifierSchema();
      properties["run_id"] = IdentifierSchema();
      required.emplace_back("source_run_id");
      break;
    case McpOperationKind::kReportRun:
      add_run();
      properties["node_ids"] =
          ArraySchema(IdentifierSchema(), 1U, kMaximumSafeCollection, true);
      properties["include_artifacts"] = TypeSchema("boolean");
      break;
    case McpOperationKind::kInvokeRuntimeCommand:
      add_run();
      properties["command"] = BuildMcpSimulationCommandSchema();
      required.emplace_back("command");
      break;
    case McpOperationKind::kCreateFiroQtLauncher:
      add_run();
      add_node();
      break;
    case McpOperationKind::kAddNode:
      add_run();
      properties["request"] = NodeMutationConfigSchema();
      required.emplace_back("request");
      break;
    case McpOperationKind::kRemoveNode:
    case McpOperationKind::kStopNode:
    case McpOperationKind::kKillNode:
    case McpOperationKind::kRestartNode:
      add_run();
      add_node();
      add_timeout();
      break;
    case McpOperationKind::kReplaceNode:
      add_run();
      add_node();
      properties["replacement"] = NodeMutationConfigSchema();
      required.emplace_back("replacement");
      add_timeout();
      break;
    case McpOperationKind::kAddWallet:
      add_run();
      properties["node_id"] = IdentifierSchema();
      properties["count"] = IntegerSchema(1U, kMaximumSafeCollection);
      properties["mode"] =
          StringEnumSchema(boost::json::array{"public", "private"});
      properties["create_node"] = NodeMutationConfigSchema();
      properties["readiness_confirmations"] = Uint64Schema();
      required.emplace_back("count");
      required.emplace_back("mode");
      add_timeout();
      break;
    case McpOperationKind::kRemoveWallet:
      add_run();
      add_node();
      break;
    case McpOperationKind::kAssignRole:
    case McpOperationKind::kRemoveRole:
      add_run();
      properties["node_ids"] =
          ArraySchema(IdentifierSchema(), 1U, kMaximumSafeCollection, true);
      properties["roles"] = ArraySchema(RoleSchema(), 1U, 4U, true);
      required.emplace_back("node_ids");
      required.emplace_back("roles");
      add_timeout();
      break;
    case McpOperationKind::kAddMiner:
      add_run();
      properties["node_ids"] =
          ArraySchema(IdentifierSchema(), 1U, kMaximumSafeCollection, true);
      properties["count"] = IntegerSchema(1U, kMaximumSafeCollection);
      properties["create_nodes"] = NodeMutationConfigSchema();
      properties["wallet_node_id"] = IdentifierSchema();
      required.emplace_back("count");
      add_timeout();
      break;
    case McpOperationKind::kRemoveMiner:
      add_run();
      properties["node_ids"] =
          ArraySchema(IdentifierSchema(), 1U, kMaximumSafeCollection, true);
      required.emplace_back("node_ids");
      add_timeout();
      break;
    case McpOperationKind::kAddMasternode:
      add_run();
      properties["node_ids"] =
          ArraySchema(IdentifierSchema(), 1U, kMaximumSafeCollection, true);
      properties["count"] = IntegerSchema(1U, kMaximumSafeCollection);
      properties["create_nodes"] = NodeMutationConfigSchema();
      properties["funding_wallet_id"] = IdentifierSchema();
      required.emplace_back("count");
      required.emplace_back("funding_wallet_id");
      add_timeout();
      break;
    case McpOperationKind::kRemoveMasternode:
    case McpOperationKind::kRestartMasternode:
      add_run();
      properties["node_ids"] =
          ArraySchema(IdentifierSchema(), 1U, kMaximumSafeCollection, true);
      required.emplace_back("node_ids");
      add_timeout();
      break;
    case McpOperationKind::kStartWorkload:
      add_run();
      properties["workload_id"] = IdentifierSchema();
      properties["workload"] = BuildMcpWorkloadSchema();
      required.emplace_back("workload");
      break;
    case McpOperationKind::kReconfigureWorkload:
      add_run();
      properties["workload_id"] = IdentifierSchema();
      properties["workload"] = BuildMcpWorkloadSchema();
      required.emplace_back("workload_id");
      required.emplace_back("workload");
      break;
    case McpOperationKind::kPauseWorkload:
    case McpOperationKind::kResumeWorkload:
    case McpOperationKind::kStopWorkload:
      add_run();
      properties["workload_id"] = IdentifierSchema();
      required.emplace_back("workload_id");
      add_timeout();
      break;
    case McpOperationKind::kStartInstrumentation:
      add_run();
      properties["instrumentation_id"] = IdentifierSchema();
      properties["targets"] = ArraySchema(InstrumentationTargetSchema(), 1U);
      properties["counters"] = PerfCounterArraySchema();
      properties["sample_interval"] = DurationSchema();
      properties["window"] = DurationSchema();
      required.emplace_back("targets");
      required.emplace_back("counters");
      break;
    case McpOperationKind::kReconfigureInstrumentation:
      add_run();
      properties["instrumentation_id"] = IdentifierSchema();
      properties["targets"] = ArraySchema(InstrumentationTargetSchema(), 1U);
      properties["counters"] = PerfCounterArraySchema();
      properties["sample_interval"] = DurationSchema();
      required.emplace_back("instrumentation_id");
      required.emplace_back("targets");
      required.emplace_back("counters");
      break;
    case McpOperationKind::kStopInstrumentation:
      add_run();
      properties["instrumentation_id"] = IdentifierSchema();
      required.emplace_back("instrumentation_id");
      add_timeout();
      break;
    case McpOperationKind::kQueryEvidence:
      add_run();
      properties["families"] = ArraySchema(
          InformationFamilySchema(), 1U,
          static_cast<std::size_t>(McpInformationFamily::kCount), true);
      properties["node_ids"] =
          ArraySchema(IdentifierSchema(), 1U, kMaximumSafeCollection, true);
      properties["cursor"] = CursorSchema();
      properties["limit"] = BoundedLimitSchema();
      required.emplace_back("families");
      break;
    case McpOperationKind::kQueryLogs:
    case McpOperationKind::kFollowLogs:
      add_run();
      properties["node_ids"] =
          ArraySchema(IdentifierSchema(), 1U, kMaximumSafeCollection, true);
      properties["cursor"] = CursorSchema();
      properties["start_sequence"] = Uint64Schema();
      properties["end_sequence"] = Uint64Schema();
      properties["limit"] = BoundedLimitSchema();
      required.emplace_back("node_ids");
      break;
    case McpOperationKind::kReadArtifact:
      add_run();
      properties["artifact_id"] = IdentifierSchema();
      properties["offset"] = Uint64Schema();
      properties["limit"] = IntegerSchema(1U, 1U << 20U);
      required.emplace_back("artifact_id");
      break;
    case McpOperationKind::kGetOperation:
    case McpOperationKind::kCancelOperation:
      properties["operation_id"] = IdentifierSchema();
      required.emplace_back("operation_id");
      break;
    case McpOperationKind::kCreateSubscription:
      add_run();
      properties["families"] = ArraySchema(
          InformationFamilySchema(), 1U,
          static_cast<std::size_t>(McpInformationFamily::kCount), true);
      properties["node_ids"] =
          ArraySchema(IdentifierSchema(), 1U, kMaximumSafeCollection, true);
      properties["cursor"] = CursorSchema();
      required.emplace_back("families");
      break;
    case McpOperationKind::kPollSubscription:
      properties["subscription_id"] = IdentifierSchema();
      properties["cursor"] = CursorSchema();
      properties["limit"] = BoundedLimitSchema();
      properties["timeout_sec"] = IntegerSchema(0U, 60U);
      required.emplace_back("subscription_id");
      break;
    case McpOperationKind::kCancelSubscription:
      properties["subscription_id"] = IdentifierSchema();
      required.emplace_back("subscription_id");
      break;
    case McpOperationKind::kCount:
      throw std::logic_error("unknown MCP operation kind");
  }
  return AddDraft(ClosedObject(std::move(properties), std::move(required)));
}

boost::json::object BuildMcpResultSchema(McpResultFamily family) {
  boost::json::object properties;
  boost::json::array constraints;
  properties["result_family"] = ConstStringSchema(McpResultFamilyName(family));
  boost::json::array required = Required({"result_family"});
  const auto require = [&](std::initializer_list<std::string_view> fields) {
    for (const std::string_view field : fields) {
      required.emplace_back(field);
    }
  };
  switch (family) {
    case McpResultFamily::kValidation:
      properties["valid"] = TypeSchema("boolean");
      properties["diagnostics"] = ArraySchema(DiagnosticSchema());
      require({"valid", "diagnostics"});
      break;
    case McpResultFamily::kScenario:
      properties["scenario"] = BuildMcpScenarioSchema();
      require({"scenario"});
      break;
    case McpResultFamily::kRunLifecycle:
      properties["run_id"] = IdentifierSchema();
      properties["state"] = RunStateSchema();
      properties["operation_id"] = IdentifierSchema();
      properties["node_count"] = IntegerSchema();
      require({"run_id", "state"});
      break;
    case McpResultFamily::kRuntimeCommand:
      properties["run_id"] = IdentifierSchema();
      properties["command_id"] = IdentifierSchema();
      properties["operation_id"] = IdentifierSchema();
      properties["accepted"] = TypeSchema("boolean");
      properties["state"] = OperationStateSchema();
      require({"run_id", "command_id", "accepted", "state"});
      break;
    case McpResultFamily::kMutation:
      properties["run_id"] = IdentifierSchema();
      properties["operation_id"] = IdentifierSchema();
      properties["added_node_ids"] =
          ArraySchema(IdentifierSchema(), 0U, kMaximumSafeCollection, true);
      properties["removed_node_ids"] =
          ArraySchema(IdentifierSchema(), 0U, kMaximumSafeCollection, true);
      properties["unchanged"] = TypeSchema("boolean");
      require({"run_id", "added_node_ids", "removed_node_ids", "unchanged"});
      break;
    case McpResultFamily::kRoleMutation:
      properties["run_id"] = IdentifierSchema();
      properties["operation_id"] = IdentifierSchema();
      properties["node_ids"] =
          ArraySchema(IdentifierSchema(), 1U, kMaximumSafeCollection, true);
      properties["assigned_roles"] = ArraySchema(RoleSchema(), 0U, 4U, true);
      properties["removed_roles"] = ArraySchema(RoleSchema(), 0U, 4U, true);
      require({"run_id", "node_ids", "assigned_roles", "removed_roles"});
      break;
    case McpResultFamily::kWorkload:
      properties["run_id"] = IdentifierSchema();
      properties["workload_id"] = IdentifierSchema();
      properties["operation_id"] = IdentifierSchema();
      properties["state"] = WorkloadStateSchema();
      properties["accounting"] = ExactAccountingSchema();
      require({"run_id", "workload_id", "state", "accounting"});
      break;
    case McpResultFamily::kInstrumentation:
      properties["run_id"] = IdentifierSchema();
      properties["instrumentation_id"] = IdentifierSchema();
      properties["operation_id"] = IdentifierSchema();
      properties["state"] = OperationStateSchema();
      properties["sample_count"] = Uint64Schema();
      properties["targets"] = ArraySchema(InstrumentationTargetSchema(), 1U);
      require(
          {"run_id", "instrumentation_id", "state", "sample_count", "targets"});
      break;
    case McpResultFamily::kEvidencePage:
      properties["run_id"] = IdentifierSchema();
      properties["items"] =
          ArraySchema(EvidenceRecordSchema(), 0U, kMcpListPageSize);
      properties["next_cursor"] = CursorSchema();
      properties["truncated"] = TypeSchema("boolean");
      require({"run_id", "items", "next_cursor", "truncated"});
      break;
    case McpResultFamily::kArtifactContent:
      properties["run_id"] = IdentifierSchema();
      properties["artifact_id"] = IdentifierSchema();
      properties["offset"] = Uint64Schema();
      properties["size"] = Uint64Schema();
      properties["encoding"] = ConstStringSchema("base64");
      properties["content"] = boost::json::object{
          {"type", "string"}, {"maxLength", (1U << 20U) * 2U}};
      properties["next_offset"] = Uint64Schema();
      properties["eof"] = TypeSchema("boolean");
      require({"run_id", "artifact_id", "offset", "size", "encoding", "content",
               "next_offset", "eof"});
      break;
    case McpResultFamily::kOperation:
      properties["operation_id"] = IdentifierSchema();
      properties["operation"] = StringEnumSchema(
          EnumNames(McpOperationKind::kCount, [](McpOperationKind operation) {
            return McpOperationKindName(operation);
          }));
      properties["state"] = OperationStateSchema();
      properties["progress_completed"] = Uint64Schema();
      properties["progress_total"] = Uint64Schema();
      properties["cancel_requested"] = TypeSchema("boolean");
      properties["terminal_result_family"] = ResultFamilyNameSchema();
      require({"operation_id", "operation", "state", "progress_completed",
               "progress_total", "cancel_requested", "terminal_result_family"});
      {
        boost::json::array terminal_results;
        for (std::size_t index = 0U;
             index < static_cast<std::size_t>(McpResultFamily::kCount);
             ++index) {
          const auto result_family = static_cast<McpResultFamily>(index);
          if (result_family == McpResultFamily::kError) {
            continue;
          }
          if (result_family == McpResultFamily::kOperation) {
            terminal_results.emplace_back(
                AddDraft(ClosedObject(properties, required)));
          } else {
            terminal_results.emplace_back(BuildMcpResultSchema(result_family));
          }
        }
        properties["terminal_result"] =
            boost::json::object{{"oneOf", std::move(terminal_results)}};
        properties["terminal_error"] =
            BuildMcpResultSchema(McpResultFamily::kError);
        constraints.emplace_back(boost::json::object{
            {"if", boost::json::object{{"properties",
                                        boost::json::object{
                                            {"state", ConstStringSchema(
                                                          "succeeded")}}}}},
            {"then",
             boost::json::object{
                 {"required", Required({"terminal_result"})},
                 {"not", boost::json::object{
                             {"required", Required({"terminal_error"})}}}}}});
        constraints.emplace_back(boost::json::object{
            {"if",
             boost::json::object{
                 {"properties",
                  boost::json::object{
                      {"state", StringEnumSchema(boost::json::array{
                                    "failed", "cancelled"})}}}}},
            {"then",
             boost::json::object{
                 {"required", Required({"terminal_error"})},
                 {"not", boost::json::object{
                             {"required", Required({"terminal_result"})}}}}}});
        constraints.emplace_back(boost::json::object{
            {"if",
             boost::json::object{
                 {"properties",
                  boost::json::object{
                      {"state", StringEnumSchema(boost::json::array{
                                    "queued", "running", "cancelling"})}}}}},
            {"then",
             boost::json::object{
                 {"not",
                  boost::json::object{
                      {"anyOf",
                       boost::json::array{
                           boost::json::object{
                               {"required", Required({"terminal_result"})}},
                           boost::json::object{
                               {"required",
                                Required({"terminal_error"})}}}}}}}}});
      }
      break;
    case McpResultFamily::kSubscription:
      properties["subscription_id"] = IdentifierSchema();
      properties["items"] = ArraySchema(EvidenceRecordSchema(), 0U,
                                        kMcpMaximumNotificationsPerSession);
      properties["next_cursor"] = CursorSchema();
      properties["dropped"] = Uint64Schema();
      properties["active"] = TypeSchema("boolean");
      require({"subscription_id", "items", "next_cursor", "dropped", "active"});
      break;
    case McpResultFamily::kCleanup:
      properties["run_id"] = IdentifierSchema();
      properties["operation_id"] = IdentifierSchema();
      properties["verified_owned"] = TypeSchema("boolean");
      properties["processes_remaining"] = IntegerSchema();
      properties["network_resources_remaining"] = IntegerSchema();
      properties["cgroups_remaining"] = IntegerSchema();
      properties["credentials_remaining"] = IntegerSchema();
      properties["complete"] = TypeSchema("boolean");
      require({"run_id", "verified_owned", "processes_remaining",
               "network_resources_remaining", "cgroups_remaining",
               "credentials_remaining", "complete"});
      break;
    case McpResultFamily::kError:
      properties["operation_id"] = IdentifierSchema();
      properties["code"] = IdentifierSchema();
      properties["message"] = StringSchema(1U);
      properties["retryable"] = TypeSchema("boolean");
      properties["diagnostics"] = ArraySchema(DiagnosticSchema());
      require({"code", "message", "retryable", "diagnostics"});
      break;
    case McpResultFamily::kCount:
      throw std::logic_error("unknown MCP result family");
  }
  boost::json::object schema =
      ClosedObject(std::move(properties), std::move(required));
  if (!constraints.empty()) {
    schema["allOf"] = std::move(constraints);
  }
  return AddDraft(std::move(schema));
}

boost::json::object BuildMcpOperationOutputSchema(McpOperationKind operation) {
  boost::json::array choices;
  const McpResultFamily result_family = McpOperationResultFamily(operation);
  choices.emplace_back(BuildMcpResultSchema(result_family));
  if (result_family != McpResultFamily::kOperation) {
    // Long actions return their stable operation immediately; callers then
    // use operation.get/subscriptions for progress and the typed terminal
    // result. Fast application services may still return the direct family.
    choices.emplace_back(BuildMcpResultSchema(McpResultFamily::kOperation));
  }
  if (result_family != McpResultFamily::kError) {
    choices.emplace_back(BuildMcpResultSchema(McpResultFamily::kError));
  }
  return AddDraft(boost::json::object{{"oneOf", std::move(choices)}});
}

boost::json::array BuildMcpToolRegistry() {
  const std::span<const McpNamedCapability> operations = McpOperationRegistry();
  boost::json::array tools;
  tools.reserve(operations.size());
  for (std::size_t index = 0U; index < operations.size(); ++index) {
    const McpOperationKind operation = static_cast<McpOperationKind>(index);
    tools.emplace_back(boost::json::object{
        {"name", operations[index].name},
        {"description", operations[index].description},
        {"inputSchema", BuildMcpOperationInputSchema(operation)},
        {"outputSchema", BuildMcpOperationOutputSchema(operation)},
        {"execution", boost::json::object{{"taskSupport", "optional"}}}});
  }
  return tools;
}

}  // namespace bbp
