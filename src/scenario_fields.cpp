#include "bbp/scenario_fields.h"

#include <algorithm>
#include <array>
#include <set>
#include <stdexcept>
#include <utility>

#include "bbp/chain_kind.h"
#include "bbp/drivers/chain_driver_registry.h"

namespace bbp {
namespace {

template <typename Range>
bool Contains(const Range& range, std::string_view value) {
  return std::find(range.begin(), range.end(), value) != range.end();
}

template <typename... Values>
constexpr auto Fields(Values... values) {
  return std::array<std::string_view, sizeof...(Values)>{values...};
}

}  // namespace

std::string_view ScenarioObjectKindName(ScenarioObjectKind kind) {
  switch (kind) {
    case ScenarioObjectKind::kRoot:
      return "scenario";
    case ScenarioObjectKind::kSimulation:
      return "simulation";
    case ScenarioObjectKind::kChainDefinition:
      return "chain_definition";
    case ScenarioObjectKind::kNode:
      return "node";
    case ScenarioObjectKind::kNodeWallet:
      return "node_wallet";
    case ScenarioObjectKind::kWalletInitialization:
      return "wallet_initialization";
    case ScenarioObjectKind::kNodeChainConfig:
      return "node_chain_config";
    case ScenarioObjectKind::kNodeProfile:
      return "node_profile";
    case ScenarioObjectKind::kPeerConnectivity:
      return "peer_connectivity";
    case ScenarioObjectKind::kTopologyEdge:
      return "topology_edge";
    case ScenarioObjectKind::kTopologyRegionEdge:
      return "topology_region_edge";
    case ScenarioObjectKind::kDistribution:
      return "distribution";
    case ScenarioObjectKind::kIoLimit:
      return "io_limit";
    case ScenarioObjectKind::kResourceLimits:
      return "resource_limits";
    case ScenarioObjectKind::kResourceProfile:
      return "resource_profile";
    case ScenarioObjectKind::kRuntimeResourceLimits:
      return "runtime_resource_limits";
    case ScenarioObjectKind::kNetworkCondition:
      return "network_condition";
    case ScenarioObjectKind::kNodeNetworkCondition:
      return "node_network_condition";
    case ScenarioObjectKind::kNetworkBlockRule:
      return "network_block_rule";
    case ScenarioObjectKind::kNetworkFlow:
      return "network_flow";
    case ScenarioObjectKind::kNetworkPartition:
      return "network_partition";
    case ScenarioObjectKind::kSimulationPartitionGroup:
      return "simulation_partition_group";
    case ScenarioObjectKind::kSimulationPartition:
      return "simulation_partition";
    case ScenarioObjectKind::kPerfTarget:
      return "perf_target";
    case ScenarioObjectKind::kWalletSend:
      return "wallet_send";
    case ScenarioObjectKind::kBlockProduction:
      return "block_production";
    case ScenarioObjectKind::kResources:
      return "resources";
    case ScenarioObjectKind::kNetwork:
      return "network";
    case ScenarioObjectKind::kProcess:
      return "process";
    case ScenarioObjectKind::kProcessRestart:
      return "process_restart";
    case ScenarioObjectKind::kProcessFreeze:
      return "process_freeze";
    case ScenarioObjectKind::kCount:
      break;
  }
  throw std::logic_error("unknown scenario object kind");
}

std::span<const std::string_view> ScenarioObjectFields(
    ScenarioObjectKind kind) {
  static constexpr auto kRoot =
      Fields("simulation", "chains", "chain", "chain_daemon", "output_dir",
             "run_id", "topology", "nodes", "node_count", "block_production",
             "generate_node", "ready_timeout_sec", "sync_timeout_sec",
             "metrics_sample_count", "metrics_interval_ms", "isolated_network",
             "workloads", "events", "resources", "resource_profiles", "process",
             "network", "network_profiles", "generate_blocks");
  static constexpr auto kSimulation =
      Fields("name", "seed", "duration", "time_scale", "cleanup_policy",
             "privilege_mode", "log_retention_policy", "metrics_interval",
             "tick_interval", "output_dir", "tui_refresh_interval");
  static constexpr auto kChainDefinition = Fields("driver", "default_binary");
  static constexpr auto kNode = Fields(
      "id", "chain", "role", "binary", "data_dir", "resources", "network",
      "chain_config", "wallet", "start_time", "stop_time", "restart_policy");
  static constexpr auto kNodeWallet = Fields("enabled", "initialization");
  static constexpr auto kWalletInitialization = Fields("strategy", "mode");
  static constexpr auto kNodeChainConfig = Fields("network", "extra_args");
  static constexpr auto kNodeProfile = Fields("profile");
  static constexpr auto kPeerConnectivity =
      Fields("node", "all_peers", "min_peer_count", "max_peer_count");
  static constexpr auto kTopologyEdge =
      Fields("from", "to", "bidirectional", "active", "latency_ms",
             "bandwidth_mbps", "delay_ms", "jitter_ms", "loss_basis_points",
             "loss_percent", "duplicate_basis_points", "corrupt_basis_points",
             "reorder_basis_points", "limit_packets");
  static constexpr auto kTopologyRegionEdge =
      Fields("from_region", "to_region", "bidirectional", "active",
             "latency_ms", "bandwidth_mbps", "delay_ms", "jitter_ms",
             "loss_basis_points", "loss_percent", "duplicate_basis_points",
             "corrupt_basis_points", "reorder_basis_points", "limit_packets");
  static constexpr auto kDistribution = Fields("distribution", "min", "max");
  static constexpr auto kIoLimit =
      Fields("device", "read_bytes_per_sec", "write_bytes_per_sec",
             "read_operations_per_sec", "write_operations_per_sec");
  static constexpr auto kResourceLimits =
      Fields("memory_high_bytes", "memory_max_bytes", "cpu_quota_us",
             "cpu_period_us", "cpu_weight", "io_weight", "io_max", "pids_max");
  static constexpr auto kResourceProfile =
      Fields("memory_high", "memory_max", "cpu_quota", "cpu_max",
             "memory_high_bytes", "memory_max_bytes", "cpu_quota_us",
             "cpu_period_us", "cpu_weight", "io_weight", "io_max", "pids_max");
  static constexpr auto kRuntimeResourceLimits =
      Fields("node", "memory_high_bytes", "memory_max_bytes", "cpu_quota_us",
             "cpu_period_us", "cpu_weight", "io_weight", "io_max", "pids_max");
  static constexpr auto kNetworkCondition =
      Fields("bandwidth_mbps", "delay_ms", "jitter_ms", "loss_basis_points",
             "loss_percent", "duplicate_basis_points", "corrupt_basis_points",
             "reorder_basis_points", "limit_packets");
  static constexpr auto kNodeNetworkCondition =
      Fields("node", "bandwidth_mbps", "delay_ms", "jitter_ms",
             "loss_basis_points", "loss_percent", "duplicate_basis_points",
             "corrupt_basis_points", "reorder_basis_points", "limit_packets");
  static constexpr auto kNetworkBlockRule = Fields(
      "node", "src_address", "src_port", "dst_address", "dst_port", "handle");
  static constexpr auto kNetworkFlow =
      Fields("src_address", "src_port", "dst_address", "dst_port", "handle");
  static constexpr auto kNetworkPartition = Fields("group_a", "group_b");
  static constexpr auto kSimulationPartitionGroup =
      Fields("group_ids", "node_ids");
  static constexpr auto kSimulationPartition =
      Fields("scope", "group_a", "group_b");
  static constexpr auto kPerfTarget = Fields("kind", "id", "node_ids");
  static constexpr auto kWalletSend =
      Fields("sender_wallet_index", "receiver_wallet_index", "amount", "fee",
             "timeout_sec");
  static constexpr auto kBlockProduction =
      Fields("enabled", "native_mining", "period_ms", "probability", "seed",
             "difficulty");
  static constexpr auto kResources = Fields(
      "memory_high_bytes", "memory_max_bytes", "cpu_quota_us", "cpu_period_us",
      "cpu_weight", "io_weight", "io_max", "pids_max", "runtime_node_limits");
  static constexpr auto kNetwork = Fields(
      "isolated", "default_condition", "node_conditions",
      "runtime_node_conditions", "runtime_node_blocks", "runtime_node_unblocks",
      "runtime_partitions", "runtime_partition_heals");
  static constexpr auto kProcess =
      Fields("runtime_node_restarts", "runtime_node_freezes");
  static constexpr auto kProcessRestart = Fields("node");
  static constexpr auto kProcessFreeze = Fields("node", "duration_ms");

  switch (kind) {
    case ScenarioObjectKind::kRoot:
      return kRoot;
    case ScenarioObjectKind::kSimulation:
      return kSimulation;
    case ScenarioObjectKind::kChainDefinition:
      return kChainDefinition;
    case ScenarioObjectKind::kNode:
      return kNode;
    case ScenarioObjectKind::kNodeWallet:
      return kNodeWallet;
    case ScenarioObjectKind::kWalletInitialization:
      return kWalletInitialization;
    case ScenarioObjectKind::kNodeChainConfig:
      return kNodeChainConfig;
    case ScenarioObjectKind::kNodeProfile:
      return kNodeProfile;
    case ScenarioObjectKind::kPeerConnectivity:
      return kPeerConnectivity;
    case ScenarioObjectKind::kTopologyEdge:
      return kTopologyEdge;
    case ScenarioObjectKind::kTopologyRegionEdge:
      return kTopologyRegionEdge;
    case ScenarioObjectKind::kDistribution:
      return kDistribution;
    case ScenarioObjectKind::kIoLimit:
      return kIoLimit;
    case ScenarioObjectKind::kResourceLimits:
      return kResourceLimits;
    case ScenarioObjectKind::kResourceProfile:
      return kResourceProfile;
    case ScenarioObjectKind::kRuntimeResourceLimits:
      return kRuntimeResourceLimits;
    case ScenarioObjectKind::kNetworkCondition:
      return kNetworkCondition;
    case ScenarioObjectKind::kNodeNetworkCondition:
      return kNodeNetworkCondition;
    case ScenarioObjectKind::kNetworkBlockRule:
      return kNetworkBlockRule;
    case ScenarioObjectKind::kNetworkFlow:
      return kNetworkFlow;
    case ScenarioObjectKind::kNetworkPartition:
      return kNetworkPartition;
    case ScenarioObjectKind::kSimulationPartitionGroup:
      return kSimulationPartitionGroup;
    case ScenarioObjectKind::kSimulationPartition:
      return kSimulationPartition;
    case ScenarioObjectKind::kPerfTarget:
      return kPerfTarget;
    case ScenarioObjectKind::kWalletSend:
      return kWalletSend;
    case ScenarioObjectKind::kBlockProduction:
      return kBlockProduction;
    case ScenarioObjectKind::kResources:
      return kResources;
    case ScenarioObjectKind::kNetwork:
      return kNetwork;
    case ScenarioObjectKind::kProcess:
      return kProcess;
    case ScenarioObjectKind::kProcessRestart:
      return kProcessRestart;
    case ScenarioObjectKind::kProcessFreeze:
      return kProcessFreeze;
    case ScenarioObjectKind::kCount:
      break;
  }
  throw std::logic_error("unknown scenario object kind");
}

bool ScenarioObjectFieldAllowed(ScenarioObjectKind kind,
                                std::string_view field) {
  return Contains(ScenarioObjectFields(kind), field);
}

std::span<const std::string_view> ScenarioTopologyCommonFields() {
  static constexpr auto kFields =
      Fields("type", "node_count", "wallet_node_count", "miner_node_count",
             "wallet_nodes", "miner_nodes", "allow_miner_wallet_overlap",
             "wallet_initialization", "peer_connectivity");
  return kFields;
}

std::span<const std::string_view> ScenarioTopologyKindFields(
    PeerTopologyKind kind) {
  static constexpr std::array<std::string_view, 0> kNone{};
  static constexpr auto kStar = Fields("center_node");
  static constexpr auto kRandom = Fields("seed", "average_degree");
  static constexpr auto kScaleFree =
      Fields("seed", "average_degree", "attachment_count");
  static constexpr auto kLatency = Fields("latency_matrix_ms");
  static constexpr auto kEdges = Fields("edges");
  static constexpr auto kGroups = Fields("groups");
  static constexpr auto kRegions = Fields("regions", "region_edges");
  switch (kind) {
    case PeerTopologyKind::kFullMesh:
    case PeerTopologyKind::kRing:
      return kNone;
    case PeerTopologyKind::kStar:
      return kStar;
    case PeerTopologyKind::kRandomGraph:
      return kRandom;
    case PeerTopologyKind::kScaleFreeGraph:
      return kScaleFree;
    case PeerTopologyKind::kLatencyMatrix:
      return kLatency;
    case PeerTopologyKind::kCustomEdgeList:
      return kEdges;
    case PeerTopologyKind::kPartitionedGroups:
      return kGroups;
    case PeerTopologyKind::kInternetLikeRegionGraph:
      return kRegions;
    case PeerTopologyKind::kCount:
      break;
  }
  throw std::logic_error("unknown topology kind");
}

bool ScenarioTopologyFieldAllowed(PeerTopologyKind kind,
                                  std::string_view field) {
  const std::span<const std::string_view> kind_fields =
      ScenarioTopologyKindFields(kind);
  return Contains(ScenarioTopologyCommonFields(), field) ||
         Contains(kind_fields, field);
}

std::span<const std::string_view> ScenarioWorkloadFields(WorkloadKind kind) {
  static constexpr auto kBlockGeneration =
      Fields("node", "nodes", "count", "sync_timeout_sec");
  static constexpr auto kHeight =
      Fields("node", "nodes", "height", "timeout_sec");
  static constexpr auto kPeers =
      Fields("node", "nodes", "peer_count", "timeout_sec");
  static constexpr auto kPeer = Fields("node", "nodes", "peer", "timeout_sec");
  static constexpr auto kNode = Fields("node", "nodes");
  static constexpr auto kFreeze = Fields("node", "nodes", "duration_ms");
  static constexpr auto kResource = Fields(
      "node", "nodes", "memory_high_bytes", "memory_max_bytes", "cpu_quota_us",
      "cpu_period_us", "cpu_weight", "io_weight", "io_max", "pids_max");
  static constexpr auto kProfile = Fields("nodes", "profile");
  static constexpr auto kPressure =
      Fields("node", "nodes", "duration_ms", "memory_high_bytes",
             "memory_max_bytes", "cpu_quota_us", "cpu_period_us", "cpu_weight",
             "io_weight", "io_max", "pids_max");
  static constexpr auto kCondition =
      Fields("node", "nodes", "bandwidth_mbps", "delay_ms", "jitter_ms",
             "loss_basis_points", "loss_percent", "duplicate_basis_points",
             "corrupt_basis_points", "reorder_basis_points", "limit_packets");
  static constexpr auto kFlow =
      Fields("node", "nodes", "src_address", "src_port", "dst_address",
             "dst_port", "handle");
  static constexpr auto kPartition = Fields("group_a", "group_b");
  static constexpr auto kEdge =
      Fields("from", "to", "timeout_sec", "latency_ms", "bandwidth_mbps",
             "delay_ms", "jitter_ms", "loss_basis_points", "loss_percent",
             "duplicate_basis_points", "corrupt_basis_points",
             "reorder_basis_points", "limit_packets");
  static constexpr auto kRawTransaction =
      Fields("nodes", "funding_node", "submit_node", "source_address",
             "source_private_key", "destination_address", "funding_blocks",
             "amount", "fee", "timeout_sec");
  static constexpr auto kWalletTransactions = Fields(
      "funding_strategy", "strategy", "funding_blocks_per_wallet",
      "readiness_confirmations", "transaction_count", "transaction_rate",
      "duration", "concurrency", "queue_capacity", "mode", "amount", "interval",
      "fee_policy", "fee", "funding_threshold", "seed", "sender_wallets",
      "receiver_wallets", "timeout_sec", "wallets", "private_key",
      "source_private_key", "address", "source_address", "destination_address");
  static constexpr auto kCheckpoint = Fields("name");
  switch (kind) {
    case WorkloadKind::kBlockGeneration:
      return kBlockGeneration;
    case WorkloadKind::kWaitUntilHeight:
      return kHeight;
    case WorkloadKind::kWaitForPeers:
      return kPeers;
    case WorkloadKind::kConnectPeer:
    case WorkloadKind::kDisconnectPeer:
      return kPeer;
    case WorkloadKind::kRestartNode:
      return kNode;
    case WorkloadKind::kFreezeNode:
      return kFreeze;
    case WorkloadKind::kUpdateResourceLimits:
      return kResource;
    case WorkloadKind::kSetResourceProfile:
    case WorkloadKind::kSetNetworkProfile:
      return kProfile;
    case WorkloadKind::kResourcePressure:
      return kPressure;
    case WorkloadKind::kSetNetworkCondition:
      return kCondition;
    case WorkloadKind::kBlockNetworkFlow:
    case WorkloadKind::kUnblockNetworkFlow:
      return kFlow;
    case WorkloadKind::kPartitionNodes:
    case WorkloadKind::kHealPartition:
      return kPartition;
    case WorkloadKind::kSetEdgeCondition:
    case WorkloadKind::kActivateEdge:
    case WorkloadKind::kDeactivateEdge:
    case WorkloadKind::kRestoreEdge:
      return kEdge;
    case WorkloadKind::kSendRawTransaction:
      return kRawTransaction;
    case WorkloadKind::kWalletTransactions:
      return kWalletTransactions;
    case WorkloadKind::kCheckpoint:
      return kCheckpoint;
    case WorkloadKind::kCount:
      break;
  }
  throw std::logic_error("unknown workload kind");
}

bool ScenarioWorkloadFieldAllowed(WorkloadKind kind, std::string_view field) {
  return Contains(ScenarioWorkloadFields(kind), field);
}

std::span<const std::string_view> ScenarioCommandFields(
    SimulationCommandKind kind) {
  static constexpr std::array<std::string_view, 0> kNone{};
  static constexpr auto kPolicy = Fields("period_ms", "probability", "seed");
  static constexpr auto kDifficulty = Fields("difficulty");
  static constexpr auto kPeer = Fields("peer_node_id");
  static constexpr auto kPeerCount =
      Fields("minimum_peer_count", "maximum_peer_count");
  static constexpr auto kBlocks = Fields("block_count");
  static constexpr auto kProfile = Fields("profile");
  static constexpr auto kResource = Fields("resource_limits");
  static constexpr auto kCondition = Fields("network_condition");
  static constexpr auto kFlow = Fields("network_flow");
  static constexpr auto kPartition = Fields("partition");
  static constexpr auto kPerf = Fields("perf_target", "perf_counters");
  static constexpr auto kWallet = Fields("wallet_send");
  switch (kind) {
    case SimulationCommandKind::kIncreaseLogVerbosity:
    case SimulationCommandKind::kDecreaseLogVerbosity:
    case SimulationCommandKind::kStopMining:
    case SimulationCommandKind::kDisconnectNode:
    case SimulationCommandKind::kReconnectNode:
    case SimulationCommandKind::kKillNode:
    case SimulationCommandKind::kFreezeNode:
    case SimulationCommandKind::kThawNode:
    case SimulationCommandKind::kStopNode:
    case SimulationCommandKind::kRestartNode:
    case SimulationCommandKind::kExportNodeReport:
      return kNone;
    case SimulationCommandKind::kSetBlockProductionPolicy:
      return kPolicy;
    case SimulationCommandKind::kSetMiningDifficulty:
      return kDifficulty;
    case SimulationCommandKind::kConnectPeer:
    case SimulationCommandKind::kDisconnectPeer:
      return kPeer;
    case SimulationCommandKind::kSetPeerCountPolicy:
      return kPeerCount;
    case SimulationCommandKind::kGenerateBlocks:
      return kBlocks;
    case SimulationCommandKind::kSetResourceProfile:
    case SimulationCommandKind::kSetNetworkProfile:
      return kProfile;
    case SimulationCommandKind::kSetResourceLimits:
      return kResource;
    case SimulationCommandKind::kSetNetworkCondition:
      return kCondition;
    case SimulationCommandKind::kBlockNetworkFlow:
    case SimulationCommandKind::kUnblockNetworkFlow:
      return kFlow;
    case SimulationCommandKind::kPartitionNodes:
    case SimulationCommandKind::kHealPartition:
      return kPartition;
    case SimulationCommandKind::kSetPerfCounters:
      return kPerf;
    case SimulationCommandKind::kSendWalletTransaction:
      return kWallet;
    case SimulationCommandKind::kCount:
      break;
  }
  throw std::logic_error("unknown simulation command kind");
}

bool ScenarioCommandFieldAllowed(SimulationCommandKind kind,
                                 std::string_view field) {
  const std::span<const std::string_view> fields = ScenarioCommandFields(kind);
  if (field == "node") {
    return kind != SimulationCommandKind::kSetBlockProductionPolicy &&
           kind != SimulationCommandKind::kPartitionNodes &&
           kind != SimulationCommandKind::kHealPartition &&
           kind != SimulationCommandKind::kSetPerfCounters;
  }
  return Contains(fields, field);
}

std::vector<std::string> BuildScenarioMemberRegistry() {
  std::set<std::string> members;
  for (std::size_t index = 0U;
       index < static_cast<std::size_t>(ScenarioObjectKind::kCount); ++index) {
    const auto kind = static_cast<ScenarioObjectKind>(index);
    const std::string context(ScenarioObjectKindName(kind));
    for (const std::string_view field : ScenarioObjectFields(kind)) {
      members.insert(context + "." + std::string(field));
    }
  }
  for (std::size_t index = 0U;
       index < static_cast<std::size_t>(ChainKind::kCount); ++index) {
    const auto chain = static_cast<ChainKind>(index);
    members.insert("scenario." +
                   ChainDriverSpecFor(chain).daemon_scenario_field);
  }
  for (const std::string_view field : ScenarioTopologyCommonFields()) {
    members.insert("topology." + std::string(field));
  }
  for (std::size_t index = 0U;
       index < static_cast<std::size_t>(PeerTopologyKind::kCount); ++index) {
    const auto kind = static_cast<PeerTopologyKind>(index);
    for (const std::string_view field : ScenarioTopologyKindFields(kind)) {
      members.insert("topology." + std::string(field));
    }
  }
  for (std::size_t index = 0U;
       index < static_cast<std::size_t>(WorkloadKind::kCount); ++index) {
    const auto kind = static_cast<WorkloadKind>(index);
    members.insert("workload." + std::string(WorkloadKindName(kind)) + ".type");
    for (const std::string_view field : ScenarioWorkloadFields(kind)) {
      members.insert("workload." + std::string(WorkloadKindName(kind)) + "." +
                     std::string(field));
    }
  }
  for (std::size_t index = 0U;
       index < static_cast<std::size_t>(SimulationCommandKind::kCount);
       ++index) {
    const auto kind = static_cast<SimulationCommandKind>(index);
    const std::string prefix =
        "command." + std::string(SimulationCommandKindName(kind)) + ".";
    if (ScenarioCommandFieldAllowed(kind, "node")) {
      members.insert(prefix + "node");
    }
    for (const std::string_view field : ScenarioCommandFields(kind)) {
      members.insert(prefix + std::string(field));
    }
  }
  return {members.begin(), members.end()};
}

}  // namespace bbp
