#pragma once

#include <optional>
#include <string_view>

namespace bbp {

enum class WorkloadKind {
  kBlockGeneration,
  kWaitUntilHeight,
  kWaitForPeers,
  kConnectPeer,
  kDisconnectPeer,
  kRestartNode,
  kFreezeNode,
  kUpdateResourceLimits,
  kResourcePressure,
  kPartitionNodes,
  kHealPartition,
  kSetEdgeCondition,
  kActivateEdge,
  kDeactivateEdge,
  kRestoreEdge,
  kSendRawTransaction,
  kWalletTransactions,
};

constexpr std::string_view WorkloadKindName(WorkloadKind kind) {
  switch (kind) {
    case WorkloadKind::kBlockGeneration:
      return "block_generation";
    case WorkloadKind::kWaitUntilHeight:
      return "wait_until_height";
    case WorkloadKind::kWaitForPeers:
      return "wait_for_peers";
    case WorkloadKind::kConnectPeer:
      return "connect_peer";
    case WorkloadKind::kDisconnectPeer:
      return "disconnect_peer";
    case WorkloadKind::kRestartNode:
      return "restart_node";
    case WorkloadKind::kFreezeNode:
      return "freeze_node";
    case WorkloadKind::kUpdateResourceLimits:
      return "update_resource_limits";
    case WorkloadKind::kResourcePressure:
      return "resource_pressure";
    case WorkloadKind::kPartitionNodes:
      return "partition_nodes";
    case WorkloadKind::kHealPartition:
      return "heal_partition";
    case WorkloadKind::kSetEdgeCondition:
      return "set_edge_condition";
    case WorkloadKind::kActivateEdge:
      return "activate_edge";
    case WorkloadKind::kDeactivateEdge:
      return "deactivate_edge";
    case WorkloadKind::kRestoreEdge:
      return "restore_edge";
    case WorkloadKind::kSendRawTransaction:
      return "send_raw_transaction";
    case WorkloadKind::kWalletTransactions:
      return "wallet_transactions";
  }
  return "unknown";
}

constexpr std::optional<WorkloadKind> ParseWorkloadKind(std::string_view name) {
  if (name == "block_generation") {
    return WorkloadKind::kBlockGeneration;
  }
  if (name == "wait_until_height") {
    return WorkloadKind::kWaitUntilHeight;
  }
  if (name == "wait_for_peers") {
    return WorkloadKind::kWaitForPeers;
  }
  if (name == "connect_peer") {
    return WorkloadKind::kConnectPeer;
  }
  if (name == "disconnect_peer") {
    return WorkloadKind::kDisconnectPeer;
  }
  if (name == "restart_node") {
    return WorkloadKind::kRestartNode;
  }
  if (name == "freeze_node") {
    return WorkloadKind::kFreezeNode;
  }
  if (name == "update_resource_limits") {
    return WorkloadKind::kUpdateResourceLimits;
  }
  if (name == "resource_pressure") {
    return WorkloadKind::kResourcePressure;
  }
  if (name == "partition_nodes") {
    return WorkloadKind::kPartitionNodes;
  }
  if (name == "heal_partition") {
    return WorkloadKind::kHealPartition;
  }
  if (name == "set_edge_condition") {
    return WorkloadKind::kSetEdgeCondition;
  }
  if (name == "activate_edge") {
    return WorkloadKind::kActivateEdge;
  }
  if (name == "deactivate_edge") {
    return WorkloadKind::kDeactivateEdge;
  }
  if (name == "restore_edge") {
    return WorkloadKind::kRestoreEdge;
  }
  if (name == "send_raw_transaction") {
    return WorkloadKind::kSendRawTransaction;
  }
  if (name == "wallet_transactions") {
    return WorkloadKind::kWalletTransactions;
  }
  return std::nullopt;
}

}  // namespace bbp
