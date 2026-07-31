#pragma once

#include <array>
#include <cstddef>
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
  kSetResourceProfile,
  kSetNetworkProfile,
  kResourcePressure,
  kSetNetworkCondition,
  kBlockNetworkFlow,
  kUnblockNetworkFlow,
  kPartitionNodes,
  kHealPartition,
  kSetEdgeCondition,
  kActivateEdge,
  kDeactivateEdge,
  kRestoreEdge,
  kSendRawTransaction,
  kWalletTransactions,
  kCheckpoint,
  kCount,
};

inline constexpr std::array kLifecycleWorkloadKinds{
    WorkloadKind::kBlockGeneration,
    WorkloadKind::kWaitUntilHeight,
    WorkloadKind::kWaitForPeers,
    WorkloadKind::kWalletTransactions,
};

inline constexpr std::array kOneShotWorkloadKinds{
    WorkloadKind::kConnectPeer,          WorkloadKind::kDisconnectPeer,
    WorkloadKind::kRestartNode,          WorkloadKind::kFreezeNode,
    WorkloadKind::kUpdateResourceLimits, WorkloadKind::kSetResourceProfile,
    WorkloadKind::kSetNetworkProfile,    WorkloadKind::kResourcePressure,
    WorkloadKind::kSetNetworkCondition,  WorkloadKind::kBlockNetworkFlow,
    WorkloadKind::kUnblockNetworkFlow,   WorkloadKind::kPartitionNodes,
    WorkloadKind::kHealPartition,        WorkloadKind::kSetEdgeCondition,
    WorkloadKind::kActivateEdge,         WorkloadKind::kDeactivateEdge,
    WorkloadKind::kRestoreEdge,          WorkloadKind::kSendRawTransaction,
    WorkloadKind::kCheckpoint,
};

constexpr bool IsLifecycleWorkloadKind(WorkloadKind kind) {
  for (const WorkloadKind candidate : kLifecycleWorkloadKinds) {
    if (candidate == kind) {
      return true;
    }
  }
  return false;
}

constexpr bool IsOneShotWorkloadKind(WorkloadKind kind) {
  for (const WorkloadKind candidate : kOneShotWorkloadKinds) {
    if (candidate == kind) {
      return true;
    }
  }
  return false;
}

static_assert(kLifecycleWorkloadKinds.size() + kOneShotWorkloadKinds.size() ==
              static_cast<std::size_t>(WorkloadKind::kCount));
static_assert([] {
  for (std::size_t index = 0U;
       index < static_cast<std::size_t>(WorkloadKind::kCount); ++index) {
    const auto kind = static_cast<WorkloadKind>(index);
    if (IsLifecycleWorkloadKind(kind) == IsOneShotWorkloadKind(kind)) {
      return false;
    }
  }
  return !IsLifecycleWorkloadKind(WorkloadKind::kCount) &&
         !IsOneShotWorkloadKind(WorkloadKind::kCount);
}());

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
    case WorkloadKind::kSetResourceProfile:
      return "set_resource_profile";
    case WorkloadKind::kSetNetworkProfile:
      return "set_network_profile";
    case WorkloadKind::kResourcePressure:
      return "resource_pressure";
    case WorkloadKind::kSetNetworkCondition:
      return "set_network_condition";
    case WorkloadKind::kBlockNetworkFlow:
      return "block_network_flow";
    case WorkloadKind::kUnblockNetworkFlow:
      return "unblock_network_flow";
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
    case WorkloadKind::kCheckpoint:
      return "checkpoint";
    case WorkloadKind::kCount:
      break;
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
  if (name == "set_resource_profile") {
    return WorkloadKind::kSetResourceProfile;
  }
  if (name == "set_network_profile") {
    return WorkloadKind::kSetNetworkProfile;
  }
  if (name == "resource_pressure") {
    return WorkloadKind::kResourcePressure;
  }
  if (name == "set_network_condition") {
    return WorkloadKind::kSetNetworkCondition;
  }
  if (name == "block_network_flow") {
    return WorkloadKind::kBlockNetworkFlow;
  }
  if (name == "unblock_network_flow") {
    return WorkloadKind::kUnblockNetworkFlow;
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
  if (name == "checkpoint") {
    return WorkloadKind::kCheckpoint;
  }
  return std::nullopt;
}

}  // namespace bbp
