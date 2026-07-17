#include "bbp/simulation_event_kind.h"

#include <stdexcept>

namespace bbp {

std::string_view SimulationEventKindName(SimulationEventKind kind) {
  switch (kind) {
    case SimulationEventKind::kState:
      return "state";
    case SimulationEventKind::kStdoutTail:
      return "stdout_tail";
    case SimulationEventKind::kStderrTail:
      return "stderr_tail";
    case SimulationEventKind::kDaemonLogTail:
      return "daemon_log_tail";
    case SimulationEventKind::kNetworkConditionVerified:
      return "network_condition_verified";
    case SimulationEventKind::kDirectionalNetworkPoliciesVerified:
      return "directional_network_policies_verified";
    case SimulationEventKind::kNetworkReady:
      return "network_ready";
    case SimulationEventKind::kProcessStarted:
      return "process_started";
    case SimulationEventKind::kProcessExitedBeforeRpcReady:
      return "process_exited_before_rpc_ready";
    case SimulationEventKind::kRpcReady:
      return "rpc_ready";
    case SimulationEventKind::kStartupPeerConnected:
      return "startup_peer_connected";
    case SimulationEventKind::kWalletAddressRequested:
      return "wallet_address_requested";
    case SimulationEventKind::kWalletAddressCreated:
      return "wallet_address_created";
    case SimulationEventKind::kWalletFunded:
      return "wallet_funded";
    case SimulationEventKind::kResourceLimitsUpdated:
      return "resource_limits_updated";
    case SimulationEventKind::kResourceProfileUpdated:
      return "resource_profile_updated";
    case SimulationEventKind::kResourcePressureStarted:
      return "resource_pressure_started";
    case SimulationEventKind::kResourcePressureRestoredAfterError:
      return "resource_pressure_restored_after_error";
    case SimulationEventKind::kResourcePressureFinished:
      return "resource_pressure_finished";
    case SimulationEventKind::kPeerConnected:
      return "peer_connected";
    case SimulationEventKind::kPeerDisconnected:
      return "peer_disconnected";
    case SimulationEventKind::kRawTransactionSubmitted:
      return "raw_transaction_submitted";
    case SimulationEventKind::kWalletTransactionSubmitted:
      return "wallet_transaction_submitted";
    case SimulationEventKind::kTransactionVisible:
      return "transaction_visible";
    case SimulationEventKind::kTransactionConfirmed:
      return "transaction_confirmed";
    case SimulationEventKind::kNetworkConditionUpdated:
      return "network_condition_updated";
    case SimulationEventKind::kNetworkProfileUpdated:
      return "network_profile_updated";
    case SimulationEventKind::kProfileUpdateRollbackFailed:
      return "profile_update_rollback_failed";
    case SimulationEventKind::kNetworkBlockApplied:
      return "network_block_applied";
    case SimulationEventKind::kNetworkBlockRemoved:
      return "network_block_removed";
    case SimulationEventKind::kNetworkPartitionApplied:
      return "network_partition_applied";
    case SimulationEventKind::kNetworkPartitionHealed:
      return "network_partition_healed";
    case SimulationEventKind::kTopologyEdgeUpdated:
      return "topology_edge_updated";
    case SimulationEventKind::kTopologyEdgeUpdateRollbackFailed:
      return "topology_edge_update_rollback_failed";
    case SimulationEventKind::kRestartRequested:
      return "restart_requested";
    case SimulationEventKind::kSigterm:
      return "sigterm";
    case SimulationEventKind::kProcessRestarted:
      return "process_restarted";
    case SimulationEventKind::kCgroupFrozen:
      return "cgroup_frozen";
    case SimulationEventKind::kCgroupThawed:
      return "cgroup_thawed";
    case SimulationEventKind::kRpcStop:
      return "rpc_stop";
    case SimulationEventKind::kRpcStopSkipped:
      return "rpc_stop_skipped";
    case SimulationEventKind::kCgroupRemoveFailed:
      return "cgroup_remove_failed";
    case SimulationEventKind::kNetworkRemoved:
      return "network_removed";
    case SimulationEventKind::kRunCgroupRemoveFailed:
      return "run_cgroup_remove_failed";
    case SimulationEventKind::kRunStarted:
      return "run_started";
    case SimulationEventKind::kRunFailed:
      return "run_failed";
    case SimulationEventKind::kRunCancelled:
      return "run_cancelled";
    case SimulationEventKind::kRunFinished:
      return "run_finished";
    case SimulationEventKind::kScheduledEventStarted:
      return "scheduled_event_started";
    case SimulationEventKind::kScheduledEventCompleted:
      return "scheduled_event_completed";
    case SimulationEventKind::kScheduledEventFailed:
      return "scheduled_event_failed";
    case SimulationEventKind::kScheduledBlockProduced:
      return "scheduled_block_produced";
    case SimulationEventKind::kScheduledBlockFailed:
      return "scheduled_block_failed";
    case SimulationEventKind::kPeerPolicyConnected:
      return "peer_policy_connected";
    case SimulationEventKind::kPeerPolicyDisconnected:
      return "peer_policy_disconnected";
    case SimulationEventKind::kPeerPolicyEnforcementFailed:
      return "peer_policy_enforcement_failed";
    case SimulationEventKind::kOperatorCommandStarted:
      return "operator_command_started";
    case SimulationEventKind::kProcessKillRequested:
      return "process_kill_requested";
    case SimulationEventKind::kProcessKilled:
      return "process_killed";
    case SimulationEventKind::kOperatorCommandCompleted:
      return "operator_command_completed";
    case SimulationEventKind::kOperatorCommandFailed:
      return "operator_command_failed";
    case SimulationEventKind::kMetricsNodeUnavailable:
      return "metrics_node_unavailable";
    case SimulationEventKind::kWalletMetricsUnavailable:
      return "wallet_metrics_unavailable";
    case SimulationEventKind::kMetricsSample:
      return "metrics_sample";
    case SimulationEventKind::kGeneratedBlocks:
      return "generated_blocks";
    case SimulationEventKind::kHeightReached:
      return "height_reached";
    case SimulationEventKind::kHeightWaitReached:
      return "height_wait_reached";
    case SimulationEventKind::kPeerCountReached:
      return "peer_count_reached";
    case SimulationEventKind::kNodeRestarted:
      return "node_restarted";
    case SimulationEventKind::kNodeFreezeCompleted:
      return "node_freeze_completed";
    case SimulationEventKind::kCheckpointRecorded:
      return "checkpoint_recorded";
  }
  throw std::runtime_error("unknown simulation event kind");
}

std::optional<SimulationEventKind> SimulationEventKindFromName(
    std::string_view name) {
  if (name == "state") return SimulationEventKind::kState;
  if (name == "stdout_tail") return SimulationEventKind::kStdoutTail;
  if (name == "stderr_tail") return SimulationEventKind::kStderrTail;
  if (name == "daemon_log_tail") return SimulationEventKind::kDaemonLogTail;
  if (name == "network_condition_verified")
    return SimulationEventKind::kNetworkConditionVerified;
  if (name == "directional_network_policies_verified")
    return SimulationEventKind::kDirectionalNetworkPoliciesVerified;
  if (name == "network_ready") return SimulationEventKind::kNetworkReady;
  if (name == "process_started") return SimulationEventKind::kProcessStarted;
  if (name == "process_exited_before_rpc_ready")
    return SimulationEventKind::kProcessExitedBeforeRpcReady;
  if (name == "rpc_ready") return SimulationEventKind::kRpcReady;
  if (name == "startup_peer_connected")
    return SimulationEventKind::kStartupPeerConnected;
  if (name == "wallet_address_requested")
    return SimulationEventKind::kWalletAddressRequested;
  if (name == "wallet_address_created")
    return SimulationEventKind::kWalletAddressCreated;
  if (name == "wallet_funded") return SimulationEventKind::kWalletFunded;
  if (name == "resource_limits_updated")
    return SimulationEventKind::kResourceLimitsUpdated;
  if (name == "resource_profile_updated")
    return SimulationEventKind::kResourceProfileUpdated;
  if (name == "resource_pressure_started")
    return SimulationEventKind::kResourcePressureStarted;
  if (name == "resource_pressure_restored_after_error")
    return SimulationEventKind::kResourcePressureRestoredAfterError;
  if (name == "resource_pressure_finished")
    return SimulationEventKind::kResourcePressureFinished;
  if (name == "peer_connected") return SimulationEventKind::kPeerConnected;
  if (name == "peer_disconnected")
    return SimulationEventKind::kPeerDisconnected;
  if (name == "raw_transaction_submitted")
    return SimulationEventKind::kRawTransactionSubmitted;
  if (name == "wallet_transaction_submitted")
    return SimulationEventKind::kWalletTransactionSubmitted;
  if (name == "transaction_visible")
    return SimulationEventKind::kTransactionVisible;
  if (name == "transaction_confirmed")
    return SimulationEventKind::kTransactionConfirmed;
  if (name == "network_condition_updated")
    return SimulationEventKind::kNetworkConditionUpdated;
  if (name == "network_profile_updated")
    return SimulationEventKind::kNetworkProfileUpdated;
  if (name == "profile_update_rollback_failed")
    return SimulationEventKind::kProfileUpdateRollbackFailed;
  if (name == "network_block_applied")
    return SimulationEventKind::kNetworkBlockApplied;
  if (name == "network_block_removed")
    return SimulationEventKind::kNetworkBlockRemoved;
  if (name == "network_partition_applied")
    return SimulationEventKind::kNetworkPartitionApplied;
  if (name == "network_partition_healed")
    return SimulationEventKind::kNetworkPartitionHealed;
  if (name == "topology_edge_updated")
    return SimulationEventKind::kTopologyEdgeUpdated;
  if (name == "topology_edge_update_rollback_failed")
    return SimulationEventKind::kTopologyEdgeUpdateRollbackFailed;
  if (name == "restart_requested")
    return SimulationEventKind::kRestartRequested;
  if (name == "sigterm") return SimulationEventKind::kSigterm;
  if (name == "process_restarted")
    return SimulationEventKind::kProcessRestarted;
  if (name == "cgroup_frozen") return SimulationEventKind::kCgroupFrozen;
  if (name == "cgroup_thawed") return SimulationEventKind::kCgroupThawed;
  if (name == "rpc_stop") return SimulationEventKind::kRpcStop;
  if (name == "rpc_stop_skipped") return SimulationEventKind::kRpcStopSkipped;
  if (name == "cgroup_remove_failed")
    return SimulationEventKind::kCgroupRemoveFailed;
  if (name == "network_removed") return SimulationEventKind::kNetworkRemoved;
  if (name == "run_cgroup_remove_failed")
    return SimulationEventKind::kRunCgroupRemoveFailed;
  if (name == "run_started") return SimulationEventKind::kRunStarted;
  if (name == "run_failed") return SimulationEventKind::kRunFailed;
  if (name == "run_cancelled") return SimulationEventKind::kRunCancelled;
  if (name == "run_finished") return SimulationEventKind::kRunFinished;
  if (name == "scheduled_event_started")
    return SimulationEventKind::kScheduledEventStarted;
  if (name == "scheduled_event_completed")
    return SimulationEventKind::kScheduledEventCompleted;
  if (name == "scheduled_event_failed")
    return SimulationEventKind::kScheduledEventFailed;
  if (name == "scheduled_block_produced")
    return SimulationEventKind::kScheduledBlockProduced;
  if (name == "scheduled_block_failed")
    return SimulationEventKind::kScheduledBlockFailed;
  if (name == "peer_policy_connected")
    return SimulationEventKind::kPeerPolicyConnected;
  if (name == "peer_policy_disconnected")
    return SimulationEventKind::kPeerPolicyDisconnected;
  if (name == "peer_policy_enforcement_failed")
    return SimulationEventKind::kPeerPolicyEnforcementFailed;
  if (name == "operator_command_started")
    return SimulationEventKind::kOperatorCommandStarted;
  if (name == "process_kill_requested")
    return SimulationEventKind::kProcessKillRequested;
  if (name == "process_killed") return SimulationEventKind::kProcessKilled;
  if (name == "operator_command_completed")
    return SimulationEventKind::kOperatorCommandCompleted;
  if (name == "operator_command_failed")
    return SimulationEventKind::kOperatorCommandFailed;
  if (name == "metrics_node_unavailable")
    return SimulationEventKind::kMetricsNodeUnavailable;
  if (name == "wallet_metrics_unavailable")
    return SimulationEventKind::kWalletMetricsUnavailable;
  if (name == "metrics_sample") return SimulationEventKind::kMetricsSample;
  if (name == "generated_blocks") return SimulationEventKind::kGeneratedBlocks;
  if (name == "height_reached") return SimulationEventKind::kHeightReached;
  if (name == "height_wait_reached")
    return SimulationEventKind::kHeightWaitReached;
  if (name == "peer_count_reached")
    return SimulationEventKind::kPeerCountReached;
  if (name == "node_restarted") return SimulationEventKind::kNodeRestarted;
  if (name == "node_freeze_completed")
    return SimulationEventKind::kNodeFreezeCompleted;
  if (name == "checkpoint_recorded")
    return SimulationEventKind::kCheckpointRecorded;
  return std::nullopt;
}

}  // namespace bbp
