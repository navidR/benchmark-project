#include "bbp/simulation_command.h"

#include <algorithm>
#include <stdexcept>
#include <thread>

namespace bbp {

std::string_view SimulationNodeRestartPhaseName(
    SimulationNodeRestartPhase phase) {
  switch (phase) {
    case SimulationNodeRestartPhase::kBeforeStop:
      return "before_stop";
    case SimulationNodeRestartPhase::kStopRequested:
      return "stop_requested";
    case SimulationNodeRestartPhase::kOriginalExited:
      return "original_exited";
    case SimulationNodeRestartPhase::kReplacementReady:
      return "replacement_ready";
    case SimulationNodeRestartPhase::kCompleted:
      return "completed";
  }
  throw std::logic_error("unknown node restart phase");
}

SimulationNodeRestartReconciliation ReconcileCancelledSimulationNodeRestart(
    const SimulationNodeProcessObservation& initial,
    SimulationNodeRestartPhase phase,
    std::chrono::steady_clock::time_point deadline,
    const std::function<SimulationNodeProcessObservation()>& observe) {
  if (!observe) {
    throw std::invalid_argument(
        "node restart reconciliation requires a process observer");
  }
  const auto same_generation =
      [&](const SimulationNodeProcessObservation& observation) {
        return observation.running == initial.running &&
               observation.restart_count == initial.restart_count &&
               (!initial.running || observation.pid == initial.pid);
      };
  while (true) {
    const SimulationNodeProcessObservation current = observe();
    if (phase == SimulationNodeRestartPhase::kBeforeStop) {
      return same_generation(current)
                 ? SimulationNodeRestartReconciliation::kUnchanged
                 : SimulationNodeRestartReconciliation::kUnconfirmed;
    }
    if (!initial.running && !current.running &&
        current.restart_count == initial.restart_count) {
      return SimulationNodeRestartReconciliation::kUnchanged;
    }
    if (!current.running && current.restart_count == initial.restart_count) {
      return SimulationNodeRestartReconciliation::kStopped;
    }
    if (current.running &&
        phase >= SimulationNodeRestartPhase::kReplacementReady &&
        (current.pid != initial.pid ||
         current.restart_count != initial.restart_count)) {
      return SimulationNodeRestartReconciliation::kReplacementReady;
    }
    const auto now = std::chrono::steady_clock::now();
    if (now >= deadline) {
      return SimulationNodeRestartReconciliation::kUnconfirmed;
    }
    std::this_thread::sleep_for(std::min(
        std::chrono::milliseconds(5),
        std::chrono::duration_cast<std::chrono::milliseconds>(deadline - now)));
  }
}

std::string_view SimulationCommandKindName(SimulationCommandKind kind) {
  switch (kind) {
    case SimulationCommandKind::kIncreaseLogVerbosity:
      return "increase_log_verbosity";
    case SimulationCommandKind::kDecreaseLogVerbosity:
      return "decrease_log_verbosity";
    case SimulationCommandKind::kStopMining:
      return "stop_mining";
    case SimulationCommandKind::kDisconnectNode:
      return "disconnect_node";
    case SimulationCommandKind::kReconnectNode:
      return "reconnect_node";
    case SimulationCommandKind::kSetBlockProductionPolicy:
      return "set_block_production_policy";
    case SimulationCommandKind::kSetMiningDifficulty:
      return "set_mining_difficulty";
    case SimulationCommandKind::kKillNode:
      return "kill_node";
    case SimulationCommandKind::kConnectPeer:
      return "connect_peer";
    case SimulationCommandKind::kDisconnectPeer:
      return "disconnect_peer";
    case SimulationCommandKind::kSetPeerCountPolicy:
      return "set_peer_count_policy";
    case SimulationCommandKind::kFreezeNode:
      return "freeze_node";
    case SimulationCommandKind::kThawNode:
      return "thaw_node";
    case SimulationCommandKind::kStopNode:
      return "stop_node";
    case SimulationCommandKind::kRestartNode:
      return "restart_node";
    case SimulationCommandKind::kGenerateBlocks:
      return "generate_blocks";
    case SimulationCommandKind::kSetResourceProfile:
      return "set_resource_profile";
    case SimulationCommandKind::kSetResourceLimits:
      return "set_resource_limits";
    case SimulationCommandKind::kSetNetworkProfile:
      return "set_network_profile";
    case SimulationCommandKind::kSetNetworkCondition:
      return "set_network_condition";
    case SimulationCommandKind::kBlockNetworkFlow:
      return "block_network_flow";
    case SimulationCommandKind::kUnblockNetworkFlow:
      return "unblock_network_flow";
    case SimulationCommandKind::kPartitionNodes:
      return "partition_nodes";
    case SimulationCommandKind::kHealPartition:
      return "heal_partition";
    case SimulationCommandKind::kExportNodeReport:
      return "export_node_report";
    case SimulationCommandKind::kSetPerfCounters:
      return "set_perf_counters";
    case SimulationCommandKind::kSendWalletTransaction:
      return "send_wallet_transaction";
    case SimulationCommandKind::kCount:
      break;
  }
  throw std::runtime_error("unknown simulation command kind");
}

std::optional<SimulationCommandKind> SimulationCommandKindFromName(
    std::string_view name) {
  if (name == "increase_log_verbosity") {
    return SimulationCommandKind::kIncreaseLogVerbosity;
  }
  if (name == "decrease_log_verbosity") {
    return SimulationCommandKind::kDecreaseLogVerbosity;
  }
  if (name == "stop_mining") {
    return SimulationCommandKind::kStopMining;
  }
  if (name == "disconnect_node") {
    return SimulationCommandKind::kDisconnectNode;
  }
  if (name == "reconnect_node") {
    return SimulationCommandKind::kReconnectNode;
  }
  if (name == "set_block_production_policy") {
    return SimulationCommandKind::kSetBlockProductionPolicy;
  }
  if (name == "set_mining_difficulty") {
    return SimulationCommandKind::kSetMiningDifficulty;
  }
  if (name == "kill_node") {
    return SimulationCommandKind::kKillNode;
  }
  if (name == "connect_peer") {
    return SimulationCommandKind::kConnectPeer;
  }
  if (name == "disconnect_peer") {
    return SimulationCommandKind::kDisconnectPeer;
  }
  if (name == "set_peer_count_policy") {
    return SimulationCommandKind::kSetPeerCountPolicy;
  }
  if (name == "freeze_node") {
    return SimulationCommandKind::kFreezeNode;
  }
  if (name == "thaw_node") {
    return SimulationCommandKind::kThawNode;
  }
  if (name == "stop_node") {
    return SimulationCommandKind::kStopNode;
  }
  if (name == "restart_node") {
    return SimulationCommandKind::kRestartNode;
  }
  if (name == "generate_blocks") {
    return SimulationCommandKind::kGenerateBlocks;
  }
  if (name == "set_resource_profile") {
    return SimulationCommandKind::kSetResourceProfile;
  }
  if (name == "set_resource_limits") {
    return SimulationCommandKind::kSetResourceLimits;
  }
  if (name == "set_network_profile") {
    return SimulationCommandKind::kSetNetworkProfile;
  }
  if (name == "set_network_condition") {
    return SimulationCommandKind::kSetNetworkCondition;
  }
  if (name == "block_network_flow") {
    return SimulationCommandKind::kBlockNetworkFlow;
  }
  if (name == "unblock_network_flow") {
    return SimulationCommandKind::kUnblockNetworkFlow;
  }
  if (name == "partition_nodes") {
    return SimulationCommandKind::kPartitionNodes;
  }
  if (name == "heal_partition") {
    return SimulationCommandKind::kHealPartition;
  }
  if (name == "export_node_report") {
    return SimulationCommandKind::kExportNodeReport;
  }
  if (name == "set_perf_counters") {
    return SimulationCommandKind::kSetPerfCounters;
  }
  if (name == "send_wallet_transaction") {
    return SimulationCommandKind::kSendWalletTransaction;
  }
  return std::nullopt;
}

bool SimulationCommandRequiresConfirmation(SimulationCommandKind kind) {
  switch (kind) {
    case SimulationCommandKind::kStopMining:
    case SimulationCommandKind::kDisconnectNode:
    case SimulationCommandKind::kKillNode:
    case SimulationCommandKind::kDisconnectPeer:
    case SimulationCommandKind::kSetPeerCountPolicy:
    case SimulationCommandKind::kFreezeNode:
    case SimulationCommandKind::kStopNode:
    case SimulationCommandKind::kRestartNode:
    case SimulationCommandKind::kSetResourceProfile:
    case SimulationCommandKind::kSetResourceLimits:
    case SimulationCommandKind::kSetNetworkProfile:
    case SimulationCommandKind::kSetNetworkCondition:
    case SimulationCommandKind::kBlockNetworkFlow:
    case SimulationCommandKind::kPartitionNodes:
    case SimulationCommandKind::kGenerateBlocks:
    case SimulationCommandKind::kSendWalletTransaction:
      return true;
    case SimulationCommandKind::kIncreaseLogVerbosity:
    case SimulationCommandKind::kDecreaseLogVerbosity:
    case SimulationCommandKind::kReconnectNode:
    case SimulationCommandKind::kSetBlockProductionPolicy:
    case SimulationCommandKind::kSetMiningDifficulty:
    case SimulationCommandKind::kConnectPeer:
    case SimulationCommandKind::kThawNode:
    case SimulationCommandKind::kUnblockNetworkFlow:
    case SimulationCommandKind::kHealPartition:
    case SimulationCommandKind::kExportNodeReport:
    case SimulationCommandKind::kSetPerfCounters:
      return false;
    case SimulationCommandKind::kCount:
      break;
  }
  throw std::runtime_error("unknown simulation command kind");
}

}  // namespace bbp
