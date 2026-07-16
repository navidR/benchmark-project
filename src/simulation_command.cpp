#include "bbp/simulation_command.h"

#include <stdexcept>

namespace bbp {

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
    case SimulationCommandKind::kSetNetworkProfile:
      return "set_network_profile";
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
  if (name == "set_network_profile") {
    return SimulationCommandKind::kSetNetworkProfile;
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
    case SimulationCommandKind::kSetNetworkProfile:
      return true;
    case SimulationCommandKind::kIncreaseLogVerbosity:
    case SimulationCommandKind::kDecreaseLogVerbosity:
    case SimulationCommandKind::kReconnectNode:
    case SimulationCommandKind::kSetBlockProductionPolicy:
    case SimulationCommandKind::kSetMiningDifficulty:
    case SimulationCommandKind::kConnectPeer:
    case SimulationCommandKind::kThawNode:
    case SimulationCommandKind::kGenerateBlocks:
      return false;
  }
  throw std::runtime_error("unknown simulation command kind");
}

}  // namespace bbp
