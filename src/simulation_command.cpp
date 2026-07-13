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
  return std::nullopt;
}

}  // namespace bbp
