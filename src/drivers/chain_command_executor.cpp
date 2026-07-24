#include "bbp/drivers/chain_command_executor.h"

#include <algorithm>
#include <stdexcept>
#include <string>
#include <utility>

namespace bbp {
namespace {

const std::string& RequirePeerNodeId(const SimulationCommand& command) {
  if (!command.peer_node_id || command.peer_node_id->empty()) {
    throw std::runtime_error("peer command requires a target peer node id");
  }
  return *command.peer_node_id;
}

}  // namespace

ChainCommandExecutor::ChainCommandExecutor(
    const ChainDriver& driver, std::vector<ChainNodeConfig> nodes,
    StopMiningHandler stop_mining_handler,
    BlockProductionPolicyHandler policy_handler,
    MiningDifficultyHandler difficulty_handler,
    PeerHandler connect_peer_handler, PeerHandler disconnect_peer_handler,
    PeerCountPolicyHandler peer_count_policy_handler)
    : ChainCommandExecutor(
          driver,
          [nodes = std::move(nodes)] { return NodeConfigSnapshot(nodes); },
          std::move(stop_mining_handler), std::move(policy_handler),
          std::move(difficulty_handler), std::move(connect_peer_handler),
          std::move(disconnect_peer_handler),
          std::move(peer_count_policy_handler)) {}

ChainCommandExecutor::ChainCommandExecutor(
    const ChainDriver& driver, NodeProvider node_provider,
    StopMiningHandler stop_mining_handler,
    BlockProductionPolicyHandler policy_handler,
    MiningDifficultyHandler difficulty_handler,
    PeerHandler connect_peer_handler, PeerHandler disconnect_peer_handler,
    PeerCountPolicyHandler peer_count_policy_handler)
    : driver_(driver),
      node_provider_(std::move(node_provider)),
      stop_mining_handler_(std::move(stop_mining_handler)),
      policy_handler_(std::move(policy_handler)),
      difficulty_handler_(std::move(difficulty_handler)),
      connect_peer_handler_(std::move(connect_peer_handler)),
      disconnect_peer_handler_(std::move(disconnect_peer_handler)),
      peer_count_policy_handler_(std::move(peer_count_policy_handler)) {
  if (!node_provider_ || !stop_mining_handler_ || !policy_handler_ ||
      !difficulty_handler_ || !connect_peer_handler_ ||
      !disconnect_peer_handler_ || !peer_count_policy_handler_) {
    throw std::runtime_error("chain command executor requires all handlers");
  }
}

void ChainCommandExecutor::Execute(const SimulationCommand& command,
                                   std::stop_token stop_token) const {
  const NodeConfigSnapshot snapshot = node_provider_();
  const std::vector<ChainNodeConfig>& nodes = snapshot.nodes();
  switch (command.kind) {
    case SimulationCommandKind::kIncreaseLogVerbosity:
      driver_.ChangeLogVerbosity(FindNode(nodes, command.node_id),
                                 ChainLogVerbosityChange::kIncrease,
                                 stop_token);
      return;
    case SimulationCommandKind::kDecreaseLogVerbosity:
      driver_.ChangeLogVerbosity(FindNode(nodes, command.node_id),
                                 ChainLogVerbosityChange::kDecrease,
                                 stop_token);
      return;
    case SimulationCommandKind::kStopMining:
      stop_mining_handler_(FindNode(nodes, command.node_id), stop_token);
      return;
    case SimulationCommandKind::kDisconnectNode:
      driver_.SetNetworkActive(FindNode(nodes, command.node_id), false,
                               stop_token);
      return;
    case SimulationCommandKind::kReconnectNode:
      driver_.SetNetworkActive(FindNode(nodes, command.node_id), true,
                               stop_token);
      return;
    case SimulationCommandKind::kSetBlockProductionPolicy:
      if (!command.block_production_policy) {
        throw std::runtime_error(
            "set-block-production-policy command requires a policy");
      }
      policy_handler_(*command.block_production_policy);
      return;
    case SimulationCommandKind::kSetMiningDifficulty:
      if (!command.mining_difficulty) {
        throw std::runtime_error(
            "set-mining-difficulty command requires a difficulty");
      }
      difficulty_handler_(FindNode(nodes, command.node_id),
                          *command.mining_difficulty, stop_token);
      return;
    case SimulationCommandKind::kKillNode:
      throw std::runtime_error(
          "kill-node commands must be handled by the simulator process owner");
    case SimulationCommandKind::kConnectPeer: {
      const ChainNodeConfig& node = FindNode(nodes, command.node_id);
      const ChainNodeConfig& peer = FindNode(nodes, RequirePeerNodeId(command));
      if (node.id == peer.id) {
        throw std::runtime_error("peer command source and target must differ");
      }
      connect_peer_handler_(node, peer, stop_token);
      return;
    }
    case SimulationCommandKind::kDisconnectPeer: {
      const ChainNodeConfig& node = FindNode(nodes, command.node_id);
      const ChainNodeConfig& peer = FindNode(nodes, RequirePeerNodeId(command));
      if (node.id == peer.id) {
        throw std::runtime_error("peer command source and target must differ");
      }
      disconnect_peer_handler_(node, peer, stop_token);
      return;
    }
    case SimulationCommandKind::kSetPeerCountPolicy: {
      if (!command.peer_count_policy) {
        throw std::runtime_error(
            "set-peer-count-policy command requires a policy");
      }
      peer_count_policy_handler_(FindNode(nodes, command.node_id),
                                 *command.peer_count_policy);
      return;
    }
    case SimulationCommandKind::kFreezeNode:
    case SimulationCommandKind::kThawNode:
    case SimulationCommandKind::kStopNode:
    case SimulationCommandKind::kRestartNode:
    case SimulationCommandKind::kGenerateBlocks:
    case SimulationCommandKind::kSetResourceProfile:
    case SimulationCommandKind::kSetResourceLimits:
    case SimulationCommandKind::kSetNetworkProfile:
    case SimulationCommandKind::kSetNetworkCondition:
    case SimulationCommandKind::kBlockNetworkFlow:
    case SimulationCommandKind::kUnblockNetworkFlow:
    case SimulationCommandKind::kPartitionNodes:
    case SimulationCommandKind::kHealPartition:
    case SimulationCommandKind::kExportNodeReport:
    case SimulationCommandKind::kSetPerfCounters:
    case SimulationCommandKind::kSendWalletTransaction:
    case SimulationCommandKind::kAddNodes:
    case SimulationCommandKind::kCount:
      throw std::runtime_error(
          "command must be handled by the simulator resource owner");
  }
  throw std::runtime_error("unknown simulation command kind");
}

const ChainNodeConfig& ChainCommandExecutor::FindNode(
    const std::vector<ChainNodeConfig>& nodes, const std::string& node_id) {
  const auto node = std::find_if(nodes.begin(), nodes.end(),
                                 [&node_id](const ChainNodeConfig& candidate) {
                                   return candidate.id == node_id;
                                 });
  if (node == nodes.end()) {
    throw std::runtime_error("simulation command references unknown node: " +
                             node_id);
  }
  return *node;
}

}  // namespace bbp
