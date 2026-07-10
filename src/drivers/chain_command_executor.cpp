#include "benchmark_sim/drivers/chain_command_executor.h"

#include <algorithm>
#include <stdexcept>
#include <utility>

namespace bsim {

ChainCommandExecutor::ChainCommandExecutor(
    const ChainDriver& driver, std::vector<ChainNodeConfig> nodes,
    StopMiningHandler stop_mining_handler,
    BlockProductionPolicyHandler policy_handler,
    MiningDifficultyHandler difficulty_handler)
    : driver_(driver),
      nodes_(std::move(nodes)),
      stop_mining_handler_(std::move(stop_mining_handler)),
      policy_handler_(std::move(policy_handler)),
      difficulty_handler_(std::move(difficulty_handler)) {
  if (!stop_mining_handler_ || !policy_handler_ || !difficulty_handler_) {
    throw std::runtime_error(
        "chain command executor requires mining control handlers");
  }
}

void ChainCommandExecutor::Execute(const SimulationCommand& command,
                                   std::stop_token stop_token) const {
  switch (command.kind) {
    case SimulationCommandKind::kIncreaseLogVerbosity:
      driver_.ChangeLogVerbosity(FindNode(command.node_id),
                                 ChainLogVerbosityChange::kIncrease,
                                 stop_token);
      return;
    case SimulationCommandKind::kDecreaseLogVerbosity:
      driver_.ChangeLogVerbosity(FindNode(command.node_id),
                                 ChainLogVerbosityChange::kDecrease,
                                 stop_token);
      return;
    case SimulationCommandKind::kStopMining:
      stop_mining_handler_(FindNode(command.node_id), stop_token);
      return;
    case SimulationCommandKind::kDisconnectNode:
      driver_.SetNetworkActive(FindNode(command.node_id), false, stop_token);
      return;
    case SimulationCommandKind::kReconnectNode:
      driver_.SetNetworkActive(FindNode(command.node_id), true, stop_token);
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
      difficulty_handler_(FindNode(command.node_id), *command.mining_difficulty,
                          stop_token);
      return;
    case SimulationCommandKind::kKillNode:
      throw std::runtime_error(
          "kill-node commands must be handled by the simulator process owner");
  }
  throw std::runtime_error("unknown simulation command kind");
}

const ChainNodeConfig& ChainCommandExecutor::FindNode(
    const std::string& node_id) const {
  const auto node = std::find_if(nodes_.begin(), nodes_.end(),
                                 [&node_id](const ChainNodeConfig& candidate) {
                                   return candidate.id == node_id;
                                 });
  if (node == nodes_.end()) {
    throw std::runtime_error("simulation command references unknown node: " +
                             node_id);
  }
  return *node;
}

}  // namespace bsim
