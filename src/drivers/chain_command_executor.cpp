#include "benchmark_sim/drivers/chain_command_executor.h"

#include <algorithm>
#include <stdexcept>
#include <utility>

namespace bsim {

ChainCommandExecutor::ChainCommandExecutor(const ChainDriver& driver,
                                           std::vector<ChainNodeConfig> nodes)
    : driver_(driver), nodes_(std::move(nodes)) {}

void ChainCommandExecutor::Execute(const SimulationCommand& command) const {
  const ChainNodeConfig& node = FindNode(command.node_id);
  switch (command.kind) {
    case SimulationCommandKind::kIncreaseLogVerbosity:
      driver_.ChangeLogVerbosity(node, ChainLogVerbosityChange::kIncrease);
      return;
    case SimulationCommandKind::kDecreaseLogVerbosity:
      driver_.ChangeLogVerbosity(node, ChainLogVerbosityChange::kDecrease);
      return;
    case SimulationCommandKind::kStopMining:
      driver_.StopMining(node);
      return;
    case SimulationCommandKind::kDisconnectNode:
      driver_.SetNetworkActive(node, false);
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
