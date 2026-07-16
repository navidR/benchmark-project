#include "bbp/simulation_command_queue.h"

#include <stdexcept>
#include <utility>

namespace bbp {

std::uint64_t SimulationCommandQueue::Push(SimulationCommandKind kind,
                                           std::string node_id,
                                           bool confirmed) {
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
      break;
    case SimulationCommandKind::kSetBlockProductionPolicy:
    case SimulationCommandKind::kSetMiningDifficulty:
    case SimulationCommandKind::kConnectPeer:
    case SimulationCommandKind::kDisconnectPeer:
    case SimulationCommandKind::kSetPeerCountPolicy:
    case SimulationCommandKind::kGenerateBlocks:
    case SimulationCommandKind::kSetResourceProfile:
    case SimulationCommandKind::kSetNetworkProfile:
    case SimulationCommandKind::kSetNetworkCondition:
    case SimulationCommandKind::kBlockNetworkFlow:
    case SimulationCommandKind::kUnblockNetworkFlow:
    case SimulationCommandKind::kPartitionNodes:
    case SimulationCommandKind::kHealPartition:
      throw std::runtime_error(
          "simulation command kind requires a typed payload method");
  }
  return PushCommand(SimulationCommand{
      .sequence = 0U,
      .kind = kind,
      .node_id = std::move(node_id),
      .block_production_policy = std::nullopt,
      .mining_difficulty = std::nullopt,
      .peer_node_id = std::nullopt,
      .peer_count_policy = std::nullopt,
      .block_count = std::nullopt,
      .profile = std::nullopt,
      .network_condition = std::nullopt,
      .network_flow = std::nullopt,
      .confirmed = confirmed,
  });
}

std::uint64_t SimulationCommandQueue::PushBlockProductionPolicy(
    BlockProductionPolicy policy) {
  return PushCommand(SimulationCommand{
      .sequence = 0U,
      .kind = SimulationCommandKind::kSetBlockProductionPolicy,
      .node_id = "sim",
      .block_production_policy = policy,
      .mining_difficulty = std::nullopt,
      .peer_node_id = std::nullopt,
      .peer_count_policy = std::nullopt,
      .block_count = std::nullopt,
      .profile = std::nullopt,
      .network_condition = std::nullopt,
      .network_flow = std::nullopt,
      .confirmed = false,
  });
}

std::uint64_t SimulationCommandQueue::PushMiningDifficulty(
    std::string node_id, MiningDifficulty difficulty) {
  return PushCommand(SimulationCommand{
      .sequence = 0U,
      .kind = SimulationCommandKind::kSetMiningDifficulty,
      .node_id = std::move(node_id),
      .block_production_policy = std::nullopt,
      .mining_difficulty = difficulty,
      .peer_node_id = std::nullopt,
      .peer_count_policy = std::nullopt,
      .block_count = std::nullopt,
      .profile = std::nullopt,
      .network_condition = std::nullopt,
      .network_flow = std::nullopt,
      .confirmed = false,
  });
}

std::uint64_t SimulationCommandQueue::PushPeerCommand(
    SimulationCommandKind kind, std::string node_id, std::string peer_node_id,
    bool confirmed) {
  if (kind != SimulationCommandKind::kConnectPeer &&
      kind != SimulationCommandKind::kDisconnectPeer) {
    throw std::runtime_error("peer command requires a peer command kind");
  }
  if (peer_node_id.empty()) {
    throw std::runtime_error("peer command requires a target peer node id");
  }
  if (node_id == peer_node_id) {
    throw std::runtime_error("peer command source and target must differ");
  }
  return PushCommand(SimulationCommand{
      .sequence = 0U,
      .kind = kind,
      .node_id = std::move(node_id),
      .block_production_policy = std::nullopt,
      .mining_difficulty = std::nullopt,
      .peer_node_id = std::move(peer_node_id),
      .peer_count_policy = std::nullopt,
      .block_count = std::nullopt,
      .profile = std::nullopt,
      .network_condition = std::nullopt,
      .network_flow = std::nullopt,
      .confirmed = confirmed,
  });
}

std::uint64_t SimulationCommandQueue::PushPeerCountPolicy(
    std::string node_id, PeerCountPolicy policy, bool confirmed) {
  return PushCommand(SimulationCommand{
      .sequence = 0U,
      .kind = SimulationCommandKind::kSetPeerCountPolicy,
      .node_id = std::move(node_id),
      .block_production_policy = std::nullopt,
      .mining_difficulty = std::nullopt,
      .peer_node_id = std::nullopt,
      .peer_count_policy = policy,
      .block_count = std::nullopt,
      .profile = std::nullopt,
      .network_condition = std::nullopt,
      .network_flow = std::nullopt,
      .confirmed = confirmed,
  });
}

std::uint64_t SimulationCommandQueue::PushGenerateBlocks(
    std::string node_id, std::uint32_t block_count) {
  if (block_count == 0U) {
    throw std::runtime_error("generate-blocks count must be positive");
  }
  return PushCommand(SimulationCommand{
      .sequence = 0U,
      .kind = SimulationCommandKind::kGenerateBlocks,
      .node_id = std::move(node_id),
      .block_production_policy = std::nullopt,
      .mining_difficulty = std::nullopt,
      .peer_node_id = std::nullopt,
      .peer_count_policy = std::nullopt,
      .block_count = block_count,
      .profile = std::nullopt,
      .network_condition = std::nullopt,
      .network_flow = std::nullopt,
      .confirmed = false,
  });
}

std::uint64_t SimulationCommandQueue::PushProfileCommand(
    SimulationCommandKind kind, std::string node_id, std::string profile,
    bool confirmed) {
  if (kind != SimulationCommandKind::kSetResourceProfile &&
      kind != SimulationCommandKind::kSetNetworkProfile) {
    throw std::runtime_error("profile command requires a profile command kind");
  }
  if (profile.empty()) {
    throw std::runtime_error("profile command requires a profile name");
  }
  return PushCommand(SimulationCommand{
      .sequence = 0U,
      .kind = kind,
      .node_id = std::move(node_id),
      .block_production_policy = std::nullopt,
      .mining_difficulty = std::nullopt,
      .peer_node_id = std::nullopt,
      .peer_count_policy = std::nullopt,
      .block_count = std::nullopt,
      .profile = std::move(profile),
      .network_condition = std::nullopt,
      .network_flow = std::nullopt,
      .confirmed = confirmed,
  });
}

std::uint64_t SimulationCommandQueue::PushNetworkCondition(
    std::string node_id, NetworkCondition condition, bool confirmed) {
  ValidateNetworkCondition(condition);
  return PushCommand(SimulationCommand{
      .sequence = 0U,
      .kind = SimulationCommandKind::kSetNetworkCondition,
      .node_id = std::move(node_id),
      .block_production_policy = std::nullopt,
      .mining_difficulty = std::nullopt,
      .peer_node_id = std::nullopt,
      .peer_count_policy = std::nullopt,
      .block_count = std::nullopt,
      .profile = std::nullopt,
      .network_condition = condition,
      .network_flow = std::nullopt,
      .confirmed = confirmed,
  });
}

std::uint64_t SimulationCommandQueue::PushNetworkFlowCommand(
    SimulationCommandKind kind, std::string node_id, SimulationNetworkFlow flow,
    bool confirmed) {
  if (kind != SimulationCommandKind::kBlockNetworkFlow &&
      kind != SimulationCommandKind::kUnblockNetworkFlow) {
    throw std::runtime_error(
        "network flow command requires a block or unblock kind");
  }
  const bool complete_match = !flow.dst_address.empty() && flow.dst_port != 0U;
  if (kind == SimulationCommandKind::kBlockNetworkFlow && !complete_match) {
    throw std::runtime_error(
        "block network flow requires a destination address and port");
  }
  if (kind == SimulationCommandKind::kUnblockNetworkFlow && flow.handle == 0U &&
      !complete_match) {
    throw std::runtime_error(
        "unblock network flow requires a handle or complete match");
  }
  if (complete_match) {
    ValidateIpv4Address(flow.dst_address, "network flow destination");
  }
  if (!flow.src_address.empty()) {
    ValidateIpv4Address(flow.src_address, "network flow source");
  }
  return PushCommand(SimulationCommand{
      .sequence = 0U,
      .kind = kind,
      .node_id = std::move(node_id),
      .block_production_policy = std::nullopt,
      .mining_difficulty = std::nullopt,
      .peer_node_id = std::nullopt,
      .peer_count_policy = std::nullopt,
      .block_count = std::nullopt,
      .profile = std::nullopt,
      .network_condition = std::nullopt,
      .network_flow = std::move(flow),
      .confirmed = confirmed,
  });
}

std::uint64_t SimulationCommandQueue::PushPartitionCommand(
    SimulationCommandKind kind, std::string node_id, std::string peer_node_id,
    bool confirmed) {
  if (kind != SimulationCommandKind::kPartitionNodes &&
      kind != SimulationCommandKind::kHealPartition) {
    throw std::runtime_error(
        "partition command requires a partition or heal kind");
  }
  if (peer_node_id.empty()) {
    throw std::runtime_error("partition command requires a peer node id");
  }
  if (node_id == peer_node_id) {
    throw std::runtime_error("partition command nodes must differ");
  }
  return PushCommand(SimulationCommand{
      .sequence = 0U,
      .kind = kind,
      .node_id = std::move(node_id),
      .block_production_policy = std::nullopt,
      .mining_difficulty = std::nullopt,
      .peer_node_id = std::move(peer_node_id),
      .peer_count_policy = std::nullopt,
      .block_count = std::nullopt,
      .profile = std::nullopt,
      .network_condition = std::nullopt,
      .network_flow = std::nullopt,
      .confirmed = confirmed,
  });
}

std::uint64_t SimulationCommandQueue::PushCommand(SimulationCommand command) {
  const std::string& node_id = command.node_id;
  if (node_id.empty() &&
      command.kind != SimulationCommandKind::kSetBlockProductionPolicy) {
    throw std::runtime_error("simulation command requires a node id");
  }
  if (SimulationCommandRequiresConfirmation(command.kind) &&
      !command.confirmed) {
    throw std::runtime_error("destructive simulation command is unconfirmed");
  }

  std::lock_guard<std::mutex> lock(mutex_);
  if (closed_) {
    throw std::runtime_error("simulation command queue is closed");
  }
  const std::uint64_t sequence = next_sequence_++;
  command.sequence = sequence;
  commands_.push_back(std::move(command));
  ready_.notify_one();
  return sequence;
}

std::optional<SimulationCommand> SimulationCommandQueue::TryPop() {
  std::lock_guard<std::mutex> lock(mutex_);
  if (commands_.empty()) {
    return std::nullopt;
  }
  SimulationCommand command = std::move(commands_.front());
  commands_.pop_front();
  return command;
}

std::optional<SimulationCommand> SimulationCommandQueue::WaitPop() {
  std::unique_lock<std::mutex> lock(mutex_);
  ready_.wait(lock, [this] { return closed_ || !commands_.empty(); });
  if (commands_.empty()) {
    return std::nullopt;
  }
  SimulationCommand command = std::move(commands_.front());
  commands_.pop_front();
  return command;
}

void SimulationCommandQueue::Close() {
  {
    std::lock_guard<std::mutex> lock(mutex_);
    closed_ = true;
  }
  ready_.notify_all();
}

void SimulationCommandQueue::Cancel() {
  {
    std::lock_guard<std::mutex> lock(mutex_);
    closed_ = true;
    commands_.clear();
  }
  ready_.notify_all();
}

bool SimulationCommandQueue::IsClosed() const {
  std::lock_guard<std::mutex> lock(mutex_);
  return closed_;
}

}  // namespace bbp
