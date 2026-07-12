#include "bbp/simulation_command_queue.h"

#include <stdexcept>
#include <utility>

namespace bbp {

std::uint64_t SimulationCommandQueue::Push(SimulationCommandKind kind,
                                           std::string node_id) {
  return PushCommand(SimulationCommand{
      .sequence = 0U,
      .kind = kind,
      .node_id = std::move(node_id),
      .block_production_policy = std::nullopt,
      .mining_difficulty = std::nullopt,
      .peer_node_id = std::nullopt,
      .peer_count_policy = std::nullopt,
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
  });
}

std::uint64_t SimulationCommandQueue::PushPeerCommand(
    SimulationCommandKind kind, std::string node_id, std::string peer_node_id) {
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
  });
}

std::uint64_t SimulationCommandQueue::PushPeerCountPolicy(
    std::string node_id, PeerCountPolicy policy) {
  return PushCommand(SimulationCommand{
      .sequence = 0U,
      .kind = SimulationCommandKind::kSetPeerCountPolicy,
      .node_id = std::move(node_id),
      .block_production_policy = std::nullopt,
      .mining_difficulty = std::nullopt,
      .peer_node_id = std::nullopt,
      .peer_count_policy = policy,
  });
}

std::uint64_t SimulationCommandQueue::PushCommand(SimulationCommand command) {
  const std::string& node_id = command.node_id;
  if (node_id.empty() &&
      command.kind != SimulationCommandKind::kSetBlockProductionPolicy) {
    throw std::runtime_error("simulation command requires a node id");
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
