#include "bbp/simulation_command_queue.h"

#include <limits>
#include <set>
#include <stdexcept>
#include <string_view>
#include <utility>

namespace bbp {
namespace {

std::set<std::string> ValidatePartitionGroup(
    const SimulationPartitionGroup& group, std::string_view name) {
  if (group.group_ids.empty()) {
    throw std::runtime_error("partition " + std::string(name) +
                             " requires a group id");
  }
  if (group.node_ids.empty()) {
    throw std::runtime_error("partition " + std::string(name) +
                             " requires a node id");
  }
  std::set<std::string> group_ids;
  for (const std::string& group_id : group.group_ids) {
    if (group_id.empty()) {
      throw std::runtime_error("partition " + std::string(name) +
                               " contains an empty group id");
    }
    if (!group_ids.insert(group_id).second) {
      throw std::runtime_error("partition " + std::string(name) +
                               " contains duplicate group ids");
    }
  }
  std::set<std::string> node_ids;
  for (const std::string& node_id : group.node_ids) {
    if (node_id.empty()) {
      throw std::runtime_error("partition " + std::string(name) +
                               " contains an empty node id");
    }
    if (!node_ids.insert(node_id).second) {
      throw std::runtime_error("partition " + std::string(name) +
                               " contains duplicate node ids");
    }
  }
  return node_ids;
}

void ValidatePartition(const SimulationPartition& partition) {
  const std::set<std::string> group_a_nodes =
      ValidatePartitionGroup(partition.group_a, "group A");
  const std::set<std::string> group_b_nodes =
      ValidatePartitionGroup(partition.group_b, "group B");
  for (const std::string& node_id : group_a_nodes) {
    if (group_b_nodes.contains(node_id)) {
      throw std::runtime_error("partition groups overlap at node " + node_id);
    }
  }
  if (partition.scope == SimulationPartitionScope::kNodePair) {
    if (partition.group_a.group_ids.size() != 1U ||
        partition.group_a.node_ids.size() != 1U ||
        partition.group_b.group_ids.size() != 1U ||
        partition.group_b.node_ids.size() != 1U) {
      throw std::runtime_error(
          "node-pair partition requires one node in each group");
    }
  } else if (partition.group_a.group_ids.size() != 1U) {
    throw std::runtime_error(
        "selected topology partition requires exactly one selected group");
  }
}

}  // namespace

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
    case SimulationCommandKind::kExportNodeReport:
      break;
    case SimulationCommandKind::kSetBlockProductionPolicy:
    case SimulationCommandKind::kSetMiningDifficulty:
    case SimulationCommandKind::kConnectPeer:
    case SimulationCommandKind::kDisconnectPeer:
    case SimulationCommandKind::kSetPeerCountPolicy:
    case SimulationCommandKind::kGenerateBlocks:
    case SimulationCommandKind::kSetResourceProfile:
    case SimulationCommandKind::kSetResourceLimits:
    case SimulationCommandKind::kSetNetworkProfile:
    case SimulationCommandKind::kSetNetworkCondition:
    case SimulationCommandKind::kBlockNetworkFlow:
    case SimulationCommandKind::kUnblockNetworkFlow:
    case SimulationCommandKind::kPartitionNodes:
    case SimulationCommandKind::kHealPartition:
    case SimulationCommandKind::kSetPerfCounters:
    case SimulationCommandKind::kSendWalletTransaction:
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
      .resource_limit_patch = std::nullopt,
      .network_condition = std::nullopt,
      .network_flow = std::nullopt,
      .partition = std::nullopt,
      .perf_counter_target = std::nullopt,
      .perf_counter_kinds = {},
      .wallet_send = std::nullopt,
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
      .resource_limit_patch = std::nullopt,
      .network_condition = std::nullopt,
      .network_flow = std::nullopt,
      .partition = std::nullopt,
      .perf_counter_target = std::nullopt,
      .perf_counter_kinds = {},
      .wallet_send = std::nullopt,
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
      .resource_limit_patch = std::nullopt,
      .network_condition = std::nullopt,
      .network_flow = std::nullopt,
      .partition = std::nullopt,
      .perf_counter_target = std::nullopt,
      .perf_counter_kinds = {},
      .wallet_send = std::nullopt,
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
      .resource_limit_patch = std::nullopt,
      .network_condition = std::nullopt,
      .network_flow = std::nullopt,
      .partition = std::nullopt,
      .perf_counter_target = std::nullopt,
      .perf_counter_kinds = {},
      .wallet_send = std::nullopt,
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
      .resource_limit_patch = std::nullopt,
      .network_condition = std::nullopt,
      .network_flow = std::nullopt,
      .partition = std::nullopt,
      .perf_counter_target = std::nullopt,
      .perf_counter_kinds = {},
      .wallet_send = std::nullopt,
      .confirmed = confirmed,
  });
}

std::uint64_t SimulationCommandQueue::PushGenerateBlocks(
    std::string node_id, std::uint32_t block_count, bool confirmed) {
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
      .resource_limit_patch = std::nullopt,
      .network_condition = std::nullopt,
      .network_flow = std::nullopt,
      .partition = std::nullopt,
      .perf_counter_target = std::nullopt,
      .perf_counter_kinds = {},
      .wallet_send = std::nullopt,
      .confirmed = confirmed,
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
      .resource_limit_patch = std::nullopt,
      .network_condition = std::nullopt,
      .network_flow = std::nullopt,
      .partition = std::nullopt,
      .perf_counter_target = std::nullopt,
      .perf_counter_kinds = {},
      .wallet_send = std::nullopt,
      .confirmed = confirmed,
  });
}

std::uint64_t SimulationCommandQueue::PushResourceLimits(
    std::string node_id, ResourceLimitPatch patch, bool confirmed) {
  ValidateResourceLimitPatch(patch, "operator resource update");
  if (patch.io_limits_present && patch.io_limits.size() != 1U) {
    throw std::runtime_error(
        "operator io resource update requires exactly one block device");
  }
  return PushCommand(SimulationCommand{
      .sequence = 0U,
      .kind = SimulationCommandKind::kSetResourceLimits,
      .node_id = std::move(node_id),
      .block_production_policy = std::nullopt,
      .mining_difficulty = std::nullopt,
      .peer_node_id = std::nullopt,
      .peer_count_policy = std::nullopt,
      .block_count = std::nullopt,
      .profile = std::nullopt,
      .resource_limit_patch = std::move(patch),
      .network_condition = std::nullopt,
      .network_flow = std::nullopt,
      .partition = std::nullopt,
      .perf_counter_target = std::nullopt,
      .perf_counter_kinds = {},
      .wallet_send = std::nullopt,
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
      .resource_limit_patch = std::nullopt,
      .network_condition = condition,
      .network_flow = std::nullopt,
      .partition = std::nullopt,
      .perf_counter_target = std::nullopt,
      .perf_counter_kinds = {},
      .wallet_send = std::nullopt,
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
      .resource_limit_patch = std::nullopt,
      .network_condition = std::nullopt,
      .network_flow = std::move(flow),
      .partition = std::nullopt,
      .perf_counter_target = std::nullopt,
      .perf_counter_kinds = {},
      .wallet_send = std::nullopt,
      .confirmed = confirmed,
  });
}

std::uint64_t SimulationCommandQueue::PushPartitionCommand(
    SimulationCommandKind kind, SimulationPartition partition, bool confirmed) {
  if (kind != SimulationCommandKind::kPartitionNodes &&
      kind != SimulationCommandKind::kHealPartition) {
    throw std::runtime_error(
        "partition command requires a partition or heal kind");
  }
  ValidatePartition(partition);
  const std::string event_node_id =
      partition.scope == SimulationPartitionScope::kNodePair
          ? partition.group_a.node_ids.front()
          : "sim";
  return PushCommand(SimulationCommand{
      .sequence = 0U,
      .kind = kind,
      .node_id = event_node_id,
      .block_production_policy = std::nullopt,
      .mining_difficulty = std::nullopt,
      .peer_node_id = std::nullopt,
      .peer_count_policy = std::nullopt,
      .block_count = std::nullopt,
      .profile = std::nullopt,
      .resource_limit_patch = std::nullopt,
      .network_condition = std::nullopt,
      .network_flow = std::nullopt,
      .partition = std::move(partition),
      .perf_counter_target = std::nullopt,
      .perf_counter_kinds = {},
      .wallet_send = std::nullopt,
      .confirmed = confirmed,
  });
}

std::uint64_t SimulationCommandQueue::PushPerfCounters(
    PerfCounterTarget target, std::vector<PerfCounterKind> kinds) {
  if (target.id.empty()) {
    throw std::runtime_error("perf counter target id must not be empty");
  }
  if (target.node_ids.empty()) {
    throw std::runtime_error("perf counter target must resolve to a node");
  }
  std::set<std::string> unique_nodes;
  for (const std::string& node_id : target.node_ids) {
    if (node_id.empty()) {
      throw std::runtime_error("perf counter target node id must not be empty");
    }
    if (!unique_nodes.insert(node_id).second) {
      throw std::runtime_error("perf counter target contains duplicate nodes");
    }
  }
  if (target.kind != PerfCounterTargetKind::kGroup &&
      target.node_ids.size() != 1U) {
    throw std::runtime_error(
        "non-group perf counter target must resolve to one node");
  }
  if (kinds.empty()) {
    throw std::runtime_error("perf counter selection must not be empty");
  }
  const std::set<PerfCounterKind> unique_kinds(kinds.begin(), kinds.end());
  if (unique_kinds.size() != kinds.size()) {
    throw std::runtime_error("perf counter selection contains duplicates");
  }
  const std::string event_node_id = target.kind == PerfCounterTargetKind::kGroup
                                        ? "sim"
                                        : target.node_ids.front();
  return PushCommand(SimulationCommand{
      .sequence = 0U,
      .kind = SimulationCommandKind::kSetPerfCounters,
      .node_id = event_node_id,
      .block_production_policy = std::nullopt,
      .mining_difficulty = std::nullopt,
      .peer_node_id = std::nullopt,
      .peer_count_policy = std::nullopt,
      .block_count = std::nullopt,
      .profile = std::nullopt,
      .resource_limit_patch = std::nullopt,
      .network_condition = std::nullopt,
      .network_flow = std::nullopt,
      .partition = std::nullopt,
      .perf_counter_target = std::move(target),
      .perf_counter_kinds = std::move(kinds),
      .wallet_send = std::nullopt,
      .confirmed = false,
  });
}

std::uint64_t SimulationCommandQueue::PushWalletSend(std::string sender_node_id,
                                                     SimulationWalletSend send,
                                                     bool confirmed) {
  if (send.sender_wallet_index == 0U) {
    throw std::runtime_error("wallet send requires a sender wallet");
  }
  if (send.receiver_wallet_index == 0U) {
    throw std::runtime_error("wallet send requires a receiver wallet");
  }
  if (send.sender_wallet_index == send.receiver_wallet_index) {
    throw std::runtime_error("wallet send source and receiver must differ");
  }
  if (send.amount_satoshis == 0U) {
    throw std::runtime_error("wallet send amount must be greater than zero");
  }
  if (send.amount_satoshis >
      std::numeric_limits<std::uint64_t>::max() - send.fee_satoshis) {
    throw std::runtime_error("wallet send amount plus fee overflows uint64");
  }
  if (send.timeout_sec == 0U) {
    throw std::runtime_error("wallet send timeout must be greater than zero");
  }
  return PushCommand(SimulationCommand{
      .sequence = 0U,
      .kind = SimulationCommandKind::kSendWalletTransaction,
      .node_id = std::move(sender_node_id),
      .block_production_policy = std::nullopt,
      .mining_difficulty = std::nullopt,
      .peer_node_id = std::nullopt,
      .peer_count_policy = std::nullopt,
      .block_count = std::nullopt,
      .profile = std::nullopt,
      .resource_limit_patch = std::nullopt,
      .network_condition = std::nullopt,
      .network_flow = std::nullopt,
      .partition = std::nullopt,
      .perf_counter_target = std::nullopt,
      .perf_counter_kinds = {},
      .wallet_send = send,
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
