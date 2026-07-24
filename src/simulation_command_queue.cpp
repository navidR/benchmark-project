#include "bbp/simulation_command_queue.h"

#include <algorithm>
#include <limits>
#include <set>
#include <stdexcept>
#include <string_view>
#include <utility>

namespace bbp {
namespace {

std::size_t SimulationCommandPayloadCount(const SimulationCommand& command) {
  return static_cast<std::size_t>(command.block_production_policy.has_value()) +
         static_cast<std::size_t>(command.mining_difficulty.has_value()) +
         static_cast<std::size_t>(command.peer_node_id.has_value()) +
         static_cast<std::size_t>(command.peer_count_policy.has_value()) +
         static_cast<std::size_t>(command.block_count.has_value()) +
         static_cast<std::size_t>(command.profile.has_value()) +
         static_cast<std::size_t>(command.resource_limit_patch.has_value()) +
         static_cast<std::size_t>(command.network_condition.has_value()) +
         static_cast<std::size_t>(command.network_flow.has_value()) +
         static_cast<std::size_t>(command.partition.has_value()) +
         static_cast<std::size_t>(command.perf_counter_target.has_value()) +
         static_cast<std::size_t>(!command.perf_counter_kinds.empty()) +
         static_cast<std::size_t>(command.wallet_send.has_value()) +
         static_cast<std::size_t>(command.node_add.has_value());
}

void RequirePayload(const SimulationCommand& command, bool expected_present,
                    std::size_t expected_count) {
  if (!expected_present ||
      SimulationCommandPayloadCount(command) != expected_count) {
    throw std::runtime_error(
        "simulation command has missing or unexpected typed payload fields");
  }
}

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

void ValidateNetworkFlowCommand(const SimulationCommand& command) {
  RequirePayload(command, command.network_flow.has_value(), 1U);
  const SimulationNetworkFlow& flow = *command.network_flow;
  const bool complete_match = !flow.dst_address.empty() && flow.dst_port != 0U;
  if (command.kind == SimulationCommandKind::kBlockNetworkFlow &&
      !complete_match) {
    throw std::runtime_error(
        "block network flow requires a destination address and port");
  }
  if (command.kind == SimulationCommandKind::kUnblockNetworkFlow &&
      flow.handle == 0U && !complete_match) {
    throw std::runtime_error(
        "unblock network flow requires a handle or complete match");
  }
  if (complete_match) {
    ValidateIpv4Address(flow.dst_address, "network flow destination");
  }
  if (!flow.src_address.empty()) {
    ValidateIpv4Address(flow.src_address, "network flow source");
  }
}

void ValidatePerfCounterCommand(const SimulationCommand& command) {
  RequirePayload(command,
                 command.perf_counter_target.has_value() &&
                     !command.perf_counter_kinds.empty(),
                 2U);
  const PerfCounterTarget& target = *command.perf_counter_target;
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
  const std::set<PerfCounterKind> unique_kinds(
      command.perf_counter_kinds.begin(), command.perf_counter_kinds.end());
  if (unique_kinds.size() != command.perf_counter_kinds.size()) {
    throw std::runtime_error("perf counter selection contains duplicates");
  }
  const std::string expected_node_id =
      target.kind == PerfCounterTargetKind::kGroup ? "sim"
                                                   : target.node_ids.front();
  if (command.node_id != expected_node_id) {
    throw std::runtime_error(
        "perf counter command node does not match its target");
  }
}

void ValidateWalletSendCommand(const SimulationCommand& command) {
  RequirePayload(command, command.wallet_send.has_value(), 1U);
  const SimulationWalletSend& send = *command.wallet_send;
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
}

void ValidateNodeAddCommand(const SimulationCommand& command) {
  RequirePayload(command, command.node_add.has_value(), 1U);
  if (command.node_id != "sim") {
    throw std::runtime_error("add-nodes command must target sim");
  }
  const SimulationNodeAddRequest& request = *command.node_add;
  switch (request.chain) {
    case ChainKind::kFiro:
    case ChainKind::kBitcoin:
    case ChainKind::kMonero:
      break;
    case ChainKind::kCount:
    default:
      throw std::runtime_error("add-nodes command requires a valid chain");
  }
  if (request.count == 0U || request.count > kSimulationNodeAddMaximumCount) {
    throw std::runtime_error("add-nodes count must be in 1.." +
                             std::to_string(kSimulationNodeAddMaximumCount));
  }
  if (!request.node_ids.empty() && request.node_ids.size() != request.count) {
    throw std::runtime_error(
        "add-nodes explicit node ids must match the requested count");
  }
  std::set<std::string> unique_node_ids;
  for (const std::string& node_id : request.node_ids) {
    if (node_id.empty()) {
      throw std::runtime_error("add-nodes explicit node id must not be empty");
    }
    if (!unique_node_ids.insert(node_id).second) {
      throw std::runtime_error("add-nodes explicit node ids must be unique");
    }
  }
  if (request.binary && request.binary->empty()) {
    throw std::runtime_error("add-nodes binary must not be empty");
  }
  if (request.ready_timeout_sec == 0U ||
      request.ready_timeout_sec > kSimulationNodeAddMaximumTimeoutSeconds) {
    throw std::runtime_error(
        "add-nodes ready timeout must be in 1.." +
        std::to_string(kSimulationNodeAddMaximumTimeoutSeconds));
  }
  if (request.sync_timeout_sec == 0U ||
      request.sync_timeout_sec > kSimulationNodeAddMaximumTimeoutSeconds) {
    throw std::runtime_error(
        "add-nodes sync timeout must be in 1.." +
        std::to_string(kSimulationNodeAddMaximumTimeoutSeconds));
  }
}

void ValidateSimulationCommand(const SimulationCommand& command) {
  if (command.sequence != 0U) {
    throw std::runtime_error(
        "simulation command sequence must be assigned by the queue");
  }
  if (command.node_id.empty() &&
      command.kind != SimulationCommandKind::kSetBlockProductionPolicy) {
    throw std::runtime_error("simulation command requires a node id");
  }
  if (SimulationCommandRequiresConfirmation(command.kind) &&
      !command.confirmed) {
    throw std::runtime_error("destructive simulation command is unconfirmed");
  }

  switch (command.kind) {
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
      RequirePayload(command, true, 0U);
      break;
    case SimulationCommandKind::kSetBlockProductionPolicy:
      RequirePayload(command, command.block_production_policy.has_value(), 1U);
      if (command.node_id != "sim") {
        throw std::runtime_error(
            "block production policy command must target sim");
      }
      break;
    case SimulationCommandKind::kSetMiningDifficulty:
      RequirePayload(command, command.mining_difficulty.has_value(), 1U);
      break;
    case SimulationCommandKind::kConnectPeer:
    case SimulationCommandKind::kDisconnectPeer:
      RequirePayload(command, command.peer_node_id.has_value(), 1U);
      if (command.peer_node_id->empty()) {
        throw std::runtime_error("peer command requires a target peer node id");
      }
      if (command.node_id == command.peer_node_id) {
        throw std::runtime_error("peer command source and target must differ");
      }
      break;
    case SimulationCommandKind::kSetPeerCountPolicy:
      RequirePayload(command, command.peer_count_policy.has_value(), 1U);
      break;
    case SimulationCommandKind::kGenerateBlocks:
      RequirePayload(command, command.block_count.has_value(), 1U);
      if (*command.block_count == 0U) {
        throw std::runtime_error("generate-blocks count must be positive");
      }
      break;
    case SimulationCommandKind::kSetResourceProfile:
    case SimulationCommandKind::kSetNetworkProfile:
      RequirePayload(command, command.profile.has_value(), 1U);
      if (command.profile->empty()) {
        throw std::runtime_error("profile command requires a profile name");
      }
      break;
    case SimulationCommandKind::kSetResourceLimits:
      RequirePayload(command, command.resource_limit_patch.has_value(), 1U);
      ValidateResourceLimitPatch(*command.resource_limit_patch,
                                 "operator resource update");
      if (command.resource_limit_patch->io_limits_present &&
          command.resource_limit_patch->io_limits.size() != 1U) {
        throw std::runtime_error(
            "operator io resource update requires exactly one block device");
      }
      break;
    case SimulationCommandKind::kSetNetworkCondition:
      RequirePayload(command, command.network_condition.has_value(), 1U);
      ValidateNetworkCondition(*command.network_condition);
      break;
    case SimulationCommandKind::kBlockNetworkFlow:
    case SimulationCommandKind::kUnblockNetworkFlow:
      ValidateNetworkFlowCommand(command);
      break;
    case SimulationCommandKind::kPartitionNodes:
    case SimulationCommandKind::kHealPartition:
      RequirePayload(command, command.partition.has_value(), 1U);
      ValidatePartition(*command.partition);
      break;
    case SimulationCommandKind::kSetPerfCounters:
      ValidatePerfCounterCommand(command);
      break;
    case SimulationCommandKind::kSendWalletTransaction:
      ValidateWalletSendCommand(command);
      break;
    case SimulationCommandKind::kAddNodes:
      ValidateNodeAddCommand(command);
      break;
    case SimulationCommandKind::kCount:
      throw std::runtime_error("unknown simulation command kind");
  }
}

}  // namespace

SimulationCommandQueue::SimulationCommandQueue(std::size_t capacity)
    : capacity_(capacity) {
  if (capacity_ == 0U) {
    throw std::runtime_error(
        "simulation command queue capacity must be greater than zero");
  }
}

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
    case SimulationCommandKind::kAddNodes:
    case SimulationCommandKind::kCount:
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
      .node_add = std::nullopt,
      .confirmed = confirmed,
      .scheduled_event_sequence = std::nullopt,
      .operation_control = nullptr,
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
      .node_add = std::nullopt,
      .confirmed = false,
      .scheduled_event_sequence = std::nullopt,
      .operation_control = nullptr,
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
      .node_add = std::nullopt,
      .confirmed = false,
      .scheduled_event_sequence = std::nullopt,
      .operation_control = nullptr,
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
      .node_add = std::nullopt,
      .confirmed = confirmed,
      .scheduled_event_sequence = std::nullopt,
      .operation_control = nullptr,
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
      .node_add = std::nullopt,
      .confirmed = confirmed,
      .scheduled_event_sequence = std::nullopt,
      .operation_control = nullptr,
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
      .node_add = std::nullopt,
      .confirmed = confirmed,
      .scheduled_event_sequence = std::nullopt,
      .operation_control = nullptr,
  });
}

std::uint64_t SimulationCommandQueue::PushScenarioCommand(
    SimulationCommand command) {
  if (!command.scheduled_event_sequence ||
      *command.scheduled_event_sequence == 0U) {
    throw std::runtime_error(
        "scenario command requires a scheduled event sequence");
  }
  if (!command.confirmed) {
    throw std::runtime_error("scenario command must be explicitly authorized");
  }
  return PushCommand(std::move(command));
}

std::uint64_t SimulationCommandQueue::PushRuntimeCommand(
    SimulationCommand command) {
  if (command.scheduled_event_sequence) {
    throw std::runtime_error(
        "runtime command must not contain a scheduled event sequence");
  }
  if (!command.confirmed) {
    throw std::runtime_error("runtime command must be explicitly authorized");
  }
  return PushCommand(std::move(command));
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
      .node_add = std::nullopt,
      .confirmed = confirmed,
      .scheduled_event_sequence = std::nullopt,
      .operation_control = nullptr,
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
      .node_add = std::nullopt,
      .confirmed = confirmed,
      .scheduled_event_sequence = std::nullopt,
      .operation_control = nullptr,
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
      .node_add = std::nullopt,
      .confirmed = confirmed,
      .scheduled_event_sequence = std::nullopt,
      .operation_control = nullptr,
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
      .node_add = std::nullopt,
      .confirmed = confirmed,
      .scheduled_event_sequence = std::nullopt,
      .operation_control = nullptr,
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
      .node_add = std::nullopt,
      .confirmed = confirmed,
      .scheduled_event_sequence = std::nullopt,
      .operation_control = nullptr,
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
      .node_add = std::nullopt,
      .confirmed = false,
      .scheduled_event_sequence = std::nullopt,
      .operation_control = nullptr,
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
      .node_add = std::nullopt,
      .confirmed = confirmed,
      .scheduled_event_sequence = std::nullopt,
      .operation_control = nullptr,
  });
}

std::uint64_t SimulationCommandQueue::PushAddNodes(
    SimulationNodeAddRequest request) {
  return PushCommand(SimulationCommand{
      .sequence = 0U,
      .kind = SimulationCommandKind::kAddNodes,
      .node_id = "sim",
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
      .node_add = std::move(request),
      .confirmed = false,
      .scheduled_event_sequence = std::nullopt,
      .operation_control = nullptr,
  });
}

std::uint64_t SimulationCommandQueue::PushCommand(SimulationCommand command) {
  ValidateSimulationCommand(command);
  if (command.kind == SimulationCommandKind::kAddNodes &&
      !command.operation_control) {
    command.operation_control = std::make_shared<SimulationCommandControl>();
  }

  std::lock_guard<std::mutex> lock(mutex_);
  if (closed_) {
    throw std::runtime_error("simulation command queue is closed");
  }
  if (commands_.size() >= capacity_) {
    if (rejected_ == std::numeric_limits<std::uint64_t>::max()) {
      throw std::runtime_error(
          "simulation command queue rejection count exceeds uint64");
    }
    ++rejected_;
    throw std::runtime_error("simulation command queue is full (capacity " +
                             std::to_string(capacity_) + ")");
  }
  if (next_sequence_ == std::numeric_limits<std::uint64_t>::max()) {
    throw std::runtime_error("simulation command sequence exceeds uint64");
  }
  const std::uint64_t sequence = next_sequence_++;
  command.sequence = sequence;
  commands_.push_back(std::move(command));
  maximum_size_ = std::max(maximum_size_, commands_.size());
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

std::vector<SimulationCommand> SimulationCommandQueue::Cancel() {
  std::vector<SimulationCommand> cancelled;
  {
    std::lock_guard<std::mutex> lock(mutex_);
    closed_ = true;
    cancelled.reserve(commands_.size());
    while (!commands_.empty()) {
      cancelled.push_back(std::move(commands_.front()));
      commands_.pop_front();
    }
  }
  ready_.notify_all();
  return cancelled;
}

bool SimulationCommandQueue::IsClosed() const {
  std::lock_guard<std::mutex> lock(mutex_);
  return closed_;
}

SimulationCommandQueueStats SimulationCommandQueue::Stats() const {
  std::lock_guard<std::mutex> lock(mutex_);
  return SimulationCommandQueueStats{
      .size = commands_.size(),
      .capacity = capacity_,
      .maximum_size = maximum_size_,
      .rejected = rejected_,
  };
}

}  // namespace bbp
