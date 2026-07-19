#pragma once

#include <cstdint>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

#include "bbp/block_production_policy.h"
#include "bbp/mining_difficulty.h"
#include "bbp/network.h"
#include "bbp/peer_count_policy.h"
#include "bbp/perf_counter.h"
#include "bbp/simulation_partition.h"
#include "bbp/simulation_wallet_send.h"
#include "bbp/simulator/resource_limit_patch.h"

namespace bbp {

enum class SimulationCommandKind {
  kIncreaseLogVerbosity,
  kDecreaseLogVerbosity,
  kStopMining,
  kDisconnectNode,
  kReconnectNode,
  kSetBlockProductionPolicy,
  kSetMiningDifficulty,
  kKillNode,
  kConnectPeer,
  kDisconnectPeer,
  kSetPeerCountPolicy,
  kFreezeNode,
  kThawNode,
  kStopNode,
  kRestartNode,
  kGenerateBlocks,
  kSetResourceProfile,
  kSetResourceLimits,
  kSetNetworkProfile,
  kSetNetworkCondition,
  kBlockNetworkFlow,
  kUnblockNetworkFlow,
  kPartitionNodes,
  kHealPartition,
  kExportNodeReport,
  kSetPerfCounters,
  kSendWalletTransaction,
};

struct SimulationNetworkFlow {
  std::string src_address;
  std::uint16_t src_port = 0;
  std::string dst_address;
  std::uint16_t dst_port = 0;
  std::uint32_t handle = 0;
};

struct SimulationCommand {
  std::uint64_t sequence = 0;
  SimulationCommandKind kind = SimulationCommandKind::kIncreaseLogVerbosity;
  std::string node_id;
  std::optional<BlockProductionPolicy> block_production_policy;
  std::optional<MiningDifficulty> mining_difficulty;
  std::optional<std::string> peer_node_id;
  std::optional<PeerCountPolicy> peer_count_policy;
  std::optional<std::uint32_t> block_count;
  std::optional<std::string> profile;
  std::optional<ResourceLimitPatch> resource_limit_patch;
  std::optional<NetworkCondition> network_condition;
  std::optional<SimulationNetworkFlow> network_flow;
  std::optional<SimulationPartition> partition;
  std::optional<PerfCounterTarget> perf_counter_target;
  std::vector<PerfCounterKind> perf_counter_kinds;
  std::optional<SimulationWalletSend> wallet_send;
  bool confirmed = false;
  std::optional<std::uint32_t> scheduled_event_sequence;
};

std::string_view SimulationCommandKindName(SimulationCommandKind kind);
std::optional<SimulationCommandKind> SimulationCommandKindFromName(
    std::string_view name);
bool SimulationCommandRequiresConfirmation(SimulationCommandKind kind);

}  // namespace bbp
