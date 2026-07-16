#pragma once

#include <cstdint>
#include <optional>
#include <string>
#include <string_view>

#include "bbp/block_production_policy.h"
#include "bbp/mining_difficulty.h"
#include "bbp/peer_count_policy.h"

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
  kSetNetworkProfile,
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
  bool confirmed = false;
};

std::string_view SimulationCommandKindName(SimulationCommandKind kind);
std::optional<SimulationCommandKind> SimulationCommandKindFromName(
    std::string_view name);
bool SimulationCommandRequiresConfirmation(SimulationCommandKind kind);

}  // namespace bbp
