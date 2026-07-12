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
};

struct SimulationCommand {
  std::uint64_t sequence = 0;
  SimulationCommandKind kind = SimulationCommandKind::kIncreaseLogVerbosity;
  std::string node_id;
  std::optional<BlockProductionPolicy> block_production_policy;
  std::optional<MiningDifficulty> mining_difficulty;
  std::optional<std::string> peer_node_id;
  std::optional<PeerCountPolicy> peer_count_policy;
};

std::string_view SimulationCommandKindName(SimulationCommandKind kind);

}  // namespace bbp
