#pragma once

#include <cstdint>
#include <optional>
#include <string>
#include <string_view>

#include "benchmark_sim/block_production_policy.h"
#include "benchmark_sim/mining_difficulty.h"

namespace bsim {

enum class SimulationCommandKind {
  kIncreaseLogVerbosity,
  kDecreaseLogVerbosity,
  kStopMining,
  kDisconnectNode,
  kReconnectNode,
  kSetBlockProductionPolicy,
  kSetMiningDifficulty,
  kKillNode,
};

struct SimulationCommand {
  std::uint64_t sequence = 0;
  SimulationCommandKind kind = SimulationCommandKind::kIncreaseLogVerbosity;
  std::string node_id;
  std::optional<BlockProductionPolicy> block_production_policy;
  std::optional<MiningDifficulty> mining_difficulty;
};

std::string_view SimulationCommandKindName(SimulationCommandKind kind);

}  // namespace bsim
