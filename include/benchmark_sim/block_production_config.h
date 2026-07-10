#pragma once

#include <chrono>
#include <optional>

#include "benchmark_sim/block_production_policy.h"
#include "benchmark_sim/mining_difficulty.h"
#include "benchmark_sim/mining_mode.h"

namespace bsim {

struct BlockProductionConfig {
  bool enabled = true;
  MiningMode mode = MiningMode::kScheduledBlockProduction;
  BlockProductionPolicy policy{std::chrono::milliseconds(1000), 0.5, 0U};
  std::optional<MiningDifficulty> difficulty;
};

}  // namespace bsim
