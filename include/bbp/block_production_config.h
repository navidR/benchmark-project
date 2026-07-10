#pragma once

#include <chrono>
#include <optional>

#include "bbp/block_production_policy.h"
#include "bbp/mining_difficulty.h"
#include "bbp/mining_mode.h"

namespace bbp {

struct BlockProductionConfig {
  bool enabled = true;
  MiningMode mode = MiningMode::kScheduledBlockProduction;
  BlockProductionPolicy policy{std::chrono::milliseconds(1000), 0.5, 0U};
  std::optional<MiningDifficulty> difficulty;
};

}  // namespace bbp
