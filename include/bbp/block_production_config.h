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
  std::uint32_t min_pool_transactions = 0;
  std::uint32_t max_pool_wait_ms = 0;
};

}  // namespace bbp
