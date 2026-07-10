#pragma once

#include <optional>

#include "benchmark_sim/block_production_policy.h"
#include "benchmark_sim/mining_difficulty.h"
#include "benchmark_sim/simulation_command.h"

namespace bsim {

struct ParsedTuiCommand {
  SimulationCommandKind kind = SimulationCommandKind::kStopMining;
  std::optional<BlockProductionPolicy> block_production_policy;
  std::optional<MiningDifficulty> mining_difficulty;
};

}  // namespace bsim
