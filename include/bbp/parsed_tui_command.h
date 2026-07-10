#pragma once

#include <optional>

#include "bbp/block_production_policy.h"
#include "bbp/mining_difficulty.h"
#include "bbp/simulation_command.h"

namespace bbp {

struct ParsedTuiCommand {
  SimulationCommandKind kind = SimulationCommandKind::kStopMining;
  std::optional<BlockProductionPolicy> block_production_policy;
  std::optional<MiningDifficulty> mining_difficulty;
};

}  // namespace bbp
