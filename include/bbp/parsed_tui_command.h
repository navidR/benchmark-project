#pragma once

#include <cstdint>
#include <optional>
#include <string>

#include "bbp/block_production_policy.h"
#include "bbp/mining_difficulty.h"
#include "bbp/peer_count_policy.h"
#include "bbp/simulation_command.h"

namespace bbp {

struct ParsedTuiCommand {
  SimulationCommandKind kind = SimulationCommandKind::kStopMining;
  std::optional<BlockProductionPolicy> block_production_policy;
  std::optional<MiningDifficulty> mining_difficulty;
  std::optional<std::string> peer_node_id;
  std::optional<PeerCountPolicy> peer_count_policy;
  std::optional<std::uint32_t> block_count;
  std::optional<std::string> profile;
};

}  // namespace bbp
