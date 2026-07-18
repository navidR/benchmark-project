#pragma once

#include <cstdint>
#include <optional>
#include <string>
#include <vector>

#include "bbp/block_production_policy.h"
#include "bbp/mining_difficulty.h"
#include "bbp/network.h"
#include "bbp/peer_count_policy.h"
#include "bbp/simulation_command.h"
#include "bbp/simulator/resource_limit_patch.h"

namespace bbp {

enum class TuiPartitionTargetKind {
  kNodePair,
  kSelectedTopologyGroup,
};

struct ParsedTuiCommand {
  SimulationCommandKind kind = SimulationCommandKind::kStopMining;
  std::optional<BlockProductionPolicy> block_production_policy;
  std::optional<MiningDifficulty> mining_difficulty;
  std::optional<std::string> peer_node_id;
  std::optional<PeerCountPolicy> peer_count_policy;
  std::optional<std::uint32_t> block_count;
  std::optional<std::string> profile;
  std::optional<ResourceLimitPatch> resource_limit_patch;
  std::optional<NetworkCondition> network_condition;
  std::optional<SimulationNetworkFlow> network_flow;
  std::optional<TuiPartitionTargetKind> partition_target_kind;
  std::optional<PerfCounterTargetKind> perf_counter_target_kind;
  std::optional<std::string> perf_counter_target_id;
  std::vector<PerfCounterKind> perf_counter_kinds;
};

}  // namespace bbp
