#pragma once

#include <vector>

#include "bbp/simulation_partition.h"
#include "bbp/simulator/network_block_rule.h"
#include "bbp/simulator/network_partition_rule.h"

namespace bbp {

class RuntimeNodeSnapshot;

namespace simulator_app_internal {

NetworkPartitionRule RuntimePartitionRule(const SimulationPartition& partition,
                                          const RuntimeNodeSnapshot& nodes);

std::vector<NetworkBlockRule> PartitionBlockRules(
    const NetworkPartitionRule& partition, const RuntimeNodeSnapshot& nodes);

}  // namespace simulator_app_internal
}  // namespace bbp
