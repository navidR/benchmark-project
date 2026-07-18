#pragma once

#include <string>
#include <string_view>
#include <vector>

namespace bbp {

enum class SimulationPartitionScope {
  kNodePair,
  kPartitionGroup,
  kRegion,
  kRole,
};

constexpr std::string_view SimulationPartitionScopeName(
    SimulationPartitionScope scope) {
  switch (scope) {
    case SimulationPartitionScope::kNodePair:
      return "node_pair";
    case SimulationPartitionScope::kPartitionGroup:
      return "partition_group";
    case SimulationPartitionScope::kRegion:
      return "region";
    case SimulationPartitionScope::kRole:
      return "role";
  }
  return "unknown";
}

struct SimulationPartitionGroup {
  std::vector<std::string> group_ids;
  std::vector<std::string> node_ids;

  auto operator<=>(const SimulationPartitionGroup&) const = default;
};

struct SimulationPartition {
  SimulationPartitionScope scope = SimulationPartitionScope::kNodePair;
  SimulationPartitionGroup group_a;
  SimulationPartitionGroup group_b;

  auto operator<=>(const SimulationPartition&) const = default;
};

}  // namespace bbp
