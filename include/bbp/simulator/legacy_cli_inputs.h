#pragma once

#include <string>
#include <vector>

namespace bbp {

struct LegacyCliInputs {
  std::vector<std::string> node_network_conditions;
  std::vector<std::string> runtime_node_network_conditions;
  std::vector<std::string> runtime_node_blocks;
  std::vector<std::string> runtime_node_unblocks;
  std::vector<std::string> runtime_partitions;
  std::vector<std::string> runtime_partition_heals;
  std::vector<std::string> runtime_node_resources;
  std::vector<std::string> runtime_node_restarts;
  std::vector<std::string> runtime_node_freezes;
};

}  // namespace bbp
