#include "simulator_network_partition_planning.h"

#include <algorithm>
#include <cstdint>
#include <iterator>
#include <limits>
#include <stdexcept>
#include <string>
#include <string_view>
#include <vector>

#include "bbp/runtime_node_inventory.h"
#include "simulator_network_rule_decoding.h"

namespace bbp::simulator_app_internal {
namespace {

std::uint32_t PartitionNodeIndex(const RuntimeNodeSnapshot& nodes,
                                 std::string_view node_id) {
  const auto node = std::find_if(nodes.begin(), nodes.end(),
                                 [&](const NodeRuntime& candidate) {
                                   return candidate.config.id == node_id;
                                 });
  if (node == nodes.end()) {
    throw std::runtime_error("partition command references unknown node: " +
                             std::string(node_id));
  }
  const std::size_t index =
      static_cast<std::size_t>(std::distance(nodes.begin(), node));
  if (index > std::numeric_limits<std::uint32_t>::max()) {
    throw std::runtime_error("partition command node index exceeds uint32");
  }
  return static_cast<std::uint32_t>(index);
}

NetworkBlockRule MakeP2pBlockRule(uint32_t src_node_index,
                                  uint32_t dst_node_index,
                                  const RuntimeNodeSnapshot& nodes) {
  if (src_node_index >= nodes.size() || dst_node_index >= nodes.size()) {
    throw std::runtime_error("partition node is out of range");
  }
  NetworkBlockRule rule;
  rule.node_index = dst_node_index;
  if (!nodes[src_node_index].network || !nodes[dst_node_index].network) {
    throw std::runtime_error(
        "partition nodes do not have isolated network addresses");
  }
  rule.src_address = nodes[src_node_index].network->node_address;
  rule.dst_address = nodes[dst_node_index].network->node_address;
  rule.dst_port = nodes[dst_node_index].config.p2p_port;
  rule.handle = StableRuleHandle(rule);
  return rule;
}

}  // namespace

NetworkPartitionRule RuntimePartitionRule(const SimulationPartition& partition,
                                          const RuntimeNodeSnapshot& nodes) {
  NetworkPartitionRule rule;
  rule.group_a.reserve(partition.group_a.node_ids.size());
  rule.group_b.reserve(partition.group_b.node_ids.size());
  for (const std::string& node_id : partition.group_a.node_ids) {
    rule.group_a.push_back(PartitionNodeIndex(nodes, node_id));
  }
  for (const std::string& node_id : partition.group_b.node_ids) {
    rule.group_b.push_back(PartitionNodeIndex(nodes, node_id));
  }
  ValidateNetworkPartitionRule(rule, static_cast<std::uint32_t>(nodes.size()),
                               "operator partition command");
  return rule;
}

std::vector<NetworkBlockRule> PartitionBlockRules(
    const NetworkPartitionRule& partition, const RuntimeNodeSnapshot& nodes) {
  std::vector<NetworkBlockRule> rules;
  rules.reserve((partition.group_a.size() * partition.group_b.size()) * 2U);
  for (uint32_t node_a : partition.group_a) {
    for (uint32_t node_b : partition.group_b) {
      rules.push_back(MakeP2pBlockRule(node_a, node_b, nodes));
      rules.push_back(MakeP2pBlockRule(node_b, node_a, nodes));
    }
  }
  return rules;
}

}  // namespace bbp::simulator_app_internal
