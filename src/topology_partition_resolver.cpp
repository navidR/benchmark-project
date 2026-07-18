#include "bbp/topology_partition_resolver.h"

#include <algorithm>
#include <boost/algorithm/string/join.hpp>
#include <boost/json/array.hpp>
#include <boost/json/value.hpp>
#include <set>
#include <stdexcept>
#include <utility>

namespace bbp {
namespace {

const boost::json::array& TopologyGroups(const boost::json::object& report) {
  const boost::json::value* value =
      report.if_contains("topology_groups_summary");
  if (value == nullptr || !value->is_array()) {
    throw std::runtime_error("topology group summaries are unavailable");
  }
  return value->as_array();
}

const boost::json::object& GroupObject(const boost::json::value& value) {
  if (!value.is_object()) {
    throw std::runtime_error("topology group summary must be an object");
  }
  return value.as_object();
}

std::string RequiredString(const boost::json::object& object,
                           std::string_view field) {
  const boost::json::value* value = object.if_contains(field);
  if (value == nullptr || !value->is_string() || value->as_string().empty()) {
    throw std::runtime_error("topology group " + std::string(field) +
                             " must be a non-empty string");
  }
  return std::string(value->as_string());
}

SimulationPartitionScope ScopeFromGroupKind(std::string_view kind) {
  if (kind == "partition_group") {
    return SimulationPartitionScope::kPartitionGroup;
  }
  if (kind == "region") {
    return SimulationPartitionScope::kRegion;
  }
  if (kind == "role") {
    return SimulationPartitionScope::kRole;
  }
  if (kind == "all") {
    throw std::runtime_error(
        "the aggregate all group cannot be partitioned; select a configured "
        "topology, region, or role group");
  }
  throw std::runtime_error("unsupported topology group kind: " +
                           std::string(kind));
}

std::vector<std::string> RequiredNodeIds(const boost::json::object& object,
                                         std::string_view group_id) {
  const boost::json::value* value = object.if_contains("node_ids");
  if (value == nullptr || !value->is_array() || value->as_array().empty()) {
    throw std::runtime_error("topology group " + std::string(group_id) +
                             " must contain node ids");
  }
  std::vector<std::string> node_ids;
  std::set<std::string> unique;
  node_ids.reserve(value->as_array().size());
  for (const boost::json::value& node_value : value->as_array()) {
    if (!node_value.is_string() || node_value.as_string().empty()) {
      throw std::runtime_error("topology group " + std::string(group_id) +
                               " contains an invalid node id");
    }
    std::string node_id(node_value.as_string());
    if (!unique.insert(node_id).second) {
      throw std::runtime_error("topology group " + std::string(group_id) +
                               " contains duplicate node id " + node_id);
    }
    node_ids.push_back(std::move(node_id));
  }
  return node_ids;
}

}  // namespace

SimulationPartition MakeNodePairPartition(std::string source_node_id,
                                          std::string peer_node_id) {
  if (source_node_id.empty() || peer_node_id.empty()) {
    throw std::runtime_error("partition node ids must not be empty");
  }
  if (source_node_id == peer_node_id) {
    throw std::runtime_error("partition command nodes must differ");
  }
  return SimulationPartition{
      .scope = SimulationPartitionScope::kNodePair,
      .group_a =
          SimulationPartitionGroup{.group_ids = {source_node_id},
                                   .node_ids = {std::move(source_node_id)}},
      .group_b =
          SimulationPartitionGroup{.group_ids = {peer_node_id},
                                   .node_ids = {std::move(peer_node_id)}},
  };
}

SimulationPartition ResolveSelectedTopologyPartition(
    const boost::json::object& report, std::size_t selected_group) {
  const boost::json::array& groups = TopologyGroups(report);
  if (selected_group >= groups.size()) {
    throw std::runtime_error("selected topology group is out of range");
  }
  const boost::json::object& selected = GroupObject(groups[selected_group]);
  const std::string selected_id = RequiredString(selected, "group");
  const std::string selected_kind = RequiredString(selected, "kind");
  const SimulationPartitionScope scope = ScopeFromGroupKind(selected_kind);
  std::vector<std::string> selected_nodes =
      RequiredNodeIds(selected, selected_id);
  const std::set<std::string> selected_node_set(selected_nodes.begin(),
                                                selected_nodes.end());

  std::vector<std::string> counterpart_ids;
  std::vector<std::string> counterpart_nodes;
  std::set<std::string> unique_group_ids{selected_id};
  std::set<std::string> unique_counterpart_nodes;
  for (std::size_t index = 0; index < groups.size(); ++index) {
    if (index == selected_group) {
      continue;
    }
    const boost::json::object& candidate = GroupObject(groups[index]);
    if (RequiredString(candidate, "kind") != selected_kind) {
      continue;
    }
    const std::string candidate_id = RequiredString(candidate, "group");
    if (!unique_group_ids.insert(candidate_id).second) {
      throw std::runtime_error("duplicate topology group id: " + candidate_id);
    }
    std::vector<std::string> candidate_nodes =
        RequiredNodeIds(candidate, candidate_id);
    for (const std::string& node_id : candidate_nodes) {
      if (selected_node_set.contains(node_id)) {
        throw std::runtime_error("topology groups " + selected_id + " and " +
                                 candidate_id + " overlap at node " + node_id);
      }
      if (!unique_counterpart_nodes.insert(node_id).second) {
        throw std::runtime_error(
            "counterpart topology groups overlap at node " + node_id);
      }
      counterpart_nodes.push_back(node_id);
    }
    counterpart_ids.push_back(candidate_id);
  }
  if (counterpart_ids.empty()) {
    throw std::runtime_error("selected topology group " + selected_id +
                             " has no same-kind counterpart group");
  }

  return SimulationPartition{
      .scope = scope,
      .group_a =
          SimulationPartitionGroup{.group_ids = {selected_id},
                                   .node_ids = std::move(selected_nodes)},
      .group_b =
          SimulationPartitionGroup{.group_ids = std::move(counterpart_ids),
                                   .node_ids = std::move(counterpart_nodes)},
  };
}

std::string SimulationPartitionTargetText(
    const SimulationPartition& partition) {
  return boost::algorithm::join(partition.group_a.group_ids, ",") + " vs " +
         boost::algorithm::join(partition.group_b.group_ids, ",");
}

}  // namespace bbp
