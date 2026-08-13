#pragma once

#include <boost/json/object.hpp>
#include <cstdint>
#include <string_view>
#include <vector>

#include "bbp/default_peer_topology.h"
#include "bbp/network.h"
#include "bbp/simulation_registry.h"

namespace bbp::simulator_app_internal {

std::vector<std::uint32_t> ConsecutiveNodeIndexes(std::uint32_t first_index,
                                                  std::uint32_t count);
bool NodeListsOverlap(const std::vector<std::uint32_t>& left,
                      const std::vector<std::uint32_t>& right);
bool IsTopologyEdgeConditionField(std::string_view field);
NetworkCondition ParseTopologyEdgeWorkloadCondition(
    const boost::json::object& object);
void RejectTopologyEdgeConditionFields(const boost::json::object& object,
                                       std::string_view action);
PeerTopologyConfig ParsePeerTopologyConfig(const boost::json::object& object,
                                           std::uint32_t node_count,
                                           std::uint64_t default_seed);
NodeRoleTopology ParseNodeRoleTopologyObject(const boost::json::object& object,
                                             std::uint32_t nodes,
                                             std::uint64_t default_seed);

}  // namespace bbp::simulator_app_internal
