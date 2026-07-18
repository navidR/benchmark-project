#pragma once

#include <boost/json/object.hpp>
#include <cstddef>
#include <string>
#include <string_view>

#include "bbp/simulation_partition.h"

namespace bbp {

SimulationPartition MakeNodePairPartition(std::string source_node_id,
                                          std::string peer_node_id);

SimulationPartition ResolveSelectedTopologyPartition(
    const boost::json::object& report, std::size_t selected_group);

std::string SimulationPartitionTargetText(const SimulationPartition& partition);

}  // namespace bbp
