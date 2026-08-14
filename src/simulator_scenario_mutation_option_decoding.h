#pragma once

#include <boost/json/array.hpp>
#include <cstdint>
#include <map>
#include <string_view>
#include <vector>

#include "bbp/network.h"
#include "bbp/simulator/network_block_rule.h"
#include "bbp/simulator/network_partition_rule.h"
#include "bbp/simulator/resource_limit_patch.h"

namespace bbp::simulator_app_internal {

void ApplyNodeConditions(const boost::json::array& conditions,
                         std::uint32_t nodes, std::string_view source,
                         std::map<std::uint32_t, NetworkCondition>& output);
void ApplyNetworkBlockRules(const boost::json::array& rules,
                            std::uint32_t nodes, std::string_view source,
                            std::vector<NetworkBlockRule>& output);
void ApplyNetworkPartitionRules(const boost::json::array& rules,
                                std::uint32_t nodes, std::string_view source,
                                std::vector<NetworkPartitionRule>& output);
void ApplyResourceLimitPatches(
    const boost::json::array& updates, std::uint32_t nodes,
    std::string_view source,
    std::map<std::uint32_t, ResourceLimitPatch>& output);

}  // namespace bbp::simulator_app_internal
