#pragma once

#include <boost/json/object.hpp>
#include <cstdint>
#include <string_view>
#include <vector>

#include "bbp/network.h"
#include "bbp/simulator/network_block_rule.h"
#include "bbp/simulator/network_partition_rule.h"

namespace bbp::simulator_app_internal {

NetworkCondition ParseNetworkConditionObject(const boost::json::object& object);
std::vector<std::uint32_t> JsonNodeGroupField(const boost::json::object& object,
                                              const char* field);
std::uint32_t StableRuleHandle(const NetworkBlockRule& rule);
NetworkBlockRule ParseNetworkBlockRuleObject(const boost::json::object& object);
NetworkPartitionRule ParseNetworkPartitionRuleObject(
    const boost::json::object& object);
void ValidateNetworkPartitionRule(const NetworkPartitionRule& rule,
                                  std::uint32_t nodes, std::string_view source);

}  // namespace bbp::simulator_app_internal
