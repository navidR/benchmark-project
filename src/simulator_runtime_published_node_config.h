#pragma once

#include <boost/json/object.hpp>
#include <cstdint>

namespace bbp {

struct ChainNodeConfig;
struct NodeRoleTopology;
struct NodeRuntime;
struct Options;

namespace simulator_app_internal {

boost::json::object RuntimePublishedNodeConfig(
    const Options& options, const NodeRuntime& runtime,
    const ChainNodeConfig& config, std::uint32_t node_index,
    const NodeRoleTopology* runtime_topology = nullptr);

}  // namespace simulator_app_internal
}  // namespace bbp
