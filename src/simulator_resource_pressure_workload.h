#pragma once

#include <cstdint>
#include <filesystem>
#include <mutex>
#include <stop_token>

namespace bbp {

class ChainDriver;
class RunProcessState;
class RuntimeNodeSnapshot;
struct NodeRoleTopology;
struct Options;
struct ResourcePressureWorkload;

namespace simulator_app_internal {

void ApplyResourcePressureWorkload(
    const Options& options, const std::filesystem::path& events_path,
    const std::filesystem::path& metrics_path, const ChainDriver& driver,
    const RuntimeNodeSnapshot& nodes, std::mutex& node_network_state_mutex,
    std::mutex& node_resource_state_mutex, RunProcessState& run_process_state,
    const NodeRoleTopology& runtime_role_topology,
    const ResourcePressureWorkload& workload, std::uint32_t workload_index,
    std::uint32_t workload_count, std::stop_token stop_token);

}  // namespace simulator_app_internal
}  // namespace bbp
