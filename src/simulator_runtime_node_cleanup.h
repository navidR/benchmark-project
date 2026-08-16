#pragma once

#include <chrono>
#include <cstdint>
#include <filesystem>
#include <optional>
#include <stop_token>
#include <vector>

#include "bbp/runtime_node_resource_manifest.h"
#include "bbp/simulator/node_runtime.h"

namespace bbp {

class ChainDriver;
class RuntimeNodeSnapshot;
struct Options;

namespace simulator_app_internal {

using RuntimeNodeResourceEntryBuilder =
    RuntimeNodeResourceEntry (*)(const Options&, const ChainNodeConfig&,
                                 std::uint32_t, RuntimeNodeResourceState);

bool RuntimeNodeSupportDestructionAllowed(bool daemon_absence_verified,
                                          bool exact_cgroup_acquired,
                                          bool exact_cgroup_empty,
                                          bool allow_partial_preparation);

std::vector<bool> StopRuntimeNodes(
    const Options& options, const std::filesystem::path& events_path,
    const ChainDriver& driver, const RuntimeNodeSnapshot& nodes,
    RuntimeNodeResourceEntryBuilder resource_entry, bool best_effort,
    bool remove_run_cgroup,
    const std::vector<std::uint32_t>* explicit_resource_slots,
    std::optional<std::chrono::steady_clock::time_point> absolute_deadline,
    std::stop_token cleanup_stop_token, bool allow_partial_preparation);

std::vector<bool> StopRuntimeNodes(
    const Options& options, const std::filesystem::path& events_path,
    const ChainDriver& driver, std::vector<NodeRuntime>& nodes,
    RuntimeNodeResourceEntryBuilder resource_entry, bool best_effort,
    bool remove_run_cgroup,
    const std::vector<std::uint32_t>* explicit_resource_slots,
    std::optional<std::chrono::steady_clock::time_point> absolute_deadline,
    std::stop_token cleanup_stop_token, bool allow_partial_preparation);

}  // namespace simulator_app_internal
}  // namespace bbp
