#pragma once

#include <cstdint>
#include <filesystem>
#include <functional>
#include <mutex>
#include <optional>
#include <stop_token>

namespace bbp {

class RuntimeNodeSnapshot;
struct NodeRuntime;
struct Options;
struct ResourceLimitPatch;

namespace simulator_app_internal {

void ApplyResourceLimitUpdate(
    const Options& options, const std::filesystem::path& events_path,
    NodeRuntime& node, const ResourceLimitPatch& patch,
    std::mutex& node_resource_state_mutex, std::stop_token stop_token = {},
    const std::function<void()>& authorize_mutation = {},
    std::optional<std::uint32_t> workload_index = std::nullopt,
    std::optional<std::uint32_t> workload_count = std::nullopt,
    std::optional<std::uint32_t> workload_node = std::nullopt,
    std::optional<std::uint64_t> operator_sequence = std::nullopt,
    bool resolve_operator_io_limit = false);

void ApplyRuntimeResourceLimitUpdates(const Options& options,
                                      const std::filesystem::path& events_path,
                                      const RuntimeNodeSnapshot& nodes,
                                      std::mutex& node_resource_state_mutex,
                                      std::stop_token stop_token);

}  // namespace simulator_app_internal
}  // namespace bbp
