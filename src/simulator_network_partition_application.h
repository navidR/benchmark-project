#pragma once

#include <cstdint>
#include <filesystem>
#include <mutex>
#include <optional>
#include <stop_token>

namespace bbp {

class RuntimeNodeSnapshot;
struct NetworkPartitionRule;
struct Options;

namespace simulator_app_internal {

void ApplyRuntimeNetworkPartition(
    const Options& options, const std::filesystem::path& events_path,
    const RuntimeNodeSnapshot& nodes, std::mutex& node_network_state_mutex,
    const NetworkPartitionRule& partition, bool heal,
    std::uint32_t workload_index = 0U, std::uint32_t workload_count = 0U,
    std::stop_token stop_token = {},
    std::optional<std::uint64_t> operator_sequence = std::nullopt);

}  // namespace simulator_app_internal
}  // namespace bbp
