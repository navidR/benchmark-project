#pragma once

#include <filesystem>
#include <mutex>
#include <stop_token>

namespace bbp {

class RuntimeNodeSnapshot;
struct Options;

namespace simulator_app_internal {

void ApplyRuntimeNetworkPartitions(const Options& options,
                                   const std::filesystem::path& events_path,
                                   const RuntimeNodeSnapshot& nodes,
                                   std::mutex& node_network_state_mutex,
                                   std::stop_token stop_token);

void ApplyRuntimeNetworkPartitionHeals(const Options& options,
                                       const std::filesystem::path& events_path,
                                       const RuntimeNodeSnapshot& nodes,
                                       std::mutex& node_network_state_mutex,
                                       std::stop_token stop_token);

}  // namespace simulator_app_internal
}  // namespace bbp
