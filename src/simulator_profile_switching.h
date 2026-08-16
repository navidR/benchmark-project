#pragma once

#include <cstdint>
#include <filesystem>
#include <functional>
#include <mutex>
#include <stop_token>

namespace bbp {

class RuntimeNodeSnapshot;
struct Options;
struct ProfileSwitchWorkload;

namespace simulator_app_internal {

void ApplyResourceProfileSwitch(
    const Options& options, const std::filesystem::path& events_path,
    const RuntimeNodeSnapshot& nodes, std::mutex& node_resource_state_mutex,
    const ProfileSwitchWorkload& workload, std::uint32_t workload_index,
    std::uint32_t workload_count, std::stop_token stop_token,
    const std::function<void()>& authorize_mutation = {});

void ApplyNetworkProfileSwitch(
    const Options& options, const std::filesystem::path& events_path,
    const RuntimeNodeSnapshot& nodes, std::mutex& node_network_state_mutex,
    const ProfileSwitchWorkload& workload, std::uint32_t workload_index,
    std::uint32_t workload_count, std::stop_token stop_token);

}  // namespace simulator_app_internal
}  // namespace bbp
