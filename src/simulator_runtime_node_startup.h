#pragma once

#include <chrono>
#include <filesystem>
#include <mutex>
#include <stop_token>
#include <string_view>

namespace bbp {

class ChainDriver;
struct ChainNodeConfig;
struct NodeRuntime;
struct Options;

namespace simulator_app_internal {

void ApplyDeclarativeStopDuringStart(
    const Options& options, const std::filesystem::path& events_path,
    const ChainDriver& driver, NodeRuntime& node,
    std::chrono::steady_clock::time_point lifecycle_epoch,
    std::stop_token stop_token);

bool StartNodeProcessWithPolicy(
    const Options& options, const std::filesystem::path& events_path,
    const ChainDriver& driver, NodeRuntime& node,
    std::mutex& node_network_state_mutex, std::string_view reason,
    std::chrono::steady_clock::time_point lifecycle_epoch,
    bool first_attempt_is_restart, bool transition_to_running,
    std::stop_token stop_token,
    const ChainNodeConfig* process_config_override = nullptr);

bool StartPreparedNode(const Options& options,
                       const std::filesystem::path& events_path,
                       const ChainDriver& driver, NodeRuntime& node,
                       std::mutex& node_network_state_mutex,
                       std::string_view reason,
                       std::chrono::steady_clock::time_point lifecycle_epoch,
                       std::stop_token stop_token);

}  // namespace simulator_app_internal
}  // namespace bbp
