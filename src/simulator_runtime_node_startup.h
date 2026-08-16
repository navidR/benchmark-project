#pragma once

#include <chrono>
#include <cstddef>
#include <filesystem>
#include <mutex>
#include <optional>
#include <stop_token>
#include <string_view>
#include <vector>

namespace bbp {

class ChainDriver;
struct ChainNodeConfig;
struct NodeRuntime;
struct Options;
class RuntimeNodeSnapshot;

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

void StartInitialPreparedNodes(
    const Options& options, const std::filesystem::path& events_path,
    const ChainDriver& driver, std::vector<NodeRuntime>& nodes,
    std::mutex& node_network_state_mutex,
    std::chrono::steady_clock::time_point simulation_epoch,
    std::stop_token stop_token);

void ConnectAvailableStartupPeers(
    const Options& options, const std::filesystem::path& events_path,
    const ChainDriver& driver, std::vector<NodeRuntime>& nodes,
    std::mutex& node_network_state_mutex,
    std::optional<std::size_t> changed_node,
    std::chrono::steady_clock::time_point lifecycle_epoch,
    std::stop_token stop_token);

void ConnectAvailableStartupPeers(
    const Options& options, const std::filesystem::path& events_path,
    const ChainDriver& driver, const RuntimeNodeSnapshot& nodes,
    std::mutex& node_network_state_mutex,
    std::optional<std::size_t> changed_node,
    std::chrono::steady_clock::time_point lifecycle_epoch,
    std::stop_token stop_token);

}  // namespace simulator_app_internal
}  // namespace bbp
