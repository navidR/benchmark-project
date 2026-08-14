#pragma once

#include <cstdint>
#include <filesystem>
#include <functional>
#include <mutex>
#include <set>
#include <stop_token>
#include <string>
#include <string_view>

namespace bbp {

class ChainDriver;
class RunProcessState;
class RuntimeNodeSnapshot;
class SimulationRegistry;
struct NodeRoleTopology;
struct NodeRuntime;
struct Options;

namespace simulator_app_internal {

struct MetricsSnapshotSynchronization {
  std::mutex& network_state;
  std::mutex& resource_state;
};

using NodeMetricsFailureHandler =
    std::function<void(const NodeRuntime&, std::string_view)>;
using MetricsStopRequested = std::function<bool()>;
using NodeMetricsRecordHandler =
    std::function<void(const NodeRuntime&, std::string_view)>;
using WalletMetricsFailureHandler =
    std::function<void(std::uint32_t wallet_index, const NodeRuntime& node,
                       std::string_view error)>;

std::uint32_t WriteMetricsSnapshot(
    const std::filesystem::path& metrics_path, const Options& options,
    const ChainDriver& driver, const RuntimeNodeSnapshot& nodes,
    RunProcessState& run_process_state,
    MetricsSnapshotSynchronization synchronization,
    const NodeMetricsFailureHandler& node_failure_handler = {},
    const MetricsStopRequested& stop_requested = {},
    std::stop_token stop_token = {},
    const NodeRoleTopology* runtime_topology = nullptr,
    const std::set<std::string>* selected_node_ids = nullptr,
    const NodeMetricsRecordHandler& record_handler = {});

std::uint32_t WriteWalletMetricsSnapshot(
    const std::filesystem::path& metrics_path, const Options& options,
    const ChainDriver& driver, const RuntimeNodeSnapshot& nodes,
    const SimulationRegistry& registry,
    const WalletMetricsFailureHandler& failure_handler = {},
    std::stop_token stop_token = {});

}  // namespace simulator_app_internal
}  // namespace bbp
