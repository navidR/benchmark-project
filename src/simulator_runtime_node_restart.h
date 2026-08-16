#pragma once

#include <chrono>
#include <filesystem>
#include <stop_token>
#include <string_view>

#include "bbp/simulation_command.h"

namespace bbp {

class ChainDriver;
class PeerConnectivityController;
class RuntimeNodeSnapshot;
struct ChainNodeConfig;
struct NodeRuntime;
struct Options;

namespace simulator_app_internal {

struct NodeRestartAdmission {
  SimulationNodeProcessObservation process;
  NodeRuntimeLifecycle lifecycle = NodeRuntimeLifecycle::kDefined;
  bool admitted = false;
};

using RuntimeNodeStartWithPolicy = bool (*)(
    const Options&, const std::filesystem::path&, const ChainDriver&,
    NodeRuntime&, std::string_view, std::chrono::steady_clock::time_point, bool,
    bool, std::stop_token, const ChainNodeConfig*);

bool RestartNode(
    const Options& options, const std::filesystem::path& events_path,
    const ChainDriver& driver,
    PeerConnectivityController& peer_connectivity_controller, NodeRuntime& node,
    std::chrono::steady_clock::time_point lifecycle_epoch,
    RuntimeNodeStartWithPolicy start_node, std::stop_token stop_token,
    std::string_view reason = "requested",
    SimulationCommandControl* operation_control = nullptr,
    NodeRestartAdmission* admitted_state = nullptr,
    bool request_topology_restore = true,
    const ChainNodeConfig* process_config_override = nullptr,
    bool publish_running = true,
    SimulationCommandControl* cancellation_commit_control = nullptr,
    std::stop_token committed_stop_token = {});

void ApplyRuntimeNodeRestarts(
    const Options& options, const std::filesystem::path& events_path,
    const ChainDriver& driver,
    PeerConnectivityController& peer_connectivity_controller,
    const RuntimeNodeSnapshot& nodes,
    std::chrono::steady_clock::time_point lifecycle_epoch,
    RuntimeNodeStartWithPolicy start_node, std::stop_token stop_token);

}  // namespace simulator_app_internal
}  // namespace bbp
