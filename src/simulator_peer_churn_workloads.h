#pragma once

#include <cstdint>
#include <filesystem>
#include <stop_token>

namespace bbp {

class ChainDriver;
class PeerConnectivityController;
class RuntimeNodeSnapshot;
struct ConnectPeerWorkload;
struct DisconnectPeerWorkload;
struct Options;
struct SimulationCommandControl;

namespace simulator_app_internal {

void ApplyConnectPeerWorkload(
    const Options& options, const std::filesystem::path& events_path,
    const ChainDriver& driver, PeerConnectivityController& controller,
    const RuntimeNodeSnapshot& nodes, const ConnectPeerWorkload& workload,
    std::uint32_t workload_index, std::uint32_t workload_count,
    std::stop_token stop_token,
    SimulationCommandControl* cancellation_commit_control = nullptr);

void ApplyDisconnectPeerWorkload(
    const Options& options, const std::filesystem::path& events_path,
    const ChainDriver& driver, PeerConnectivityController& controller,
    const RuntimeNodeSnapshot& nodes, const DisconnectPeerWorkload& workload,
    std::uint32_t workload_index, std::uint32_t workload_count,
    std::stop_token stop_token,
    SimulationCommandControl* cancellation_commit_control = nullptr);

}  // namespace simulator_app_internal
}  // namespace bbp
