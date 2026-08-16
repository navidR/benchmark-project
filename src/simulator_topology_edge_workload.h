#pragma once

#include <cstdint>
#include <filesystem>
#include <mutex>
#include <stop_token>
#include <string>
#include <vector>

namespace bbp {

class ChainDriver;
class PeerConnectivityController;
class RuntimeNodeSnapshot;
class RuntimePeerTopology;
struct ChainDriverSpec;
struct ChainNodeConfig;
struct Options;
struct TopologyEdgeWorkload;
enum class WorkloadKind;

namespace simulator_app_internal {

std::vector<std::string> DynamicPhysicalTopologyPeerEndpoints(
    const RuntimePeerTopology& topology,
    const std::vector<ChainNodeConfig>& configs, std::uint32_t node_index);

void ApplyTopologyEdgeWorkload(
    const Options& options, const std::filesystem::path& events_path,
    const ChainDriverSpec& chain_spec, const ChainDriver& driver,
    PeerConnectivityController& controller,
    RuntimePeerTopology& runtime_topology, const RuntimeNodeSnapshot& nodes,
    std::mutex& node_network_state_mutex, const TopologyEdgeWorkload& workload,
    WorkloadKind action, std::uint32_t workload_index,
    std::uint32_t workload_count, std::stop_token stop_token);

}  // namespace simulator_app_internal
}  // namespace bbp
