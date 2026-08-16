#pragma once

#include <cstdint>
#include <filesystem>
#include <optional>
#include <stop_token>
#include <string>
#include <vector>

#include "bbp/network.h"
#include "bbp/runtime_node_resource_manifest.h"
#include "bbp/simulator/resource_limits.h"

namespace bbp {

class RunProcessState;
class RuntimeNodeSnapshot;
struct ChainNodeConfig;
struct NodeRuntime;
struct Options;

namespace simulator_app_internal {

RuntimeNodeResourceEntry RuntimeNodeResourceEntryFor(
    const Options& options, const ChainNodeConfig& config,
    std::uint32_t resource_slot, RuntimeNodeResourceState state);

RuntimeNodeResourceManifest RuntimeNodeResourceManifestFor(
    const Options& options, const RuntimeNodeSnapshot& nodes,
    RuntimeNodeResourceState state = RuntimeNodeResourceState::kLive);

void PrepareNodeRuntime(
    const Options& options, const std::filesystem::path& events_path,
    NodeRuntime& runtime, ChainNodeConfig config, std::uint32_t resource_slot,
    ResourceLimits resources, std::string resource_profile,
    std::string network_profile,
    std::vector<DirectionalNetworkPolicy> directional_network_policies,
    std::optional<NetworkCondition> network_condition,
    RunProcessState& run_process_state, std::stop_token stop_token,
    bool* runtime_root_acquired = nullptr);

}  // namespace simulator_app_internal
}  // namespace bbp
