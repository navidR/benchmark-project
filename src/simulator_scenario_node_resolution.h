#pragma once

#include <cstdint>
#include <filesystem>
#include <string>
#include <vector>

namespace bbp {

class ChainExtraArgs;
enum class ChainNetwork;
struct NodeLifecyclePolicy;
struct Options;
struct ScenarioNodeConfig;
struct ScenarioNodeWalletConfig;

namespace simulator_app_internal {

bool NodeListContains(const std::vector<uint32_t>& nodes, uint32_t node_index);
std::string ScenarioNodeId(const Options& options, uint32_t node_index);
const ScenarioNodeConfig* ScenarioNodeConfigAt(const Options& options,
                                               uint32_t node_index);
std::filesystem::path EffectiveNodeBinary(const Options& options,
                                          uint32_t node_index);
std::filesystem::path NodeDataDirectoryRelative(const Options& options,
                                                uint32_t node_index);
ChainNetwork EffectiveNodeChainNetwork(const Options& options,
                                       uint32_t node_index);
const ChainExtraArgs& EffectiveNodeExtraArgs(const Options& options,
                                             uint32_t node_index);
NodeLifecyclePolicy EffectiveNodeLifecyclePolicy(const Options& options,
                                                 uint32_t node_index);
bool HasTimedNodeLifecycle(const Options& options);
ScenarioNodeWalletConfig EffectiveNodeWalletConfig(const Options& options,
                                                   uint32_t node_index);

}  // namespace simulator_app_internal
}  // namespace bbp
