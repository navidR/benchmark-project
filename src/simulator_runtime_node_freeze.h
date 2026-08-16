#pragma once

#include <cstdint>
#include <filesystem>
#include <stop_token>

namespace bbp {

class Cgroup;
class RuntimeNodeSnapshot;
struct NodeRuntime;
struct Options;

namespace simulator_app_internal {

bool WaitForNodeFrozenState(const Cgroup& cgroup, bool expected,
                            std::stop_token stop_token);

void SetNodeFrozen(const Options& options,
                   const std::filesystem::path& events_path, NodeRuntime& node,
                   bool frozen, std::stop_token stop_token);

void FreezeNodeForDuration(const Options& options,
                           const std::filesystem::path& events_path,
                           NodeRuntime& node, std::uint32_t duration_ms,
                           std::stop_token stop_token);

void ApplyRuntimeNodeFreezes(const Options& options,
                             const std::filesystem::path& events_path,
                             const RuntimeNodeSnapshot& nodes,
                             std::stop_token stop_token);

}  // namespace simulator_app_internal
}  // namespace bbp
