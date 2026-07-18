#pragma once

#include <chrono>
#include <optional>
#include <string_view>

namespace bbp {

enum class NodeRestartPolicy {
  kNever,
  kOnFailure,
  kAlways,
};

std::string_view NodeRestartPolicyName(NodeRestartPolicy policy);
std::optional<NodeRestartPolicy> NodeRestartPolicyFromName(
    std::string_view name);

struct NodeLifecyclePolicy {
  std::optional<std::chrono::milliseconds> start_time;
  std::optional<std::chrono::milliseconds> stop_time;
  NodeRestartPolicy restart_policy = NodeRestartPolicy::kNever;
};

void ValidateNodeLifecyclePolicy(
    const NodeLifecyclePolicy& policy,
    std::optional<std::chrono::milliseconds> simulation_duration);

bool ProcessExitSucceeded(int wait_status);
bool NodeRestartPolicyAllowsRestart(NodeRestartPolicy policy, int wait_status);

}  // namespace bbp
