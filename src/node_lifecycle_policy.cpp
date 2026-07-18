#include "bbp/node_lifecycle_policy.h"

#include <sys/wait.h>

#include <stdexcept>

namespace bbp {

std::string_view NodeRestartPolicyName(NodeRestartPolicy policy) {
  switch (policy) {
    case NodeRestartPolicy::kNever:
      return "never";
    case NodeRestartPolicy::kOnFailure:
      return "on_failure";
    case NodeRestartPolicy::kAlways:
      return "always";
  }
  throw std::runtime_error("unknown node restart policy");
}

std::optional<NodeRestartPolicy> NodeRestartPolicyFromName(
    std::string_view name) {
  if (name == "never") {
    return NodeRestartPolicy::kNever;
  }
  if (name == "on_failure") {
    return NodeRestartPolicy::kOnFailure;
  }
  if (name == "always") {
    return NodeRestartPolicy::kAlways;
  }
  return std::nullopt;
}

void ValidateNodeLifecyclePolicy(
    const NodeLifecyclePolicy& policy,
    std::optional<std::chrono::milliseconds> simulation_duration) {
  if (policy.start_time && policy.start_time->count() <= 0) {
    throw std::runtime_error("node start_time must be greater than zero");
  }
  if (policy.stop_time && policy.stop_time->count() <= 0) {
    throw std::runtime_error("node stop_time must be greater than zero");
  }
  const std::chrono::milliseconds effective_start =
      policy.start_time.value_or(std::chrono::milliseconds(0));
  if (policy.stop_time && *policy.stop_time <= effective_start) {
    throw std::runtime_error("node stop_time must be after start_time");
  }
  if (!simulation_duration) {
    return;
  }
  if (effective_start >= *simulation_duration && policy.start_time) {
    throw std::runtime_error(
        "node start_time must be before simulation duration");
  }
  if (policy.stop_time && *policy.stop_time >= *simulation_duration) {
    throw std::runtime_error(
        "node stop_time must be before simulation duration");
  }
}

bool ProcessExitSucceeded(int wait_status) {
  return WIFEXITED(wait_status) && WEXITSTATUS(wait_status) == 0;
}

bool NodeRestartPolicyAllowsRestart(NodeRestartPolicy policy, int wait_status) {
  switch (policy) {
    case NodeRestartPolicy::kNever:
      return false;
    case NodeRestartPolicy::kOnFailure:
      return !ProcessExitSucceeded(wait_status);
    case NodeRestartPolicy::kAlways:
      return true;
  }
  throw std::runtime_error("unknown node restart policy");
}

}  // namespace bbp
