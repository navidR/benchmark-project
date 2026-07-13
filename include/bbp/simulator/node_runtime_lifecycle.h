#pragma once

#include <optional>
#include <string_view>

namespace bbp {

enum class NodeRuntimeLifecycle {
  kPreparing,
  kStarting,
  kNetworkNamespaceReady,
  kCgroupReady,
  kRunning,
  kRestarting,
  kStopping,
  kStopped,
  kCleaning,
  kCleaned,
  kFailed,
  kKilling,
  kKilled,
};

constexpr std::string_view NodeRuntimeLifecycleName(
    NodeRuntimeLifecycle lifecycle) {
  switch (lifecycle) {
    case NodeRuntimeLifecycle::kPreparing:
      return "Preparing";
    case NodeRuntimeLifecycle::kStarting:
      return "Starting";
    case NodeRuntimeLifecycle::kNetworkNamespaceReady:
      return "NetnsReady";
    case NodeRuntimeLifecycle::kCgroupReady:
      return "CgroupReady";
    case NodeRuntimeLifecycle::kRunning:
      return "Running";
    case NodeRuntimeLifecycle::kRestarting:
      return "Restarting";
    case NodeRuntimeLifecycle::kStopping:
      return "Stopping";
    case NodeRuntimeLifecycle::kStopped:
      return "Stopped";
    case NodeRuntimeLifecycle::kCleaning:
      return "Cleaning";
    case NodeRuntimeLifecycle::kCleaned:
      return "Cleaned";
    case NodeRuntimeLifecycle::kFailed:
      return "Failed";
    case NodeRuntimeLifecycle::kKilling:
      return "Killing";
    case NodeRuntimeLifecycle::kKilled:
      return "Killed";
  }
  return "Unknown";
}

constexpr std::optional<NodeRuntimeLifecycle> ParseNodeRuntimeLifecycleName(
    std::string_view name) {
  if (name == "Preparing") {
    return NodeRuntimeLifecycle::kPreparing;
  }
  if (name == "Starting") {
    return NodeRuntimeLifecycle::kStarting;
  }
  if (name == "NetnsReady") {
    return NodeRuntimeLifecycle::kNetworkNamespaceReady;
  }
  if (name == "CgroupReady") {
    return NodeRuntimeLifecycle::kCgroupReady;
  }
  if (name == "Running") {
    return NodeRuntimeLifecycle::kRunning;
  }
  if (name == "Restarting") {
    return NodeRuntimeLifecycle::kRestarting;
  }
  if (name == "Stopping") {
    return NodeRuntimeLifecycle::kStopping;
  }
  if (name == "Stopped") {
    return NodeRuntimeLifecycle::kStopped;
  }
  if (name == "Cleaning") {
    return NodeRuntimeLifecycle::kCleaning;
  }
  if (name == "Cleaned") {
    return NodeRuntimeLifecycle::kCleaned;
  }
  if (name == "Failed") {
    return NodeRuntimeLifecycle::kFailed;
  }
  if (name == "Killing") {
    return NodeRuntimeLifecycle::kKilling;
  }
  if (name == "Killed") {
    return NodeRuntimeLifecycle::kKilled;
  }
  return std::nullopt;
}

}  // namespace bbp
