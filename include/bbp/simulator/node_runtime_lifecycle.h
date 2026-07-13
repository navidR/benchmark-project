#pragma once

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

}  // namespace bbp
