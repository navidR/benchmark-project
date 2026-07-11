#pragma once

namespace bbp {

enum class NodeRuntimeLifecycle {
  kStarting,
  kRunning,
  kRestarting,
  kKilling,
  kKilled,
};

}  // namespace bbp
