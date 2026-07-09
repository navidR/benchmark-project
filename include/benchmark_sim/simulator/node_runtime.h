#pragma once

#include <cstdint>
#include <optional>

#include "benchmark_sim/cgroup.h"
#include "benchmark_sim/drivers/chain_driver.h"
#include "benchmark_sim/network.h"
#include "benchmark_sim/process.h"
#include "benchmark_sim/simulator/resource_limits.h"

namespace bsim {

struct NodeRuntime {
  ChainNodeConfig config;
  std::optional<Cgroup> cgroup;
  std::optional<NetworkNamespace> network_namespace;
  std::optional<NodeVethConfig> network;
  ChildProcess process;
  ResourceLimits resources;
  std::uint64_t generated_block_count = 0;
  std::uint64_t restart_count = 0;
  std::uint64_t stdout_offset = 0;
  std::uint64_t stderr_offset = 0;
  std::uint64_t daemon_log_offset = 0;
};

}  // namespace bsim
