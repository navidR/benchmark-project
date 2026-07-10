#pragma once

#include <atomic>
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
  LogTailCursor stdout_log_cursor;
  LogTailCursor stderr_log_cursor;
  LogTailCursor daemon_log_cursor;

  std::uint64_t GeneratedBlockCount() {
    return std::atomic_ref<std::uint64_t>(generated_block_count_)
        .load(std::memory_order_relaxed);
  }

  void AddGeneratedBlocks(std::uint64_t count) {
    std::atomic_ref<std::uint64_t>(generated_block_count_)
        .fetch_add(count, std::memory_order_relaxed);
  }

  std::uint64_t RestartCount() {
    return std::atomic_ref<std::uint64_t>(restart_count_)
        .load(std::memory_order_relaxed);
  }

  std::uint64_t IncrementRestartCount() {
    return std::atomic_ref<std::uint64_t>(restart_count_)
               .fetch_add(1U, std::memory_order_relaxed) +
           1U;
  }

 private:
  alignas(std::atomic_ref<std::uint64_t>::required_alignment) std::uint64_t
      generated_block_count_ = 0;
  alignas(std::atomic_ref<std::uint64_t>::required_alignment) std::uint64_t
      restart_count_ = 0;
};

}  // namespace bsim
