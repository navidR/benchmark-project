#pragma once

#include <atomic>
#include <cstdint>
#include <optional>

#include "bbp/cgroup.h"
#include "bbp/drivers/chain_driver.h"
#include "bbp/network.h"
#include "bbp/process.h"
#include "bbp/simulator/resource_limits.h"

namespace bbp {

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

}  // namespace bbp
