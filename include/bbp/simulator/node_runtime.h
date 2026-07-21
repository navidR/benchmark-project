#pragma once

#include <atomic>
#include <chrono>
#include <cstdint>
#include <filesystem>
#include <optional>
#include <string>
#include <vector>

#include "bbp/cgroup.h"
#include "bbp/drivers/chain_driver.h"
#include "bbp/network.h"
#include "bbp/node_lifecycle_policy.h"
#include "bbp/perf_counter.h"
#include "bbp/process.h"
#include "bbp/run_process_state.h"
#include "bbp/simulator/node_runtime_lifecycle.h"
#include "bbp/simulator/resource_limits.h"

namespace bbp {

struct NodeRuntime {
  ChainNodeConfig config;
  std::optional<Cgroup> cgroup;
  std::optional<NetworkNamespace> network_namespace;
  std::optional<NodeVethConfig> network;
  std::vector<DirectionalNetworkPolicy> directional_network_policies;
  ChildProcess process;
  RunProcessState* run_process_state = nullptr;
  NodeLifecyclePolicy lifecycle_policy;
  std::optional<std::chrono::steady_clock::time_point> process_started_at;
  std::vector<PerfCounterKind> perf_counter_kinds = DefaultPerfCounterKinds();
  PerfCounterTargetKind perf_counter_target_kind = PerfCounterTargetKind::kNode;
  std::string perf_counter_target_id;
  std::optional<ProcessPerfCounters> process_perf_counters;
  std::optional<CgroupPerfCounters> cgroup_perf_counters;
  pid_t perf_counter_target_pid = -1;
  pid_t perf_counter_attached_pid = -1;
  std::uint64_t perf_counter_process_generation = 0;
  std::filesystem::path perf_counter_cgroup_path;
  std::vector<int> perf_counter_cpus;
  std::optional<PerfCounterErrorKind> perf_counter_error_kind;
  std::string perf_counter_error;
  ResourceLimits resources;
  std::string resource_profile;
  std::string network_profile;
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

  std::uint64_t MinedTransactionCount() {
    return std::atomic_ref<std::uint64_t>(mined_transaction_count_)
        .load(std::memory_order_relaxed);
  }

  void AddMinedTransactions(std::uint64_t count) {
    std::atomic_ref<std::uint64_t>(mined_transaction_count_)
        .fetch_add(count, std::memory_order_relaxed);
  }

  bool MinedTransactionCountComplete() const {
    return std::atomic_ref<bool>(mined_transaction_count_complete_)
        .load(std::memory_order_relaxed);
  }

  void MarkMinedTransactionCountIncomplete() {
    std::atomic_ref<bool>(mined_transaction_count_complete_)
        .store(false, std::memory_order_relaxed);
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

  NodeRuntimeLifecycle Lifecycle() const {
    return std::atomic_ref<NodeRuntimeLifecycle>(lifecycle_)
        .load(std::memory_order_acquire);
  }

  void SetLifecycle(NodeRuntimeLifecycle lifecycle) {
    std::atomic_ref<NodeRuntimeLifecycle>(lifecycle_)
        .store(lifecycle, std::memory_order_release);
  }

  bool AllowsChainMetrics() const {
    return Lifecycle() == NodeRuntimeLifecycle::kRunning;
  }

  bool DeclarativeStopApplied() const {
    return std::atomic_ref<bool>(declarative_stop_applied_)
        .load(std::memory_order_acquire);
  }

  void MarkDeclarativeStopApplied() {
    std::atomic_ref<bool>(declarative_stop_applied_)
        .store(true, std::memory_order_release);
  }

 private:
  alignas(std::atomic_ref<std::uint64_t>::required_alignment) std::uint64_t
      generated_block_count_ = 0;
  alignas(std::atomic_ref<std::uint64_t>::required_alignment) std::uint64_t
      mined_transaction_count_ = 0;
  alignas(
      std::atomic_ref<bool>::
          required_alignment) mutable bool mined_transaction_count_complete_ =
      true;
  alignas(std::atomic_ref<std::uint64_t>::required_alignment) std::uint64_t
      restart_count_ = 0;
  alignas(std::atomic_ref<NodeRuntimeLifecycle>::
              required_alignment) mutable NodeRuntimeLifecycle lifecycle_ =
      NodeRuntimeLifecycle::kDefined;
  alignas(std::atomic_ref<
          bool>::required_alignment) mutable bool declarative_stop_applied_ =
      false;
};

}  // namespace bbp
