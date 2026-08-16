#pragma once

#include <sys/types.h>

#include <cstdint>
#include <filesystem>
#include <functional>
#include <optional>
#include <span>
#include <stop_token>
#include <string>
#include <vector>

#include "bbp/perf_counter.h"
#include "bbp/run_process_state.h"

namespace bbp {

class RuntimeNodeSnapshot;
struct NodeRuntime;
struct SimulationCommand;

namespace simulator_app_internal {

struct NodePerfCounterConfiguration {
  std::string node_id;
  std::vector<PerfCounterKind> kinds;
  PerfCounterTargetKind target_kind = PerfCounterTargetKind::kNode;
  std::string target_id;
};

struct NodePerfCounterAssignment {
  NodeRuntime* node = nullptr;
  NodePerfCounterConfiguration configuration;
  bool require_running = true;
  bool require_attachment = true;
};

struct NodePerfCounterTransactionBackend {
  std::function<bool(const NodeRuntime&)> is_running;
  std::function<void(NodeRuntime&, const RunProcessState::Guard&,
                     bool require_attachment)>
      attach;
  std::function<void(NodeRuntime&, const RunProcessState::Guard&)> reset;
};

struct NodePerfCounterSnapshot {
  explicit NodePerfCounterSnapshot(
      NodeRuntime& runtime, const NodePerfCounterConfiguration& configuration);

  NodeRuntime* node;
  std::vector<PerfCounterKind> configuration_kinds;
  PerfCounterTargetKind configuration_kind = PerfCounterTargetKind::kNode;
  std::string configuration_id;
  std::optional<ProcessPerfCounters> process_counters;
  std::optional<CgroupPerfCounters> cgroup_counters;
  pid_t target_pid = -1;
  pid_t attached_pid = -1;
  std::uint64_t process_generation = 0U;
  std::filesystem::path cgroup_path;
  std::vector<int> cpus;
  std::optional<PerfCounterErrorKind> error_kind;
  std::string error;
  bool replacement_started = false;
};

NodePerfCounterConfiguration SnapshotPerfCounterConfiguration(
    const NodePerfCounterSnapshot& snapshot);
std::vector<NodePerfCounterSnapshot> ReplaceNodePerfCountersTransactional(
    std::span<const NodePerfCounterAssignment> assignments,
    const RunProcessState::Guard& process_guard,
    std::stop_token stop_token = {},
    const NodePerfCounterTransactionBackend* backend = nullptr);
void ApplyPerfCounterCommand(
    const SimulationCommand& command, RuntimeNodeSnapshot& nodes,
    const RunProcessState::Guard& process_guard,
    const NodePerfCounterTransactionBackend* backend = nullptr);
void RollBackNodePerfCounterSnapshots(
    std::vector<NodePerfCounterSnapshot>& snapshots,
    RunProcessState& run_process_state);

}  // namespace simulator_app_internal
}  // namespace bbp
