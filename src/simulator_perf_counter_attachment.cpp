#include "simulator_perf_counter_attachment.h"

#include <filesystem>
#include <string_view>
#include <utility>

#include "bbp/logging.h"
#include "bbp/simulator/node_runtime.h"

namespace bbp::simulator_app_internal {

void ResetNodePerfCounters(NodeRuntime& node, const RunProcessState::Guard&) {
  node.process_perf_counters.reset();
  node.cgroup_perf_counters.reset();
  node.perf_counter_target_pid = -1;
  node.perf_counter_attached_pid = -1;
  node.perf_counter_cgroup_path.clear();
  node.perf_counter_cpus.clear();
  node.perf_counter_error_kind.reset();
  node.perf_counter_error.clear();
}

void SetNodePerfCounterError(NodeRuntime& node, const RunProcessState::Guard&,
                             PerfCounterErrorKind kind,
                             std::string_view error) {
  node.process_perf_counters.reset();
  node.cgroup_perf_counters.reset();
  node.perf_counter_attached_pid = -1;
  node.perf_counter_cgroup_path.clear();
  node.perf_counter_cpus.clear();
  node.perf_counter_error_kind = kind;
  node.perf_counter_error = error;
}

void AttachNodePerfCounters(NodeRuntime& node,
                            const RunProcessState::Guard& guard) {
  ResetNodePerfCounters(node, guard);
  node.perf_counter_process_generation = node.RestartCount();
  if (node.process.pid() <= 0 || !node.process.running()) {
    SetNodePerfCounterError(node, guard,
                            PerfCounterErrorKind::kProcessUnavailable,
                            "node process is not running");
    return;
  }

  try {
    if (node.perf_counter_target_kind == PerfCounterTargetKind::kNode ||
        node.perf_counter_target_kind == PerfCounterTargetKind::kWallet) {
      const pid_t target_pid = node.process.pid();
      node.perf_counter_target_pid = target_pid;
      ProcessPerfCounters counters =
          ProcessPerfCounters::Open(target_pid, node.perf_counter_kinds);
      if (node.process.pid() != target_pid || !node.process.running()) {
        SetNodePerfCounterError(
            node, guard, PerfCounterErrorKind::kProcessUnavailable,
            "node process exited or changed while perf counters were opening");
        return;
      }
      node.perf_counter_attached_pid = target_pid;
      node.process_perf_counters.emplace(std::move(counters));
      return;
    }

    if (!node.cgroup) {
      SetNodePerfCounterError(node, guard,
                              PerfCounterErrorKind::kProcessUnavailable,
                              "node cgroup is unavailable");
      return;
    }
    const std::filesystem::path exact_cgroup_path = node.cgroup->access_path();
    CgroupPerfCounters counters =
        CgroupPerfCounters::Open(exact_cgroup_path, node.perf_counter_kinds);
    if (!node.process.running() || !node.cgroup ||
        counters.cgroup_path() != exact_cgroup_path ||
        node.cgroup->access_path() != exact_cgroup_path) {
      SetNodePerfCounterError(
          node, guard, PerfCounterErrorKind::kProcessUnavailable,
          "node process or cgroup changed while perf counters were opening");
      return;
    }
    node.perf_counter_cgroup_path = node.cgroup->path();
    node.perf_counter_cpus = counters.cpus();
    node.cgroup_perf_counters.emplace(std::move(counters));
  } catch (const PerfCounterError& error) {
    SetNodePerfCounterError(node, guard, error.kind(), error.what());
    BBP_LOG(warning) << "perf counters unavailable for " << node.config.id
                     << " target="
                     << PerfCounterTargetKindName(node.perf_counter_target_kind)
                     << ":" << node.perf_counter_target_id << ": "
                     << error.what();
  }
}

}  // namespace bbp::simulator_app_internal
