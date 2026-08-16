#include "simulator_perf_counter_transactions.h"

#include <algorithm>
#include <set>
#include <stdexcept>
#include <utility>

#include "bbp/runtime_node_inventory.h"
#include "bbp/simulation_command.h"
#include "simulator_cancellable_waiting.h"
#include "simulator_perf_counter_attachment.h"

namespace bbp::simulator_app_internal {

NodePerfCounterSnapshot::NodePerfCounterSnapshot(
    NodeRuntime& runtime, const NodePerfCounterConfiguration& configuration)
    : node(&runtime),
      configuration_kinds(configuration.kinds),
      configuration_kind(configuration.target_kind),
      configuration_id(configuration.target_id) {}

NodePerfCounterConfiguration SnapshotPerfCounterConfiguration(
    const NodePerfCounterSnapshot& snapshot) {
  return NodePerfCounterConfiguration{
      .node_id = snapshot.node->config.id,
      .kinds = snapshot.configuration_kinds,
      .target_kind = snapshot.configuration_kind,
      .target_id = snapshot.configuration_id,
  };
}

void SwapNodePerfCounterSnapshot(NodePerfCounterSnapshot& snapshot,
                                 const RunProcessState::Guard&) {
  NodeRuntime& node = *snapshot.node;
  node.perf_counter_kinds.swap(snapshot.configuration_kinds);
  node.perf_counter_target_id.swap(snapshot.configuration_id);
  std::swap(node.perf_counter_target_kind, snapshot.configuration_kind);
  node.process_perf_counters.swap(snapshot.process_counters);
  node.cgroup_perf_counters.swap(snapshot.cgroup_counters);
  std::swap(node.perf_counter_target_pid, snapshot.target_pid);
  std::swap(node.perf_counter_attached_pid, snapshot.attached_pid);
  std::swap(node.perf_counter_process_generation, snapshot.process_generation);
  node.perf_counter_cgroup_path.swap(snapshot.cgroup_path);
  node.perf_counter_cpus.swap(snapshot.cpus);
  node.perf_counter_error_kind.swap(snapshot.error_kind);
  node.perf_counter_error.swap(snapshot.error);
}

void BeginNodePerfCounterReplacement(NodePerfCounterSnapshot& snapshot,
                                     const RunProcessState::Guard& guard) {
  SwapNodePerfCounterSnapshot(snapshot, guard);
  snapshot.replacement_started = true;
}

void RestoreNodePerfCounterSnapshot(NodePerfCounterSnapshot& snapshot,
                                    const RunProcessState::Guard& guard) {
  if (!snapshot.replacement_started) {
    return;
  }
  SwapNodePerfCounterSnapshot(snapshot, guard);
  snapshot.replacement_started = false;
}

void RequireNodePerfCounterAttachment(const NodeRuntime& node,
                                      const RunProcessState::Guard&) {
  if (node.perf_counter_error_kind || !node.perf_counter_error.empty()) {
    throw std::runtime_error(
        "perf counter attachment failed for " + node.config.id + ": " +
        (node.perf_counter_error.empty() ? std::string(PerfCounterErrorKindName(
                                               *node.perf_counter_error_kind))
                                         : node.perf_counter_error));
  }
  const bool process_target =
      node.perf_counter_target_kind == PerfCounterTargetKind::kNode ||
      node.perf_counter_target_kind == PerfCounterTargetKind::kWallet;
  if (process_target) {
    if (!node.process_perf_counters || node.cgroup_perf_counters ||
        node.perf_counter_target_pid <= 0 ||
        node.perf_counter_attached_pid != node.perf_counter_target_pid) {
      throw std::runtime_error(
          "process perf counter attachment is incomplete "
          "for " +
          node.config.id);
    }
    return;
  }
  if (!node.cgroup_perf_counters || node.process_perf_counters ||
      node.perf_counter_cgroup_path.empty() || node.perf_counter_cpus.empty()) {
    throw std::runtime_error(
        "cgroup perf counter attachment is incomplete for " + node.config.id);
  }
}

std::vector<NodePerfCounterSnapshot> ReplaceNodePerfCountersTransactional(
    std::span<const NodePerfCounterAssignment> assignments,
    const RunProcessState::Guard& process_guard, std::stop_token stop_token,
    const NodePerfCounterTransactionBackend* backend) {
  if (assignments.empty()) {
    throw std::invalid_argument(
        "perf counter transaction requires at least one node");
  }
  if (backend != nullptr &&
      (!backend->is_running || !backend->attach || !backend->reset)) {
    throw std::invalid_argument(
        "perf counter transaction backend is incomplete");
  }
  std::set<std::string> unique_nodes;
  for (const NodePerfCounterAssignment& assignment : assignments) {
    if (assignment.node == nullptr ||
        assignment.configuration.node_id != assignment.node->config.id ||
        assignment.configuration.target_id.empty() ||
        assignment.configuration.kinds.empty() ||
        !unique_nodes.insert(assignment.configuration.node_id).second) {
      throw std::invalid_argument(
          "perf counter transaction contains an invalid or duplicate node");
    }
    const std::set<PerfCounterKind> unique_kinds(
        assignment.configuration.kinds.begin(),
        assignment.configuration.kinds.end());
    if (unique_kinds.size() != assignment.configuration.kinds.size()) {
      throw std::invalid_argument(
          "perf counter transaction contains duplicate counters");
    }
    const bool running = backend != nullptr
                             ? backend->is_running(*assignment.node)
                             : assignment.node->process.running();
    if (assignment.require_running && !running) {
      throw std::runtime_error("perf counter target node is not running: " +
                               assignment.configuration.node_id);
    }
    if ((assignment.configuration.target_kind ==
             PerfCounterTargetKind::kGroup ||
         assignment.configuration.target_kind ==
             PerfCounterTargetKind::kCgroup) &&
        !assignment.node->cgroup) {
      throw std::runtime_error(
          "perf counter target node has no owned cgroup: " +
          assignment.configuration.node_id);
    }
  }
  ThrowIfStopRequested(stop_token);

  std::vector<NodePerfCounterSnapshot> snapshots;
  snapshots.reserve(assignments.size());
  try {
    for (const NodePerfCounterAssignment& assignment : assignments) {
      ThrowIfStopRequested(stop_token);
      snapshots.emplace_back(*assignment.node, assignment.configuration);
      NodePerfCounterSnapshot& snapshot = snapshots.back();
      BeginNodePerfCounterReplacement(snapshot, process_guard);
      const bool running = backend != nullptr
                               ? backend->is_running(*assignment.node)
                               : assignment.node->process.running();
      if (running) {
        if (backend != nullptr) {
          backend->attach(*assignment.node, process_guard,
                          assignment.require_attachment);
        } else {
          AttachNodePerfCounters(*assignment.node, process_guard);
          if (assignment.require_attachment) {
            RequireNodePerfCounterAttachment(*assignment.node, process_guard);
          }
        }
      } else {
        if (backend != nullptr) {
          backend->reset(*assignment.node, process_guard);
        } else {
          ResetNodePerfCounters(*assignment.node, process_guard);
        }
      }
    }
    ThrowIfStopRequested(stop_token);
  } catch (...) {
    for (auto snapshot = snapshots.rbegin(); snapshot != snapshots.rend();
         ++snapshot) {
      RestoreNodePerfCounterSnapshot(*snapshot, process_guard);
    }
    throw;
  }
  return snapshots;
}

void ApplyPerfCounterCommand(const SimulationCommand& command,
                             RuntimeNodeSnapshot& nodes,
                             const RunProcessState::Guard& process_guard,
                             const NodePerfCounterTransactionBackend* backend) {
  if (!command.perf_counter_target) {
    throw std::runtime_error("perf counter command requires a typed target");
  }
  const PerfCounterTarget& target = *command.perf_counter_target;
  if (target.id.empty()) {
    throw std::runtime_error("perf counter target id must not be empty");
  }
  if (target.node_ids.empty()) {
    throw std::runtime_error("perf counter target must resolve to a node");
  }
  if (command.perf_counter_kinds.empty()) {
    throw std::runtime_error("perf counter selection must not be empty");
  }
  const std::set<PerfCounterKind> unique_kinds(
      command.perf_counter_kinds.begin(), command.perf_counter_kinds.end());
  if (unique_kinds.size() != command.perf_counter_kinds.size()) {
    throw std::runtime_error("perf counter selection contains duplicates");
  }
  if (target.kind == PerfCounterTargetKind::kGroup) {
    if (command.node_id != "sim") {
      throw std::runtime_error("group perf counter command must target sim");
    }
  } else {
    if (target.node_ids.size() != 1U ||
        command.node_id != target.node_ids.front()) {
      throw std::runtime_error(
          "non-group perf counter command must target its resolved node");
    }
    if ((target.kind == PerfCounterTargetKind::kNode ||
         target.kind == PerfCounterTargetKind::kCgroup) &&
        target.id != target.node_ids.front()) {
      throw std::runtime_error(
          "node and cgroup perf target ids must equal their resolved node");
    }
    if (target.kind == PerfCounterTargetKind::kWallet &&
        !IsCanonicalWalletPerfTargetId(target.id)) {
      throw std::runtime_error(
          "wallet perf target id must be wallet-<positive-index>");
    }
  }

  std::vector<NodeRuntime*> target_nodes;
  target_nodes.reserve(target.node_ids.size());
  std::set<std::string> unique_nodes;
  for (const std::string& node_id : target.node_ids) {
    if (node_id.empty() || !unique_nodes.insert(node_id).second) {
      throw std::runtime_error(
          "perf counter target contains an empty or duplicate node id");
    }
    const auto node = std::find_if(nodes.begin(), nodes.end(),
                                   [&](const NodeRuntime& candidate) {
                                     return candidate.config.id == node_id;
                                   });
    if (node == nodes.end()) {
      throw std::runtime_error("perf counter target references unknown node: " +
                               node_id);
    }
    const bool running = backend != nullptr ? backend->is_running(*node)
                                            : node->process.running();
    if (!running) {
      throw std::runtime_error("perf counter target node is not running: " +
                               node_id);
    }
    if ((target.kind == PerfCounterTargetKind::kGroup ||
         target.kind == PerfCounterTargetKind::kCgroup) &&
        !node->cgroup) {
      throw std::runtime_error(
          "perf counter target node has no owned cgroup: " + node_id);
    }
    target_nodes.push_back(&*node);
  }

  std::vector<NodePerfCounterAssignment> assignments;
  assignments.reserve(target_nodes.size());
  for (NodeRuntime* node : target_nodes) {
    assignments.push_back(NodePerfCounterAssignment{
        .node = node,
        .configuration =
            NodePerfCounterConfiguration{
                .node_id = node->config.id,
                .kinds = command.perf_counter_kinds,
                .target_kind = target.kind,
                .target_id = target.id,
            },
        .require_running = true,
        .require_attachment = true,
    });
  }
  static_cast<void>(ReplaceNodePerfCountersTransactional(
      assignments, process_guard, {}, backend));
}

void RollBackNodePerfCounterSnapshots(
    std::vector<NodePerfCounterSnapshot>& snapshots,
    RunProcessState& run_process_state) {
  auto process_guard = run_process_state.Lock();
  for (auto snapshot = snapshots.rbegin(); snapshot != snapshots.rend();
       ++snapshot) {
    RestoreNodePerfCounterSnapshot(*snapshot, process_guard);
  }
}

}  // namespace bbp::simulator_app_internal
