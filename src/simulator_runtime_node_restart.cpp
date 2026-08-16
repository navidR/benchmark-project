#include "simulator_runtime_node_restart.h"

#include <atomic>
#include <chrono>
#include <cstdint>
#include <stdexcept>
#include <string>

#include "bbp/cgroup.h"
#include "bbp/drivers/chain_driver.h"
#include "bbp/peer_connectivity_controller.h"
#include "bbp/runtime_node_inventory.h"
#include "bbp/simulation_cancelled.h"
#include "bbp/simulation_event_kind.h"
#include "bbp/simulator/node_runtime.h"
#include "bbp/simulator/options.h"
#include "simulator_cancellable_waiting.h"
#include "simulator_event_writing.h"
#include "simulator_node_lifecycle_event_details.h"
#include "simulator_node_process_state.h"
#include "simulator_perf_counter_attachment.h"
#include "simulator_runtime_node_freeze.h"

namespace bbp::simulator_app_internal {

bool RestartNode(
    const Options& options, const std::filesystem::path& events_path,
    const ChainDriver& driver,
    PeerConnectivityController& peer_connectivity_controller, NodeRuntime& node,
    std::chrono::steady_clock::time_point lifecycle_epoch,
    RuntimeNodeStartWithPolicy start_node, std::stop_token stop_token,
    std::string_view reason, SimulationCommandControl* operation_control,
    NodeRestartAdmission* admitted_state, bool request_topology_restore,
    const ChainNodeConfig* process_config_override, bool publish_running,
    SimulationCommandControl* cancellation_commit_control,
    std::stop_token committed_stop_token) {
  ThrowIfStopRequested(stop_token);
  if (!node.cgroup) {
    throw std::runtime_error("node restart requires a node cgroup");
  }
  if (node.DeclarativeStopApplied()) {
    throw std::runtime_error("node restart is forbidden after stop_time: " +
                             node.config.id);
  }
  if (node.lifecycle_policy.start_time &&
      node.Lifecycle() == NodeRuntimeLifecycle::kCgroupReady) {
    throw std::runtime_error("node restart is forbidden before start_time: " +
                             node.config.id);
  }

  {
    auto process_guard = LockNodeProcessState(node);
    const NodeRuntimeLifecycle lifecycle = node.Lifecycle();
    if (lifecycle != NodeRuntimeLifecycle::kRunning &&
        lifecycle != NodeRuntimeLifecycle::kFailed &&
        lifecycle != NodeRuntimeLifecycle::kStopped &&
        lifecycle != NodeRuntimeLifecycle::kKilled) {
      throw std::runtime_error(
          "node restart conflicts with an active lifecycle operation: " +
          node.config.id +
          " (state=" + std::string(NodeRuntimeLifecycleName(lifecycle)) + ")");
    }
    if (cancellation_commit_control != nullptr) {
      if (!cancellation_commit_control->TryBeginCommit()) {
        if (cancellation_commit_control->CommitPhase() ==
            SimulationCommandCommitPhase::kCancelled) {
          throw SimulationCancelled();
        }
        ThrowIfStopRequested(stop_token);
        throw std::logic_error(
            "node restart cancellation won without requesting stop");
      }
      stop_token = committed_stop_token;
      ThrowIfStopRequested(stop_token);
    }
    if (admitted_state != nullptr) {
      admitted_state->process = {
          .running = node.process.running(),
          .pid = node.process.pid(),
          .restart_count = node.RestartCount(),
      };
      admitted_state->lifecycle = lifecycle;
      admitted_state->admitted = true;
    }
    node.SetLifecycle(NodeRuntimeLifecycle::kRestarting);
    ResetNodePerfCounters(node, process_guard);
  }
  WriteNodeStateEvent(events_path, options.run_id, node,
                      NodeRuntimeLifecycle::kRestarting);
  WriteEvent(events_path, options.run_id, node.config.id,
             SimulationEventKind::kRestartRequested,
             RestartRequestedDetail(node, reason));
  if (node.cgroup->Frozen()) {
    SetNodeFrozen(options, events_path, node, false, stop_token);
  }
  if (NodeProcessRunning(node)) {
    WriteEvent(events_path, options.run_id, node.config.id,
               SimulationEventKind::kRpcStop);
    if (operation_control != nullptr) {
      operation_control->restart_phase.store(
          SimulationNodeRestartPhase::kStopRequested,
          std::memory_order_release);
    }
    try {
      driver.Stop(node.config, stop_token);
    } catch (...) {
      bool restored_running = false;
      const bool cancellation_after_stop_request =
          operation_control != nullptr && stop_token.stop_requested() &&
          operation_control->restart_phase.load(std::memory_order_acquire) >=
              SimulationNodeRestartPhase::kStopRequested;
      if (!cancellation_after_stop_request) {
        auto process_guard = LockNodeProcessState(node);
        if (node.process.running()) {
          AttachNodePerfCounters(node, process_guard);
          node.SetLifecycle(NodeRuntimeLifecycle::kRunning);
          restored_running = true;
        }
      }
      if (restored_running) {
        WriteNodeStateEvent(events_path, options.run_id, node,
                            NodeRuntimeLifecycle::kRunning);
      }
      throw;
    }
  } else {
    WriteEvent(events_path, options.run_id, node.config.id,
               SimulationEventKind::kRpcStopSkipped, "process is not running");
    if (operation_control != nullptr) {
      operation_control->restart_phase.store(
          SimulationNodeRestartPhase::kOriginalExited,
          std::memory_order_release);
    }
  }
  const auto exit_deadline =
      std::chrono::steady_clock::now() + std::chrono::seconds(15);
  while (NodeProcessRunning(node) &&
         std::chrono::steady_clock::now() < exit_deadline) {
    WaitForDuration(std::chrono::milliseconds(50), stop_token);
  }
  if (NodeProcessRunning(node)) {
    WriteEvent(events_path, options.run_id, node.config.id,
               SimulationEventKind::kSigterm);
    static_cast<void>(RequestNodeTerminate(node));
    const auto terminate_deadline =
        std::chrono::steady_clock::now() + std::chrono::seconds(5);
    try {
      if (!WaitForNodeProcessExitUntil(node, terminate_deadline, stop_token)) {
        static_cast<void>(RequestNodeKill(node));
        const auto kill_deadline =
            std::chrono::steady_clock::now() + std::chrono::seconds(5);
        if (!WaitForNodeProcessExitUntil(node, kill_deadline, stop_token)) {
          throw std::runtime_error("node process survived SIGKILL: " +
                                   node.config.id);
        }
      }
    } catch (...) {
      static_cast<void>(RequestNodeKill(node));
      throw;
    }
  }
  if (operation_control != nullptr) {
    operation_control->restart_phase.store(
        SimulationNodeRestartPhase::kOriginalExited, std::memory_order_release);
  }
  ThrowIfStopRequested(stop_token);
  if (!start_node(options, events_path, driver, node, reason, lifecycle_epoch,
                  true, false, stop_token, process_config_override)) {
    return false;
  }
  if (operation_control != nullptr) {
    operation_control->restart_phase.store(
        SimulationNodeRestartPhase::kReplacementReady,
        std::memory_order_release);
  }
  {
    auto process_guard = LockNodeProcessState(node);
    node.SetLifecycle(publish_running ? NodeRuntimeLifecycle::kRunning
                                      : NodeRuntimeLifecycle::kRestarting);
  }
  WriteNodeStateEvent(events_path, options.run_id, node,
                      publish_running ? NodeRuntimeLifecycle::kRunning
                                      : NodeRuntimeLifecycle::kRestarting);
  if (request_topology_restore) {
    static_cast<void>(
        peer_connectivity_controller.RequestTopologyRestore(node.config.id));
  }
  if (operation_control != nullptr) {
    operation_control->restart_phase.store(
        SimulationNodeRestartPhase::kCompleted, std::memory_order_release);
  }
  return true;
}

void ApplyRuntimeNodeRestarts(
    const Options& options, const std::filesystem::path& events_path,
    const ChainDriver& driver,
    PeerConnectivityController& peer_connectivity_controller,
    const RuntimeNodeSnapshot& nodes,
    std::chrono::steady_clock::time_point lifecycle_epoch,
    RuntimeNodeStartWithPolicy start_node, std::stop_token stop_token) {
  for (std::uint32_t node_index : options.runtime_node_restarts) {
    ThrowIfStopRequested(stop_token);
    if (node_index >= nodes.size()) {
      throw std::runtime_error("runtime restart node is out of range");
    }
    if (!RestartNode(options, events_path, driver, peer_connectivity_controller,
                     nodes[node_index], lifecycle_epoch, start_node,
                     stop_token)) {
      throw std::runtime_error(
          "runtime restart reached node stop_time before completion: " +
          nodes[node_index].config.id);
    }
  }
}

}  // namespace bbp::simulator_app_internal
