#include "simulator_runtime_node_stop.h"

#include <algorithm>
#include <atomic>
#include <chrono>
#include <exception>
#include <stdexcept>
#include <string>

#include "bbp/drivers/chain_driver.h"
#include "bbp/simulation_command.h"
#include "bbp/simulation_event_kind.h"
#include "bbp/simulator/node_runtime.h"
#include "bbp/simulator/options.h"
#include "simulator_cancellable_waiting.h"
#include "simulator_event_writing.h"
#include "simulator_node_process_state.h"
#include "simulator_perf_counter_attachment.h"
#include "simulator_runtime_node_freeze.h"

namespace bbp::simulator_app_internal {
namespace {

std::string ExceptionMessage(const std::exception_ptr& error) {
  try {
    std::rethrow_exception(error);
  } catch (const std::exception& exception) {
    return exception.what();
  } catch (...) {
    return "unknown exception";
  }
}

}  // namespace

void StopNodeProcess(const Options& options,
                     const std::filesystem::path& events_path,
                     const ChainDriver& driver, NodeRuntime& node,
                     std::stop_token stop_token, bool allow_rpc_unavailable,
                     SimulationCommandControl* operation_control) {
  ThrowIfStopRequested(stop_token);
  bool rpc_stop_requested = false;
  {
    auto process_guard = LockNodeProcessState(node);
    if (!node.process.running()) {
      throw std::runtime_error("node process is not running: " +
                               node.config.id);
    }
    const NodeRuntimeLifecycle lifecycle = node.Lifecycle();
    if (lifecycle != NodeRuntimeLifecycle::kRunning &&
        !(allow_rpc_unavailable &&
          lifecycle == NodeRuntimeLifecycle::kStarting)) {
      throw std::runtime_error(
          "node stop conflicts with an active lifecycle operation: " +
          node.config.id +
          " (state=" + std::string(NodeRuntimeLifecycleName(lifecycle)) + ")");
    }
    rpc_stop_requested =
        !allow_rpc_unavailable || lifecycle == NodeRuntimeLifecycle::kRunning;
    ResetNodePerfCounters(node, process_guard);
    node.SetLifecycle(NodeRuntimeLifecycle::kStopping);
  }
  WriteNodeStateEvent(events_path, options.run_id, node,
                      NodeRuntimeLifecycle::kStopping);
  try {
    if (node.cgroup && node.cgroup->Frozen()) {
      SetNodeFrozen(options, events_path, node, false, stop_token);
    }
    if (rpc_stop_requested) {
      WriteEvent(events_path, options.run_id, node.config.id,
                 SimulationEventKind::kRpcStop);
      try {
        driver.Stop(node.config, stop_token);
      } catch (...) {
        if (!allow_rpc_unavailable) {
          throw;
        }
        rpc_stop_requested = false;
        WriteEvent(
            events_path, options.run_id, node.config.id,
            SimulationEventKind::kRpcStopSkipped,
            "RPC unavailable during declarative startup stop; using process "
            "termination: " +
                ExceptionMessage(std::current_exception()));
      }
    } else {
      WriteEvent(events_path, options.run_id, node.config.id,
                 SimulationEventKind::kRpcStopSkipped,
                 "RPC readiness incomplete during declarative startup stop; "
                 "using process termination");
    }
    if (rpc_stop_requested) {
      const auto exit_deadline =
          std::chrono::steady_clock::now() + std::chrono::seconds(15);
      while (NodeProcessRunning(node) &&
             std::chrono::steady_clock::now() < exit_deadline) {
        WaitForDuration(std::chrono::milliseconds(50), stop_token);
      }
    }
    if (NodeProcessRunning(node)) {
      WriteEvent(events_path, options.run_id, node.config.id,
                 SimulationEventKind::kSigterm);
      static_cast<void>(RequestNodeTerminate(node));
      const auto terminate_deadline =
          std::chrono::steady_clock::now() + std::chrono::seconds(5);
      if (!WaitForNodeProcessExitUntil(node, terminate_deadline, stop_token)) {
        static_cast<void>(RequestNodeKill(node));
        const auto kill_deadline =
            std::chrono::steady_clock::now() + std::chrono::seconds(5);
        if (!WaitForNodeProcessExitUntil(node, kill_deadline, stop_token)) {
          throw std::runtime_error("node process survived SIGKILL: " +
                                   node.config.id);
        }
      }
    }
    if (NodeProcessRunning(node)) {
      throw std::runtime_error("node process survived graceful stop: " +
                               node.config.id);
    }
  } catch (...) {
    if (operation_control && operation_control->stop_source.stop_requested() &&
        rpc_stop_requested) {
      auto reconciliation_deadline =
          std::chrono::steady_clock::now() +
          kSimulationCommandCancellationReconciliation;
      if (operation_control->absolute_deadline) {
        reconciliation_deadline = std::min(
            reconciliation_deadline, *operation_control->absolute_deadline);
      }
      if (WaitForNodeProcessExitUntil(node, reconciliation_deadline)) {
        {
          auto process_guard = LockNodeProcessState(node);
          ResetNodePerfCounters(node, process_guard);
          node.SetLifecycle(NodeRuntimeLifecycle::kStopped);
        }
        WriteNodeStateEvent(events_path, options.run_id, node,
                            NodeRuntimeLifecycle::kStopped);
        throw;
      }
      operation_control->outcome_unconfirmed.store(true,
                                                   std::memory_order_release);
    }
    bool restored_running = false;
    bool reconciled_stopped = false;
    if (!allow_rpc_unavailable) {
      auto process_guard = LockNodeProcessState(node);
      const bool outcome_unconfirmed =
          operation_control && operation_control->outcome_unconfirmed.load(
                                   std::memory_order_acquire);
      if (node.process.running() && !outcome_unconfirmed) {
        AttachNodePerfCounters(node, process_guard);
        node.SetLifecycle(NodeRuntimeLifecycle::kRunning);
        restored_running = true;
      } else if (!node.process.running()) {
        if (operation_control) {
          operation_control->outcome_unconfirmed.store(
              false, std::memory_order_release);
        }
        ResetNodePerfCounters(node, process_guard);
        node.SetLifecycle(NodeRuntimeLifecycle::kStopped);
        reconciled_stopped = true;
      }
    }
    if (restored_running) {
      WriteNodeStateEvent(events_path, options.run_id, node,
                          NodeRuntimeLifecycle::kRunning);
    } else if (reconciled_stopped) {
      WriteNodeStateEvent(events_path, options.run_id, node,
                          NodeRuntimeLifecycle::kStopped);
    }
    throw;
  }
  {
    auto process_guard = LockNodeProcessState(node);
    node.SetLifecycle(NodeRuntimeLifecycle::kStopped);
  }
  WriteNodeStateEvent(events_path, options.run_id, node,
                      NodeRuntimeLifecycle::kStopped);
}

}  // namespace bbp::simulator_app_internal
