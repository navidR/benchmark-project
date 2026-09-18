#include "simulator_process_spawn_readiness.h"

#include <chrono>
#include <cstdint>
#include <mutex>
#include <optional>
#include <stop_token>
#include <string>
#include <thread>
#include <utility>

#include "bbp/drivers/chain_driver.h"
#include "bbp/logging.h"
#include "bbp/process.h"
#include "bbp/simulation_cancelled.h"
#include "bbp/simulation_event_kind.h"
#include "bbp/simulator/node_runtime.h"
#include "bbp/simulator/options.h"
#include "simulator_cancellable_waiting.h"
#include "simulator_event_writing.h"
#include "simulator_node_lifecycle_event_details.h"
#include "simulator_node_process_state.h"
#include "simulator_perf_counter_attachment.h"

namespace bbp::simulator_app_internal {
namespace {

[[noreturn]] void RecordNodeExitBeforeRpcReady(
    const Options& options, const std::filesystem::path& events_path,
    NodeRuntime& node) {
  std::string exit_detail;
  std::optional<int> wait_status;
  {
    auto process_guard = LockNodeProcessState(node);
    ResetNodePerfCounters(node, process_guard);
    exit_detail = ProcessExitDetail(node.process, process_guard);
    wait_status = node.process.exit_status();
    node.SetLifecycle(NodeRuntimeLifecycle::kFailed);
  }
  WriteEvent(events_path, options.run_id, node.config.id,
             SimulationEventKind::kProcessExitedBeforeRpcReady, exit_detail);
  WriteNodeStateEvent(events_path, options.run_id, node,
                      NodeRuntimeLifecycle::kFailed);
  if (!wait_status) {
    throw std::runtime_error(
        "node exited before RPC readiness without a wait status: " +
        node.config.id);
  }
  throw NodeExitedBeforeRpcReady(node.config.id, *wait_status);
}

}  // namespace

void StartNodeProcessAttempt(
    const Options& options, const std::filesystem::path& events_path,
    const ChainDriver& driver, NodeRuntime& node,
    std::mutex& node_network_state_mutex, std::string_view reason,
    std::chrono::steady_clock::time_point lifecycle_epoch, bool restart_attempt,
    bool transition_to_running, std::stop_token stop_token,
    const ChainNodeConfig* process_config_override) {
  ProcessSpec process;
  std::vector<ProcessSpec> companions;
  ChainNodeConfig process_config;
  {
    std::lock_guard<std::mutex> network_lock(node_network_state_mutex);
    process_config = process_config_override == nullptr
                         ? node.config
                         : *process_config_override;
    if (process_config.id != node.config.id) {
      throw std::logic_error(
          "node process configuration override changed node identity");
    }
    if (node.uses_physical_start_connect_peers) {
      process_config.connect_peers = node.process_start_connect_peers;
    }
    process = driver.RenderProcess(process_config);
    companions = driver.RenderCompanionProcesses(process_config);
  }
  if (node.network_namespace) {
    process.network_namespace_fd = node.network_namespace->fd();
  }
  {
    auto process_guard = LockNodeProcessState(node);
    const NodeRuntimeLifecycle expected =
        restart_attempt ? NodeRuntimeLifecycle::kRestarting
                        : NodeRuntimeLifecycle::kCgroupReady;
    if (node.Lifecycle() != expected) {
      throw std::runtime_error(
          "node start conflicts with an active lifecycle operation: " +
          node.config.id + " (state=" +
          std::string(NodeRuntimeLifecycleName(node.Lifecycle())) + ")");
    }
    node.SetLifecycle(NodeRuntimeLifecycle::kStarting);
  }
  WriteNodeStateEvent(events_path, options.run_id, node,
                      NodeRuntimeLifecycle::kStarting);
  ChildProcess spawned;
  StopNodeCompanionProcesses(node);
  try {
    {
      auto process_guard = LockNodeProcessState(node);
      node.companion_processes.reserve(companions.size());
    }
    spawned = ChildProcess::Spawn(process, node.cgroup->access_path());
    for (auto& companion : companions) {
      companion.network_namespace_fd = process.network_namespace_fd;
      auto child = ChildProcess::Spawn(companion, node.cgroup->access_path());
      auto process_guard = LockNodeProcessState(node);
      node.companion_processes.push_back(std::move(child));
    }
  } catch (...) {
    spawned.Kill();
    StopNodeCompanionProcesses(node);
    {
      auto process_guard = LockNodeProcessState(node);
      ResetNodePerfCounters(node, process_guard);
      node.SetLifecycle(NodeRuntimeLifecycle::kFailed);
    }
    WriteNodeStateEvent(events_path, options.run_id, node,
                        NodeRuntimeLifecycle::kFailed);
    throw;
  }
  const std::uint64_t restart_count =
      restart_attempt ? node.IncrementRestartCount() : node.RestartCount();
  pid_t process_pid = -1;
  std::string process_detail;
  {
    auto process_guard = LockNodeProcessState(node);
    node.process = std::move(spawned);
    node.process_started_at = std::chrono::steady_clock::now();
    AttachNodePerfCounters(node, process_guard);
    process_pid = node.process.pid();
    process_detail =
        restart_attempt
            ? RestartDetail(process_pid, restart_count, reason)
            : ProcessStartedDetail(node, reason, lifecycle_epoch,
                                   options.time_scale, process_guard);
  }
  BBP_LOG(info) << "started " << node.config.id << " pid=" << process_pid
                << " reason=" << reason;
  if (restart_attempt) {
    WriteEvent(events_path, options.run_id, node.config.id,
               SimulationEventKind::kProcessRestarted, process_detail);
  } else {
    WriteEvent(events_path, options.run_id, node.config.id,
               SimulationEventKind::kProcessStarted, process_detail);
  }
  std::stop_source readiness_stop_source;
  std::stop_callback stop_readiness_on_request(
      stop_token,
      [&readiness_stop_source] { readiness_stop_source.request_stop(); });
  const auto companions_running = [&] {
    auto process_guard = LockNodeProcessState(node);
    for (auto& companion : node.companion_processes) {
      if (!companion.running()) return false;
    }
    return true;
  };
  const auto record_companion_exit = [&] {
    {
      auto process_guard = LockNodeProcessState(node);
      node.SetLifecycle(NodeRuntimeLifecycle::kFailed);
    }
    WriteNodeStateEvent(events_path, options.run_id, node,
                        NodeRuntimeLifecycle::kFailed);
    throw std::runtime_error("node companion exited before RPC readiness: " + node.config.id);
  };
  std::jthread exit_monitor([&](std::stop_token monitor_stop_token) {
    try {
      while (!monitor_stop_token.stop_requested()) {
        if (!NodeProcessRunning(node) || !companions_running()) {
          readiness_stop_source.request_stop();
          return;
        }
        WaitForDuration(std::chrono::milliseconds(10), monitor_stop_token);
      }
    } catch (const SimulationCancelled&) {
    }
  });
  const auto stop_exit_monitor = [&] {
    exit_monitor.request_stop();
    if (exit_monitor.joinable()) {
      exit_monitor.join();
    }
  };
  try {
    driver.WaitReady(process_config,
                     std::chrono::seconds(options.ready_timeout_sec),
                     readiness_stop_source.get_token());
  } catch (const SimulationCancelled&) {
    stop_exit_monitor();
    if (!NodeProcessRunning(node)) {
      RecordNodeExitBeforeRpcReady(options, events_path, node);
    }
    if (!companions_running()) record_companion_exit();
    throw;
  } catch (...) {
    stop_exit_monitor();
    if (!NodeProcessRunning(node)) {
      RecordNodeExitBeforeRpcReady(options, events_path, node);
    }
    {
      auto process_guard = LockNodeProcessState(node);
      node.SetLifecycle(NodeRuntimeLifecycle::kFailed);
    }
    WriteNodeStateEvent(events_path, options.run_id, node,
                        NodeRuntimeLifecycle::kFailed);
    throw;
  }
  stop_exit_monitor();
  if (!NodeProcessRunning(node)) {
    RecordNodeExitBeforeRpcReady(options, events_path, node);
  }
  if (!companions_running()) record_companion_exit();
  WriteEvent(events_path, options.run_id, node.config.id,
             SimulationEventKind::kRpcReady);
  if (transition_to_running) {
    {
      auto process_guard = LockNodeProcessState(node);
      node.SetLifecycle(NodeRuntimeLifecycle::kRunning);
    }
    WriteNodeStateEvent(events_path, options.run_id, node,
                        NodeRuntimeLifecycle::kRunning);
  }
}

}  // namespace bbp::simulator_app_internal
