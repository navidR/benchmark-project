#include "simulator_runtime_node_cleanup.h"

#include <algorithm>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <exception>
#include <memory>
#include <optional>
#include <stdexcept>
#include <stop_token>
#include <string>
#include <string_view>
#include <thread>
#include <utility>
#include <vector>

#include "bbp/cgroup.h"
#include "bbp/drivers/chain_driver.h"
#include "bbp/logging.h"
#include "bbp/network.h"
#include "bbp/run_process_state.h"
#include "bbp/runtime_node_inventory.h"
#include "bbp/simulation_cancelled.h"
#include "bbp/simulation_event_kind.h"
#include "bbp/simulator/options.h"
#include "simulator_cancellable_waiting.h"
#include "simulator_event_writing.h"
#include "simulator_network_launch_planning.h"
#include "simulator_perf_counter_attachment.h"

namespace bbp::simulator_app_internal {

bool RuntimeNodeSupportDestructionAllowed(bool daemon_absence_verified,
                                          bool exact_cgroup_acquired,
                                          bool exact_cgroup_empty,
                                          bool allow_partial_preparation) {
  return daemon_absence_verified &&
         (exact_cgroup_acquired ? exact_cgroup_empty
                                : allow_partial_preparation);
}

namespace {

template <typename Nodes>
std::vector<bool> StopRuntimeNodesImpl(
    const Options& options, const std::filesystem::path& events_path,
    const ChainDriver& driver, Nodes& nodes,
    RuntimeNodeResourceEntryBuilder resource_entry, bool best_effort,
    bool remove_run_cgroup,
    const std::vector<std::uint32_t>* explicit_resource_slots,
    std::optional<std::chrono::steady_clock::time_point> absolute_deadline,
    std::stop_token cleanup_stop_token, bool allow_partial_preparation) {
  const auto shutdown_started = std::chrono::steady_clock::now();
  const auto shutdown_timeout =
      best_effort ? std::chrono::seconds(2) : std::chrono::seconds(15);
  auto shutdown_deadline = shutdown_started + shutdown_timeout;
  if (absolute_deadline) {
    shutdown_deadline = std::min(shutdown_deadline, *absolute_deadline);
  }
  const auto shutdown_budget =
      shutdown_deadline > shutdown_started
          ? shutdown_deadline - shutdown_started
          : std::chrono::steady_clock::duration::zero();
  const auto shutdown_phase = shutdown_budget / 5;
  // Preserve one immutable deadline while reserving bounded opportunities for
  // RPC stop, graceful exit, SIGTERM, SIGKILL, and verified resource cleanup.
  // An earlier phase may finish early, but cannot consume a later reservation.
  const auto rpc_deadline = shutdown_started + shutdown_phase;
  const auto graceful_exit_deadline = shutdown_started + shutdown_phase * 2;
  const auto terminate_deadline = shutdown_started + shutdown_phase * 3;
  const auto kill_deadline = shutdown_started + shutdown_phase * 4;
  std::vector<bool> resource_cleanup_verified(nodes.size(), false);
  std::exception_ptr first_failure;
  const auto record_failure = [&](std::string_view description,
                                  const std::exception_ptr& failure) {
    if (!failure) {
      return;
    }
    if (!first_failure) {
      first_failure = failure;
    }
    if (best_effort) {
      try {
        std::rethrow_exception(failure);
      } catch (const std::exception& error) {
        BBP_LOG(error) << description
                       << " failed during node cleanup: " << error.what();
      } catch (...) {
        BBP_LOG(error) << description << " failed during node cleanup";
      }
    }
  };
  const auto cleanup_step = [&](std::string_view description, auto&& action) {
    try {
      action();
      return true;
    } catch (...) {
      record_failure(description, std::current_exception());
      return false;
    }
  };
  const auto lock_process_state =
      [&](NodeRuntime& node,
          std::chrono::steady_clock::time_point phase_deadline) {
        if (node.run_process_state == nullptr) {
          throw std::logic_error(
              "node has no run process synchronization state: " +
              node.config.id);
        }
        std::optional<RunProcessState::Guard> guard =
            node.run_process_state->TryLockUntil(phase_deadline,
                                                 cleanup_stop_token);
        if (!guard) {
          throw std::runtime_error(
              "node cleanup could not acquire process state before deadline: " +
              node.config.id);
        }
        return std::move(*guard);
      };
  const auto node_process_running =
      [&](NodeRuntime& node,
          std::chrono::steady_clock::time_point phase_deadline) {
        try {
          auto process_guard = lock_process_state(node, phase_deadline);
          return node.process.running();
        } catch (...) {
          return true;
        }
      };
  const auto transition_node_state =
      [&](NodeRuntime& node, NodeRuntimeLifecycle state,
          std::chrono::steady_clock::time_point phase_deadline) {
        {
          auto process_guard = lock_process_state(node, phase_deadline);
          node.SetLifecycle(state);
        }
        WriteNodeStateEvent(events_path, options.run_id, node, state);
      };
  const auto request_node_terminate =
      [&](NodeRuntime& node,
          std::chrono::steady_clock::time_point phase_deadline) {
        auto process_guard = lock_process_state(node, phase_deadline);
        return node.process.RequestTerminate();
      };
  const auto request_node_kill =
      [&](NodeRuntime& node,
          std::chrono::steady_clock::time_point phase_deadline) {
        auto process_guard = lock_process_state(node, phase_deadline);
        return node.process.RequestKill();
      };

  for (auto& node : nodes) {
    bool running = false;
    cleanup_step("perf counter reset", [&] {
      auto process_guard = lock_process_state(node, rpc_deadline);
      ResetNodePerfCounters(node, process_guard);
      running = node.process.running();
    });
    if (running) {
      cleanup_step("stopping state event", [&] {
        transition_node_state(node, NodeRuntimeLifecycle::kStopping,
                              rpc_deadline);
      });
    }
  }

  std::stop_source rpc_stop_source;
  std::stop_callback stop_rpc_cleanup(cleanup_stop_token, [&rpc_stop_source] {
    rpc_stop_source.request_stop();
  });
  std::vector<std::exception_ptr> rpc_failures(nodes.size());
  std::vector<std::size_t> running_processes;
  running_processes.reserve(nodes.size());
  for (std::size_t index = 0; index < nodes.size(); ++index) {
    if (node_process_running(nodes[index], rpc_deadline)) {
      running_processes.push_back(index);
      cleanup_step("RPC stop event", [&] {
        WriteEvent(events_path, options.run_id, nodes[index].config.id,
                   SimulationEventKind::kRpcStop);
      });
    } else {
      cleanup_step("RPC stop skipped event", [&] {
        WriteEvent(events_path, options.run_id, nodes[index].config.id,
                   SimulationEventKind::kRpcStopSkipped,
                   "process is not running");
      });
    }
  }
  std::jthread rpc_deadline_timer(
      [rpc_deadline, &rpc_stop_source](std::stop_token stop_token) {
        try {
          WaitUntil(rpc_deadline, stop_token);
        } catch (const SimulationCancelled&) {
          return;
        }
        rpc_stop_source.request_stop();
      });
  for (const std::size_t index : running_processes) {
    if (cleanup_stop_token.stop_requested() ||
        std::chrono::steady_clock::now() >= rpc_deadline) {
      rpc_failures[index] = std::make_exception_ptr(
          std::runtime_error("node RPC stop deadline expired before request: " +
                             nodes[index].config.id));
      continue;
    }
    try {
      driver.Stop(nodes[index].config, rpc_stop_source.get_token());
    } catch (...) {
      rpc_failures[index] = std::current_exception();
    }
  }
  rpc_stop_source.request_stop();
  rpc_deadline_timer.request_stop();
  for (const std::exception_ptr& failure : rpc_failures) {
    record_failure("node RPC stop", failure);
  }

  std::vector<std::size_t> graceful_processes;
  graceful_processes.reserve(running_processes.size());
  for (const std::size_t index : running_processes) {
    if (!rpc_failures[index]) {
      graceful_processes.push_back(index);
    }
  }
  bool process_running = !graceful_processes.empty();
  while (process_running &&
         std::chrono::steady_clock::now() < graceful_exit_deadline &&
         !cleanup_stop_token.stop_requested()) {
    process_running = false;
    for (const std::size_t index : graceful_processes) {
      process_running =
          node_process_running(nodes[index], graceful_exit_deadline) ||
          process_running;
    }
    if (process_running) {
      const auto now = std::chrono::steady_clock::now();
      if (now < graceful_exit_deadline) {
        std::this_thread::sleep_for(
            std::min(std::chrono::milliseconds(20),
                     std::chrono::duration_cast<std::chrono::milliseconds>(
                         graceful_exit_deadline - now)));
      }
    }
  }

  running_processes.clear();
  for (std::size_t index = 0; index < nodes.size(); ++index) {
    if (!node_process_running(nodes[index], terminate_deadline)) {
      continue;
    }
    running_processes.push_back(index);
    cleanup_step("SIGTERM event", [&] {
      WriteEvent(events_path, options.run_id, nodes[index].config.id,
                 SimulationEventKind::kSigterm);
    });
  }
  std::vector<std::exception_ptr> termination_failures(nodes.size());
  for (const std::size_t index : running_processes) {
    try {
      static_cast<void>(
          request_node_terminate(nodes[index], terminate_deadline));
    } catch (...) {
      termination_failures[index] = std::current_exception();
    }
  }
  bool terminating_processes = true;
  while (terminating_processes &&
         std::chrono::steady_clock::now() < terminate_deadline &&
         !cleanup_stop_token.stop_requested()) {
    terminating_processes = false;
    for (const std::size_t index : running_processes) {
      terminating_processes =
          node_process_running(nodes[index], terminate_deadline) ||
          terminating_processes;
    }
    if (terminating_processes) {
      const auto now = std::chrono::steady_clock::now();
      if (now < terminate_deadline) {
        std::this_thread::sleep_for(
            std::min(std::chrono::milliseconds(20),
                     std::chrono::duration_cast<std::chrono::milliseconds>(
                         terminate_deadline - now)));
      }
    }
  }
  std::vector<std::size_t> kill_processes;
  kill_processes.reserve(running_processes.size());
  for (const std::size_t index : running_processes) {
    if (!node_process_running(nodes[index], kill_deadline)) {
      continue;
    }
    kill_processes.push_back(index);
    try {
      static_cast<void>(request_node_kill(nodes[index], kill_deadline));
    } catch (...) {
      if (!termination_failures[index]) {
        termination_failures[index] = std::current_exception();
      }
    }
  }
  bool killed_processes_running = true;
  while (killed_processes_running &&
         std::chrono::steady_clock::now() < kill_deadline &&
         !cleanup_stop_token.stop_requested()) {
    killed_processes_running = false;
    for (const std::size_t index : kill_processes) {
      killed_processes_running =
          node_process_running(nodes[index], kill_deadline) ||
          killed_processes_running;
    }
    if (killed_processes_running) {
      const auto now = std::chrono::steady_clock::now();
      if (now < kill_deadline) {
        std::this_thread::sleep_for(
            std::min(std::chrono::milliseconds(20),
                     std::chrono::duration_cast<std::chrono::milliseconds>(
                         kill_deadline - now)));
      }
    }
  }
  for (const std::size_t index : kill_processes) {
    if (node_process_running(nodes[index], shutdown_deadline) &&
        !termination_failures[index]) {
      termination_failures[index] = std::make_exception_ptr(std::runtime_error(
          "node process survived SIGKILL: " + nodes[index].config.id));
    }
  }
  for (const std::exception_ptr& failure : termination_failures) {
    record_failure("node process termination", failure);
  }

  for (std::size_t index = 0U; index < nodes.size(); ++index) {
    auto& node = nodes[index];
    std::optional<RunProcessState::Guard> final_process_guard;
    try {
      final_process_guard.emplace(lock_process_state(node, shutdown_deadline));
      const bool daemon_absence_verified = !node.process.running();
      if (!daemon_absence_verified) {
        throw std::runtime_error(
            "refusing resource destruction while node process is running: " +
            node.config.id);
      }
      if (node.Lifecycle() != NodeRuntimeLifecycle::kStopped) {
        node.SetLifecycle(NodeRuntimeLifecycle::kStopped);
        cleanup_step("stopped state event", [&] {
          WriteNodeStateEvent(events_path, options.run_id, node,
                              NodeRuntimeLifecycle::kStopped);
        });
      }
      node.SetLifecycle(NodeRuntimeLifecycle::kCleaning);
      cleanup_step("cleaning state event", [&] {
        WriteNodeStateEvent(events_path, options.run_id, node,
                            NodeRuntimeLifecycle::kCleaning);
      });

      if (node.network_namespace) {
        node.network_namespace->StopHelperAndVerify(shutdown_deadline,
                                                    cleanup_stop_token);
      }
      if (absolute_deadline &&
          std::chrono::steady_clock::now() >= *absolute_deadline) {
        throw std::runtime_error(
            "node cleanup deadline expired before cgroup verification");
      }

      const bool exact_cgroup_acquired = static_cast<bool>(node.cgroup);
      bool exact_cgroup_empty = false;
      if (node.cgroup) {
        node.cgroup->KillAll(shutdown_deadline, cleanup_stop_token);
        exact_cgroup_empty = node.cgroup->Empty();
      }
      if (!RuntimeNodeSupportDestructionAllowed(
              daemon_absence_verified, exact_cgroup_acquired,
              exact_cgroup_empty, allow_partial_preparation)) {
        throw std::runtime_error(
            "refusing support-resource destruction without verified daemon "
            "absence and an empty exact-owned cgroup or an explicitly "
            "permitted unacquired candidate cgroup: " +
            node.config.id);
      }

      std::uint32_t resource_slot = 0U;
      if (explicit_resource_slots != nullptr) {
        resource_slot = explicit_resource_slots->at(index);
      } else if constexpr (requires { nodes.slot(index); }) {
        resource_slot = nodes.slot(index);
      } else {
        resource_slot = static_cast<std::uint32_t>(index);
      }
      bool network_identity_verified_absent = false;
      if (node.network) {
        if (!node.network_namespace) {
          throw std::runtime_error(
              "refusing node network deletion without its namespace: " +
              node.config.id);
        }
        if (!node.network_namespace->node_veth_identity() &&
            !allow_partial_preparation) {
          throw std::runtime_error(
              "refusing node network deletion without its acquired identity: " +
              node.config.id);
        }
        if (node.network_namespace->node_veth_identity()) {
          DeleteNodeVethNetwork(*node.network,
                                *node.network_namespace->node_veth_identity(),
                                cleanup_stop_token);
        } else {
          DeleteNodeVethNetwork(*node.network, cleanup_stop_token);
        }
        network_identity_verified_absent = true;
        cleanup_step("network removed event", [&] {
          WriteEvent(events_path, options.run_id, node.config.id,
                     SimulationEventKind::kNetworkRemoved);
        });
      }
      if (node.network_namespace) {
        node.network_namespace->StopAndVerify(shutdown_deadline,
                                              cleanup_stop_token);
      }
      if (options.cleanup_policy != CleanupPolicy::kRetainCgroups &&
          node.cgroup) {
        try {
          node.cgroup->Remove(shutdown_deadline, cleanup_stop_token);
        } catch (const std::exception& error) {
          cleanup_step("cgroup removal failure event", [&] {
            WriteEvent(events_path, options.run_id, node.config.id,
                       SimulationEventKind::kCgroupRemoveFailed, error.what());
          });
          throw;
        }
      }
      CleanupRuntimeNodeRpcCredential(
          RequireRunOwnership(options),
          resource_entry(options, node.config, resource_slot,
                         RuntimeNodeResourceState::kLive));
      if (node.process.running()) {
        throw std::runtime_error(
            "node process changed while resource cleanup held its state "
            "lock: " +
            node.config.id);
      }
      if (network_identity_verified_absent) {
        node.network_namespace->ClearNodeVethIdentity();
      }
      resource_cleanup_verified[index] = true;
      node.SetLifecycle(NodeRuntimeLifecycle::kCleaned);
      cleanup_step("cleaned state event", [&] {
        WriteNodeStateEvent(events_path, options.run_id, node,
                            NodeRuntimeLifecycle::kCleaned);
      });
    } catch (...) {
      record_failure("node support-resource cleanup", std::current_exception());
      node.SetLifecycle(NodeRuntimeLifecycle::kFailed);
      cleanup_step("cleanup failure state event", [&] {
        WriteNodeStateEvent(events_path, options.run_id, node,
                            NodeRuntimeLifecycle::kFailed);
      });
    }
  }
  if (remove_run_cgroup &&
      options.cleanup_policy != CleanupPolicy::kRetainCgroups &&
      std::all_of(resource_cleanup_verified.begin(),
                  resource_cleanup_verified.end(),
                  [](bool verified) { return verified; })) {
    cleanup_step("run cgroup removal", [&] {
      try {
        Cgroup::RemoveRun(RequireRunOwnership(options).cgroup_name,
                          shutdown_deadline, cleanup_stop_token);
      } catch (const std::exception& error) {
        WriteEvent(events_path, options.run_id, "sim",
                   SimulationEventKind::kRunCgroupRemoveFailed, error.what());
        throw;
      }
    });
  }
  if (!best_effort && first_failure) {
    std::rethrow_exception(first_failure);
  }
  return resource_cleanup_verified;
}

}  // namespace

std::vector<bool> StopRuntimeNodes(
    const Options& options, const std::filesystem::path& events_path,
    const ChainDriver& driver, const RuntimeNodeSnapshot& nodes,
    RuntimeNodeResourceEntryBuilder resource_entry, bool best_effort,
    bool remove_run_cgroup,
    const std::vector<std::uint32_t>* explicit_resource_slots,
    std::optional<std::chrono::steady_clock::time_point> absolute_deadline,
    std::stop_token cleanup_stop_token, bool allow_partial_preparation) {
  return StopRuntimeNodesImpl(options, events_path, driver, nodes,
                              resource_entry, best_effort, remove_run_cgroup,
                              explicit_resource_slots, absolute_deadline,
                              cleanup_stop_token, allow_partial_preparation);
}

std::vector<bool> StopRuntimeNodes(
    const Options& options, const std::filesystem::path& events_path,
    const ChainDriver& driver, std::vector<NodeRuntime>& nodes,
    RuntimeNodeResourceEntryBuilder resource_entry, bool best_effort,
    bool remove_run_cgroup,
    const std::vector<std::uint32_t>* explicit_resource_slots,
    std::optional<std::chrono::steady_clock::time_point> absolute_deadline,
    std::stop_token cleanup_stop_token, bool allow_partial_preparation) {
  return StopRuntimeNodesImpl(options, events_path, driver, nodes,
                              resource_entry, best_effort, remove_run_cgroup,
                              explicit_resource_slots, absolute_deadline,
                              cleanup_stop_token, allow_partial_preparation);
}

}  // namespace bbp::simulator_app_internal
