#include "simulator_runtime_node_startup.h"

#include <algorithm>
#include <atomic>
#include <boost/json/object.hpp>
#include <boost/json/serialize.hpp>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <exception>
#include <map>
#include <mutex>
#include <optional>
#include <set>
#include <stdexcept>
#include <stop_token>
#include <string>
#include <string_view>
#include <thread>
#include <utility>
#include <vector>

#include "bbp/capability.h"
#include "bbp/drivers/chain_driver.h"
#include "bbp/drivers/chain_driver_registry.h"
#include "bbp/node_lifecycle_policy.h"
#include "bbp/runtime_node_inventory.h"
#include "bbp/runtime_node_resource_manifest.h"
#include "bbp/simulation_cancelled.h"
#include "bbp/simulation_event_kind.h"
#include "bbp/simulator/node_runtime.h"
#include "bbp/simulator/options.h"
#include "simulator_cancellable_waiting.h"
#include "simulator_event_writing.h"
#include "simulator_network_launch_planning.h"
#include "simulator_node_lifecycle_event_details.h"
#include "simulator_node_process_state.h"
#include "simulator_perf_counter_attachment.h"
#include "simulator_process_spawn_readiness.h"
#include "simulator_resource_profile_decoding.h"
#include "simulator_runtime_node_preparation.h"
#include "simulator_runtime_node_stop.h"
#include "simulator_scenario_node_resolution.h"

namespace bbp::simulator_app_internal {

void ApplyDeclarativeStopDuringStart(
    const Options& options, const std::filesystem::path& events_path,
    const ChainDriver& driver, NodeRuntime& node,
    std::chrono::steady_clock::time_point lifecycle_epoch,
    std::stop_token stop_token) {
  if (node.DeclarativeStopApplied()) {
    return;
  }
  if (!node.lifecycle_policy.stop_time) {
    throw std::logic_error(
        "declarative start interruption requires node stop_time");
  }
  node.MarkDeclarativeStopApplied();
  WriteEvent(events_path, options.run_id, node.config.id,
             SimulationEventKind::kNodeStopDeadlineReached,
             NodeLifecycleDeadlineDetail(
                 node, options.time_scale, lifecycle_epoch,
                 *node.lifecycle_policy.stop_time, "declarative_stop"));
  if (NodeProcessRunning(node)) {
    StopNodeProcess(options, events_path, driver, node, stop_token, true);
  } else {
    {
      auto process_guard = LockNodeProcessState(node);
      ResetNodePerfCounters(node, process_guard);
      node.SetLifecycle(NodeRuntimeLifecycle::kStopped);
    }
    WriteNodeStateEvent(events_path, options.run_id, node,
                        NodeRuntimeLifecycle::kStopped);
  }
}

bool StartNodeProcessWithPolicy(
    const Options& options, const std::filesystem::path& events_path,
    const ChainDriver& driver, NodeRuntime& node,
    std::mutex& node_network_state_mutex, std::string_view reason,
    std::chrono::steady_clock::time_point lifecycle_epoch,
    bool first_attempt_is_restart, bool transition_to_running,
    std::stop_token stop_token,
    const ChainNodeConfig* process_config_override) {
  constexpr std::chrono::milliseconds kRestartBackoff(100);
  std::stop_source operation_stop_source;
  std::stop_callback stop_on_run_request(stop_token, [&operation_stop_source] {
    operation_stop_source.request_stop();
  });
  std::atomic<bool> node_stop_deadline_reached = false;
  std::optional<std::jthread> node_stop_timer;
  std::optional<std::chrono::steady_clock::time_point> node_stop_deadline;
  if (node.lifecycle_policy.stop_time) {
    node_stop_deadline = SteadyDeadline(
        lifecycle_epoch,
        options.time_scale.WallDuration(*node.lifecycle_policy.stop_time));
    node_stop_timer.emplace(
        [deadline = *node_stop_deadline, &node_stop_deadline_reached,
         &operation_stop_source](std::stop_token timer_stop_token) {
          try {
            WaitUntil(deadline, timer_stop_token);
          } catch (const SimulationCancelled&) {
            return;
          }
          node_stop_deadline_reached.store(true, std::memory_order_release);
          operation_stop_source.request_stop();
        });
  }
  const auto stop_due = [&] {
    return node_stop_deadline_reached.load(std::memory_order_acquire) ||
           (node_stop_deadline &&
            std::chrono::steady_clock::now() >= *node_stop_deadline);
  };
  const auto stop_timer = [&] {
    if (node_stop_timer) {
      node_stop_timer->request_stop();
      if (node_stop_timer->joinable()) {
        node_stop_timer->join();
      }
      node_stop_timer.reset();
    }
  };

  bool restart_attempt = first_attempt_is_restart;
  bool startup_exit_seen = false;
  std::string attempt_reason(reason);
  try {
    while (true) {
      if (stop_due() && !stop_token.stop_requested()) {
        ApplyDeclarativeStopDuringStart(options, events_path, driver, node,
                                        lifecycle_epoch, stop_token);
        if (startup_exit_seen) {
          throw std::runtime_error(
              "node stop_time was reached before restart policy recovered "
              "RPC readiness: " +
              node.config.id);
        }
        stop_timer();
        return false;
      }
      ThrowIfStopRequested(stop_token);
      try {
        StartNodeProcessAttempt(
            options, events_path, driver, node, node_network_state_mutex,
            attempt_reason, lifecycle_epoch, restart_attempt,
            transition_to_running, operation_stop_source.get_token(),
            process_config_override);
        if (stop_due() && !stop_token.stop_requested()) {
          ApplyDeclarativeStopDuringStart(options, events_path, driver, node,
                                          lifecycle_epoch, stop_token);
          if (startup_exit_seen) {
            throw std::runtime_error(
                "node stop_time was reached before restart policy recovered "
                "RPC readiness: " +
                node.config.id);
          }
          stop_timer();
          return false;
        }
        if (stop_token.stop_requested()) {
          if (startup_exit_seen) {
            throw std::runtime_error(
                "simulation stopped before restart policy recovered RPC "
                "readiness: " +
                node.config.id);
          }
          throw SimulationCancelled();
        }
        stop_timer();
        return true;
      } catch (const NodeExitedBeforeRpcReady& exit) {
        startup_exit_seen = true;
        const bool policy_allows = NodeRestartPolicyAllowsRestart(
            node.lifecycle_policy.restart_policy, exit.wait_status());
        const bool declarative_stop_due = stop_due();
        const bool restart = policy_allows && !declarative_stop_due &&
                             !stop_token.stop_requested();
        const std::string_view suppression_reason =
            declarative_stop_due
                ? "node_stop_time"
                : (stop_token.stop_requested() ? "simulation_stop" : "");
        WriteEvent(events_path, options.run_id, node.config.id,
                   SimulationEventKind::kRestartPolicyApplied,
                   RestartPolicyAppliedDetail(node, exit.wait_status(), restart,
                                              suppression_reason));
        if (!restart) {
          if (declarative_stop_due && !stop_token.stop_requested()) {
            ApplyDeclarativeStopDuringStart(options, events_path, driver, node,
                                            lifecycle_epoch, stop_token);
          }
          if (!policy_allows) {
            throw std::runtime_error(
                "node process exited before RPC readiness and restart policy "
                "did not restart it: " +
                node.config.id);
          }
          throw std::runtime_error(
              "node process exited before RPC readiness and restart was "
              "suppressed before recovery: " +
              node.config.id);
        }
        {
          auto process_guard = LockNodeProcessState(node);
          if (node.Lifecycle() != NodeRuntimeLifecycle::kFailed) {
            throw std::runtime_error(
                "startup restart conflicts with an active lifecycle "
                "operation: " +
                node.config.id);
          }
          node.SetLifecycle(NodeRuntimeLifecycle::kRestarting);
        }
        WriteNodeStateEvent(events_path, options.run_id, node,
                            NodeRuntimeLifecycle::kRestarting);
        WriteEvent(events_path, options.run_id, node.config.id,
                   SimulationEventKind::kRestartRequested,
                   RestartRequestedDetail(node, "restart_policy"));
        try {
          WaitForDuration(kRestartBackoff, operation_stop_source.get_token());
        } catch (const SimulationCancelled&) {
          if (stop_due() && !stop_token.stop_requested()) {
            ApplyDeclarativeStopDuringStart(options, events_path, driver, node,
                                            lifecycle_epoch, stop_token);
          }
          throw std::runtime_error(
              "node process exited before RPC readiness and restart was "
              "cancelled before recovery: " +
              node.config.id);
        }
        restart_attempt = true;
        attempt_reason = "restart_policy";
      } catch (const SimulationCancelled&) {
        if (stop_due() && !stop_token.stop_requested()) {
          ApplyDeclarativeStopDuringStart(options, events_path, driver, node,
                                          lifecycle_epoch, stop_token);
          if (startup_exit_seen) {
            throw std::runtime_error(
                "node stop_time was reached before restart policy recovered "
                "RPC readiness: " +
                node.config.id);
          }
          stop_timer();
          return false;
        }
        if (startup_exit_seen) {
          throw std::runtime_error(
              "simulation stopped before restart policy recovered RPC "
              "readiness: " +
              node.config.id);
        }
        throw;
      }
    }
  } catch (...) {
    stop_timer();
    throw;
  }
}

bool StartPreparedNode(const Options& options,
                       const std::filesystem::path& events_path,
                       const ChainDriver& driver, NodeRuntime& node,
                       std::mutex& node_network_state_mutex,
                       std::string_view reason,
                       std::chrono::steady_clock::time_point lifecycle_epoch,
                       std::stop_token stop_token) {
  ThrowIfStopRequested(stop_token);
  if (!node.cgroup) {
    throw std::runtime_error("node start requires a prepared cgroup: " +
                             node.config.id);
  }
  if (node.DeclarativeStopApplied()) {
    throw std::runtime_error("node start is forbidden after stop_time: " +
                             node.config.id);
  }
  if (NodeProcessRunning(node)) {
    throw std::runtime_error("node process is already running: " +
                             node.config.id);
  }
  return StartNodeProcessWithPolicy(options, events_path, driver, node,
                                    node_network_state_mutex, reason,
                                    lifecycle_epoch, false, true, stop_token);
}

void StartInitialPreparedNodes(
    const Options& options, const std::filesystem::path& events_path,
    const ChainDriver& driver, std::vector<NodeRuntime>& nodes,
    std::mutex& node_network_state_mutex,
    std::chrono::steady_clock::time_point simulation_epoch,
    std::stop_token stop_token) {
  std::stop_source initial_start_stop_source;
  std::stop_callback stop_initial_starts_on_request(
      stop_token, [&initial_start_stop_source] {
        initial_start_stop_source.request_stop();
      });
  std::mutex initial_start_failure_mutex;
  std::exception_ptr initial_start_failure;
  const std::size_t initial_start_count = static_cast<std::size_t>(
      std::count_if(nodes.begin(), nodes.end(), [](const NodeRuntime& node) {
        return !node.lifecycle_policy.start_time.has_value();
      }));
  std::atomic<std::size_t> initial_starts_in_progress(initial_start_count);
  std::condition_variable_any initial_starts_finished;
  std::mutex initial_starts_finished_mutex;
  std::vector<std::jthread> initial_starts;
  initial_starts.reserve(initial_start_count);
  for (NodeRuntime& node : nodes) {
    if (node.lifecycle_policy.start_time) {
      continue;
    }
    NodeRuntime* initial_node = &node;
    initial_starts.emplace_back([&options, &events_path, &driver, initial_node,
                                 &node_network_state_mutex, simulation_epoch,
                                 &initial_start_stop_source,
                                 &initial_start_failure_mutex,
                                 &initial_start_failure,
                                 &initial_starts_in_progress,
                                 &initial_starts_finished,
                                 &initial_starts_finished_mutex] {
      bool start_in_progress = true;
      const auto mark_start_finished = [&] {
        if (!start_in_progress) {
          return;
        }
        start_in_progress = false;
        initial_starts_in_progress.fetch_sub(1U, std::memory_order_acq_rel);
        initial_starts_finished.notify_all();
      };
      try {
        const bool started = StartPreparedNode(
            options, events_path, driver, *initial_node,
            node_network_state_mutex, "initial_start", simulation_epoch,
            initial_start_stop_source.get_token());
        mark_start_finished();
        if (started && initial_node->lifecycle_policy.stop_time &&
            !initial_node->DeclarativeStopApplied() &&
            initial_starts_in_progress.load(std::memory_order_acquire) != 0U) {
          const auto stop_deadline = SteadyDeadline(
              simulation_epoch, options.time_scale.WallDuration(
                                    *initial_node->lifecycle_policy.stop_time));
          std::unique_lock<std::mutex> wait_lock(initial_starts_finished_mutex);
          initial_starts_finished.wait_until(
              wait_lock, initial_start_stop_source.get_token(), stop_deadline,
              [&initial_starts_in_progress] {
                return initial_starts_in_progress.load(
                           std::memory_order_acquire) == 0U;
              });
          wait_lock.unlock();
          ThrowIfStopRequested(initial_start_stop_source.get_token());
          if (initial_starts_in_progress.load(std::memory_order_acquire) !=
                  0U &&
              std::chrono::steady_clock::now() >= stop_deadline) {
            ApplyDeclarativeStopDuringStart(
                options, events_path, driver, *initial_node, simulation_epoch,
                initial_start_stop_source.get_token());
          }
        }
      } catch (...) {
        mark_start_finished();
        {
          std::lock_guard<std::mutex> lock(initial_start_failure_mutex);
          if (!initial_start_failure) {
            initial_start_failure = std::current_exception();
          }
        }
        initial_start_stop_source.request_stop();
      }
    });
  }
  for (std::jthread& initial_start : initial_starts) {
    if (initial_start.joinable()) {
      initial_start.join();
    }
  }
  if (initial_start_failure) {
    std::rethrow_exception(initial_start_failure);
  }
}

void StartInitialNodes(
    const Options& options, const std::filesystem::path& run_root,
    const std::filesystem::path& events_path, const ChainDriverSpec& chain_spec,
    const ChainDriver& driver, const RuntimePeerTopology& runtime_topology,
    std::vector<NodeRuntime>& nodes, RunProcessState& run_process_state,
    std::mutex& node_network_state_mutex,
    std::chrono::steady_clock::time_point simulation_epoch,
    std::stop_token stop_token) {
  if (options.isolate_network) {
    RequireNetworkSetupCapabilities();
  }
  if (options.isolate_network && options.nodes > 1 &&
      !HostIpv4ForwardingEnabled()) {
    throw std::runtime_error(
        "isolated multi-node chain runs require IPv4 forwarding in the parent "
        "network namespace");
  }
  nodes.reserve(options.nodes);
  std::vector<ChainNodeConfig> prepared_configs;
  prepared_configs.reserve(options.nodes);
  for (uint32_t i = 0; i < options.nodes; ++i) {
    ChainNodeConfigRequest config_request;
    config_request.run_id = options.run_id;
    config_request.run_root = run_root;
    config_request.daemon_binary = EffectiveNodeBinary(options, i);
    config_request.data_dir = NodeDataDirectoryRelative(options, i);
    config_request.node_index = i;
    config_request.network = EffectiveNodeChainNetwork(options, i);
    config_request.extra_args = EffectiveNodeExtraArgs(options, i);
    if (!options.node_ids.empty()) {
      config_request.node_id = options.node_ids.at(i);
    }
    config_request.wallet_enabled =
        EffectiveNodeWalletConfig(options, i).enabled;
    config_request.connect_peers =
        StartupPeerAddresses(options, runtime_topology, chain_spec, i);
    prepared_configs.push_back(MakeChainNodeConfig(chain_spec, config_request));
  }
  RuntimeNodeResourceManifest startup_manifest{
      .ownership = RequireRunOwnership(options),
      .isolated_network = options.isolate_network,
      .nodes = {},
  };
  startup_manifest.nodes.reserve(prepared_configs.size());
  WriteRuntimeNodeResourceManifest(startup_manifest);

  for (uint32_t i = 0; i < options.nodes; ++i) {
    ThrowIfStopRequested(stop_token);
    std::string resource_profile_name;
    const auto resource_profile = options.node_resource_profiles.find(i);
    if (resource_profile != options.node_resource_profiles.end()) {
      resource_profile_name = resource_profile->second;
    }
    std::string network_profile_name;
    const auto network_profile = options.node_network_profiles.find(i);
    if (network_profile != options.node_network_profiles.end()) {
      network_profile_name = network_profile->second;
    }
    std::vector<DirectionalNetworkPolicy> directional_network_policies;
    if (options.isolate_network) {
      directional_network_policies =
          DirectionalNetworkPoliciesForNode(options, runtime_topology, i);
    }
    nodes.emplace_back();
    startup_manifest.nodes.push_back(
        RuntimeNodeResourceEntryFor(options, prepared_configs[i], i,
                                    RuntimeNodeResourceState::kPendingAdd));
    WriteRuntimeNodeResourceManifest(startup_manifest);
    PrepareNodeRuntime(
        options, events_path, nodes.back(), std::move(prepared_configs[i]), i,
        InitialResourceLimits(options, i), std::move(resource_profile_name),
        std::move(network_profile_name),
        std::move(directional_network_policies), std::nullopt,
        run_process_state, stop_token);
    nodes.back().lifecycle_policy = EffectiveNodeLifecyclePolicy(options, i);
  }

  StartInitialPreparedNodes(options, events_path, driver, nodes,
                            node_network_state_mutex, simulation_epoch,
                            stop_token);
  ConnectAvailableStartupPeers(options, events_path, driver, nodes,
                               node_network_state_mutex, std::nullopt,
                               simulation_epoch, stop_token);
  for (RuntimeNodeResourceEntry& entry : startup_manifest.nodes) {
    entry.state = RuntimeNodeResourceState::kLive;
  }
  WriteRuntimeNodeResourceManifest(startup_manifest);
  const std::optional<RuntimeNodeResourceManifest> published_manifest =
      TryLoadRuntimeNodeResourceManifest(RequireRunOwnership(options));
  if (!published_manifest || *published_manifest != startup_manifest) {
    throw std::runtime_error(
        "startup runtime resource manifest read-back failed");
  }
}

namespace {

std::string NodePeerEndpoint(const NodeRuntime& node) {
  return node.config.p2p_host + ":" + std::to_string(node.config.p2p_port);
}

void ConnectAvailableStartupPeersImpl(
    const Options& options, const std::filesystem::path& events_path,
    const ChainDriver& driver, auto& nodes,
    std::mutex& node_network_state_mutex,
    std::optional<std::size_t> changed_node,
    std::chrono::steady_clock::time_point lifecycle_epoch,
    std::stop_token stop_token) {
  std::set<std::pair<std::size_t, std::string>> completed_connections;
  while (true) {
    ThrowIfStopRequested(stop_token);
    bool stop_applied = false;
    const auto now = std::chrono::steady_clock::now();
    for (std::size_t index = 0; index < nodes.size(); ++index) {
      if (changed_node && index != *changed_node) {
        continue;
      }
      NodeRuntime& node = nodes[index];
      if (node.lifecycle_policy.stop_time && !node.DeclarativeStopApplied() &&
          now >= SteadyDeadline(lifecycle_epoch,
                                options.time_scale.WallDuration(
                                    *node.lifecycle_policy.stop_time))) {
        ApplyDeclarativeStopDuringStart(options, events_path, driver, node,
                                        lifecycle_epoch, stop_token);
        stop_applied = true;
      }
    }
    if (stop_applied) {
      continue;
    }

    std::optional<std::chrono::steady_clock::time_point> next_stop_deadline;
    for (std::size_t index = 0; index < nodes.size(); ++index) {
      if (changed_node && index != *changed_node) {
        continue;
      }
      const NodeRuntime& node = nodes[index];
      if (!node.lifecycle_policy.stop_time || node.DeclarativeStopApplied()) {
        continue;
      }
      const auto deadline = SteadyDeadline(
          lifecycle_epoch,
          options.time_scale.WallDuration(*node.lifecycle_policy.stop_time));
      if (!next_stop_deadline || deadline < *next_stop_deadline) {
        next_stop_deadline = deadline;
      }
    }

    std::stop_source operation_stop_source;
    std::stop_callback stop_on_request(stop_token, [&operation_stop_source] {
      operation_stop_source.request_stop();
    });
    std::optional<std::jthread> deadline_timer;
    if (next_stop_deadline) {
      deadline_timer.emplace(
          [deadline = *next_stop_deadline,
           &operation_stop_source](std::stop_token timer_stop_token) {
            try {
              WaitUntil(deadline, timer_stop_token);
            } catch (const SimulationCancelled&) {
              return;
            }
            operation_stop_source.request_stop();
          });
    }
    const auto stop_deadline_timer = [&] {
      if (deadline_timer) {
        deadline_timer->request_stop();
        if (deadline_timer->joinable()) {
          deadline_timer->join();
        }
        deadline_timer.reset();
      }
    };

    try {
      std::map<std::string, std::size_t> running_endpoints;
      for (std::size_t index = 0; index < nodes.size(); ++index) {
        if (nodes[index].AllowsChainMetrics() &&
            NodeProcessRunning(nodes[index])) {
          running_endpoints.emplace(NodePeerEndpoint(nodes[index]), index);
        }
      }
      for (std::size_t source_index = 0; source_index < nodes.size();
           ++source_index) {
        NodeRuntime& source = nodes[source_index];
        if (!source.AllowsChainMetrics() || !NodeProcessRunning(source)) {
          continue;
        }
        std::vector<std::string> connect_peers;
        {
          std::lock_guard<std::mutex> network_lock(node_network_state_mutex);
          connect_peers = source.config.connect_peers;
        }
        for (const std::string& peer : connect_peers) {
          if (completed_connections.contains({source_index, peer})) {
            continue;
          }
          const auto target = running_endpoints.find(peer);
          if (target == running_endpoints.end()) {
            continue;
          }
          if (changed_node && source_index != *changed_node &&
              target->second != *changed_node) {
            continue;
          }
          ThrowIfStopRequested(operation_stop_source.get_token());
          driver.ConnectPeer(source.config, peer,
                             operation_stop_source.get_token());
          driver.WaitForPeerAddress(
              source.config, peer,
              std::chrono::seconds(options.ready_timeout_sec),
              operation_stop_source.get_token());
          boost::json::object detail;
          detail["address"] = peer;
          detail["reason"] =
              changed_node ? "declarative_start" : "initial_startup";
          WriteEvent(events_path, options.run_id, source.config.id,
                     SimulationEventKind::kStartupPeerConnected,
                     boost::json::serialize(detail));
          completed_connections.emplace(source_index, peer);
        }
      }
      const bool lifecycle_deadline_reached =
          next_stop_deadline &&
          std::chrono::steady_clock::now() >= *next_stop_deadline;
      const bool lifecycle_timer_requested =
          operation_stop_source.stop_requested() &&
          !stop_token.stop_requested();
      stop_deadline_timer();
      if (lifecycle_deadline_reached || lifecycle_timer_requested) {
        continue;
      }
      ThrowIfStopRequested(stop_token);
      return;
    } catch (const SimulationCancelled&) {
      stop_deadline_timer();
      if (stop_token.stop_requested()) {
        throw;
      }
    } catch (...) {
      stop_deadline_timer();
      throw;
    }
  }
}

}  // namespace

void ConnectAvailableStartupPeers(
    const Options& options, const std::filesystem::path& events_path,
    const ChainDriver& driver, std::vector<NodeRuntime>& nodes,
    std::mutex& node_network_state_mutex,
    std::optional<std::size_t> changed_node,
    std::chrono::steady_clock::time_point lifecycle_epoch,
    std::stop_token stop_token) {
  ConnectAvailableStartupPeersImpl(options, events_path, driver, nodes,
                                   node_network_state_mutex, changed_node,
                                   lifecycle_epoch, stop_token);
}

void ConnectAvailableStartupPeers(
    const Options& options, const std::filesystem::path& events_path,
    const ChainDriver& driver, const RuntimeNodeSnapshot& nodes,
    std::mutex& node_network_state_mutex,
    std::optional<std::size_t> changed_node,
    std::chrono::steady_clock::time_point lifecycle_epoch,
    std::stop_token stop_token) {
  ConnectAvailableStartupPeersImpl(options, events_path, driver, nodes,
                                   node_network_state_mutex, changed_node,
                                   lifecycle_epoch, stop_token);
}

}  // namespace bbp::simulator_app_internal
