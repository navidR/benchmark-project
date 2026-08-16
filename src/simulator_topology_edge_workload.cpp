#include "simulator_topology_edge_workload.h"

#include <algorithm>
#include <chrono>
#include <condition_variable>
#include <exception>
#include <mutex>
#include <optional>
#include <stdexcept>
#include <string>
#include <string_view>
#include <thread>
#include <utility>
#include <vector>

#include "bbp/drivers/chain_driver.h"
#include "bbp/drivers/chain_driver_registry.h"
#include "bbp/logging.h"
#include "bbp/network.h"
#include "bbp/peer_connectivity_controller.h"
#include "bbp/runtime_node_inventory.h"
#include "bbp/runtime_peer_topology.h"
#include "bbp/simulation_event_kind.h"
#include "bbp/simulator/node_runtime.h"
#include "bbp/simulator/options.h"
#include "bbp/simulator/topology_edge_workload.h"
#include "bbp/simulator/workload_kind.h"
#include "simulator_cancellable_waiting.h"
#include "simulator_event_writing.h"
#include "simulator_network_event_details.h"
#include "simulator_network_launch_planning.h"
#include "simulator_workload_event_details.h"
#include "simulator_workload_mutation_error.h"

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

constexpr auto kWorkloadMutationRollbackTimeout = std::chrono::seconds(10);

class WorkloadMutationRollbackControl {
 public:
  explicit WorkloadMutationRollbackControl(std::chrono::milliseconds timeout)
      : timeout_(timeout) {}

  WorkloadMutationRollbackControl(const WorkloadMutationRollbackControl&) =
      delete;
  WorkloadMutationRollbackControl& operator=(
      const WorkloadMutationRollbackControl&) = delete;

  std::chrono::steady_clock::time_point Begin() {
    std::call_once(begin_once_, [this] {
      deadline_ = std::chrono::steady_clock::now() + timeout_;
      try {
        rollback_timer_.emplace(
            [this, deadline = *deadline_](std::stop_token timer_stop_token) {
              try {
                std::condition_variable_any condition;
                std::mutex mutex;
                std::unique_lock<std::mutex> lock(mutex);
                condition.wait_until(lock, timer_stop_token, deadline,
                                     [] { return false; });
              } catch (...) {
                rollback_stop_source_.request_stop();
                return;
              }
              if (!timer_stop_token.stop_requested()) {
                rollback_stop_source_.request_stop();
              }
            });
      } catch (...) {
        rollback_stop_source_.request_stop();
        throw;
      }
    });
    return *deadline_;
  }

  std::stop_token token() const { return rollback_stop_source_.get_token(); }

 private:
  std::chrono::milliseconds timeout_;
  std::stop_source rollback_stop_source_;
  std::once_flag begin_once_;
  std::optional<std::chrono::steady_clock::time_point> deadline_;
  std::optional<std::jthread> rollback_timer_;
};

std::unique_lock<std::mutex> AcquireWorkloadRollbackLock(
    std::mutex& mutex, std::stop_token stop_token,
    std::chrono::steady_clock::time_point deadline) {
  std::unique_lock<std::mutex> lock(mutex, std::defer_lock);
  while (true) {
    ThrowIfStopRequested(stop_token);
    if (std::chrono::steady_clock::now() >= deadline) {
      throw std::runtime_error("workload rollback lock deadline exceeded");
    }
    if (lock.try_lock()) {
      if (std::chrono::steady_clock::now() >= deadline) {
        lock.unlock();
        throw std::runtime_error("workload rollback lock deadline exceeded");
      }
      return lock;
    }
    WaitUntil(std::min(deadline, std::chrono::steady_clock::now() +
                                     std::chrono::milliseconds(20)),
              stop_token);
  }
}

std::string PeerAddress(const RuntimeNodeSnapshot& nodes, uint32_t node) {
  const uint32_t node_index = node - 1U;
  return nodes[node_index].config.p2p_host + ":" +
         std::to_string(nodes[node_index].config.p2p_port);
}

std::vector<std::string> RuntimeTopologyAllowedPeers(
    const RuntimePeerTopology& topology, const RuntimeNodeSnapshot& nodes,
    std::uint32_t node_index) {
  std::vector<std::string> allowed;
  for (const std::uint32_t peer_index :
       topology.ActivePeerIndexes(node_index)) {
    if (peer_index >= nodes.size()) {
      throw std::runtime_error(
          "runtime topology peer references an unknown node");
    }
    allowed.push_back(nodes[peer_index].config.id);
  }
  return allowed;
}

bool DynamicPhysicalPeerRequired(const RuntimePeerTopology& topology,
                                 std::uint32_t first, std::uint32_t second) {
  return topology.PhysicalPeerRequired(first, second);
}

}  // namespace

std::vector<std::string> DynamicPhysicalTopologyPeerEndpoints(
    const RuntimePeerTopology& topology,
    const std::vector<ChainNodeConfig>& configs, std::uint32_t node_index) {
  std::vector<std::string> peers;
  for (std::uint32_t peer_index = 0U; peer_index < configs.size();
       ++peer_index) {
    if (peer_index == node_index ||
        !DynamicPhysicalPeerRequired(topology, node_index, peer_index)) {
      continue;
    }
    peers.push_back(configs[peer_index].p2p_host + ":" +
                    std::to_string(configs[peer_index].p2p_port));
  }
  return peers;
}

void ApplyTopologyEdgeWorkload(
    const Options& options, const std::filesystem::path& events_path,
    const ChainDriverSpec& chain_spec, const ChainDriver& driver,
    PeerConnectivityController& controller,
    RuntimePeerTopology& runtime_topology, const RuntimeNodeSnapshot& nodes,
    std::mutex& node_network_state_mutex, const TopologyEdgeWorkload& workload,
    WorkloadKind action, std::uint32_t workload_index,
    std::uint32_t workload_count, std::stop_token stop_token) {
  ThrowIfStopRequested(stop_token);
  const std::uint32_t from = workload.from - 1U;
  const std::uint32_t to = workload.to - 1U;
  if (from >= nodes.size() || to >= nodes.size() || from == to) {
    throw std::runtime_error("runtime topology edge action node is invalid");
  }

  NodeRuntime& source = nodes[from];
  NodeRuntime& target = nodes[to];
  const RuntimePeerTopologyEdge previous = runtime_topology.Edge(from, to);
  const bool physical_peer_required_before =
      runtime_topology.PhysicalPeerRequired(from, to);
  RuntimePeerTopologyEdge attempted = previous;
  const std::vector<DirectionalNetworkPolicy> expected_previous_policies =
      runtime_topology.DirectionalPolicies(NetworkAddressPlan(options), from);
  std::vector<DirectionalNetworkPolicy> previous_policies;
  std::vector<std::string> previous_restart_peers;
  std::vector<ChainNodeConfig> node_configs;
  std::vector<std::vector<std::string>> previous_process_start_peers(
      nodes.size());
  std::vector<bool> uses_physical_start_peers(nodes.size(), false);
  {
    std::lock_guard<std::mutex> lock(node_network_state_mutex);
    previous_policies = source.directional_network_policies;
    previous_restart_peers = source.config.connect_peers;
    node_configs.reserve(nodes.size());
    for (std::size_t index = 0U; index < nodes.size(); ++index) {
      node_configs.push_back(nodes[index].config);
      uses_physical_start_peers[index] =
          nodes[index].uses_physical_start_connect_peers;
      if (uses_physical_start_peers[index]) {
        previous_process_start_peers[index] =
            nodes[index].process_start_connect_peers;
      }
    }
  }
  if (previous_policies != expected_previous_policies) {
    throw std::runtime_error(
        "runtime directional policy state does not match topology model");
  }
  const std::vector<std::string> expected_previous_allowed =
      RuntimeTopologyAllowedPeers(runtime_topology, nodes, from);
  const std::vector<std::string> previous_allowed =
      controller.AllowedPeersFor(source.config.id);
  if (previous_allowed != expected_previous_allowed) {
    throw std::runtime_error(
        "runtime allowed-peer state does not match topology model");
  }
  const std::vector<std::string> expected_previous_restart_peers =
      StartupPeerAddresses(options, runtime_topology, chain_spec, from);
  if (previous_restart_peers != expected_previous_restart_peers) {
    throw std::runtime_error(
        "runtime restart-peer state does not match topology model");
  }
  for (std::uint32_t index = 0U; index < nodes.size(); ++index) {
    if (!uses_physical_start_peers[index]) {
      continue;
    }
    const std::vector<std::string> expected =
        DynamicPhysicalTopologyPeerEndpoints(runtime_topology, node_configs,
                                             index);
    if (previous_process_start_peers[index] != expected) {
      throw std::runtime_error(
          "runtime physical restart-peer state does not match topology model");
    }
  }

  const std::string peer_address = PeerAddress(nodes, workload.to);
  std::vector<std::string> before_peer_addresses;
  bool connected_before = false;

  bool model_mutated = false;
  bool physical_peer_transition = false;
  bool physical_peer_required_after = physical_peer_required_before;
  bool kernel_updated = false;
  bool allowed_updated = false;
  bool peer_transition_attempted = false;
  bool config_updated = false;
  bool publication_started = false;
  std::vector<DirectionalNetworkPolicy> desired_policies;
  std::vector<DirectionalNetworkPolicy> rollback_policy_state;
  std::vector<std::string> desired_allowed;
  std::vector<std::string> desired_restart_peers;
  std::vector<std::vector<std::string>> desired_process_start_peers(
      nodes.size());
  WorkloadMutationRollbackControl rollback_control(
      kWorkloadMutationRollbackTimeout);
  const auto begin_rollback = [&rollback_control] {
    return rollback_control.Begin();
  };
  try {
    if (action == WorkloadKind::kSetEdgeCondition) {
      if (!workload.condition) {
        throw std::runtime_error(
            "set_edge_condition action is missing its condition");
      }
      static_cast<void>(
          runtime_topology.SetCondition(from, to, *workload.condition));
    } else if (action == WorkloadKind::kActivateEdge) {
      static_cast<void>(runtime_topology.SetActive(from, to, true));
    } else if (action == WorkloadKind::kDeactivateEdge) {
      static_cast<void>(runtime_topology.SetActive(from, to, false));
    } else if (action == WorkloadKind::kRestoreEdge) {
      static_cast<void>(runtime_topology.RestoreBaseline(from, to));
    } else {
      throw std::runtime_error("unknown runtime topology edge action");
    }
    model_mutated = true;
    attempted = runtime_topology.Edge(from, to);
    physical_peer_required_after =
        runtime_topology.PhysicalPeerRequired(from, to);
    physical_peer_transition =
        physical_peer_required_before != physical_peer_required_after;
    if (physical_peer_transition) {
      before_peer_addresses = driver.PeerAddresses(source.config, stop_token);
      connected_before = !driver
                              .ConnectedPeerAddresses(
                                  source.config, {peer_address}, stop_token)
                              .empty();
    }

    desired_policies =
        runtime_topology.DirectionalPolicies(NetworkAddressPlan(options), from);
    desired_allowed =
        RuntimeTopologyAllowedPeers(runtime_topology, nodes, from);
    desired_restart_peers =
        StartupPeerAddresses(options, runtime_topology, chain_spec, from);
    for (std::uint32_t index = 0U; index < nodes.size(); ++index) {
      if (uses_physical_start_peers[index]) {
        desired_process_start_peers[index] =
            DynamicPhysicalTopologyPeerEndpoints(runtime_topology, node_configs,
                                                 index);
      }
    }
    controller.ValidateAllowedPeerUpdate(source.config.id, desired_allowed);
    rollback_policy_state = desired_policies;

    ThrowIfStopRequested(stop_token);
    if (source.network) {
      if (!source.network_namespace) {
        throw std::runtime_error(
            "runtime topology source has no network namespace");
      }
      std::lock_guard<std::mutex> lock(node_network_state_mutex);
      UpdateDirectionalNetworkPoliciesInNamespace(
          source.network_namespace->fd(), source.network->peer_name,
          previous_policies, desired_policies, stop_token, std::nullopt,
          rollback_control.token(), begin_rollback);
      source.directional_network_policies.swap(rollback_policy_state);
      kernel_updated = true;
    } else if (previous_policies != desired_policies) {
      throw std::runtime_error(
          "runtime topology conditions require isolated networking");
    }

    if (previous_allowed != desired_allowed) {
      controller.SetAllowedPeers(source.config.id, desired_allowed);
      allowed_updated = true;
    }

    std::vector<std::string> after_peer_addresses;
    bool connected_after = connected_before;
    if (physical_peer_transition) {
      peer_transition_attempted = true;
      if (physical_peer_required_after) {
        controller.ConnectPeer(source.config.id, target.config.id,
                               std::chrono::seconds(workload.timeout_sec),
                               stop_token);
      } else {
        controller.DisconnectPeer(source.config.id, target.config.id,
                                  std::chrono::seconds(workload.timeout_sec),
                                  stop_token);
      }
      after_peer_addresses = driver.PeerAddresses(source.config, stop_token);
      connected_after = physical_peer_required_after;
    }

    {
      std::lock_guard<std::mutex> lock(node_network_state_mutex);
      source.config.connect_peers.swap(desired_restart_peers);
      for (std::size_t index = 0U; index < nodes.size(); ++index) {
        if (uses_physical_start_peers[index]) {
          nodes[index].process_start_connect_peers.swap(
              desired_process_start_peers[index]);
        }
      }
    }
    config_updated = true;

    if (physical_peer_transition) {
      publication_started = true;
      WriteEvent(events_path, options.run_id, source.config.id,
                 physical_peer_required_after
                     ? SimulationEventKind::kPeerConnected
                     : SimulationEventKind::kPeerDisconnected,
                 PeerChurnDetail(
                     workload_index, workload_count, workload.from, workload.to,
                     peer_address,
                     static_cast<std::uint64_t>(before_peer_addresses.size()),
                     static_cast<std::uint64_t>(after_peer_addresses.size()),
                     connected_before, connected_after, workload.timeout_sec));
    }
    publication_started = true;
    WriteEvent(events_path, options.run_id, source.config.id,
               SimulationEventKind::kTopologyEdgeUpdated,
               TopologyEdgeUpdateDetail(action, workload_index, workload_count,
                                        previous, attempted, true, true,
                                        workload.timeout_sec));
  } catch (...) {
    const std::exception_ptr original_error = std::current_exception();
    bool original_outcome_unconfirmed = false;
    try {
      std::rethrow_exception(original_error);
    } catch (const DirectionalNetworkPolicyOutcomeUnconfirmed&) {
      original_outcome_unconfirmed = true;
    } catch (...) {
    }
    std::vector<std::string> rollback_errors;
    auto rollback_deadline = std::chrono::steady_clock::now();
    try {
      rollback_deadline = begin_rollback();
    } catch (...) {
      rollback_errors.push_back("rollback control: " +
                                ExceptionMessage(std::current_exception()));
    }
    const std::stop_token rollback_stop_token = rollback_control.token();
    const auto rollback = [&](std::string_view step, const auto& operation) {
      try {
        ThrowIfStopRequested(rollback_stop_token);
        if (std::chrono::steady_clock::now() >= rollback_deadline) {
          throw std::runtime_error("topology edge rollback deadline expired");
        }
        operation();
      } catch (const std::exception& error) {
        rollback_errors.push_back(std::string(step) + ": " + error.what());
      } catch (...) {
        rollback_errors.push_back(std::string(step) + ": unknown exception");
      }
    };

    if (kernel_updated) {
      rollback("kernel policy", [&] {
        auto lock = AcquireWorkloadRollbackLock(
            node_network_state_mutex, rollback_stop_token, rollback_deadline);
        UpdateDirectionalNetworkPoliciesInNamespace(
            source.network_namespace->fd(), source.network->peer_name,
            desired_policies, previous_policies, rollback_stop_token,
            rollback_deadline, rollback_stop_token, begin_rollback);
        source.directional_network_policies.swap(rollback_policy_state);
      });
    }
    // A deactivation removes the target from the allow-list before
    // disconnecting it.  Restore that list before attempting a compensating
    // reconnect.  For an activation, keep the target eligible until any
    // compensating disconnect has completed, then restore the prior (inactive)
    // allow-list.
    const bool restore_allowed_before_peer =
        allowed_updated && physical_peer_required_before;
    if (restore_allowed_before_peer) {
      rollback("allowed peers", [&] {
        controller.SetAllowedPeers(source.config.id, previous_allowed);
      });
      allowed_updated = false;
    }
    if (peer_transition_attempted) {
      rollback("peer state", [&] {
        const bool connected_now =
            !driver
                 .ConnectedPeerAddresses(source.config, {peer_address},
                                         rollback_stop_token)
                 .empty();
        if (connected_now == connected_before) {
          return;
        }
        if (connected_before) {
          if (!physical_peer_required_before) {
            throw std::runtime_error(
                "cannot restore a pre-existing physical session for an "
                "inactive logical edge");
          }
          controller.ConnectPeer(source.config.id, target.config.id,
                                 std::chrono::seconds(workload.timeout_sec),
                                 rollback_stop_token);
        } else {
          controller.DisconnectPeer(source.config.id, target.config.id,
                                    std::chrono::seconds(workload.timeout_sec),
                                    rollback_stop_token);
        }
      });
    }
    if (allowed_updated) {
      rollback("allowed peers", [&] {
        controller.SetAllowedPeers(source.config.id, previous_allowed);
      });
    }
    if (config_updated || model_mutated) {
      rollback("runtime config", [&] {
        auto lock = AcquireWorkloadRollbackLock(
            node_network_state_mutex, rollback_stop_token, rollback_deadline);
        source.config.connect_peers.swap(previous_restart_peers);
        for (std::size_t index = 0U; index < nodes.size(); ++index) {
          if (uses_physical_start_peers[index]) {
            nodes[index].process_start_connect_peers.swap(
                previous_process_start_peers[index]);
          }
        }
      });
    }
    if (model_mutated) {
      rollback("topology model",
               [&] { runtime_topology.RestoreState(previous); });
    }

    if (!rollback_errors.empty()) {
      const std::string original_message = ExceptionMessage(original_error);
      for (const std::string& rollback_error : rollback_errors) {
        BBP_LOG(error) << "topology edge rollback failed after "
                       << original_message << ": " << rollback_error;
      }
      try {
        WriteEvent(events_path, options.run_id, source.config.id,
                   SimulationEventKind::kTopologyEdgeUpdateRollbackFailed,
                   TopologyEdgeRollbackFailureDetail(
                       action, previous, attempted, original_message,
                       rollback_errors));
      } catch (const std::exception& event_error) {
        BBP_LOG(error) << "failed to record topology rollback failure: "
                       << event_error.what();
      }
    }
    if (original_outcome_unconfirmed || publication_started ||
        !rollback_errors.empty()) {
      ThrowWorkloadMutationOutcomeUnconfirmed(
          "topology edge update outcome is unconfirmed", original_error,
          rollback_errors);
    }
    std::rethrow_exception(original_error);
  }
}

}  // namespace bbp::simulator_app_internal
