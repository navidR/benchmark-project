#include "simulator_runtime_node_replacement.h"

#include <algorithm>
#include <boost/asio/io_context.hpp>
#include <boost/asio/ip/tcp.hpp>
#include <boost/json/array.hpp>
#include <boost/json/object.hpp>
#include <boost/json/parse.hpp>
#include <boost/json/serialize.hpp>
#include <boost/system/error_code.hpp>
#include <chrono>
#include <cstdint>
#include <exception>
#include <filesystem>
#include <limits>
#include <memory>
#include <mutex>
#include <optional>
#include <stdexcept>
#include <stop_token>
#include <string>
#include <string_view>
#include <system_error>
#include <thread>
#include <utility>
#include <vector>

#include "bbp/cgroup.h"
#include "bbp/drivers/chain_driver_registry.h"
#include "bbp/network.h"
#include "bbp/peer_connectivity_controller.h"
#include "bbp/run_process_state.h"
#include "bbp/runtime_node_inventory.h"
#include "bbp/runtime_peer_topology.h"
#include "bbp/runtime_wallet_registry.h"
#include "bbp/scenario_service.h"
#include "bbp/simulation_cancelled.h"
#include "bbp/simulation_command.h"
#include "bbp/simulation_event_kind.h"
#include "bbp/simulation_node_add.h"
#include "bbp/simulator/node_runtime.h"
#include "bbp/simulator/options.h"
#include "bbp/util.h"
#include "simulator_cancellable_waiting.h"
#include "simulator_event_writing.h"
#include "simulator_live_workload_state.h"
#include "simulator_network_condition_application.h"
#include "simulator_network_event_details.h"
#include "simulator_network_launch_planning.h"
#include "simulator_node_process_state.h"
#include "simulator_perf_counter_attachment.h"
#include "simulator_resource_event_details.h"
#include "simulator_resource_limit_application.h"
#include "simulator_resource_profile_decoding.h"
#include "simulator_runtime_published_node_config.h"
#include "simulator_scenario_serialization.h"
#include "simulator_tcp_endpoint_reservation.h"
#include "simulator_transaction_observation_tracking.h"

namespace bbp::simulator_app_internal {

namespace {

std::string RuntimeNodeReplacementExceptionMessage(
    const std::exception_ptr& error) {
  try {
    std::rethrow_exception(error);
  } catch (const std::exception& exception) {
    return exception.what();
  } catch (...) {
    return "unknown exception";
  }
}

}  // namespace

RuntimeNodeReplaceResult ReplaceRuntimeNodeTransactional(
    const Options& options, const std::filesystem::path& run_root,
    const std::filesystem::path& events_path, const ChainDriver& driver,
    RuntimeNodeInventory& inventory, RuntimeWalletRegistry& runtime_registry,
    PeerConnectivityController& peer_controller,
    const RuntimePeerTopology& runtime_topology,
    const PeerTopologyConfig& live_topology_config,
    const std::shared_ptr<LiveWalletWorkloadRegistry>& wallet_workloads,
    const std::shared_ptr<LiveBlockGenerationWorkloadRegistry>&
        block_generation_workloads,
    const std::shared_ptr<LiveWaitUntilHeightWorkloadRegistry>&
        wait_until_height_workloads,
    const std::shared_ptr<LiveWaitForPeersWorkloadRegistry>&
        wait_for_peers_workloads,
    const TransactionObservationTracker& transaction_tracker,
    RunProcessState& run_process_state,
    std::chrono::steady_clock::time_point lifecycle_epoch,
    std::string_view node_id, const SimulationNodeReplaceRequest& request,
    bool restore_native_mining, std::string_view native_mining_reward_address,
    SimulationCommandControl* operation_control, std::stop_token stop_token,
    const RuntimeNodeReplacementDependencies& dependencies) {
  if (request.chain != options.chain) {
    throw std::runtime_error(
        "node-replace chain must match the active simulation chain");
  }
  if (request.count != 1U) {
    throw std::runtime_error(
        "node-replace requires count one and preserves topology");
  }
  if (!request.node_ids.empty() &&
      (request.node_ids.size() != 1U || request.node_ids.front() != node_id)) {
    throw std::runtime_error(
        "node-replace explicit node id must equal the selected node");
  }
  if (!wallet_workloads) {
    throw std::logic_error("node-replace wallet workload service is missing");
  }

  const RuntimeNodeSnapshot before = inventory.Snapshot();
  const RuntimeWalletSnapshot before_registry = runtime_registry.Snapshot();
  if (before.generation() == std::numeric_limits<std::uint64_t>::max()) {
    throw std::overflow_error("node-replace inventory generation overflow");
  }
  std::size_t target_index = before.size();
  std::vector<std::string> initial_node_ids;
  std::vector<ChainNodeConfig> final_configs;
  initial_node_ids.reserve(before.size());
  final_configs.reserve(before.size());
  {
    std::lock_guard<std::mutex> network_lock(dependencies.network_state_mutex);
    for (std::size_t index = 0U; index < before.size(); ++index) {
      initial_node_ids.push_back(before[index].config.id);
      final_configs.push_back(before[index].config);
      if (before[index].config.id == node_id) {
        target_index = index;
      }
    }
  }
  if (target_index == before.size()) {
    throw std::runtime_error(
        "node-replace references an unknown active node id");
  }
  if (operation_control != nullptr &&
      !operation_control->RecordInitialInventory(before.generation(),
                                                 initial_node_ids)) {
    throw std::logic_error(
        "node-replace command has a conflicting initial inventory");
  }

  NodeRuntime& target = before[target_index];
  NodeProcessGeneration initial_process;
  {
    auto process_guard = run_process_state.Lock();
    initial_process =
        RunningNodeProcessGeneration(target, process_guard, "node replacement");
  }
  {
    std::vector<std::shared_ptr<LiveWalletWorkloadRecord>> records;
    {
      std::lock_guard<std::mutex> lock(wallet_workloads->mutex);
      records.reserve(wallet_workloads->records.size());
      for (const auto& [id, record] : wallet_workloads->records) {
        static_cast<void>(id);
        records.push_back(record);
      }
    }
    for (const std::shared_ptr<LiveWalletWorkloadRecord>& record : records) {
      std::lock_guard<std::mutex> lock(record->mutex);
      if (!IsTerminalLiveWalletWorkloadState(record->state)) {
        throw std::runtime_error(
            "node-replace is unavailable while wallet workload " + record->id +
            " is " + std::string(LiveWalletWorkloadStateName(record->state)));
      }
    }
  }
  RequireNoActiveBlockGenerationWorkloads(block_generation_workloads,
                                          "node-replace");
  RequireNoActiveWaitUntilHeightWorkloads(
      wait_until_height_workloads, "node-replace", {std::string(node_id)});
  RequireNoActiveWaitForPeersWorkloads(wait_for_peers_workloads,
                                       "node-replace");
  if (transaction_tracker.HasPending()) {
    throw std::runtime_error(
        "node-replace is unavailable while transaction observations are "
        "pending");
  }
  if (request.network && !options.isolate_network) {
    throw std::runtime_error(
        "node-replace network condition requires isolated networking");
  }
  if (!target.cgroup) {
    throw std::runtime_error("node-replace target has no owned cgroup");
  }
  if (options.isolate_network &&
      (!target.network || !target.network_namespace)) {
    throw std::runtime_error(
        "node-replace isolated target has incomplete network resources");
  }

  const ChainNodeConfig prior_config = final_configs[target_index];
  ChainNodeConfig final_config = prior_config;
  if (request.binary) {
    final_config.binary = *request.binary;
  }
  RequireExecutable(final_config.binary);
  final_configs[target_index] = final_config;

  const NodeRoleTopology& roles = before_registry.registry().topology();
  const bool masternode_role =
      std::find(roles.masternode_nodes.begin(), roles.masternode_nodes.end(),
                static_cast<std::uint32_t>(target_index)) !=
      roles.masternode_nodes.end();
  const MasternodeIdentity* masternode = nullptr;
  if (masternode_role) {
    const auto found = std::find_if(before_registry.masternodes().begin(),
                                    before_registry.masternodes().end(),
                                    [&](const MasternodeIdentity& identity) {
                                      return identity.node_id == node_id;
                                    });
    if (found == before_registry.masternodes().end()) {
      throw std::logic_error(
          "node-replace masternode role has no registered identity");
    }
    masternode = &*found;
  }

  const RuntimeNodeResourceManifest prior_manifest =
      dependencies.resource_manifest(options, before);
  const std::optional<RuntimeNodeResourceManifest> stored_manifest =
      TryLoadRuntimeNodeResourceManifest(RequireRunOwnership(options));
  if (!stored_manifest || *stored_manifest != prior_manifest) {
    throw std::runtime_error(
        "node-replace runtime resource manifest does not match the live "
        "inventory");
  }

  ResourceLimits prior_resources;
  std::string prior_resource_profile;
  {
    std::lock_guard<std::mutex> resource_lock(
        dependencies.resource_state_mutex);
    prior_resources = target.resources;
    prior_resource_profile = target.resource_profile;
  }
  const ResourceLimits final_resources =
      request.resources
          ? ApplyResourceLimitPatch(prior_resources, *request.resources,
                                    prior_config.id)
          : prior_resources;
  std::optional<NodeVethConfig> prior_network;
  std::string prior_network_profile;
  {
    std::lock_guard<std::mutex> network_lock(dependencies.network_state_mutex);
    prior_network = target.network;
    prior_network_profile = target.network_profile;
  }

  const std::uint32_t target_slot = before.slot(target_index);
  std::string staging_root_name = "bbpr-" + std::to_string(target_slot) + "-" +
                                  std::to_string(before.generation());
  for (std::uint32_t attempt = 0U;; ++attempt) {
    const std::string candidate =
        attempt == 0U ? staging_root_name
                      : staging_root_name + "-" + std::to_string(attempt);
    if (candidate.size() > 32U || attempt >= 16U) {
      throw std::runtime_error(
          "node-replace could not allocate a collision-free staging root");
    }
    const bool active_id =
        std::find(initial_node_ids.begin(), initial_node_ids.end(),
                  candidate) != initial_node_ids.end();
    if (!active_id &&
        !RuntimeNodeRootEntryExists(RequireRunOwnership(options), candidate)) {
      staging_root_name = candidate;
      break;
    }
  }
  RuntimeNodeResourceEntry live_entry = prior_manifest.nodes[target_index];
  RuntimeNodeResourceEntry staging_entry{
      .node_id = prior_config.id,
      .slot = target_slot,
      .chain = options.chain,
      .data_dir = std::filesystem::path("nodes") / staging_root_name / "data",
      .root_name = staging_root_name,
      .state = RuntimeNodeResourceState::kPendingReplace,
  };
  RuntimeNodeResourceManifest pending_manifest = prior_manifest;
  pending_manifest.nodes.push_back(staging_entry);

  ChainNodeConfig staging_config = final_config;
  const std::filesystem::path staging_root =
      run_root / "nodes" / staging_root_name;
  staging_config.data_dir = staging_root / "data";
  staging_config.log_dir = staging_root;
  if (staging_config.rpc_authentication == RpcAuthenticationMode::kCookieFile) {
    staging_config.rpc_cookie_file = staging_root / ".bbp-rpc-cookie";
  }
  staging_config.masternode.reset();

  std::unique_ptr<boost::asio::ip::tcp::acceptor> staging_rpc_reservation;
  std::unique_ptr<boost::asio::ip::tcp::acceptor> staging_p2p_reservation;
  boost::asio::io_context staging_port_context;
  if (options.isolate_network) {
    const ChainDriverSpec& chain_spec = ChainDriverSpecFor(options.chain);
    const std::uint32_t offset = chain_spec.max_nodes + target_slot;
    const auto temporary_port = [&](std::uint16_t base,
                                    std::string_view description) {
      const std::uint32_t value = static_cast<std::uint32_t>(base) + offset;
      if (value > std::numeric_limits<std::uint16_t>::max()) {
        throw std::runtime_error("node-replace temporary " +
                                 std::string(description) +
                                 " port exceeds uint16");
      }
      return static_cast<std::uint16_t>(value);
    };
    staging_config.rpc_port = temporary_port(chain_spec.rpc_port_base, "RPC");
    staging_config.p2p_port = temporary_port(chain_spec.p2p_port_base, "P2P");
    if (staging_config.rpc_port == prior_config.rpc_port ||
        staging_config.rpc_port == prior_config.p2p_port ||
        staging_config.p2p_port == prior_config.rpc_port ||
        staging_config.p2p_port == prior_config.p2p_port ||
        staging_config.rpc_port == staging_config.p2p_port) {
      throw std::runtime_error(
          "node-replace temporary isolated ports collide with the live node");
    }
  } else {
    staging_rpc_reservation = ReserveTcpEndpoint(
        staging_port_context,
        staging_config.rpc_bind.empty() ? staging_config.rpc_host
                                        : staging_config.rpc_bind,
        0U, "tcp_port", staging_config.id, "replacement RPC");
    staging_config.rpc_port = staging_rpc_reservation->local_endpoint().port();
    staging_p2p_reservation = ReserveTcpEndpoint(
        staging_port_context,
        EffectiveP2pBindAddress(options.chain, staging_config), 0U, "tcp_port",
        staging_config.id, "replacement P2P");
    staging_config.p2p_port = staging_p2p_reservation->local_endpoint().port();
  }

  std::vector<bool> target_component(before.size(), false);
  target_component[target_index] = true;
  bool component_changed = true;
  while (component_changed) {
    component_changed = false;
    for (const RuntimePeerTopologyEdge& edge : runtime_topology.edges()) {
      if (!edge.active || edge.from >= before.size() ||
          edge.to >= before.size()) {
        continue;
      }
      if (target_component[edge.from] && !target_component[edge.to]) {
        target_component[edge.to] = true;
        component_changed = true;
      }
      if (target_component[edge.to] && !target_component[edge.from]) {
        target_component[edge.from] = true;
        component_changed = true;
      }
    }
  }

  const auto synchronize_candidate =
      [&](const ChainNodeConfig& candidate_config, bool include_old_target,
          std::optional<ChainMetrics> minimum_tip,
          std::stop_token synchronization_stop_token) {
        for (;;) {
          ThrowIfStopRequested(synchronization_stop_token);
          if (!minimum_tip && !NodeProcessRunning(target)) {
            throw std::runtime_error(
                "node-replace old target stopped during staged "
                "synchronization");
          }
          const ChainMetrics candidate_metrics =
              driver.ReadMetrics(candidate_config, synchronization_stop_token);
          std::vector<ChainMetrics> reference_metrics;
          reference_metrics.reserve(before.size());
          for (std::size_t index = 0U; index < before.size(); ++index) {
            if (!target_component[index] ||
                (index == target_index && !include_old_target) ||
                !before[index].AllowsChainMetrics() ||
                !NodeProcessRunning(before[index])) {
              continue;
            }
            reference_metrics.push_back(driver.ReadMetrics(
                before[index].config, synchronization_stop_token));
          }
          const bool minimum_reached =
              !minimum_tip || candidate_metrics.height > minimum_tip->height ||
              (candidate_metrics.height == minimum_tip->height &&
               candidate_metrics.best_hash == minimum_tip->best_hash);
          const bool references_match = std::all_of(
              reference_metrics.begin(), reference_metrics.end(),
              [&](const ChainMetrics& metric) {
                return metric.sync_status == ChainSyncStatus::kSynced &&
                       metric.height == candidate_metrics.height &&
                       metric.best_hash == candidate_metrics.best_hash;
              });
          if (candidate_metrics.sync_status == ChainSyncStatus::kSynced &&
              !candidate_metrics.best_hash.empty() && minimum_reached &&
              references_match) {
            return candidate_metrics;
          }
          WaitForDuration(std::chrono::milliseconds(100),
                          synchronization_stop_token);
        }
      };
  const auto synchronize_until_deadline =
      [&](const ChainNodeConfig& config, bool include_old_target,
          std::optional<ChainMetrics> minimum_tip,
          std::stop_token operation_stop_token) {
        const auto deadline = std::chrono::steady_clock::now() +
                              std::chrono::seconds(request.sync_timeout_sec);
        std::stop_source synchronization_stop_source;
        std::stop_callback stop_on_request(
            operation_stop_token, [&synchronization_stop_source] {
              synchronization_stop_source.request_stop();
            });
        std::jthread timer([deadline, &synchronization_stop_source](
                               std::stop_token timer_stop_token) {
          try {
            WaitUntil(deadline, timer_stop_token);
          } catch (const SimulationCancelled&) {
            return;
          }
          synchronization_stop_source.request_stop();
        });
        try {
          return synchronize_candidate(config, include_old_target, minimum_tip,
                                       synchronization_stop_source.get_token());
        } catch (const SimulationCancelled&) {
          if (operation_stop_token.stop_requested()) {
            throw;
          }
          throw std::runtime_error(
              "node-replace chain synchronization deadline expired");
        }
      };

  const std::filesystem::path staged_events_path =
      run_root / ".runtime-node-replace-events.pending";
  bool staged_events_created = false;
  bool staging_root_exists = false;
  bool target_frozen = false;
  RuntimeNodeRootOrientation root_orientation =
      RuntimeNodeRootOrientation::kOriginal;
  bool resource_restore_required = false;
  bool network_restore_required = false;
  bool published = false;
  auto candidate = std::make_shared<NodeRuntime>();
  candidate->config = staging_config;
  candidate->run_process_state = &run_process_state;
  std::shared_ptr<Cgroup> staging_cgroup;
  std::vector<RuntimeNodeReplacementPeerEdge> prior_connected_peer_edges;
  const auto peer_endpoint = [](const ChainNodeConfig& config) {
    return config.p2p_host + ":" + std::to_string(config.p2p_port);
  };
  const auto restore_connected_peer_edges =
      [&](bool use_final_target,
          std::chrono::steady_clock::time_point readiness_deadline,
          std::stop_token restoration_stop_token) {
        for (const RuntimeNodeReplacementPeerEdge& edge :
             prior_connected_peer_edges) {
          ThrowIfStopRequested(restoration_stop_token);
          const auto remaining =
              readiness_deadline - std::chrono::steady_clock::now();
          if (remaining <= std::chrono::steady_clock::duration::zero()) {
            throw std::runtime_error(
                "node-replace peer and role readiness deadline expired");
          }
          const ChainNodeConfig& source =
              edge.from == target_index
                  ? (use_final_target ? final_config : prior_config)
                  : before[edge.from].config;
          const ChainNodeConfig& destination =
              edge.to == target_index
                  ? (use_final_target ? final_config : prior_config)
                  : before[edge.to].config;
          const std::string endpoint = peer_endpoint(destination);
          driver.ConnectPeer(source, endpoint, restoration_stop_token);
          driver.WaitForPeerAddress(
              source, endpoint,
              std::chrono::ceil<std::chrono::seconds>(remaining),
              restoration_stop_token);
        }
      };
  const auto remove_staged_events = [&] {
    if (!staged_events_created) {
      return;
    }
    std::error_code error;
    const bool removed = std::filesystem::remove(staged_events_path, error);
    if (error || !removed || std::filesystem::exists(staged_events_path)) {
      throw std::runtime_error(
          "node-replace staged event cleanup failed: " +
          (error ? error.message() : std::string("file survived removal")));
    }
    staged_events_created = false;
  };

  const auto rollback = [&] {
    const auto rollback_deadline = std::chrono::steady_clock::now() +
                                   kSimulationNodeReplaceRollbackTimeout;
    std::stop_source rollback_stop_source;
    std::jthread rollback_timer([rollback_deadline, &rollback_stop_source](
                                    std::stop_token timer_stop_token) {
      try {
        WaitUntil(rollback_deadline, timer_stop_token);
      } catch (const SimulationCancelled&) {
        return;
      }
      rollback_stop_source.request_stop();
    });
    const std::stop_token rollback_stop_token =
        rollback_stop_source.get_token();
    std::vector<std::string> errors;
    const auto rollback_step = [&](std::string_view description,
                                   const auto& action) {
      try {
        if (rollback_stop_token.stop_requested() ||
            std::chrono::steady_clock::now() >= rollback_deadline) {
          throw std::runtime_error("node-replace rollback deadline expired");
        }
        action();
      } catch (...) {
        errors.push_back(
            std::string(description) + ": " +
            RuntimeNodeReplacementExceptionMessage(std::current_exception()));
      }
    };

    rollback_step("thaw original cgroup", [&] {
      if (target_frozen || target.cgroup->Frozen()) {
        target.cgroup->Thaw();
        if (!dependencies.wait_for_frozen_state(*target.cgroup, false,
                                                rollback_stop_token)) {
          throw std::runtime_error(
              "original cgroup remained frozen during replacement rollback");
        }
        target_frozen = false;
      }
    });
    rollback_step("stop replacement candidate", [&] {
      if (!NodeProcessRunning(*candidate)) {
        return;
      }
      try {
        dependencies.stop_node(options, staged_events_path, driver, *candidate,
                               rollback_stop_token, true);
      } catch (...) {
        if (NodeProcessRunning(*candidate)) {
          static_cast<void>(RequestNodeTerminate(*candidate));
          if (!WaitForNodeProcessExitUntil(
                  *candidate,
                  std::min(rollback_deadline, std::chrono::steady_clock::now() +
                                                  std::chrono::seconds(3)),
                  rollback_stop_token)) {
            static_cast<void>(RequestNodeKill(*candidate));
          }
          if (!WaitForNodeProcessExitUntil(*candidate, rollback_deadline,
                                           rollback_stop_token)) {
            throw;
          }
        }
      }
      if (NodeProcessRunning(*candidate)) {
        throw std::runtime_error(
            "replacement candidate survived rollback termination");
      }
    });
    if (root_orientation == RuntimeNodeRootOrientation::kExchanged) {
      rollback_step("restore original and staging roots", [&] {
        ExchangeRuntimeNodeRootsForReplacement(RequireRunOwnership(options),
                                               live_entry, staging_entry,
                                               &root_orientation);
        if (root_orientation != RuntimeNodeRootOrientation::kOriginal) {
          throw std::runtime_error(
              "runtime node replacement roots remained exchanged");
        }
        staging_entry.state = RuntimeNodeResourceState::kPendingReplace;
      });
    }
    const bool root_restoration_verified =
        root_orientation == RuntimeNodeRootOrientation::kOriginal;
    if (!root_restoration_verified &&
        root_orientation == RuntimeNodeRootOrientation::kUnknown) {
      errors.emplace_back(
          "restore original and staging roots: root orientation is unknown");
    }
    if (network_restore_required) {
      rollback_step("restore network condition", [&] {
        std::lock_guard<std::mutex> network_lock(
            dependencies.network_state_mutex);
        if (!prior_network) {
          throw std::logic_error(
              "node-replace lost its prior isolated network state");
        }
        RestoreNodeNetworkCondition(*prior_network);
        target.network = *prior_network;
        target.network_profile = prior_network_profile;
        network_restore_required = false;
      });
    }
    if (resource_restore_required) {
      rollback_step("restore resource limits", [&] {
        std::lock_guard<std::mutex> resource_lock(
            dependencies.resource_state_mutex);
        WriteResourceLimits(*target.cgroup, final_resources, prior_resources);
        target.resources = prior_resources;
        target.resource_profile = prior_resource_profile;
        resource_restore_required = false;
      });
    }

    bool original_process_unchanged = false;
    {
      auto process_guard = run_process_state.Lock();
      original_process_unchanged =
          root_restoration_verified && target.process.running() &&
          target.process.pid() == initial_process.pid &&
          target.RestartCount() == initial_process.restart_count;
      if (original_process_unchanged) {
        AttachNodePerfCounters(target, process_guard);
        target.SetLifecycle(NodeRuntimeLifecycle::kRunning);
      }
    }
    bool prior_process_restored = original_process_unchanged;
    if (!original_process_unchanged && root_restoration_verified) {
      rollback_step("relaunch prior process", [&] {
        if (NodeProcessRunning(target)) {
          throw std::runtime_error(
              "original runtime has an unexpected running process");
        }
        {
          auto process_guard = run_process_state.Lock();
          ResetNodePerfCounters(target, process_guard);
          target.SetLifecycle(NodeRuntimeLifecycle::kRestarting);
        }
        Options rollback_options = options;
        rollback_options.ready_timeout_sec = request.ready_timeout_sec;
        if (!dependencies.start_node(
                rollback_options, staged_events_path, driver, target,
                "runtime_node_replace_rollback", lifecycle_epoch, true, true,
                rollback_stop_token)) {
          throw std::runtime_error(
              "prior node reached stop_time during replacement rollback");
        }
        prior_process_restored = true;
      });
    }
    if (!prior_process_restored) {
      errors.emplace_back(
          "verify restored chain state: prior process was not restored");
    } else {
      rollback_step("verify restored chain state", [&] {
        if (!original_process_unchanged) {
          restore_connected_peer_edges(false, rollback_deadline,
                                       rollback_stop_token);
        }
        static_cast<void>(synchronize_until_deadline(
            prior_config, false, std::nullopt, rollback_stop_token));
      });
      if (restore_native_mining && !original_process_unchanged) {
        rollback_step("restore native mining", [&] {
          driver.StartMining(prior_config,
                             std::string(native_mining_reward_address),
                             rollback_stop_token);
        });
      }
    }
    if (staging_cgroup) {
      rollback_step("remove staging cgroup", [&] {
        staging_cgroup->KillAll(rollback_deadline, rollback_stop_token);
        staging_cgroup->Remove(rollback_deadline, rollback_stop_token);
        staging_cgroup.reset();
      });
    }
    if (staging_root_exists && root_restoration_verified &&
        prior_process_restored) {
      rollback_step("remove staging node root", [&] {
        RemoveRuntimeNodeRoot(RequireRunOwnership(options), staging_entry,
                              rollback_deadline, rollback_stop_token);
        if (RuntimeNodeRootEntryExists(RequireRunOwnership(options),
                                       staging_entry)) {
          throw std::runtime_error(
              "staging node root survived replacement rollback");
        }
        staging_root_exists = false;
      });
    }
    if (root_restoration_verified && prior_process_restored &&
        !staging_root_exists) {
      rollback_step("restore resource manifest", [&] {
        WriteRuntimeNodeResourceManifest(prior_manifest);
        const RuntimeNodeResourceManifest live =
            dependencies.resource_manifest(options, inventory.Snapshot());
        const std::optional<RuntimeNodeResourceManifest> persisted =
            TryLoadRuntimeNodeResourceManifest(RequireRunOwnership(options));
        if (live != prior_manifest || !persisted ||
            *persisted != prior_manifest) {
          throw std::runtime_error(
              "node-replace rollback resource manifest read-back failed");
        }
      });
    } else {
      rollback_step("retain unresolved replacement manifest", [&] {
        staging_entry.state =
            root_orientation == RuntimeNodeRootOrientation::kUnknown
                ? RuntimeNodeResourceState::kPendingReplaceUncertain
            : root_orientation == RuntimeNodeRootOrientation::kExchanged
                ? RuntimeNodeResourceState::kPendingReplaceExchanged
                : RuntimeNodeResourceState::kPendingReplace;
        pending_manifest.nodes.back() = staging_entry;
        WriteRuntimeNodeResourceManifest(pending_manifest);
        const std::optional<RuntimeNodeResourceManifest> persisted =
            TryLoadRuntimeNodeResourceManifest(RequireRunOwnership(options));
        if (!persisted || *persisted != pending_manifest) {
          throw std::runtime_error(
              "unresolved node-replace manifest read-back failed");
        }
      });
    }
    rollback_step("remove staged node-replace events", remove_staged_events);
    if (!errors.empty()) {
      std::string detail;
      for (const std::string& error : errors) {
        if (!detail.empty()) {
          detail += "; ";
        }
        detail += error;
      }
      throw std::runtime_error(std::move(detail));
    }
  };

  RuntimeNodeReplaceResult result;
  try {
    if (std::filesystem::exists(staged_events_path)) {
      throw std::runtime_error("node-replace staged event path already exists");
    }
    WriteText(staged_events_path, "");
    staged_events_created = true;
    WriteRuntimeNodeResourceManifest(pending_manifest);
    target_frozen = true;
    target.cgroup->Freeze();
    if (!dependencies.wait_for_frozen_state(*target.cgroup, true, stop_token)) {
      throw std::runtime_error(
          "node-replace original cgroup did not freeze for root cloning");
    }
    CloneRuntimeNodeRootForReplacement(
        RequireRunOwnership(options), live_entry, staging_entry,
        operation_control ? operation_control->absolute_deadline : std::nullopt,
        stop_token, &staging_root_exists);
    target.cgroup->Thaw();
    if (!dependencies.wait_for_frozen_state(*target.cgroup, false,
                                            stop_token)) {
      throw std::runtime_error(
          "node-replace original cgroup remained frozen after root cloning");
    }
    target_frozen = false;

    candidate->network_namespace = target.network_namespace;
    candidate->network = prior_network;
    candidate->resources = final_resources;
    candidate->perf_counter_target_kind = PerfCounterTargetKind::kNode;
    candidate->perf_counter_target_id = prior_config.id;
    candidate->lifecycle_policy = target.lifecycle_policy;
    candidate->lifecycle_policy.stop_time.reset();
    staging_cgroup = Cgroup::CreateShared(
        RequireRunOwnership(options).cgroup_name, staging_root_name);
    candidate->cgroup = staging_cgroup;
    staging_cgroup->SetMemoryHigh(final_resources.memory_high_bytes);
    staging_cgroup->SetMemoryMax(final_resources.memory_max_bytes);
    staging_cgroup->SetCpuMax(final_resources.cpu_quota_us,
                              final_resources.cpu_period_us);
    staging_cgroup->SetCpuWeight(final_resources.cpu_weight);
    staging_cgroup->SetIoWeight(final_resources.io_weight);
    for (const IoLimit& limit : final_resources.io_limits) {
      staging_cgroup->SetIoMax(limit);
    }
    staging_cgroup->SetPidsMax(final_resources.pids_max);
    VerifyResourceLimits(*staging_cgroup, final_resources);
    candidate->SetLifecycle(NodeRuntimeLifecycle::kCgroupReady);
    if (operation_control != nullptr) {
      operation_control->ReportProgress(1U);
    }

    Options replacement_options = options;
    replacement_options.ready_timeout_sec = request.ready_timeout_sec;
    replacement_options.sync_timeout_sec = request.sync_timeout_sec;
    if (staging_rpc_reservation) {
      boost::system::error_code error;
      staging_rpc_reservation->close(error);
      if (error) {
        throw std::runtime_error(
            "node-replace could not release its staging RPC port: " +
            error.message());
      }
      staging_p2p_reservation->close(error);
      if (error) {
        throw std::runtime_error(
            "node-replace could not release its staging P2P port: " +
            error.message());
      }
    }
    if (!dependencies.start_node(replacement_options, staged_events_path,
                                 driver, *candidate,
                                 "runtime_node_replace_candidate",
                                 lifecycle_epoch, false, true, stop_token)) {
      throw std::runtime_error(
          "replacement candidate reached stop_time before readiness");
    }
    if (operation_control != nullptr) {
      operation_control->ReportProgress(2U);
    }

    const ChainMetrics staged_tip = synchronize_until_deadline(
        staging_config, true, std::nullopt, stop_token);
    if (operation_control != nullptr) {
      operation_control->ReportProgress(3U);
    }

    PeerConnectivityController::AllowedPeerMap allowed_peers;
    for (const ChainNodeConfig& config : final_configs) {
      allowed_peers.emplace(config.id,
                            peer_controller.AllowedPeersFor(config.id));
    }
    const std::vector<RuntimeNodeReplacementPeerEdge> peer_edges =
        ResolveRuntimeNodeReplacementPeerEdges(
            runtime_topology, initial_node_ids, allowed_peers,
            static_cast<std::uint32_t>(target_index));
    for (const RuntimeNodeReplacementPeerEdge& edge : peer_edges) {
      const ChainNodeConfig& source = before[edge.from].config;
      const std::string endpoint = peer_endpoint(before[edge.to].config);
      if (!driver.ConnectedPeerAddresses(source, {endpoint}, stop_token)
               .empty()) {
        prior_connected_peer_edges.push_back(edge);
      }
    }
    std::optional<PeerConnectivityController::RpcMutationLease> peer_rpc_lease;
    peer_rpc_lease.emplace(peer_controller.AcquireRpcMutationLease(stop_token));
    std::optional<PeerConnectivityController::PreparedFinalRegistration>
        prepared_peer_registration;
    prepared_peer_registration.emplace(peer_controller.PrepareFinalRegistration(
        final_configs, {}, allowed_peers, {}, *peer_rpc_lease));

    dependencies.stop_node(replacement_options, staged_events_path, driver,
                           *candidate, stop_token, true);
    staging_cgroup->KillAll(
        std::chrono::steady_clock::now() + std::chrono::seconds(10),
        stop_token);
    staging_cgroup->Remove(
        std::chrono::steady_clock::now() + std::chrono::seconds(10),
        stop_token);
    staging_cgroup.reset();
    candidate->cgroup.reset();

    dependencies.stop_node(replacement_options, staged_events_path, driver,
                           target, stop_token, false);
    RemoveRuntimeNodeRoot(
        RequireRunOwnership(options), staging_entry,
        operation_control ? operation_control->absolute_deadline : std::nullopt,
        stop_token);
    staging_root_exists = false;
    CloneRuntimeNodeRootForReplacement(
        RequireRunOwnership(options), live_entry, staging_entry,
        operation_control ? operation_control->absolute_deadline : std::nullopt,
        stop_token, &staging_root_exists);
    if (root_orientation != RuntimeNodeRootOrientation::kOriginal) {
      throw std::runtime_error(
          "node-replace compatible live root changed orientation");
    }

    candidate->config = final_config;
    candidate->process_start_connect_peers = target.process_start_connect_peers;
    candidate->uses_physical_start_connect_peers =
        target.uses_physical_start_connect_peers;
    candidate->cgroup = target.cgroup;
    candidate->network_namespace = target.network_namespace;
    candidate->network = prior_network;
    candidate->directional_network_policies =
        target.directional_network_policies;
    candidate->run_process_state = &run_process_state;
    candidate->lifecycle_policy = target.lifecycle_policy;
    candidate->resources = final_resources;
    candidate->resource_profile =
        request.resources ? std::string{} : prior_resource_profile;
    candidate->network_profile =
        request.network ? std::string{} : prior_network_profile;
    candidate->perf_counter_kinds = target.perf_counter_kinds;
    candidate->perf_counter_target_kind = target.perf_counter_target_kind;
    candidate->perf_counter_target_id = target.perf_counter_target_id;
    candidate->stdout_log_cursor = target.stdout_log_cursor;
    candidate->stderr_log_cursor = target.stderr_log_cursor;
    candidate->daemon_log_cursor = target.daemon_log_cursor;
    candidate->CopyAccountingForReplacementFrom(target);

    if (request.resources) {
      std::lock_guard<std::mutex> resource_lock(
          dependencies.resource_state_mutex);
      resource_restore_required = true;
      WriteResourceLimits(*target.cgroup, prior_resources, final_resources);
    }
    if (request.network) {
      std::lock_guard<std::mutex> network_lock(
          dependencies.network_state_mutex);
      network_restore_required = true;
      const QdiscInfo qdisc = ReplaceNodeNetworkConditionTransactional(
          candidate.get(), *request.network, stop_token);
      WriteEvent(
          staged_events_path, options.run_id, candidate->config.id,
          SimulationEventKind::kNetworkConditionUpdated,
          NetworkConditionVerificationDetail(*candidate->network, qdisc));
    }
    candidate->SetLifecycle(NodeRuntimeLifecycle::kRestarting);
    if (!dependencies.start_node(replacement_options, staged_events_path,
                                 driver, *candidate, "runtime_node_replace",
                                 lifecycle_epoch, true, true, stop_token)) {
      throw std::runtime_error(
          "replacement target reached stop_time before readiness");
    }
    const auto final_role_readiness_started = std::chrono::steady_clock::now();
    const auto final_role_readiness_budget =
        std::chrono::seconds(request.ready_timeout_sec);
    const auto final_role_readiness_deadline =
        final_role_readiness_started + final_role_readiness_budget;
    restore_connected_peer_edges(true, final_role_readiness_deadline,
                                 stop_token);
    const auto peer_readiness_elapsed =
        std::chrono::steady_clock::now() - final_role_readiness_started;
    const auto remaining_role_readiness =
        peer_readiness_elapsed < final_role_readiness_budget
            ? final_role_readiness_budget - peer_readiness_elapsed
            : std::chrono::steady_clock::duration::zero();
    const ChainMetrics synchronized_tip =
        synchronize_until_deadline(final_config, false, staged_tip, stop_token);
    if (masternode != nullptr) {
      ThrowIfStopRequested(stop_token);
      if (remaining_role_readiness <=
          std::chrono::steady_clock::duration::zero()) {
        throw std::runtime_error(
            "node-replace peer and role readiness deadline expired");
      }
      const ChainMasternodeStatus status = driver.WaitForMasternodeReady(
          final_config, masternode->pro_tx_hash,
          std::chrono::ceil<std::chrono::seconds>(remaining_role_readiness),
          stop_token);
      if (!status.ready() || status.pro_tx_hash != masternode->pro_tx_hash ||
          status.service != masternode->service) {
        throw std::runtime_error(
            "node-replace masternode readiness identity changed");
      }
    }
    if (restore_native_mining) {
      driver.StartMining(final_config,
                         std::string(native_mining_reward_address), stop_token);
    }
    WriteEvent(
        staged_events_path, options.run_id, candidate->config.id,
        SimulationEventKind::kHeightReached,
        boost::json::serialize(boost::json::object{
            {"height", synchronized_tip.height},
            {"best_hash", synchronized_tip.best_hash},
            {"sync_status",
             std::string(ChainSyncStatusName(synchronized_tip.sync_status))},
            {"reason", "runtime_node_replace"}}));

    std::unique_lock<std::timed_mutex> publication_lock =
        dependencies.acquire_publication_lock(stop_token);
    RuntimeNodeInventory::PreparedReplacement prepared_inventory =
        inventory.PrepareReplacement(
            before.generation(), node_id,
            RuntimeNodeInsertion{.slot = target_slot, .runtime = candidate},
            final_configs);
    std::unique_lock<std::mutex> resource_lock(
        dependencies.resource_state_mutex);
    std::unique_lock<std::mutex> network_lock(dependencies.network_state_mutex);
    if (request.resources) {
      WriteEvent(staged_events_path, options.run_id, candidate->config.id,
                 SimulationEventKind::kResourceLimitsUpdated,
                 ResourceLimitUpdateDetail(*request.resources, prior_resources,
                                           final_resources));
    }

    boost::json::array published_node_ids;
    boost::json::array published_node_configs;
    published_node_ids.reserve(final_configs.size());
    published_node_configs.reserve(final_configs.size());
    for (std::uint32_t index = 0U; index < final_configs.size(); ++index) {
      published_node_ids.emplace_back(final_configs[index].id);
      boost::json::object published_config = RuntimePublishedNodeConfig(
          options, index == target_index ? *candidate : before[index],
          final_configs[index], index, &roles);
      if (index == target_index) {
        published_config["lifecycle"] = "Running";
      }
      published_node_configs.push_back(std::move(published_config));
    }
    boost::json::object published_topology;
    AddPeerTopologyJson(live_topology_config,
                        static_cast<std::uint32_t>(final_configs.size()),
                        &published_topology);
    boost::json::object generation_detail{
        {"generation", before.generation() + 1U},
        {"node_count", final_configs.size()},
        {"node_ids", std::move(published_node_ids)},
        {"node_configs", std::move(published_node_configs)},
        {"topology", std::move(published_topology)},
        {"topology_current_edges",
         RuntimePeerTopologyEdgesJson(runtime_topology)},
        {"replaced_node_id", node_id},
        {"manifest_state", "live"}};
    WriteNodeStateEvent(staged_events_path, options.run_id, *candidate,
                        NodeRuntimeLifecycle::kRunning);
    constexpr std::size_t kMaximumStagedNodeReplaceEventBytes =
        4U * 1024U * 1024U;
    const std::string staged_events =
        ReadText(staged_events_path, kMaximumStagedNodeReplaceEventBytes, {});
    if (!staged_events.empty() && staged_events.back() != '\n') {
      throw std::runtime_error(
          "node-replace staged event stream ended with an incomplete record");
    }
    std::vector<std::string> staged_event_records;
    std::size_t staged_offset = 0U;
    while (staged_offset < staged_events.size()) {
      const std::size_t end = staged_events.find('\n', staged_offset);
      std::string record(staged_events.data() + staged_offset,
                         end - staged_offset);
      if (record.empty() || !boost::json::parse(record).is_object()) {
        throw std::runtime_error(
            "node-replace staged event stream contains an invalid record");
      }
      staged_event_records.push_back(std::move(record));
      staged_offset = end + 1U;
    }
    if (operation_control != nullptr) {
      operation_control->ReportProgress(4U);
      if (!operation_control->TryBeginCommit()) {
        throw SimulationCancelled();
      }
    } else {
      ThrowIfStopRequested(stop_token);
    }

    staging_entry.state = RuntimeNodeResourceState::kPendingReplaceCommitted;
    pending_manifest.nodes.back() = staging_entry;
    WriteRuntimeNodeResourceManifest(pending_manifest);
    prepared_peer_registration->Commit();
    const RuntimeNodeSnapshot published_nodes = prepared_inventory.Commit();
    prepared_peer_registration.reset();
    peer_rpc_lease.reset();
    published = true;
    result.inventory_generation = published_nodes.generation();
    result.final_node_count =
        static_cast<std::uint32_t>(published_nodes.size());
    network_lock.unlock();
    resource_lock.unlock();
    publication_lock.unlock();

    RemoveRuntimeNodeRoot(
        RequireRunOwnership(options), staging_entry,
        std::chrono::steady_clock::now() + std::chrono::seconds(60), {});
    staging_root_exists = false;
    WriteRuntimeNodeResourceManifest(prior_manifest);
    const std::optional<RuntimeNodeResourceManifest> persisted_manifest =
        TryLoadRuntimeNodeResourceManifest(RequireRunOwnership(options));
    if (!persisted_manifest || *persisted_manifest != prior_manifest) {
      throw SimulationCommandOutcomeUnconfirmed(
          "node-replace committed resource manifest read-back failed");
    }
    generation_detail["topology_restore_request_sequence"] =
        peer_controller.RequestTopologyRestore(candidate->config.id);
    WriteEvent(events_path, options.run_id, "sim",
               SimulationEventKind::kRuntimeGenerationPublished,
               boost::json::serialize(generation_detail));
    for (const std::string& record : staged_event_records) {
      AppendLine(events_path, record);
    }
    remove_staged_events();
    if (operation_control != nullptr) {
      operation_control->MarkCommitted();
      operation_control->ReportProgress(kSimulationNodeAddProgressTotal);
    }
  } catch (...) {
    const std::exception_ptr failure = std::current_exception();
    if (published) {
      if (operation_control != nullptr) {
        operation_control->outcome_unconfirmed.store(true,
                                                     std::memory_order_release);
      }
      try {
        remove_staged_events();
      } catch (...) {
        throw SimulationCommandOutcomeUnconfirmed(
            RuntimeNodeReplacementExceptionMessage(failure) +
            "; staged event cleanup failed: " +
            RuntimeNodeReplacementExceptionMessage(std::current_exception()));
      }
      throw SimulationCommandOutcomeUnconfirmed(
          RuntimeNodeReplacementExceptionMessage(failure));
    }
    try {
      rollback();
    } catch (...) {
      throw SimulationCommandOutcomeUnconfirmed(
          "node-replace failed: " +
          RuntimeNodeReplacementExceptionMessage(failure) +
          "; rollback could not be verified: " +
          RuntimeNodeReplacementExceptionMessage(std::current_exception()));
    }
    std::rethrow_exception(failure);
  }
  return result;
}

}  // namespace bbp::simulator_app_internal
