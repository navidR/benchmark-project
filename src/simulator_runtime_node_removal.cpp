#include "simulator_runtime_node_removal.h"

#include <algorithm>
#include <boost/json/array.hpp>
#include <boost/json/object.hpp>
#include <boost/json/serialize.hpp>
#include <chrono>
#include <cstdint>
#include <exception>
#include <filesystem>
#include <limits>
#include <memory>
#include <mutex>
#include <optional>
#include <set>
#include <stdexcept>
#include <stop_token>
#include <string>
#include <string_view>
#include <thread>
#include <type_traits>
#include <utility>
#include <vector>

#include "bbp/default_peer_topology.h"
#include "bbp/peer_connectivity_controller.h"
#include "bbp/runtime_node_inventory.h"
#include "bbp/runtime_peer_topology.h"
#include "bbp/runtime_wallet_registry.h"
#include "bbp/simulation_cancelled.h"
#include "bbp/simulation_command.h"
#include "bbp/simulation_event_kind.h"
#include "bbp/simulation_node_add.h"
#include "bbp/simulator/options.h"
#include "simulator_cancellable_waiting.h"
#include "simulator_event_writing.h"
#include "simulator_live_workload_state.h"
#include "simulator_network_launch_planning.h"
#include "simulator_runtime_identity_details.h"
#include "simulator_runtime_published_node_config.h"
#include "simulator_scenario_identifier.h"
#include "simulator_scenario_serialization.h"
#include "simulator_transaction_observation_tracking.h"

namespace bbp::simulator_app_internal {

namespace {

std::string RuntimeNodeRemovalExceptionMessage(
    const std::exception_ptr& error) {
  try {
    std::rethrow_exception(error);
  } catch (const std::exception& exception) {
    return exception.what();
  } catch (...) {
    return "unknown exception";
  }
}

std::string RuntimeNodeRemovalPeerEndpoint(const NodeRuntime& node) {
  return node.config.p2p_host + ":" + std::to_string(node.config.p2p_port);
}

}  // namespace

RuntimeNodeRemoveResult RemoveRuntimeNodesTransactional(
    const Options& options, const std::filesystem::path& events_path,
    const ChainDriver& driver, RuntimeNodeInventory& inventory,
    RuntimeWalletRegistry& runtime_registry,
    PeerConnectivityController& peer_controller,
    std::unique_ptr<RuntimePeerTopology>* runtime_topology,
    PeerTopologyConfig* live_topology_config,
    const std::shared_ptr<LiveWalletWorkloadRegistry>& wallet_workloads,
    const std::shared_ptr<LiveBlockGenerationWorkloadRegistry>&
        block_generation_workloads,
    const std::shared_ptr<LiveWaitUntilHeightWorkloadRegistry>&
        wait_until_height_workloads,
    const std::shared_ptr<LiveWaitForPeersWorkloadRegistry>&
        wait_for_peers_workloads,
    const TransactionObservationTracker& transaction_tracker,
    const SimulationNodeRemoveRequest& request,
    SimulationCommandControl* operation_control, std::stop_token stop_token,
    const RuntimeNodeRemovalDependencies& dependencies) {
  if (request.node_ids.empty() ||
      request.node_ids.size() > kSimulationNodeRemoveMaximumCount) {
    throw std::runtime_error("node-remove count is out of range");
  }
  if (runtime_topology == nullptr || !*runtime_topology ||
      live_topology_config == nullptr || !wallet_workloads) {
    throw std::logic_error("node-remove runtime services are unavailable");
  }
  RuntimeNodeSnapshot before = inventory.Snapshot();
  RuntimeWalletSnapshot before_registry = runtime_registry.Snapshot();
  if (request.node_ids.size() > before.size()) {
    throw std::runtime_error("node-remove count exceeds the active node count");
  }
  if (before.generation() == std::numeric_limits<std::uint64_t>::max()) {
    throw std::overflow_error("node-remove inventory generation overflow");
  }
  if (operation_control != nullptr) {
    std::vector<std::string> initial_node_ids;
    initial_node_ids.reserve(before.size());
    for (const NodeRuntime& node : before) {
      initial_node_ids.push_back(node.config.id);
    }
    if (!operation_control->RecordInitialInventory(
            before.generation(), std::move(initial_node_ids))) {
      throw std::logic_error(
          "node-remove command has a conflicting initial inventory");
    }
  }

  std::set<std::string> requested_ids;
  for (const std::string& node_id : request.node_ids) {
    RequireSafeScenarioIdentifier(node_id, "node-remove node id");
    if (!requested_ids.insert(node_id).second) {
      throw std::runtime_error("node-remove node ids must be unique");
    }
  }
  std::vector<std::optional<std::uint32_t>> old_to_new(before.size());
  std::vector<std::uint32_t> retained_old_indexes;
  std::vector<std::uint32_t> removed_old_indexes;
  std::vector<ChainNodeConfig> final_configs;
  std::vector<std::uint32_t> final_resource_slots;
  std::set<std::string> found_ids;
  final_configs.reserve(before.size() - request.node_ids.size());
  final_resource_slots.reserve(before.size() - request.node_ids.size());
  {
    std::lock_guard<std::mutex> network_lock(dependencies.network_state_mutex);
    for (std::uint32_t old_index = 0U; old_index < before.size(); ++old_index) {
      const NodeRuntime& node = before[old_index];
      if (requested_ids.contains(node.config.id)) {
        found_ids.insert(node.config.id);
        removed_old_indexes.push_back(old_index);
        continue;
      }
      const std::uint32_t next_index =
          static_cast<std::uint32_t>(retained_old_indexes.size());
      old_to_new[old_index] = next_index;
      retained_old_indexes.push_back(old_index);
      final_configs.push_back(node.config);
      final_resource_slots.push_back(before.slot(old_index));
    }
  }
  if (found_ids != requested_ids) {
    throw std::runtime_error(
        "node-remove references an unknown active node id");
  }

  const NodeRoleTopology& before_roles = before_registry.registry().topology();
  for (const std::uint32_t removed : removed_old_indexes) {
    if (std::find(before_roles.wallet_nodes.begin(),
                  before_roles.wallet_nodes.end(),
                  removed) != before_roles.wallet_nodes.end()) {
      throw std::runtime_error(
          "node-remove requires wallet.remove before removing wallet node " +
          before[removed].config.id);
    }
    if (std::find(before_roles.miner_nodes.begin(),
                  before_roles.miner_nodes.end(),
                  removed) != before_roles.miner_nodes.end()) {
      throw std::runtime_error(
          "node-remove requires miner.remove before removing miner node " +
          before[removed].config.id);
    }
    if (std::find(before_roles.masternode_nodes.begin(),
                  before_roles.masternode_nodes.end(),
                  removed) != before_roles.masternode_nodes.end()) {
      throw std::runtime_error(
          "node-remove requires masternode.remove before removing masternode "
          "node " +
          before[removed].config.id);
    }
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
            "node-remove is unavailable while wallet workload " + record->id +
            " is " + std::string(LiveWalletWorkloadStateName(record->state)));
      }
    }
  }
  RequireNoActiveBlockGenerationWorkloads(block_generation_workloads,
                                          "node-remove");
  RequireNoActiveWaitUntilHeightWorkloads(wait_until_height_workloads,
                                          "node-remove", requested_ids);
  RequireNoActiveWaitForPeersWorkloads(wait_for_peers_workloads, "node-remove");
  if (transaction_tracker.HasPending()) {
    throw std::runtime_error(
        "node-remove is unavailable while transaction observations are "
        "pending");
  }

  const std::uint32_t final_count =
      static_cast<std::uint32_t>(final_configs.size());
  PeerTopologyConfig next_topology_config =
      RemapPeerTopologyConfig(*live_topology_config, old_to_new);
  auto next_runtime_topology = std::make_unique<RuntimePeerTopology>(
      next_topology_config, final_count, final_count == 0U);
  if (final_count != 0U) {
    next_runtime_topology->PreserveRemappedStateFrom(**runtime_topology,
                                                     old_to_new);
  }
  SimulationRegistry next_registry =
      before_registry.registry().RemapRuntimeNodes(old_to_new,
                                                   next_topology_config);
  for (std::uint32_t index = 0U; index < final_count; ++index) {
    final_configs[index].connect_peers = dependencies.restart_peer_endpoints(
        next_registry.topology(), *next_runtime_topology, final_configs, index);
  }
  if (!options.isolate_network &&
      std::any_of(next_runtime_topology->edges().begin(),
                  next_runtime_topology->edges().end(),
                  [](const RuntimePeerTopologyEdge& edge) {
                    return edge.active && edge.condition.has_value();
                  })) {
    throw std::runtime_error(
        "node-remove conditioned topology requires isolated networking");
  }
  if (options.chain == ChainKind::kMonero) {
    for (std::size_t first = 0U; first < retained_old_indexes.size(); ++first) {
      for (std::size_t second = first + 1U;
           second < retained_old_indexes.size(); ++second) {
        if ((*runtime_topology)
                ->PhysicalPeerRequired(retained_old_indexes[first],
                                       retained_old_indexes[second]) !=
            next_runtime_topology->PhysicalPeerRequired(
                static_cast<std::uint32_t>(first),
                static_cast<std::uint32_t>(second))) {
          throw std::runtime_error(
              "Monero node-remove cannot change physical connectivity "
              "between surviving nodes");
        }
      }
    }
  }

  PeerConnectivityController::AllowedPeerMap final_allowed_peers;
  for (std::uint32_t index = 0U; index < final_count; ++index) {
    final_allowed_peers.emplace(
        final_configs[index].id,
        dependencies.topology_peer_ids(*next_runtime_topology, final_configs,
                                       index));
  }
  auto peer_rpc_lease = peer_controller.AcquireRpcMutationLease(stop_token);
  auto prepared_peer_registration = peer_controller.PrepareFinalRegistration(
      final_configs, {}, final_allowed_peers, {}, peer_rpc_lease);

  std::vector<std::vector<DirectionalNetworkPolicy>> prior_policies(
      retained_old_indexes.size());
  std::vector<std::vector<DirectionalNetworkPolicy>> desired_policies(
      retained_old_indexes.size());
  std::vector<std::vector<std::string>> desired_process_start_peers(
      retained_old_indexes.size());
  std::vector<std::uint32_t> old_resource_slots;
  if (options.isolate_network) {
    old_resource_slots.reserve(before.size());
    for (std::size_t index = 0U; index < before.size(); ++index) {
      old_resource_slots.push_back(before.slot(index));
    }
  }
  {
    std::lock_guard<std::mutex> network_lock(dependencies.network_state_mutex);
    for (std::size_t next_index = 0U; next_index < retained_old_indexes.size();
         ++next_index) {
      NodeRuntime& node = before[retained_old_indexes[next_index]];
      if (options.isolate_network) {
        prior_policies[next_index] = node.directional_network_policies;
        const std::vector<DirectionalNetworkPolicy> expected =
            dependencies.directional_network_policies(
                **runtime_topology, NetworkAddressPlan(options),
                old_resource_slots, retained_old_indexes[next_index]);
        if (prior_policies[next_index] != expected) {
          throw std::runtime_error(
              "node-remove directional policy state does not match the "
              "current topology");
        }
        desired_policies[next_index] =
            dependencies.directional_network_policies(
                *next_runtime_topology, NetworkAddressPlan(options),
                final_resource_slots, static_cast<std::uint32_t>(next_index));
      }
      if (node.uses_physical_start_connect_peers) {
        desired_process_start_peers[next_index] =
            dependencies.physical_peer_endpoints(
                *next_runtime_topology, final_configs,
                static_cast<std::uint32_t>(next_index));
      }
    }
  }

  std::vector<std::vector<std::string>> candidate_endpoints(before.size());
  std::vector<std::optional<std::set<std::string>>> prior_connections(
      before.size());
  for (std::size_t source = 0U; source < before.size(); ++source) {
    candidate_endpoints[source].reserve(before.size() - 1U);
    for (std::size_t target = 0U; target < before.size(); ++target) {
      if (source != target) {
        candidate_endpoints[source].push_back(
            before[target].config.p2p_host + ":" +
            std::to_string(before[target].config.p2p_port));
      }
    }
    if (before[source].AllowsChainMetrics()) {
      const std::vector<std::string> connected = driver.ConnectedPeerAddresses(
          before[source].config, candidate_endpoints[source], stop_token);
      prior_connections[source].emplace(connected.begin(), connected.end());
    }
  }

  RuntimeNodeResourceManifest prior_manifest =
      dependencies.resource_manifest(options, before);
  const std::optional<RuntimeNodeResourceManifest> stored_manifest =
      TryLoadRuntimeNodeResourceManifest(RequireRunOwnership(options));
  if (!stored_manifest || *stored_manifest != prior_manifest) {
    throw std::runtime_error(
        "node-remove runtime resource manifest does not match the live "
        "inventory");
  }
  RuntimeNodeResourceManifest pending_manifest = prior_manifest;
  for (RuntimeNodeResourceEntry& entry : pending_manifest.nodes) {
    if (requested_ids.contains(entry.node_id)) {
      entry.state = RuntimeNodeResourceState::kPendingRemove;
    }
  }
  try {
    WriteRuntimeNodeResourceManifest(pending_manifest);
    const std::optional<RuntimeNodeResourceManifest> pending_readback =
        TryLoadRuntimeNodeResourceManifest(RequireRunOwnership(options));
    if (!pending_readback || *pending_readback != pending_manifest) {
      throw std::runtime_error(
          "node-remove pending resource manifest read-back failed");
    }
  } catch (...) {
    const std::exception_ptr pending_failure = std::current_exception();
    try {
      WriteRuntimeNodeResourceManifest(prior_manifest);
      const std::optional<RuntimeNodeResourceManifest> restored =
          TryLoadRuntimeNodeResourceManifest(RequireRunOwnership(options));
      if (!restored || *restored != prior_manifest) {
        throw std::runtime_error(
            "node-remove prior resource manifest restoration read-back "
            "failed");
      }
    } catch (...) {
      throw SimulationCommandOutcomeUnconfirmed(
          "node-remove pending resource manifest publication failed: " +
          RuntimeNodeRemovalExceptionMessage(pending_failure) +
          "; prior manifest restoration failed: " +
          RuntimeNodeRemovalExceptionMessage(std::current_exception()));
    }
    std::rethrow_exception(pending_failure);
  }

  std::vector<bool> policy_updated(retained_old_indexes.size(), false);
  bool peer_mutation_started = false;
  bool published = false;
  RuntimeNodeRemoveResult result{.removed_node_ids = request.node_ids};
  try {
    if (options.isolate_network) {
      for (std::size_t next_index = 0U;
           next_index < retained_old_indexes.size(); ++next_index) {
        if (prior_policies[next_index] == desired_policies[next_index]) {
          continue;
        }
        NodeRuntime& node = before[retained_old_indexes[next_index]];
        std::lock_guard<std::mutex> network_lock(
            dependencies.network_state_mutex);
        if (!node.network_namespace || !node.network) {
          throw std::runtime_error(
              "surviving isolated node lost its network resource");
        }
        UpdateDirectionalNetworkPoliciesInNamespace(
            node.network_namespace->fd(), node.network->peer_name,
            prior_policies[next_index], desired_policies[next_index],
            stop_token);
        node.directional_network_policies.swap(desired_policies[next_index]);
        policy_updated[next_index] = true;
      }
    }
    if (operation_control != nullptr) {
      operation_control->ReportProgress(1U);
    }

    peer_mutation_started = true;
    const auto peer_visible = [&](std::uint32_t source, std::uint32_t target) {
      if (!before[source].AllowsChainMetrics() ||
          !before[target].AllowsChainMetrics()) {
        return false;
      }
      const std::string endpoint =
          before[target].config.p2p_host + ":" +
          std::to_string(before[target].config.p2p_port);
      return !driver
                  .ConnectedPeerAddresses(before[source].config, {endpoint},
                                          stop_token)
                  .empty();
    };
    for (std::uint32_t first = 0U; first < before.size(); ++first) {
      for (std::uint32_t second = first + 1U; second < before.size();
           ++second) {
        if (!before[first].AllowsChainMetrics() ||
            !before[second].AllowsChainMetrics()) {
          continue;
        }
        const bool visible_from_first = peer_visible(first, second);
        const bool visible_from_second = peer_visible(second, first);
        const bool connected = visible_from_first || visible_from_second;
        const bool should_connect =
            old_to_new[first] && old_to_new[second] &&
            next_runtime_topology->PhysicalPeerRequired(*old_to_new[first],
                                                        *old_to_new[second]);
        if (connected == should_connect) {
          continue;
        }
        ThrowIfStopRequested(stop_token);
        std::uint32_t source = first;
        std::uint32_t target = second;
        if (!should_connect && !visible_from_first && visible_from_second) {
          source = second;
          target = first;
        }
        const std::string endpoint =
            before[target].config.p2p_host + ":" +
            std::to_string(before[target].config.p2p_port);
        if (should_connect) {
          driver.ConnectPeer(before[source].config, endpoint, stop_token);
          driver.WaitForPeerAddress(before[source].config, endpoint,
                                    std::chrono::seconds(request.timeout_sec),
                                    stop_token);
        } else {
          driver.DisconnectPeer(before[source].config, endpoint, stop_token);
          driver.WaitForPeerAddressAbsent(
              before[source].config, endpoint,
              std::chrono::seconds(request.timeout_sec), stop_token);
        }
        if ((peer_visible(first, second) || peer_visible(second, first)) !=
            should_connect) {
          throw std::runtime_error(
              "node-remove physical peer mutation read-back failed");
        }
      }
    }
    if (operation_control != nullptr) {
      operation_control->ReportProgress(2U);
    }

    boost::json::array published_node_ids;
    boost::json::array published_node_configs;
    published_node_ids.reserve(final_count);
    published_node_configs.reserve(final_count);
    for (std::uint32_t index = 0U; index < final_count; ++index) {
      published_node_ids.emplace_back(final_configs[index].id);
      published_node_configs.push_back(RuntimePublishedNodeConfig(
          options, before[retained_old_indexes[index]], final_configs[index],
          index, &next_registry.topology()));
    }
    boost::json::object published_topology;
    if (final_count == 0U) {
      published_topology["type"] = "full_mesh";
      published_topology["resolved_edges"] = boost::json::array{};
    } else {
      AddPeerTopologyJson(next_topology_config, final_count,
                          &published_topology);
    }
    const boost::json::object generation_detail{
        {"generation", before.generation() + 1U},
        {"node_count", final_count},
        {"node_ids", std::move(published_node_ids)},
        {"node_configs", std::move(published_node_configs)},
        {"topology", std::move(published_topology)},
        {"topology_current_edges",
         RuntimePeerTopologyEdgesJson(*next_runtime_topology)},
        {"removed_node_ids",
         [&] {
           boost::json::array ids;
           for (const std::string& id : request.node_ids) {
             ids.emplace_back(id);
           }
           return ids;
         }()},
        {"manifest_state", "live"}};

    std::unique_lock<std::timed_mutex> publication_lock =
        dependencies.acquire_publication_lock(stop_token);
    RuntimeNodeInventory::PreparedRemoval prepared_inventory =
        inventory.PrepareRemoval(before.generation(), request.node_ids,
                                 final_configs);
    RuntimeWalletRegistry::PreparedAppend prepared_registry =
        runtime_registry.PrepareReplace(before_registry.generation(),
                                        std::move(next_registry));
    std::unique_lock<std::mutex> network_lock(dependencies.network_state_mutex);
    if (operation_control != nullptr) {
      if (!operation_control->TryBeginCommit()) {
        throw SimulationCancelled();
      }
    } else {
      ThrowIfStopRequested(stop_token);
    }
    prepared_peer_registration.Commit();
    for (std::size_t next_index = 0U; next_index < retained_old_indexes.size();
         ++next_index) {
      NodeRuntime& node = before[retained_old_indexes[next_index]];
      node.config.connect_peers.swap(final_configs[next_index].connect_peers);
      if (node.uses_physical_start_connect_peers) {
        node.process_start_connect_peers.swap(
            desired_process_start_peers[next_index]);
      }
    }
    runtime_topology->swap(next_runtime_topology);
    static_assert(std::is_nothrow_swappable_v<PeerTopologyConfig>);
    using std::swap;
    swap(*live_topology_config, next_topology_config);
    RuntimeNodeSnapshot published_nodes = prepared_inventory.Commit();
    const RuntimeWalletSnapshot published_registry = prepared_registry.Commit();
    published = true;
    result.inventory_generation = published_nodes.generation();
    result.final_node_count =
        static_cast<std::uint32_t>(published_nodes.size());
    network_lock.unlock();
    publication_lock.unlock();
    if (operation_control != nullptr) {
      operation_control->ReportProgress(3U);
    }

    before = RuntimeNodeSnapshot{};
    before_registry = RuntimeWalletSnapshot{};
    const auto cleanup_deadline =
        operation_control && operation_control->absolute_deadline
            ? *operation_control->absolute_deadline
            : std::chrono::steady_clock::now() +
                  std::chrono::seconds(request.timeout_sec);
    while (!prepared_inventory.ReadersDrained()) {
      if (std::chrono::steady_clock::now() >= cleanup_deadline) {
        throw SimulationCommandOutcomeUnconfirmed(
            "node-remove published but pre-publication readers did not drain "
            "before timeout");
      }
      std::this_thread::sleep_for(std::chrono::milliseconds(2));
    }
    if (operation_control != nullptr) {
      operation_control->ReportProgress(4U);
    }

    std::vector<NodeRuntime> retired_nodes;
    std::vector<std::uint32_t> retired_slots;
    retired_nodes.reserve(prepared_inventory.retired_nodes().size());
    retired_slots.reserve(prepared_inventory.retired_nodes().size());
    for (const RuntimeNodeInsertion& retired :
         prepared_inventory.retired_nodes()) {
      retired_slots.push_back(retired.slot);
      retired_nodes.push_back(std::move(*retired.runtime));
    }
    for (const NodeRuntime& survivor : published_nodes) {
      for (const NodeRuntime& retired : retired_nodes) {
        ChainNodeConfig permit_config = survivor.config;
        const std::string retired_endpoint =
            RuntimeNodeRemovalPeerEndpoint(retired);
        if (std::find(permit_config.connect_peers.begin(),
                      permit_config.connect_peers.end(),
                      retired_endpoint) == permit_config.connect_peers.end()) {
          permit_config.connect_peers.push_back(retired_endpoint);
        }
        driver.ConnectPeer(permit_config, retired_endpoint, stop_token);
      }
    }
    Options cleanup_options = options;
    cleanup_options.cleanup_policy = CleanupPolicy::kAutomatic;
    const std::vector<bool> cleaned = dependencies.stop_retired_nodes(
        cleanup_options, events_path, driver, retired_nodes, retired_slots,
        cleanup_deadline);
    if (cleaned.size() != retired_nodes.size() ||
        std::find(cleaned.begin(), cleaned.end(), false) != cleaned.end()) {
      throw SimulationCommandOutcomeUnconfirmed(
          "node-remove resource cleanup was not positively verified");
    }
    for (std::size_t index = 0U; index < retired_nodes.size(); ++index) {
      const auto manifest_entry = std::find_if(
          pending_manifest.nodes.begin(), pending_manifest.nodes.end(),
          [&](const RuntimeNodeResourceEntry& entry) {
            return entry.node_id == retired_nodes[index].config.id;
          });
      if (manifest_entry == pending_manifest.nodes.end()) {
        throw SimulationCommandOutcomeUnconfirmed(
            "node-remove lost a retired manifest entry");
      }
      RemoveRuntimeNodeRoot(RequireRunOwnership(options), *manifest_entry,
                            cleanup_deadline);
      if (RuntimeNodeRootEntryExists(RequireRunOwnership(options),
                                     manifest_entry->node_id)) {
        throw SimulationCommandOutcomeUnconfirmed(
            "node-remove retired node root survived cleanup");
      }
    }
    RuntimeNodeResourceManifest final_manifest =
        dependencies.resource_manifest(options, published_nodes);
    try {
      WriteRuntimeNodeResourceManifest(final_manifest);
      const std::optional<RuntimeNodeResourceManifest> final_readback =
          TryLoadRuntimeNodeResourceManifest(RequireRunOwnership(options));
      if (!final_readback || *final_readback != final_manifest) {
        throw std::runtime_error(
            "node-remove final resource manifest read-back failed");
      }
    } catch (...) {
      const std::exception_ptr promotion_failure = std::current_exception();
      try {
        WriteRuntimeNodeResourceManifest(pending_manifest);
        const std::optional<RuntimeNodeResourceManifest> restored =
            TryLoadRuntimeNodeResourceManifest(RequireRunOwnership(options));
        if (!restored || *restored != pending_manifest) {
          throw std::runtime_error(
              "node-remove pending resource manifest restoration read-back "
              "failed");
        }
      } catch (...) {
        throw SimulationCommandOutcomeUnconfirmed(
            "node-remove final resource manifest promotion failed: " +
            RuntimeNodeRemovalExceptionMessage(promotion_failure) +
            "; pending manifest restoration failed: " +
            RuntimeNodeRemovalExceptionMessage(std::current_exception()));
      }
      throw SimulationCommandOutcomeUnconfirmed(
          "node-remove final resource manifest promotion failed: " +
          RuntimeNodeRemovalExceptionMessage(promotion_failure));
    }
    WriteEvent(events_path, options.run_id, "sim",
               SimulationEventKind::kRuntimeGenerationPublished,
               boost::json::serialize(generation_detail));
    WriteEvent(events_path, options.run_id, "sim",
               SimulationEventKind::kRuntimeRoleGenerationPublished,
               boost::json::serialize(RuntimeRoleGenerationDetail(
                   published_registry, published_nodes)));
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
      throw SimulationCommandOutcomeUnconfirmed(
          RuntimeNodeRemovalExceptionMessage(failure));
    }
    std::vector<std::string> rollback_errors;
    constexpr auto kRollbackTimeout = std::chrono::seconds(10);
    const auto rollback_deadline =
        std::chrono::steady_clock::now() + kRollbackTimeout;
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
    const auto require_rollback_time = [&] {
      if (rollback_stop_token.stop_requested() ||
          std::chrono::steady_clock::now() >= rollback_deadline) {
        throw std::runtime_error("node-remove rollback deadline expired");
      }
    };
    const auto rollback = [&](std::string_view description,
                              const auto& action) {
      try {
        require_rollback_time();
        action();
      } catch (...) {
        rollback_errors.push_back(
            std::string(description) + ": " +
            RuntimeNodeRemovalExceptionMessage(std::current_exception()));
      }
    };
    if (peer_mutation_started) {
      for (std::size_t source = 0U; source < before.size(); ++source) {
        if (!prior_connections[source]) {
          continue;
        }
        rollback("restore peer state for " + before[source].config.id, [&] {
          const std::vector<std::string> observed =
              driver.ConnectedPeerAddresses(before[source].config,
                                            candidate_endpoints[source],
                                            rollback_stop_token);
          const std::set<std::string> current(observed.begin(), observed.end());
          for (const std::string& endpoint : candidate_endpoints[source]) {
            const bool was_connected =
                prior_connections[source]->contains(endpoint);
            const bool is_connected = current.contains(endpoint);
            if (was_connected == is_connected) {
              continue;
            }
            if (was_connected) {
              driver.ConnectPeer(before[source].config, endpoint,
                                 rollback_stop_token);
              driver.WaitForPeerAddress(before[source].config, endpoint,
                                        kRollbackTimeout, rollback_stop_token);
            } else {
              driver.DisconnectPeer(before[source].config, endpoint,
                                    rollback_stop_token);
              driver.WaitForPeerAddressAbsent(before[source].config, endpoint,
                                              kRollbackTimeout,
                                              rollback_stop_token);
            }
          }
          const std::vector<std::string> restored =
              driver.ConnectedPeerAddresses(before[source].config,
                                            candidate_endpoints[source],
                                            rollback_stop_token);
          if (std::set<std::string>(restored.begin(), restored.end()) !=
              *prior_connections[source]) {
            throw std::runtime_error(
                "peer state differs from its pre-remove snapshot");
          }
        });
      }
    }
    if (options.isolate_network) {
      for (std::size_t next_index = 0U;
           next_index < retained_old_indexes.size(); ++next_index) {
        if (!policy_updated[next_index]) {
          continue;
        }
        NodeRuntime& node = before[retained_old_indexes[next_index]];
        rollback("restore directional policy for " + node.config.id, [&] {
          std::lock_guard<std::mutex> network_lock(
              dependencies.network_state_mutex);
          UpdateDirectionalNetworkPoliciesInNamespace(
              node.network_namespace->fd(), node.network->peer_name,
              node.directional_network_policies, prior_policies[next_index],
              rollback_stop_token);
          node.directional_network_policies.swap(prior_policies[next_index]);
        });
      }
    }
    rollback("restore resource manifest", [&] {
      WriteRuntimeNodeResourceManifest(prior_manifest);
      const std::optional<RuntimeNodeResourceManifest> restored =
          TryLoadRuntimeNodeResourceManifest(RequireRunOwnership(options));
      if (!restored || *restored != prior_manifest) {
        throw std::runtime_error(
            "node-remove resource manifest rollback read-back failed");
      }
    });
    if (!rollback_errors.empty()) {
      std::string detail =
          "node-remove failed: " + RuntimeNodeRemovalExceptionMessage(failure) +
          "; rollback could not be verified";
      for (const std::string& error : rollback_errors) {
        detail += "; " + error;
      }
      throw SimulationCommandOutcomeUnconfirmed(std::move(detail));
    }
    std::rethrow_exception(failure);
  }
  return result;
}

}  // namespace bbp::simulator_app_internal
