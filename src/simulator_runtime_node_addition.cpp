#include "simulator_runtime_node_addition.h"

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
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <set>
#include <stdexcept>
#include <stop_token>
#include <string>
#include <string_view>
#include <system_error>
#include <thread>
#include <type_traits>
#include <utility>
#include <vector>

#include "bbp/capability.h"
#include "bbp/default_peer_topology.h"
#include "bbp/drivers/chain_driver_registry.h"
#include "bbp/peer_connectivity_controller.h"
#include "bbp/probabilistic_block_scheduler.h"
#include "bbp/run_process_state.h"
#include "bbp/runtime_node_inventory.h"
#include "bbp/runtime_peer_topology.h"
#include "bbp/runtime_wallet_registry.h"
#include "bbp/simulation_cancelled.h"
#include "bbp/simulation_command.h"
#include "bbp/simulation_event_kind.h"
#include "bbp/simulation_network_address_plan.h"
#include "bbp/simulation_node_add.h"
#include "bbp/simulator/options.h"
#include "bbp/util.h"
#include "simulator_cancellable_waiting.h"
#include "simulator_event_writing.h"
#include "simulator_host_probes.h"
#include "simulator_masternode_funding_boundary.h"
#include "simulator_network_launch_planning.h"
#include "simulator_node_process_state.h"
#include "simulator_resource_profile_decoding.h"
#include "simulator_runtime_identity_details.h"
#include "simulator_runtime_published_node_config.h"
#include "simulator_scenario_identifier.h"
#include "simulator_scenario_serialization.h"
#include "simulator_tcp_endpoint_reservation.h"
#include "simulator_wallet_transaction_validation.h"

namespace bbp::simulator_app_internal {

namespace {

std::string RuntimeNodeAdditionExceptionMessage(
    const std::exception_ptr& error) {
  try {
    std::rethrow_exception(error);
  } catch (const std::exception& exception) {
    return exception.what();
  } catch (...) {
    return "unknown exception";
  }
}

std::string RuntimeNodeAdditionPeerEndpoint(const NodeRuntime& node) {
  return node.config.p2p_host + ":" + std::to_string(node.config.p2p_port);
}

}  // namespace

RuntimeNodeAddResult AddRuntimeNodesTransactional(
    const Options& options, const std::filesystem::path& run_root,
    const std::filesystem::path& events_path, const ChainDriverSpec& chain_spec,
    const ChainDriver& driver, RuntimeNodeInventory& inventory,
    RuntimeWalletRegistry& runtime_registry, RuntimeNodeAdditionRole added_role,
    ProbabilisticBlockScheduler* block_scheduler,
    std::vector<std::string>* configured_miner_node_ids,
    std::mutex* configured_miner_node_ids_mutex,
    const RuntimeMasternodeAddContext* masternode_context,
    PeerConnectivityController& peer_controller,
    std::unique_ptr<RuntimePeerTopology>* runtime_topology,
    PeerTopologyConfig* live_topology_config,
    RunProcessState& run_process_state,
    std::chrono::steady_clock::time_point lifecycle_epoch,
    const SimulationNodeAddRequest& request,
    SimulationCommandControl* operation_control, std::stop_token stop_token,
    const RuntimeNodeAdditionDependencies& dependencies) {
  if (request.chain != options.chain) {
    throw std::runtime_error(
        "node-add chain must match the active simulation chain");
  }
  if (request.count == 0U) {
    throw std::runtime_error("node-add count must be greater than zero");
  }
  const RuntimeNodeSnapshot before = inventory.Snapshot();
  const RuntimeWalletSnapshot before_registry = runtime_registry.Snapshot();
  const WalletIdentity* masternode_funding_wallet = nullptr;
  std::optional<std::uint32_t> masternode_miner_index;
  ChainMasternodeFundingRequirements masternode_funding_requirements;
  if (added_role == RuntimeNodeAdditionRole::kWallet) {
    if (before_registry.wallets().size() >
        std::numeric_limits<std::uint32_t>::max() - request.count) {
      throw std::overflow_error("wallet.add wallet index exceeds uint32");
    }
  }
  if (added_role == RuntimeNodeAdditionRole::kMiner) {
    if (configured_miner_node_ids == nullptr ||
        configured_miner_node_ids_mutex == nullptr) {
      throw std::logic_error(
          "miner node addition requires configured-miner state");
    }
    if (options.block_production.enabled &&
        options.block_production.mode == MiningMode::kNativeMining) {
      throw UnsupportedChainOperation("runtime miner addition",
                                      "transactional native-miner activation");
    }
    if (options.block_production.enabled &&
        options.block_production.difficulty) {
      throw UnsupportedChainOperation(
          "runtime miner addition",
          "transactional mining-difficulty activation");
    }
    if (options.block_production.enabled &&
        options.block_production.mode ==
            MiningMode::kScheduledBlockProduction &&
        block_scheduler == nullptr) {
      throw std::logic_error(
          "scheduled miner addition requires the block scheduler");
    }
  }
  if (added_role == RuntimeNodeAdditionRole::kMasternode) {
    if (masternode_context == nullptr ||
        masternode_context->block_generation_mutex == nullptr ||
        masternode_context->funding_wallet_node_id.empty()) {
      throw std::logic_error(
          "masternode node addition requires funding context");
    }
    if (!driver.SupportsMasternodes()) {
      throw UnsupportedChainOperation(ChainKindName(options.chain),
                                      "masternode addition");
    }
    if (before_registry.registry().wallet_initialization().mode !=
        WalletPrivacyMode::kPublic) {
      throw std::runtime_error(
          "masternode addition requires public wallet mode");
    }
    const auto wallet = std::find_if(
        before_registry.wallets().begin(), before_registry.wallets().end(),
        [&](const WalletIdentity& candidate) {
          return candidate.node_id ==
                 masternode_context->funding_wallet_node_id;
        });
    if (wallet == before_registry.wallets().end()) {
      throw std::runtime_error("masternode funding wallet is not registered");
    }
    if (wallet->node == 0U || wallet->node > before.size() ||
        wallet->funding_address.empty()) {
      throw std::runtime_error(
          "masternode funding wallet identity is incomplete");
    }
    masternode_funding_wallet = &*wallet;
    for (const std::uint32_t miner :
         before_registry.registry().topology().miner_nodes) {
      if (miner < before.size() && before[miner].AllowsChainMetrics() &&
          NodeProcessRunning(before[miner])) {
        masternode_miner_index = miner;
        break;
      }
    }
    if (!masternode_miner_index) {
      throw std::runtime_error(
          "masternode addition requires a running configured miner");
    }
    if (options.block_production.enabled &&
        options.block_production.mode == MiningMode::kNativeMining) {
      throw UnsupportedChainOperation(
          ChainKindName(options.chain),
          "masternode mutation while native mining is active");
    }
    masternode_funding_requirements =
        driver.MasternodeFundingRequirements(request.count);
  }
  if (before.generation() == std::numeric_limits<std::uint64_t>::max()) {
    throw std::overflow_error("node-add inventory generation overflow");
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
          "node-add command has a conflicting initial inventory");
    }
  }
  if (request.count > inventory.capacity() - before.size()) {
    throw std::runtime_error(
        "node-add request exceeds the configured node capacity");
  }
  if (request.network && !options.isolate_network) {
    throw std::runtime_error(
        "node-add network condition requires an isolated simulation");
  }
  if (options.isolate_network) {
    RequireNetworkSetupCapabilities();
    if (before.size() + request.count > 1U && !HostIpv4ForwardingEnabled()) {
      throw std::runtime_error(
          "isolated multi-node chain runs require IPv4 forwarding in the "
          "parent network namespace");
    }
  }

  std::set<std::uint32_t> used_slots;
  std::set<std::string> used_ids;
  std::vector<std::uint32_t> resource_slots;
  std::vector<ChainNodeConfig> final_configs;
  resource_slots.reserve(before.size() + request.count);
  final_configs.reserve(before.size() + request.count);
  {
    std::lock_guard<std::mutex> network_lock(dependencies.network_state_mutex);
    for (std::size_t index = 0U; index < before.size(); ++index) {
      used_slots.insert(before.slot(index));
      used_ids.insert(before[index].config.id);
      resource_slots.push_back(before.slot(index));
      final_configs.push_back(before[index].config);
    }
  }
  for (std::uint32_t slot = 0U;
       slot < inventory.capacity() &&
       resource_slots.size() < before.size() + request.count;
       ++slot) {
    if (!used_slots.contains(slot)) {
      resource_slots.push_back(slot);
    }
  }
  if (resource_slots.size() != before.size() + request.count) {
    throw std::logic_error(
        "configured capacity did not provide enough free resource slots");
  }
  if (options.isolate_network) {
    const std::vector<std::uint32_t> selected_slots(
        resource_slots.begin() +
            static_cast<std::vector<std::uint32_t>::difference_type>(
                before.size()),
        resource_slots.end());
    NetworkAddressPlan(options).RequireNodeSlotsAvailable(
        selected_slots, ListIpv4Routes(stop_token),
        ListIpv4Addresses(stop_token));
    const std::vector<LinkInfo> links = ListNetworkLinks(stop_token);
    std::set<std::string> candidate_link_names;
    std::set<std::string> candidate_link_aliases;
    for (const std::uint32_t slot : selected_slots) {
      const NodeVethConfig candidate = MakeNodeVethConfig(options, slot);
      for (const std::string& name :
           {candidate.host_name, candidate.peer_name}) {
        if (!candidate_link_names.insert(name).second) {
          throw std::runtime_error(
              "node-add candidate network interface names collide: " + name);
        }
      }
      for (const std::string& alias :
           {candidate.host_ownership_alias, candidate.peer_ownership_alias}) {
        if (!candidate_link_aliases.insert(alias).second) {
          throw std::runtime_error(
              "node-add candidate network ownership aliases collide");
        }
      }
      const auto collision =
          std::find_if(links.begin(), links.end(), [&](const LinkInfo& link) {
            return candidate_link_names.contains(link.name) ||
                   candidate_link_aliases.contains(link.ownership_alias);
          });
      if (collision != links.end()) {
        throw std::runtime_error(
            "node-add network identity collides with existing link: " +
            collision->name);
      }
    }
  }

  std::vector<std::string> node_ids = request.node_ids;
  if (node_ids.empty()) {
    node_ids.reserve(request.count);
    for (std::uint64_t suffix = 1U;
         suffix <= std::numeric_limits<std::uint32_t>::max() &&
         node_ids.size() < request.count;
         ++suffix) {
      const std::string candidate =
          chain_spec.node_id_prefix + "-" + std::to_string(suffix);
      if (used_ids.contains(candidate) || inventory.WasNodeIdUsed(candidate) ||
          RuntimeNodeRootEntryExists(RequireRunOwnership(options), candidate)) {
        continue;
      }
      used_ids.insert(candidate);
      node_ids.push_back(candidate);
    }
    if (node_ids.size() != request.count) {
      throw std::runtime_error(
          "node-add could not allocate canonical node ids within the uint32 "
          "identity space");
    }
  } else {
    if (node_ids.size() != request.count) {
      throw std::runtime_error(
          "node-add explicit ids must match the requested count");
    }
    for (const std::string& node_id : node_ids) {
      RequireSafeScenarioIdentifier(node_id, "node-add node id");
      if (!used_ids.insert(node_id).second ||
          inventory.WasNodeIdUsed(node_id)) {
        throw std::runtime_error(
            "node-add node id was already used by this run: " + node_id);
      }
      if (RuntimeNodeRootEntryExists(RequireRunOwnership(options), node_id)) {
        throw std::runtime_error(
            "node-add refuses to adopt an existing node root: " + node_id);
      }
    }
  }

  const std::filesystem::path binary =
      request.binary ? std::filesystem::path(*request.binary)
                     : options.chain_daemon;
  if (binary.empty()) {
    throw std::runtime_error(
        "node-add requires a daemon binary in the request or run");
  }
  for (std::size_t index = 0U; index < node_ids.size(); ++index) {
    ChainNodeConfigRequest config_request;
    config_request.run_id = options.run_id;
    config_request.run_root = run_root;
    config_request.daemon_binary = binary;
    config_request.data_dir =
        std::filesystem::path("nodes") / node_ids[index] / "data";
    config_request.node_index = resource_slots.at(before.size() + index);
    config_request.node_id = node_ids[index];
    config_request.network = ChainNetwork::kRegtest;
    config_request.wallet_enabled =
        added_role == RuntimeNodeAdditionRole::kWallet;
    ChainNodeConfig config = MakeChainNodeConfig(chain_spec, config_request);
    if (options.isolate_network) {
      config.rpc_host =
          NetworkAddressPlan(options).NodeAddress(config_request.node_index);
      config.rpc_bind = config.rpc_host;
      config.rpc_allow_ips = {
          NetworkAddressPlan(options).HostAddress(config_request.node_index)};
      config.p2p_host = config.rpc_host;
      config.p2p_bind = config.rpc_host;
    }
    final_configs.push_back(std::move(config));
  }

  const std::uint32_t final_count =
      static_cast<std::uint32_t>(final_configs.size());
  PeerTopologyConfig next_topology_config =
      request.topology ? *request.topology : *live_topology_config;
  auto next_runtime_topology =
      std::make_unique<RuntimePeerTopology>(next_topology_config, final_count);
  if (!request.topology) {
    next_runtime_topology->PreserveCommonStateFrom(**runtime_topology);
  }
  for (std::uint32_t index = 0U; index < final_count; ++index) {
    final_configs[index].connect_peers = dependencies.restart_peer_endpoints(
        before_registry.registry().topology(), *next_runtime_topology,
        final_configs, index);
  }
  std::vector<ChainNodeConfig> startup_configs = final_configs;
  for (std::uint32_t index = static_cast<std::uint32_t>(before.size());
       index < final_count; ++index) {
    startup_configs[index].connect_peers = dependencies.physical_peer_endpoints(
        *next_runtime_topology, final_configs, index);
  }
  if (!options.isolate_network &&
      std::any_of(next_runtime_topology->edges().begin(),
                  next_runtime_topology->edges().end(),
                  [](const RuntimePeerTopologyEdge& edge) {
                    return edge.active && edge.condition.has_value();
                  })) {
    throw std::runtime_error(
        "node-add conditioned topology requires an isolated simulation");
  }
  if (options.chain == ChainKind::kMonero &&
      !next_runtime_topology->PreservesPhysicalPeerRequirementsFrom(
          **runtime_topology, static_cast<std::uint32_t>(before.size()))) {
    throw std::runtime_error(
        "Monero node-add cannot change physical connectivity between existing "
        "nodes");
  }

  struct CandidatePortReservations {
    std::unique_ptr<boost::asio::ip::tcp::acceptor> rpc;
    std::unique_ptr<boost::asio::ip::tcp::acceptor> p2p;
  };
  boost::asio::io_context port_reservation_context;
  std::vector<CandidatePortReservations> port_reservations;
  port_reservations.reserve(request.count);
  if (!options.isolate_network) {
    for (std::size_t index = before.size(); index < final_configs.size();
         ++index) {
      const ChainNodeConfig& config = final_configs[index];
      CandidatePortReservations reservation;
      reservation.rpc = ReserveTcpEndpoint(
          port_reservation_context,
          config.rpc_bind.empty() ? config.rpc_host : config.rpc_bind,
          config.rpc_port, "tcp_port", config.id, "RPC");
      reservation.p2p =
          ReserveTcpEndpoint(port_reservation_context,
                             EffectiveP2pBindAddress(options.chain, config),
                             config.p2p_port, "tcp_port", config.id, "P2P");
      port_reservations.push_back(std::move(reservation));
    }
  }

  PeerConnectivityController::OptionalPolicyMap new_policies;
  PeerConnectivityController::AllowedPeerMap final_allowed_peers;
  for (std::uint32_t index = 0U; index < final_count; ++index) {
    final_allowed_peers.emplace(
        final_configs[index].id,
        dependencies.topology_peer_ids(*next_runtime_topology, final_configs,
                                       index));
    if (index >= before.size()) {
      new_policies.emplace(final_configs[index].id, std::nullopt);
    }
  }
  auto peer_rpc_lease = peer_controller.AcquireRpcMutationLease(stop_token);
  auto prepared_peer_registration = peer_controller.PrepareFinalRegistration(
      final_configs, new_policies, final_allowed_peers, {}, peer_rpc_lease);

  std::vector<std::vector<std::string>> candidate_endpoints(final_count);
  std::vector<std::vector<std::string>> existing_endpoints(before.size());
  std::vector<ChainNodeConfig> rollback_configs = final_configs;
  for (std::uint32_t source = 0U; source < final_count; ++source) {
    for (std::uint32_t target = 0U; target < final_count; ++target) {
      if (source == target) {
        continue;
      }
      candidate_endpoints[source].push_back(
          final_configs[target].p2p_host + ":" +
          std::to_string(final_configs[target].p2p_port));
      if (source < before.size() && target < before.size()) {
        existing_endpoints[source].push_back(
            final_configs[target].p2p_host + ":" +
            std::to_string(final_configs[target].p2p_port));
      }
    }
    if (source < before.size()) {
      rollback_configs[source].connect_peers = existing_endpoints[source];
    }
  }
  std::vector<std::optional<std::set<std::string>>> prior_connections(
      before.size());
  for (std::size_t source = 0U; source < before.size(); ++source) {
    if (!before[source].AllowsChainMetrics()) {
      continue;
    }
    const std::vector<std::string> connected = driver.ConnectedPeerAddresses(
        before[source].config, candidate_endpoints[source], stop_token);
    prior_connections[source].emplace(connected.begin(), connected.end());
  }

  std::vector<std::vector<DirectionalNetworkPolicy>> prior_directional_policies(
      before.size());
  std::vector<std::vector<DirectionalNetworkPolicy>>
      desired_directional_policies(before.size());
  std::vector<std::vector<std::string>> desired_process_start_peers(
      before.size());
  {
    std::lock_guard<std::mutex> network_lock(dependencies.network_state_mutex);
    for (std::size_t source = 0U; source < before.size(); ++source) {
      if (options.isolate_network) {
        prior_directional_policies[source] =
            before[source].directional_network_policies;
        const std::vector<DirectionalNetworkPolicy> expected_prior =
            dependencies.directional_network_policies(
                **runtime_topology, NetworkAddressPlan(options), resource_slots,
                static_cast<std::uint32_t>(source));
        if (prior_directional_policies[source] != expected_prior) {
          throw std::runtime_error(
              "node-add runtime directional policy state does not match the "
              "current topology");
        }
        desired_directional_policies[source] =
            dependencies.directional_network_policies(
                *next_runtime_topology, NetworkAddressPlan(options),
                resource_slots, static_cast<std::uint32_t>(source));
      }
      if (before[source].uses_physical_start_connect_peers) {
        desired_process_start_peers[source] =
            dependencies.physical_peer_endpoints(
                *next_runtime_topology, final_configs,
                static_cast<std::uint32_t>(source));
      }
    }
  }

  std::vector<RuntimeNodeInsertion> insertions;
  insertions.reserve(request.count);
  std::vector<bool> candidate_root_acquired;
  candidate_root_acquired.reserve(request.count);
  RuntimeNodeResourceManifest prior_manifest =
      dependencies.resource_manifest(options, before);
  const std::optional<RuntimeNodeResourceManifest> stored_manifest =
      TryLoadRuntimeNodeResourceManifest(RequireRunOwnership(options));
  if (!stored_manifest || *stored_manifest != prior_manifest) {
    throw std::runtime_error(
        "node-add runtime resource manifest does not match the live "
        "inventory");
  }
  RuntimeNodeResourceManifest pending_manifest = prior_manifest;

  std::vector<bool> directional_policy_updated(before.size(), false);
  bool physical_peer_mutation_started = false;
  bool published = false;
  RuntimeNodeAddResult result;
  result.added_node_ids = node_ids;
  std::vector<ChainMasternodeRegistration> registered_masternodes;
  std::vector<MasternodeIdentity> added_masternodes;
  const std::filesystem::path staged_events_path =
      run_root / ".runtime-node-add-events.pending";
  bool staged_events_created = false;
  const auto remove_staged_events = [&] {
    if (!staged_events_created) {
      return;
    }
    std::error_code error;
    const bool removed = std::filesystem::remove(staged_events_path, error);
    if (error || !removed || std::filesystem::exists(staged_events_path)) {
      throw std::runtime_error(
          "node-add staged event cleanup failed: " +
          (error ? error.message() : std::string("file survived removal")));
    }
    staged_events_created = false;
  };
  const auto rollback = [&] {
    const auto rollback_timeout = registered_masternodes.empty()
                                      ? std::chrono::seconds(10)
                                      : std::chrono::seconds(60);
    const auto rollback_deadline =
        std::chrono::steady_clock::now() + rollback_timeout;
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
        throw std::runtime_error("node-add rollback deadline expired");
      }
    };
    std::vector<std::string> errors;
    const auto rollback_step = [&](std::string_view description,
                                   auto&& action) {
      try {
        require_rollback_time();
        action();
      } catch (...) {
        errors.push_back(
            std::string(description) + ": " +
            RuntimeNodeAdditionExceptionMessage(std::current_exception()));
      }
    };
    if (!registered_masternodes.empty()) {
      rollback_step("confirm and revoke registered masternodes", [&] {
        if (masternode_context == nullptr ||
            masternode_context->block_generation_mutex == nullptr ||
            masternode_funding_wallet == nullptr || !masternode_miner_index) {
          throw std::logic_error(
              "masternode rollback funding context is unavailable");
        }
        NodeRuntime& funding_node =
            before[masternode_funding_wallet->node - 1U];
        NodeRuntime& miner = before[*masternode_miner_index];
        RuntimeNodePointers active_nodes;
        active_nodes.reserve(before.size() + insertions.size());
        for (NodeRuntime& node : before) {
          active_nodes.push_back(&node);
        }
        for (RuntimeNodeInsertion& insertion : insertions) {
          if (insertion.runtime) {
            active_nodes.push_back(&*insertion.runtime);
          }
        }
        std::vector<MasternodeTransactionConfirmation>
            registration_transactions;
        registration_transactions.reserve(registered_masternodes.size());
        for (const ChainMasternodeRegistration& registration :
             registered_masternodes) {
          registration_transactions.push_back(MasternodeTransactionConfirmation{
              .funding_wallet = &funding_node,
              .transaction_id = registration.pro_tx_hash,
          });
        }
        ConfirmMasternodeTransactions(
            driver, *masternode_context->block_generation_mutex, miner,
            masternode_funding_wallet->funding_address, active_nodes,
            registration_transactions,
            masternode_funding_requirements.registration_confirmation_blocks,
            rollback_timeout, rollback_stop_token);

        std::vector<MasternodeTransactionConfirmation> revocations;
        revocations.reserve(registered_masternodes.size());
        for (const ChainMasternodeRegistration& registration :
             registered_masternodes) {
          revocations.push_back(MasternodeTransactionConfirmation{
              .funding_wallet = &funding_node,
              .transaction_id = driver.RevokeMasternode(
                  funding_node.config, registration.pro_tx_hash,
                  registration.operator_secret_key,
                  masternode_funding_wallet->funding_address,
                  rollback_stop_token),
          });
        }
        ConfirmMasternodeTransactions(
            driver, *masternode_context->block_generation_mutex, miner,
            masternode_funding_wallet->funding_address, active_nodes,
            revocations,
            masternode_funding_requirements.revocation_confirmation_blocks,
            rollback_timeout, rollback_stop_token);
      });
    }
    std::vector<std::uint32_t> cleanup_resource_slots;
    std::vector<std::size_t> cleanup_manifest_indexes;
    std::vector<NodeRuntime> cleanup_nodes;
    cleanup_resource_slots.reserve(insertions.size());
    cleanup_manifest_indexes.reserve(insertions.size());
    cleanup_nodes.reserve(insertions.size());
    for (std::size_t insertion_index = 0U; insertion_index < insertions.size();
         ++insertion_index) {
      if (!candidate_root_acquired.at(insertion_index)) {
        continue;
      }
      const RuntimeNodeInsertion& insertion = insertions[insertion_index];
      cleanup_resource_slots.push_back(insertion.slot);
      cleanup_manifest_indexes.push_back(prior_manifest.nodes.size() +
                                         insertion_index);
      cleanup_nodes.push_back(std::move(*insertion.runtime));
    }
    std::vector<bool> resource_cleanup_verified(cleanup_nodes.size(), false);
    if (!cleanup_nodes.empty()) {
      Options rollback_options = options;
      rollback_options.cleanup_policy = CleanupPolicy::kAutomatic;
      rollback_step("stop candidate nodes", [&] {
        resource_cleanup_verified = dependencies.stop_candidates(
            rollback_options, staged_events_path, driver, cleanup_nodes,
            cleanup_resource_slots, rollback_deadline, rollback_stop_token);
        if (resource_cleanup_verified.size() != cleanup_nodes.size()) {
          throw std::runtime_error(
              "candidate cleanup returned an invalid result count");
        }
      });
    }
    if (physical_peer_mutation_started) {
      for (std::size_t source = 0U; source < before.size(); ++source) {
        if (!prior_connections[source]) {
          continue;
        }
        rollback_step(
            "restore peer state for " + before[source].config.id, [&] {
              const std::vector<std::string> observed =
                  driver.ConnectedPeerAddresses(rollback_configs[source],
                                                candidate_endpoints[source],
                                                rollback_stop_token);
              std::set<std::string> current(observed.begin(), observed.end());
              for (const std::string& endpoint : candidate_endpoints[source]) {
                const bool was_connected =
                    prior_connections[source]->contains(endpoint);
                const bool is_connected = current.contains(endpoint);
                if (was_connected == is_connected) {
                  continue;
                }
                if (was_connected) {
                  driver.ConnectPeer(rollback_configs[source], endpoint,
                                     rollback_stop_token);
                  driver.WaitForPeerAddress(rollback_configs[source], endpoint,
                                            rollback_timeout,
                                            rollback_stop_token);
                } else {
                  driver.DisconnectPeer(rollback_configs[source], endpoint,
                                        rollback_stop_token);
                  driver.WaitForPeerAddressAbsent(rollback_configs[source],
                                                  endpoint, rollback_timeout,
                                                  rollback_stop_token);
                }
              }
              const std::vector<std::string> restored =
                  driver.ConnectedPeerAddresses(rollback_configs[source],
                                                candidate_endpoints[source],
                                                rollback_stop_token);
              const std::set<std::string> restored_set(restored.begin(),
                                                       restored.end());
              if (restored_set != *prior_connections[source]) {
                throw std::runtime_error(
                    "peer read-back differs from the pre-add "
                    "snapshot");
              }
            });
      }
    }
    if (options.isolate_network) {
      for (std::size_t source = 0U; source < before.size(); ++source) {
        if (!directional_policy_updated[source]) {
          continue;
        }
        rollback_step(
            "restore directional policy for " + before[source].config.id, [&] {
              std::lock_guard<std::mutex> network_lock(
                  dependencies.network_state_mutex);
              if (!before[source].network_namespace ||
                  !before[source].network) {
                throw std::runtime_error(
                    "existing isolated node lost its network "
                    "resource");
              }
              UpdateDirectionalNetworkPoliciesInNamespace(
                  before[source].network_namespace->fd(),
                  before[source].network->peer_name,
                  before[source].directional_network_policies,
                  prior_directional_policies[source], rollback_stop_token);
              before[source].directional_network_policies.swap(
                  prior_directional_policies[source]);
            });
      }
    }

    const auto candidate_step =
        [&](std::size_t index, std::string_view description, auto&& action) {
          try {
            require_rollback_time();
            action();
          } catch (...) {
            resource_cleanup_verified[index] = false;
            errors.push_back(
                std::string(description) + ": " +
                RuntimeNodeAdditionExceptionMessage(std::current_exception()));
          }
        };
    for (std::size_t index = 0U; index < cleanup_nodes.size(); ++index) {
      if (!resource_cleanup_verified[index]) {
        errors.push_back(
            "candidate support-resource cleanup was not positively verified: " +
            cleanup_nodes[index].config.id);
      }
    }
    for (std::size_t index = 0U; index < cleanup_nodes.size(); ++index) {
      if (!resource_cleanup_verified[index]) {
        continue;
      }
      candidate_step(index, "remove candidate node root", [&] {
        const RuntimeNodeResourceEntry& entry =
            pending_manifest.nodes[cleanup_manifest_indexes[index]];
        RemoveRuntimeNodeRoot(RequireRunOwnership(options), entry,
                              rollback_deadline, rollback_stop_token);
        if (RuntimeNodeRootEntryExists(RequireRunOwnership(options),
                                       entry.node_id)) {
          throw std::runtime_error("candidate node root survived rollback");
        }
      });
    }
    RuntimeNodeResourceManifest rollback_manifest = prior_manifest;
    for (std::size_t index = 0U; index < resource_cleanup_verified.size();
         ++index) {
      if (!resource_cleanup_verified[index]) {
        rollback_manifest.nodes.push_back(
            pending_manifest.nodes[cleanup_manifest_indexes[index]]);
      }
    }
    rollback_step("reconcile runtime resource manifest", [&] {
      WriteRuntimeNodeResourceManifest(rollback_manifest);
      const std::optional<RuntimeNodeResourceManifest> restored =
          TryLoadRuntimeNodeResourceManifest(RequireRunOwnership(options));
      if (!restored || *restored != rollback_manifest) {
        throw std::runtime_error(
            "runtime resource manifest rollback read-back failed");
      }
    });
    rollback_step("remove staged node-add events", remove_staged_events);
    if (!errors.empty()) {
      std::string message;
      for (const std::string& error : errors) {
        if (!message.empty()) {
          message += "; ";
        }
        message += error;
      }
      throw std::runtime_error(message);
    }
  };

  try {
    if (std::filesystem::exists(staged_events_path)) {
      throw std::runtime_error("node-add staged event path already exists");
    }
    WriteText(staged_events_path, "");
    staged_events_created = true;
    for (std::size_t index = 0U; index < node_ids.size(); ++index) {
      ThrowIfStopRequested(stop_token);
      const std::size_t final_index = before.size() + index;
      insertions.push_back(RuntimeNodeInsertion{
          .slot = resource_slots[final_index],
          .runtime = std::make_shared<NodeRuntime>(),
      });
      candidate_root_acquired.push_back(false);
      std::vector<DirectionalNetworkPolicy> directional_policies;
      if (options.isolate_network) {
        const std::uint32_t slot = resource_slots[final_index];
        NetworkAddressPlan(options).RequireNodeSlotsAvailable(
            {slot}, ListIpv4Routes(stop_token), ListIpv4Addresses(stop_token));
        const NodeVethConfig expected_network =
            MakeNodeVethConfig(options, slot);
        const std::vector<LinkInfo> links = ListNetworkLinks(stop_token);
        const auto collision =
            std::find_if(links.begin(), links.end(), [&](const LinkInfo& link) {
              return link.name == expected_network.host_name ||
                     link.name == expected_network.peer_name ||
                     link.ownership_alias ==
                         expected_network.host_ownership_alias ||
                     link.ownership_alias ==
                         expected_network.peer_ownership_alias;
            });
        if (collision != links.end()) {
          throw std::runtime_error(
              "node-add network identity collides with existing link: " +
              collision->name);
        }
        directional_policies = dependencies.directional_network_policies(
            *next_runtime_topology, NetworkAddressPlan(options), resource_slots,
            static_cast<std::uint32_t>(final_index));
      }
      bool root_acquired = false;
      pending_manifest.nodes.push_back(dependencies.resource_entry(
          options, startup_configs[final_index], resource_slots[final_index],
          RuntimeNodeResourceState::kPendingAdd));
      WriteRuntimeNodeResourceManifest(pending_manifest);
      try {
        dependencies.prepare_node(
            options, staged_events_path, *insertions.back().runtime,
            startup_configs[final_index], resource_slots[final_index],
            request.resources ? *request.resources
                              : InitialResourceLimits(options),
            {}, {}, std::move(directional_policies), request.network,
            run_process_state, stop_token, &root_acquired);
      } catch (...) {
        candidate_root_acquired.back() = root_acquired;
        throw;
      }
      candidate_root_acquired.back() = root_acquired;
      insertions.back().runtime->process_start_connect_peers =
          startup_configs[final_index].connect_peers;
      insertions.back().runtime->uses_physical_start_connect_peers = true;
    }
    if (operation_control != nullptr) {
      operation_control->ReportProgress(1U);
    }

    Options dynamic_options = options;
    dynamic_options.ready_timeout_sec = request.ready_timeout_sec;
    dynamic_options.sync_timeout_sec = request.sync_timeout_sec;
    for (std::size_t index = 0U; index < insertions.size(); ++index) {
      ThrowIfStopRequested(stop_token);
      if (!options.isolate_network) {
        boost::system::error_code close_error;
        port_reservations[index].rpc->close(close_error);
        if (close_error) {
          throw std::runtime_error(
              "node-add could not release the RPC port reservation: " +
              close_error.message());
        }
        port_reservations[index].p2p->close(close_error);
        if (close_error) {
          throw std::runtime_error(
              "node-add could not release the P2P port reservation: " +
              close_error.message());
        }
      }
      RuntimeNodeInsertion& insertion = insertions[index];
      if (!dependencies.start_node(dynamic_options, staged_events_path, driver,
                                   *insertion.runtime, "runtime_node_add",
                                   lifecycle_epoch, stop_token)) {
        throw std::runtime_error(
            "dynamic node reached a declarative stop before readiness: " +
            insertion.runtime->config.id);
      }
    }
    if (operation_control != nullptr) {
      operation_control->ReportProgress(2U);
    }

    const auto runtime_at = [&](std::uint32_t index) -> NodeRuntime& {
      if (index < before.size()) {
        return before[index];
      }
      return *insertions.at(index - before.size()).runtime;
    };
    if (options.isolate_network) {
      for (std::size_t source = 0U; source < before.size(); ++source) {
        if (prior_directional_policies[source] ==
            desired_directional_policies[source]) {
          continue;
        }
        std::lock_guard<std::mutex> network_lock(
            dependencies.network_state_mutex);
        if (!before[source].network_namespace || !before[source].network) {
          throw std::runtime_error(
              "existing isolated node lost its network resource");
        }
        UpdateDirectionalNetworkPoliciesInNamespace(
            before[source].network_namespace->fd(),
            before[source].network->peer_name,
            prior_directional_policies[source],
            desired_directional_policies[source], stop_token);
        before[source].directional_network_policies.swap(
            desired_directional_policies[source]);
        directional_policy_updated[source] = true;
      }
    }

    physical_peer_mutation_started = true;
    const auto peer_visible = [&](std::uint32_t source_index,
                                  std::uint32_t target_index) {
      if (!runtime_at(source_index).AllowsChainMetrics() ||
          !runtime_at(target_index).AllowsChainMetrics()) {
        return false;
      }
      const std::string endpoint =
          RuntimeNodeAdditionPeerEndpoint(runtime_at(target_index));
      return !driver
                  .ConnectedPeerAddresses(runtime_at(source_index).config,
                                          {endpoint}, stop_token)
                  .empty();
    };
    for (std::uint32_t first = 0U; first < final_count; ++first) {
      for (std::uint32_t second = first + 1U; second < final_count; ++second) {
        if (!runtime_at(first).AllowsChainMetrics() ||
            !runtime_at(second).AllowsChainMetrics()) {
          continue;
        }
        const bool visible_from_first = peer_visible(first, second);
        const bool visible_from_second = peer_visible(second, first);
        const bool is_connected = visible_from_first || visible_from_second;
        const bool should_connect = dependencies.physical_peer_required(
            *next_runtime_topology, first, second);
        if (is_connected == should_connect) {
          continue;
        }
        ThrowIfStopRequested(stop_token);
        std::uint32_t source_index = first;
        std::uint32_t target_index = second;
        if (should_connect && first < before.size() &&
            second >= before.size()) {
          source_index = second;
          target_index = first;
        } else if (!should_connect && !visible_from_first &&
                   visible_from_second) {
          source_index = second;
          target_index = first;
        }
        NodeRuntime& source = runtime_at(source_index);
        const std::string endpoint =
            RuntimeNodeAdditionPeerEndpoint(runtime_at(target_index));
        if (should_connect) {
          driver.ConnectPeer(source.config, endpoint, stop_token);
          driver.WaitForPeerAddress(
              source.config, endpoint,
              std::chrono::seconds(request.ready_timeout_sec), stop_token);
        } else {
          driver.DisconnectPeer(source.config, endpoint, stop_token);
          driver.WaitForPeerAddressAbsent(
              source.config, endpoint,
              std::chrono::seconds(request.ready_timeout_sec), stop_token);
        }
        const bool read_back =
            peer_visible(first, second) || peer_visible(second, first);
        if (read_back != should_connect) {
          throw std::runtime_error(
              "node-add physical peer mutation read-back failed");
        }
        boost::json::object detail;
        detail["address"] = endpoint;
        detail["reason"] = "runtime_node_add";
        WriteEvent(staged_events_path, options.run_id, source.config.id,
                   should_connect ? SimulationEventKind::kStartupPeerConnected
                                  : SimulationEventKind::kPeerDisconnected,
                   boost::json::serialize(detail));
      }
    }
    for (std::size_t index = 0U; index < insertions.size(); ++index) {
      insertions[index].runtime->config.connect_peers =
          final_configs[before.size() + index].connect_peers;
    }
    if (operation_control != nullptr) {
      operation_control->ReportProgress(3U);
    }

    const auto sync_deadline = std::chrono::steady_clock::now() +
                               std::chrono::seconds(request.sync_timeout_sec);
    std::stop_source sync_stop_source;
    std::stop_callback stop_sync_on_request(
        stop_token, [&sync_stop_source] { sync_stop_source.request_stop(); });
    std::jthread sync_timer(
        [sync_deadline, &sync_stop_source](std::stop_token timer_stop_token) {
          try {
            WaitUntil(sync_deadline, timer_stop_token);
          } catch (const SimulationCancelled&) {
            return;
          }
          sync_stop_source.request_stop();
        });
    const std::stop_token sync_stop_token = sync_stop_source.get_token();
    ChainMetrics synchronized_tip;
    try {
      for (;;) {
        ThrowIfStopRequested(sync_stop_token);
        std::vector<ChainMetrics> metrics;
        metrics.reserve(final_count);
        for (std::uint32_t index = 0U; index < final_count; ++index) {
          NodeRuntime& runtime = runtime_at(index);
          if (runtime.AllowsChainMetrics()) {
            metrics.push_back(
                driver.ReadMetrics(runtime.config, sync_stop_token));
          }
        }
        if (metrics.empty()) {
          throw std::runtime_error(
              "node-add synchronization has no readable chain node");
        }
        synchronized_tip = metrics.front();
        const bool synchronized =
            synchronized_tip.sync_status == ChainSyncStatus::kSynced &&
            !synchronized_tip.best_hash.empty() &&
            std::all_of(metrics.begin(), metrics.end(),
                        [&](const ChainMetrics& metric) {
                          return metric.sync_status ==
                                     ChainSyncStatus::kSynced &&
                                 metric.height == synchronized_tip.height &&
                                 metric.best_hash == synchronized_tip.best_hash;
                        });
        if (synchronized) {
          break;
        }
        WaitForDuration(std::chrono::milliseconds(100), sync_stop_token);
      }
    } catch (const SimulationCancelled&) {
      if (stop_token.stop_requested()) {
        throw;
      }
      throw std::runtime_error(
          "node-add chain synchronization deadline expired");
    }
    for (RuntimeNodeInsertion& insertion : insertions) {
      WriteEvent(staged_events_path, options.run_id,
                 insertion.runtime->config.id,
                 SimulationEventKind::kHeightReached,
                 boost::json::serialize(boost::json::object{
                     {"height", synchronized_tip.height},
                     {"best_hash", synchronized_tip.best_hash},
                     {"sync_status", std::string(ChainSyncStatusName(
                                         synchronized_tip.sync_status))}}));
    }
    if (operation_control != nullptr) {
      operation_control->ReportProgress(4U);
    }

    if (added_role == RuntimeNodeAdditionRole::kMasternode) {
      if (masternode_context == nullptr ||
          masternode_context->block_generation_mutex == nullptr ||
          masternode_funding_wallet == nullptr || !masternode_miner_index) {
        throw std::logic_error("masternode addition funding context was lost");
      }
      RuntimeNodePointers active_nodes;
      active_nodes.reserve(final_count);
      for (NodeRuntime& node : before) {
        active_nodes.push_back(&node);
      }
      for (RuntimeNodeInsertion& insertion : insertions) {
        active_nodes.push_back(&*insertion.runtime);
      }
      NodeRuntime& funding_node = before[masternode_funding_wallet->node - 1U];
      NodeRuntime& miner = before[*masternode_miner_index];
      const auto operation_timeout =
          std::chrono::seconds(request.ready_timeout_sec);
      static_cast<void>(PrepareMasternodeFunding(
          driver, *masternode_context->block_generation_mutex, miner,
          funding_node, masternode_funding_wallet->funding_address,
          active_nodes, masternode_funding_requirements, operation_timeout,
          stop_token));

      const auto completion_timeout = std::chrono::seconds(
          std::max<std::uint32_t>(60U, request.ready_timeout_sec));
      const auto completion_deadline =
          std::chrono::steady_clock::now() + completion_timeout;
      std::stop_source completion_stop_source;
      std::jthread completion_timer(
          [completion_deadline,
           &completion_stop_source](std::stop_token timer_stop_token) {
            try {
              WaitUntil(completion_deadline, timer_stop_token);
            } catch (const SimulationCancelled&) {
              return;
            }
            completion_stop_source.request_stop();
          });
      const std::stop_token completion_stop_token =
          completion_stop_source.get_token();
      registered_masternodes.reserve(insertions.size());
      for (RuntimeNodeInsertion& insertion : insertions) {
        ThrowIfStopRequested(stop_token);
        const std::string service =
            insertion.runtime->config.p2p_host + ":" +
            std::to_string(insertion.runtime->config.p2p_port);
        registered_masternodes.push_back(driver.RegisterMasternode(
            funding_node.config, service,
            masternode_funding_wallet->funding_address, completion_stop_token));
        ThrowIfStopRequested(stop_token);
      }
      std::vector<MasternodeTransactionConfirmation> registrations;
      registrations.reserve(registered_masternodes.size());
      for (const ChainMasternodeRegistration& registration :
           registered_masternodes) {
        registrations.push_back(MasternodeTransactionConfirmation{
            .funding_wallet = &funding_node,
            .transaction_id = registration.pro_tx_hash,
        });
      }
      ConfirmMasternodeTransactions(
          driver, *masternode_context->block_generation_mutex, miner,
          masternode_funding_wallet->funding_address, active_nodes,
          registrations,
          masternode_funding_requirements.registration_confirmation_blocks,
          operation_timeout, completion_stop_token);
      ThrowIfStopRequested(stop_token);

      added_masternodes.reserve(insertions.size());
      for (std::size_t offset = 0U; offset < insertions.size(); ++offset) {
        NodeRuntime& node = *insertions[offset].runtime;
        const std::uint32_t node_index =
            static_cast<std::uint32_t>(before.size() + offset);
        const ChainMasternodeRegistration& registration =
            registered_masternodes[offset];
        node.config.masternode = ChainNodeConfig::MasternodeProcessConfig{
            .operator_secret_key = registration.operator_secret_key,
            .service = registration.service,
        };
        final_configs[before.size() + offset].masternode =
            node.config.masternode;
        if (!dependencies.restart_masternode(
                options, staged_events_path, driver, peer_controller, node,
                lifecycle_epoch, completion_stop_token, "masternode_add")) {
          throw std::runtime_error(
              "masternode.add target reached stop_time during restart: " +
              node.config.id);
        }
        for (std::uint32_t other = 0U; other < final_count; ++other) {
          if (other == node_index ||
              !dependencies.physical_peer_required(*next_runtime_topology,
                                                   node_index, other) ||
              peer_visible(node_index, other)) {
            continue;
          }
          const std::string endpoint =
              RuntimeNodeAdditionPeerEndpoint(runtime_at(other));
          driver.ConnectPeer(node.config, endpoint, completion_stop_token);
          driver.WaitForPeerAddress(node.config, endpoint, operation_timeout,
                                    completion_stop_token);
          if (!peer_visible(node_index, other)) {
            throw std::runtime_error(
                "masternode.add restart topology restoration failed");
          }
          WriteEvent(
              staged_events_path, options.run_id, node.config.id,
              SimulationEventKind::kPeerConnected,
              boost::json::serialize(boost::json::object{
                  {"peer_node_id", runtime_at(other).config.id},
                  {"reason", "masternode_add_restart_topology_restore"}}));
        }
        const ChainMasternodeStatus status = driver.WaitForMasternodeReady(
            node.config, registration.pro_tx_hash, operation_timeout,
            completion_stop_token);
        added_masternodes.push_back(RegisteredMasternodeIdentity(
            node_index + 1U, node.config.id,
            masternode_context->funding_wallet_node_id, registration, status));
        ThrowIfStopRequested(stop_token);
      }
    }

    std::vector<WalletIdentity> added_wallets;
    if (added_role == RuntimeNodeAdditionRole::kWallet) {
      const WalletInitialization initialization =
          before_registry.registry().wallet_initialization();
      added_wallets.reserve(insertions.size());
      for (std::size_t offset = 0U; offset < insertions.size(); ++offset) {
        ThrowIfStopRequested(stop_token);
        NodeRuntime& node = *insertions[offset].runtime;
        WalletIdentity wallet{
            .wallet_index = static_cast<std::uint32_t>(
                before_registry.wallets().size() + offset + 1U),
            .node = static_cast<std::uint32_t>(before.size() + offset + 1U),
            .node_id = node.config.id,
            .address = {},
            .funding_address = {},
        };
        WriteEvent(staged_events_path, options.run_id, node.config.id,
                   SimulationEventKind::kWalletAddressRequested,
                   WalletAddressDetail(wallet, initialization));
        wallet.address = driver.CreateWalletAddress(
            node.config, ToChainWalletMode(initialization), stop_token);
        if (wallet.address.empty()) {
          throw std::runtime_error(
              "wallet.add chain RPC returned an empty address");
        }
        wallet.funding_address = driver.CreateWalletFundingAddress(
            node.config, ToChainWalletMode(initialization), wallet.address,
            stop_token);
        if (wallet.funding_address.empty()) {
          throw std::runtime_error(
              "wallet.add chain RPC returned an empty funding address");
        }
        WriteEvent(staged_events_path, options.run_id, node.config.id,
                   SimulationEventKind::kWalletAddressCreated,
                   WalletAddressDetail(wallet, initialization));
        added_wallets.push_back(std::move(wallet));
      }
    }

    std::vector<std::uint32_t> added_miner_nodes;
    if (added_role == RuntimeNodeAdditionRole::kMiner) {
      added_miner_nodes.reserve(insertions.size());
      for (std::size_t offset = 0U; offset < insertions.size(); ++offset) {
        added_miner_nodes.push_back(
            static_cast<std::uint32_t>(before.size() + offset));
      }
    }
    SimulationRegistry next_registry = before_registry.registry();
    next_registry.SetRuntimeNodeCount(final_count);
    for (const WalletIdentity& wallet : added_wallets) {
      next_registry.AddWallet(wallet);
    }
    for (const std::uint32_t miner_node : added_miner_nodes) {
      next_registry.AddMinerNode(miner_node);
    }
    for (const MasternodeIdentity& masternode : added_masternodes) {
      next_registry.AddMasternode(masternode);
    }

    boost::json::array published_node_ids;
    boost::json::array published_node_configs;
    published_node_ids.reserve(final_count);
    published_node_configs.reserve(final_count);
    for (std::uint32_t index = 0U; index < final_count; ++index) {
      published_node_ids.emplace_back(final_configs[index].id);
      published_node_configs.push_back(RuntimePublishedNodeConfig(
          options, runtime_at(index), final_configs[index], index,
          &next_registry.topology()));
    }
    boost::json::object published_topology;
    AddPeerTopologyJson(next_topology_config, final_count, &published_topology);
    const std::uint64_t published_generation = before.generation() + 1U;
    const boost::json::object published_generation_detail{
        {"generation", published_generation},
        {"node_count", final_count},
        {"node_ids", std::move(published_node_ids)},
        {"node_configs", std::move(published_node_configs)},
        {"topology", std::move(published_topology)},
        {"topology_current_edges",
         RuntimePeerTopologyEdgesJson(*next_runtime_topology)},
        {"manifest_state", "live"}};

    constexpr std::size_t kMaximumStagedNodeAddEventBytes = 4U * 1024U * 1024U;
    const std::string staged_events =
        ReadText(staged_events_path, kMaximumStagedNodeAddEventBytes, {});
    if (!staged_events.empty() && staged_events.back() != '\n') {
      throw std::runtime_error(
          "node-add staged event stream ended with an incomplete record");
    }
    std::vector<std::string> staged_event_records;
    std::size_t staged_offset = 0U;
    while (staged_offset < staged_events.size()) {
      const std::size_t end = staged_events.find('\n', staged_offset);
      std::string record(staged_events.data() + staged_offset,
                         end - staged_offset);
      if (record.empty() || !boost::json::parse(record).is_object()) {
        throw std::runtime_error(
            "node-add staged event stream contains an invalid record");
      }
      staged_event_records.push_back(std::move(record));
      staged_offset = end + 1U;
    }

    std::unique_lock<std::timed_mutex> publication_lock =
        dependencies.acquire_publication_lock(stop_token);
    RuntimeNodeInventory::PreparedAppend prepared =
        inventory.PrepareAppend(before.generation(), insertions, final_configs);
    RuntimeWalletRegistry::PreparedAppend prepared_registry_update =
        runtime_registry.PrepareUpdate(before_registry.generation(),
                                       added_wallets, added_miner_nodes,
                                       added_masternodes, final_count);
    std::optional<ProbabilisticBlockScheduler::PreparedAdd>
        prepared_scheduler_add;
    std::vector<std::string> next_configured_miner_node_ids;
    std::optional<std::unique_lock<std::mutex>> configured_miner_node_ids_lock;
    if (added_role == RuntimeNodeAdditionRole::kMiner) {
      configured_miner_node_ids_lock.emplace(*configured_miner_node_ids_mutex);
      next_configured_miner_node_ids = *configured_miner_node_ids;
      next_configured_miner_node_ids.reserve(
          next_configured_miner_node_ids.size() + node_ids.size());
      for (const std::string& node_id : node_ids) {
        if (std::find(next_configured_miner_node_ids.begin(),
                      next_configured_miner_node_ids.end(),
                      node_id) != next_configured_miner_node_ids.end()) {
          throw std::logic_error("new miner node is already configured: " +
                                 node_id);
        }
        next_configured_miner_node_ids.push_back(node_id);
      }
      if (block_scheduler != nullptr) {
        prepared_scheduler_add.emplace(
            block_scheduler->PrepareAddMiners(node_ids));
      }
    }
    std::unique_lock<std::mutex> network_lock(dependencies.network_state_mutex);
    if (operation_control != nullptr) {
      if (!operation_control->TryBeginCommit()) {
        throw SimulationCancelled();
      }
    } else {
      ThrowIfStopRequested(stop_token);
    }
    prepared_peer_registration.Commit();
    for (std::size_t index = 0U; index < before.size(); ++index) {
      before[index].config.connect_peers.swap(
          final_configs[index].connect_peers);
      if (before[index].uses_physical_start_connect_peers) {
        before[index].process_start_connect_peers.swap(
            desired_process_start_peers[index]);
      }
    }
    runtime_topology->swap(next_runtime_topology);
    static_assert(std::is_nothrow_swappable_v<PeerTopologyConfig>);
    using std::swap;
    swap(*live_topology_config, next_topology_config);
    const RuntimeNodeSnapshot published_nodes = prepared.Commit();
    static_assert(std::is_nothrow_move_constructible_v<RuntimeWalletSnapshot>);
    const RuntimeWalletSnapshot published_registry =
        prepared_registry_update.Commit();
    if (prepared_scheduler_add) {
      prepared_scheduler_add->Commit();
    }
    if (added_role == RuntimeNodeAdditionRole::kMiner) {
      configured_miner_node_ids->swap(next_configured_miner_node_ids);
    }
    published = true;
    result.inventory_generation = published_nodes.generation();
    result.final_node_count =
        static_cast<std::uint32_t>(published_nodes.size());
    result.role_generation = published_registry.generation();
    result.final_miner_count =
        published_registry.registry().topology().miner_nodes.size();
    result.final_masternode_count =
        published_registry.registry().topology().masternode_nodes.size();
    if (added_role == RuntimeNodeAdditionRole::kWallet) {
      result.wallet_generation = published_registry.generation();
      result.final_wallet_count = published_registry.wallets().size();
      result.final_wallet_node_count =
          published_registry.registry().topology().wallet_nodes.size();
    }
    RuntimeNodeResourceManifest live_manifest = pending_manifest;
    for (RuntimeNodeResourceEntry& entry : live_manifest.nodes) {
      entry.state = RuntimeNodeResourceState::kLive;
    }
    try {
      WriteRuntimeNodeResourceManifest(live_manifest);
      const std::optional<RuntimeNodeResourceManifest> persisted =
          TryLoadRuntimeNodeResourceManifest(RequireRunOwnership(options));
      if (!persisted || *persisted != live_manifest) {
        throw std::runtime_error(
            "node-add live resource manifest read-back failed");
      }
    } catch (...) {
      const std::exception_ptr promotion_failure = std::current_exception();
      try {
        WriteRuntimeNodeResourceManifest(pending_manifest);
      } catch (...) {
        throw SimulationCommandOutcomeUnconfirmed(
            "node-add published but live resource manifest promotion failed: " +
            RuntimeNodeAdditionExceptionMessage(promotion_failure) +
            "; pending resource manifest restoration failed: " +
            RuntimeNodeAdditionExceptionMessage(std::current_exception()));
      }
      throw SimulationCommandOutcomeUnconfirmed(
          "node-add published but live resource manifest promotion failed: " +
          RuntimeNodeAdditionExceptionMessage(promotion_failure));
    }
    WriteEvent(events_path, options.run_id, "sim",
               SimulationEventKind::kRuntimeGenerationPublished,
               boost::json::serialize(published_generation_detail));
    WriteEvent(events_path, options.run_id, "sim",
               SimulationEventKind::kRuntimeRoleGenerationPublished,
               boost::json::serialize(RuntimeRoleGenerationDetail(
                   published_registry, published_nodes)));
    if (added_role == RuntimeNodeAdditionRole::kWallet) {
      WriteEvent(events_path, options.run_id, "sim",
                 SimulationEventKind::kRuntimeWalletGenerationPublished,
                 boost::json::serialize(RuntimeWalletGenerationDetail(
                     published_registry, added_wallets)));
    }
    for (const std::string& record : staged_event_records) {
      AppendLine(events_path, record);
    }
    remove_staged_events();
    if (operation_control != nullptr) {
      operation_control->MarkCommitted();
      operation_control->ReportProgress(kSimulationNodeAddProgressTotal);
    }
    result.added_wallets = std::move(added_wallets);
    result.added_masternodes = std::move(added_masternodes);
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
            RuntimeNodeAdditionExceptionMessage(failure) +
            "; staged event cleanup failed: " +
            RuntimeNodeAdditionExceptionMessage(std::current_exception()));
      }
      throw SimulationCommandOutcomeUnconfirmed(
          RuntimeNodeAdditionExceptionMessage(failure));
    }
    try {
      rollback();
    } catch (...) {
      throw SimulationCommandOutcomeUnconfirmed(
          "node-add failed: " + RuntimeNodeAdditionExceptionMessage(failure) +
          "; rollback could not be verified: " +
          RuntimeNodeAdditionExceptionMessage(std::current_exception()));
    }
    try {
      std::rethrow_exception(failure);
    } catch (const ChainMasternodeOutcomeUnknown& error) {
      throw SimulationCommandOutcomeUnconfirmed(error.what());
    } catch (...) {
    }
    std::rethrow_exception(failure);
  }
  return result;
}

}  // namespace bbp::simulator_app_internal
