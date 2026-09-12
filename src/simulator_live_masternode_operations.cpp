#include "simulator_live_masternode_operations.h"

#include <algorithm>
#include <array>
#include <atomic>
#include <boost/json/array.hpp>
#include <boost/json/serialize.hpp>
#include <chrono>
#include <cstdint>
#include <exception>
#include <iterator>
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

#include "bbp/chain_kind.h"
#include "bbp/drivers/chain_driver.h"
#include "bbp/mcp_live_application.h"
#include "bbp/mcp_operation_service.h"
#include "bbp/mcp_registry.h"
#include "bbp/mining_mode.h"
#include "bbp/probabilistic_block_scheduler.h"
#include "bbp/run_process_state.h"
#include "bbp/runtime_node_inventory.h"
#include "bbp/runtime_wallet_registry.h"
#include "bbp/scenario_service.h"
#include "bbp/simulation_cancelled.h"
#include "bbp/simulation_command.h"
#include "bbp/simulation_event_kind.h"
#include "bbp/simulation_node_add.h"
#include "bbp/simulation_registry.h"
#include "bbp/simulator/node_runtime.h"
#include "bbp/simulator/options.h"
#include "simulator_cancellable_waiting.h"
#include "simulator_event_writing.h"
#include "simulator_json_field_decoding.h"
#include "simulator_masternode_funding_boundary.h"
#include "simulator_node_process_state.h"
#include "simulator_runtime_identity_details.h"
#include "simulator_runtime_node_addition.h"
#include "simulator_scenario_identifier.h"
#include "simulator_scenario_node_resolution.h"

namespace bbp::simulator_app_internal {

boost::json::object ExecuteLiveMasternodeOperation(
    const LiveMasternodeOperationContext& context, McpOperationKind kind,
    const boost::json::object& arguments,
    std::stop_token operation_stop_token) {
  const bool adding = kind == McpOperationKind::kAddMasternode;
  const bool removing = kind == McpOperationKind::kRemoveMasternode;
  const std::string action = std::string(McpOperationKindName(kind));
  if (adding) {
    constexpr std::array<std::string_view, 6U> kAllowedFields = {
        "run_id",       "node_ids",          "count",
        "create_nodes", "funding_wallet_id", "timeout_sec"};
    RejectUnsupportedFields(arguments, kAllowedFields, action);
  } else {
    constexpr std::array<std::string_view, 3U> kAllowedFields = {
        "run_id", "node_ids", "timeout_sec"};
    RejectUnsupportedFields(arguments, kAllowedFields, action);
  }
  if (!context.driver.SupportsMasternodes()) {
    const UnsupportedChainOperation error(ChainKindName(context.options.chain),
                                          action);
    throw McpOperationFailure("unsupported_chain_operation", error.what(),
                              false);
  }
  if (context.options.block_production.enabled &&
      context.options.block_production.mode == MiningMode::kNativeMining) {
    const UnsupportedChainOperation error(
        ChainKindName(context.options.chain),
        "masternode mutation while native mining is active");
    throw McpOperationFailure("unsupported_chain_operation", error.what(),
                              false);
  }

  const std::uint32_t timeout_sec =
      JsonOptionalUint32Field(arguments, "timeout_sec", 60U);
  if (timeout_sec == 0U || timeout_sec > 3600U) {
    throw std::invalid_argument(action + " timeout_sec must be in 1..3600");
  }
  const std::uint32_t count =
      adding ? JsonOptionalUint32Field(arguments, "count", 0U) : 0U;
  if (adding && (count == 0U || count > kSimulationNodeAddMaximumCount)) {
    throw std::invalid_argument("masternode.add count must be in 1..16");
  }
  std::vector<std::string> requested_node_ids;
  if (const boost::json::value* node_ids = arguments.if_contains("node_ids")) {
    if (!node_ids->is_array()) {
      throw std::invalid_argument(action + " node_ids must be an array");
    }
    std::set<std::string> unique_node_ids;
    requested_node_ids.reserve(node_ids->as_array().size());
    for (const boost::json::value& node_id : node_ids->as_array()) {
      if (!node_id.is_string()) {
        throw std::invalid_argument(action + " node_ids must contain strings");
      }
      std::string id(node_id.as_string());
      RequireSafeScenarioIdentifier(id, action + " node_ids");
      if (!unique_node_ids.insert(id).second) {
        throw std::invalid_argument(action + " node_ids must be unique");
      }
      requested_node_ids.push_back(std::move(id));
    }
  }
  if (adding && !requested_node_ids.empty() &&
      requested_node_ids.size() != count) {
    throw std::invalid_argument(
        "masternode.add count must match node_ids size");
  }
  if (!adding && requested_node_ids.empty()) {
    throw std::invalid_argument(action + " node_ids must not be empty");
  }
  const boost::json::value* create_nodes =
      adding ? arguments.if_contains("create_nodes") : nullptr;
  if (create_nodes != nullptr && !create_nodes->is_object()) {
    throw std::invalid_argument(
        "masternode.add create_nodes must be an object");
  }
  if (create_nodes != nullptr && !requested_node_ids.empty()) {
    throw std::invalid_argument(
        "masternode.add node_ids and create_nodes are mutually "
        "exclusive");
  }
  std::string funding_wallet_id;
  if (adding) {
    funding_wallet_id = JsonOptionalStringField(arguments, "funding_wallet_id",
                                                std::string_view());
    RequireSafeScenarioIdentifier(funding_wallet_id,
                                  "masternode.add funding_wallet_id");
  }

  const auto deadline =
      std::chrono::steady_clock::now() + std::chrono::seconds(timeout_sec);
  std::stop_source bounded_stop_source;
  std::atomic_bool deadline_expired = false;
  std::stop_callback stop_on_operation(
      operation_stop_token,
      [&bounded_stop_source] { bounded_stop_source.request_stop(); });
  std::jthread deadline_timer(
      [deadline, &bounded_stop_source,
       &deadline_expired](std::stop_token timer_stop_token) {
        try {
          WaitUntil(deadline, timer_stop_token);
        } catch (const SimulationCancelled&) {
          return;
        }
        deadline_expired.store(true, std::memory_order_release);
        bounded_stop_source.request_stop();
      });
  const std::stop_token bounded_stop_token = bounded_stop_source.get_token();
  ThrowIfStopRequested(bounded_stop_token);
  auto mutation_lock = context.acquire_node_mutation_lock(
      context.node_mutation_mutex, bounded_stop_token);
  RuntimeNodeSnapshot current_nodes = context.node_inventory.Snapshot();
  const RuntimeWalletSnapshot before_roles =
      context.runtime_wallet_registry.Snapshot();
  const NodeRoleTopology& before_topology = before_roles.registry().topology();

  if (adding && create_nodes != nullptr) {
    if (before_roles.registry().wallet_initialization().mode !=
        WalletPrivacyMode::kPublic) {
      throw McpOperationFailure(
          "wallet_mode_conflict",
          "masternode.add requires the active run public wallet mode", false);
    }
    Options validation_options = context.options;
    validation_options.nodes = static_cast<std::uint32_t>(current_nodes.size());
    validation_options.node_capacity = current_nodes.capacity();
    validation_options.node_ids.clear();
    for (const NodeRuntime& node : current_nodes) {
      validation_options.node_ids.push_back(node.config.id);
    }
    const SimulationNodeAddRequest node_request =
        ParseAndValidateSimulationNodeAddRequest(create_nodes->as_object(),
                                                 validation_options);
    if (node_request.count != count) {
      throw std::invalid_argument(
          "masternode.add count must match create_nodes.count");
    }
    SimulationCommandControl role_control;
    role_control.absolute_deadline = deadline;
    std::stop_callback cancel_role_publication(bounded_stop_token, [&] {
      static_cast<void>(role_control.RequestCancellation(
          deadline_expired.load(std::memory_order_acquire)
              ? SimulationCommandCancellationCause::kDeadline
              : SimulationCommandCancellationCause::kClientCancel));
    });
    RuntimeMasternodeAddContext masternode_context{
        .funding_wallet_node_id = funding_wallet_id,
        .block_generation_mutex = &context.block_generation_mutex,
    };
    RuntimeNodeAddResult added;
    try {
      std::lock_guard<std::mutex> topology_lock(context.runtime_topology_mutex);
      added = AddRuntimeNodesTransactional(
          context.options, context.run_root, context.events_path,
          context.chain_spec, context.driver, context.node_inventory,
          context.runtime_wallet_registry, RuntimeNodeAdditionRole::kMasternode,
          context.block_scheduler.get(), &context.miner_node_ids,
          &context.configured_miner_node_ids_mutex, &masternode_context,
          *context.peer_connectivity_controller, &context.runtime_topology,
          &context.live_topology_config, context.run_process_state,
          context.lifecycle_epoch, node_request, &role_control,
          bounded_stop_token, context.runtime_node_addition_dependencies);
    } catch (const SimulationNodeResourceUnavailable& error) {
      if (role_control.CommitPhase() ==
          SimulationCommandCommitPhase::kCancelled) {
        throw SimulationCancelled();
      }
      const SimulationNodeResourceFailure& failure = error.failure();
      throw McpOperationFailure(
          "node_resource_unavailable", error.what(), true,
          boost::json::array{boost::json::object{
              {"code", "node_resource_unavailable"},
              {"message", error.what()},
              {"path", "create_nodes"},
              {"resource_kind", failure.resource_kind},
              {"node_id", failure.node_id},
              {"address", failure.address},
              {"port", failure.port},
              {"purpose", failure.purpose},
              {"mutation_started", failure.mutation_started},
              {"action", action},
              {"recoverable", true}}});
    } catch (const SimulationCommandOutcomeUnconfirmed& error) {
      context.mcp_application.MarkRunStopping();
      context.request_simulation_stop();
      throw McpOperationFailure(
          "masternode_add_outcome_unconfirmed",
          "masternode.add create_nodes outcome is unconfirmed: " +
              std::string(error.what()),
          false);
    } catch (...) {
      if (role_control.CommitPhase() ==
          SimulationCommandCommitPhase::kCancelled) {
        throw SimulationCancelled();
      }
      throw;
    }
    try {
      if (!added.role_generation || !added.final_masternode_count ||
          added.added_node_ids.size() != count ||
          added.added_masternodes.size() != count) {
        throw std::logic_error(
            "masternode.add create_nodes omitted its joint "
            "publication evidence");
      }
      boost::json::array node_ids;
      boost::json::array created_node_ids;
      boost::json::array masternodes;
      for (const std::string& node_id : added.added_node_ids) {
        node_ids.emplace_back(node_id);
        created_node_ids.emplace_back(node_id);
      }
      for (const MasternodeIdentity& masternode : added.added_masternodes) {
        masternodes.push_back(RuntimeMasternodeIdentityJson(masternode));
      }
      return boost::json::object{
          {"node_ids", std::move(node_ids)},
          {"assigned_roles", boost::json::array{"masternode"}},
          {"removed_roles", boost::json::array{}},
          {"action", action},
          {"state", "ready"},
          {"created_node_ids", std::move(created_node_ids)},
          {"role_generation", *added.role_generation},
          {"final_masternode_count", *added.final_masternode_count},
          {"masternodes", std::move(masternodes)},
          {"inventory_generation", added.inventory_generation},
          {"final_node_count", added.final_node_count},
          {"node_capacity", added.node_capacity},
          {"network_allocation", added.network_allocation},
      };
    } catch (...) {
      context.mcp_application.MarkRunStopping();
      context.request_simulation_stop();
      throw McpOperationFailure(
          "masternode_add_outcome_unconfirmed",
          "masternode.add create_nodes published but completion "
          "evidence failed: " +
              context.exception_message(std::current_exception()),
          false);
    }
  }

  RuntimeNodePointers active_nodes;
  active_nodes.reserve(current_nodes.size());
  for (NodeRuntime& node : current_nodes) {
    active_nodes.push_back(&node);
  }
  const auto find_node_index = [&](std::string_view node_id) {
    const auto found = std::find_if(
        current_nodes.begin(), current_nodes.end(),
        [&](const NodeRuntime& node) { return node.config.id == node_id; });
    if (found == current_nodes.end()) {
      throw McpOperationFailure(
          "node_not_found",
          action + " node is not active: " + std::string(node_id), false);
    }
    return static_cast<std::size_t>(
        std::distance(current_nodes.begin(), found));
  };
  const auto find_wallet =
      [&](std::string_view node_id) -> const WalletIdentity& {
    const auto found = std::find_if(before_roles.wallets().begin(),
                                    before_roles.wallets().end(),
                                    [&](const WalletIdentity& wallet) {
                                      return wallet.node_id == node_id;
                                    });
    if (found == before_roles.wallets().end()) {
      throw McpOperationFailure(
          "funding_wallet_not_found",
          action + " funding wallet is not registered: " + std::string(node_id),
          false);
    }
    return *found;
  };
  const auto running_miner_index = [&]() -> std::size_t {
    for (const std::uint32_t miner : before_topology.miner_nodes) {
      if (miner < current_nodes.size() &&
          current_nodes[miner].AllowsChainMetrics() &&
          NodeProcessRunning(current_nodes[miner])) {
        return miner;
      }
    }
    throw McpOperationFailure("miner_unavailable",
                              action + " requires a running configured miner",
                              false);
  };
  const auto public_identities_json =
      [](const std::vector<MasternodeIdentity>& identities) {
        boost::json::array result;
        result.reserve(identities.size());
        for (const MasternodeIdentity& identity : identities) {
          result.push_back(RuntimeMasternodeIdentityJson(identity));
        }
        return result;
      };
  const auto node_ids_json = [](const std::vector<std::string>& node_ids) {
    boost::json::array result;
    result.reserve(node_ids.size());
    for (const std::string& node_id : node_ids) {
      result.emplace_back(node_id);
    }
    return result;
  };

  if (adding) {
    SimulationCommandControl role_control;
    role_control.absolute_deadline = deadline;
    std::stop_callback cancel_role_publication(bounded_stop_token, [&] {
      static_cast<void>(role_control.RequestCancellation(
          deadline_expired.load(std::memory_order_acquire)
              ? SimulationCommandCancellationCause::kDeadline
              : SimulationCommandCancellationCause::kClientCancel));
    });
    if (before_roles.registry().wallet_initialization().mode !=
        WalletPrivacyMode::kPublic) {
      throw McpOperationFailure(
          "wallet_mode_conflict",
          "masternode.add requires the active run public wallet mode", false);
    }
    const WalletIdentity& funding_wallet = find_wallet(funding_wallet_id);
    if (funding_wallet.node == 0U ||
        funding_wallet.node > current_nodes.size() ||
        funding_wallet.funding_address.empty()) {
      throw std::logic_error(
          "masternode funding wallet identity is incomplete");
    }
    NodeRuntime& funding_node = current_nodes[funding_wallet.node - 1U];
    RequireNodeRunning(funding_node, "masternode.add funding wallet");
    if (!funding_node.config.wallet_enabled) {
      throw McpOperationFailure(
          "wallet_support_unavailable",
          "masternode.add funding node was started without wallet "
          "support: " +
              funding_node.config.id,
          false);
    }
    NodeRuntime& miner = current_nodes[running_miner_index()];
    std::vector<std::size_t> selected_indexes;
    selected_indexes.reserve(count);
    if (!requested_node_ids.empty()) {
      for (const std::string& node_id : requested_node_ids) {
        selected_indexes.push_back(find_node_index(node_id));
      }
    } else {
      for (std::size_t index = 0U;
           index < current_nodes.size() && selected_indexes.size() < count;
           ++index) {
        if (!NodeListContains(before_topology.masternode_nodes,
                              static_cast<std::uint32_t>(index)) &&
            !current_nodes[index].config.masternode &&
            current_nodes[index].AllowsChainMetrics() &&
            NodeProcessRunning(current_nodes[index])) {
          selected_indexes.push_back(index);
        }
      }
      if (selected_indexes.size() != count) {
        throw McpOperationFailure(
            "masternode_backing_node_unavailable",
            "masternode.add found fewer compatible running nodes than "
            "requested",
            false);
      }
    }
    std::vector<std::string> selected_node_ids;
    selected_node_ids.reserve(selected_indexes.size());
    {
      auto process_guard = context.run_process_state.Lock();
      for (const std::size_t index : selected_indexes) {
        if (NodeListContains(before_topology.masternode_nodes,
                             static_cast<std::uint32_t>(index)) ||
            current_nodes[index].config.masternode) {
          throw McpOperationFailure(
              "masternode_already_configured",
              "masternode.add node is already a masternode: " +
                  current_nodes[index].config.id,
              false);
        }
        RequireNodeRunning(current_nodes[index], process_guard,
                           "masternode.add");
        selected_node_ids.push_back(current_nodes[index].config.id);
      }
    }
    const ChainMasternodeFundingRequirements requirements =
        context.driver.MasternodeFundingRequirements(count);
    const auto operation_timeout = std::chrono::seconds(timeout_sec);
    static_cast<void>(PrepareMasternodeFunding(
        context.driver, context.block_generation_mutex, miner, funding_node,
        funding_wallet.funding_address, active_nodes, requirements,
        operation_timeout, bounded_stop_token));

    const auto completion_deadline =
        std::chrono::steady_clock::now() +
        std::chrono::seconds(std::max<std::uint32_t>(60U, timeout_sec));
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
    std::vector<ChainMasternodeRegistration> registrations;
    std::vector<MasternodeIdentity> added_masternodes;
    std::vector<bool> scheduled_miner_was_active(selected_indexes.size(),
                                                 false);
    registrations.reserve(selected_indexes.size());
    added_masternodes.reserve(selected_indexes.size());
    const auto rollback_registration = [&] {
      const auto rollback_deadline =
          std::chrono::steady_clock::now() + std::chrono::seconds(60);
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
      const auto resume_scheduled_miners = [&] {
        if (context.block_scheduler == nullptr) {
          return;
        }
        for (std::size_t offset = 0U; offset < selected_indexes.size();
             ++offset) {
          if (!scheduled_miner_was_active[offset]) {
            continue;
          }
          NodeRuntime& node = current_nodes[selected_indexes[offset]];
          if (!NodeProcessRunning(node)) {
            continue;
          }
          context.block_scheduler->StartMiner(node.config.id);
          scheduled_miner_was_active[offset] = false;
        }
      };
      try {
        for (std::size_t offset = 0U; offset < selected_indexes.size();
             ++offset) {
          const std::size_t index = selected_indexes[offset];
          NodeRuntime& node = current_nodes[index];
          if (!node.config.masternode) {
            continue;
          }
          const bool stopped_miner =
              context.block_scheduler != nullptr &&
              NodeListContains(before_topology.miner_nodes,
                               static_cast<std::uint32_t>(index)) &&
              context.block_scheduler->StopMiner(node.config.id);
          scheduled_miner_was_active[offset] =
              scheduled_miner_was_active[offset] || stopped_miner;
          node.config.masternode.reset();
          if (!RestartNode(context.options, context.events_path, context.driver,
                           *context.peer_connectivity_controller, node,
                           context.lifecycle_epoch, context.start_node,
                           rollback_stop_token, "masternode_add_rollback")) {
            throw std::runtime_error(
                "masternode rollback reached node stop_time: " +
                node.config.id);
          }
        }

        NodeRuntime& rollback_miner = current_nodes[running_miner_index()];
        std::vector<MasternodeTransactionConfirmation>
            registration_transactions;
        registration_transactions.reserve(registrations.size());
        for (const ChainMasternodeRegistration& registration : registrations) {
          registration_transactions.push_back(MasternodeTransactionConfirmation{
              .funding_wallet = &funding_node,
              .transaction_id = registration.pro_tx_hash,
          });
        }
        ConfirmMasternodeTransactions(
            context.driver, context.block_generation_mutex, rollback_miner,
            funding_wallet.funding_address, active_nodes,
            registration_transactions,
            requirements.registration_confirmation_blocks,
            std::chrono::seconds(60), rollback_stop_token);

        std::vector<MasternodeTransactionConfirmation> revocations;
        revocations.reserve(registrations.size());
        for (const ChainMasternodeRegistration& registration : registrations) {
          revocations.push_back(MasternodeTransactionConfirmation{
              .funding_wallet = &funding_node,
              .transaction_id = context.driver.RevokeMasternode(
                  funding_node.config, registration.pro_tx_hash,
                  registration.operator_secret_key,
                  funding_wallet.funding_address, rollback_stop_token),
          });
        }
        ConfirmMasternodeTransactions(
            context.driver, context.block_generation_mutex, rollback_miner,
            funding_wallet.funding_address, active_nodes, revocations,
            requirements.revocation_confirmation_blocks,
            std::chrono::seconds(60), rollback_stop_token);
      } catch (...) {
        const std::exception_ptr rollback_failure = std::current_exception();
        try {
          resume_scheduled_miners();
        } catch (...) {
          throw std::runtime_error(
              "masternode rollback failed: " +
              context.exception_message(rollback_failure) +
              "; scheduled miner restoration failed: " +
              context.exception_message(std::current_exception()));
        }
        std::rethrow_exception(rollback_failure);
      }
      resume_scheduled_miners();
      const RuntimeWalletSnapshot restored =
          context.runtime_wallet_registry.Snapshot();
      if (restored.generation() != before_roles.generation() ||
          restored.masternodes().size() != before_roles.masternodes().size()) {
        throw std::runtime_error(
            "masternode.add rollback registry read-back changed");
      }
    };
    try {
      for (const std::size_t index : selected_indexes) {
        ThrowIfStopRequested(bounded_stop_token);
        NodeRuntime& node = current_nodes[index];
        const std::string service =
            node.config.p2p_host + ":" + std::to_string(node.config.p2p_port);
        registrations.push_back(context.driver.RegisterMasternode(
            funding_node.config, service, funding_wallet.funding_address,
            completion_stop_token));
        ThrowIfStopRequested(bounded_stop_token);
      }
      std::vector<MasternodeTransactionConfirmation> registration_transactions;
      registration_transactions.reserve(registrations.size());
      for (const ChainMasternodeRegistration& registration : registrations) {
        registration_transactions.push_back(MasternodeTransactionConfirmation{
            .funding_wallet = &funding_node,
            .transaction_id = registration.pro_tx_hash,
        });
      }
      ConfirmMasternodeTransactions(
          context.driver, context.block_generation_mutex, miner,
          funding_wallet.funding_address, active_nodes,
          registration_transactions,
          requirements.registration_confirmation_blocks, operation_timeout,
          completion_stop_token);
      ThrowIfStopRequested(bounded_stop_token);
      for (std::size_t offset = 0U; offset < selected_indexes.size();
           ++offset) {
        const std::size_t index = selected_indexes[offset];
        NodeRuntime& node = current_nodes[index];
        const ChainMasternodeRegistration& registration = registrations[offset];
        const bool resume_miner =
            context.block_scheduler != nullptr &&
            NodeListContains(before_topology.miner_nodes,
                             static_cast<std::uint32_t>(index)) &&
            context.block_scheduler->StopMiner(node.config.id);
        scheduled_miner_was_active[offset] = resume_miner;
        node.config.masternode = ChainNodeConfig::MasternodeProcessConfig{
            .operator_secret_key = registration.operator_secret_key,
            .service = registration.service,
        };
        try {
          if (!RestartNode(context.options, context.events_path, context.driver,
                           *context.peer_connectivity_controller, node,
                           context.lifecycle_epoch, context.start_node,
                           completion_stop_token, "masternode_add")) {
            throw std::runtime_error(
                "masternode.add target reached stop_time during "
                "restart: " +
                node.config.id);
          }
          const ChainMasternodeStatus status =
              context.driver.WaitForMasternodeReady(
                  node.config, registration.pro_tx_hash, operation_timeout,
                  completion_stop_token);
          added_masternodes.push_back(RegisteredMasternodeIdentity(
              static_cast<std::uint32_t>(index + 1U), node.config.id,
              funding_wallet_id, registration, status));
        } catch (...) {
          if (resume_miner && NodeProcessRunning(node)) {
            context.block_scheduler->StartMiner(node.config.id);
          }
          throw;
        }
        if (resume_miner) {
          context.block_scheduler->StartMiner(node.config.id);
        }
        ThrowIfStopRequested(bounded_stop_token);
      }
    } catch (...) {
      const std::exception_ptr failure = std::current_exception();
      bool registration_outcome_unknown = false;
      try {
        std::rethrow_exception(failure);
      } catch (const ChainMasternodeOutcomeUnknown&) {
        registration_outcome_unknown = true;
      } catch (...) {
      }
      if (!registrations.empty()) {
        try {
          rollback_registration();
        } catch (...) {
          context.mcp_application.MarkRunStopping();
          context.request_simulation_stop();
          throw McpOperationFailure(
              "masternode_add_outcome_unconfirmed",
              "masternode.add failed after registration: " +
                  context.exception_message(failure) +
                  "; rollback could not be verified: " +
                  context.exception_message(std::current_exception()),
              false);
        }
      }
      if (registration_outcome_unknown) {
        context.mcp_application.MarkRunStopping();
        context.request_simulation_stop();
        throw McpOperationFailure("masternode_add_outcome_unconfirmed",
                                  context.exception_message(failure), false);
      }
      if (role_control.CommitPhase() ==
          SimulationCommandCommitPhase::kCancelled) {
        throw SimulationCancelled();
      }
      std::rethrow_exception(failure);
    }

    RuntimeWalletSnapshot published_roles;
    try {
      std::unique_lock<std::timed_mutex> publication_lock =
          context.acquire_runtime_publication_lock(bounded_stop_token);
      ThrowIfStopRequested(bounded_stop_token);
      RuntimeWalletRegistry::PreparedAppend prepared_roles =
          context.runtime_wallet_registry.PrepareUpdate(
              before_roles.generation(), {}, {}, added_masternodes,
              static_cast<std::uint32_t>(current_nodes.size()));
      if (!role_control.TryBeginCommit()) {
        throw SimulationCancelled();
      }
      published_roles = prepared_roles.Commit();
      role_control.MarkCommitted();
    } catch (...) {
      const std::exception_ptr failure = std::current_exception();
      try {
        rollback_registration();
      } catch (...) {
        context.mcp_application.MarkRunStopping();
        context.request_simulation_stop();
        throw McpOperationFailure(
            "masternode_add_outcome_unconfirmed",
            "masternode.add could not publish after registration: " +
                context.exception_message(failure) +
                "; rollback could not be verified: " +
                context.exception_message(std::current_exception()),
            false);
      }
      if (role_control.CommitPhase() ==
          SimulationCommandCommitPhase::kCancelled) {
        throw SimulationCancelled();
      }
      std::rethrow_exception(failure);
    }
    try {
      WriteEvent(context.events_path, context.options.run_id, "sim",
                 SimulationEventKind::kRuntimeRoleGenerationPublished,
                 boost::json::serialize(RuntimeRoleGenerationDetail(
                     published_roles, current_nodes)));
      return boost::json::object{
          {"node_ids", node_ids_json(selected_node_ids)},
          {"assigned_roles", boost::json::array{"masternode"}},
          {"removed_roles", boost::json::array{}},
          {"action", action},
          {"state", "ready"},
          {"created_node_ids", boost::json::array{}},
          {"role_generation", published_roles.generation()},
          {"final_masternode_count", published_roles.masternodes().size()},
          {"masternodes", public_identities_json(added_masternodes)},
          {"inventory_generation", current_nodes.generation()},
          {"final_node_count", current_nodes.size()},
          {"node_capacity", current_nodes.capacity()},
          {"network_allocation",
           current_nodes.network_address_plan()
               ? boost::json::value(
                     current_nodes.network_address_plan()->ToSerialized())
               : boost::json::value(nullptr)},
      };
    } catch (...) {
      context.mcp_application.MarkRunStopping();
      context.request_simulation_stop();
      throw McpOperationFailure(
          "masternode_add_outcome_unconfirmed",
          "masternode.add published but completion evidence failed: " +
              context.exception_message(std::current_exception()),
          false);
    }
  }

  std::vector<MasternodeIdentity> selected_masternodes;
  std::vector<std::size_t> selected_indexes;
  selected_masternodes.reserve(requested_node_ids.size());
  selected_indexes.reserve(requested_node_ids.size());
  for (const std::string& node_id : requested_node_ids) {
    const auto identity = std::find_if(
        before_roles.masternodes().begin(), before_roles.masternodes().end(),
        [&](const MasternodeIdentity& masternode) {
          return masternode.node_id == node_id;
        });
    if (identity == before_roles.masternodes().end()) {
      throw McpOperationFailure(
          "masternode_not_found",
          action + " node has no registered masternode role: " + node_id,
          false);
    }
    const std::size_t index = find_node_index(node_id);
    if (!current_nodes[index].config.masternode ||
        current_nodes[index].config.masternode->operator_secret_key !=
            identity->operator_secret_key ||
        current_nodes[index].config.masternode->service != identity->service) {
      throw McpOperationFailure(
          "masternode_configuration_mismatch",
          action +
              " process configuration does not match the registered "
              "masternode identity: " +
              node_id,
          false);
    }
    selected_masternodes.push_back(*identity);
    selected_indexes.push_back(index);
  }

  if (removing) {
    const std::size_t miner_index = running_miner_index();
    NodeRuntime& miner = current_nodes[miner_index];
    const ChainMasternodeFundingRequirements requirements =
        context.driver.MasternodeFundingRequirements(
            static_cast<std::uint32_t>(selected_masternodes.size()));
    const auto completion_deadline =
        std::chrono::steady_clock::now() +
        std::chrono::seconds(std::max<std::uint32_t>(60U, timeout_sec));
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
    std::vector<MasternodeTransactionConfirmation> revocations;
    revocations.reserve(selected_masternodes.size());
    bool revocation_attempted = false;
    try {
      ThrowIfStopRequested(bounded_stop_token);
      for (const MasternodeIdentity& masternode : selected_masternodes) {
        const WalletIdentity& wallet =
            find_wallet(masternode.funding_wallet_node_id);
        NodeRuntime& funding_node = current_nodes.at(wallet.node - 1U);
        RequireNodeRunning(funding_node, "masternode.remove funding");
        revocation_attempted = true;
        revocations.push_back(MasternodeTransactionConfirmation{
            .funding_wallet = &funding_node,
            .transaction_id = context.driver.RevokeMasternode(
                funding_node.config, masternode.pro_tx_hash,
                masternode.operator_secret_key, wallet.funding_address,
                completion_stop_token),
        });
      }
      const WalletIdentity& confirmation_wallet =
          find_wallet(selected_masternodes.front().funding_wallet_node_id);
      ConfirmMasternodeTransactions(
          context.driver, context.block_generation_mutex, miner,
          confirmation_wallet.funding_address, active_nodes, revocations,
          requirements.revocation_confirmation_blocks,
          std::chrono::seconds(timeout_sec), completion_stop_token);
      for (const std::size_t index : selected_indexes) {
        NodeRuntime& node = current_nodes[index];
        const bool resume_miner =
            context.block_scheduler != nullptr &&
            NodeListContains(before_topology.miner_nodes,
                             static_cast<std::uint32_t>(index)) &&
            context.block_scheduler->StopMiner(node.config.id);
        node.config.masternode.reset();
        try {
          if (!RestartNode(context.options, context.events_path, context.driver,
                           *context.peer_connectivity_controller, node,
                           context.lifecycle_epoch, context.start_node,
                           completion_stop_token, "masternode_remove")) {
            throw std::runtime_error(
                "masternode.remove target reached stop_time during "
                "restart: " +
                node.config.id);
          }
        } catch (...) {
          if (resume_miner && NodeProcessRunning(node)) {
            context.block_scheduler->StartMiner(node.config.id);
          }
          throw;
        }
        if (resume_miner) {
          context.block_scheduler->StartMiner(node.config.id);
        }
      }
      SimulationRegistry next_registry = before_roles.registry();
      std::vector<std::uint32_t> removed_indexes;
      removed_indexes.reserve(selected_indexes.size());
      for (const std::size_t index : selected_indexes) {
        removed_indexes.push_back(static_cast<std::uint32_t>(index));
      }
      next_registry.RemoveMasternodeNodes(removed_indexes);
      std::unique_lock<std::timed_mutex> publication_lock =
          context.acquire_runtime_publication_lock(completion_stop_token);
      RuntimeWalletRegistry::PreparedAppend prepared_roles =
          context.runtime_wallet_registry.PrepareReplace(
              before_roles.generation(), std::move(next_registry));
      const RuntimeWalletSnapshot published_roles = prepared_roles.Commit();
      for (MasternodeIdentity& masternode : selected_masternodes) {
        masternode.state = "REVOKED";
        masternode.status = "revoked";
      }
      WriteEvent(context.events_path, context.options.run_id, "sim",
                 SimulationEventKind::kRuntimeRoleGenerationPublished,
                 boost::json::serialize(RuntimeRoleGenerationDetail(
                     published_roles, current_nodes)));
      return boost::json::object{
          {"node_ids", node_ids_json(requested_node_ids)},
          {"assigned_roles", boost::json::array{}},
          {"removed_roles", boost::json::array{"masternode"}},
          {"action", action},
          {"state", "removed"},
          {"created_node_ids", boost::json::array{}},
          {"role_generation", published_roles.generation()},
          {"final_masternode_count", published_roles.masternodes().size()},
          {"masternodes", public_identities_json(selected_masternodes)},
          {"inventory_generation", current_nodes.generation()},
          {"final_node_count", current_nodes.size()},
          {"node_capacity", current_nodes.capacity()},
          {"network_allocation",
           current_nodes.network_address_plan()
               ? boost::json::value(
                     current_nodes.network_address_plan()->ToSerialized())
               : boost::json::value(nullptr)},
      };
    } catch (...) {
      const std::exception_ptr failure = std::current_exception();
      if (revocation_attempted) {
        context.mcp_application.MarkRunStopping();
        context.request_simulation_stop();
        throw McpOperationFailure(
            "masternode_remove_outcome_unconfirmed",
            "masternode.remove failed after revocation began: " +
                context.exception_message(failure),
            false);
      }
      std::rethrow_exception(failure);
    }
  }

  const auto completion_deadline =
      std::chrono::steady_clock::now() +
      std::chrono::seconds(std::max<std::uint32_t>(60U, timeout_sec));
  std::stop_source completion_stop_source;
  std::jthread completion_timer([completion_deadline, &completion_stop_source](
                                    std::stop_token timer_stop_token) {
    try {
      WaitUntil(completion_deadline, timer_stop_token);
    } catch (const SimulationCancelled&) {
      return;
    }
    completion_stop_source.request_stop();
  });
  const std::stop_token completion_stop_token =
      completion_stop_source.get_token();
  std::vector<MasternodeIdentity> restarted_masternodes;
  restarted_masternodes.reserve(selected_indexes.size());
  bool restart_mutation_started = false;
  try {
    ThrowIfStopRequested(bounded_stop_token);
    for (std::size_t offset = 0U; offset < selected_indexes.size(); ++offset) {
      const std::size_t index = selected_indexes[offset];
      NodeRuntime& node = current_nodes[index];
      const bool resume_miner =
          context.block_scheduler != nullptr &&
          NodeListContains(before_topology.miner_nodes,
                           static_cast<std::uint32_t>(index)) &&
          context.block_scheduler->StopMiner(node.config.id);
      NodeRestartAdmission restart_admission;
      try {
        if (!RestartNode(context.options, context.events_path, context.driver,
                         *context.peer_connectivity_controller, node,
                         context.lifecycle_epoch, context.start_node,
                         completion_stop_token, "masternode_restart", nullptr,
                         &restart_admission)) {
          throw std::runtime_error(
              "masternode.restart target reached stop_time during "
              "restart: " +
              node.config.id);
        }
        restart_mutation_started =
            restart_mutation_started || restart_admission.admitted;
        const ChainMasternodeStatus status =
            context.driver.WaitForMasternodeReady(
                node.config, selected_masternodes[offset].pro_tx_hash,
                std::chrono::seconds(timeout_sec), completion_stop_token);
        if (status.collateral_hash !=
                selected_masternodes[offset].collateral_hash ||
            status.collateral_index !=
                selected_masternodes[offset].collateral_index) {
          throw std::runtime_error(
              "masternode.restart readiness returned a different "
              "collateral identity");
        }
        MasternodeIdentity current = selected_masternodes[offset];
        current.state = status.state;
        current.status = status.status;
        restarted_masternodes.push_back(std::move(current));
      } catch (...) {
        restart_mutation_started =
            restart_mutation_started || restart_admission.admitted;
        if (resume_miner && NodeProcessRunning(node)) {
          context.block_scheduler->StartMiner(node.config.id);
        }
        throw;
      }
      if (resume_miner) {
        context.block_scheduler->StartMiner(node.config.id);
      }
    }
    return boost::json::object{
        {"node_ids", node_ids_json(requested_node_ids)},
        {"assigned_roles", boost::json::array{}},
        {"removed_roles", boost::json::array{}},
        {"action", action},
        {"state", "ready"},
        {"created_node_ids", boost::json::array{}},
        {"role_generation", before_roles.generation()},
        {"final_masternode_count", before_roles.masternodes().size()},
        {"masternodes", public_identities_json(restarted_masternodes)},
        {"inventory_generation", current_nodes.generation()},
        {"final_node_count", current_nodes.size()},
        {"node_capacity", current_nodes.capacity()},
        {"network_allocation",
         current_nodes.network_address_plan()
             ? boost::json::value(
                   current_nodes.network_address_plan()->ToSerialized())
             : boost::json::value(nullptr)},
    };
  } catch (...) {
    if (!restart_mutation_started) {
      throw;
    }
    const std::exception_ptr failure = std::current_exception();
    context.mcp_application.MarkRunStopping();
    context.request_simulation_stop();
    throw McpOperationFailure(
        "masternode_restart_outcome_unconfirmed",
        "masternode.restart failed after process mutation began: " +
            context.exception_message(failure),
        false);
  }
}

}  // namespace bbp::simulator_app_internal
