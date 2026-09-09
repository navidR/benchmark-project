#include "simulator_live_wallet_addition.h"

#include <algorithm>
#include <array>
#include <atomic>
#include <boost/json/array.hpp>
#include <boost/json/serialize.hpp>
#include <chrono>
#include <cstdint>
#include <exception>
#include <iterator>
#include <limits>
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

#include "bbp/drivers/chain_driver.h"
#include "bbp/mcp_live_application.h"
#include "bbp/mcp_operation_service.h"
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
#include "simulator_node_process_state.h"
#include "simulator_runtime_identity_details.h"
#include "simulator_runtime_node_addition.h"
#include "simulator_scenario_identifier.h"
#include "simulator_scenario_node_resolution.h"
#include "simulator_wallet_transaction_validation.h"

namespace bbp::simulator_app_internal {

boost::json::object AddLiveWalletRoles(const LiveWalletAdditionContext& context,
                                       const boost::json::object& arguments,
                                       std::stop_token operation_stop_token) {
  constexpr std::array<std::string_view, 8U> kAllowedFields = {
      "run_id",
      "node_id",
      "node_ids",
      "count",
      "mode",
      "create_node",
      "readiness_confirmations",
      "timeout_sec"};
  RejectUnsupportedFields(arguments, kAllowedFields, "wallet.add");
  const boost::json::value* create_node = arguments.if_contains("create_node");
  if (create_node != nullptr && !create_node->is_object()) {
    throw std::invalid_argument("wallet.add create_node must be an object");
  }
  const std::uint32_t count = JsonOptionalUint32Field(arguments, "count", 0U);
  if (count == 0U || count > kSimulationNodeAddMaximumCount) {
    throw std::invalid_argument("wallet.add count must be in 1..16");
  }
  const std::string mode_name =
      JsonOptionalStringField(arguments, "mode", std::string_view());
  const std::optional<WalletPrivacyMode> requested_mode =
      WalletPrivacyModeFromName(mode_name);
  if (!requested_mode) {
    throw std::invalid_argument("wallet.add mode must be public or private");
  }
  const std::uint64_t readiness_confirmations =
      JsonOptionalUint64Field(arguments, "readiness_confirmations", 0U);
  if (readiness_confirmations != 0U) {
    throw McpOperationFailure(
        "wallet_funding_policy_required",
        "wallet.add readiness_confirmations requires a funding policy", false);
  }
  const std::uint32_t timeout_sec =
      JsonOptionalUint32Field(arguments, "timeout_sec", 30U);
  if (timeout_sec == 0U || timeout_sec > 3600U) {
    throw std::invalid_argument("wallet.add timeout_sec must be in 1..3600");
  }
  std::optional<std::string> requested_node_id;
  if (arguments.if_contains("node_id") != nullptr) {
    requested_node_id =
        JsonOptionalStringField(arguments, "node_id", std::string_view());
    RequireSafeScenarioIdentifier(*requested_node_id, "wallet.add node_id");
    if (count != 1U) {
      throw std::invalid_argument("wallet.add with node_id requires count=1");
    }
  }
  std::vector<std::string> requested_node_ids;
  if (const boost::json::value* node_ids = arguments.if_contains("node_ids")) {
    if (!node_ids->is_array() || node_ids->as_array().empty() ||
        node_ids->as_array().size() > kSimulationNodeAddMaximumCount) {
      throw std::invalid_argument("wallet.add node_ids must contain 1..16 ids");
    }
    std::set<std::string> unique_node_ids;
    requested_node_ids.reserve(node_ids->as_array().size());
    for (const boost::json::value& node_id : node_ids->as_array()) {
      if (!node_id.is_string()) {
        throw std::invalid_argument("wallet.add node_ids must contain strings");
      }
      std::string id(node_id.as_string());
      RequireSafeScenarioIdentifier(id, "wallet.add node_ids");
      if (!unique_node_ids.insert(id).second) {
        throw std::invalid_argument("wallet.add node_ids must be unique");
      }
      requested_node_ids.push_back(std::move(id));
    }
    if (requested_node_ids.size() != count) {
      throw std::invalid_argument("wallet.add count must match node_ids size");
    }
  }
  if (requested_node_id && !requested_node_ids.empty()) {
    throw std::invalid_argument(
        "wallet.add node_id and node_ids are mutually exclusive");
  }
  if ((requested_node_id || !requested_node_ids.empty()) &&
      create_node != nullptr) {
    throw std::invalid_argument(
        "wallet.add explicit node ids and create_node are mutually "
        "exclusive");
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
  const RuntimeWalletSnapshot before_wallets =
      context.runtime_wallet_registry.Snapshot();
  const WalletInitialization initialization =
      before_wallets.registry().wallet_initialization();
  if (*requested_mode != initialization.mode) {
    throw McpOperationFailure(
        "wallet_mode_conflict",
        "wallet.add mode must match the active run wallet mode", false);
  }

  if (create_node != nullptr) {
    Options validation_options = context.options;
    validation_options.nodes = static_cast<std::uint32_t>(current_nodes.size());
    SimulationNodeAddRequest node_request;
    try {
      node_request = ParseAndValidateSimulationNodeAddRequest(
          create_node->as_object(), validation_options);
    } catch (const std::runtime_error& error) {
      if (std::string_view(error.what()) !=
          "node.add request exceeds the configured node capacity") {
        throw;
      }
      throw McpOperationFailure(
          "node_capacity_exceeded", error.what(), false,
          boost::json::array{boost::json::object{
              {"code", "node_capacity_exceeded"},
              {"message",
               "the requested wallet-node batch exceeds available "
               "capacity"},
              {"path", "create_node.count"},
              {"requested_count", count},
              {"current_node_count", current_nodes.size()},
              {"node_capacity", context.node_inventory.capacity()},
              {"available_node_capacity",
               current_nodes.size() <= context.node_inventory.capacity()
                   ? context.node_inventory.capacity() - current_nodes.size()
                   : 0U},
              {"recoverable", false}}});
    }
    if (node_request.count != count) {
      throw std::invalid_argument(
          "wallet.add count must match create_node.count");
    }

    SimulationCommandControl node_add_control;
    node_add_control.absolute_deadline = deadline;
    std::stop_callback cancel_node_add(bounded_stop_token, [&] {
      static_cast<void>(node_add_control.RequestCancellation(
          deadline_expired.load(std::memory_order_acquire)
              ? SimulationCommandCancellationCause::kDeadline
              : SimulationCommandCancellationCause::kClientCancel));
    });
    const auto cancellation_won = [&] {
      return node_add_control.CommitPhase() ==
             SimulationCommandCommitPhase::kCancelled;
    };
    RuntimeNodeAddResult added;
    try {
      std::lock_guard<std::mutex> topology_lock(context.runtime_topology_mutex);
      added = AddRuntimeNodesTransactional(
          context.options, context.run_root, context.events_path,
          context.chain_spec, context.driver, context.node_inventory,
          context.runtime_wallet_registry, RuntimeNodeAdditionRole::kWallet,
          context.block_scheduler.get(), &context.miner_node_ids,
          &context.configured_miner_node_ids_mutex, nullptr,
          *context.peer_connectivity_controller, &context.runtime_topology,
          &context.live_topology_config, context.run_process_state,
          context.lifecycle_epoch, node_request, &node_add_control,
          bounded_stop_token, context.runtime_node_addition_dependencies);
    } catch (const SimulationNodeResourceUnavailable& error) {
      if (cancellation_won()) {
        throw SimulationCancelled();
      }
      const SimulationNodeResourceFailure& failure = error.failure();
      throw McpOperationFailure(
          "node_resource_unavailable", error.what(), true,
          boost::json::array{boost::json::object{
              {"code", "node_resource_unavailable"},
              {"message", error.what()},
              {"path", "create_node"},
              {"resource_kind", failure.resource_kind},
              {"node_id", failure.node_id},
              {"address", failure.address},
              {"port", failure.port},
              {"purpose", failure.purpose},
              {"mutation_started", failure.mutation_started},
              {"action", "wallet.add"},
              {"recoverable", true}}});
    } catch (const SimulationCommandOutcomeUnconfirmed& error) {
      context.mcp_application.MarkRunStopping();
      context.request_simulation_stop();
      throw McpOperationFailure(
          "wallet_add_outcome_unconfirmed",
          "wallet.add create_node outcome is unconfirmed: " +
              std::string(error.what()),
          false);
    } catch (const std::exception&) {
      if (cancellation_won()) {
        throw SimulationCancelled();
      }
      throw;
    } catch (...) {
      if (cancellation_won()) {
        throw SimulationCancelled();
      }
      throw;
    }

    try {
      if (!added.wallet_generation || !added.final_wallet_count ||
          !added.final_wallet_node_count ||
          added.added_wallets.size() != count ||
          added.added_node_ids.size() != count) {
        throw std::logic_error(
            "wallet.add create_node omitted its joint publication "
            "evidence");
      }
      boost::json::array added_node_ids;
      boost::json::array affected_node_ids;
      boost::json::array wallets_json;
      added_node_ids.reserve(added.added_node_ids.size());
      affected_node_ids.reserve(added.added_node_ids.size());
      wallets_json.reserve(added.added_wallets.size());
      for (const std::string& node_id : added.added_node_ids) {
        added_node_ids.emplace_back(node_id);
        affected_node_ids.emplace_back(node_id);
      }
      for (const WalletIdentity& wallet : added.added_wallets) {
        wallets_json.push_back(
            RuntimeWalletIdentityJson(wallet, initialization));
      }
      return boost::json::object{
          {"added_node_ids", std::move(added_node_ids)},
          {"removed_node_ids", boost::json::array{}},
          {"affected_node_ids", std::move(affected_node_ids)},
          {"action", "wallet.add"},
          {"state", "ready"},
          {"unchanged", false},
          {"wallets", std::move(wallets_json)},
          {"inventory_generation", added.inventory_generation},
          {"final_node_count", added.final_node_count},
          {"wallet_generation", *added.wallet_generation},
          {"final_wallet_count", *added.final_wallet_count},
          {"final_wallet_node_count", *added.final_wallet_node_count},
      };
    } catch (...) {
      context.mcp_application.MarkRunStopping();
      context.request_simulation_stop();
      throw McpOperationFailure(
          "wallet_add_outcome_unconfirmed",
          "wallet.add create_node published but completion evidence "
          "failed: " +
              context.exception_message(std::current_exception()),
          false);
    }
  }

  SimulationCommandControl role_control;
  role_control.absolute_deadline = deadline;
  std::stop_callback cancel_role_publication(bounded_stop_token, [&] {
    static_cast<void>(role_control.RequestCancellation(
        deadline_expired.load(std::memory_order_acquire)
            ? SimulationCommandCancellationCause::kDeadline
            : SimulationCommandCancellationCause::kClientCancel));
  });
  const auto is_registered_wallet = [&](std::string_view node_id) {
    return std::any_of(before_wallets.wallets().begin(),
                       before_wallets.wallets().end(),
                       [&](const WalletIdentity& wallet) {
                         return wallet.node_id == node_id;
                       });
  };
  const auto has_forbidden_miner_overlap = [&](std::size_t index) {
    const NodeRoleTopology& topology = before_wallets.registry().topology();
    return !topology.allow_miner_wallet_overlap &&
           NodeListContains(topology.miner_nodes,
                            static_cast<std::uint32_t>(index));
  };
  std::vector<std::size_t> selected_indexes;
  selected_indexes.reserve(count);
  if (!requested_node_ids.empty()) {
    for (const std::string& requested : requested_node_ids) {
      const auto selected = std::find_if(
          current_nodes.begin(), current_nodes.end(),
          [&](const NodeRuntime& node) { return node.config.id == requested; });
      if (selected == current_nodes.end()) {
        throw McpOperationFailure(
            "node_not_found",
            "wallet.add backing node is not active: " + requested, false);
      }
      selected_indexes.push_back(static_cast<std::size_t>(
          std::distance(current_nodes.begin(), selected)));
    }
  } else if (requested_node_id) {
    const auto selected =
        std::find_if(current_nodes.begin(), current_nodes.end(),
                     [&](const NodeRuntime& node) {
                       return node.config.id == *requested_node_id;
                     });
    if (selected == current_nodes.end()) {
      throw McpOperationFailure(
          "node_not_found",
          "wallet.add backing node is not active: " + *requested_node_id,
          false);
    }
    selected_indexes.push_back(static_cast<std::size_t>(
        std::distance(current_nodes.begin(), selected)));
  } else {
    for (std::size_t index = 0U;
         index < current_nodes.size() && selected_indexes.size() < count;
         ++index) {
      if (current_nodes[index].config.wallet_enabled &&
          !has_forbidden_miner_overlap(index)) {
        selected_indexes.push_back(index);
      }
    }
    if (selected_indexes.size() != count) {
      throw McpOperationFailure(
          "wallet_backing_node_unavailable",
          "wallet.add found fewer wallet-capable nodes than requested; "
          "use create_node after wallet-enabled node creation is "
          "available",
          false);
    }
  }

  {
    auto process_guard = context.run_process_state.Lock();
    for (const std::size_t index : selected_indexes) {
      NodeRuntime& node = current_nodes[index];
      if (!requested_node_ids.empty() && is_registered_wallet(node.config.id)) {
        throw McpOperationFailure(
            "wallet_already_configured",
            "wallet.add node is already a wallet: " + node.config.id, false);
      }
      if (has_forbidden_miner_overlap(index)) {
        throw McpOperationFailure(
            "role_conflict",
            "wallet.add cannot overlap the selected miner role: " +
                node.config.id,
            false);
      }
      if (!node.config.wallet_enabled) {
        throw McpOperationFailure(
            "wallet_support_unavailable",
            "wallet.add backing node was started without wallet "
            "support: " +
                node.config.id,
            false);
      }
      RequireNodeRunning(node, process_guard, "wallet.add");
    }
  }

  if (before_wallets.wallets().size() >
      std::numeric_limits<std::uint32_t>::max() - count) {
    throw std::overflow_error("wallet.add wallet index exceeds uint32");
  }
  std::vector<WalletIdentity> added_wallets;
  added_wallets.reserve(count);
  for (std::size_t offset = 0U; offset < selected_indexes.size(); ++offset) {
    ThrowIfStopRequested(bounded_stop_token);
    NodeRuntime& node = current_nodes[selected_indexes[offset]];
    WalletIdentity wallet{
        .wallet_index = static_cast<std::uint32_t>(
            before_wallets.wallets().size() + offset + 1U),
        .node = static_cast<std::uint32_t>(selected_indexes[offset] + 1U),
        .node_id = node.config.id,
        .address = {},
        .funding_address = {},
    };
    WriteEvent(context.events_path, context.options.run_id, node.config.id,
               SimulationEventKind::kWalletAddressRequested,
               WalletAddressDetail(wallet, initialization));
    wallet.address = context.driver.CreateWalletAddress(
        node.config, ToChainWalletMode(initialization), bounded_stop_token);
    if (wallet.address.empty()) {
      throw std::runtime_error(
          "wallet.add chain RPC returned an empty address");
    }
    wallet.funding_address = context.driver.CreateWalletFundingAddress(
        node.config, ToChainWalletMode(initialization), wallet.address,
        bounded_stop_token);
    if (wallet.funding_address.empty()) {
      throw std::runtime_error(
          "wallet.add chain RPC returned an empty funding address");
    }
    added_wallets.push_back(std::move(wallet));
  }

  std::unique_lock<std::timed_mutex> publication_lock =
      context.acquire_runtime_publication_lock(bounded_stop_token);
  ThrowIfStopRequested(bounded_stop_token);
  RuntimeWalletRegistry::PreparedAppend prepared =
      context.runtime_wallet_registry.PrepareAppend(
          before_wallets.generation(), added_wallets,
          static_cast<std::uint32_t>(current_nodes.size()));
  if (!role_control.TryBeginCommit()) {
    throw SimulationCancelled();
  }
  const RuntimeWalletSnapshot published = prepared.Commit();
  role_control.MarkCommitted();
  try {
    for (const WalletIdentity& wallet : added_wallets) {
      WriteEvent(context.events_path, context.options.run_id, wallet.node_id,
                 SimulationEventKind::kWalletAddressCreated,
                 WalletAddressDetail(wallet, initialization));
    }
    WriteEvent(context.events_path, context.options.run_id, "sim",
               SimulationEventKind::kRuntimeWalletGenerationPublished,
               boost::json::serialize(
                   RuntimeWalletGenerationDetail(published, added_wallets)));
    WriteEvent(context.events_path, context.options.run_id, "sim",
               SimulationEventKind::kRuntimeRoleGenerationPublished,
               boost::json::serialize(
                   RuntimeRoleGenerationDetail(published, current_nodes)));

    boost::json::array affected_node_ids;
    boost::json::array wallets_json;
    affected_node_ids.reserve(added_wallets.size());
    wallets_json.reserve(added_wallets.size());
    for (const WalletIdentity& wallet : added_wallets) {
      affected_node_ids.emplace_back(wallet.node_id);
      wallets_json.push_back(RuntimeWalletIdentityJson(wallet, initialization));
    }
    return boost::json::object{
        {"added_node_ids", boost::json::array{}},
        {"removed_node_ids", boost::json::array{}},
        {"affected_node_ids", std::move(affected_node_ids)},
        {"action", "wallet.add"},
        {"state", "ready"},
        {"unchanged", false},
        {"wallets", std::move(wallets_json)},
        {"inventory_generation", current_nodes.generation()},
        {"final_node_count", current_nodes.size()},
        {"wallet_generation", published.generation()},
        {"final_wallet_count", published.wallets().size()},
        {"final_wallet_node_count",
         published.registry().topology().wallet_nodes.size()},
    };
  } catch (...) {
    context.mcp_application.MarkRunStopping();
    context.request_simulation_stop();
    throw McpOperationFailure(
        "wallet_add_outcome_unconfirmed",
        "wallet.add published but completion evidence failed: " +
            context.exception_message(std::current_exception()),
        false);
  }
}

}  // namespace bbp::simulator_app_internal
