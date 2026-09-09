#include "simulator_live_wallet_removal.h"

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
#include <set>
#include <span>
#include <stdexcept>
#include <stop_token>
#include <string>
#include <string_view>
#include <thread>
#include <utility>
#include <vector>

#include "bbp/mcp_live_application.h"
#include "bbp/mcp_operation_service.h"
#include "bbp/runtime_node_inventory.h"
#include "bbp/runtime_wallet_registry.h"
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
#include "simulator_live_workload_state.h"
#include "simulator_runtime_identity_details.h"
#include "simulator_scenario_identifier.h"
#include "simulator_scenario_node_resolution.h"

namespace bbp::simulator_app_internal {

boost::json::object RemoveLiveWalletRoles(
    const LiveWalletRemovalContext& context,
    const boost::json::object& arguments,
    std::stop_token operation_stop_token) {
  constexpr std::array<std::string_view, 4U> kAllowedFields = {
      "run_id", "node_id", "node_ids", "timeout_sec"};
  RejectUnsupportedFields(arguments, kAllowedFields, "wallet.remove");
  const bool has_node_id = arguments.if_contains("node_id") != nullptr;
  const bool has_node_ids = arguments.if_contains("node_ids") != nullptr;
  if (has_node_id == has_node_ids) {
    throw std::invalid_argument(
        "wallet.remove requires exactly one of node_id or node_ids");
  }
  std::vector<std::string> requested_node_ids;
  if (has_node_id) {
    requested_node_ids.push_back(
        JsonOptionalStringField(arguments, "node_id", std::string_view()));
    RequireSafeScenarioIdentifier(requested_node_ids.front(),
                                  "wallet.remove node_id");
  } else {
    const boost::json::value& node_ids = arguments.at("node_ids");
    if (!node_ids.is_array() || node_ids.as_array().empty() ||
        node_ids.as_array().size() > kSimulationNodeRemoveMaximumCount) {
      throw std::invalid_argument(
          "wallet.remove node_ids must contain 1..16 ids");
    }
    std::set<std::string> unique_node_ids;
    requested_node_ids.reserve(node_ids.as_array().size());
    for (const boost::json::value& node_id : node_ids.as_array()) {
      if (!node_id.is_string()) {
        throw std::invalid_argument(
            "wallet.remove node_ids must contain strings");
      }
      std::string id(node_id.as_string());
      RequireSafeScenarioIdentifier(id, "wallet.remove node_ids");
      if (!unique_node_ids.insert(id).second) {
        throw std::invalid_argument("wallet.remove node_ids must be unique");
      }
      requested_node_ids.push_back(std::move(id));
    }
  }
  const std::uint32_t timeout_sec =
      JsonOptionalUint32Field(arguments, "timeout_sec", 30U);
  if (timeout_sec == 0U || timeout_sec > 3600U) {
    throw std::invalid_argument("wallet.remove timeout_sec must be in 1..3600");
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
  SimulationCommandControl role_control;
  role_control.absolute_deadline = deadline;
  std::stop_callback cancel_role_publication(bounded_stop_token, [&] {
    static_cast<void>(role_control.RequestCancellation(
        deadline_expired.load(std::memory_order_acquire)
            ? SimulationCommandCancellationCause::kDeadline
            : SimulationCommandCancellationCause::kClientCancel));
  });
  ThrowIfStopRequested(bounded_stop_token);
  auto mutation_lock = context.acquire_node_mutation_lock(
      context.node_mutation_mutex, bounded_stop_token);
  const RuntimeNodeSnapshot current_nodes = context.node_inventory.Snapshot();
  const RuntimeWalletSnapshot before_wallets =
      context.runtime_wallet_registry.Snapshot();
  std::vector<std::uint32_t> selected_role_indexes;
  selected_role_indexes.reserve(requested_node_ids.size());
  for (const std::string& requested_node_id : requested_node_ids) {
    const auto selected =
        std::find_if(current_nodes.begin(), current_nodes.end(),
                     [&](const NodeRuntime& node) {
                       return node.config.id == requested_node_id;
                     });
    if (selected == current_nodes.end()) {
      throw McpOperationFailure(
          "node_not_found",
          "wallet.remove node is not active: " + requested_node_id, false);
    }
    const std::uint32_t node_index = static_cast<std::uint32_t>(
        std::distance(current_nodes.begin(), selected));
    if (!NodeListContains(before_wallets.registry().topology().wallet_nodes,
                          node_index)) {
      throw McpOperationFailure(
          "wallet_not_found",
          "wallet.remove node has no registered wallet role: " +
              requested_node_id,
          false);
    }
    selected_role_indexes.push_back(node_index);
  }

  std::vector<WalletIdentity> removed_wallets;
  for (const std::string& requested_node_id : requested_node_ids) {
    const std::size_t previous_size = removed_wallets.size();
    for (const WalletIdentity& wallet : before_wallets.wallets()) {
      if (wallet.node_id == requested_node_id) {
        removed_wallets.push_back(wallet);
      }
    }
    if (removed_wallets.size() == previous_size) {
      throw std::logic_error(
          "wallet.remove role has no registered wallet identities: " +
          requested_node_id);
    }
  }
  for (const MasternodeIdentity& masternode : before_wallets.masternodes()) {
    if (std::find(requested_node_ids.begin(), requested_node_ids.end(),
                  masternode.funding_wallet_node_id) !=
        requested_node_ids.end()) {
      throw McpOperationFailure(
          "wallet_in_use",
          "wallet.remove requires removing funded masternode " +
              masternode.node_id + " first",
          false);
    }
  }

  const auto acquire_workload_lock =
      [&](std::mutex& mutex) -> std::unique_lock<std::mutex> {
    std::unique_lock<std::mutex> lock(mutex, std::defer_lock);
    while (!lock.try_lock()) {
      WaitForDuration(std::chrono::milliseconds(20), bounded_stop_token);
    }
    ThrowIfStopRequested(bounded_stop_token);
    return lock;
  };
  std::unique_lock<std::mutex> workloads_lock =
      acquire_workload_lock(context.wallet_workloads->mutex);
  for (const auto& [workload_id, record] : context.wallet_workloads->records) {
    std::unique_lock<std::mutex> record_lock =
        acquire_workload_lock(record->mutex);
    if (!IsTerminalLiveWalletWorkloadState(record->state)) {
      throw McpOperationFailure(
          "wallet_in_use",
          "wallet.remove requires every wallet workload to be terminal; "
          "active workload: " +
              workload_id,
          true);
    }
  }

  SimulationRegistry next_registry = before_wallets.registry();
  for (const std::uint32_t node_index : selected_role_indexes) {
    next_registry.RemoveWalletNode(node_index);
  }
  std::unique_lock<std::timed_mutex> publication_lock =
      context.acquire_runtime_publication_lock(bounded_stop_token);
  ThrowIfStopRequested(bounded_stop_token);
  RuntimeWalletRegistry::PreparedAppend prepared =
      context.runtime_wallet_registry.PrepareReplace(
          before_wallets.generation(), std::move(next_registry));
  if (std::chrono::steady_clock::now() >= deadline) {
    deadline_expired.store(true, std::memory_order_release);
    bounded_stop_source.request_stop();
  }
  if (!role_control.TryBeginCommit()) {
    throw SimulationCancelled();
  }
  const RuntimeWalletSnapshot published = prepared.Commit();
  role_control.MarkCommitted();
  workloads_lock.unlock();

  try {
    WriteEvent(
        context.events_path, context.options.run_id, "sim",
        SimulationEventKind::kRuntimeWalletGenerationPublished,
        boost::json::serialize(RuntimeWalletGenerationDetail(
            published, std::span<const WalletIdentity>{}, removed_wallets)));
    WriteEvent(context.events_path, context.options.run_id, "sim",
               SimulationEventKind::kRuntimeRoleGenerationPublished,
               boost::json::serialize(
                   RuntimeRoleGenerationDetail(published, current_nodes)));
    boost::json::array wallets;
    wallets.reserve(removed_wallets.size());
    for (const WalletIdentity& wallet : removed_wallets) {
      wallets.push_back(RuntimeWalletIdentityJson(
          wallet, published.registry().wallet_initialization()));
    }
    boost::json::array affected_node_ids;
    affected_node_ids.reserve(requested_node_ids.size());
    for (const std::string& node_id : requested_node_ids) {
      affected_node_ids.emplace_back(node_id);
    }
    return boost::json::object{
        {"added_node_ids", boost::json::array{}},
        {"removed_node_ids", boost::json::array{}},
        {"affected_node_ids", std::move(affected_node_ids)},
        {"action", "wallet.remove"},
        {"state", "removed"},
        {"unchanged", false},
        {"wallets", std::move(wallets)},
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
        "wallet_remove_outcome_unconfirmed",
        "wallet.remove published but completion evidence failed: " +
            context.exception_message(std::current_exception()),
        false);
  }
}

}  // namespace bbp::simulator_app_internal
