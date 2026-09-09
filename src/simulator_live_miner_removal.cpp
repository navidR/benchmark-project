#include "simulator_live_miner_removal.h"

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
#include "bbp/runtime_node_inventory.h"
#include "bbp/runtime_wallet_registry.h"
#include "bbp/simulation_cancelled.h"
#include "bbp/simulation_command.h"
#include "bbp/simulation_event_kind.h"
#include "bbp/simulation_registry.h"
#include "bbp/simulator/node_runtime.h"
#include "bbp/simulator/options.h"
#include "simulator_cancellable_waiting.h"
#include "simulator_event_writing.h"
#include "simulator_json_field_decoding.h"
#include "simulator_runtime_identity_details.h"
#include "simulator_scenario_identifier.h"
#include "simulator_scenario_node_resolution.h"

namespace bbp::simulator_app_internal {

boost::json::object RemoveLiveMinerRoles(const LiveMinerRemovalContext& context,
                                         const boost::json::object& arguments,
                                         std::stop_token operation_stop_token) {
  constexpr std::array<std::string_view, 3U> kAllowedFields = {
      "run_id", "node_ids", "timeout_sec"};
  RejectUnsupportedFields(arguments, kAllowedFields, "miner.remove");
  const boost::json::value* node_ids = arguments.if_contains("node_ids");
  if (node_ids == nullptr || !node_ids->is_array() ||
      node_ids->as_array().empty() ||
      node_ids->as_array().size() > kMcpMaximumSelectionItems) {
    throw std::invalid_argument(
        "miner.remove node_ids must be a nonempty bounded array");
  }
  std::set<std::string> unique_node_ids;
  std::vector<std::string> requested_node_ids;
  requested_node_ids.reserve(node_ids->as_array().size());
  for (const boost::json::value& node_id : node_ids->as_array()) {
    if (!node_id.is_string()) {
      throw std::invalid_argument("miner.remove node_ids must contain strings");
    }
    std::string id(node_id.as_string());
    RequireSafeScenarioIdentifier(id, "miner.remove node_ids");
    if (!unique_node_ids.insert(id).second) {
      throw std::invalid_argument("miner.remove node_ids must be unique");
    }
    requested_node_ids.push_back(std::move(id));
  }
  const std::uint32_t timeout_sec =
      JsonOptionalUint32Field(arguments, "timeout_sec", 30U);
  if (timeout_sec == 0U || timeout_sec > 3600U) {
    throw std::invalid_argument("miner.remove timeout_sec must be in 1..3600");
  }
  if (context.options.block_production.enabled &&
      context.options.block_production.mode == MiningMode::kNativeMining) {
    const UnsupportedChainOperation error(
        ChainKindName(context.options.chain),
        "transactional runtime native-miner removal");
    throw McpOperationFailure("unsupported_chain_operation", error.what(),
                              false);
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
  const RuntimeWalletSnapshot before_roles =
      context.runtime_wallet_registry.Snapshot();
  const NodeRoleTopology& before_topology = before_roles.registry().topology();

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
          "miner.remove node is not active: " + requested_node_id, false);
    }
    const std::uint32_t index = static_cast<std::uint32_t>(
        std::distance(current_nodes.begin(), selected));
    if (!NodeListContains(before_topology.miner_nodes, index)) {
      throw McpOperationFailure(
          "miner_not_found",
          "miner.remove node has no configured miner role: " +
              requested_node_id,
          false);
    }
    selected_role_indexes.push_back(index);
  }
  if (context.options.block_production.enabled &&
      selected_role_indexes.size() == before_topology.miner_nodes.size()) {
    throw McpOperationFailure(
        "miner_required",
        "miner.remove cannot remove every miner while block production "
        "is enabled",
        false);
  }
  if (context.options.block_production.enabled &&
      context.options.block_production.mode ==
          MiningMode::kScheduledBlockProduction &&
      context.block_scheduler == nullptr) {
    throw McpOperationFailure(
        "mining_scheduler_unavailable",
        "miner.remove requires the active scheduled block producer", true);
  }

  SimulationRegistry next_registry = before_roles.registry();
  next_registry.RemoveMinerNodes(selected_role_indexes);
  std::unique_lock<std::timed_mutex> publication_lock =
      context.acquire_runtime_publication_lock(bounded_stop_token);
  ThrowIfStopRequested(bounded_stop_token);
  RuntimeWalletRegistry::PreparedAppend prepared_roles =
      context.runtime_wallet_registry.PrepareReplace(before_roles.generation(),
                                                     std::move(next_registry));
  std::unique_lock<std::mutex> configured_miners_lock(
      context.configured_miner_node_ids_mutex);
  std::vector<std::string> next_miner_node_ids = context.miner_node_ids;
  for (const std::string& node_id : requested_node_ids) {
    const auto miner = std::find(next_miner_node_ids.begin(),
                                 next_miner_node_ids.end(), node_id);
    if (miner == next_miner_node_ids.end()) {
      throw std::logic_error(
          "miner.remove registry and configured miners disagree: " + node_id);
    }
    next_miner_node_ids.erase(miner);
  }
  std::optional<ProbabilisticBlockScheduler::PreparedRemove> prepared_scheduler;
  if (context.block_scheduler != nullptr) {
    prepared_scheduler.emplace(context.block_scheduler->PrepareRemoveMiners(
        requested_node_ids, bounded_stop_token));
  }
  if (!role_control.TryBeginCommit()) {
    throw SimulationCancelled();
  }
  const RuntimeWalletSnapshot published_roles = prepared_roles.Commit();
  if (prepared_scheduler) {
    prepared_scheduler->Commit();
  }
  context.miner_node_ids.swap(next_miner_node_ids);
  role_control.MarkCommitted();

  try {
    WriteEvent(context.events_path, context.options.run_id, "sim",
               SimulationEventKind::kRuntimeRoleGenerationPublished,
               boost::json::serialize(RuntimeRoleGenerationDetail(
                   published_roles, current_nodes)));
    boost::json::array removed_node_ids;
    removed_node_ids.reserve(requested_node_ids.size());
    for (const std::string& node_id : requested_node_ids) {
      removed_node_ids.emplace_back(node_id);
    }
    return boost::json::object{
        {"node_ids", std::move(removed_node_ids)},
        {"assigned_roles", boost::json::array{}},
        {"removed_roles", boost::json::array{"miner"}},
        {"action", "miner.remove"},
        {"state", "removed"},
        {"created_node_ids", boost::json::array{}},
        {"role_generation", published_roles.generation()},
        {"final_miner_count",
         published_roles.registry().topology().miner_nodes.size()},
        {"inventory_generation", current_nodes.generation()},
        {"final_node_count", current_nodes.size()},
    };
  } catch (...) {
    context.mcp_application.MarkRunStopping();
    context.request_simulation_stop();
    throw McpOperationFailure(
        "miner_remove_outcome_unconfirmed",
        "miner.remove published but completion evidence failed: " +
            context.exception_message(std::current_exception()),
        false);
  }
}

}  // namespace bbp::simulator_app_internal
