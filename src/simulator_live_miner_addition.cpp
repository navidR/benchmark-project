#include "simulator_live_miner_addition.h"

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
#include "simulator_node_process_state.h"
#include "simulator_runtime_identity_details.h"
#include "simulator_runtime_node_addition.h"
#include "simulator_scenario_identifier.h"
#include "simulator_scenario_node_resolution.h"

namespace bbp::simulator_app_internal {

boost::json::object AddLiveMinerRoles(const LiveMinerAdditionContext& context,
                                      const boost::json::object& arguments,
                                      std::stop_token operation_stop_token) {
  constexpr std::array<std::string_view, 6U> kAllowedFields = {
      "run_id",       "node_ids",       "count",
      "create_nodes", "wallet_node_id", "timeout_sec"};
  RejectUnsupportedFields(arguments, kAllowedFields, "miner.add");
  const std::uint32_t count = JsonOptionalUint32Field(arguments, "count", 0U);
  if (count == 0U || count > kSimulationNodeAddMaximumCount) {
    throw std::invalid_argument("miner.add count must be in 1..16");
  }
  const std::uint32_t timeout_sec =
      JsonOptionalUint32Field(arguments, "timeout_sec", 30U);
  if (timeout_sec == 0U || timeout_sec > 3600U) {
    throw std::invalid_argument("miner.add timeout_sec must be in 1..3600");
  }

  std::vector<std::string> requested_node_ids;
  if (const boost::json::value* node_ids = arguments.if_contains("node_ids")) {
    if (!node_ids->is_array()) {
      throw std::invalid_argument("miner.add node_ids must be an array");
    }
    std::set<std::string> unique_node_ids;
    requested_node_ids.reserve(node_ids->as_array().size());
    for (const boost::json::value& node_id : node_ids->as_array()) {
      if (!node_id.is_string()) {
        throw std::invalid_argument("miner.add node_ids must contain strings");
      }
      std::string id(node_id.as_string());
      RequireSafeScenarioIdentifier(id, "miner.add node_ids");
      if (!unique_node_ids.insert(id).second) {
        throw std::invalid_argument("miner.add node_ids must be unique");
      }
      requested_node_ids.push_back(std::move(id));
    }
    if (requested_node_ids.size() != count) {
      throw std::invalid_argument("miner.add count must match node_ids size");
    }
  }

  std::optional<std::string> wallet_node_id;
  if (arguments.if_contains("wallet_node_id") != nullptr) {
    wallet_node_id = JsonOptionalStringField(arguments, "wallet_node_id",
                                             std::string_view());
    RequireSafeScenarioIdentifier(*wallet_node_id, "miner.add wallet_node_id");
    if (count != 1U) {
      throw std::invalid_argument("miner.add wallet_node_id requires count=1");
    }
  }
  const boost::json::value* create_nodes =
      arguments.if_contains("create_nodes");
  if (create_nodes != nullptr && !create_nodes->is_object()) {
    throw std::invalid_argument("miner.add create_nodes must be an object");
  }
  const std::uint32_t selector_count = (!requested_node_ids.empty() ? 1U : 0U) +
                                       (wallet_node_id ? 1U : 0U) +
                                       (create_nodes != nullptr ? 1U : 0U);
  if (selector_count > 1U) {
    throw std::invalid_argument(
        "miner.add node_ids, wallet_node_id, and create_nodes are "
        "mutually exclusive");
  }

  if (context.options.block_production.enabled &&
      context.options.block_production.mode == MiningMode::kNativeMining) {
    const UnsupportedChainOperation error(
        ChainKindName(context.options.chain),
        "transactional runtime native-miner activation");
    throw McpOperationFailure("unsupported_chain_operation", error.what(),
                              false);
  }
  if (context.options.block_production.enabled &&
      context.options.block_production.difficulty) {
    const UnsupportedChainOperation error(
        ChainKindName(context.options.chain),
        "transactional runtime mining-difficulty activation");
    throw McpOperationFailure("unsupported_chain_operation", error.what(),
                              false);
  }
  if (context.options.block_production.enabled &&
      context.options.block_production.mode ==
          MiningMode::kScheduledBlockProduction &&
      context.block_scheduler == nullptr) {
    throw McpOperationFailure(
        "mining_scheduler_unavailable",
        "miner.add requires the active scheduled block producer", true);
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

  if (create_nodes != nullptr) {
    Options validation_options = context.options;
    validation_options.nodes = static_cast<std::uint32_t>(current_nodes.size());
    SimulationNodeAddRequest node_request;
    try {
      node_request = ParseAndValidateSimulationNodeAddRequest(
          create_nodes->as_object(), validation_options);
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
               "the requested miner-node batch exceeds available "
               "capacity"},
              {"path", "create_nodes.count"},
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
          "miner.add count must match create_nodes.count");
    }

    RuntimeNodeAddResult added;
    try {
      std::lock_guard<std::mutex> topology_lock(context.runtime_topology_mutex);
      added = AddRuntimeNodesTransactional(
          context.options, context.run_root, context.events_path,
          context.chain_spec, context.driver, context.node_inventory,
          context.runtime_wallet_registry, RuntimeNodeAdditionRole::kMiner,
          context.block_scheduler.get(), &context.miner_node_ids,
          &context.configured_miner_node_ids_mutex, nullptr,
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
              {"action", "miner.add"},
              {"recoverable", true}}});
    } catch (const SimulationCommandOutcomeUnconfirmed& error) {
      context.mcp_application.MarkRunStopping();
      context.request_simulation_stop();
      throw McpOperationFailure(
          "miner_add_outcome_unconfirmed",
          "miner.add create_nodes outcome is unconfirmed: " +
              std::string(error.what()),
          false);
    } catch (const std::exception&) {
      if (role_control.CommitPhase() ==
          SimulationCommandCommitPhase::kCancelled) {
        throw SimulationCancelled();
      }
      throw;
    } catch (...) {
      if (role_control.CommitPhase() ==
          SimulationCommandCommitPhase::kCancelled) {
        throw SimulationCancelled();
      }
      throw;
    }

    try {
      if (!added.role_generation || !added.final_miner_count ||
          added.added_node_ids.size() != count) {
        throw std::logic_error(
            "miner.add create_nodes omitted its joint publication "
            "evidence");
      }
      boost::json::array node_ids;
      boost::json::array created_node_ids;
      node_ids.reserve(added.added_node_ids.size());
      created_node_ids.reserve(added.added_node_ids.size());
      for (const std::string& node_id : added.added_node_ids) {
        node_ids.emplace_back(node_id);
        created_node_ids.emplace_back(node_id);
      }
      return boost::json::object{
          {"node_ids", std::move(node_ids)},
          {"assigned_roles", boost::json::array{"miner"}},
          {"removed_roles", boost::json::array{}},
          {"action", "miner.add"},
          {"state", "ready"},
          {"created_node_ids", std::move(created_node_ids)},
          {"role_generation", *added.role_generation},
          {"final_miner_count", *added.final_miner_count},
          {"inventory_generation", added.inventory_generation},
          {"final_node_count", added.final_node_count},
      };
    } catch (...) {
      context.mcp_application.MarkRunStopping();
      context.request_simulation_stop();
      throw McpOperationFailure(
          "miner_add_outcome_unconfirmed",
          "miner.add create_nodes published but completion evidence "
          "failed: " +
              context.exception_message(std::current_exception()),
          false);
    }
  }

  const auto is_wallet_node = [&](std::size_t index) {
    return NodeListContains(before_topology.wallet_nodes,
                            static_cast<std::uint32_t>(index));
  };
  const auto is_miner_node = [&](std::size_t index) {
    return NodeListContains(before_topology.miner_nodes,
                            static_cast<std::uint32_t>(index));
  };
  const auto require_compatible_index = [&](std::size_t index) {
    if (is_miner_node(index)) {
      throw McpOperationFailure("miner_already_configured",
                                "miner.add node is already a miner: " +
                                    current_nodes[index].config.id,
                                false);
    }
    if (is_wallet_node(index) && !before_topology.allow_miner_wallet_overlap) {
      throw McpOperationFailure(
          "role_conflict",
          "miner.add cannot overlap the selected wallet role: " +
              current_nodes[index].config.id,
          false);
    }
  };

  std::vector<std::size_t> selected_indexes;
  selected_indexes.reserve(count);
  if (!requested_node_ids.empty()) {
    for (const std::string& requested_node_id : requested_node_ids) {
      const auto selected =
          std::find_if(current_nodes.begin(), current_nodes.end(),
                       [&](const NodeRuntime& node) {
                         return node.config.id == requested_node_id;
                       });
      if (selected == current_nodes.end()) {
        throw McpOperationFailure(
            "node_not_found",
            "miner.add node is not active: " + requested_node_id, false);
      }
      const std::size_t index = static_cast<std::size_t>(
          std::distance(current_nodes.begin(), selected));
      require_compatible_index(index);
      selected_indexes.push_back(index);
    }
  } else if (wallet_node_id) {
    if (!before_topology.allow_miner_wallet_overlap) {
      throw McpOperationFailure(
          "role_conflict",
          "miner.add wallet_node_id requires miner-wallet overlap "
          "permission",
          false);
    }
    const auto selected =
        std::find_if(current_nodes.begin(), current_nodes.end(),
                     [&](const NodeRuntime& node) {
                       return node.config.id == *wallet_node_id;
                     });
    if (selected == current_nodes.end()) {
      throw McpOperationFailure(
          "node_not_found",
          "miner.add wallet node is not active: " + *wallet_node_id, false);
    }
    const std::size_t index = static_cast<std::size_t>(
        std::distance(current_nodes.begin(), selected));
    if (!is_wallet_node(index)) {
      throw McpOperationFailure(
          "wallet_role_required",
          "miner.add wallet_node_id is not a registered wallet node: " +
              *wallet_node_id,
          false);
    }
    require_compatible_index(index);
    selected_indexes.push_back(index);
  } else {
    auto process_guard = context.run_process_state.Lock();
    const auto select_compatible = [&](bool wallet_nodes) {
      for (std::size_t index = 0U;
           index < current_nodes.size() && selected_indexes.size() < count;
           ++index) {
        NodeRuntime& node = current_nodes[index];
        if (is_miner_node(index) || is_wallet_node(index) != wallet_nodes ||
            (wallet_nodes && !before_topology.allow_miner_wallet_overlap) ||
            !node.AllowsChainMetrics() || !node.process.running()) {
          continue;
        }
        selected_indexes.push_back(index);
      }
    };
    select_compatible(false);
    if (selected_indexes.size() < count &&
        before_topology.allow_miner_wallet_overlap) {
      select_compatible(true);
    }
    if (selected_indexes.size() != count) {
      throw McpOperationFailure(
          "miner_backing_node_unavailable",
          "miner.add found fewer compatible running non-miner nodes "
          "than requested",
          false);
    }
  }

  std::vector<std::uint32_t> selected_role_indexes;
  std::vector<std::string> selected_node_ids;
  selected_role_indexes.reserve(selected_indexes.size());
  selected_node_ids.reserve(selected_indexes.size());
  {
    auto process_guard = context.run_process_state.Lock();
    for (const std::size_t index : selected_indexes) {
      NodeRuntime& node = current_nodes[index];
      RequireNodeRunning(node, process_guard, "miner.add");
      selected_role_indexes.push_back(static_cast<std::uint32_t>(index));
      selected_node_ids.push_back(node.config.id);
    }
  }

  std::unique_lock<std::timed_mutex> publication_lock =
      context.acquire_runtime_publication_lock(bounded_stop_token);
  ThrowIfStopRequested(bounded_stop_token);
  RuntimeWalletRegistry::PreparedAppend prepared_roles =
      context.runtime_wallet_registry.PrepareUpdate(
          before_roles.generation(), {}, selected_role_indexes, {},
          static_cast<std::uint32_t>(current_nodes.size()));
  std::unique_lock<std::mutex> configured_miners_lock(
      context.configured_miner_node_ids_mutex);
  std::vector<std::string> next_miner_node_ids = context.miner_node_ids;
  next_miner_node_ids.reserve(next_miner_node_ids.size() +
                              selected_node_ids.size());
  for (const std::string& node_id : selected_node_ids) {
    if (std::find(next_miner_node_ids.begin(), next_miner_node_ids.end(),
                  node_id) != next_miner_node_ids.end()) {
      throw std::logic_error(
          "miner.add selected an already configured miner: " + node_id);
    }
    next_miner_node_ids.push_back(node_id);
  }
  std::optional<ProbabilisticBlockScheduler::PreparedAdd> prepared_scheduler;
  if (context.block_scheduler != nullptr) {
    prepared_scheduler.emplace(
        context.block_scheduler->PrepareAddMinersInactive(selected_node_ids));
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
    if (context.block_scheduler != nullptr) {
      for (const std::string& node_id : selected_node_ids) {
        context.block_scheduler->StartMiner(node_id);
      }
    }
    configured_miners_lock.unlock();
    boost::json::array node_ids;
    node_ids.reserve(selected_node_ids.size());
    for (const std::string& node_id : selected_node_ids) {
      node_ids.emplace_back(node_id);
    }
    return boost::json::object{
        {"node_ids", std::move(node_ids)},
        {"assigned_roles", boost::json::array{"miner"}},
        {"removed_roles", boost::json::array{}},
        {"action", "miner.add"},
        {"state", "ready"},
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
        "miner_add_outcome_unconfirmed",
        "miner.add published but completion evidence failed: " +
            context.exception_message(std::current_exception()),
        false);
  }
}

}  // namespace bbp::simulator_app_internal
