#include "simulator_live_workload_service_binding.h"

#include <boost/json/object.hpp>
#include <cstdint>
#include <memory>
#include <mutex>
#include <optional>
#include <stdexcept>
#include <stop_token>
#include <string>
#include <string_view>
#include <utility>

#include "bbp/drivers/chain_driver_registry.h"
#include "bbp/mcp_live_application.h"
#include "bbp/runtime_node_inventory.h"
#include "bbp/runtime_wallet_registry.h"
#include "bbp/scenario_service.h"
#include "bbp/simulator/options.h"
#include "simulator_live_block_generation_control.h"
#include "simulator_live_block_generation_workload_launcher.h"
#include "simulator_live_height_wait_control.h"
#include "simulator_live_height_wait_workload_launcher.h"
#include "simulator_live_peer_wait_workload_launcher.h"
#include "simulator_live_wait_for_peers_control.h"
#include "simulator_live_wallet_workload_control.h"
#include "simulator_live_wallet_workload_launcher.h"
#include "simulator_live_workload_reading.h"
#include "simulator_live_workload_state.h"
#include "simulator_one_shot_workload_dispatch.h"
#include "simulator_one_shot_workload_invocation.h"
#include "simulator_runtime_workload_validation.h"
#include "simulator_wallet_transaction_validation.h"

namespace bbp::simulator_app_internal {

LiveWorkloadServiceBinding MakeLiveWorkloadServiceBinding(
    const LiveWorkloadServiceBindingContext& context) {
  auto workload_service = std::make_shared<McpLiveWorkloadService>();
  const auto runtime_wallet_validation_options = [context] {
    Options validation = context.options;
    const RuntimeWalletSnapshot wallet_snapshot =
        context.runtime_wallet_registry.Snapshot();
    validation.topology = wallet_snapshot.registry().topology();
    validation.wallet_backed_workload_requested =
        !wallet_snapshot.wallets().empty();
    return validation;
  };

  const auto launch_wallet_workload = MakeLiveWalletWorkloadLauncher(
      context.options, context.events_path, context.driver,
      context.node_inventory, context.runtime_wallet_registry,
      context.transaction_tracker, context.block_generation_mutex,
      *workload_service, context.wallet_workloads,
      context.block_generation_workloads, context.wait_until_height_workloads,
      context.wait_for_peers_workloads);

  const auto find_block_generation_workload_record =
      [block_generation_workloads =
           context.block_generation_workloads](std::string_view workload_id) {
        std::lock_guard<std::mutex> lock(block_generation_workloads->mutex);
        const auto found =
            block_generation_workloads->records.find(std::string(workload_id));
        return found == block_generation_workloads->records.end()
                   ? std::shared_ptr<LiveBlockGenerationWorkloadRecord>{}
                   : found->second;
      };
  const auto launch_block_generation_workload =
      MakeLiveBlockGenerationWorkloadLauncher(
          context.options, context.events_path, context.driver,
          context.node_inventory, context.chain_spec.default_reward_address,
          context.block_generation_mutex,
          [context](std::stop_token mutation_stop_token) {
            return context.acquire_node_mutation_lock(
                context.node_mutation_mutex, mutation_stop_token);
          },
          *workload_service, context.wallet_workloads,
          context.block_generation_workloads,
          context.wait_until_height_workloads,
          context.wait_for_peers_workloads);

  const auto block_generation_operation = MakeLiveBlockGenerationOperation(
      find_block_generation_workload_record,
      [context, launch_block_generation_workload](
          const boost::json::object& workload_value,
          std::optional<std::string> requested_id,
          std::stop_token operation_stop_token) {
        auto mutation_lock = context.acquire_node_mutation_lock(
            context.node_mutation_mutex, operation_stop_token);
        const RuntimeNodeSnapshot current_nodes =
            context.node_inventory.Snapshot();
        const BlockGenerationWorkload workload =
            ParseAndValidateLiveBlockGenerationWorkload(
                workload_value, context.options, current_nodes);
        return launch_block_generation_workload(workload,
                                                std::move(requested_id));
      },
      [context](const boost::json::object& workload_value) {
        const RuntimeNodeSnapshot current_nodes =
            context.node_inventory.Snapshot();
        return ParseAndValidateLiveBlockGenerationWorkload(
            workload_value, context.options, current_nodes);
      },
      [context](const LiveBlockGenerationWorkloadRecord& record) {
        WriteLiveBlockGenerationWorkloadState(context.events_path,
                                              context.options, record);
      });

  const auto find_wait_until_height_workload_record =
      [wait_until_height_workloads =
           context.wait_until_height_workloads](std::string_view workload_id) {
        std::lock_guard<std::mutex> lock(wait_until_height_workloads->mutex);
        const auto found =
            wait_until_height_workloads->records.find(std::string(workload_id));
        return found == wait_until_height_workloads->records.end()
                   ? std::shared_ptr<LiveWaitUntilHeightWorkloadRecord>{}
                   : found->second;
      };
  const auto launch_wait_until_height_workload =
      MakeLiveHeightWaitWorkloadLauncher(
          context.options, context.events_path, context.driver,
          context.node_inventory, context.run_stop_tick,
          [context](std::stop_token mutation_stop_token) {
            return context.acquire_node_mutation_lock(
                context.node_mutation_mutex, mutation_stop_token);
          },
          *workload_service, context.wallet_workloads,
          context.block_generation_workloads,
          context.wait_until_height_workloads,
          context.wait_for_peers_workloads);

  const auto wait_until_height_operation = MakeLiveWaitUntilHeightOperation(
      find_wait_until_height_workload_record,
      [context, launch_wait_until_height_workload](
          const boost::json::object& workload_value,
          std::optional<std::string> requested_id,
          std::stop_token operation_stop_token) {
        auto mutation_lock = context.acquire_node_mutation_lock(
            context.node_mutation_mutex, operation_stop_token);
        const RuntimeNodeSnapshot current_nodes =
            context.node_inventory.Snapshot();
        const WaitUntilHeightWorkload workload =
            ParseAndValidateLiveWaitUntilHeightWorkload(
                workload_value, context.options, current_nodes);
        return launch_wait_until_height_workload(workload,
                                                 std::move(requested_id));
      },
      [context](const boost::json::object& workload_value,
                std::stop_token operation_stop_token) {
        auto mutation_lock = context.acquire_node_mutation_lock(
            context.node_mutation_mutex, operation_stop_token);
        return ParseAndValidateLiveWaitUntilHeightWorkload(
            workload_value, context.options, context.node_inventory.Snapshot());
      },
      [context](const LiveWaitUntilHeightWorkloadRecord& record) {
        WriteLiveWaitUntilHeightWorkloadState(context.events_path,
                                              context.options, record);
      });

  const auto find_wait_for_peers_workload_record =
      [wait_for_peers_workloads =
           context.wait_for_peers_workloads](std::string_view workload_id) {
        std::lock_guard<std::mutex> lock(wait_for_peers_workloads->mutex);
        const auto found =
            wait_for_peers_workloads->records.find(std::string(workload_id));
        return found == wait_for_peers_workloads->records.end()
                   ? std::shared_ptr<LiveWaitForPeersWorkloadRecord>{}
                   : found->second;
      };
  const auto launch_wait_for_peers_workload = MakeLivePeerWaitWorkloadLauncher(
      context.options, context.events_path, context.driver,
      context.node_inventory, context.run_stop_tick,
      [context](std::stop_token mutation_stop_token) {
        return context.acquire_node_mutation_lock(context.node_mutation_mutex,
                                                  mutation_stop_token);
      },
      *workload_service, context.wallet_workloads,
      context.block_generation_workloads, context.wait_until_height_workloads,
      context.wait_for_peers_workloads);

  const auto wait_for_peers_operation = MakeLiveWaitForPeersOperation(
      find_wait_for_peers_workload_record,
      [context, launch_wait_for_peers_workload](
          const boost::json::object& workload_value,
          std::optional<std::string> requested_id,
          std::stop_token operation_stop_token) {
        auto mutation_lock = context.acquire_node_mutation_lock(
            context.node_mutation_mutex, operation_stop_token);
        const RuntimeNodeSnapshot current_nodes =
            context.node_inventory.Snapshot();
        const WaitForPeersWorkload workload =
            ParseAndValidateLiveWaitForPeersWorkload(
                workload_value, context.options, current_nodes);
        return launch_wait_for_peers_workload(workload,
                                              std::move(requested_id));
      },
      [context](const boost::json::object& workload_value,
                std::stop_token operation_stop_token) {
        auto mutation_lock = context.acquire_node_mutation_lock(
            context.node_mutation_mutex, operation_stop_token);
        return ParseAndValidateLiveWaitForPeersWorkload(
            workload_value, context.options, context.node_inventory.Snapshot());
      },
      [context](const LiveWaitForPeersWorkloadRecord& record) {
        WriteLiveWaitForPeersWorkloadState(context.events_path, context.options,
                                           record);
      });

  const auto dispatch_one_shot_workload =
      [context](
          const ScenarioWorkload& scenario_workload,
          const RuntimeNodeSnapshot& nodes, std::uint32_t action_index,
          std::uint32_t action_count, std::stop_token operation_stop_token,
          SimulationCommandControl* cancellation_commit_control = nullptr) {
        DispatchOneShotWorkload(
            OneShotWorkloadContext{
                .options = context.options,
                .events_path = context.events_path,
                .metrics_path = context.metrics_path,
                .wallet_metrics_path = context.wallet_metrics_path,
                .chain_spec = context.chain_spec,
                .driver = context.driver,
                .peer_connectivity_controller =
                    context.peer_connectivity_controller,
                .runtime_topology = context.runtime_topology,
                .runtime_wallet_registry = context.runtime_wallet_registry,
                .transaction_tracker = context.transaction_tracker,
                .run_process_state = context.run_process_state,
                .node_network_state_mutex = context.node_network_state_mutex,
                .node_resource_state_mutex = context.node_resource_state_mutex,
                .runtime_topology_mutex = context.runtime_topology_mutex,
                .block_generation_mutex = context.block_generation_mutex,
                .lifecycle_epoch = context.lifecycle_epoch,
                .start_node = context.start_node,
                .stop_token = context.stop_token,
            },
            scenario_workload, nodes, action_index, action_count,
            operation_stop_token, cancellation_commit_control);
      };

  const auto execute_one_shot_workload =
      [context, dispatch_one_shot_workload](
          const ScenarioWorkload& scenario_workload, std::uint32_t action_index,
          std::uint32_t action_count, std::stop_token operation_stop_token) {
        auto one_shot_lock = context.acquire_node_mutation_lock(
            context.one_shot_workload_mutex, operation_stop_token);
        auto mutation_lock = context.acquire_node_mutation_lock(
            context.node_mutation_mutex, operation_stop_token);
        const RuntimeNodeSnapshot current_nodes =
            context.node_inventory.Snapshot();
        dispatch_one_shot_workload(scenario_workload, current_nodes,
                                   action_index, action_count,
                                   operation_stop_token);
      };

  const auto invoke_one_shot_workload = MakeOneShotWorkloadInvoker(
      context.options, context.node_inventory, context.runtime_wallet_registry,
      context.live_topology_config, context.one_shot_workload_mutex,
      context.node_mutation_mutex, context.next_one_shot_invocation,
      context.mcp_application, context.request_simulation_stop,
      context.acquire_node_mutation_lock, dispatch_one_shot_workload,
      context.stop_token);

  const auto wallet_workload_operation = MakeLiveWalletWorkloadOperation(
      context.wallet_workloads,
      [context, launch_wallet_workload, runtime_wallet_validation_options](
          const boost::json::object& workload_value,
          std::optional<std::string> requested_id,
          std::stop_token operation_stop_token) {
        auto mutation_lock = context.acquire_node_mutation_lock(
            context.node_mutation_mutex, operation_stop_token);
        const Options validation_options = runtime_wallet_validation_options();
        const WalletTransactionsWorkload workload =
            ParseAndValidateWalletTransactionsWorkload(workload_value,
                                                       validation_options);
        return launch_wallet_workload(workload, std::move(requested_id));
      },
      [runtime_wallet_validation_options](
          const boost::json::object& workload_value) {
        const Options validation_options = runtime_wallet_validation_options();
        return ParseAndValidateWalletTransactionsWorkload(workload_value,
                                                          validation_options);
      },
      [context](const WalletTransactionsWorkload& workload) {
        RuntimeWalletSnapshot wallet_snapshot =
            context.runtime_wallet_registry.Snapshot();
        Options validation = context.options;
        validation.topology = wallet_snapshot.registry().topology();
        validation.wallet_backed_workload_requested =
            !wallet_snapshot.wallets().empty();
        ValidateWalletTransactionsWorkload(workload, validation);
        return wallet_snapshot;
      });

  workload_service->operation = [wallet_workload_operation,
                                 invoke_one_shot_workload,
                                 find_block_generation_workload_record,
                                 block_generation_operation,
                                 find_wait_until_height_workload_record,
                                 wait_until_height_operation,
                                 find_wait_for_peers_workload_record,
                                 wait_for_peers_operation](
                                    McpOperationKind kind,
                                    const boost::json::object& arguments,
                                    std::stop_token operation_stop_token) {
    if (kind == McpOperationKind::kInvokeWorkload) {
      const boost::json::value* workload = arguments.if_contains("workload");
      if (workload == nullptr || !workload->is_object()) {
        throw std::invalid_argument(
            "workload.invoke requires a workload object");
      }
      return invoke_one_shot_workload(workload->as_object(),
                                      operation_stop_token);
    }
    if (kind == McpOperationKind::kStartWorkload) {
      const boost::json::value* workload_value =
          arguments.if_contains("workload");
      if (workload_value != nullptr && workload_value->is_object()) {
        const boost::json::value* type =
            workload_value->as_object().if_contains("type");
        if (type != nullptr && type->is_string()) {
          if (type->as_string() == "block_generation") {
            return block_generation_operation(kind, arguments,
                                              operation_stop_token);
          }
          if (type->as_string() == "wait_until_height") {
            return wait_until_height_operation(kind, arguments,
                                               operation_stop_token);
          }
          if (type->as_string() == "wait_for_peers") {
            return wait_for_peers_operation(kind, arguments,
                                            operation_stop_token);
          }
        }
      }
    } else {
      const boost::json::value* workload_id =
          arguments.if_contains("workload_id");
      if (workload_id != nullptr && workload_id->is_string()) {
        if (find_block_generation_workload_record(workload_id->as_string())) {
          return block_generation_operation(kind, arguments,
                                            operation_stop_token);
        }
        if (find_wait_until_height_workload_record(workload_id->as_string())) {
          return wait_until_height_operation(kind, arguments,
                                             operation_stop_token);
        }
        if (find_wait_for_peers_workload_record(workload_id->as_string())) {
          return wait_for_peers_operation(kind, arguments,
                                          operation_stop_token);
        }
      }
    }
    return wallet_workload_operation(kind, arguments, operation_stop_token);
  };
  workload_service->read =
      [wallet_workloads = context.wallet_workloads,
       block_generation_workloads = context.block_generation_workloads,
       wait_until_height_workloads = context.wait_until_height_workloads,
       wait_for_peers_workloads = context.wait_for_peers_workloads](
          bool history, std::stop_token read_stop_token) {
        return ReadLiveWorkloads(wallet_workloads, block_generation_workloads,
                                 wait_until_height_workloads,
                                 wait_for_peers_workloads, history,
                                 read_stop_token);
      };
  return LiveWorkloadServiceBinding{
      .service = std::move(workload_service),
      .launch_wallet_workload = launch_wallet_workload,
      .execute_one_shot_workload = execute_one_shot_workload,
  };
}

}  // namespace bbp::simulator_app_internal
