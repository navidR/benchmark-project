#include "simulator_live_lifecycle_supervisor.h"

#include <chrono>
#include <condition_variable>
#include <cstddef>
#include <exception>
#include <mutex>
#include <optional>
#include <stdexcept>
#include <stop_token>
#include <string>

#include "bbp/drivers/chain_driver_registry.h"
#include "bbp/mcp_live_application.h"
#include "bbp/node_lifecycle_policy.h"
#include "bbp/probabilistic_block_scheduler.h"
#include "bbp/run_process_state.h"
#include "bbp/runtime_node_inventory.h"
#include "bbp/simulation_cancelled.h"
#include "bbp/simulation_event_kind.h"
#include "bbp/simulator/node_runtime.h"
#include "bbp/simulator/options.h"
#include "simulator_cancellable_waiting.h"
#include "simulator_event_writing.h"
#include "simulator_native_mining_rpc.h"
#include "simulator_node_lifecycle_event_details.h"
#include "simulator_node_process_state.h"
#include "simulator_operator_connection_publication.h"
#include "simulator_perf_counter_attachment.h"
#include "simulator_runtime_node_startup.h"
#include "simulator_runtime_node_stop.h"

namespace bbp::simulator_app_internal {

void RunLiveLifecycleSupervisor(std::stop_token supervisor_stop_token,
                                LiveLifecycleSupervisorContext context) {
  std::stop_source operation_stop_source;
  std::stop_callback stop_on_supervisor(
      supervisor_stop_token,
      [&operation_stop_source] { operation_stop_source.request_stop(); });
  std::stop_callback stop_on_simulation(
      context.stop_token,
      [&operation_stop_source] { operation_stop_source.request_stop(); });
  const std::stop_token operation_stop_token =
      operation_stop_source.get_token();
  try {
    std::condition_variable_any wakeup;
    std::mutex wakeup_mutex;
    while (!operation_stop_token.stop_requested()) {
      {
        auto mutation_lock = context.acquire_node_mutation_lock(
            context.node_mutation_mutex, operation_stop_token);
        const RuntimeNodeSnapshot nodes = context.node_inventory.Snapshot();
        for (std::size_t index = 0; index < nodes.size(); ++index) {
          if (operation_stop_token.stop_requested()) {
            return;
          }
          NodeRuntime& node = nodes[index];
          const auto now = std::chrono::steady_clock::now();
          const bool stop_due =
              node.lifecycle_policy.stop_time &&
              now >= SteadyDeadline(context.lifecycle_epoch,
                                    context.options.time_scale.WallDuration(
                                        *node.lifecycle_policy.stop_time));
          if (stop_due && !node.DeclarativeStopApplied()) {
            if (context.block_scheduler &&
                context.is_configured_miner(node.config.id)) {
              context.block_scheduler->StopMiner(node.config.id);
            }
            if (node.DeclarativeStopApplied()) {
              continue;
            }
            node.MarkDeclarativeStopApplied();
            WriteEvent(
                context.events_path, context.options.run_id, node.config.id,
                SimulationEventKind::kNodeStopDeadlineReached,
                NodeLifecycleDeadlineDetail(
                    node, context.options.time_scale, context.lifecycle_epoch,
                    *node.lifecycle_policy.stop_time, "declarative_stop"));
            if (NodeProcessRunning(node)) {
              StopNodeProcess(context.options, context.events_path,
                              context.driver, node, operation_stop_token);
            } else {
              {
                auto process_guard = context.run_process_state.Lock();
                ResetNodePerfCounters(node, process_guard);
                node.SetLifecycle(NodeRuntimeLifecycle::kStopped);
              }
              WriteNodeStateEvent(context.events_path, context.options.run_id,
                                  node, NodeRuntimeLifecycle::kStopped);
            }
            {
              auto process_guard = context.run_process_state.Lock();
              context.run_process_state.RemoveActiveNativeMiner(process_guard,
                                                                node.config.id);
            }
            continue;
          }

          bool node_started = false;
          if (node.lifecycle_policy.start_time &&
              node.Lifecycle() == NodeRuntimeLifecycle::kCgroupReady &&
              now >= SteadyDeadline(context.lifecycle_epoch,
                                    context.options.time_scale.WallDuration(
                                        *node.lifecycle_policy.start_time))) {
            WriteEvent(
                context.events_path, context.options.run_id, node.config.id,
                SimulationEventKind::kNodeStartDeadlineReached,
                NodeLifecycleDeadlineDetail(
                    node, context.options.time_scale, context.lifecycle_epoch,
                    *node.lifecycle_policy.start_time, "declarative_start"));
            node_started = StartPreparedNode(
                context.options, context.events_path, context.driver, node,
                context.node_network_state_mutex, "declarative_start",
                context.lifecycle_epoch, operation_stop_token);
            if (node_started) {
              ConnectAvailableStartupPeers(
                  context.options, context.events_path, context.driver, nodes,
                  context.node_network_state_mutex, index,
                  context.lifecycle_epoch, operation_stop_token);
              if (context.is_configured_miner(node.config.id) &&
                  context.options.block_production.enabled &&
                  context.options.block_production.difficulty) {
                RequireNodeRunning(node, "declarative mining difficulty");
                context.driver.SetMiningDifficulty(
                    node.config, *context.options.block_production.difficulty,
                    operation_stop_token);
              }
              PublishOperatorConnectionCommand(
                  context.options, context.run_root, context.events_path,
                  context.driver, nodes, &context.operator_connection_resolved);
            }
          }
          if (node_started && context.block_scheduler &&
              context.is_configured_miner(node.config.id)) {
            context.block_scheduler->StartMiner(node.config.id);
          } else if (node_started && context.options.block_production.enabled &&
                     context.options.block_production.mode ==
                         MiningMode::kNativeMining &&
                     context.is_configured_miner(node.config.id)) {
            static_cast<void>(StartNativeMiningForCurrentProcess(
                context.driver, node, context.run_process_state,
                context.chain_spec.default_reward_address, operation_stop_token,
                "declarative native mining start"));
          }

          bool node_restarted = false;
          std::optional<int> exited_wait_status;
          std::string process_exit_detail;
          {
            auto process_guard = context.run_process_state.Lock();
            if (node.Lifecycle() != NodeRuntimeLifecycle::kRunning ||
                node.process.running()) {
              continue;
            }
            exited_wait_status = node.process.exit_status();
            if (!exited_wait_status) {
              throw std::runtime_error("node exited without a wait status: " +
                                       node.config.id);
            }
            ResetNodePerfCounters(node, process_guard);
            process_exit_detail =
                ProcessExitDetail(node.process, process_guard);
            node.SetLifecycle(NodeRuntimeLifecycle::kFailed);
          }
          WriteEvent(context.events_path, context.options.run_id,
                     node.config.id, SimulationEventKind::kProcessExited,
                     process_exit_detail);
          WriteEvent(context.events_path, context.options.run_id,
                     node.config.id, SimulationEventKind::kState,
                     NodeRuntimeLifecycleName(NodeRuntimeLifecycle::kFailed));
          const bool restart = NodeRestartPolicyAllowsRestart(
              node.lifecycle_policy.restart_policy, *exited_wait_status);
          WriteEvent(
              context.events_path, context.options.run_id, node.config.id,
              SimulationEventKind::kRestartPolicyApplied,
              RestartPolicyAppliedDetail(node, *exited_wait_status, restart));
          if (!restart) {
            throw std::runtime_error(
                "node process exited and restart policy did not restart "
                "it: " +
                node.config.id);
          }
          node_restarted =
              RestartNode(context.options, context.events_path, context.driver,
                          *context.peer_connectivity_controller, node,
                          context.lifecycle_epoch, context.start_node,
                          operation_stop_token, "restart_policy");
          if (node_restarted && context.is_configured_miner(node.config.id) &&
              context.options.block_production.enabled &&
              context.options.block_production.difficulty) {
            RequireNodeRunning(node, "restart mining difficulty restore");
            context.driver.SetMiningDifficulty(
                node.config, *context.options.block_production.difficulty,
                operation_stop_token);
          }
          if (node_restarted && context.block_scheduler &&
              context.is_configured_miner(node.config.id)) {
            context.block_scheduler->StartMiner(node.config.id);
          } else if (node_restarted &&
                     context.options.block_production.enabled &&
                     context.options.block_production.mode ==
                         MiningMode::kNativeMining &&
                     context.is_configured_miner(node.config.id)) {
            static_cast<void>(StartNativeMiningForCurrentProcess(
                context.driver, node, context.run_process_state,
                context.chain_spec.default_reward_address, operation_stop_token,
                "restart-policy native mining restore"));
          }
        }
      }
      std::unique_lock<std::mutex> wait_lock(wakeup_mutex);
      wakeup.wait_for(wait_lock, operation_stop_token,
                      std::chrono::milliseconds(20), [] { return false; });
    }
  } catch (const SimulationCancelled&) {
    if (operation_stop_token.stop_requested()) {
      return;
    }
    throw;
  } catch (...) {
    {
      std::lock_guard<std::mutex> lock(context.lifecycle_failure_mutex);
      if (!context.lifecycle_failure) {
        context.lifecycle_failure = std::current_exception();
      }
    }
    context.mcp_application.MarkRunStopping();
    context.request_simulation_stop();
  }
}

}  // namespace bbp::simulator_app_internal
