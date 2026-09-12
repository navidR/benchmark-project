#include "simulator_live_command_execution.h"

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <exception>
#include <iterator>
#include <limits>
#include <memory>
#include <mutex>
#include <optional>
#include <stdexcept>
#include <stop_token>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "bbp/cgroup.h"
#include "bbp/drivers/chain_command_executor.h"
#include "bbp/drivers/chain_driver_registry.h"
#include "bbp/logging.h"
#include "bbp/mcp_live_application.h"
#include "bbp/peer_connectivity_controller.h"
#include "bbp/probabilistic_block_scheduler.h"
#include "bbp/run_process_state.h"
#include "bbp/runtime_node_inventory.h"
#include "bbp/runtime_peer_topology.h"
#include "bbp/runtime_wallet_registry.h"
#include "bbp/simulation_cancelled.h"
#include "bbp/simulation_command_processor.h"
#include "bbp/simulation_event_kind.h"
#include "bbp/simulation_registry.h"
#include "bbp/simulator/node_runtime.h"
#include "bbp/simulator/options.h"
#include "simulator_block_generation_boundary.h"
#include "simulator_cancellable_waiting.h"
#include "simulator_combined_stop_token.h"
#include "simulator_event_writing.h"
#include "simulator_live_instrumentation_controller.h"
#include "simulator_native_mining_rpc.h"
#include "simulator_network_block_application.h"
#include "simulator_network_condition_application.h"
#include "simulator_network_event_details.h"
#include "simulator_network_partition_application.h"
#include "simulator_network_partition_planning.h"
#include "simulator_network_rule_decoding.h"
#include "simulator_node_process_state.h"
#include "simulator_node_report_export.h"
#include "simulator_perf_counter_attachment.h"
#include "simulator_perf_counter_transactions.h"
#include "simulator_profile_switching.h"
#include "simulator_resource_limit_orchestration.h"
#include "simulator_runtime_node_addition.h"
#include "simulator_runtime_node_freeze.h"
#include "simulator_runtime_node_removal.h"
#include "simulator_runtime_node_replacement.h"
#include "simulator_runtime_node_stop.h"
#include "simulator_scheduled_command_event_details.h"
#include "simulator_transaction_observation_tracking.h"
#include "simulator_wallet_transaction_validation.h"
#include "simulator_workload_event_details.h"

namespace bbp::simulator_app_internal {
namespace {

bool SimulationCommandRequiresNodeMutationLock(SimulationCommandKind kind) {
  switch (kind) {
    case SimulationCommandKind::kSetBlockProductionPolicy:
    case SimulationCommandKind::kGenerateBlocks:
    case SimulationCommandKind::kExportNodeReport:
    case SimulationCommandKind::kAssignRole:
    case SimulationCommandKind::kRemoveRole:
      return false;
    case SimulationCommandKind::kCount:
      return false;
    default:
      return true;
  }
}

}  // namespace

std::unique_ptr<ChainCommandExecutor> MakeLiveChainCommandExecutor(
    const LiveCommandExecutionContext& context) {
  return std::make_unique<ChainCommandExecutor>(
      context.driver,
      [context] { return context.node_inventory.ConfigSnapshot(); },
      [context](const ChainNodeConfig& config,
                std::stop_token command_stop_token) {
        if (!context.is_configured_miner(config.id)) {
          throw std::runtime_error("node is not a configured miner: " +
                                   config.id);
        }
        if (context.options.block_production.mode ==
            MiningMode::kScheduledBlockProduction) {
          if (!context.block_scheduler) {
            throw std::runtime_error(
                "scheduled block production is not active");
          }
          context.block_scheduler->StopMiner(config.id);
        } else {
          const RuntimeNodeSnapshot current_nodes =
              context.node_inventory.Snapshot();
          StopNativeMining(
              context.driver,
              context.find_node_runtime_by_id(current_nodes, config.id),
              context.run_process_state, command_stop_token);
        }
      },
      [context](BlockProductionPolicy policy) {
        if (!context.block_scheduler) {
          throw UnsupportedChainOperation(
              "active mining mode",
              "probabilistic block production policy adjustment");
        }
        context.block_scheduler->UpdatePolicy(policy);
      },
      [context](const ChainNodeConfig& config, MiningDifficulty difficulty,
                std::stop_token command_stop_token) {
        if (!context.is_configured_miner(config.id)) {
          throw std::runtime_error("node is not a configured miner: " +
                                   config.id);
        }
        context.driver.SetMiningDifficulty(config, difficulty,
                                           command_stop_token);
      },
      [context](const ChainNodeConfig& config, const ChainNodeConfig& peer,
                std::stop_token command_stop_token) {
        context.peer_connectivity_controller->ConnectPeer(
            config.id, peer.id, std::chrono::seconds(10), command_stop_token);
      },
      [context](const ChainNodeConfig& config, const ChainNodeConfig& peer,
                std::stop_token command_stop_token) {
        context.peer_connectivity_controller->DisconnectPeer(
            config.id, peer.id, std::chrono::seconds(10), command_stop_token);
      },
      [context](const ChainNodeConfig& config, PeerCountPolicy policy) {
        context.peer_connectivity_controller->SetPolicy(config.id, policy);
      });
}

std::unique_ptr<SimulationCommandProcessor> MakeLiveSimulationCommandProcessor(
    SimulationCommandQueue& command_queue,
    const LiveCommandExecutionContext& context) {
  // The processor reports each outcome on the same thread as its handler.
  // Keep role mutations excluded until the committed inventory is read back.
  const auto mutation_admission =
      std::make_shared<std::unique_lock<std::timed_mutex>>();
  return std::make_unique<SimulationCommandProcessor>(
      command_queue,
      [context, mutation_admission](const SimulationCommand& command) {
        SimulationCommandOutcome command_outcome;
        std::stop_callback application_shutdown_callback(
            context.command_rpc_stop_source.get_token(), [&] {
              if ((command.kind == SimulationCommandKind::kAddNodes ||
                   command.kind == SimulationCommandKind::kReplaceNode ||
                   command.kind == SimulationCommandKind::kRemoveNodes ||
                   command.kind == SimulationCommandKind::kAssignRole ||
                   command.kind == SimulationCommandKind::kRemoveRole) &&
                  command.operation_control) {
                static_cast<
                    void>(command.operation_control->RequestCancellation(
                    SimulationCommandCancellationCause::kApplicationShutdown));
              }
            });
        const std::stop_token operation_stop_token =
            command.operation_control
                ? command.operation_control->stop_source.get_token()
                : std::stop_token{};
        CombinedStopToken combined_stop_token(
            context.command_rpc_stop_source.get_token(), operation_stop_token);
        const std::stop_token command_stop_token =
            combined_stop_token.get_token();
        ThrowIfStopRequested(command_stop_token);
        if (SimulationCommandRequiresNodeMutationLock(command.kind)) {
          *mutation_admission = context.acquire_node_mutation_lock(
              context.node_mutation_mutex, command_stop_token);
        }
        RequireNoLiveInstrumentationCommandConflict(
            *context.live_instrumentation, command);
        WriteEvent(context.events_path, context.options.run_id, command.node_id,
                   SimulationEventKind::kOperatorCommandStarted,
                   SimulationCommandDetail(command));
        const bool scheduled_miner =
            context.block_scheduler &&
            context.is_configured_miner(command.node_id);
        const auto stop_scheduled_miner = [&] {
          return scheduled_miner
                     ? context.block_scheduler->StopMiner(command.node_id)
                     : false;
        };
        const bool needs_runtime_snapshot =
            command.kind != SimulationCommandKind::kExportNodeReport &&
            command.kind != SimulationCommandKind::kSetBlockProductionPolicy &&
            command.kind != SimulationCommandKind::kAddNodes &&
            command.kind != SimulationCommandKind::kReplaceNode &&
            command.kind != SimulationCommandKind::kRemoveNodes &&
            command.kind != SimulationCommandKind::kAssignRole &&
            command.kind != SimulationCommandKind::kRemoveRole;
        const bool needs_direct_node =
            needs_runtime_snapshot &&
            command.kind != SimulationCommandKind::kSetPerfCounters &&
            command.kind != SimulationCommandKind::kSendWalletTransaction &&
            command.kind != SimulationCommandKind::kPartitionNodes &&
            command.kind != SimulationCommandKind::kHealPartition;
        RuntimeNodeSnapshot nodes;
        std::optional<RuntimeWalletSnapshot> command_wallet_snapshot;
        if (needs_runtime_snapshot) {
          if (command.kind == SimulationCommandKind::kSendWalletTransaction) {
            std::unique_lock<std::timed_mutex> publication_lock =
                context.acquire_runtime_publication_lock(command_stop_token);
            nodes = context.node_inventory.Snapshot();
            if (context.wallets_initialized.load(std::memory_order_acquire)) {
              command_wallet_snapshot.emplace(
                  context.runtime_wallet_registry.Snapshot());
            }
          } else {
            nodes = context.node_inventory.Snapshot();
          }
        }
        NodeRuntime unused_node;
        NodeRuntime& node = needs_direct_node ? context.find_node_runtime_by_id(
                                                    nodes, command.node_id)
                                              : unused_node;
        if (command.kind == SimulationCommandKind::kAddNodes) {
          if (!command.node_add) {
            throw std::runtime_error("node-add payload is missing");
          }
          std::lock_guard<std::mutex> topology_lock(
              context.runtime_topology_mutex);
          try {
            RuntimeNodeAddResult added = AddRuntimeNodesTransactional(
                context.options, context.run_root, context.events_path,
                context.chain_spec, context.driver, context.node_inventory,
                context.runtime_wallet_registry, RuntimeNodeAdditionRole::kBase,
                context.block_scheduler.get(), &context.miner_node_ids,
                &context.configured_miner_node_ids_mutex, nullptr,
                *context.peer_connectivity_controller,
                &context.runtime_topology, &context.live_topology_config,
                context.run_process_state, context.lifecycle_epoch,
                *command.node_add, command.operation_control.get(),
                command_stop_token, context.runtime_node_addition_dependencies);
            command_outcome.added_node_ids = std::move(added.added_node_ids);
            command_outcome.inventory_generation = added.inventory_generation;
            command_outcome.final_node_count = added.final_node_count;
            command_outcome.node_capacity = added.node_capacity;
            command_outcome.network_allocation =
                std::move(added.network_allocation);
          } catch (const SimulationNodeResourceUnavailable& error) {
            command.operation_control->RecordNodeResourceFailure(
                error.failure());
            throw;
          }
        } else if (command.kind == SimulationCommandKind::kReplaceNode) {
          if (!command.node_replace) {
            throw std::runtime_error("node-replace payload is missing");
          }
          const auto mining_intent_deadline =
              command.operation_control->absolute_deadline.value_or(
                  std::chrono::steady_clock::now() +
                  SimulationNodeReplaceDefaultExecutionTimeout(
                      *command.node_replace));
          std::optional<RunProcessState::NativeMiningRestartIntent>
              mining_intent =
                  context.run_process_state.TryBeginNativeMiningRestart(
                      command.node_id, mining_intent_deadline,
                      command_stop_token);
          if (!mining_intent) {
            ThrowIfStopRequested(command_stop_token);
            throw std::runtime_error(
                "native mining RPC lock deadline expired before node "
                "replacement: " +
                command.node_id);
          }
          const bool resume_native_miner = mining_intent->native_miner_active;
          const bool resume_scheduled_miner =
              stop_scheduled_miner() || mining_intent->scheduled_miner_paused;
          if (resume_scheduled_miner) {
            auto process_guard = context.run_process_state.Lock();
            context.run_process_state.PauseScheduledMiner(process_guard,
                                                          command.node_id);
          }
          const auto restore_scheduled_miner = [&] {
            if (!resume_scheduled_miner) {
              return;
            }
            if (!context.block_scheduler) {
              throw std::logic_error(
                  "node-replace lost its scheduled block producer");
            }
            const RuntimeNodeSnapshot current_nodes =
                context.node_inventory.Snapshot();
            NodeRuntime& current_node =
                context.find_node_runtime_by_id(current_nodes, command.node_id);
            if (!NodeProcessRunning(current_node)) {
              throw std::runtime_error(
                  "node-replace cannot restore scheduled mining on a "
                  "stopped node");
            }
            context.block_scheduler->StartMiner(command.node_id);
            auto process_guard = context.run_process_state.Lock();
            static_cast<void>(context.run_process_state.ResumeScheduledMiner(
                process_guard, command.node_id));
          };
          try {
            std::lock_guard<std::mutex> topology_lock(
                context.runtime_topology_mutex);
            RuntimeNodeReplaceResult replaced = ReplaceRuntimeNodeTransactional(
                context.options, context.run_root, context.events_path,
                context.driver, context.node_inventory,
                context.runtime_wallet_registry,
                *context.peer_connectivity_controller,
                *context.runtime_topology, context.live_topology_config,
                context.wallet_workloads, context.block_generation_workloads,
                context.wait_until_height_workloads,
                context.wait_for_peers_workloads, context.transaction_tracker,
                context.run_process_state, context.lifecycle_epoch,
                command.node_id, *command.node_replace, resume_native_miner,
                context.chain_spec.default_reward_address,
                command.operation_control.get(), command_stop_token,
                context.runtime_node_replacement_dependencies);
            command_outcome.inventory_generation =
                replaced.inventory_generation;
            command_outcome.final_node_count = replaced.final_node_count;
          } catch (const SimulationCommandOutcomeUnconfirmed&) {
            mining_intent.reset();
            command.operation_control->outcome_unconfirmed.store(
                true, std::memory_order_release);
            context.request_simulation_stop();
            throw;
          } catch (...) {
            const std::exception_ptr failure = std::current_exception();
            mining_intent.reset();
            try {
              restore_scheduled_miner();
            } catch (...) {
              command.operation_control->outcome_unconfirmed.store(
                  true, std::memory_order_release);
              context.request_simulation_stop();
              throw SimulationCommandOutcomeUnconfirmed(
                  "node-replace failed: " + context.exception_message(failure) +
                  "; scheduled mining restoration failed: " +
                  context.exception_message(std::current_exception()));
            }
            std::rethrow_exception(failure);
          }
          mining_intent.reset();
          try {
            restore_scheduled_miner();
          } catch (...) {
            command.operation_control->outcome_unconfirmed.store(
                true, std::memory_order_release);
            context.request_simulation_stop();
            throw SimulationCommandOutcomeUnconfirmed(
                "node-replace committed but scheduled mining restoration "
                "failed: " +
                context.exception_message(std::current_exception()));
          }
        } else if (command.kind == SimulationCommandKind::kRemoveNodes) {
          if (!command.node_remove) {
            throw std::runtime_error("node-remove payload is missing");
          }
          std::lock_guard<std::mutex> topology_lock(
              context.runtime_topology_mutex);
          RuntimeNodeRemoveResult removed = RemoveRuntimeNodesTransactional(
              context.options, context.events_path, context.driver,
              context.node_inventory, context.runtime_wallet_registry,
              *context.peer_connectivity_controller, &context.runtime_topology,
              &context.live_topology_config, context.wallet_workloads,
              context.block_generation_workloads,
              context.wait_until_height_workloads,
              context.wait_for_peers_workloads, context.transaction_tracker,
              *command.node_remove, command.operation_control.get(),
              command_stop_token, context.runtime_node_removal_dependencies);
          command_outcome.removed_node_ids =
              std::move(removed.removed_node_ids);
          command_outcome.inventory_generation = removed.inventory_generation;
          command_outcome.final_node_count = removed.final_node_count;
        } else if (command.kind == SimulationCommandKind::kAssignRole ||
                   command.kind == SimulationCommandKind::kRemoveRole) {
          if (!command.role_mutation) {
            throw std::runtime_error("role mutation payload is missing");
          }
          if (!command.operation_control) {
            throw std::runtime_error(
                "role mutation operation control is missing");
          }
          if (!command.operation_control->absolute_deadline) {
            command.operation_control->absolute_deadline =
                std::chrono::steady_clock::now() +
                SimulationRoleMutationExecutionTimeout(command.kind,
                                                       *command.role_mutation);
          }
          const std::shared_ptr<McpLiveRoleService> role_service =
              context.command_role_service.load(std::memory_order_acquire);
          if (!role_service) {
            throw std::runtime_error(
                "authoritative role mutation service is unavailable");
          }
          try {
            command_outcome.role_mutation =
                ExecuteAndNormalizeSimulationRoleMutation(
                    *role_service, context.options.run_id, command.kind,
                    *command.role_mutation, command_stop_token);
          } catch (const SimulationCancelled&) {
            if (std::chrono::steady_clock::now() >=
                *command.operation_control->absolute_deadline) {
              static_cast<void>(command.operation_control->RequestCancellation(
                  SimulationCommandCancellationCause::kDeadline));
            }
            throw;
          }
        } else if (command.kind == SimulationCommandKind::kExportNodeReport) {
          ExportNodeReport(context.run_root, command);
        } else if (command.kind == SimulationCommandKind::kSetPerfCounters) {
          auto process_guard = context.run_process_state.Lock();
          ApplyPerfCounterCommand(command, nodes, process_guard);
        } else if (command.kind ==
                   SimulationCommandKind::kSendWalletTransaction) {
          if (!command.wallet_send) {
            throw std::runtime_error("wallet send payload is missing");
          }
          if (!context.wallets_initialized.load(std::memory_order_acquire)) {
            throw std::runtime_error(
                "wallet registry is not initialized for live sends");
          }
          const SimulationWalletSend& send = *command.wallet_send;
          if (send.sender_wallet_index == 0U ||
              send.receiver_wallet_index == 0U ||
              send.sender_wallet_index == send.receiver_wallet_index ||
              send.amount_satoshis == 0U || send.timeout_sec == 0U ||
              send.amount_satoshis > std::numeric_limits<std::uint64_t>::max() -
                                         send.fee_satoshis) {
            throw std::runtime_error("wallet send payload is invalid");
          }
          if (!command_wallet_snapshot) {
            throw std::runtime_error(
                "wallet registry snapshot is unavailable for live sends");
          }
          const RuntimeWalletSnapshot& wallet_snapshot =
              *command_wallet_snapshot;
          const SimulationRegistry& registry = wallet_snapshot.registry();
          const WalletIdentity& sender = registry.WalletByIndex(
              static_cast<std::size_t>(send.sender_wallet_index - 1U));
          const WalletIdentity& receiver = registry.WalletByIndex(
              static_cast<std::size_t>(send.receiver_wallet_index - 1U));
          if (sender.wallet_index != send.sender_wallet_index ||
              receiver.wallet_index != send.receiver_wallet_index) {
            throw std::runtime_error(
                "wallet registry index does not match live send payload");
          }
          if (sender.node == 0U || sender.node > nodes.size() ||
              receiver.node == 0U || receiver.node > nodes.size()) {
            throw std::runtime_error(
                "wallet send references an invalid backing node");
          }
          if (sender.address.empty() || receiver.address.empty()) {
            throw std::runtime_error(
                "wallet send requires initialized wallet addresses");
          }
          NodeRuntime& sender_node = nodes[sender.node - 1U];
          if (sender_node.config.id != command.node_id) {
            throw std::runtime_error(
                "wallet send backing node does not match sender wallet");
          }
          ChainWalletTransactionResult transaction;
          {
            auto process_guard = context.run_process_state.Lock();
            RequireNodeRunning(sender_node, process_guard,
                               "operator wallet send");
            if (!sender_node.config.wallet_enabled) {
              throw std::runtime_error(
                  "operator wallet send requires wallet support on " +
                  sender_node.config.id);
            }
          }
          TransactionObservationTracker::Reservation observation_reservation =
              context.transaction_tracker.Reserve(nodes);
          transaction = context.driver.SendWalletTransaction(
              sender_node.config,
              ToChainWalletMode(registry.wallet_initialization()),
              receiver.address, send.amount_satoshis, send.fee_satoshis,
              std::chrono::seconds(send.timeout_sec), command_stop_token);
          const std::string& txid = RequireSingleWalletTransactionId(
              transaction, "operator wallet send");
          WriteEvent(
              context.events_path, context.options.run_id,
              sender_node.config.id,
              SimulationEventKind::kWalletTransactionSubmitted,
              OperatorWalletTransactionDetail(send, sender, receiver,
                                              registry.wallet_initialization(),
                                              transaction, command.sequence));
          context.transaction_tracker.TrackAndWaitForVisibility(
              std::move(observation_reservation), context.options,
              context.events_path, context.driver, nodes,
              TrackedTransaction{
                  .txid = txid,
                  .submission_kind = "operator_wallet_send",
                  .workload_id = {},
                  .workload_index = 0U,
                  .workload_count = 0U,
                  .transaction_index = command.sequence,
                  .transaction_count = std::nullopt,
                  .transaction_rate = std::nullopt,
                  .txid_index = 1U,
                  .submission_node = sender.node,
                  .load_confirmation = nullptr,
              },
              std::chrono::seconds(send.timeout_sec), command_stop_token);
        } else if (command.kind == SimulationCommandKind::kSetResourceLimits) {
          if (!command.resource_limit_patch) {
            throw std::runtime_error("resource limit patch is missing");
          }
          ApplyResourceLimitUpdate(context.options, context.events_path, node,
                                   *command.resource_limit_patch,
                                   context.node_resource_state_mutex, {}, {},
                                   std::nullopt, std::nullopt, std::nullopt,
                                   command.sequence, true);
        } else if (command.kind == SimulationCommandKind::kKillNode) {
          bool was_paused = false;
          {
            auto process_guard = context.run_process_state.Lock();
            was_paused = context.run_process_state.IsPausedScheduledMiner(
                process_guard, command.node_id);
          }
          const bool resume_on_failure = stop_scheduled_miner() || was_paused;
          pid_t pid = -1;
          try {
            {
              auto process_guard = context.run_process_state.Lock();
              if (node.Lifecycle() != NodeRuntimeLifecycle::kRunning) {
                throw std::runtime_error(
                    "node kill conflicts with an active lifecycle "
                    "operation: " +
                    command.node_id + " (state=" +
                    std::string(NodeRuntimeLifecycleName(node.Lifecycle())) +
                    ")");
              }
              if (!node.process.running()) {
                throw std::runtime_error("node process is not running: " +
                                         command.node_id);
              }
              pid = node.process.pid();
              ResetNodePerfCounters(node, process_guard);
              node.SetLifecycle(NodeRuntimeLifecycle::kKilling);
            }
            WriteNodeStateEvent(context.events_path, context.options.run_id,
                                node, NodeRuntimeLifecycle::kKilling);
            if (node.cgroup && node.cgroup->Frozen()) {
              SetNodeFrozen(context.options, context.events_path, node, false,
                            command_stop_token);
            }
            WriteEvent(context.events_path, context.options.run_id,
                       command.node_id,
                       SimulationEventKind::kProcessKillRequested,
                       "pid=" + std::to_string(pid));
            try {
              static_cast<void>(RequestNodeKill(node));
              const auto kill_deadline =
                  std::chrono::steady_clock::now() + std::chrono::seconds(5);
              if (!WaitForNodeProcessExitUntil(node, kill_deadline,
                                               command_stop_token)) {
                throw std::runtime_error("node process survived SIGKILL: " +
                                         command.node_id);
              }
            } catch (const SimulationCancelled&) {
              auto reconciliation_deadline =
                  std::chrono::steady_clock::now() +
                  kSimulationCommandCancellationReconciliation;
              if (command.operation_control &&
                  command.operation_control->absolute_deadline) {
                reconciliation_deadline =
                    std::min(reconciliation_deadline,
                             *command.operation_control->absolute_deadline);
              }
              if (!WaitForNodeProcessExitUntil(node, reconciliation_deadline)) {
                if (command.operation_control) {
                  command.operation_control->outcome_unconfirmed.store(
                      true, std::memory_order_release);
                }
                throw;
              }
              throw;
            } catch (...) {
              bool restored_running = false;
              bool reconciled_killed = false;
              {
                auto process_guard = context.run_process_state.Lock();
                if (node.process.running()) {
                  AttachNodePerfCounters(node, process_guard);
                  node.SetLifecycle(NodeRuntimeLifecycle::kRunning);
                  restored_running = true;
                } else if (node.Lifecycle() == NodeRuntimeLifecycle::kKilling) {
                  ResetNodePerfCounters(node, process_guard);
                  node.SetLifecycle(NodeRuntimeLifecycle::kKilled);
                  reconciled_killed = true;
                }
              }
              if (restored_running) {
                WriteNodeStateEvent(context.events_path, context.options.run_id,
                                    node, NodeRuntimeLifecycle::kRunning);
              } else if (reconciled_killed) {
                WriteEvent(context.events_path, context.options.run_id,
                           command.node_id, SimulationEventKind::kProcessKilled,
                           "pid=" + std::to_string(pid));
                WriteNodeStateEvent(context.events_path, context.options.run_id,
                                    node, NodeRuntimeLifecycle::kKilled);
              }
              throw;
            }
            WriteEvent(context.events_path, context.options.run_id,
                       command.node_id, SimulationEventKind::kProcessKilled,
                       "pid=" + std::to_string(pid));
            {
              auto process_guard = context.run_process_state.Lock();
              node.SetLifecycle(NodeRuntimeLifecycle::kKilled);
            }
            WriteNodeStateEvent(context.events_path, context.options.run_id,
                                node, NodeRuntimeLifecycle::kKilled);
          } catch (...) {
            bool restored_running = false;
            bool reconciled_killed = false;
            const bool outcome_unconfirmed =
                command.operation_control &&
                command.operation_control->outcome_unconfirmed.load(
                    std::memory_order_acquire);
            {
              auto process_guard = context.run_process_state.Lock();
              if (node.Lifecycle() == NodeRuntimeLifecycle::kKilling &&
                  node.process.running() && !outcome_unconfirmed) {
                AttachNodePerfCounters(node, process_guard);
                node.SetLifecycle(NodeRuntimeLifecycle::kRunning);
                restored_running = true;
              } else if (node.Lifecycle() == NodeRuntimeLifecycle::kKilling &&
                         !node.process.running()) {
                if (command.operation_control) {
                  command.operation_control->outcome_unconfirmed.store(
                      false, std::memory_order_release);
                }
                ResetNodePerfCounters(node, process_guard);
                node.SetLifecycle(NodeRuntimeLifecycle::kKilled);
                reconciled_killed = true;
              }
            }
            if (restored_running) {
              WriteNodeStateEvent(context.events_path, context.options.run_id,
                                  node, NodeRuntimeLifecycle::kRunning);
            } else if (reconciled_killed) {
              WriteEvent(context.events_path, context.options.run_id,
                         command.node_id, SimulationEventKind::kProcessKilled,
                         "pid=" + std::to_string(pid));
              WriteNodeStateEvent(context.events_path, context.options.run_id,
                                  node, NodeRuntimeLifecycle::kKilled);
            }
            const bool node_running = NodeProcessRunning(node);
            if (resume_on_failure && node_running && !outcome_unconfirmed) {
              context.block_scheduler->StartMiner(command.node_id);
              auto process_guard = context.run_process_state.Lock();
              static_cast<void>(context.run_process_state.ResumeScheduledMiner(
                  process_guard, command.node_id));
            } else if (!node_running) {
              auto process_guard = context.run_process_state.Lock();
              static_cast<void>(context.run_process_state.ResumeScheduledMiner(
                  process_guard, command.node_id));
              context.run_process_state.RemoveActiveNativeMiner(
                  process_guard, command.node_id);
            }
            throw;
          }
          {
            auto process_guard = context.run_process_state.Lock();
            static_cast<void>(context.run_process_state.ResumeScheduledMiner(
                process_guard, command.node_id));
            context.run_process_state.RemoveActiveNativeMiner(process_guard,
                                                              command.node_id);
          }
        } else if (command.kind == SimulationCommandKind::kStopNode) {
          bool was_paused = false;
          {
            auto process_guard = context.run_process_state.Lock();
            was_paused = context.run_process_state.IsPausedScheduledMiner(
                process_guard, command.node_id);
          }
          const bool resume_on_failure = stop_scheduled_miner() || was_paused;
          try {
            StopNodeProcess(context.options, context.events_path,
                            context.driver, node, command_stop_token, false,
                            command.operation_control.get());
          } catch (...) {
            const bool node_running = NodeProcessRunning(node);
            const bool outcome_unconfirmed =
                command.operation_control &&
                command.operation_control->outcome_unconfirmed.load(
                    std::memory_order_acquire);
            if (resume_on_failure && node_running && !outcome_unconfirmed) {
              context.block_scheduler->StartMiner(command.node_id);
              auto process_guard = context.run_process_state.Lock();
              static_cast<void>(context.run_process_state.ResumeScheduledMiner(
                  process_guard, command.node_id));
            } else if (!node_running) {
              auto process_guard = context.run_process_state.Lock();
              static_cast<void>(context.run_process_state.ResumeScheduledMiner(
                  process_guard, command.node_id));
              context.run_process_state.RemoveActiveNativeMiner(
                  process_guard, command.node_id);
            }
            throw;
          }
          {
            auto process_guard = context.run_process_state.Lock();
            static_cast<void>(context.run_process_state.ResumeScheduledMiner(
                process_guard, command.node_id));
            context.run_process_state.RemoveActiveNativeMiner(process_guard,
                                                              command.node_id);
          }
        } else if (command.kind == SimulationCommandKind::kRestartNode) {
          bool was_paused = false;
          bool resume_native_miner = false;
          bool native_mining_rpc_attempted = false;
          NodeRestartAdmission restart_admission;
          SimulationCommandControl local_restart_control;
          SimulationCommandControl* restart_control =
              command.operation_control ? command.operation_control.get()
                                        : &local_restart_control;
          const auto resume_intent_deadline =
              restart_control->absolute_deadline.value_or(
                  std::chrono::steady_clock::now() +
                  std::chrono::seconds(context.options.ready_timeout_sec));
          std::optional<RunProcessState::NativeMiningRestartIntent>
              restart_intent =
                  context.run_process_state.TryBeginNativeMiningRestart(
                      command.node_id, resume_intent_deadline,
                      command_stop_token);
          if (!restart_intent) {
            ThrowIfStopRequested(command_stop_token);
            throw std::runtime_error(
                "native mining RPC lock deadline expired before node "
                "restart: " +
                command.node_id);
          }
          was_paused = restart_intent->scheduled_miner_paused;
          resume_native_miner = restart_intent->native_miner_active;
          const bool resume_scheduled_miner =
              stop_scheduled_miner() || was_paused;
          if (resume_scheduled_miner) {
            auto process_guard = context.run_process_state.Lock();
            context.run_process_state.PauseScheduledMiner(process_guard,
                                                          command.node_id);
          }
          try {
            if (!RestartNode(context.options, context.events_path,
                             context.driver,
                             *context.peer_connectivity_controller, node,
                             context.lifecycle_epoch, context.start_node,
                             command_stop_token, "requested", restart_control,
                             &restart_admission)) {
              throw std::runtime_error(
                  "operator restart reached node stop_time before "
                  "completion: " +
                  node.config.id);
            }
            restart_intent.reset();
            if (resume_native_miner) {
              if (!StartNativeMiningForCurrentProcess(
                      context.driver, node, context.run_process_state,
                      context.chain_spec.default_reward_address,
                      command_stop_token, "operator native mining restart",
                      restart_control->absolute_deadline,
                      &native_mining_rpc_attempted)) {
                throw std::runtime_error(
                    "node process changed during native mining "
                    "restart: " +
                    node.config.id);
              }
            }
          } catch (...) {
            restart_intent.reset();
            bool resume_miner_after_failure = true;
            auto reconciliation_deadline =
                std::chrono::steady_clock::now() +
                kSimulationCommandCancellationReconciliation;
            if (restart_control->absolute_deadline) {
              reconciliation_deadline = std::min(
                  reconciliation_deadline, *restart_control->absolute_deadline);
            }
            if (command_stop_token.stop_requested() &&
                restart_admission.admitted) {
              const SimulationNodeRestartReconciliation reconciliation =
                  ReconcileCancelledSimulationNodeRestart(
                      restart_admission.process,
                      restart_control->restart_phase.load(
                          std::memory_order_acquire),
                      reconciliation_deadline,
                      [&] {
                        auto process_guard = context.run_process_state.Lock();
                        return SimulationNodeProcessObservation{
                            .running = node.process.running(),
                            .pid = node.process.pid(),
                            .restart_count = node.RestartCount(),
                        };
                      },
                      [&](const SimulationNodeProcessObservation& expected) {
                        bool requested = false;
                        try {
                          {
                            auto process_guard =
                                context.run_process_state.Lock();
                            if (node.process.running() &&
                                node.process.pid() == expected.pid &&
                                node.RestartCount() == expected.restart_count) {
                              requested = node.process.RequestKill();
                            }
                          }
                        } catch (const std::exception& error) {
                          BBP_LOG(error)
                              << "failed to stop unready replacement "
                              << expected.pid << " for " << command.node_id
                              << ": " << error.what();
                          return false;
                        }
                        if (requested) {
                          try {
                            WriteEvent(
                                context.events_path, context.options.run_id,
                                command.node_id,
                                SimulationEventKind::kProcessKillRequested,
                                "restart cancellation "
                                "reconciliation pid=" +
                                    std::to_string(expected.pid));
                          } catch (const std::exception& error) {
                            BBP_LOG(error)
                                << "failed to record unready replacement "
                                   "stop for "
                                << command.node_id << ": " << error.what();
                          }
                        }
                        return requested;
                      });
              const bool unchanged =
                  reconciliation ==
                  SimulationNodeRestartReconciliation::kUnchanged;
              const bool stopped =
                  reconciliation ==
                  SimulationNodeRestartReconciliation::kStopped;
              const bool replacement_ready =
                  reconciliation ==
                  SimulationNodeRestartReconciliation::kReplacementReady;
              NodeRuntimeLifecycle observed_lifecycle;
              {
                auto process_guard = context.run_process_state.Lock();
                observed_lifecycle = node.Lifecycle();
              }
              const NodeRuntimeLifecycle reconciled_state =
                  ReconciledSimulationNodeRestartLifecycle(
                      restart_admission.lifecycle, observed_lifecycle,
                      reconciliation);
              bool state_changed = false;
              {
                auto process_guard = context.run_process_state.Lock();
                if (reconciled_state == NodeRuntimeLifecycle::kRunning) {
                  AttachNodePerfCounters(node, process_guard);
                } else {
                  ResetNodePerfCounters(node, process_guard);
                }
                if (node.Lifecycle() != reconciled_state) {
                  node.SetLifecycle(reconciled_state);
                  state_changed = true;
                }
              }
              if (!unchanged && !stopped && !replacement_ready) {
                resume_miner_after_failure = false;
                restart_control->outcome_unconfirmed.store(
                    true, std::memory_order_release);
              }
              if (state_changed) {
                WriteNodeStateEvent(context.events_path, context.options.run_id,
                                    node, reconciled_state);
              }
            }
            if (resume_native_miner && restart_admission.admitted) {
              auto mining_rpc_guard =
                  context.run_process_state.TryLockNativeMiningRpcUntil(
                      reconciliation_deadline);
              bool original_running_generation_unchanged = false;
              bool replacement_running = false;
              if (mining_rpc_guard) {
                auto process_guard = context.run_process_state.Lock();
                original_running_generation_unchanged =
                    restart_admission.process.running &&
                    node.process.running() &&
                    node.process.pid() == restart_admission.process.pid &&
                    node.RestartCount() ==
                        restart_admission.process.restart_count;
                replacement_running = !original_running_generation_unchanged &&
                                      node.process.running();
              }

              bool replacement_mining_confirmed_inactive =
                  mining_rpc_guard &&
                  (!replacement_running || !native_mining_rpc_attempted);
              std::string compensation_error;
              if (mining_rpc_guard && replacement_running &&
                  native_mining_rpc_attempted) {
                replacement_mining_confirmed_inactive =
                    StopNativeMiningBeforeDeadline(context.driver, node,
                                                   reconciliation_deadline,
                                                   &compensation_error);
              } else if (!mining_rpc_guard) {
                compensation_error = "native mining RPC lock deadline expired";
              }

              if (mining_rpc_guard) {
                auto process_guard = context.run_process_state.Lock();
                context.run_process_state
                    .ReconcileActiveNativeMinerAfterRestartFailure(
                        *mining_rpc_guard, process_guard, command.node_id,
                        original_running_generation_unchanged,
                        replacement_mining_confirmed_inactive);
              }
              if (!original_running_generation_unchanged &&
                  !replacement_mining_confirmed_inactive) {
                resume_miner_after_failure = false;
                restart_control->outcome_unconfirmed.store(
                    true, std::memory_order_release);
                BBP_LOG(warning)
                    << "native mining state remains active after failed "
                       "restart reconciliation for "
                    << command.node_id << ": " << compensation_error;
              }
            }
            if (!command.operation_control &&
                restart_control->outcome_unconfirmed.load(
                    std::memory_order_acquire)) {
              context.request_simulation_stop();
              throw SimulationCommandOutcomeUnconfirmed(
                  context.exception_message(std::current_exception()));
            }
            if (resume_scheduled_miner && resume_miner_after_failure &&
                NodeProcessRunning(node)) {
              context.block_scheduler->StartMiner(command.node_id);
              auto process_guard = context.run_process_state.Lock();
              static_cast<void>(context.run_process_state.ResumeScheduledMiner(
                  process_guard, command.node_id));
            }
            throw;
          }
          if (resume_scheduled_miner) {
            context.block_scheduler->StartMiner(command.node_id);
            auto process_guard = context.run_process_state.Lock();
            static_cast<void>(context.run_process_state.ResumeScheduledMiner(
                process_guard, command.node_id));
          }
        } else if (command.kind == SimulationCommandKind::kFreezeNode) {
          const bool resume_on_thaw = stop_scheduled_miner();
          try {
            RequireNodeRunning(node, "operator freeze");
            SetNodeFrozen(context.options, context.events_path, node, true,
                          command_stop_token);
          } catch (...) {
            if (resume_on_thaw) {
              context.block_scheduler->StartMiner(command.node_id);
            }
            throw;
          }
          if (resume_on_thaw) {
            auto process_guard = context.run_process_state.Lock();
            context.run_process_state.PauseScheduledMiner(process_guard,
                                                          command.node_id);
          }
        } else if (command.kind == SimulationCommandKind::kThawNode) {
          SetNodeFrozen(context.options, context.events_path, node, false,
                        command_stop_token);
          bool resume_scheduled_miner = false;
          {
            auto process_guard = context.run_process_state.Lock();
            resume_scheduled_miner =
                context.run_process_state.ResumeScheduledMiner(process_guard,
                                                               command.node_id);
          }
          if (resume_scheduled_miner) {
            context.block_scheduler->StartMiner(command.node_id);
          }
        } else if (command.kind == SimulationCommandKind::kGenerateBlocks) {
          if (!command.block_count || *command.block_count == 0U) {
            throw std::runtime_error(
                "generate-blocks command requires a positive count");
          }
          RequireNodeRunning(node, "operator block generation");
          const std::uint64_t start_height =
              context.driver.ReadMetrics(node.config, command_stop_token)
                  .height;
          const std::vector<std::string> hashes = GenerateBlocksSerialized(
              context.block_generation_mutex, context.driver, node.config,
              *command.block_count, context.chain_spec.default_reward_address,
              command_stop_token);
          RecordGeneratedBlocks(context.driver, node, hashes,
                                command_stop_token);
          if (start_height >
              std::numeric_limits<std::uint64_t>::max() - hashes.size()) {
            throw std::runtime_error(
                "generated block target height overflows uint64");
          }
          const auto node_iter = std::find_if(
              nodes.begin(), nodes.end(), [&](const NodeRuntime& candidate) {
                return candidate.config.id == command.node_id;
              });
          const std::uint32_t one_based_node =
              static_cast<std::uint32_t>(
                  std::distance(nodes.begin(), node_iter)) +
              1U;
          WriteEvent(
              context.events_path, context.options.run_id, command.node_id,
              SimulationEventKind::kGeneratedBlocks,
              GeneratedBlocksDetail(
                  0U, 0U, one_based_node, start_height,
                  start_height + static_cast<std::uint64_t>(hashes.size()),
                  hashes, context.chain_spec.default_reward_address,
                  command.sequence));
        } else if (command.kind ==
                   SimulationCommandKind::kSetNetworkCondition) {
          if (!command.network_condition) {
            throw std::runtime_error(
                "set-network-condition command requires a condition");
          }
          QdiscInfo qdisc;
          NodeVethConfig updated_network;
          {
            std::lock_guard<std::mutex> lock(context.node_network_state_mutex);
            qdisc = ReplaceNodeNetworkConditionTransactional(
                &node, *command.network_condition, command_stop_token);
            updated_network = *node.network;
          }
          WriteEvent(context.events_path, context.options.run_id,
                     command.node_id,
                     SimulationEventKind::kNetworkConditionUpdated,
                     NetworkConditionVerificationDetail(
                         updated_network, qdisc, 0U, 0U, command.sequence));
        } else if (command.kind == SimulationCommandKind::kBlockNetworkFlow ||
                   command.kind == SimulationCommandKind::kUnblockNetworkFlow) {
          if (!command.network_flow) {
            throw std::runtime_error(
                "network flow command requires a typed flow");
          }
          const auto node_iter = std::find_if(
              nodes.begin(), nodes.end(), [&](const NodeRuntime& candidate) {
                return candidate.config.id == command.node_id;
              });
          if (node_iter == nodes.end()) {
            throw std::runtime_error("unknown network flow node: " +
                                     command.node_id);
          }
          const std::size_t zero_based_node =
              static_cast<std::size_t>(std::distance(nodes.begin(), node_iter));
          if (zero_based_node > std::numeric_limits<std::uint32_t>::max()) {
            throw std::runtime_error("network flow node index exceeds uint32");
          }
          NetworkBlockRule rule;
          NetworkBlockMutationResult result;
          {
            std::lock_guard<std::mutex> lock(context.node_network_state_mutex);
            if (command.network_flow->dst_address.empty()) {
              if (command.kind != SimulationCommandKind::kUnblockNetworkFlow ||
                  command.network_flow->handle == 0U) {
                throw std::runtime_error(
                    "handle-only network flow command must be unblock");
              }
              const std::optional<NetworkBlockRule> existing =
                  NetworkBlockRuleForHandle(node, command.network_flow->handle);
              if (!existing) {
                throw std::runtime_error(
                    "active network block rule handle was not found: " +
                    std::to_string(command.network_flow->handle));
              }
              rule = *existing;
            } else {
              rule.src_address = command.network_flow->src_address;
              rule.src_port = command.network_flow->src_port;
              rule.dst_address = command.network_flow->dst_address;
              rule.dst_port = command.network_flow->dst_port;
              rule.handle = command.network_flow->handle;
            }
            rule.node_index = static_cast<std::uint32_t>(zero_based_node);
            if (rule.handle == 0U) {
              rule.handle = StableRuleHandle(rule);
            }
            result = MutateNetworkBlockRuleTransactional(
                node, rule,
                command.kind == SimulationCommandKind::kUnblockNetworkFlow,
                command_stop_token);
          }
          WriteEvent(context.events_path, context.options.run_id,
                     command.node_id,
                     command.kind == SimulationCommandKind::kUnblockNetworkFlow
                         ? SimulationEventKind::kNetworkBlockRemoved
                         : SimulationEventKind::kNetworkBlockApplied,
                     NetworkBlockRuleDetail(node, rule, result.existed_before,
                                            result.present_after, 0U, 0U,
                                            command.sequence));
        } else if (command.kind == SimulationCommandKind::kPartitionNodes ||
                   command.kind == SimulationCommandKind::kHealPartition) {
          if (!command.partition) {
            throw std::runtime_error(
                "partition command requires a typed partition target");
          }
          const NetworkPartitionRule partition =
              RuntimePartitionRule(*command.partition, nodes);
          ApplyRuntimeNetworkPartition(
              context.options, context.events_path, nodes,
              context.node_network_state_mutex, partition,
              command.kind == SimulationCommandKind::kHealPartition, 0U, 0U,
              command_stop_token, command.sequence);
        } else if (command.kind == SimulationCommandKind::kSetResourceProfile ||
                   command.kind == SimulationCommandKind::kSetNetworkProfile) {
          if (!command.profile || command.profile->empty()) {
            throw std::runtime_error("profile command requires a profile name");
          }
          const auto node_iter = std::find_if(
              nodes.begin(), nodes.end(), [&](const NodeRuntime& candidate) {
                return candidate.config.id == command.node_id;
              });
          const std::uint32_t one_based_node =
              static_cast<std::uint32_t>(
                  std::distance(nodes.begin(), node_iter)) +
              1U;
          const ProfileSwitchWorkload workload{
              .nodes = {one_based_node},
              .node_ids = {command.node_id},
              .profile = *command.profile,
          };
          if (command.kind == SimulationCommandKind::kSetResourceProfile) {
            if (!context.options.resource_profiles.contains(*command.profile)) {
              throw std::runtime_error("unknown resource profile: " +
                                       *command.profile);
            }
            ApplyResourceProfileSwitch(context.options, context.events_path,
                                       nodes, context.node_resource_state_mutex,
                                       workload, 0U, 0U, command_stop_token);
          } else {
            if (!context.options.network_profiles.contains(*command.profile)) {
              throw std::runtime_error("unknown network profile: " +
                                       *command.profile);
            }
            ApplyNetworkProfileSwitch(context.options, context.events_path,
                                      nodes, context.node_network_state_mutex,
                                      workload, 0U, 0U, command_stop_token);
          }
        } else {
          try {
            context.chain_command_executor->Execute(command,
                                                    command_stop_token);
          } catch (const PeerMutationOutcomeUnconfirmed&) {
            if (command.operation_control) {
              command.operation_control->outcome_unconfirmed.store(
                  true, std::memory_order_release);
            } else {
              context.request_simulation_stop();
            }
            throw;
          }
        }
        try {
          WriteEvent(context.events_path, context.options.run_id,
                     command.node_id,
                     SimulationEventKind::kOperatorCommandCompleted,
                     SimulationCommandDetail(command, {}, &command_outcome));
        } catch (const std::exception& error) {
          if ((command.kind == SimulationCommandKind::kAddNodes &&
               !command_outcome.added_node_ids.empty()) ||
              (command.kind == SimulationCommandKind::kReplaceNode &&
               command_outcome.inventory_generation.has_value()) ||
              (command.kind == SimulationCommandKind::kRemoveNodes &&
               !command_outcome.removed_node_ids.empty()) ||
              ((command.kind == SimulationCommandKind::kAssignRole ||
                command.kind == SimulationCommandKind::kRemoveRole) &&
               command_outcome.role_mutation.has_value())) {
            throw SimulationCommandOutcomeUnconfirmed(
                "runtime mutation published but completion evidence "
                "failed: " +
                std::string(error.what()));
          }
          throw;
        }
        BBP_LOG(info) << "command #" << command.sequence << " "
                      << SimulationCommandKindName(command.kind) << " for "
                      << command.node_id << " completed";
        return command_outcome;
      },
      [context](const SimulationCommand& command, std::string_view error) {
        WriteEvent(context.events_path, context.options.run_id, command.node_id,
                   SimulationEventKind::kOperatorCommandFailed,
                   SimulationCommandDetail(command, error));
        BBP_LOG(warning) << "command #" << command.sequence << " "
                         << SimulationCommandKindName(command.kind) << " for "
                         << command.node_id << " failed: " << error;
      },
      [context, mutation_admission](const SimulationCommand& command,
                                    const SimulationCommandOutcome& outcome) {
        const std::unique_lock<std::timed_mutex> mutation_lock(
            std::move(*mutation_admission));
        SimulationCommandOutcome authoritative_outcome = outcome;
        if (outcome.state ==
            SimulationCommandOutcomeState::kOutcomeUnconfirmed) {
          BBP_LOG(error) << "command #" << command.sequence << " "
                         << SimulationCommandKindName(command.kind) << " for "
                         << command.node_id << " has an unconfirmed outcome: "
                         << outcome.error.value_or("no diagnostic");
          if (command.operation_control) {
            command.operation_control->outcome_unconfirmed.store(
                true, std::memory_order_release);
          }
          context.request_simulation_stop();
        }
        const RuntimeNodeSnapshot nodes = context.node_inventory.Snapshot();
        const auto node = std::find_if(
            nodes.begin(), nodes.end(), [&](const NodeRuntime& candidate) {
              return candidate.config.id == command.node_id;
            });
        if (node != nodes.end()) {
          auto process_guard = context.run_process_state.Lock();
          std::string lifecycle(NodeRuntimeLifecycleName(node->Lifecycle()));
          std::transform(lifecycle.begin(), lifecycle.end(), lifecycle.begin(),
                         [](char character) {
                           return character >= 'A' && character <= 'Z'
                                      ? static_cast<char>(character - 'A' + 'a')
                                      : character;
                         });
          authoritative_outcome.node_lifecycle = std::move(lifecycle);
        }
        std::optional<std::string> scheduled_error;
        if (authoritative_outcome.state !=
            SimulationCommandOutcomeState::kSucceeded) {
          scheduled_error = authoritative_outcome.error.value_or(
              "scheduled command did not succeed");
        }
        context.record_scheduled_command_outcome(
            command, scheduled_error
                         ? std::optional<std::string_view>(*scheduled_error)
                         : std::nullopt);
        context.mcp_application.RecordCommandOutcome(command,
                                                     authoritative_outcome);
      });
}

}  // namespace bbp::simulator_app_internal
