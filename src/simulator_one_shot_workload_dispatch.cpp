#include "simulator_one_shot_workload_dispatch.h"

#include <algorithm>
#include <cstdint>
#include <exception>
#include <functional>
#include <mutex>
#include <stdexcept>
#include <stop_token>
#include <string>

#include "bbp/runtime_node_inventory.h"
#include "bbp/runtime_wallet_registry.h"
#include "bbp/simulation_cancelled.h"
#include "bbp/simulation_command.h"
#include "bbp/simulation_event_kind.h"
#include "bbp/simulator/node_runtime.h"
#include "bbp/simulator/options.h"
#include "bbp/simulator/scenario_workload.h"
#include "bbp/simulator/workload_kind.h"
#include "simulator_block_generation_boundary.h"
#include "simulator_cancellable_waiting.h"
#include "simulator_event_writing.h"
#include "simulator_metrics_sampling.h"
#include "simulator_network_block_application.h"
#include "simulator_network_condition_application.h"
#include "simulator_network_event_details.h"
#include "simulator_network_partition_application.h"
#include "simulator_node_process_state.h"
#include "simulator_peer_churn_workloads.h"
#include "simulator_profile_switching.h"
#include "simulator_raw_transaction_workload.h"
#include "simulator_resource_limit_orchestration.h"
#include "simulator_resource_pressure_workload.h"
#include "simulator_runtime_node_freeze.h"
#include "simulator_scheduled_command_event_details.h"
#include "simulator_topology_edge_workload.h"
#include "simulator_transaction_observation_tracking.h"
#include "simulator_workload_mutation_error.h"

namespace bbp::simulator_app_internal {

void DispatchOneShotWorkload(
    const OneShotWorkloadContext& context,
    const ScenarioWorkload& scenario_workload, const RuntimeNodeSnapshot& nodes,
    std::uint32_t action_index, std::uint32_t action_count,
    std::stop_token operation_stop_token,
    SimulationCommandControl* cancellation_commit_control) {
  if (!IsOneShotWorkloadKind(scenario_workload.kind)) {
    throw std::logic_error("one-shot dispatcher received a lifecycle workload");
  }
  ThrowIfStopRequested(operation_stop_token);
  std::function<void()> authorize_mutation;
  if (cancellation_commit_control != nullptr) {
    authorize_mutation = [&] {
      if (cancellation_commit_control->TryBeginCommit()) {
        return;
      }
      if (cancellation_commit_control->CommitPhase() ==
          SimulationCommandCommitPhase::kCancelled) {
        throw SimulationCancelled();
      }
      ThrowIfStopRequested(operation_stop_token);
      throw std::logic_error(
          "one-shot workload mutation admission reached an unexpected "
          "commit phase");
    };
  }
  switch (scenario_workload.kind) {
    case WorkloadKind::kConnectPeer:
      ApplyConnectPeerWorkload(
          context.options, context.events_path, context.driver,
          *context.peer_connectivity_controller, nodes,
          scenario_workload.connect_peer, action_index, action_count,
          operation_stop_token, cancellation_commit_control);
      break;
    case WorkloadKind::kDisconnectPeer:
      ApplyDisconnectPeerWorkload(
          context.options, context.events_path, context.driver,
          *context.peer_connectivity_controller, nodes,
          scenario_workload.disconnect_peer, action_index, action_count,
          operation_stop_token, cancellation_commit_control);
      break;
    case WorkloadKind::kRestartNode: {
      const RestartNodeWorkload& workload = scenario_workload.restart_node;
      const auto selected = std::find_if(
          nodes.begin(), nodes.end(), [&](const NodeRuntime& candidate) {
            return candidate.config.id == workload.node_id;
          });
      if (selected == nodes.end()) {
        throw std::runtime_error(
            "restart_node workload references an inactive node id: " +
            workload.node_id);
      }
      NodeRuntime& node = *selected;
      if (!RestartNode(context.options, context.events_path, context.driver,
                       *context.peer_connectivity_controller, node,
                       context.lifecycle_epoch, context.start_node,
                       operation_stop_token, "requested",
                       cancellation_commit_control, nullptr, true, nullptr,
                       true, cancellation_commit_control, context.stop_token)) {
        throw std::runtime_error(
            "restart_node workload reached node stop_time before "
            "completion: " +
            node.config.id);
      }
      WriteEvent(context.events_path, context.options.run_id, node.config.id,
                 SimulationEventKind::kNodeRestarted,
                 RestartNodeWorkloadDetail(action_index, action_count,
                                           workload.node, node.RestartCount()));
      if (cancellation_commit_control != nullptr) {
        cancellation_commit_control->MarkCommitted();
      }
      break;
    }
    case WorkloadKind::kFreezeNode: {
      const FreezeNodeWorkload& workload = scenario_workload.freeze_node;
      NodeRuntime& node = RequireRuntimeNodeNumber(nodes, workload.node,
                                                   "freeze_node workload");
      RequireNodeRunning(node, "freeze_node workload");
      FreezeNodeForDuration(context.options, context.events_path, node,
                            workload.duration_ms, operation_stop_token);
      try {
        WriteEvent(
            context.events_path, context.options.run_id, node.config.id,
            SimulationEventKind::kNodeFreezeCompleted,
            FreezeNodeWorkloadDetail(action_index, action_count, workload.node,
                                     workload.duration_ms));
      } catch (...) {
        ThrowWorkloadMutationOutcomeUnconfirmed(
            "node freeze completed without a publishable workload "
            "outcome",
            std::current_exception());
      }
      break;
    }
    case WorkloadKind::kUpdateResourceLimits: {
      const ResourceLimitUpdateWorkload& workload =
          scenario_workload.update_resource_limits;
      NodeRuntime& node = nodes[workload.node - 1U];
      ApplyResourceLimitUpdate(
          context.options, context.events_path, node, workload.patch,
          context.node_resource_state_mutex, operation_stop_token,
          authorize_mutation, action_index, action_count, workload.node);
      break;
    }
    case WorkloadKind::kSetResourceProfile:
      ApplyResourceProfileSwitch(
          context.options, context.events_path, nodes,
          context.node_resource_state_mutex, scenario_workload.profile_switch,
          action_index, action_count, operation_stop_token, authorize_mutation);
      break;
    case WorkloadKind::kSetNetworkProfile:
      ApplyNetworkProfileSwitch(context.options, context.events_path, nodes,
                                context.node_network_state_mutex,
                                scenario_workload.profile_switch, action_index,
                                action_count, operation_stop_token);
      break;
    case WorkloadKind::kResourcePressure:
      ApplyResourcePressureWorkload(
          context.options, context.events_path, context.metrics_path,
          context.driver, nodes, context.node_network_state_mutex,
          context.node_resource_state_mutex, context.run_process_state,
          context.runtime_wallet_registry.Snapshot().registry().topology(),
          scenario_workload.resource_pressure, action_index, action_count,
          operation_stop_token);
      break;
    case WorkloadKind::kSetNetworkCondition: {
      const NetworkConditionWorkload& workload =
          scenario_workload.network_condition;
      NodeRuntime& node = nodes[workload.node - 1U];
      QdiscInfo qdisc;
      NodeVethConfig updated_network;
      {
        std::lock_guard<std::mutex> lock(context.node_network_state_mutex);
        qdisc = ReplaceNodeNetworkConditionTransactional(
            &node, workload.condition, operation_stop_token);
        try {
          updated_network = *node.network;
        } catch (...) {
          ThrowWorkloadMutationOutcomeUnconfirmed(
              "network condition update completed without coherent "
              "runtime evidence",
              std::current_exception());
        }
      }
      try {
        WriteEvent(context.events_path, context.options.run_id, node.config.id,
                   SimulationEventKind::kNetworkConditionUpdated,
                   NetworkConditionVerificationDetail(
                       updated_network, qdisc, action_index, action_count));
      } catch (...) {
        ThrowWorkloadMutationOutcomeUnconfirmed(
            "network condition update completed without a publishable "
            "outcome",
            std::current_exception());
      }
      break;
    }
    case WorkloadKind::kBlockNetworkFlow:
    case WorkloadKind::kUnblockNetworkFlow: {
      const NetworkBlockRule& rule = scenario_workload.network_block.rule;
      NodeRuntime& node = nodes[rule.node_index];
      NetworkBlockMutationResult result;
      {
        std::lock_guard<std::mutex> lock(context.node_network_state_mutex);
        result = MutateNetworkBlockRuleTransactional(
            node, rule,
            scenario_workload.kind == WorkloadKind::kUnblockNetworkFlow,
            operation_stop_token);
      }
      try {
        WriteEvent(context.events_path, context.options.run_id, node.config.id,
                   scenario_workload.kind == WorkloadKind::kUnblockNetworkFlow
                       ? SimulationEventKind::kNetworkBlockRemoved
                       : SimulationEventKind::kNetworkBlockApplied,
                   NetworkBlockRuleDetail(node, rule, result.existed_before,
                                          result.present_after, action_index,
                                          action_count));
      } catch (...) {
        ThrowWorkloadMutationOutcomeUnconfirmed(
            "network flow mutation completed without a publishable "
            "outcome",
            std::current_exception());
      }
      break;
    }
    case WorkloadKind::kPartitionNodes:
      ApplyRuntimeNetworkPartition(
          context.options, context.events_path, nodes,
          context.node_network_state_mutex,
          scenario_workload.network_partition.partition, false, action_index,
          action_count, operation_stop_token);
      break;
    case WorkloadKind::kHealPartition:
      ApplyRuntimeNetworkPartition(
          context.options, context.events_path, nodes,
          context.node_network_state_mutex,
          scenario_workload.network_partition.partition, true, action_index,
          action_count, operation_stop_token);
      break;
    case WorkloadKind::kSetEdgeCondition:
    case WorkloadKind::kActivateEdge:
    case WorkloadKind::kDeactivateEdge:
    case WorkloadKind::kRestoreEdge: {
      std::lock_guard<std::mutex> topology_lock(context.runtime_topology_mutex);
      ApplyTopologyEdgeWorkload(
          context.options, context.events_path, context.chain_spec,
          context.driver, *context.peer_connectivity_controller,
          *context.runtime_topology, nodes, context.node_network_state_mutex,
          scenario_workload.topology_edge, scenario_workload.kind, action_index,
          action_count, operation_stop_token);
      break;
    }
    case WorkloadKind::kSendRawTransaction:
      ApplySendRawTransactionWorkload(
          context.options, context.events_path, context.driver,
          context.block_generation_mutex, nodes, context.transaction_tracker,
          scenario_workload.send_raw_transaction, action_index, action_count,
          operation_stop_token, cancellation_commit_control);
      break;
    case WorkloadKind::kCheckpoint: {
      const CheckpointWorkload& workload = scenario_workload.checkpoint;
      const std::string name =
          workload.name.empty() ? "checkpoint-" + std::to_string(action_index)
                                : workload.name;
      context.transaction_tracker.ObserveAll(
          context.options, context.events_path, context.driver, nodes,
          operation_stop_token);
      const RuntimeWalletSnapshot checkpoint_registry =
          context.runtime_wallet_registry.Snapshot();
      ThrowIfStopRequested(operation_stop_token);
      try {
        const std::uint32_t node_metric_samples = WriteMetricsSnapshot(
            context.metrics_path, context.options, context.driver, nodes,
            context.run_process_state,
            {context.node_network_state_mutex,
             context.node_resource_state_mutex},
            {}, {}, operation_stop_token,
            &checkpoint_registry.registry().topology());
        const std::uint32_t wallet_metric_samples = WriteWalletMetricsSnapshot(
            context.wallet_metrics_path, context.options, context.driver, nodes,
            checkpoint_registry.registry(), {}, operation_stop_token);
        WriteEvent(context.events_path, context.options.run_id, "sim",
                   SimulationEventKind::kCheckpointRecorded,
                   CheckpointWorkloadDetail(action_index, action_count, name,
                                            node_metric_samples,
                                            wallet_metric_samples));
      } catch (const SimulationCancelled&) {
        throw;
      } catch (...) {
        ThrowWorkloadMutationOutcomeUnconfirmed(
            "checkpoint did not reach a publishable completion "
            "boundary",
            std::current_exception());
      }
      break;
    }
    case WorkloadKind::kBlockGeneration:
    case WorkloadKind::kWaitUntilHeight:
    case WorkloadKind::kWaitForPeers:
    case WorkloadKind::kWalletTransactions:
    case WorkloadKind::kCount:
      throw std::logic_error(
          "one-shot workload kind has no production dispatcher");
  }
}

}  // namespace bbp::simulator_app_internal
