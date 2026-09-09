#include "simulator_one_shot_workload_invocation.h"

#include <atomic>
#include <boost/json/array.hpp>
#include <cstdint>
#include <functional>
#include <limits>
#include <mutex>
#include <stdexcept>
#include <stop_token>
#include <string>
#include <string_view>
#include <utility>

#include "bbp/mcp_live_application.h"
#include "bbp/mcp_operation_service.h"
#include "bbp/peer_connectivity_controller.h"
#include "bbp/runtime_node_inventory.h"
#include "bbp/runtime_wallet_registry.h"
#include "bbp/simulation_cancelled.h"
#include "bbp/simulation_command.h"
#include "bbp/simulator/options.h"
#include "bbp/simulator/scenario_workload.h"
#include "bbp/simulator/workload_kind.h"
#include "simulator_raw_transaction_workload.h"
#include "simulator_runtime_workload_validation.h"
#include "simulator_workload_mutation_error.h"

namespace bbp::simulator_app_internal {

OneShotWorkloadInvoker MakeOneShotWorkloadInvoker(
    const Options& options, const RuntimeNodeInventory& node_inventory,
    const RuntimeWalletRegistry& runtime_wallet_registry,
    const PeerTopologyConfig& live_topology_config,
    std::timed_mutex& one_shot_workload_mutex,
    std::timed_mutex& node_mutation_mutex,
    std::uint64_t& next_one_shot_invocation,
    McpLiveApplication& mcp_application,
    std::function<bool()> request_simulation_stop,
    OneShotWorkloadMutationLock acquire_node_mutation_lock,
    OneShotWorkloadDispatcher dispatch_one_shot_workload,
    std::stop_token stop_token) {
  return [&options, &node_inventory, &runtime_wallet_registry,
          &live_topology_config, &one_shot_workload_mutex, &node_mutation_mutex,
          &next_one_shot_invocation, &mcp_application,
          request_simulation_stop = std::move(request_simulation_stop),
          acquire_node_mutation_lock,
          dispatch_one_shot_workload = std::move(dispatch_one_shot_workload),
          stop_token](const boost::json::object& workload,
                      std::stop_token operation_stop_token) {
    SimulationCommandControl cancellation_commit_control;
    std::atomic_bool interrupt_after_mutation_admission = false;
    std::stop_callback cancel_before_irreversible_commit(
        operation_stop_token, [&] {
          const bool cancellation_won =
              cancellation_commit_control.RequestCancellation(
                  SimulationCommandCancellationCause::kClientCancel);
          if (!cancellation_won && interrupt_after_mutation_admission.load(
                                       std::memory_order_acquire)) {
            cancellation_commit_control.stop_source.request_stop();
          }
        });
    const std::stop_token invocation_stop_token =
        cancellation_commit_control.stop_source.get_token();
    auto one_shot_lock = acquire_node_mutation_lock(one_shot_workload_mutex,
                                                    invocation_stop_token);
    auto mutation_lock =
        acquire_node_mutation_lock(node_mutation_mutex, invocation_stop_token);
    const RuntimeNodeSnapshot current_nodes = node_inventory.Snapshot();
    const RuntimeWalletSnapshot current_roles =
        runtime_wallet_registry.Snapshot();
    ScenarioWorkload parsed = ParseAndValidateOneShotWorkload(
        workload,
        RuntimeOneShotWorkloadValidationOptions(
            options, current_nodes, current_roles, live_topology_config));
    interrupt_after_mutation_admission.store(
        parsed.kind == WorkloadKind::kConnectPeer ||
            parsed.kind == WorkloadKind::kDisconnectPeer ||
            parsed.kind == WorkloadKind::kSendRawTransaction,
        std::memory_order_release);
    if (next_one_shot_invocation == std::numeric_limits<std::uint64_t>::max()) {
      throw std::overflow_error(
          "one-shot workload invocation identity exhausted");
    }
    const std::string invocation_id =
        "workload-invocation-" + std::to_string(next_one_shot_invocation++);
    if (parsed.kind == WorkloadKind::kCheckpoint &&
        parsed.checkpoint.name.empty()) {
      parsed.checkpoint.name = invocation_id;
    }
    const std::string action(WorkloadKindName(parsed.kind));
    boost::json::object completed_result{
        {"result_family", "workload_invocation"},
        {"run_id", options.run_id},
        {"invocation_id", invocation_id},
        {"action", action},
        {"state", "completed"},
    };
    const auto irreversible_commit_started = [&] {
      const SimulationCommandCommitPhase commit_phase =
          cancellation_commit_control.CommitPhase();
      return commit_phase == SimulationCommandCommitPhase::kCommitStarted ||
             commit_phase == SimulationCommandCommitPhase::kCommitted;
    };
    const auto throw_outcome_unconfirmed =
        [&](std::string_view evidence = {}) -> void {
      mcp_application.MarkRunStopping();
      request_simulation_stop();
      std::string message =
          action +
          " did not reach its authoritative completion boundary "
          "after workload side effects may have begun; run stop was "
          "requested and blind retry is unsafe";
      if (!evidence.empty()) {
        message += ": " + std::string(evidence);
      }
      boost::json::object diagnostic{
          {"code", "workload_invocation_outcome_unconfirmed"},
          {"message", message},
          {"path", invocation_id},
          {"action", action},
          {"state", "indeterminate"},
          {"recoverable", false}};
      if (parsed.kind == WorkloadKind::kRestartNode) {
        diagnostic["node_id"] = parsed.restart_node.node_id;
        diagnostic["phase"] = SimulationNodeRestartPhaseName(
            cancellation_commit_control.restart_phase.load(
                std::memory_order_acquire));
      }
      throw McpOperationFailure("workload_invocation_outcome_unconfirmed",
                                message, false,
                                boost::json::array{std::move(diagnostic)});
    };
    try {
      dispatch_one_shot_workload(parsed, current_nodes, 1U, 1U,
                                 invocation_stop_token,
                                 &cancellation_commit_control);
      SimulationCommandCommitPhase phase =
          cancellation_commit_control.CommitPhase();
      if (phase == SimulationCommandCommitPhase::kOpen) {
        if (cancellation_commit_control.TryBeginCommit()) {
          phase = SimulationCommandCommitPhase::kCommitStarted;
        } else {
          phase = cancellation_commit_control.CommitPhase();
        }
      }
      if (phase == SimulationCommandCommitPhase::kCommitStarted) {
        cancellation_commit_control.MarkCommitted();
      } else if (phase == SimulationCommandCommitPhase::kCancelled) {
        throw_outcome_unconfirmed(
            "cancellation was accepted before completion could be "
            "committed");
      } else if (phase != SimulationCommandCommitPhase::kCommitted) {
        throw std::logic_error(
            "one-shot workload reached an unknown commit phase");
      }
    } catch (const WorkloadMutationCancelledAfterRollback&) {
      throw SimulationCancelled();
    } catch (const WorkloadMutationFailedAfterRollback& error) {
      throw std::runtime_error(error.what());
    } catch (const WorkloadMutationOutcomeUnconfirmed& error) {
      throw_outcome_unconfirmed(error.what());
    } catch (const PeerMutationOutcomeUnconfirmed& error) {
      throw_outcome_unconfirmed(error.what());
    } catch (const OneShotRawTransactionRejected&) {
      throw;
    } catch (const SimulationCancelled&) {
      if (irreversible_commit_started()) {
        if (parsed.kind == WorkloadKind::kRestartNode &&
            stop_token.stop_requested()) {
          throw;
        }
        throw_outcome_unconfirmed();
      }
      throw;
    } catch (...) {
      if (irreversible_commit_started()) {
        throw_outcome_unconfirmed();
      }
      throw;
    }
    return completed_result;
  };
}

}  // namespace bbp::simulator_app_internal
