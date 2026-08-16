#include "simulator_peer_churn_workloads.h"

#include <chrono>
#include <exception>
#include <stdexcept>
#include <string>
#include <vector>

#include "bbp/drivers/chain_driver.h"
#include "bbp/peer_connectivity_controller.h"
#include "bbp/runtime_node_inventory.h"
#include "bbp/simulation_cancelled.h"
#include "bbp/simulation_command.h"
#include "bbp/simulation_event_kind.h"
#include "bbp/simulator/connect_peer_workload.h"
#include "bbp/simulator/disconnect_peer_workload.h"
#include "bbp/simulator/node_runtime.h"
#include "bbp/simulator/options.h"
#include "simulator_cancellable_waiting.h"
#include "simulator_event_writing.h"
#include "simulator_workload_event_details.h"
#include "simulator_workload_mutation_error.h"

namespace bbp::simulator_app_internal {
namespace {

std::string PeerAddress(const RuntimeNodeSnapshot& nodes, uint32_t node) {
  const uint32_t node_index = node - 1U;
  return nodes[node_index].config.p2p_host + ":" +
         std::to_string(nodes[node_index].config.p2p_port);
}

}  // namespace

void ApplyConnectPeerWorkload(
    const Options& options, const std::filesystem::path& events_path,
    const ChainDriver& driver, PeerConnectivityController& controller,
    const RuntimeNodeSnapshot& nodes, const ConnectPeerWorkload& workload,
    uint32_t workload_index, uint32_t workload_count,
    std::stop_token stop_token,
    SimulationCommandControl* cancellation_commit_control) {
  NodeRuntime& node = nodes[workload.node - 1U];
  const std::string address = PeerAddress(nodes, workload.peer);
  const std::vector<std::string> before_addresses =
      driver.PeerAddresses(node.config, stop_token);
  const bool connected_before =
      !driver.ConnectedPeerAddresses(node.config, {address}, stop_token)
           .empty();
  const auto authorize_mutation = [&] {
    if (cancellation_commit_control == nullptr) {
      return;
    }
    if (cancellation_commit_control->TryBeginCommit()) {
      return;
    }
    if (cancellation_commit_control->CommitPhase() ==
        SimulationCommandCommitPhase::kCancelled) {
      throw SimulationCancelled();
    }
    ThrowIfStopRequested(stop_token);
    throw std::logic_error(
        "peer connection cancellation won without requesting stop");
  };
  const auto admitted_mutation = [&] {
    if (cancellation_commit_control == nullptr) {
      return false;
    }
    const SimulationCommandCommitPhase phase =
        cancellation_commit_control->CommitPhase();
    return phase == SimulationCommandCommitPhase::kCommitStarted ||
           phase == SimulationCommandCommitPhase::kCommitted;
  };
  bool mutation_completed = false;
  try {
    controller.ConnectPeer(node.config.id, nodes[workload.peer - 1U].config.id,
                           std::chrono::seconds(workload.timeout_sec),
                           stop_token, authorize_mutation);
    mutation_completed = true;
    const std::stop_token completion_stop_token =
        cancellation_commit_control == nullptr ? stop_token : std::stop_token{};
    const std::vector<std::string> after_addresses =
        driver.PeerAddresses(node.config, completion_stop_token);
    const bool connected_after =
        !driver
             .ConnectedPeerAddresses(node.config, {address},
                                     completion_stop_token)
             .empty();
    WriteEvent(events_path, options.run_id, node.config.id,
               SimulationEventKind::kPeerConnected,
               PeerChurnDetail(
                   workload_index, workload_count, workload.node, workload.peer,
                   address, static_cast<uint64_t>(before_addresses.size()),
                   static_cast<uint64_t>(after_addresses.size()),
                   connected_before, connected_after, workload.timeout_sec));
    if (cancellation_commit_control != nullptr) {
      cancellation_commit_control->MarkCommitted();
    }
  } catch (const WorkloadMutationOutcomeUnconfirmed&) {
    throw;
  } catch (const PeerMutationOutcomeUnconfirmed&) {
    ThrowWorkloadMutationOutcomeUnconfirmed(
        "peer connection outcome is unconfirmed", std::current_exception());
  } catch (const SimulationCancelled&) {
    if (mutation_completed) {
      ThrowWorkloadMutationOutcomeUnconfirmed(
          "peer connection completed without a publishable outcome",
          std::current_exception());
    }
    if (admitted_mutation()) {
      throw WorkloadMutationCancelledAfterRollback();
    }
    throw;
  } catch (const std::exception& error) {
    if (mutation_completed) {
      ThrowWorkloadMutationOutcomeUnconfirmed(
          "peer connection completed without a publishable outcome",
          std::current_exception());
    }
    if (admitted_mutation()) {
      throw WorkloadMutationFailedAfterRollback(error.what());
    }
    throw;
  } catch (...) {
    if (mutation_completed) {
      ThrowWorkloadMutationOutcomeUnconfirmed(
          "peer connection completed without a publishable outcome",
          std::current_exception());
    }
    if (admitted_mutation()) {
      throw WorkloadMutationFailedAfterRollback(
          "peer connection failed after a verified rollback");
    }
    throw;
  }
}

void ApplyDisconnectPeerWorkload(
    const Options& options, const std::filesystem::path& events_path,
    const ChainDriver& driver, PeerConnectivityController& controller,
    const RuntimeNodeSnapshot& nodes, const DisconnectPeerWorkload& workload,
    uint32_t workload_index, uint32_t workload_count,
    std::stop_token stop_token,
    SimulationCommandControl* cancellation_commit_control) {
  NodeRuntime& node = nodes[workload.node - 1U];
  const std::string address = PeerAddress(nodes, workload.peer);
  const std::vector<std::string> before_addresses =
      driver.PeerAddresses(node.config, stop_token);
  const bool connected_before =
      !driver.ConnectedPeerAddresses(node.config, {address}, stop_token)
           .empty();
  const auto authorize_mutation = [&] {
    if (cancellation_commit_control == nullptr) {
      return;
    }
    if (cancellation_commit_control->TryBeginCommit()) {
      return;
    }
    if (cancellation_commit_control->CommitPhase() ==
        SimulationCommandCommitPhase::kCancelled) {
      throw SimulationCancelled();
    }
    ThrowIfStopRequested(stop_token);
    throw std::logic_error(
        "peer disconnection cancellation won without requesting stop");
  };
  const auto admitted_mutation = [&] {
    if (cancellation_commit_control == nullptr) {
      return false;
    }
    const SimulationCommandCommitPhase phase =
        cancellation_commit_control->CommitPhase();
    return phase == SimulationCommandCommitPhase::kCommitStarted ||
           phase == SimulationCommandCommitPhase::kCommitted;
  };
  bool mutation_completed = false;
  try {
    controller.DisconnectPeer(node.config.id,
                              nodes[workload.peer - 1U].config.id,
                              std::chrono::seconds(workload.timeout_sec),
                              stop_token, authorize_mutation);
    mutation_completed = true;
    const std::stop_token completion_stop_token =
        cancellation_commit_control == nullptr ? stop_token : std::stop_token{};
    const std::vector<std::string> after_addresses =
        driver.PeerAddresses(node.config, completion_stop_token);
    const bool connected_after =
        !driver
             .ConnectedPeerAddresses(node.config, {address},
                                     completion_stop_token)
             .empty();
    WriteEvent(events_path, options.run_id, node.config.id,
               SimulationEventKind::kPeerDisconnected,
               PeerChurnDetail(
                   workload_index, workload_count, workload.node, workload.peer,
                   address, static_cast<uint64_t>(before_addresses.size()),
                   static_cast<uint64_t>(after_addresses.size()),
                   connected_before, connected_after, workload.timeout_sec));
    if (cancellation_commit_control != nullptr) {
      cancellation_commit_control->MarkCommitted();
    }
  } catch (const WorkloadMutationOutcomeUnconfirmed&) {
    throw;
  } catch (const PeerMutationOutcomeUnconfirmed&) {
    ThrowWorkloadMutationOutcomeUnconfirmed(
        "peer disconnection outcome is unconfirmed", std::current_exception());
  } catch (const SimulationCancelled&) {
    if (mutation_completed) {
      ThrowWorkloadMutationOutcomeUnconfirmed(
          "peer disconnection completed without a publishable outcome",
          std::current_exception());
    }
    if (admitted_mutation()) {
      throw WorkloadMutationCancelledAfterRollback();
    }
    throw;
  } catch (const std::exception& error) {
    if (mutation_completed) {
      ThrowWorkloadMutationOutcomeUnconfirmed(
          "peer disconnection completed without a publishable outcome",
          std::current_exception());
    }
    if (admitted_mutation()) {
      throw WorkloadMutationFailedAfterRollback(error.what());
    }
    throw;
  } catch (...) {
    if (mutation_completed) {
      ThrowWorkloadMutationOutcomeUnconfirmed(
          "peer disconnection completed without a publishable outcome",
          std::current_exception());
    }
    if (admitted_mutation()) {
      throw WorkloadMutationFailedAfterRollback(
          "peer disconnection failed after a verified rollback");
    }
    throw;
  }
}

}  // namespace bbp::simulator_app_internal
