#pragma once

#include <sys/types.h>

#include <atomic>
#include <chrono>
#include <cstdint>
#include <exception>
#include <functional>
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

#include "bbp/block_production_policy.h"
#include "bbp/mining_difficulty.h"
#include "bbp/network.h"
#include "bbp/peer_count_policy.h"
#include "bbp/perf_counter.h"
#include "bbp/simulation_node_add.h"
#include "bbp/simulation_partition.h"
#include "bbp/simulation_wallet_send.h"
#include "bbp/simulator/node_runtime_lifecycle.h"
#include "bbp/simulator/resource_limit_patch.h"

namespace bbp {

enum class SimulationCommandKind {
  kIncreaseLogVerbosity,
  kDecreaseLogVerbosity,
  kStopMining,
  kDisconnectNode,
  kReconnectNode,
  kSetBlockProductionPolicy,
  kSetMiningDifficulty,
  kKillNode,
  kConnectPeer,
  kDisconnectPeer,
  kSetPeerCountPolicy,
  kFreezeNode,
  kThawNode,
  kStopNode,
  kRestartNode,
  kGenerateBlocks,
  kSetResourceProfile,
  kSetResourceLimits,
  kSetNetworkProfile,
  kSetNetworkCondition,
  kBlockNetworkFlow,
  kUnblockNetworkFlow,
  kPartitionNodes,
  kHealPartition,
  kExportNodeReport,
  kSetPerfCounters,
  kSendWalletTransaction,
  kAddNodes,
  kCount,
};

struct SimulationNetworkFlow {
  std::string src_address;
  std::uint16_t src_port = 0;
  std::string dst_address;
  std::uint16_t dst_port = 0;
  std::uint32_t handle = 0;
};

enum class SimulationCommandCancellationCause {
  kNone,
  kClientCancel,
  kDeadline,
  kApplicationShutdown,
};

enum class SimulationCommandOutcomeState {
  kSucceeded,
  kFailed,
  kCancelled,
  kTimedOut,
  kOutcomeUnconfirmed,
};

enum class SimulationNodeRestartPhase {
  kBeforeStop,
  kStopRequested,
  kOriginalExited,
  kReplacementReady,
  kCompleted,
};

enum class SimulationCommandCommitPhase {
  kOpen,
  kCancelled,
  kCommitStarted,
  kCommitted,
};

std::string_view SimulationNodeRestartPhaseName(
    SimulationNodeRestartPhase phase);

inline constexpr auto kSimulationCommandCancellationReconciliation =
    std::chrono::milliseconds(250);
inline constexpr auto kSimulationNodeAddCancellationReconciliation =
    std::chrono::seconds(30);
inline constexpr std::uint64_t kSimulationNodeAddProgressTotal = 5U;

struct SimulationCommandControl {
  std::stop_source stop_source;
  std::atomic_bool outcome_unconfirmed{false};
  std::atomic<SimulationNodeRestartPhase> restart_phase{
      SimulationNodeRestartPhase::kBeforeStop};
  std::atomic<std::uint32_t> commit_state{
      EncodeCommitState(SimulationCommandCommitPhase::kOpen,
                        SimulationCommandCancellationCause::kNone)};
  std::atomic<std::uint64_t> progress_completed{0U};
  std::atomic<std::uint64_t> initial_inventory_generation{
      kUnsetInventoryGeneration};
  std::optional<std::chrono::steady_clock::time_point> absolute_deadline;

  bool RequestCancellation(SimulationCommandCancellationCause cause) noexcept {
    if (cause == SimulationCommandCancellationCause::kNone) {
      return false;
    }
    std::uint32_t expected =
        EncodeCommitState(SimulationCommandCommitPhase::kOpen,
                          SimulationCommandCancellationCause::kNone);
    const bool recorded = commit_state.compare_exchange_strong(
        expected,
        EncodeCommitState(SimulationCommandCommitPhase::kCancelled, cause),
        std::memory_order_acq_rel, std::memory_order_acquire);
    if (!recorded) {
      return false;
    }
    stop_source.request_stop();
    return true;
  }

  bool TryBeginCommit() noexcept {
    std::uint32_t expected =
        EncodeCommitState(SimulationCommandCommitPhase::kOpen,
                          SimulationCommandCancellationCause::kNone);
    return commit_state.compare_exchange_strong(
        expected,
        EncodeCommitState(SimulationCommandCommitPhase::kCommitStarted,
                          SimulationCommandCancellationCause::kNone),
        std::memory_order_acq_rel, std::memory_order_acquire);
  }

  void MarkCommitted() noexcept {
    std::uint32_t expected =
        EncodeCommitState(SimulationCommandCommitPhase::kCommitStarted,
                          SimulationCommandCancellationCause::kNone);
    if (!commit_state.compare_exchange_strong(
            expected,
            EncodeCommitState(SimulationCommandCommitPhase::kCommitted,
                              SimulationCommandCancellationCause::kNone),
            std::memory_order_acq_rel, std::memory_order_acquire)) {
      std::terminate();
    }
  }

  [[nodiscard]] SimulationCommandCommitPhase CommitPhase() const noexcept {
    return static_cast<SimulationCommandCommitPhase>(
        commit_state.load(std::memory_order_acquire) & 0xffU);
  }

  [[nodiscard]] SimulationCommandCancellationCause CancellationCause()
      const noexcept {
    return static_cast<SimulationCommandCancellationCause>(
        (commit_state.load(std::memory_order_acquire) >> 8U) & 0xffU);
  }

  bool RecordInitialInventory(std::uint64_t generation,
                              std::vector<std::string> node_ids) {
    if (generation == kUnsetInventoryGeneration) {
      return false;
    }
    std::lock_guard<std::mutex> lock(initial_inventory_mutex_);
    const std::uint64_t recorded_generation =
        initial_inventory_generation.load(std::memory_order_acquire);
    if (recorded_generation != kUnsetInventoryGeneration) {
      return recorded_generation == generation &&
             initial_inventory_node_ids_ == node_ids;
    }
    initial_inventory_node_ids_ = std::move(node_ids);
    std::uint64_t expected = kUnsetInventoryGeneration;
    return initial_inventory_generation.compare_exchange_strong(
        expected, generation, std::memory_order_release,
        std::memory_order_relaxed);
  }

  [[nodiscard]] std::optional<std::uint64_t> InitialInventoryGeneration()
      const noexcept {
    const std::uint64_t generation =
        initial_inventory_generation.load(std::memory_order_acquire);
    return generation == kUnsetInventoryGeneration
               ? std::nullopt
               : std::optional<std::uint64_t>(generation);
  }

  [[nodiscard]] std::optional<std::vector<std::string>>
  InitialInventoryNodeIds() const {
    std::lock_guard<std::mutex> lock(initial_inventory_mutex_);
    if (initial_inventory_generation.load(std::memory_order_acquire) ==
        kUnsetInventoryGeneration) {
      return std::nullopt;
    }
    return initial_inventory_node_ids_;
  }

  bool ReportProgress(std::uint64_t completed) noexcept {
    if (completed > kSimulationNodeAddProgressTotal) {
      return false;
    }
    std::uint64_t current =
        progress_completed.load(std::memory_order_acquire);
    while (completed > current) {
      if (progress_completed.compare_exchange_weak(
              current, completed, std::memory_order_release,
              std::memory_order_acquire)) {
        return true;
      }
    }
    return completed == current;
  }

  void RecordNodeResourceFailure(SimulationNodeResourceFailure failure) {
    std::lock_guard<std::mutex> lock(resource_failure_mutex_);
    node_resource_failure_ = std::move(failure);
  }

  std::optional<SimulationNodeResourceFailure> NodeResourceFailure() const {
    std::lock_guard<std::mutex> lock(resource_failure_mutex_);
    return node_resource_failure_;
  }

 private:
  static constexpr std::uint64_t kUnsetInventoryGeneration =
      std::numeric_limits<std::uint64_t>::max();

  static constexpr std::uint32_t EncodeCommitState(
      SimulationCommandCommitPhase phase,
      SimulationCommandCancellationCause cause) noexcept {
    return static_cast<std::uint32_t>(phase) |
           (static_cast<std::uint32_t>(cause) << 8U);
  }

  mutable std::mutex initial_inventory_mutex_;
  std::vector<std::string> initial_inventory_node_ids_;
  mutable std::mutex resource_failure_mutex_;
  std::optional<SimulationNodeResourceFailure> node_resource_failure_;
};

struct SimulationCommandOutcome {
  SimulationCommandOutcomeState state =
      SimulationCommandOutcomeState::kSucceeded;
  SimulationCommandCancellationCause cancellation_cause =
      SimulationCommandCancellationCause::kNone;
  std::optional<std::string> error;
  std::optional<std::string> node_lifecycle;
  std::vector<std::string> added_node_ids;
  std::optional<std::uint64_t> inventory_generation;
  std::optional<std::uint32_t> final_node_count;
};

class SimulationCommandOutcomeUnconfirmed final : public std::runtime_error {
 public:
  explicit SimulationCommandOutcomeUnconfirmed(std::string message)
      : std::runtime_error(std::move(message)) {}
};

struct SimulationNodeProcessObservation {
  bool running = false;
  pid_t pid = -1;
  std::uint64_t restart_count = 0U;
};

enum class SimulationNodeRestartReconciliation {
  kUnchanged,
  kStopped,
  kReplacementReady,
  kUnconfirmed,
};

SimulationNodeRestartReconciliation ReconcileCancelledSimulationNodeRestart(
    const SimulationNodeProcessObservation& initial,
    SimulationNodeRestartPhase phase,
    std::chrono::steady_clock::time_point deadline,
    const std::function<SimulationNodeProcessObservation()>& observe,
    const std::function<bool(const SimulationNodeProcessObservation&)>&
        request_unready_replacement_stop = {});

NodeRuntimeLifecycle ReconciledSimulationNodeRestartLifecycle(
    NodeRuntimeLifecycle admitted, NodeRuntimeLifecycle observed,
    SimulationNodeRestartReconciliation reconciliation);

struct SimulationCommand {
  std::uint64_t sequence = 0;
  SimulationCommandKind kind = SimulationCommandKind::kIncreaseLogVerbosity;
  std::string node_id;
  std::optional<BlockProductionPolicy> block_production_policy;
  std::optional<MiningDifficulty> mining_difficulty;
  std::optional<std::string> peer_node_id;
  std::optional<PeerCountPolicy> peer_count_policy;
  std::optional<std::uint32_t> block_count;
  std::optional<std::string> profile;
  std::optional<ResourceLimitPatch> resource_limit_patch;
  std::optional<NetworkCondition> network_condition;
  std::optional<SimulationNetworkFlow> network_flow;
  std::optional<SimulationPartition> partition;
  std::optional<PerfCounterTarget> perf_counter_target;
  std::vector<PerfCounterKind> perf_counter_kinds;
  std::optional<SimulationWalletSend> wallet_send;
  std::optional<SimulationNodeAddRequest> node_add;
  bool confirmed = false;
  std::optional<std::uint32_t> scheduled_event_sequence;
  std::shared_ptr<SimulationCommandControl> operation_control;
};

std::string_view SimulationCommandKindName(SimulationCommandKind kind);
std::optional<SimulationCommandKind> SimulationCommandKindFromName(
    std::string_view name);
bool SimulationCommandRequiresConfirmation(SimulationCommandKind kind);

}  // namespace bbp
