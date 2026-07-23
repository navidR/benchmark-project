#pragma once

#include <sys/types.h>

#include <atomic>
#include <chrono>
#include <cstdint>
#include <functional>
#include <memory>
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

std::string_view SimulationNodeRestartPhaseName(
    SimulationNodeRestartPhase phase);

inline constexpr auto kSimulationCommandCancellationReconciliation =
    std::chrono::milliseconds(250);

struct SimulationCommandControl {
  std::stop_source stop_source;
  std::atomic<SimulationCommandCancellationCause> cancellation_cause{
      SimulationCommandCancellationCause::kNone};
  std::atomic_bool outcome_unconfirmed{false};
  std::atomic<SimulationNodeRestartPhase> restart_phase{
      SimulationNodeRestartPhase::kBeforeStop};
  std::optional<std::chrono::steady_clock::time_point> absolute_deadline;

  bool RequestCancellation(SimulationCommandCancellationCause cause) noexcept {
    SimulationCommandCancellationCause expected =
        SimulationCommandCancellationCause::kNone;
    const bool recorded = cancellation_cause.compare_exchange_strong(
        expected, cause, std::memory_order_acq_rel, std::memory_order_acquire);
    stop_source.request_stop();
    return recorded;
  }
};

struct SimulationCommandOutcome {
  SimulationCommandOutcomeState state =
      SimulationCommandOutcomeState::kSucceeded;
  SimulationCommandCancellationCause cancellation_cause =
      SimulationCommandCancellationCause::kNone;
  std::optional<std::string> error;
  std::optional<std::string> node_lifecycle;
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
  bool confirmed = false;
  std::optional<std::uint32_t> scheduled_event_sequence;
  std::shared_ptr<SimulationCommandControl> operation_control;
};

std::string_view SimulationCommandKindName(SimulationCommandKind kind);
std::optional<SimulationCommandKind> SimulationCommandKindFromName(
    std::string_view name);
bool SimulationCommandRequiresConfirmation(SimulationCommandKind kind);

}  // namespace bbp
