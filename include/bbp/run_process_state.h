#pragma once

#include <chrono>
#include <mutex>
#include <optional>
#include <set>
#include <stop_token>
#include <string>
#include <string_view>
#include <vector>

namespace bbp {

// One instance is owned by each simulator run. A Guard is required for every
// access to NodeRuntime process, process-start, and process-linked perf state.
// The same lock owns both miner collections so command, lifecycle, mining,
// metrics, readiness, and cleanup threads share one synchronization contract.
// Callers must not hold scheduler, event, network, or RPC-internal locks while
// acquiring the state lock, and must release it before potentially blocking
// RPC. Native-mining callers acquire NativeMiningRpcGuard first, then briefly
// acquire Guard to snapshot or reconcile; no caller may invert that order.
class RunProcessState {
 public:
  class Guard {
   public:
    Guard(const Guard&) = delete;
    Guard& operator=(const Guard&) = delete;
    Guard(Guard&&) noexcept = default;
    Guard& operator=(Guard&&) noexcept = default;

   private:
    friend class RunProcessState;

    explicit Guard(RunProcessState& owner);

    RunProcessState* owner_;
    std::unique_lock<std::mutex> lock_;
  };

  class NativeMiningRpcGuard {
   public:
    NativeMiningRpcGuard(const NativeMiningRpcGuard&) = delete;
    NativeMiningRpcGuard& operator=(const NativeMiningRpcGuard&) = delete;
    NativeMiningRpcGuard(NativeMiningRpcGuard&&) noexcept = default;
    NativeMiningRpcGuard& operator=(NativeMiningRpcGuard&&) noexcept = default;

   private:
    friend class RunProcessState;

    explicit NativeMiningRpcGuard(RunProcessState& owner);
    NativeMiningRpcGuard(RunProcessState& owner, std::adopt_lock_t);

    RunProcessState* owner_;
    std::unique_lock<std::mutex> lock_;
  };

  struct NativeMiningRestartIntent {
    NativeMiningRpcGuard rpc_guard;
    bool native_miner_active = false;
    bool scheduled_miner_paused = false;
  };

  [[nodiscard]] Guard Lock();
  [[nodiscard]] NativeMiningRpcGuard LockNativeMiningRpc();
  [[nodiscard]] std::optional<NativeMiningRpcGuard> TryLockNativeMiningRpcUntil(
      std::chrono::steady_clock::time_point deadline,
      std::stop_token stop_token = {});
  [[nodiscard]] std::optional<NativeMiningRestartIntent>
  TryBeginNativeMiningRestart(std::string_view node_id,
                              std::chrono::steady_clock::time_point deadline,
                              std::stop_token stop_token = {});

  [[nodiscard]] bool IsActiveNativeMiner(const Guard& guard,
                                         std::string_view node_id) const;
  void AddActiveNativeMiner(const Guard& guard, std::string node_id);
  void RemoveActiveNativeMiner(const Guard& guard, std::string_view node_id);
  void ReconcileActiveNativeMinerAfterRestartFailure(
      const NativeMiningRpcGuard& mining_rpc_guard, const Guard& guard,
      std::string_view node_id, bool original_running_generation_unchanged,
      bool replacement_mining_confirmed_inactive);
  [[nodiscard]] std::vector<std::string> ActiveNativeMiners(
      const Guard& guard) const;

  [[nodiscard]] bool IsPausedScheduledMiner(const Guard& guard,
                                            std::string_view node_id) const;
  void PauseScheduledMiner(const Guard& guard, std::string node_id);
  [[nodiscard]] bool ResumeScheduledMiner(const Guard& guard,
                                          std::string_view node_id);

 private:
  void RequireGuard(const Guard& guard) const;
  void RequireNativeMiningRpcGuard(const NativeMiningRpcGuard& guard) const;

  std::mutex mutex_;
  // Native mining RPCs stay outside mutex_, but are serialized so a stale
  // generation can be reconciled before a newer generation changes mining.
  std::mutex native_mining_rpc_mutex_;
  std::vector<std::string> active_native_miner_ids_;
  std::set<std::string, std::less<>> paused_scheduled_miners_;
};

}  // namespace bbp
