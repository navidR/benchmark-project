#include "bbp/run_process_state.h"

#include <algorithm>
#include <chrono>
#include <stdexcept>
#include <thread>
#include <utility>

namespace bbp {

RunProcessState::Guard::Guard(RunProcessState& owner)
    : owner_(&owner), lock_(owner.mutex_) {}

RunProcessState::NativeMiningRpcGuard::NativeMiningRpcGuard(
    RunProcessState& owner)
    : owner_(&owner), lock_(owner.native_mining_rpc_mutex_) {}

RunProcessState::NativeMiningRpcGuard::NativeMiningRpcGuard(
    RunProcessState& owner, std::adopt_lock_t)
    : owner_(&owner), lock_(owner.native_mining_rpc_mutex_, std::adopt_lock) {}

RunProcessState::Guard RunProcessState::Lock() { return Guard(*this); }

RunProcessState::NativeMiningRpcGuard RunProcessState::LockNativeMiningRpc() {
  return NativeMiningRpcGuard(*this);
}

std::optional<RunProcessState::NativeMiningRpcGuard>
RunProcessState::TryLockNativeMiningRpcUntil(
    std::chrono::steady_clock::time_point deadline,
    std::stop_token stop_token) {
  while (true) {
    if (stop_token.stop_requested()) {
      return std::nullopt;
    }
    const auto now = std::chrono::steady_clock::now();
    if (now >= deadline) {
      return std::nullopt;
    }
    if (native_mining_rpc_mutex_.try_lock()) {
      if (std::chrono::steady_clock::now() >= deadline) {
        native_mining_rpc_mutex_.unlock();
        return std::nullopt;
      }
      return NativeMiningRpcGuard(*this, std::adopt_lock);
    }
    std::this_thread::sleep_for(std::min(
        std::chrono::milliseconds(1),
        std::chrono::duration_cast<std::chrono::milliseconds>(deadline - now)));
  }
}

std::optional<RunProcessState::NativeMiningRestartIntent>
RunProcessState::TryBeginNativeMiningRestart(
    std::string_view node_id, std::chrono::steady_clock::time_point deadline,
    std::stop_token stop_token) {
  std::optional<NativeMiningRpcGuard> rpc_guard =
      TryLockNativeMiningRpcUntil(deadline, stop_token);
  if (!rpc_guard) {
    return std::nullopt;
  }
  auto guard = Lock();
  return NativeMiningRestartIntent{
      .rpc_guard = std::move(*rpc_guard),
      .native_miner_active = IsActiveNativeMiner(guard, node_id),
      .scheduled_miner_paused = IsPausedScheduledMiner(guard, node_id),
  };
}

bool RunProcessState::IsActiveNativeMiner(const Guard& guard,
                                          std::string_view node_id) const {
  RequireGuard(guard);
  return std::find(active_native_miner_ids_.begin(),
                   active_native_miner_ids_.end(),
                   node_id) != active_native_miner_ids_.end();
}

void RunProcessState::AddActiveNativeMiner(const Guard& guard,
                                           std::string node_id) {
  RequireGuard(guard);
  if (!IsActiveNativeMiner(guard, node_id)) {
    active_native_miner_ids_.push_back(std::move(node_id));
  }
}

void RunProcessState::RemoveActiveNativeMiner(const Guard& guard,
                                              std::string_view node_id) {
  RequireGuard(guard);
  active_native_miner_ids_.erase(
      std::remove(active_native_miner_ids_.begin(),
                  active_native_miner_ids_.end(), node_id),
      active_native_miner_ids_.end());
}

void RunProcessState::ReconcileActiveNativeMinerAfterRestartFailure(
    const NativeMiningRpcGuard& mining_rpc_guard, const Guard& guard,
    std::string_view node_id, bool original_running_generation_unchanged,
    bool replacement_mining_confirmed_inactive) {
  RequireNativeMiningRpcGuard(mining_rpc_guard);
  RequireGuard(guard);
  if (!original_running_generation_unchanged &&
      replacement_mining_confirmed_inactive) {
    RemoveActiveNativeMiner(guard, node_id);
  }
}

std::vector<std::string> RunProcessState::ActiveNativeMiners(
    const Guard& guard) const {
  RequireGuard(guard);
  return active_native_miner_ids_;
}

bool RunProcessState::IsPausedScheduledMiner(const Guard& guard,
                                             std::string_view node_id) const {
  RequireGuard(guard);
  return paused_scheduled_miners_.contains(node_id);
}

void RunProcessState::PauseScheduledMiner(const Guard& guard,
                                          std::string node_id) {
  RequireGuard(guard);
  paused_scheduled_miners_.insert(std::move(node_id));
}

bool RunProcessState::ResumeScheduledMiner(const Guard& guard,
                                           std::string_view node_id) {
  RequireGuard(guard);
  return paused_scheduled_miners_.erase(std::string(node_id)) != 0U;
}

void RunProcessState::RequireGuard(const Guard& guard) const {
  if (guard.owner_ != this || !guard.lock_.owns_lock()) {
    throw std::logic_error("run process state guard does not own this state");
  }
}

void RunProcessState::RequireNativeMiningRpcGuard(
    const NativeMiningRpcGuard& guard) const {
  if (guard.owner_ != this || !guard.lock_.owns_lock()) {
    throw std::logic_error("native mining RPC guard does not own this state");
  }
}

}  // namespace bbp
