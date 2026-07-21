#include "bbp/run_process_state.h"

#include <algorithm>
#include <stdexcept>
#include <utility>

namespace bbp {

RunProcessState::Guard::Guard(RunProcessState& owner)
    : owner_(&owner), lock_(owner.mutex_) {}

RunProcessState::NativeMiningRpcGuard::NativeMiningRpcGuard(
    RunProcessState& owner)
    : owner_(&owner), lock_(owner.native_mining_rpc_mutex_) {}

RunProcessState::Guard RunProcessState::Lock() { return Guard(*this); }

RunProcessState::NativeMiningRpcGuard RunProcessState::LockNativeMiningRpc() {
  return NativeMiningRpcGuard(*this);
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

}  // namespace bbp
