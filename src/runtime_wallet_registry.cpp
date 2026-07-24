#include "bbp/runtime_wallet_registry.h"

#include <limits>
#include <set>
#include <stdexcept>
#include <utility>

namespace bbp {

struct RuntimeWalletSnapshot::Generation {
  std::uint64_t sequence = 0U;
  SimulationRegistry registry;
};

std::uint64_t RuntimeWalletSnapshot::generation() const {
  return generation_ ? generation_->sequence : 0U;
}

const SimulationRegistry& RuntimeWalletSnapshot::registry() const {
  if (!generation_) {
    throw std::logic_error("runtime wallet snapshot is not initialized");
  }
  return generation_->registry;
}

const std::vector<WalletIdentity>& RuntimeWalletSnapshot::wallets() const {
  return registry().wallets();
}

RuntimeWalletRegistry::RuntimeWalletRegistry()
    : generation_(std::make_shared<RuntimeWalletSnapshot::Generation>()) {}

void RuntimeWalletRegistry::Initialize(SimulationRegistry registry) {
  std::lock_guard<std::mutex> lock(mutex_);
  if (generation_->sequence != 0U || !generation_->registry.wallets().empty()) {
    throw std::logic_error("runtime wallet registry is already initialized");
  }
  generation_ = std::make_shared<RuntimeWalletSnapshot::Generation>(
      RuntimeWalletSnapshot::Generation{.sequence = 1U,
                                        .registry = std::move(registry)});
}

RuntimeWalletSnapshot RuntimeWalletRegistry::Snapshot() const {
  std::lock_guard<std::mutex> lock(mutex_);
  if (generation_->sequence == 0U) {
    throw std::logic_error("runtime wallet registry is not initialized");
  }
  return RuntimeWalletSnapshot(generation_);
}

RuntimeWalletRegistry::PreparedAppend RuntimeWalletRegistry::PrepareAppend(
    std::uint64_t expected_generation, std::vector<WalletIdentity> wallets,
    std::uint32_t runtime_node_count) {
  std::unique_lock<std::mutex> lock(mutex_);
  if (generation_->sequence == 0U) {
    throw std::logic_error("runtime wallet registry is not initialized");
  }
  if (generation_->sequence != expected_generation) {
    throw std::runtime_error(
        "runtime wallet registry changed before publication");
  }
  if (wallets.empty()) {
    throw std::invalid_argument(
        "runtime wallet publication requires at least one wallet");
  }
  if (generation_->sequence == std::numeric_limits<std::uint64_t>::max()) {
    throw std::overflow_error("runtime wallet registry generation overflow");
  }

  SimulationRegistry next = generation_->registry;
  next.SetRuntimeNodeCount(runtime_node_count);
  std::set<std::string> addresses;
  for (const WalletIdentity& wallet : next.wallets()) {
    if (!wallet.address.empty()) {
      addresses.insert(wallet.address);
    }
  }
  for (WalletIdentity& wallet : wallets) {
    if (wallet.node == 0U || wallet.node > runtime_node_count ||
        wallet.node_id.empty() || wallet.address.empty() ||
        wallet.funding_address.empty()) {
      throw std::invalid_argument(
          "runtime wallet publication contains an incomplete wallet");
    }
    if (!addresses.insert(wallet.address).second) {
      throw std::invalid_argument(
          "runtime wallet publication contains a duplicate address");
    }
    next.AddWallet(std::move(wallet));
  }
  auto next_generation = std::make_shared<RuntimeWalletSnapshot::Generation>(
      RuntimeWalletSnapshot::Generation{
          .sequence = generation_->sequence + 1U,
          .registry = std::move(next),
      });
  return PreparedAppend(this, std::move(lock), std::move(next_generation));
}

RuntimeWalletSnapshot RuntimeWalletRegistry::PreparedAppend::Commit() noexcept {
  if (owner_ == nullptr || !lock_.owns_lock() ||
      lock_.mutex() != &owner_->mutex_ || !generation_) {
    std::terminate();
  }
  owner_->generation_.swap(generation_);
  RuntimeWalletSnapshot snapshot(owner_->generation_);
  lock_.unlock();
  owner_ = nullptr;
  generation_.reset();
  return snapshot;
}

}  // namespace bbp
