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

const std::vector<MasternodeIdentity>& RuntimeWalletSnapshot::masternodes()
    const {
  return registry().masternodes();
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
  return PrepareUpdate(expected_generation, std::move(wallets), {}, {},
                       runtime_node_count);
}

RuntimeWalletRegistry::PreparedAppend RuntimeWalletRegistry::PrepareUpdate(
    std::uint64_t expected_generation, std::vector<WalletIdentity> wallets,
    std::vector<std::uint32_t> miner_nodes,
    std::vector<MasternodeIdentity> masternodes,
    std::uint32_t runtime_node_count) {
  std::unique_lock<std::mutex> lock(mutex_);
  if (generation_->sequence == 0U) {
    throw std::logic_error("runtime wallet registry is not initialized");
  }
  if (generation_->sequence != expected_generation) {
    throw std::runtime_error(
        "runtime wallet registry changed before publication");
  }
  if (wallets.empty() && miner_nodes.empty() && masternodes.empty() &&
      runtime_node_count == generation_->registry.topology().node_count) {
    throw std::invalid_argument(
        "runtime role publication requires a state change");
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
  std::set<std::uint32_t> unique_miner_nodes;
  for (const std::uint32_t miner_node : miner_nodes) {
    if (!unique_miner_nodes.insert(miner_node).second) {
      throw std::invalid_argument(
          "runtime role publication contains a duplicate miner node");
    }
    next.AddMinerNode(miner_node);
  }
  std::set<std::uint32_t> unique_masternode_nodes;
  std::set<std::string> unique_pro_tx_hashes;
  std::set<std::string> unique_services;
  for (const MasternodeIdentity& existing : next.masternodes()) {
    unique_masternode_nodes.insert(existing.node - 1U);
    unique_pro_tx_hashes.insert(existing.pro_tx_hash);
    unique_services.insert(existing.service);
  }
  for (MasternodeIdentity& masternode : masternodes) {
    if (masternode.node == 0U || masternode.node > runtime_node_count ||
        masternode.node_id.empty() ||
        masternode.funding_wallet_node_id.empty() ||
        masternode.pro_tx_hash.empty() || masternode.service.empty() ||
        masternode.collateral_address.empty() ||
        masternode.owner_address.empty() ||
        masternode.operator_public_key.empty() ||
        masternode.operator_secret_key.empty() ||
        masternode.voting_address.empty() ||
        masternode.payout_address.empty() || masternode.state != "READY" ||
        !unique_masternode_nodes.insert(masternode.node - 1U).second ||
        !unique_pro_tx_hashes.insert(masternode.pro_tx_hash).second ||
        !unique_services.insert(masternode.service).second) {
      throw std::invalid_argument(
          "runtime role publication contains an invalid masternode");
    }
    next.AddMasternode(std::move(masternode));
  }
  auto next_generation = std::make_shared<RuntimeWalletSnapshot::Generation>(
      RuntimeWalletSnapshot::Generation{
          .sequence = generation_->sequence + 1U,
          .registry = std::move(next),
      });
  return PreparedAppend(this, std::move(lock), std::move(next_generation));
}

RuntimeWalletRegistry::PreparedAppend RuntimeWalletRegistry::PrepareReplace(
    std::uint64_t expected_generation, SimulationRegistry registry) {
  std::unique_lock<std::mutex> lock(mutex_);
  if (generation_->sequence == 0U) {
    throw std::logic_error("runtime wallet registry is not initialized");
  }
  if (generation_->sequence != expected_generation) {
    throw std::runtime_error(
        "runtime wallet registry changed before replacement publication");
  }
  if (generation_->sequence == std::numeric_limits<std::uint64_t>::max()) {
    throw std::overflow_error("runtime wallet registry generation overflow");
  }
  const NodeRoleTopology& topology = registry.topology();
  if (topology.wallet_node_count != topology.wallet_nodes.size() ||
      topology.miner_node_count != topology.miner_nodes.size() ||
      topology.masternode_node_count != topology.masternode_nodes.size()) {
    throw std::invalid_argument(
        "runtime role replacement count does not match its node indexes");
  }
  std::set<std::uint32_t> wallet_nodes;
  for (const std::uint32_t node : topology.wallet_nodes) {
    if (node >= topology.node_count || !wallet_nodes.insert(node).second) {
      throw std::invalid_argument(
          "runtime role replacement contains an invalid wallet node");
    }
  }
  std::set<std::uint32_t> miner_nodes;
  for (const std::uint32_t node : topology.miner_nodes) {
    if (node >= topology.node_count || !miner_nodes.insert(node).second) {
      throw std::invalid_argument(
          "runtime role replacement contains an invalid miner node");
    }
  }
  std::set<std::uint32_t> masternode_nodes;
  for (const std::uint32_t node : topology.masternode_nodes) {
    if (node >= topology.node_count || !masternode_nodes.insert(node).second) {
      throw std::invalid_argument(
          "runtime role replacement contains an invalid masternode node");
    }
  }
  if (registry.masternodes().size() != masternode_nodes.size()) {
    throw std::invalid_argument(
        "runtime role replacement masternode identities do not match its "
        "node indexes");
  }
  if (!topology.allow_miner_wallet_overlap) {
    for (const std::uint32_t node : miner_nodes) {
      if (wallet_nodes.contains(node)) {
        throw std::invalid_argument(
            "runtime role replacement contains a forbidden role overlap");
      }
    }
  }
  std::set<std::string> addresses;
  std::uint32_t expected_wallet_index = 1U;
  for (const WalletIdentity& wallet : registry.wallets()) {
    if (wallet.wallet_index != expected_wallet_index++ || wallet.node == 0U ||
        wallet.node > topology.node_count || wallet.node_id.empty() ||
        wallet.address.empty() || wallet.funding_address.empty() ||
        !addresses.insert(wallet.address).second ||
        !wallet_nodes.contains(wallet.node - 1U)) {
      throw std::invalid_argument(
          "runtime role replacement contains an invalid wallet identity");
    }
  }
  std::set<std::string> pro_tx_hashes;
  std::set<std::string> services;
  for (const MasternodeIdentity& masternode : registry.masternodes()) {
    if (masternode.node == 0U || masternode.node > topology.node_count ||
        masternode.node_id.empty() ||
        masternode.funding_wallet_node_id.empty() ||
        masternode.pro_tx_hash.empty() || masternode.service.empty() ||
        masternode.collateral_address.empty() ||
        masternode.owner_address.empty() ||
        masternode.operator_public_key.empty() ||
        masternode.operator_secret_key.empty() ||
        masternode.voting_address.empty() ||
        masternode.payout_address.empty() || masternode.state.empty() ||
        !pro_tx_hashes.insert(masternode.pro_tx_hash).second ||
        !services.insert(masternode.service).second ||
        !masternode_nodes.contains(masternode.node - 1U)) {
      throw std::invalid_argument(
          "runtime role replacement contains an invalid masternode identity");
    }
  }
  auto next_generation = std::make_shared<RuntimeWalletSnapshot::Generation>(
      RuntimeWalletSnapshot::Generation{
          .sequence = generation_->sequence + 1U,
          .registry = std::move(registry),
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
