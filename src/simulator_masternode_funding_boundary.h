#pragma once

#include <chrono>
#include <cstdint>
#include <mutex>
#include <stop_token>
#include <string>
#include <vector>

namespace bbp {

class ChainDriver;
struct ChainMasternodeFundingRequirements;
struct ChainMasternodeRegistration;
struct ChainMasternodeStatus;
struct MasternodeIdentity;
struct NodeRuntime;

namespace simulator_app_internal {

using RuntimeNodePointers = std::vector<NodeRuntime*>;

std::uint64_t PrepareMasternodeFunding(
    const ChainDriver& driver, std::timed_mutex& block_generation_mutex,
    NodeRuntime& miner, NodeRuntime& funding_wallet,
    const std::string& funding_address, const RuntimeNodePointers& nodes,
    const ChainMasternodeFundingRequirements& requirements,
    std::chrono::seconds timeout, std::stop_token stop_token);

struct MasternodeTransactionConfirmation {
  NodeRuntime* funding_wallet = nullptr;
  std::string transaction_id;
};

void ConfirmMasternodeTransactions(
    const ChainDriver& driver, std::timed_mutex& block_generation_mutex,
    NodeRuntime& miner, const std::string& reward_address,
    const RuntimeNodePointers& nodes,
    const std::vector<MasternodeTransactionConfirmation>& transactions,
    std::uint32_t confirmation_blocks, std::chrono::seconds timeout,
    std::stop_token stop_token);
MasternodeIdentity RegisteredMasternodeIdentity(
    std::uint32_t node, std::string node_id, std::string funding_wallet_node_id,
    const ChainMasternodeRegistration& registration,
    const ChainMasternodeStatus& status);

}  // namespace simulator_app_internal
}  // namespace bbp
