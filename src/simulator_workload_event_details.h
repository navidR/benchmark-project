#pragma once

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

namespace bbp {

struct ChainRawTransactionResult;
struct ChainTransactionObservation;
struct ChainWalletFundingResult;
struct ChainWalletTransactionResult;
struct SendRawTransactionWorkload;
struct SimulationWalletSend;
struct TransactionLoadSnapshot;
struct TrackedTransaction;
struct WalletIdentity;
struct WalletInitialization;
struct WalletTransactionLoadTask;
struct WalletTransactionsWorkload;
enum class TransactionLoadOutcome;

namespace simulator_app_internal {

std::string GeneratedBlocksDetail(
    std::uint32_t workload_index, std::uint32_t workload_count,
    std::uint32_t generator_node, std::uint64_t start_height,
    std::uint64_t target_height, const std::vector<std::string>& hashes,
    const std::string& reward_address,
    std::optional<std::uint64_t> operator_command_sequence = std::nullopt,
    std::optional<std::string_view> workload_id = std::nullopt);
std::string HeightWaitDetail(
    std::uint32_t workload_index, std::uint32_t workload_count,
    std::uint32_t node, std::uint64_t target_height,
    std::uint64_t observed_height,
    std::optional<std::string_view> workload_id = std::nullopt);
std::string PeerCountWaitDetail(
    std::uint32_t workload_index, std::uint32_t workload_count,
    std::uint32_t node, std::uint64_t target_peer_count,
    std::uint64_t observed_peer_count,
    std::optional<std::string_view> workload_id = std::nullopt);
std::string PeerChurnDetail(std::uint32_t workload_index,
                            std::uint32_t workload_count, std::uint32_t node,
                            std::uint32_t peer, const std::string& address,
                            std::uint64_t before_peer_count,
                            std::uint64_t after_peer_count,
                            bool connected_before, bool connected_after,
                            std::optional<std::uint32_t> timeout_sec);
std::string RawTransactionDetail(std::uint32_t workload_index,
                                 std::uint32_t workload_count,
                                 const SendRawTransactionWorkload& workload,
                                 std::uint64_t start_height,
                                 std::uint64_t target_height,
                                 const std::vector<std::string>& funding_hashes,
                                 const ChainRawTransactionResult& transaction,
                                 bool mempool_size_observed = true);
std::string WalletFundingDetail(
    std::uint32_t workload_index, std::uint32_t workload_count,
    const WalletTransactionsWorkload& workload, const WalletIdentity& wallet,
    std::uint32_t miner_node, std::uint64_t start_height,
    std::uint64_t target_height, std::uint64_t funding_hash_count,
    std::uint64_t ready_height, const ChainWalletFundingResult& preparation,
    std::uint64_t preparation_hash_count, std::uint64_t ready_balance_satoshis);
std::string WalletTransactionDetail(
    std::uint32_t workload_index, std::uint32_t workload_count,
    const WalletTransactionsWorkload& workload, std::uint64_t transaction_index,
    const WalletIdentity& sender, const WalletIdentity& receiver,
    std::uint32_t funding_miner_node, std::uint64_t funding_start_height,
    std::uint64_t funding_target_height, std::uint64_t funding_hash_count,
    std::uint64_t funding_ready_height,
    const ChainWalletFundingResult& funding_preparation,
    std::uint64_t funding_preparation_hash_count,
    std::uint64_t funding_ready_balance_satoshis, std::uint64_t amount_satoshis,
    std::chrono::milliseconds interval_before,
    std::optional<std::chrono::milliseconds> scheduled_simulation_elapsed,
    std::optional<std::chrono::milliseconds> scheduled_wall_elapsed,
    const ChainWalletTransactionResult& transaction);
std::string TransactionLoadAttemptDetail(
    std::uint32_t workload_index, std::uint32_t workload_count,
    const WalletTransactionsWorkload& workload,
    std::optional<std::uint64_t> attempt_limit,
    const WalletTransactionLoadTask& task, const WalletIdentity& sender,
    const WalletIdentity& receiver, TransactionLoadOutcome outcome,
    std::chrono::microseconds latency,
    const ChainWalletTransactionResult* transaction,
    std::string_view error_class, std::string_view error_message);
std::string TransactionLoadProgressDetail(
    std::uint32_t workload_index, std::uint32_t workload_count,
    const TransactionLoadSnapshot& snapshot);
std::string TransactionLoadCompletedDetail(
    std::uint32_t workload_index, std::uint32_t workload_count,
    const WalletTransactionsWorkload& workload,
    std::optional<std::uint64_t> attempt_limit, std::size_t queue_maximum_size,
    const TransactionLoadSnapshot& snapshot);
std::string OperatorWalletTransactionDetail(
    const SimulationWalletSend& send, const WalletIdentity& sender,
    const WalletIdentity& receiver,
    const WalletInitialization& wallet_initialization,
    const ChainWalletTransactionResult& transaction,
    std::uint64_t operator_command_sequence);
std::string TransactionObservationDetail(
    const TrackedTransaction& transaction, std::uint32_t node,
    const std::string& node_id, const ChainTransactionObservation& observation);

}  // namespace simulator_app_internal
}  // namespace bbp
