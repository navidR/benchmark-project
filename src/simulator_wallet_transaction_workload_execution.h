#pragma once

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <functional>
#include <memory>
#include <mutex>
#include <optional>
#include <span>
#include <stop_token>
#include <string>
#include <vector>

#include "bbp/drivers/chain_driver.h"
#include "bbp/simulator/transaction_load.h"

namespace bbp {

class RuntimeNodeSnapshot;
class SimulationRegistry;
struct Options;

namespace simulator_app_internal {

class TransactionObservationTracker;

struct PendingTransactionLoadCompletion {
  std::uint32_t workload_index = 0U;
  std::uint32_t workload_count = 0U;
  WalletTransactionsWorkload workload;
  std::optional<std::uint64_t> attempt_limit;
  std::size_t queue_maximum_size = 0U;
  std::shared_ptr<TransactionLoadAccounting> accounting;
  std::chrono::microseconds elapsed{0};
};

enum class WalletWorkloadExecutionEnd {
  kCompleted,
  kYielded,
  kRefreshBalances,
};

struct WalletWorkloadFundingState {
  uint32_t miner_node = 1;
  uint64_t start_height = 0;
  uint64_t target_height = 0;
  uint64_t ready_height = 0;
  uint64_t ready_balance_satoshis = 0;
  std::vector<std::string> hashes;
  ChainWalletFundingResult preparation;
  std::vector<std::string> preparation_hashes;
};

struct WalletWorkloadExecutionContext {
  std::shared_ptr<TransactionLoadAccounting> accounting;
  std::string workload_id;
  std::chrono::steady_clock::time_point started_at;
  std::uint64_t next_transaction_index = 0U;
  std::optional<std::vector<WalletWorkloadFundingState>>* prepared_funding =
      nullptr;
  std::function<bool()> yield_requested;
  std::function<bool()> settle_requested;
  std::function<void(std::uint64_t)> record_planned;
  std::function<void(std::uint64_t)> record_accepted;
  std::function<void(std::span<const WalletTransactionPlanEntry>)>
      record_load_admission;
  std::function<void(std::uint64_t)> record_released_atomic_units;
  std::function<void()> execution_started;
};

struct WalletWorkloadExecutionResult {
  WalletWorkloadExecutionEnd end = WalletWorkloadExecutionEnd::kCompleted;
  std::uint64_t next_transaction_index = 0U;
  std::size_t queue_maximum_size = 0U;
};

WalletWorkloadExecutionResult ApplyWalletTransactionsWorkload(
    const Options& options, const std::filesystem::path& events_path,
    const ChainDriver& driver, std::timed_mutex& block_generation_mutex,
    const RuntimeNodeSnapshot& nodes, const SimulationRegistry& registry,
    TransactionObservationTracker& transaction_tracker,
    std::vector<PendingTransactionLoadCompletion>* pending_load_completions,
    const WalletTransactionsWorkload& workload, std::uint32_t workload_index,
    std::uint32_t workload_count, std::stop_token stop_token,
    WalletWorkloadExecutionContext* execution = nullptr);

}  // namespace simulator_app_internal
}  // namespace bbp
