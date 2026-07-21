#pragma once

#include <chrono>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <deque>
#include <mutex>
#include <optional>
#include <stop_token>
#include <string_view>
#include <vector>

#include "bbp/simulator/wallet_transaction_plan.h"

namespace bbp {

enum class TransactionLoadOutcome {
  kSubmitted,
  kRejected,
  kTimedOut,
  kBackpressured,
  kFailed,
};

std::string_view TransactionLoadOutcomeName(TransactionLoadOutcome outcome);

struct WalletTransactionLoadTask {
  std::uint64_t transaction_index = 0;
  WalletTransactionPlanEntry plan;
  std::optional<std::chrono::milliseconds> scheduled_simulation_elapsed;
  std::optional<std::chrono::milliseconds> scheduled_wall_elapsed;
  std::chrono::steady_clock::time_point scheduled_at;
};

class BoundedWalletTransactionQueue {
 public:
  explicit BoundedWalletTransactionQueue(std::size_t capacity);

  bool TryPush(WalletTransactionLoadTask task);
  bool TryPushBatch(std::vector<WalletTransactionLoadTask> tasks);
  std::optional<WalletTransactionLoadTask> Pop(std::stop_token stop_token = {});
  void Close();

  [[nodiscard]] std::size_t capacity() const;
  [[nodiscard]] std::size_t size() const;
  [[nodiscard]] std::size_t maximum_size() const;
  [[nodiscard]] bool closed() const;

 private:
  const std::size_t capacity_;
  mutable std::mutex mutex_;
  std::condition_variable_any ready_;
  std::deque<WalletTransactionLoadTask> tasks_;
  std::size_t maximum_size_ = 0U;
  bool closed_ = false;
};

struct TransactionLoadSnapshot {
  std::uint64_t attempted = 0;
  std::uint64_t submitted = 0;
  std::uint64_t rejected = 0;
  std::uint64_t timed_out = 0;
  std::uint64_t backpressured = 0;
  std::uint64_t failed = 0;
  std::uint64_t propagated = 0;
  std::uint64_t confirmed = 0;
  std::uint64_t observation_errors = 0;
  std::uint64_t latency_sample_count = 0;
  std::uint64_t latency_total_us = 0;
  std::uint64_t latency_min_us = 0;
  std::uint64_t latency_max_us = 0;
  std::uint64_t elapsed_us = 0;
  double average_latency_ms = 0.0;
  double attempted_per_second = 0.0;
  double submitted_per_second = 0.0;
  double propagated_per_second = 0.0;
  double confirmed_per_second = 0.0;

  [[nodiscard]] bool InvariantsHold() const;
};

class TransactionLoadAccounting {
 public:
  void RecordOutcome(TransactionLoadOutcome outcome,
                     std::chrono::microseconds latency);
  void RecordPropagated(bool confirmed);
  void RecordConfirmed();
  void RecordObservationError();

  [[nodiscard]] TransactionLoadSnapshot Snapshot(
      std::chrono::microseconds elapsed) const;

 private:
  mutable std::mutex mutex_;
  TransactionLoadSnapshot counters_;
};

}  // namespace bbp
