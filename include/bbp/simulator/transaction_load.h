#pragma once

#include <chrono>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <deque>
#include <functional>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <set>
#include <stop_token>
#include <string>
#include <string_view>
#include <utility>
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

struct TransactionLoadBatchAdmission {
  std::vector<WalletTransactionPlanEntry> plans;
  bool admitted = false;
  std::uint64_t balance_revision = 0U;

  [[nodiscard]] bool has_plan() const { return !plans.empty(); }
};

class TransactionLoadBalanceReservations {
 public:
  using AdmissionCallback =
      std::function<bool(const std::vector<WalletTransactionPlanEntry>&)>;

  TransactionLoadBalanceReservations(
      std::vector<std::uint64_t> available_balances,
      std::uint64_t fee_reserve_satoshis, std::size_t maximum_reservations);

  TransactionLoadBatchAdmission PlanAndReserve(
      WalletTransactionLoadPlanner* planner,
      std::uint64_t first_transaction_index, std::uint64_t remaining_attempts,
      const AdmissionCallback& admit_batch);
  void Settle(std::uint64_t transaction_index,
              std::optional<std::uint64_t> actual_available_balance,
              bool release_if_balance_unavailable);
  bool WaitForResolution(std::uint64_t observed_revision,
                         std::stop_token stop_token = {});

  [[nodiscard]] std::vector<std::uint64_t> available_balances() const;
  [[nodiscard]] std::size_t outstanding_size() const;
  [[nodiscard]] std::size_t maximum_size() const;

 private:
  struct Reservation {
    std::size_t sender_index = 0U;
    std::uint64_t amount_satoshis = 0U;
  };

  void RollBackReservations(
      const std::vector<std::uint64_t>& transaction_indexes);

  const std::uint64_t fee_reserve_satoshis_;
  const std::size_t maximum_reservations_;
  mutable std::mutex mutex_;
  std::condition_variable_any resolved_;
  std::vector<std::uint64_t> available_balances_;
  std::vector<std::uint64_t> reserved_by_sender_;
  std::map<std::uint64_t, Reservation> reservations_;
  std::size_t maximum_size_ = 0U;
  std::uint64_t balance_revision_ = 0U;
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

class TransactionLoadConfirmation {
 public:
  using ObservationKey = std::pair<std::string, std::string>;

  TransactionLoadConfirmation(
      std::shared_ptr<TransactionLoadAccounting> accounting,
      std::vector<ObservationKey> expected_observations);

  void RecordObservation(std::string_view txid, std::string_view node_id,
                         bool confirmed);
  void RecordPropagated(bool confirmed);

  [[nodiscard]] bool propagation_recorded() const;
  [[nodiscard]] bool confirmation_recorded() const;

 private:
  const std::shared_ptr<TransactionLoadAccounting> accounting_;
  const std::set<ObservationKey> expected_observations_;
  mutable std::mutex mutex_;
  std::set<ObservationKey> confirmed_observations_;
  bool propagation_recorded_ = false;
  bool confirmation_recorded_ = false;
};

}  // namespace bbp
