#include "bbp/simulator/transaction_load.h"

#include <algorithm>
#include <limits>
#include <stdexcept>
#include <utility>

namespace bbp {
namespace {

void Increment(std::uint64_t* value, std::string_view field) {
  if (*value == std::numeric_limits<std::uint64_t>::max()) {
    throw std::runtime_error("transaction load " + std::string(field) +
                             " counter overflow");
  }
  ++*value;
}

void RequireIncrementable(std::uint64_t value, std::string_view field) {
  if (value == std::numeric_limits<std::uint64_t>::max()) {
    throw std::runtime_error("transaction load " + std::string(field) +
                             " counter overflow");
  }
}

double PerSecond(std::uint64_t count, std::uint64_t elapsed_us) {
  if (elapsed_us == 0U) {
    return 0.0;
  }
  return static_cast<double>(count) * 1'000'000.0 /
         static_cast<double>(elapsed_us);
}

}  // namespace

std::string_view TransactionLoadOutcomeName(TransactionLoadOutcome outcome) {
  switch (outcome) {
    case TransactionLoadOutcome::kSubmitted:
      return "submitted";
    case TransactionLoadOutcome::kRejected:
      return "rejected";
    case TransactionLoadOutcome::kTimedOut:
      return "timed_out";
    case TransactionLoadOutcome::kBackpressured:
      return "backpressured";
    case TransactionLoadOutcome::kFailed:
      return "failed";
  }
  throw std::runtime_error("unknown transaction load outcome");
}

BoundedWalletTransactionQueue::BoundedWalletTransactionQueue(
    std::size_t capacity)
    : capacity_(capacity) {
  if (capacity_ == 0U) {
    throw std::runtime_error(
        "transaction load queue capacity must be greater than zero");
  }
}

bool BoundedWalletTransactionQueue::TryPush(WalletTransactionLoadTask task) {
  std::vector<WalletTransactionLoadTask> tasks;
  tasks.push_back(std::move(task));
  return TryPushBatch(std::move(tasks));
}

bool BoundedWalletTransactionQueue::TryPushBatch(
    std::vector<WalletTransactionLoadTask> tasks) {
  std::lock_guard lock(mutex_);
  if (closed_) {
    throw std::runtime_error("transaction load queue is closed");
  }
  if (tasks.size() > capacity_ || tasks_.size() > capacity_ - tasks.size()) {
    return false;
  }
  for (WalletTransactionLoadTask& task : tasks) {
    tasks_.push_back(std::move(task));
  }
  maximum_size_ = std::max(maximum_size_, tasks_.size());
  ready_.notify_all();
  return true;
}

std::optional<WalletTransactionLoadTask> BoundedWalletTransactionQueue::Pop(
    std::stop_token stop_token) {
  std::unique_lock lock(mutex_);
  if (!ready_.wait(lock, stop_token,
                   [this] { return closed_ || !tasks_.empty(); })) {
    return std::nullopt;
  }
  if (tasks_.empty()) {
    return std::nullopt;
  }
  WalletTransactionLoadTask task = std::move(tasks_.front());
  tasks_.pop_front();
  return task;
}

void BoundedWalletTransactionQueue::Close() {
  std::lock_guard lock(mutex_);
  closed_ = true;
  ready_.notify_all();
}

std::size_t BoundedWalletTransactionQueue::capacity() const {
  return capacity_;
}

std::size_t BoundedWalletTransactionQueue::size() const {
  std::lock_guard lock(mutex_);
  return tasks_.size();
}

std::size_t BoundedWalletTransactionQueue::maximum_size() const {
  std::lock_guard lock(mutex_);
  return maximum_size_;
}

bool BoundedWalletTransactionQueue::closed() const {
  std::lock_guard lock(mutex_);
  return closed_;
}

TransactionLoadBalanceReservations::TransactionLoadBalanceReservations(
    std::vector<std::uint64_t> available_balances,
    std::uint64_t fee_reserve_satoshis, std::size_t maximum_reservations)
    : fee_reserve_satoshis_(fee_reserve_satoshis),
      maximum_reservations_(maximum_reservations),
      available_balances_(std::move(available_balances)),
      reserved_by_sender_(available_balances_.size(), 0U) {
  if (available_balances_.empty()) {
    throw std::runtime_error(
        "transaction load balance ledger requires a wallet");
  }
  if (fee_reserve_satoshis_ == 0U) {
    throw std::runtime_error(
        "transaction load balance ledger requires a fee reserve");
  }
  if (maximum_reservations_ == 0U) {
    throw std::runtime_error(
        "transaction load balance ledger requires a positive bound");
  }
}

TransactionLoadBatchAdmission
TransactionLoadBalanceReservations::PlanAndReserve(
    WalletTransactionLoadPlanner* planner,
    std::uint64_t first_transaction_index, std::uint64_t remaining_attempts,
    const AdmissionCallback& admit_batch) {
  if (planner == nullptr) {
    throw std::runtime_error(
        "transaction load balance ledger requires a planner");
  }
  if (first_transaction_index == 0U || remaining_attempts == 0U) {
    throw std::runtime_error(
        "transaction load balance ledger requires a valid attempt range");
  }
  if (!admit_batch) {
    throw std::runtime_error(
        "transaction load balance ledger requires an admission callback");
  }

  std::unique_lock lock(mutex_);
  std::vector<std::uint64_t> planned_balances = available_balances_;
  std::optional<std::vector<WalletTransactionPlanEntry>> planned =
      planner->NextBatch(&planned_balances);
  if (!planned) {
    return TransactionLoadBatchAdmission{
        .plans = {},
        .admitted = false,
        .balance_revision = balance_revision_,
    };
  }
  if (planned->empty() || planned->size() > remaining_attempts) {
    throw std::runtime_error(
        "transaction load planner returned an invalid batch size");
  }
  if (reservations_.size() > maximum_reservations_ ||
      planned->size() > maximum_reservations_ - reservations_.size()) {
    throw std::runtime_error(
        "transaction load reservation bound would be exceeded");
  }

  std::vector<std::uint64_t> expected_balances = available_balances_;
  std::vector<std::uint64_t> expected_reserved = reserved_by_sender_;
  std::map<std::uint64_t, Reservation> staged;
  std::vector<std::uint64_t> transaction_indexes;
  transaction_indexes.reserve(planned->size());
  for (std::size_t offset = 0U; offset < planned->size(); ++offset) {
    if (offset >
        std::numeric_limits<std::uint64_t>::max() - first_transaction_index) {
      throw std::runtime_error(
          "transaction load reservation transaction index overflows uint64");
    }
    const std::uint64_t transaction_index =
        first_transaction_index + static_cast<std::uint64_t>(offset);
    if (reservations_.contains(transaction_index)) {
      throw std::runtime_error(
          "transaction load reservation transaction index is duplicated");
    }
    const WalletTransactionPlanEntry& entry = planned->at(offset);
    if (entry.sender_index >= expected_balances.size()) {
      throw std::runtime_error(
          "transaction load reservation sender is outside the balance ledger");
    }
    if (entry.amount_satoshis >
        std::numeric_limits<std::uint64_t>::max() - fee_reserve_satoshis_) {
      throw std::runtime_error(
          "transaction load reservation amount plus fee overflows uint64");
    }
    const std::uint64_t reserved_amount =
        entry.amount_satoshis + fee_reserve_satoshis_;
    if (expected_balances[entry.sender_index] < reserved_amount) {
      throw std::runtime_error(
          "transaction load reservation exceeds the available balance");
    }
    if (expected_reserved[entry.sender_index] >
        std::numeric_limits<std::uint64_t>::max() - reserved_amount) {
      throw std::runtime_error(
          "transaction load outstanding reservation total overflows uint64");
    }
    expected_balances[entry.sender_index] -= reserved_amount;
    expected_reserved[entry.sender_index] += reserved_amount;
    staged.emplace(transaction_index,
                   Reservation{.sender_index = entry.sender_index,
                               .amount_satoshis = reserved_amount});
    transaction_indexes.push_back(transaction_index);
  }
  if (expected_balances != planned_balances) {
    throw std::runtime_error(
        "transaction load planner balance delta does not match reservations");
  }

  available_balances_ = std::move(expected_balances);
  reserved_by_sender_ = std::move(expected_reserved);
  reservations_.merge(staged);
  if (!staged.empty()) {
    throw std::runtime_error(
        "transaction load reservation staging contains a duplicate");
  }
  maximum_size_ = std::max(maximum_size_, reservations_.size());

  bool admitted = false;
  try {
    admitted = admit_batch(*planned);
  } catch (...) {
    RollBackReservations(transaction_indexes);
    throw;
  }
  if (!admitted) {
    RollBackReservations(transaction_indexes);
  }
  return TransactionLoadBatchAdmission{
      .plans = std::move(*planned),
      .admitted = admitted,
      .balance_revision = balance_revision_,
  };
}

void TransactionLoadBalanceReservations::Settle(
    std::uint64_t transaction_index,
    std::optional<std::uint64_t> actual_available_balance,
    bool release_if_balance_unavailable) {
  std::string reconciliation_error;
  {
    std::lock_guard lock(mutex_);
    const auto found = reservations_.find(transaction_index);
    if (found == reservations_.end()) {
      throw std::runtime_error(
          "transaction load reservation was settled more than once");
    }
    const Reservation reservation = found->second;
    if (reserved_by_sender_.at(reservation.sender_index) <
        reservation.amount_satoshis) {
      throw std::runtime_error(
          "transaction load outstanding reservation total underflows");
    }
    const std::uint64_t remaining_reserved =
        reserved_by_sender_[reservation.sender_index] -
        reservation.amount_satoshis;
    std::uint64_t available = available_balances_[reservation.sender_index];
    if (actual_available_balance) {
      if (*actual_available_balance < remaining_reserved) {
        available = 0U;
        reconciliation_error =
            "transaction load actual balance is below outstanding "
            "reservations";
      } else {
        available = *actual_available_balance - remaining_reserved;
      }
    } else if (release_if_balance_unavailable) {
      if (available > std::numeric_limits<std::uint64_t>::max() -
                          reservation.amount_satoshis) {
        throw std::runtime_error(
            "transaction load released reservation overflows balance");
      }
      available += reservation.amount_satoshis;
    }
    if (balance_revision_ == std::numeric_limits<std::uint64_t>::max()) {
      throw std::runtime_error(
          "transaction load balance revision overflows uint64");
    }
    available_balances_[reservation.sender_index] = available;
    reserved_by_sender_[reservation.sender_index] = remaining_reserved;
    reservations_.erase(found);
    ++balance_revision_;
  }
  resolved_.notify_all();
  if (!reconciliation_error.empty()) {
    throw std::runtime_error(reconciliation_error);
  }
}

bool TransactionLoadBalanceReservations::WaitForResolution(
    std::uint64_t observed_revision, std::stop_token stop_token) {
  std::unique_lock lock(mutex_);
  if (reservations_.empty()) {
    return balance_revision_ != observed_revision;
  }
  return resolved_.wait(lock, stop_token,
                        [this] { return reservations_.empty(); });
}

std::vector<std::uint64_t>
TransactionLoadBalanceReservations::available_balances() const {
  std::lock_guard lock(mutex_);
  return available_balances_;
}

std::size_t TransactionLoadBalanceReservations::outstanding_size() const {
  std::lock_guard lock(mutex_);
  return reservations_.size();
}

std::size_t TransactionLoadBalanceReservations::maximum_size() const {
  std::lock_guard lock(mutex_);
  return maximum_size_;
}

void TransactionLoadBalanceReservations::RollBackReservations(
    const std::vector<std::uint64_t>& transaction_indexes) {
  for (const std::uint64_t transaction_index : transaction_indexes) {
    const auto found = reservations_.find(transaction_index);
    if (found == reservations_.end()) {
      throw std::runtime_error(
          "transaction load admission rollback lost a reservation");
    }
    const Reservation reservation = found->second;
    if (reserved_by_sender_.at(reservation.sender_index) <
        reservation.amount_satoshis) {
      throw std::runtime_error(
          "transaction load admission rollback underflows reservations");
    }
    if (available_balances_[reservation.sender_index] >
        std::numeric_limits<std::uint64_t>::max() -
            reservation.amount_satoshis) {
      throw std::runtime_error(
          "transaction load admission rollback overflows balance");
    }
    reserved_by_sender_[reservation.sender_index] -=
        reservation.amount_satoshis;
    available_balances_[reservation.sender_index] +=
        reservation.amount_satoshis;
    reservations_.erase(found);
  }
}

bool TransactionLoadSnapshot::InvariantsHold() const {
  if (submitted > std::numeric_limits<std::uint64_t>::max() - rejected) {
    return false;
  }
  const std::uint64_t submitted_and_rejected = submitted + rejected;
  if (submitted_and_rejected >
      std::numeric_limits<std::uint64_t>::max() - timed_out) {
    return false;
  }
  const std::uint64_t through_timeout = submitted_and_rejected + timed_out;
  if (through_timeout >
      std::numeric_limits<std::uint64_t>::max() - backpressured) {
    return false;
  }
  const std::uint64_t through_backpressure = through_timeout + backpressured;
  if (through_backpressure >
      std::numeric_limits<std::uint64_t>::max() - failed) {
    return false;
  }
  return attempted == through_backpressure + failed &&
         confirmed <= propagated && propagated <= submitted &&
         latency_sample_count == attempted;
}

void TransactionLoadAccounting::RecordOutcome(
    TransactionLoadOutcome outcome, std::chrono::microseconds latency) {
  if (latency.count() < 0) {
    throw std::runtime_error("transaction load latency must not be negative");
  }
  const auto latency_us = static_cast<std::uint64_t>(latency.count());
  std::lock_guard lock(mutex_);
  RequireIncrementable(counters_.attempted, "attempted");
  RequireIncrementable(counters_.latency_sample_count, "latency sample");
  if (counters_.latency_total_us >
      std::numeric_limits<std::uint64_t>::max() - latency_us) {
    throw std::runtime_error("transaction load latency total overflow");
  }
  switch (outcome) {
    case TransactionLoadOutcome::kSubmitted:
      RequireIncrementable(counters_.submitted, "submitted");
      break;
    case TransactionLoadOutcome::kRejected:
      RequireIncrementable(counters_.rejected, "rejected");
      break;
    case TransactionLoadOutcome::kTimedOut:
      RequireIncrementable(counters_.timed_out, "timed_out");
      break;
    case TransactionLoadOutcome::kBackpressured:
      RequireIncrementable(counters_.backpressured, "backpressured");
      break;
    case TransactionLoadOutcome::kFailed:
      RequireIncrementable(counters_.failed, "failed");
      break;
  }

  Increment(&counters_.attempted, "attempted");
  switch (outcome) {
    case TransactionLoadOutcome::kSubmitted:
      Increment(&counters_.submitted, "submitted");
      break;
    case TransactionLoadOutcome::kRejected:
      Increment(&counters_.rejected, "rejected");
      break;
    case TransactionLoadOutcome::kTimedOut:
      Increment(&counters_.timed_out, "timed_out");
      break;
    case TransactionLoadOutcome::kBackpressured:
      Increment(&counters_.backpressured, "backpressured");
      break;
    case TransactionLoadOutcome::kFailed:
      Increment(&counters_.failed, "failed");
      break;
  }
  counters_.latency_total_us += latency_us;
  Increment(&counters_.latency_sample_count, "latency sample");
  if (counters_.latency_sample_count == 1U) {
    counters_.latency_min_us = latency_us;
    counters_.latency_max_us = latency_us;
  } else {
    counters_.latency_min_us = std::min(counters_.latency_min_us, latency_us);
    counters_.latency_max_us = std::max(counters_.latency_max_us, latency_us);
  }
}

void TransactionLoadAccounting::RecordPropagated(bool confirmed) {
  std::lock_guard lock(mutex_);
  if (counters_.propagated == counters_.submitted) {
    throw std::runtime_error(
        "transaction load propagated count exceeds submitted count");
  }
  Increment(&counters_.propagated, "propagated");
  if (confirmed) {
    Increment(&counters_.confirmed, "confirmed");
  }
}

void TransactionLoadAccounting::RecordConfirmed() {
  std::lock_guard lock(mutex_);
  if (counters_.confirmed == counters_.propagated) {
    throw std::runtime_error(
        "transaction load confirmed count exceeds propagated count");
  }
  Increment(&counters_.confirmed, "confirmed");
}

void TransactionLoadAccounting::RecordObservationError() {
  std::lock_guard lock(mutex_);
  Increment(&counters_.observation_errors, "observation error");
}

TransactionLoadSnapshot TransactionLoadAccounting::Snapshot(
    std::chrono::microseconds elapsed) const {
  if (elapsed.count() < 0) {
    throw std::runtime_error(
        "transaction load elapsed time must not be negative");
  }
  std::lock_guard lock(mutex_);
  TransactionLoadSnapshot snapshot = counters_;
  snapshot.elapsed_us = static_cast<std::uint64_t>(elapsed.count());
  if (snapshot.latency_sample_count != 0U) {
    snapshot.average_latency_ms =
        static_cast<double>(snapshot.latency_total_us) /
        static_cast<double>(snapshot.latency_sample_count) / 1000.0;
  }
  snapshot.attempted_per_second =
      PerSecond(snapshot.attempted, snapshot.elapsed_us);
  snapshot.submitted_per_second =
      PerSecond(snapshot.submitted, snapshot.elapsed_us);
  snapshot.propagated_per_second =
      PerSecond(snapshot.propagated, snapshot.elapsed_us);
  snapshot.confirmed_per_second =
      PerSecond(snapshot.confirmed, snapshot.elapsed_us);
  if (!snapshot.InvariantsHold()) {
    throw std::runtime_error("transaction load accounting invariant failed");
  }
  return snapshot;
}

TransactionLoadConfirmation::TransactionLoadConfirmation(
    std::shared_ptr<TransactionLoadAccounting> accounting,
    std::vector<ObservationKey> expected_observations)
    : accounting_(std::move(accounting)),
      expected_observations_(expected_observations.begin(),
                             expected_observations.end()) {
  if (!accounting_) {
    throw std::runtime_error(
        "transaction load confirmation accounting is missing");
  }
  if (expected_observations.empty()) {
    throw std::runtime_error(
        "transaction load confirmation requires an expected observation");
  }
  if (expected_observations_.size() != expected_observations.size()) {
    throw std::runtime_error(
        "transaction load confirmation has duplicate expected observations");
  }
  for (const ObservationKey& observation : expected_observations_) {
    if (observation.first.empty() || observation.second.empty()) {
      throw std::runtime_error(
          "transaction load confirmation observation key is empty");
    }
  }
}

void TransactionLoadConfirmation::RecordObservation(std::string_view txid,
                                                    std::string_view node_id,
                                                    bool confirmed) {
  if (!confirmed) {
    return;
  }
  const ObservationKey key{std::string(txid), std::string(node_id)};
  std::lock_guard lock(mutex_);
  if (!expected_observations_.contains(key)) {
    throw std::runtime_error(
        "transaction load confirmation received an unexpected observation");
  }
  confirmed_observations_.insert(key);
  if (propagation_recorded_ && !confirmation_recorded_ &&
      confirmed_observations_.size() == expected_observations_.size()) {
    accounting_->RecordConfirmed();
    confirmation_recorded_ = true;
  }
}

void TransactionLoadConfirmation::RecordPropagated(bool confirmed) {
  std::lock_guard lock(mutex_);
  if (propagation_recorded_) {
    throw std::runtime_error(
        "transaction load propagation was recorded more than once");
  }
  const bool all_confirmed =
      confirmed_observations_.size() == expected_observations_.size();
  accounting_->RecordPropagated(confirmed || all_confirmed);
  propagation_recorded_ = true;
  confirmation_recorded_ = confirmed || all_confirmed;
}

bool TransactionLoadConfirmation::propagation_recorded() const {
  std::lock_guard lock(mutex_);
  return propagation_recorded_;
}

bool TransactionLoadConfirmation::confirmation_recorded() const {
  std::lock_guard lock(mutex_);
  return confirmation_recorded_;
}

}  // namespace bbp
