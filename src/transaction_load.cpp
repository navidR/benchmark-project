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

}  // namespace bbp
