#include "bbp/simulator/transaction_observation_store.h"

#include <algorithm>
#include <limits>
#include <stdexcept>
#include <string>
#include <utility>

namespace bbp {
namespace {

void CheckedAdd(std::uint64_t amount, std::string_view field,
                std::uint64_t* value) {
  if (*value > std::numeric_limits<std::uint64_t>::max() - amount) {
    throw std::runtime_error("transaction observation " + std::string(field) +
                             " exceeds uint64");
  }
  *value += amount;
}

}  // namespace

TransactionObservationStore::TransactionObservationStore(std::size_t capacity)
    : capacity_(capacity) {
  if (capacity_ == 0U) {
    throw std::runtime_error(
        "transaction observation capacity must be greater than zero");
  }
  entries_.reserve(capacity_);
}

TransactionObservationStore::Reservation::Reservation(
    Reservation&& other) noexcept
    : owner_(std::exchange(other.owner_, nullptr)),
      size_(std::exchange(other.size_, 0U)) {}

TransactionObservationStore::Reservation&
TransactionObservationStore::Reservation::operator=(
    Reservation&& other) noexcept {
  if (this != &other) {
    Release();
    owner_ = std::exchange(other.owner_, nullptr);
    size_ = std::exchange(other.size_, 0U);
  }
  return *this;
}

TransactionObservationStore::Reservation::~Reservation() { Release(); }

void TransactionObservationStore::Reservation::Commit(
    std::vector<TrackedTransaction> transactions,
    const std::vector<std::string>& required_node_ids) {
  if (owner_ == nullptr) {
    throw std::runtime_error(
        "transaction observation reservation is not active");
  }
  owner_->CommitReservation(this, std::move(transactions), required_node_ids);
}

void TransactionObservationStore::Reservation::Release() noexcept {
  if (owner_ != nullptr) {
    owner_->ReleaseReservation(size_);
    owner_ = nullptr;
    size_ = 0U;
  }
}

std::optional<TransactionObservationStore::Reservation>
TransactionObservationStore::TryReserve(std::size_t count) {
  if (count == 0U || count > capacity_) {
    throw std::runtime_error(
        "transaction observation reservation size must be in 1.." +
        std::to_string(capacity_));
  }
  std::lock_guard<std::mutex> lock(mutex_);
  const std::size_t retained = entries_.size() + reserved_;
  if (count > capacity_ - retained) {
    CheckedAdd(static_cast<std::uint64_t>(count), "rejections", &rejected_);
    return std::nullopt;
  }
  reserved_ += count;
  maximum_retained_ = std::max(maximum_retained_, retained + count);
  return Reservation(this, count);
}

TransactionObservationStore::Reservation TransactionObservationStore::Reserve(
    std::size_t count) {
  std::optional<Reservation> reservation = TryReserve(count);
  if (!reservation) {
    throw std::runtime_error(
        "transaction observation capacity is full (capacity " +
        std::to_string(capacity_) + ")");
  }
  return std::move(*reservation);
}

void TransactionObservationStore::Track(
    TrackedTransaction transaction,
    const std::vector<std::string>& required_node_ids) {
  std::vector<TrackedTransaction> transactions;
  transactions.push_back(std::move(transaction));
  TrackSet(std::move(transactions), required_node_ids);
}

void TransactionObservationStore::TrackSet(
    std::vector<TrackedTransaction> transactions,
    const std::vector<std::string>& required_node_ids) {
  Reservation reservation = Reserve(transactions.size());
  reservation.Commit(std::move(transactions), required_node_ids);
}

void TransactionObservationStore::CommitReservation(
    Reservation* reservation, std::vector<TrackedTransaction> transactions,
    const std::vector<std::string>& required_node_ids) {
  if (reservation == nullptr || reservation->owner_ != this) {
    throw std::runtime_error(
        "transaction observation reservation belongs to another store");
  }
  if (transactions.empty()) {
    throw std::runtime_error("cannot track an empty transaction set");
  }
  if (transactions.size() != reservation->size_) {
    throw std::runtime_error(
        "transaction observation reservation size does not match transaction "
        "count");
  }
  const std::set<std::string> required(required_node_ids.begin(),
                                       required_node_ids.end());
  if (required.empty() || required.size() != required_node_ids.size()) {
    throw std::runtime_error(
        "transaction observation nodes must be nonempty and unique");
  }
  if (required.size() > kMaximumTransactionObservationNodes) {
    throw std::runtime_error(
        "transaction observation node count exceeds " +
        std::to_string(kMaximumTransactionObservationNodes));
  }
  for (const std::string& node_id : required) {
    if (node_id.empty()) {
      throw std::runtime_error(
          "transaction observation node id must not be empty");
    }
  }

  std::vector<Entry> staged;
  staged.reserve(transactions.size());
  std::set<std::string> new_txids;
  for (TrackedTransaction& transaction : transactions) {
    if (transaction.txid.empty()) {
      throw std::runtime_error("cannot track an empty transaction id");
    }
    if (!new_txids.insert(transaction.txid).second) {
      throw std::runtime_error("duplicate submitted transaction id: " +
                               transaction.txid);
    }
    staged.push_back(Entry{
        .transaction = std::move(transaction),
        .required_node_ids = required,
        .visible_node_ids = {},
        .confirmed_node_ids = {},
    });
  }

  std::lock_guard<std::mutex> lock(mutex_);
  if (reservation->owner_ != this || reservation->size_ > reserved_) {
    throw std::runtime_error(
        "transaction observation reservation is not active");
  }
  for (const Entry& staged_entry : staged) {
    const std::string& txid = staged_entry.transaction.txid;
    const bool active_duplicate = std::any_of(
        entries_.begin(), entries_.end(),
        [&](const Entry& entry) { return entry.transaction.txid == txid; });
    if (active_duplicate || recent_retired_index_.contains(txid)) {
      throw std::runtime_error("duplicate submitted transaction id: " + txid);
    }
  }
  CheckedAdd(static_cast<std::uint64_t>(staged.size()), "tracked count",
             &tracked_);
  for (Entry& entry : staged) {
    entries_.push_back(std::move(entry));
  }
  reserved_ -= reservation->size_;
  reservation->owner_ = nullptr;
  reservation->size_ = 0U;
  maximum_active_ = std::max(maximum_active_, entries_.size());
}

void TransactionObservationStore::ReleaseReservation(
    std::size_t count) noexcept {
  std::lock_guard<std::mutex> lock(mutex_);
  if (count > reserved_) {
    std::terminate();
  }
  reserved_ -= count;
}

std::vector<TrackedTransaction>
TransactionObservationStore::PendingTransactions() const {
  std::lock_guard<std::mutex> lock(mutex_);
  std::vector<TrackedTransaction> pending;
  pending.reserve(entries_.size());
  for (const Entry& entry : entries_) {
    pending.push_back(entry.transaction);
  }
  return pending;
}

TransactionObservationTransition TransactionObservationStore::Record(
    std::string_view txid, std::string_view node_id, bool visible,
    bool confirmed) {
  if (confirmed && !visible) {
    throw std::runtime_error(
        "confirmed transaction observation must also be visible");
  }
  if (!visible) {
    return {};
  }

  TransactionObservationTransition transition;
  std::shared_ptr<TransactionLoadConfirmation> load_confirmation;
  {
    std::lock_guard<std::mutex> lock(mutex_);
    const auto entry = std::find_if(
        entries_.begin(), entries_.end(),
        [&](const Entry& value) { return value.transaction.txid == txid; });
    if (entry == entries_.end()) {
      return {};
    }
    if (!entry->required_node_ids.contains(std::string(node_id))) {
      throw std::runtime_error(
          "transaction observation used unexpected node: " +
          std::string(node_id));
    }

    transition.tracked = true;
    transition.first_visible =
        entry->visible_node_ids.insert(std::string(node_id)).second;
    if (transition.first_visible) {
      CheckedAdd(1U, "visibility transition count", &visibility_transitions_);
    }
    if (confirmed) {
      transition.first_confirmed =
          entry->confirmed_node_ids.insert(std::string(node_id)).second;
      if (transition.first_confirmed) {
        CheckedAdd(1U, "confirmation transition count",
                   &confirmation_transitions_);
      }
    }
    load_confirmation = entry->transaction.load_confirmation;
    if (entry->confirmed_node_ids.size() == entry->required_node_ids.size()) {
      const std::string retired_txid = entry->transaction.txid;
      entries_.erase(entry);
      if (recent_retired_.size() >= capacity_) {
        recent_retired_index_.erase(recent_retired_.front());
        recent_retired_.pop_front();
      }
      recent_retired_.push_back(retired_txid);
      recent_retired_index_.insert(retired_txid);
      CheckedAdd(1U, "retired count", &retired_);
      transition.retired = true;
    }
  }
  if (load_confirmation) {
    load_confirmation->RecordObservation(txid, node_id, confirmed);
  }
  return transition;
}

TransactionObservationStoreStats TransactionObservationStore::Stats() const {
  std::lock_guard<std::mutex> lock(mutex_);
  return TransactionObservationStoreStats{
      .active = entries_.size(),
      .reserved = reserved_,
      .capacity = capacity_,
      .maximum_active = maximum_active_,
      .maximum_retained = maximum_retained_,
      .recent_retired = recent_retired_.size(),
      .tracked = tracked_,
      .retired = retired_,
      .rejected = rejected_,
      .visibility_transitions = visibility_transitions_,
      .confirmation_transitions = confirmation_transitions_,
  };
}

}  // namespace bbp
