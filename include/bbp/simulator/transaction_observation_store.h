#pragma once

#include <cstddef>
#include <cstdint>
#include <deque>
#include <memory>
#include <mutex>
#include <optional>
#include <set>
#include <string>
#include <string_view>
#include <vector>

#include "bbp/simulator/transaction_load.h"

namespace bbp {

constexpr std::size_t kMaximumRetainedTransactionObservations = 256U;
constexpr std::size_t kMaximumTransactionObservationNodes = 16U;

struct TrackedTransaction {
  std::string txid;
  std::string submission_kind;
  std::uint32_t workload_index = 0U;
  std::uint32_t workload_count = 0U;
  std::uint64_t transaction_index = 0U;
  std::optional<std::uint32_t> transaction_count;
  std::optional<double> transaction_rate;
  std::uint32_t txid_index = 0U;
  std::uint32_t submission_node = 0U;
  std::shared_ptr<TransactionLoadConfirmation> load_confirmation;
};

struct TransactionObservationTransition {
  bool tracked = false;
  bool first_visible = false;
  bool first_confirmed = false;
  bool retired = false;
  std::optional<TransactionLoadSnapshot> load_progress;
};

struct TransactionObservationStoreStats {
  std::size_t active = 0U;
  std::size_t reserved = 0U;
  std::size_t capacity = 0U;
  std::size_t maximum_active = 0U;
  std::size_t maximum_retained = 0U;
  std::size_t recent_retired = 0U;
  std::uint64_t tracked = 0U;
  std::uint64_t retired = 0U;
  std::uint64_t rejected = 0U;
  std::uint64_t visibility_transitions = 0U;
  std::uint64_t confirmation_transitions = 0U;
};

class TransactionObservationStore {
 public:
  class Reservation {
   public:
    Reservation() = default;
    Reservation(const Reservation&) = delete;
    Reservation& operator=(const Reservation&) = delete;
    Reservation(Reservation&& other) noexcept;
    Reservation& operator=(Reservation&& other) noexcept;
    ~Reservation();

    void Commit(std::vector<TrackedTransaction> transactions,
                const std::vector<std::string>& required_node_ids);
    [[nodiscard]] std::size_t size() const noexcept { return size_; }
    [[nodiscard]] explicit operator bool() const noexcept {
      return owner_ != nullptr;
    }

   private:
    friend class TransactionObservationStore;
    Reservation(TransactionObservationStore* owner, std::size_t size)
        : owner_(owner), size_(size) {}
    void Release() noexcept;

    TransactionObservationStore* owner_ = nullptr;
    std::size_t size_ = 0U;
  };

  explicit TransactionObservationStore(
      std::size_t capacity = kMaximumRetainedTransactionObservations);

  [[nodiscard]] std::optional<Reservation> TryReserve(std::size_t count = 1U);
  [[nodiscard]] Reservation Reserve(std::size_t count = 1U);
  void Track(TrackedTransaction transaction,
             const std::vector<std::string>& required_node_ids);
  void TrackSet(std::vector<TrackedTransaction> transactions,
                const std::vector<std::string>& required_node_ids);
  [[nodiscard]] std::vector<TrackedTransaction> PendingTransactions() const;
  TransactionObservationTransition Record(std::string_view txid,
                                          std::string_view node_id,
                                          bool visible, bool confirmed);
  [[nodiscard]] TransactionObservationStoreStats Stats() const;

 private:
  struct Entry {
    TrackedTransaction transaction;
    std::set<std::string> required_node_ids;
    std::set<std::string> visible_node_ids;
    std::set<std::string> confirmed_node_ids;
  };

  void CommitReservation(Reservation* reservation,
                         std::vector<TrackedTransaction> transactions,
                         const std::vector<std::string>& required_node_ids);
  void ReleaseReservation(std::size_t count) noexcept;

  mutable std::mutex mutex_;
  const std::size_t capacity_;
  std::vector<Entry> entries_;
  std::size_t reserved_ = 0U;
  std::deque<std::string> recent_retired_;
  std::set<std::string> recent_retired_index_;
  std::size_t maximum_active_ = 0U;
  std::size_t maximum_retained_ = 0U;
  std::uint64_t tracked_ = 0U;
  std::uint64_t retired_ = 0U;
  std::uint64_t rejected_ = 0U;
  std::uint64_t visibility_transitions_ = 0U;
  std::uint64_t confirmation_transitions_ = 0U;
};

}  // namespace bbp
