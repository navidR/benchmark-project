#include <atomic>
#include <boost/test/unit_test.hpp>
#include <chrono>
#include <future>
#include <memory>
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>

#include "bbp/simulator/transaction_observation_store.h"

namespace {

bbp::TrackedTransaction Transaction(
    std::string txid,
    std::shared_ptr<bbp::TransactionLoadConfirmation> confirmation = nullptr) {
  return bbp::TrackedTransaction{
      .txid = std::move(txid),
      .submission_kind = "test",
      .workload_index = 1U,
      .workload_count = 1U,
      .transaction_index = 1U,
      .transaction_count = 1U,
      .transaction_rate = std::nullopt,
      .txid_index = 1U,
      .submission_node = 1U,
      .load_confirmation = std::move(confirmation),
  };
}

}  // namespace

BOOST_AUTO_TEST_CASE(transaction_observation_store_is_explicitly_bounded) {
  bbp::TransactionObservationStore store(2U);
  store.Track(Transaction("tx-1"), {"node-1", "node-2"});
  store.Track(Transaction("tx-2"), {"node-1", "node-2"});
  BOOST_CHECK_THROW(store.Track(Transaction("tx-3"), {"node-1", "node-2"}),
                    std::runtime_error);

  const bbp::TransactionObservationStoreStats stats = store.Stats();
  BOOST_TEST(stats.active == 2U);
  BOOST_TEST(stats.reserved == 0U);
  BOOST_TEST(stats.capacity == 2U);
  BOOST_TEST(stats.maximum_active == 2U);
  BOOST_TEST(stats.maximum_retained == 2U);
  BOOST_TEST(stats.tracked == 2U);
  BOOST_TEST(stats.rejected == 1U);
  BOOST_TEST(store.PendingTransactions().size() == 2U);
}

BOOST_AUTO_TEST_CASE(
    transaction_observation_reservation_prevents_mutation_when_full) {
  bbp::TransactionObservationStore store(1U);
  store.Track(Transaction("active"), {"node-1"});

  bool mutating_rpc_called = false;
  const std::optional<bbp::TransactionObservationStore::Reservation>
      reservation = store.TryReserve();
  if (reservation) {
    mutating_rpc_called = true;
  }

  BOOST_TEST(!reservation);
  BOOST_TEST(!mutating_rpc_called);
  const bbp::TransactionObservationStoreStats stats = store.Stats();
  BOOST_TEST(stats.active == 1U);
  BOOST_TEST(stats.reserved == 0U);
  BOOST_TEST(stats.maximum_retained == 1U);
  BOOST_TEST(stats.rejected == 1U);
}

BOOST_AUTO_TEST_CASE(
    transaction_observation_unused_reservation_releases_capacity) {
  bbp::TransactionObservationStore store(1U);
  {
    bbp::TransactionObservationStore::Reservation reservation = store.Reserve();
    BOOST_TEST(reservation.size() == 1U);
    BOOST_TEST(store.Stats().reserved == 1U);
    BOOST_TEST(!store.TryReserve());
  }

  BOOST_TEST(store.Stats().reserved == 0U);
  bbp::TransactionObservationStore::Reservation replacement = store.Reserve();
  replacement.Commit({Transaction("committed")}, {"node-1"});
  BOOST_TEST(store.Stats().active == 1U);
  BOOST_TEST(store.Stats().reserved == 0U);
}

BOOST_AUTO_TEST_CASE(
    transaction_observation_commit_is_atomic_and_preserves_reservation_on_error) {
  bbp::TransactionObservationStore store(2U);
  bbp::TransactionObservationStore::Reservation reservation = store.Reserve(2U);
  BOOST_CHECK_THROW(
      reservation.Commit({Transaction("duplicate"), Transaction("duplicate")},
                         {"node-1"}),
      std::runtime_error);
  BOOST_TEST(reservation.size() == 2U);
  BOOST_TEST(store.Stats().active == 0U);
  BOOST_TEST(store.Stats().reserved == 2U);
  BOOST_TEST(store.PendingTransactions().empty());

  reservation.Commit({Transaction("tx-1"), Transaction("tx-2")}, {"node-1"});
  BOOST_TEST(!static_cast<bool>(reservation));
  const bbp::TransactionObservationStoreStats stats = store.Stats();
  BOOST_TEST(stats.active == 2U);
  BOOST_TEST(stats.reserved == 0U);
  BOOST_TEST(stats.tracked == 2U);
}

BOOST_AUTO_TEST_CASE(
    transaction_observation_concurrent_reservations_never_exceed_capacity) {
  constexpr std::size_t kCapacity = 64U;
  constexpr std::size_t kWorkers = 8U;
  constexpr std::size_t kAttemptsPerWorker = 1'000U;
  bbp::TransactionObservationStore store(kCapacity);
  std::atomic<bool> release = false;
  std::vector<
      std::future<std::vector<bbp::TransactionObservationStore::Reservation>>>
      workers;
  for (std::size_t worker = 0U; worker < kWorkers; ++worker) {
    workers.push_back(std::async(std::launch::async, [&store, &release] {
      std::vector<bbp::TransactionObservationStore::Reservation> reservations;
      for (std::size_t attempt = 0U; attempt < kAttemptsPerWorker; ++attempt) {
        std::optional<bbp::TransactionObservationStore::Reservation>
            reservation = store.TryReserve();
        if (reservation) {
          reservations.push_back(std::move(*reservation));
        }
      }
      while (!release.load(std::memory_order_acquire)) {
        std::this_thread::yield();
      }
      return reservations;
    }));
  }

  while (store.Stats().reserved < kCapacity) {
    std::this_thread::yield();
  }
  const bbp::TransactionObservationStoreStats full = store.Stats();
  BOOST_TEST(full.active == 0U);
  BOOST_TEST(full.reserved == kCapacity);
  BOOST_TEST(full.maximum_retained == kCapacity);
  release.store(true, std::memory_order_release);
  std::size_t admitted = 0U;
  for (auto& worker : workers) {
    admitted += worker.get().size();
  }
  BOOST_TEST(admitted >= kCapacity);
  BOOST_TEST(store.Stats().reserved == 0U);
  BOOST_TEST(store.Stats().maximum_retained == kCapacity);
  BOOST_TEST(store.Stats().rejected + admitted ==
             kWorkers * kAttemptsPerWorker);
}

BOOST_AUTO_TEST_CASE(
    transaction_observation_store_retires_only_after_every_confirmation) {
  const auto accounting = std::make_shared<bbp::TransactionLoadAccounting>();
  accounting->RecordOutcome(bbp::TransactionLoadOutcome::kSubmitted,
                            std::chrono::microseconds(1));
  const auto confirmation = std::make_shared<bbp::TransactionLoadConfirmation>(
      accounting, std::vector<bbp::TransactionLoadConfirmation::ObservationKey>{
                      {"tx-1", "node-1"}, {"tx-1", "node-2"}});
  bbp::TransactionObservationStore store(2U);
  store.Track(Transaction("tx-1", confirmation), {"node-1", "node-2"});

  bbp::TransactionObservationTransition transition =
      store.Record("tx-1", "node-1", true, false);
  BOOST_TEST(transition.tracked);
  BOOST_TEST(transition.first_visible);
  BOOST_TEST(!transition.first_confirmed);
  BOOST_TEST(!transition.retired);
  confirmation->RecordPropagated(false);
  BOOST_TEST(accounting->Snapshot(std::chrono::microseconds(1)).propagated ==
             1U);

  transition = store.Record("tx-1", "node-1", true, true);
  BOOST_TEST(transition.first_confirmed);
  BOOST_TEST(!transition.retired);
  BOOST_TEST(store.Stats().active == 1U);
  transition = store.Record("tx-1", "node-2", true, true);
  BOOST_TEST(transition.first_visible);
  BOOST_TEST(transition.first_confirmed);
  BOOST_TEST(transition.retired);
  BOOST_TEST(store.Stats().active == 0U);
  BOOST_TEST(accounting->Snapshot(std::chrono::microseconds(1)).confirmed ==
             1U);

  const bbp::TransactionObservationTransition duplicate =
      store.Record("tx-1", "node-2", true, true);
  BOOST_TEST(!duplicate.tracked);
  BOOST_CHECK_THROW(store.Track(Transaction("tx-1"), {"node-1", "node-2"}),
                    std::runtime_error);
}

BOOST_AUTO_TEST_CASE(
    transaction_observation_store_work_is_independent_of_retired_history) {
  bbp::TransactionObservationStore store(4U);
  constexpr std::uint64_t kTransactionCount = 20'000U;
  const auto started = std::chrono::steady_clock::now();
  for (std::uint64_t index = 0U; index < kTransactionCount; ++index) {
    const std::string txid = "tx-" + std::to_string(index);
    store.Track(Transaction(txid), {"node-1", "node-2"});
    BOOST_TEST(store.PendingTransactions().size() == 1U);
    BOOST_TEST(!store.Record(txid, "node-1", true, true).retired);
    BOOST_TEST(store.Record(txid, "node-2", true, true).retired);
    BOOST_TEST(store.PendingTransactions().empty());
  }
  const auto elapsed = std::chrono::steady_clock::now() - started;
  const bbp::TransactionObservationStoreStats stats = store.Stats();
  BOOST_TEST(stats.active == 0U);
  BOOST_TEST(stats.maximum_active == 1U);
  BOOST_TEST(stats.recent_retired == 4U);
  BOOST_TEST(stats.tracked == kTransactionCount);
  BOOST_TEST(stats.retired == kTransactionCount);
  BOOST_TEST(stats.visibility_transitions == kTransactionCount * 2U);
  BOOST_TEST(stats.confirmation_transitions == kTransactionCount * 2U);
  BOOST_TEST(elapsed < std::chrono::seconds(5));
}

BOOST_AUTO_TEST_CASE(transaction_observation_store_is_concurrency_safe) {
  constexpr std::size_t kTransactionCount = 256U;
  bbp::TransactionObservationStore store(kTransactionCount);
  for (std::size_t index = 0U; index < kTransactionCount; ++index) {
    store.Track(Transaction("tx-" + std::to_string(index)),
                {"node-1", "node-2"});
  }

  std::vector<std::future<void>> workers;
  for (std::size_t worker = 0U; worker < 8U; ++worker) {
    workers.push_back(std::async(std::launch::async, [worker, &store] {
      for (std::size_t index = worker; index < kTransactionCount; index += 8U) {
        const std::string txid = "tx-" + std::to_string(index);
        static_cast<void>(store.Record(txid, "node-1", true, true));
        static_cast<void>(store.Record(txid, "node-2", true, true));
      }
    }));
  }
  for (std::future<void>& worker : workers) {
    worker.get();
  }

  const bbp::TransactionObservationStoreStats stats = store.Stats();
  BOOST_TEST(stats.active == 0U);
  BOOST_TEST(stats.maximum_active == kTransactionCount);
  BOOST_TEST(stats.retired == kTransactionCount);
}
