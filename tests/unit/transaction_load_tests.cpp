#include <boost/test/unit_test.hpp>
#include <chrono>
#include <future>
#include <optional>
#include <stdexcept>
#include <stop_token>

#include "bbp/simulator/transaction_load.h"

namespace {

using namespace std::chrono_literals;

bbp::WalletTransactionLoadTask Task(std::uint64_t transaction_index) {
  return bbp::WalletTransactionLoadTask{
      .transaction_index = transaction_index,
      .plan = {.sender_index = 0U,
               .receiver_index = 1U,
               .amount_satoshis = transaction_index,
               .interval_before = 0ms},
      .scheduled_simulation_elapsed = std::nullopt,
      .scheduled_wall_elapsed = std::nullopt,
      .scheduled_at = std::chrono::steady_clock::now(),
  };
}

}  // namespace

BOOST_AUTO_TEST_CASE(transaction_load_outcome_names_are_stable) {
  BOOST_TEST(bbp::TransactionLoadOutcomeName(
                 bbp::TransactionLoadOutcome::kSubmitted) == "submitted");
  BOOST_TEST(bbp::TransactionLoadOutcomeName(
                 bbp::TransactionLoadOutcome::kRejected) == "rejected");
  BOOST_TEST(bbp::TransactionLoadOutcomeName(
                 bbp::TransactionLoadOutcome::kTimedOut) == "timed_out");
  BOOST_TEST(bbp::TransactionLoadOutcomeName(
                 bbp::TransactionLoadOutcome::kBackpressured) ==
             "backpressured");
  BOOST_TEST(bbp::TransactionLoadOutcomeName(
                 bbp::TransactionLoadOutcome::kFailed) == "failed");
}

BOOST_AUTO_TEST_CASE(transaction_load_queue_is_bounded_and_fifo) {
  BOOST_CHECK_THROW(bbp::BoundedWalletTransactionQueue(0U), std::runtime_error);
  bbp::BoundedWalletTransactionQueue queue(2U);

  BOOST_TEST(queue.capacity() == 2U);
  BOOST_TEST(queue.size() == 0U);
  BOOST_TEST(!queue.closed());
  BOOST_TEST(queue.TryPush(Task(1U)));
  BOOST_TEST(queue.TryPush(Task(2U)));
  BOOST_TEST(!queue.TryPush(Task(3U)));
  BOOST_TEST(queue.size() == 2U);

  const std::optional<bbp::WalletTransactionLoadTask> first = queue.Pop();
  const std::optional<bbp::WalletTransactionLoadTask> second = queue.Pop();
  BOOST_REQUIRE(first);
  BOOST_REQUIRE(second);
  BOOST_TEST(first->transaction_index == 1U);
  BOOST_TEST(second->transaction_index == 2U);
  BOOST_TEST(queue.size() == 0U);

  queue.Close();
  BOOST_TEST(queue.closed());
  BOOST_TEST(!queue.Pop());
  BOOST_CHECK_THROW(queue.TryPush(Task(4U)), std::runtime_error);
}

BOOST_AUTO_TEST_CASE(transaction_load_queue_close_drains_pending_work) {
  bbp::BoundedWalletTransactionQueue queue(1U);
  BOOST_REQUIRE(queue.TryPush(Task(7U)));
  queue.Close();

  const std::optional<bbp::WalletTransactionLoadTask> task = queue.Pop();
  BOOST_REQUIRE(task);
  BOOST_TEST(task->transaction_index == 7U);
  BOOST_TEST(!queue.Pop());
}

BOOST_AUTO_TEST_CASE(transaction_load_queue_stop_wakes_waiting_consumer) {
  bbp::BoundedWalletTransactionQueue queue(1U);
  std::stop_source stop;
  std::future<std::optional<bbp::WalletTransactionLoadTask>> pending =
      std::async(std::launch::async, [&queue, token = stop.get_token()] {
        return queue.Pop(token);
      });

  BOOST_CHECK(pending.wait_for(20ms) == std::future_status::timeout);
  stop.request_stop();
  BOOST_CHECK(pending.wait_for(1s) == std::future_status::ready);
  BOOST_TEST(!pending.get());
  BOOST_TEST(!queue.closed());
}

BOOST_AUTO_TEST_CASE(transaction_load_accounting_preserves_partition_totals) {
  bbp::TransactionLoadAccounting accounting;
  accounting.RecordOutcome(bbp::TransactionLoadOutcome::kSubmitted, 100us);
  accounting.RecordOutcome(bbp::TransactionLoadOutcome::kSubmitted, 200us);
  accounting.RecordOutcome(bbp::TransactionLoadOutcome::kRejected, 300us);
  accounting.RecordOutcome(bbp::TransactionLoadOutcome::kTimedOut, 400us);
  accounting.RecordOutcome(bbp::TransactionLoadOutcome::kBackpressured, 500us);
  accounting.RecordOutcome(bbp::TransactionLoadOutcome::kFailed, 600us);
  accounting.RecordPropagated(false);
  accounting.RecordPropagated(true);
  accounting.RecordObservationError();

  const bbp::TransactionLoadSnapshot snapshot = accounting.Snapshot(2s);
  BOOST_TEST(snapshot.attempted == 6U);
  BOOST_TEST(snapshot.submitted == 2U);
  BOOST_TEST(snapshot.rejected == 1U);
  BOOST_TEST(snapshot.timed_out == 1U);
  BOOST_TEST(snapshot.backpressured == 1U);
  BOOST_TEST(snapshot.failed == 1U);
  BOOST_TEST(snapshot.propagated == 2U);
  BOOST_TEST(snapshot.confirmed == 1U);
  BOOST_TEST(snapshot.observation_errors == 1U);
  BOOST_TEST(snapshot.latency_sample_count == 6U);
  BOOST_TEST(snapshot.latency_total_us == 2100U);
  BOOST_TEST(snapshot.latency_min_us == 100U);
  BOOST_TEST(snapshot.latency_max_us == 600U);
  BOOST_TEST(snapshot.average_latency_ms == 0.35,
             boost::test_tools::tolerance(0.000001));
  BOOST_TEST(snapshot.elapsed_us == 2'000'000U);
  BOOST_TEST(snapshot.attempted_per_second == 3.0);
  BOOST_TEST(snapshot.submitted_per_second == 1.0);
  BOOST_TEST(snapshot.propagated_per_second == 1.0);
  BOOST_TEST(snapshot.confirmed_per_second == 0.5);
  BOOST_TEST(snapshot.InvariantsHold());
}

BOOST_AUTO_TEST_CASE(transaction_load_accounting_rejects_invalid_updates) {
  bbp::TransactionLoadAccounting accounting;
  BOOST_CHECK_THROW(
      accounting.RecordOutcome(bbp::TransactionLoadOutcome::kFailed, -1us),
      std::runtime_error);
  BOOST_CHECK_THROW(accounting.RecordPropagated(false), std::runtime_error);
  BOOST_CHECK_THROW(static_cast<void>(accounting.Snapshot(-1us)),
                    std::runtime_error);

  accounting.RecordOutcome(bbp::TransactionLoadOutcome::kSubmitted, 0us);
  accounting.RecordPropagated(true);
  BOOST_CHECK_THROW(accounting.RecordPropagated(false), std::runtime_error);
  const bbp::TransactionLoadSnapshot snapshot = accounting.Snapshot(0us);
  BOOST_TEST(snapshot.attempted_per_second == 0.0);
  BOOST_TEST(snapshot.submitted_per_second == 0.0);
  BOOST_TEST(snapshot.propagated_per_second == 0.0);
  BOOST_TEST(snapshot.confirmed_per_second == 0.0);
  BOOST_TEST(snapshot.InvariantsHold());
}
