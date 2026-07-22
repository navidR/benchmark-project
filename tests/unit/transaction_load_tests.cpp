#include <boost/test/unit_test.hpp>
#include <chrono>
#include <future>
#include <memory>
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

bbp::WalletTransactionsWorkload LoadWorkload(
    bbp::WalletTransferStrategy strategy,
    std::vector<std::uint32_t> receivers) {
  bbp::WalletTransactionsWorkload workload;
  workload.strategy = strategy;
  workload.transaction_count = static_cast<std::uint32_t>(receivers.size());
  workload.amount = bbp::AmountDistribution{
      .kind = bbp::ValueDistributionKind::kFixed,
      .minimum_satoshis = 100U,
      .maximum_satoshis = 2'000U,
  };
  workload.fee_satoshis = 10U;
  workload.fee_reserve_satoshis = 10U;
  workload.random_seed = 17U;
  workload.sender_wallets = {1U};
  workload.receiver_wallets = std::move(receivers);
  return workload;
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

BOOST_AUTO_TEST_CASE(transaction_load_queue_admits_batches_atomically) {
  bbp::BoundedWalletTransactionQueue queue(3U);
  BOOST_TEST(queue.TryPushBatch({Task(1U), Task(2U)}));
  BOOST_TEST(queue.size() == 2U);
  BOOST_TEST(queue.maximum_size() == 2U);

  BOOST_TEST(!queue.TryPushBatch({Task(3U), Task(4U)}));
  BOOST_TEST(queue.size() == 2U);
  const auto first = queue.Pop();
  const auto second = queue.Pop();
  BOOST_REQUIRE(first);
  BOOST_REQUIRE(second);
  BOOST_TEST(first->transaction_index == 1U);
  BOOST_TEST(second->transaction_index == 2U);

  BOOST_TEST(!queue.TryPushBatch({Task(5U), Task(6U), Task(7U), Task(8U)}));
  BOOST_TEST(queue.size() == 0U);
  BOOST_TEST(queue.maximum_size() == 2U);
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

BOOST_AUTO_TEST_CASE(
    transaction_load_failed_reservation_reconciles_and_retries) {
  bbp::WalletTransactionsWorkload workload =
      LoadWorkload(bbp::WalletTransferStrategy::kRandomBruteforce, {2U});
  workload.amount.maximum_satoshis = 100U;
  bbp::WalletTransactionLoadPlanner planner(2U, workload);
  bbp::TransactionLoadBalanceReservations reservations({550U, 0U}, 10U, 2U);

  const auto admitted = reservations.PlanAndReserve(
      &planner, 1U, 2U,
      [](const std::vector<bbp::WalletTransactionPlanEntry>&) { return true; });
  BOOST_REQUIRE(admitted.has_plan());
  BOOST_TEST(admitted.admitted);
  BOOST_REQUIRE_EQUAL(admitted.plans.size(), 1U);
  BOOST_TEST(admitted.plans.front().amount_satoshis == 100U);
  BOOST_TEST(reservations.available_balances().front() == 440U);
  BOOST_TEST(reservations.outstanding_size() == 1U);

  const auto exhausted = reservations.PlanAndReserve(
      &planner, 2U, 1U,
      [](const std::vector<bbp::WalletTransactionPlanEntry>&) { return true; });
  BOOST_TEST(!exhausted.has_plan());
  std::future<bool> waiting =
      std::async(std::launch::async,
                 [&reservations, revision = exhausted.balance_revision] {
                   return reservations.WaitForResolution(revision);
                 });
  BOOST_CHECK(waiting.wait_for(20ms) == std::future_status::timeout);

  reservations.Settle(1U, 550U, false);
  BOOST_CHECK(waiting.wait_for(1s) == std::future_status::ready);
  BOOST_TEST(waiting.get());
  BOOST_TEST(reservations.available_balances().front() == 550U);
  BOOST_TEST(reservations.outstanding_size() == 0U);

  const auto retry = reservations.PlanAndReserve(
      &planner, 2U, 1U,
      [](const std::vector<bbp::WalletTransactionPlanEntry>&) { return true; });
  BOOST_REQUIRE(retry.has_plan());
  BOOST_TEST(retry.admitted);
  BOOST_TEST(retry.plans.front().amount_satoshis == 100U);
  reservations.Settle(2U, std::nullopt, true);
  BOOST_TEST(reservations.available_balances().front() == 550U);
  BOOST_TEST(reservations.outstanding_size() == 0U);
  BOOST_TEST(reservations.maximum_size() == 1U);
  BOOST_CHECK_THROW(reservations.Settle(2U, 550U, false), std::runtime_error);
}

BOOST_AUTO_TEST_CASE(
    transaction_load_partial_equal_fanout_uses_actual_sender_balance) {
  bbp::WalletTransactionsWorkload workload =
      LoadWorkload(bbp::WalletTransferStrategy::kEqualFanout, {2U, 3U, 4U});
  workload.fee_reserve_satoshis = 100U;
  bbp::WalletTransactionLoadPlanner planner(4U, workload);
  bbp::TransactionLoadBalanceReservations reservations({3'500U, 0U, 0U, 0U},
                                                       100U, 6U);

  const auto first = reservations.PlanAndReserve(
      &planner, 1U, 6U,
      [](const std::vector<bbp::WalletTransactionPlanEntry>&) { return true; });
  BOOST_REQUIRE(first.has_plan());
  BOOST_TEST(first.admitted);
  BOOST_REQUIRE_EQUAL(first.plans.size(), 3U);
  for (const bbp::WalletTransactionPlanEntry& plan : first.plans) {
    BOOST_TEST(plan.amount_satoshis == 1'066U);
  }
  BOOST_TEST(reservations.available_balances().front() == 2U);
  BOOST_TEST(reservations.outstanding_size() == 3U);

  const auto exhausted = reservations.PlanAndReserve(
      &planner, 4U, 3U,
      [](const std::vector<bbp::WalletTransactionPlanEntry>&) { return true; });
  BOOST_TEST(!exhausted.has_plan());
  std::future<bool> waiting =
      std::async(std::launch::async,
                 [&reservations, revision = exhausted.balance_revision] {
                   return reservations.WaitForResolution(revision);
                 });
  BOOST_CHECK(waiting.wait_for(20ms) == std::future_status::timeout);

  reservations.Settle(1U, std::nullopt, false);
  BOOST_CHECK(waiting.wait_for(20ms) == std::future_status::timeout);
  reservations.Settle(2U, 2'334U, false);
  BOOST_TEST(reservations.available_balances().front() == 1'168U);
  BOOST_TEST(reservations.outstanding_size() == 1U);
  BOOST_CHECK(waiting.wait_for(20ms) == std::future_status::timeout);
  reservations.Settle(3U, std::nullopt, false);
  BOOST_CHECK(waiting.wait_for(1s) == std::future_status::ready);
  BOOST_TEST(waiting.get());

  const auto second = reservations.PlanAndReserve(
      &planner, 4U, 3U,
      [](const std::vector<bbp::WalletTransactionPlanEntry>&) { return true; });
  BOOST_REQUIRE(second.has_plan());
  BOOST_TEST(second.admitted);
  BOOST_REQUIRE_EQUAL(second.plans.size(), 3U);
  for (const bbp::WalletTransactionPlanEntry& plan : second.plans) {
    BOOST_TEST(plan.amount_satoshis == 289U);
  }
  BOOST_TEST(reservations.available_balances().front() == 1U);
  BOOST_TEST(reservations.maximum_size() == 3U);
  reservations.Settle(4U, std::nullopt, false);
  reservations.Settle(5U, std::nullopt, false);
  reservations.Settle(6U, std::nullopt, false);
  BOOST_TEST(reservations.outstanding_size() == 0U);
}

BOOST_AUTO_TEST_CASE(
    transaction_load_reservation_settlement_is_concurrency_safe) {
  bbp::WalletTransactionsWorkload workload =
      LoadWorkload(bbp::WalletTransferStrategy::kRandomBruteforce, {2U});
  workload.amount.maximum_satoshis = 100U;
  bbp::WalletTransactionLoadPlanner planner(2U, workload);
  bbp::TransactionLoadBalanceReservations reservations({2'000U, 0U}, 10U, 4U);
  for (std::uint64_t index = 1U; index <= 4U; ++index) {
    const auto admitted = reservations.PlanAndReserve(
        &planner, index, 5U - index,
        [](const std::vector<bbp::WalletTransactionPlanEntry>&) {
          return true;
        });
    BOOST_REQUIRE(admitted.admitted);
  }
  BOOST_TEST(reservations.outstanding_size() == 4U);

  std::vector<std::future<void>> settlements;
  for (std::uint64_t index = 1U; index <= 4U; ++index) {
    settlements.push_back(
        std::async(std::launch::async, [&reservations, index] {
          reservations.Settle(index, std::nullopt, true);
        }));
  }
  for (std::future<void>& settlement : settlements) {
    settlement.get();
  }
  BOOST_TEST(reservations.outstanding_size() == 0U);
  BOOST_TEST(reservations.available_balances().front() == 2'000U);
  BOOST_TEST(reservations.maximum_size() == 4U);
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
  BOOST_CHECK_THROW(accounting.RecordConfirmed(), std::runtime_error);
  const bbp::TransactionLoadSnapshot snapshot = accounting.Snapshot(0us);
  BOOST_TEST(snapshot.attempted_per_second == 0.0);
  BOOST_TEST(snapshot.submitted_per_second == 0.0);
  BOOST_TEST(snapshot.propagated_per_second == 0.0);
  BOOST_TEST(snapshot.confirmed_per_second == 0.0);
  BOOST_TEST(snapshot.InvariantsHold());
}

BOOST_AUTO_TEST_CASE(transaction_load_confirmation_can_follow_propagation) {
  bbp::TransactionLoadAccounting accounting;
  accounting.RecordOutcome(bbp::TransactionLoadOutcome::kSubmitted, 1us);
  accounting.RecordPropagated(false);
  accounting.RecordConfirmed();

  const bbp::TransactionLoadSnapshot snapshot = accounting.Snapshot(1s);
  BOOST_TEST(snapshot.propagated == 1U);
  BOOST_TEST(snapshot.confirmed == 1U);
  BOOST_TEST(snapshot.InvariantsHold());
}

BOOST_AUTO_TEST_CASE(
    transaction_load_confirmation_counts_all_nodes_exactly_once) {
  auto accounting = std::make_shared<bbp::TransactionLoadAccounting>();
  accounting->RecordOutcome(bbp::TransactionLoadOutcome::kSubmitted, 1us);
  bbp::TransactionLoadConfirmation confirmation(
      accounting, {{"tx-a", "node-a"}, {"tx-a", "node-b"}});

  confirmation.RecordObservation("tx-a", "node-a", false);
  confirmation.RecordPropagated(false);
  confirmation.RecordObservation("tx-a", "node-a", true);
  BOOST_TEST(accounting->Snapshot(1s).confirmed == 0U);
  confirmation.RecordObservation("tx-a", "node-b", true);
  confirmation.RecordObservation("tx-a", "node-a", true);
  confirmation.RecordObservation("tx-a", "node-b", true);

  const bbp::TransactionLoadSnapshot snapshot = accounting->Snapshot(1s);
  BOOST_TEST(snapshot.propagated == 1U);
  BOOST_TEST(snapshot.confirmed == 1U);
  BOOST_TEST(snapshot.confirmed_per_second == 1.0);
  BOOST_TEST(snapshot.InvariantsHold());
  BOOST_TEST(confirmation.propagation_recorded());
  BOOST_TEST(confirmation.confirmation_recorded());
}

BOOST_AUTO_TEST_CASE(
    transaction_load_confirmation_reconciles_confirmation_before_propagation) {
  auto accounting = std::make_shared<bbp::TransactionLoadAccounting>();
  accounting->RecordOutcome(bbp::TransactionLoadOutcome::kSubmitted, 1us);
  bbp::TransactionLoadConfirmation confirmation(
      accounting, {{"tx-a", "node-a"}, {"tx-b", "node-a"}});

  confirmation.RecordObservation("tx-a", "node-a", true);
  confirmation.RecordObservation("tx-b", "node-a", true);
  confirmation.RecordPropagated(false);

  const bbp::TransactionLoadSnapshot snapshot = accounting->Snapshot(2s);
  BOOST_TEST(snapshot.propagated == 1U);
  BOOST_TEST(snapshot.confirmed == 1U);
  BOOST_TEST(snapshot.confirmed_per_second == 0.5);
  BOOST_TEST(snapshot.InvariantsHold());
  BOOST_CHECK_THROW(confirmation.RecordPropagated(true), std::runtime_error);
  BOOST_CHECK_THROW(confirmation.RecordObservation("other", "node-a", true),
                    std::runtime_error);
}
