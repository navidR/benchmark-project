#include <algorithm>
#include <boost/test/unit_test.hpp>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <random>
#include <stdexcept>
#include <vector>

#include "bbp/simulator/wallet_transaction_plan.h"

namespace {

bbp::WalletTransactionsWorkload FixedWorkload(
    bbp::WalletTransferStrategy strategy, std::uint32_t transaction_count) {
  bbp::WalletTransactionsWorkload workload;
  workload.strategy = strategy;
  workload.transaction_count = transaction_count;
  workload.amount = bbp::AmountDistribution{
      .kind = bbp::ValueDistributionKind::kFixed,
      .minimum_satoshis = 100U,
      .maximum_satoshis = 100U,
  };
  workload.interval = bbp::IntervalDistribution{
      .kind = bbp::ValueDistributionKind::kFixed,
      .minimum = std::chrono::milliseconds(25),
      .maximum = std::chrono::milliseconds(25),
  };
  return workload;
}

void CheckPairs(
    const std::vector<bbp::WalletTransactionPlanEntry>& plan,
    const std::vector<std::pair<std::size_t, std::size_t>>& expected) {
  BOOST_REQUIRE_EQUAL(plan.size(), expected.size());
  for (std::size_t index = 0; index < plan.size(); ++index) {
    BOOST_TEST(plan[index].sender_index == expected[index].first);
    BOOST_TEST(plan[index].receiver_index == expected[index].second);
  }
}

}  // namespace

BOOST_AUTO_TEST_CASE(wallet_transaction_plan_round_robin_is_deterministic) {
  const auto plan = bbp::BuildWalletTransactionPlan(
      3U, FixedWorkload(bbp::WalletTransferStrategy::kRoundRobin, 5U));

  CheckPairs(plan, {{0U, 1U}, {1U, 2U}, {2U, 0U}, {0U, 1U}, {1U, 2U}});
  BOOST_TEST(plan.front().interval_before.count() == 0);
  for (std::size_t index = 1; index < plan.size(); ++index) {
    BOOST_TEST(plan[index].interval_before.count() == 25);
    BOOST_TEST(plan[index].amount_satoshis == 100U);
  }
}

BOOST_AUTO_TEST_CASE(
    wallet_transaction_plan_random_preserves_seeded_pair_order) {
  bbp::WalletTransactionsWorkload workload =
      FixedWorkload(bbp::WalletTransferStrategy::kRandom, 8U);
  workload.random_seed = 42U;
  const auto plan = bbp::BuildWalletTransactionPlan(4U, workload);

  std::vector<std::size_t> expected_order = {0U, 1U, 2U, 3U};
  std::mt19937_64 expected_rng(workload.random_seed);
  std::shuffle(expected_order.begin(), expected_order.end(), expected_rng);
  std::vector<std::pair<std::size_t, std::size_t>> expected;
  for (std::size_t index = 0; index < plan.size(); ++index) {
    const std::size_t sender_position = index % expected_order.size();
    expected.emplace_back(
        expected_order[sender_position],
        expected_order[(sender_position + 1U) % expected_order.size()]);
  }
  CheckPairs(plan, expected);
  BOOST_CHECK(bbp::BuildWalletTransactionPlan(4U, workload) == plan);
}

BOOST_AUTO_TEST_CASE(
    wallet_transaction_plan_fanout_cycles_sender_cartesian_pairs) {
  bbp::WalletTransactionsWorkload workload =
      FixedWorkload(bbp::WalletTransferStrategy::kFanout, 6U);
  workload.sender_wallets = {1U, 3U};

  const auto plan = bbp::BuildWalletTransactionPlan(4U, workload);
  CheckPairs(plan,
             {{0U, 1U}, {0U, 3U}, {2U, 1U}, {2U, 3U}, {0U, 1U}, {0U, 3U}});
}

BOOST_AUTO_TEST_CASE(
    wallet_transaction_plan_hotspot_cycles_receiver_cartesian_pairs) {
  bbp::WalletTransactionsWorkload workload =
      FixedWorkload(bbp::WalletTransferStrategy::kHotspot, 6U);
  workload.receiver_wallets = {2U, 4U};

  const auto plan = bbp::BuildWalletTransactionPlan(4U, workload);
  CheckPairs(plan,
             {{0U, 1U}, {2U, 1U}, {0U, 3U}, {2U, 3U}, {0U, 1U}, {2U, 1U}});
}

BOOST_AUTO_TEST_CASE(wallet_transaction_plan_uniform_distributions_are_seeded) {
  bbp::WalletTransactionsWorkload workload =
      FixedWorkload(bbp::WalletTransferStrategy::kRandom, 64U);
  workload.random_seed = 987654321U;
  workload.amount = bbp::AmountDistribution{
      .kind = bbp::ValueDistributionKind::kUniform,
      .minimum_satoshis = 10U,
      .maximum_satoshis = 20U,
  };
  workload.interval = bbp::IntervalDistribution{
      .kind = bbp::ValueDistributionKind::kUniform,
      .minimum = std::chrono::milliseconds(5),
      .maximum = std::chrono::milliseconds(9),
  };

  const auto first = bbp::BuildWalletTransactionPlan(5U, workload);
  const auto second = bbp::BuildWalletTransactionPlan(5U, workload);
  BOOST_CHECK(first == second);
  BOOST_TEST(first.front().interval_before.count() == 0);
  bool saw_amount_minimum = false;
  bool saw_amount_maximum = false;
  bool saw_interval_minimum = false;
  bool saw_interval_maximum = false;
  for (std::size_t index = 0; index < first.size(); ++index) {
    BOOST_TEST(first[index].amount_satoshis >= 10U);
    BOOST_TEST(first[index].amount_satoshis <= 20U);
    saw_amount_minimum =
        saw_amount_minimum || first[index].amount_satoshis == 10U;
    saw_amount_maximum =
        saw_amount_maximum || first[index].amount_satoshis == 20U;
    if (index != 0U) {
      BOOST_TEST(first[index].interval_before.count() >= 5);
      BOOST_TEST(first[index].interval_before.count() <= 9);
      saw_interval_minimum =
          saw_interval_minimum || first[index].interval_before.count() == 5;
      saw_interval_maximum =
          saw_interval_maximum || first[index].interval_before.count() == 9;
    }
  }
  BOOST_TEST(saw_amount_minimum);
  BOOST_TEST(saw_amount_maximum);
  BOOST_TEST(saw_interval_minimum);
  BOOST_TEST(saw_interval_maximum);
}

BOOST_AUTO_TEST_CASE(
    wallet_transaction_planner_incremental_sequence_matches_plan) {
  bbp::WalletTransactionsWorkload workload =
      FixedWorkload(bbp::WalletTransferStrategy::kRandom, 128U);
  workload.random_seed = 112233U;
  workload.amount = bbp::AmountDistribution{
      .kind = bbp::ValueDistributionKind::kUniform,
      .minimum_satoshis = 10U,
      .maximum_satoshis = 20U,
  };
  workload.interval = bbp::IntervalDistribution{
      .kind = bbp::ValueDistributionKind::kUniform,
      .minimum = std::chrono::milliseconds(5),
      .maximum = std::chrono::milliseconds(9),
  };
  const auto complete = bbp::BuildWalletTransactionPlan(5U, workload);
  bbp::WalletTransactionPlanner planner(5U, workload);
  std::vector<bbp::WalletTransactionPlanEntry> incremental;
  incremental.reserve(complete.size());
  for (std::size_t index = 0; index < complete.size(); ++index) {
    incremental.push_back(planner.Next());
  }
  BOOST_CHECK(incremental == complete);
}

BOOST_AUTO_TEST_CASE(wallet_transaction_rate_is_fixed_resolution_and_paced) {
  const bbp::WalletTransactionRate rate =
      bbp::WalletTransactionRate::FromDouble(2.5);
  BOOST_TEST(rate.value() == 2.5);
  BOOST_TEST(rate.millionths() == 2'500'000U);
  BOOST_TEST(rate.SimulationElapsedBefore(0U).count() == 0);
  BOOST_TEST(rate.SimulationElapsedBefore(1U).count() == 400);
  BOOST_TEST(rate.SimulationElapsedBefore(2U).count() == 800);

  const bbp::WalletTransactionRate fractional =
      bbp::WalletTransactionRate::FromDouble(0.3);
  BOOST_TEST(fractional.SimulationElapsedBefore(1U).count() == 3334);
  BOOST_TEST(fractional.SimulationElapsedBefore(2U).count() == 6667);

  const bbp::WalletTransactionRate maximum =
      bbp::WalletTransactionRate::FromDouble(1000.0);
  BOOST_TEST(maximum.SimulationElapsedBefore(1U).count() == 1);
}

BOOST_AUTO_TEST_CASE(wallet_transaction_rate_rejects_invalid_values) {
  BOOST_CHECK_THROW(bbp::WalletTransactionRate::FromDouble(0.0),
                    std::runtime_error);
  BOOST_CHECK_THROW(bbp::WalletTransactionRate::FromDouble(0.0000001),
                    std::runtime_error);
  BOOST_CHECK_THROW(bbp::WalletTransactionRate::FromDouble(0.1234567),
                    std::runtime_error);
  BOOST_CHECK_THROW(bbp::WalletTransactionRate::FromDouble(1000.000001),
                    std::runtime_error);
  BOOST_CHECK_THROW(bbp::WalletTransactionRate::FromDouble(
                        std::numeric_limits<double>::infinity()),
                    std::runtime_error);
  BOOST_CHECK_THROW(bbp::WalletTransactionRate::FromDouble(
                        std::numeric_limits<double>::quiet_NaN()),
                    std::runtime_error);
}

BOOST_AUTO_TEST_CASE(wallet_transaction_plan_validates_ranges_and_selectors) {
  bbp::WalletTransactionsWorkload workload =
      FixedWorkload(bbp::WalletTransferStrategy::kRoundRobin, 1U);
  BOOST_CHECK_THROW(bbp::BuildWalletTransactionPlan(1U, workload),
                    std::runtime_error);
  workload.transaction_count = 0U;
  BOOST_CHECK_THROW(bbp::BuildWalletTransactionPlan(2U, workload),
                    std::runtime_error);

  workload = FixedWorkload(bbp::WalletTransferStrategy::kRoundRobin, 1U);
  workload.amount.minimum_satoshis = 0U;
  BOOST_CHECK_THROW(bbp::BuildWalletTransactionPlan(2U, workload),
                    std::runtime_error);
  workload.amount.minimum_satoshis = 101U;
  BOOST_CHECK_THROW(bbp::BuildWalletTransactionPlan(2U, workload),
                    std::runtime_error);
  workload.amount.kind = bbp::ValueDistributionKind::kUniform;
  workload.amount.minimum_satoshis = 101U;
  workload.amount.maximum_satoshis = 100U;
  BOOST_CHECK_THROW(bbp::BuildWalletTransactionPlan(2U, workload),
                    std::runtime_error);

  workload = FixedWorkload(bbp::WalletTransferStrategy::kRoundRobin, 1U);
  workload.interval.maximum = std::chrono::milliseconds(26);
  BOOST_CHECK_THROW(bbp::BuildWalletTransactionPlan(2U, workload),
                    std::runtime_error);
  workload.interval.kind = bbp::ValueDistributionKind::kUniform;
  workload.interval.minimum = std::chrono::milliseconds(27);
  BOOST_CHECK_THROW(bbp::BuildWalletTransactionPlan(2U, workload),
                    std::runtime_error);

  workload = FixedWorkload(bbp::WalletTransferStrategy::kRoundRobin, 1U);
  workload.sender_wallets = {1U};
  BOOST_CHECK_THROW(bbp::BuildWalletTransactionPlan(2U, workload),
                    std::runtime_error);
  workload = FixedWorkload(bbp::WalletTransferStrategy::kRandom, 1U);
  workload.receiver_wallets = {1U};
  BOOST_CHECK_THROW(bbp::BuildWalletTransactionPlan(2U, workload),
                    std::runtime_error);
}

BOOST_AUTO_TEST_CASE(
    wallet_transaction_plan_validates_fanout_and_hotspot_sets) {
  bbp::WalletTransactionsWorkload fanout =
      FixedWorkload(bbp::WalletTransferStrategy::kFanout, 1U);
  BOOST_CHECK_THROW(bbp::BuildWalletTransactionPlan(3U, fanout),
                    std::runtime_error);
  fanout.sender_wallets = {1U, 1U};
  BOOST_CHECK_THROW(bbp::BuildWalletTransactionPlan(3U, fanout),
                    std::runtime_error);
  fanout.sender_wallets = {0U};
  BOOST_CHECK_THROW(bbp::BuildWalletTransactionPlan(3U, fanout),
                    std::runtime_error);
  fanout.sender_wallets = {4U};
  BOOST_CHECK_THROW(bbp::BuildWalletTransactionPlan(3U, fanout),
                    std::runtime_error);
  fanout.sender_wallets = {1U, 2U, 3U};
  BOOST_CHECK_THROW(bbp::BuildWalletTransactionPlan(3U, fanout),
                    std::runtime_error);
  fanout.sender_wallets = {1U};
  fanout.receiver_wallets = {2U};
  BOOST_CHECK_THROW(bbp::BuildWalletTransactionPlan(3U, fanout),
                    std::runtime_error);

  bbp::WalletTransactionsWorkload hotspot =
      FixedWorkload(bbp::WalletTransferStrategy::kHotspot, 1U);
  BOOST_CHECK_THROW(bbp::BuildWalletTransactionPlan(3U, hotspot),
                    std::runtime_error);
  hotspot.receiver_wallets = {1U, 2U, 3U};
  BOOST_CHECK_THROW(bbp::BuildWalletTransactionPlan(3U, hotspot),
                    std::runtime_error);
  hotspot.receiver_wallets = {1U};
  hotspot.sender_wallets = {2U};
  BOOST_CHECK_THROW(bbp::BuildWalletTransactionPlan(3U, hotspot),
                    std::runtime_error);
}

BOOST_AUTO_TEST_CASE(
    wallet_transaction_random_bruteforce_is_seeded_balance_and_fee_safe) {
  bbp::WalletTransactionsWorkload workload =
      FixedWorkload(bbp::WalletTransferStrategy::kRandomBruteforce, 20U);
  workload.random_seed = 0x12345678U;
  workload.sender_wallets = {1U, 2U};
  workload.receiver_wallets = {3U, 4U};
  workload.amount = bbp::AmountDistribution{
      .kind = bbp::ValueDistributionKind::kUniform,
      .minimum_satoshis = 100U,
      .maximum_satoshis = 1000U,
  };
  workload.fee_satoshis = 10U;

  bbp::WalletTransactionLoadPlanner first(4U, workload);
  bbp::WalletTransactionLoadPlanner second(4U, workload);
  std::vector<std::uint64_t> first_balances = {10'000U, 1'000U, 0U, 0U};
  std::vector<std::uint64_t> second_balances = first_balances;
  std::size_t generated = 0U;
  while (true) {
    const std::vector<std::uint64_t> before = first_balances;
    const auto first_batch = first.NextBatch(&first_balances);
    const auto second_batch = second.NextBatch(&second_balances);
    BOOST_CHECK(first_batch == second_batch);
    BOOST_CHECK(first_balances == second_balances);
    if (!first_batch) {
      break;
    }
    BOOST_REQUIRE_EQUAL(first_batch->size(), 1U);
    const bbp::WalletTransactionPlanEntry& entry = first_batch->front();
    BOOST_TEST(entry.sender_index < 2U);
    BOOST_TEST(entry.receiver_index >= 2U);
    BOOST_TEST(entry.receiver_index < 4U);
    BOOST_TEST(entry.sender_index != entry.receiver_index);
    BOOST_TEST(entry.amount_satoshis >= 100U);
    BOOST_TEST(entry.amount_satoshis <= 1000U);
    BOOST_TEST(entry.amount_satoshis <= before[entry.sender_index] / 5U);
    BOOST_TEST(first_balances[entry.sender_index] ==
               before[entry.sender_index] - entry.amount_satoshis - 10U);
    ++generated;
    BOOST_REQUIRE(generated < 100U);
  }
  BOOST_TEST(generated > 1U);
  BOOST_TEST(first.batch_size() == 1U);
}

BOOST_AUTO_TEST_CASE(
    wallet_transaction_equal_fanout_is_seeded_complete_and_fee_safe) {
  bbp::WalletTransactionsWorkload workload =
      FixedWorkload(bbp::WalletTransferStrategy::kEqualFanout, 6U);
  workload.random_seed = 77U;
  workload.sender_wallets = {1U, 2U};
  workload.receiver_wallets = {3U, 4U, 5U};
  workload.amount = bbp::AmountDistribution{
      .kind = bbp::ValueDistributionKind::kUniform,
      .minimum_satoshis = 100U,
      .maximum_satoshis = 10'000U,
  };
  workload.fee_satoshis = 10U;

  bbp::WalletTransactionLoadPlanner first(5U, workload);
  bbp::WalletTransactionLoadPlanner second(5U, workload);
  std::vector<std::uint64_t> first_balances = {5'000U, 3'500U, 0U, 0U, 0U};
  std::vector<std::uint64_t> second_balances = first_balances;
  const std::vector<std::uint64_t> before = first_balances;
  const auto first_batch = first.NextBatch(&first_balances);
  const auto second_batch = second.NextBatch(&second_balances);

  BOOST_REQUIRE(first_batch);
  BOOST_CHECK(first_batch == second_batch);
  BOOST_CHECK(first_balances == second_balances);
  BOOST_REQUIRE_EQUAL(first_batch->size(), 3U);
  const std::size_t sender = first_batch->front().sender_index;
  BOOST_TEST(sender < 2U);
  const std::uint64_t expected_share = (before[sender] - 30U) / 3U;
  for (std::size_t index = 0U; index < first_batch->size(); ++index) {
    const bbp::WalletTransactionPlanEntry& entry = first_batch->at(index);
    BOOST_TEST(entry.sender_index == sender);
    BOOST_TEST(entry.receiver_index == index + 2U);
    BOOST_TEST(entry.amount_satoshis == expected_share);
  }
  BOOST_TEST(first_balances[sender] ==
             before[sender] - 3U * (expected_share + 10U));
  BOOST_TEST(first.batch_size() == 3U);
}

BOOST_AUTO_TEST_CASE(wallet_transaction_load_planner_rejects_unsafe_inputs) {
  bbp::WalletTransactionsWorkload workload =
      FixedWorkload(bbp::WalletTransferStrategy::kRandomBruteforce, 1U);
  workload.sender_wallets = {1U};
  workload.receiver_wallets = {1U};
  workload.fee_satoshis = 1U;
  BOOST_CHECK_THROW(bbp::WalletTransactionLoadPlanner(2U, workload),
                    std::runtime_error);

  workload.receiver_wallets = {2U};
  bbp::WalletTransactionLoadPlanner planner(2U, workload);
  std::vector<std::uint64_t> short_balances;
  BOOST_CHECK_THROW(planner.NextBatch(&short_balances), std::runtime_error);
  BOOST_CHECK_THROW(planner.NextBatch(nullptr), std::runtime_error);

  workload.strategy = bbp::WalletTransferStrategy::kEqualFanout;
  workload.receiver_wallets = {2U, 3U};
  workload.fee_satoshis = std::numeric_limits<std::uint64_t>::max();
  bbp::WalletTransactionLoadPlanner overflow(3U, workload);
  std::vector<std::uint64_t> balances = {
      std::numeric_limits<std::uint64_t>::max(), 0U, 0U};
  BOOST_CHECK_THROW(overflow.NextBatch(&balances), std::runtime_error);
}

BOOST_AUTO_TEST_CASE(wallet_transaction_duration_attempt_limit_is_bounded) {
  const bbp::WalletTransactionRate two_per_second =
      bbp::WalletTransactionRate::FromDouble(2.0);
  BOOST_TEST(bbp::WalletTransactionDurationAttemptLimit(
                 std::chrono::milliseconds(1500), two_per_second) == 3U);
  const bbp::WalletTransactionRate minimum_rate =
      bbp::WalletTransactionRate::FromDouble(0.000001);
  BOOST_TEST(bbp::WalletTransactionDurationAttemptLimit(
                 std::chrono::milliseconds(1), minimum_rate) == 1U);
  BOOST_CHECK_THROW(bbp::WalletTransactionDurationAttemptLimit(
                        std::chrono::milliseconds(0), two_per_second),
                    std::runtime_error);
  BOOST_CHECK_THROW(
      bbp::WalletTransactionDurationAttemptLimit(
          std::chrono::milliseconds(
              std::numeric_limits<std::chrono::milliseconds::rep>::max()),
          bbp::WalletTransactionRate::FromDouble(1000.0)),
      std::runtime_error);
}
