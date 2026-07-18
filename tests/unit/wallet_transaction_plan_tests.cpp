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
