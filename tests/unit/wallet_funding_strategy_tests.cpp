#include <boost/test/unit_test.hpp>
#include <cstdint>
#include <stdexcept>
#include <vector>

#include "bbp/simulator/wallet_funding_strategy.h"

BOOST_AUTO_TEST_CASE(wallet_funding_strategy_names_round_trip) {
  constexpr bbp::WalletFundingStrategy kStrategies[] = {
      bbp::WalletFundingStrategy::kRoundRobin,
      bbp::WalletFundingStrategy::kRandom,
  };

  for (const bbp::WalletFundingStrategy strategy : kStrategies) {
    const std::optional<bbp::WalletFundingStrategy> parsed =
        bbp::WalletFundingStrategyFromName(
            bbp::WalletFundingStrategyName(strategy));
    BOOST_REQUIRE(parsed);
    BOOST_CHECK(*parsed == strategy);
  }
  BOOST_TEST(!bbp::WalletFundingStrategyFromName("unknown"));
}

BOOST_AUTO_TEST_CASE(wallet_funding_round_robin_rotates_miners) {
  const std::vector<std::uint32_t> miners = {2U, 5U};
  const std::vector<std::uint32_t> plan = bbp::WalletFundingMinerNodes(
      miners, 5U, bbp::WalletFundingStrategy::kRoundRobin, 0U);

  const std::vector<std::uint32_t> expected = {2U, 5U, 2U, 5U, 2U};
  BOOST_TEST(plan == expected, boost::test_tools::per_element());
}

BOOST_AUTO_TEST_CASE(wallet_funding_random_is_seeded_and_uses_known_miners) {
  const std::vector<std::uint32_t> miners = {1U, 4U, 7U};
  const std::vector<std::uint32_t> first = bbp::WalletFundingMinerNodes(
      miners, 32U, bbp::WalletFundingStrategy::kRandom, 42U);
  const std::vector<std::uint32_t> second = bbp::WalletFundingMinerNodes(
      miners, 32U, bbp::WalletFundingStrategy::kRandom, 42U);

  BOOST_TEST(first == second, boost::test_tools::per_element());
  for (const std::uint32_t miner : first) {
    const bool known_miner = miner == 1U || miner == 4U || miner == 7U;
    BOOST_TEST(known_miner);
  }
}

BOOST_AUTO_TEST_CASE(wallet_funding_rejects_missing_miners) {
  BOOST_CHECK_THROW(bbp::WalletFundingMinerNodes(
                        {}, 1U, bbp::WalletFundingStrategy::kRoundRobin, 0U),
                    std::runtime_error);
  BOOST_TEST(bbp::WalletFundingMinerNodes(
                 {}, 0U, bbp::WalletFundingStrategy::kRoundRobin, 0U)
                 .empty());
}
