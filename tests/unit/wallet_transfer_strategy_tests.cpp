#include <array>
#include <boost/test/unit_test.hpp>
#include <optional>

#include "bbp/simulator/wallet_transfer_strategy.h"

BOOST_AUTO_TEST_CASE(wallet_transfer_strategy_names_round_trip) {
  constexpr std::array<bbp::WalletTransferStrategy, 6> kStrategies = {
      bbp::WalletTransferStrategy::kRoundRobin,
      bbp::WalletTransferStrategy::kRandom,
      bbp::WalletTransferStrategy::kFanout,
      bbp::WalletTransferStrategy::kHotspot,
      bbp::WalletTransferStrategy::kRandomBruteforce,
      bbp::WalletTransferStrategy::kEqualFanout,
  };

  for (const bbp::WalletTransferStrategy strategy : kStrategies) {
    const std::optional<bbp::WalletTransferStrategy> parsed =
        bbp::WalletTransferStrategyFromName(
            bbp::WalletTransferStrategyName(strategy));
    BOOST_REQUIRE(parsed);
    BOOST_CHECK(*parsed == strategy);
  }
  BOOST_TEST(bbp::IsTransactionLoadStrategy(
      bbp::WalletTransferStrategy::kRandomBruteforce));
  BOOST_TEST(bbp::IsTransactionLoadStrategy(
      bbp::WalletTransferStrategy::kEqualFanout));
  BOOST_TEST(!bbp::IsTransactionLoadStrategy(
      bbp::WalletTransferStrategy::kRoundRobin));
  BOOST_TEST(!bbp::WalletTransferStrategyFromName("unknown_strategy"));
}
