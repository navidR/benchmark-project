#include <boost/test/unit_test.hpp>

#include "bbp/drivers/chain_wallet_transaction.h"

BOOST_AUTO_TEST_CASE(chain_wallet_transaction_direction_round_trips_names) {
  constexpr bbp::ChainWalletTransactionDirection kDirections[] = {
      bbp::ChainWalletTransactionDirection::kIncoming,
      bbp::ChainWalletTransactionDirection::kOutgoing,
      bbp::ChainWalletTransactionDirection::kInternal,
  };

  for (bbp::ChainWalletTransactionDirection direction : kDirections) {
    const std::optional<bbp::ChainWalletTransactionDirection> parsed =
        bbp::ChainWalletTransactionDirectionFromName(
            bbp::ChainWalletTransactionDirectionName(direction));
    BOOST_REQUIRE(parsed);
    BOOST_CHECK(*parsed == direction);
  }
  BOOST_TEST(!bbp::ChainWalletTransactionDirectionFromName("unknown"));
}
