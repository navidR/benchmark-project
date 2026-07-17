#include <boost/test/unit_test.hpp>
#include <stdexcept>

#include "bbp/chain_network.h"

BOOST_AUTO_TEST_CASE(chain_network_parses_regtest) {
  BOOST_TEST(static_cast<int>(bbp::ParseChainNetwork("regtest")) ==
             static_cast<int>(bbp::ChainNetwork::kRegtest));
}

BOOST_AUTO_TEST_CASE(chain_network_rejects_unsupported_names) {
  BOOST_CHECK_THROW(bbp::ParseChainNetwork("mainnet"), std::runtime_error);
  BOOST_CHECK_THROW(bbp::ParseChainNetwork("testnet"), std::runtime_error);
  BOOST_CHECK_THROW(bbp::ParseChainNetwork("devnet"), std::runtime_error);
  BOOST_CHECK_THROW(bbp::ParseChainNetwork(""), std::runtime_error);
}

BOOST_AUTO_TEST_CASE(chain_network_has_canonical_name) {
  BOOST_TEST(bbp::ChainNetworkName(bbp::ChainNetwork::kRegtest) == "regtest");
  BOOST_CHECK_THROW(bbp::ChainNetworkName(static_cast<bbp::ChainNetwork>(999)),
                    std::runtime_error);
}
