#include <boost/json/parse.hpp>
#include <boost/test/unit_test.hpp>
#include <cstdint>
#include <limits>

#include "bbp/wallet_send_resolver.h"

namespace {

boost::json::object Report() {
  return boost::json::
      parse(R"({"nodes_summary":[{"node_id":"firo-wallet-a","node_index":1},{"node_id":"firo-miner-a","node_index":2},{"node_id":"firo-wallet-b","node_index":3}],"wallets_summary":[{"wallet_index":1,"node":1},{"wallet_index":2,"node":3}]})")
          .as_object();
}

bbp::SimulationWalletSend Request() {
  return {
      .sender_wallet_index = 0U,
      .receiver_wallet_index = 2U,
      .amount_satoshis = 10000000U,
      .fee_satoshis = 1000U,
      .timeout_sec = 45U,
  };
}

}  // namespace

BOOST_AUTO_TEST_CASE(wallet_send_resolver_retains_selected_wallet_identity) {
  const bbp::ResolvedWalletSend resolved =
      bbp::ResolveSelectedWalletSend(Report(), 0U, Request());

  BOOST_TEST(resolved.sender_node_id == "firo-wallet-a");
  BOOST_TEST(resolved.target_text == "wallet-1 -> wallet-2");
  BOOST_TEST(resolved.send.sender_wallet_index == 1U);
  BOOST_TEST(resolved.send.receiver_wallet_index == 2U);
  BOOST_TEST(resolved.send.amount_satoshis == 10000000U);
  BOOST_TEST(resolved.send.fee_satoshis == 1000U);
  BOOST_TEST(resolved.send.timeout_sec == 45U);
}

BOOST_AUTO_TEST_CASE(wallet_send_resolver_rejects_invalid_wallet_targets) {
  bbp::SimulationWalletSend request = Request();
  request.receiver_wallet_index = 1U;
  BOOST_CHECK_THROW(bbp::ResolveSelectedWalletSend(Report(), 0U, request),
                    std::runtime_error);

  request = Request();
  request.receiver_wallet_index = 3U;
  BOOST_CHECK_THROW(bbp::ResolveSelectedWalletSend(Report(), 0U, request),
                    std::runtime_error);
  BOOST_CHECK_THROW(bbp::ResolveSelectedWalletSend(Report(), 2U, Request()),
                    std::runtime_error);

  boost::json::object malformed = Report();
  malformed["wallets_summary"] = boost::json::parse(
      R"([{"wallet_index":1,"node":1},{"wallet_index":2,"node":3},{"wallet_index":2,"node":1}])");
  BOOST_CHECK_THROW(bbp::ResolveSelectedWalletSend(malformed, 0U, Request()),
                    std::runtime_error);

  malformed = Report();
  malformed["wallets_summary"] = boost::json::parse(
      R"([{"wallet_index":1,"node":1},{"wallet_index":2,"node":3},{"wallet_index":1,"node":3}])");
  BOOST_CHECK_THROW(bbp::ResolveSelectedWalletSend(malformed, 0U, Request()),
                    std::runtime_error);

  malformed = Report();
  malformed["nodes_summary"] = boost::json::parse(
      R"([{"node_id":"a","node_index":1},{"node_id":"b","node_index":1},{"node_id":"c","node_index":3}])");
  BOOST_CHECK_THROW(bbp::ResolveSelectedWalletSend(malformed, 0U, Request()),
                    std::runtime_error);
}

BOOST_AUTO_TEST_CASE(wallet_send_resolver_rejects_invalid_payloads) {
  bbp::SimulationWalletSend request = Request();
  request.sender_wallet_index = 1U;
  BOOST_CHECK_THROW(bbp::ResolveSelectedWalletSend(Report(), 0U, request),
                    std::runtime_error);

  request = Request();
  request.amount_satoshis = 0U;
  BOOST_CHECK_THROW(bbp::ResolveSelectedWalletSend(Report(), 0U, request),
                    std::runtime_error);

  request = Request();
  request.amount_satoshis = std::numeric_limits<std::uint64_t>::max();
  request.fee_satoshis = 1U;
  BOOST_CHECK_THROW(bbp::ResolveSelectedWalletSend(Report(), 0U, request),
                    std::runtime_error);

  request = Request();
  request.timeout_sec = 0U;
  BOOST_CHECK_THROW(bbp::ResolveSelectedWalletSend(Report(), 0U, request),
                    std::runtime_error);
}
