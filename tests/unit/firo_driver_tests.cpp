#include <unistd.h>

#include <atomic>
#include <boost/asio/ip/tcp.hpp>
#include <boost/beast/core.hpp>
#include <boost/beast/http.hpp>
#include <boost/json/parse.hpp>
#include <boost/test/unit_test.hpp>
#include <chrono>
#include <condition_variable>
#include <exception>
#include <filesystem>
#include <future>
#include <mutex>
#include <stop_token>
#include <thread>
#include <vector>

#include "bbp/drivers/firo_driver.h"
#include "bbp/simulation_cancelled.h"

namespace {

std::vector<std::string> ServeRpcResponses(
    boost::asio::ip::tcp::acceptor& acceptor,
    const std::vector<std::string>& responses,
    std::vector<boost::json::value>* requests = nullptr) {
  namespace beast = boost::beast;
  namespace http = beast::http;
  std::vector<std::string> methods;
  for (const std::string& body : responses) {
    boost::asio::ip::tcp::socket socket(acceptor.get_executor());
    acceptor.accept(socket);
    beast::flat_buffer buffer;
    http::request<http::string_body> request;
    http::read(socket, buffer, request);
    const boost::json::value request_json = boost::json::parse(request.body());
    methods.emplace_back(request_json.as_object().at("method").as_string());
    if (requests != nullptr) {
      requests->push_back(request_json);
    }

    http::response<http::string_body> response{http::status::ok, 11};
    response.set(http::field::content_type, "application/json");
    response.body() = body;
    response.prepare_payload();
    http::write(socket, response);
  }
  return methods;
}

}  // namespace

BOOST_AUTO_TEST_CASE(firo_creates_private_spark_and_funding_addresses) {
  namespace asio = boost::asio;
  using tcp = asio::ip::tcp;

  asio::io_context server_context;
  tcp::acceptor acceptor(
      server_context,
      tcp::endpoint(asio::ip::make_address_v4("127.0.0.1"), 0U));
  const std::vector<std::string> responses = {
      R"({"result":["sr1-private-address"],"error":null,"id":"bbp"})",
      R"({"result":"transparent-funding-address","error":null,"id":"bbp"})"};
  std::future<std::vector<std::string>> served =
      std::async(std::launch::async,
                 [&] { return ServeRpcResponses(acceptor, responses); });

  bbp::FiroNodeConfig config;
  config.id = "private-address-test";
  config.rpc_host = "127.0.0.1";
  config.rpc_port = acceptor.local_endpoint().port();
  config.rpc_user = "user";
  config.rpc_password = "password";
  const bbp::FiroDriver driver(std::chrono::seconds(1));

  const std::string private_address =
      driver.CreateWalletAddress(config, bbp::WalletMode::kPrivate);
  const std::string funding_address = driver.CreateWalletFundingAddress(
      config, bbp::WalletMode::kPrivate, private_address);
  const std::vector<std::string> methods = served.get();

  BOOST_TEST(private_address == "sr1-private-address");
  BOOST_TEST(funding_address == "transparent-funding-address");
  BOOST_REQUIRE_EQUAL(methods.size(), 2U);
  BOOST_TEST(methods[0] == "getnewsparkaddress");
  BOOST_TEST(methods[1] == "getnewaddress");
}

BOOST_AUTO_TEST_CASE(firo_prepares_private_spark_funding) {
  namespace asio = boost::asio;
  using tcp = asio::ip::tcp;

  asio::io_context server_context;
  tcp::acceptor acceptor(
      server_context,
      tcp::endpoint(asio::ip::make_address_v4("127.0.0.1"), 0U));
  const std::vector<std::string> responses = {
      R"({"result":2.00000000,"error":null,"id":"bbp"})",
      R"({"result":["mint-tx-1","mint-tx-2"],"error":null,"id":"bbp"})"};
  std::vector<boost::json::value> requests;
  std::future<std::vector<std::string>> served = std::async(
      std::launch::async,
      [&] { return ServeRpcResponses(acceptor, responses, &requests); });

  bbp::FiroNodeConfig config;
  config.id = "private-funding-test";
  config.rpc_host = "127.0.0.1";
  config.rpc_port = acceptor.local_endpoint().port();
  config.rpc_user = "user";
  config.rpc_password = "password";
  const bbp::FiroDriver driver(std::chrono::seconds(1));

  const bbp::ChainWalletFundingResult result = driver.PrepareWalletFunding(
      config, bbp::WalletMode::kPrivate, "sr1-destination", 100001000ULL, 101U,
      std::chrono::seconds(1));
  const std::vector<std::string> methods = served.get();

  BOOST_REQUIRE_EQUAL(result.txids.size(), 2U);
  BOOST_TEST(result.txids[0] == "mint-tx-1");
  BOOST_TEST(result.txids[1] == "mint-tx-2");
  BOOST_TEST(result.confirmation_blocks_required == 1U);
  BOOST_TEST(result.minimum_chain_height == 500U);
  BOOST_REQUIRE_EQUAL(methods.size(), 2U);
  BOOST_TEST(methods[0] == "getbalance");
  BOOST_TEST(methods[1] == "mintspark");
  BOOST_REQUIRE_EQUAL(requests.size(), 2U);
  const boost::json::array& balance_params =
      requests[0].as_object().at("params").as_array();
  BOOST_REQUIRE_EQUAL(balance_params.size(), 2U);
  BOOST_TEST(balance_params[0].as_string() == "*");
  BOOST_TEST(balance_params[1].as_int64() == 101);
  const boost::json::array& mint_params =
      requests[1].as_object().at("params").as_array();
  BOOST_REQUIRE_EQUAL(mint_params.size(), 1U);
  const boost::json::object& recipient =
      mint_params[0].as_object().at("sr1-destination").as_object();
  BOOST_TEST(recipient.at("amount").as_string() == "1.00001000");
  BOOST_TEST(recipient.at("memo").as_string().empty());
}

BOOST_AUTO_TEST_CASE(firo_waits_for_private_spark_available_balance) {
  namespace asio = boost::asio;
  using tcp = asio::ip::tcp;

  asio::io_context server_context;
  tcp::acceptor acceptor(
      server_context,
      tcp::endpoint(asio::ip::make_address_v4("127.0.0.1"), 0U));
  const std::vector<std::string> responses = {
      R"({"result":{"availableBalance":125000000,"unconfirmedBalance":50000000,"fullBalance":175000000},"error":null,"id":"bbp"})"};
  std::future<std::vector<std::string>> served =
      std::async(std::launch::async,
                 [&] { return ServeRpcResponses(acceptor, responses); });

  bbp::FiroNodeConfig config;
  config.id = "private-balance-test";
  config.rpc_host = "127.0.0.1";
  config.rpc_port = acceptor.local_endpoint().port();
  config.rpc_user = "user";
  config.rpc_password = "password";
  const bbp::FiroDriver driver(std::chrono::seconds(1));

  BOOST_TEST(driver.WaitForWalletBalance(
                 config, bbp::WalletMode::kPrivate, 100000000ULL, 101U,
                 std::chrono::seconds(1)) == 125000000ULL);
  const std::vector<std::string> methods = served.get();
  BOOST_REQUIRE_EQUAL(methods.size(), 1U);
  BOOST_TEST(methods[0] == "getsparkbalance");
}

BOOST_AUTO_TEST_CASE(firo_process_does_not_persist_simulation_peers) {
  const std::filesystem::path test_dir =
      std::filesystem::temp_directory_path() /
      ("bbp-firo-process-peers-" + std::to_string(getpid()));
  std::filesystem::remove_all(test_dir);

  bbp::FiroNodeConfig config;
  config.id = "peer-argument-test";
  config.binary = "/usr/bin/firod";
  config.data_dir = test_dir / "data";
  config.log_dir = test_dir / "logs";
  config.rpc_port = 18888U;
  config.p2p_port = 18168U;
  config.rpc_user = "user";
  config.rpc_password = "password";
  config.connect_peers = {"127.0.0.1:18169"};

  const bbp::FiroDriver driver(std::chrono::milliseconds(100));
  const bbp::ProcessSpec process = driver.RenderProcess(config);
  bool dandelion_disabled = false;
  for (const std::string& argument : process.argv) {
    BOOST_TEST(!argument.starts_with("-connect="));
    if (argument == "-dandelion=0") {
      dandelion_disabled = true;
    }
  }
  BOOST_TEST(dandelion_disabled);

  std::filesystem::remove_all(test_dir);
}

BOOST_AUTO_TEST_CASE(
    firo_peer_identity_rejects_multiple_candidates_on_the_same_host) {
  bbp::FiroNodeConfig config;
  config.id = "ambiguous-peer-test";

  const bbp::FiroDriver driver(std::chrono::milliseconds(100));
  BOOST_CHECK_THROW(driver.ConnectedPeerAddresses(
                        config, {"127.0.0.1:18168", "127.0.0.1:18169"}),
                    bbp::UnsupportedChainOperation);
}

BOOST_AUTO_TEST_CASE(firo_readiness_wait_honors_stop_token_promptly) {
  bbp::FiroNodeConfig config;
  config.id = "cancel-test";
  config.rpc_host = "127.0.0.1";
  config.rpc_port = 1U;
  config.rpc_user = "user";
  config.rpc_password = "password";

  bbp::FiroDriver driver(std::chrono::milliseconds(100));
  std::stop_source stop_source;
  std::atomic<bool> cancelled = false;
  std::exception_ptr failure;
  std::thread waiter([&] {
    try {
      driver.WaitReady(config, std::chrono::hours(24), stop_source.get_token());
    } catch (const bbp::SimulationCancelled&) {
      cancelled = true;
    } catch (...) {
      failure = std::current_exception();
    }
  });

  std::this_thread::sleep_for(std::chrono::milliseconds(20));
  const auto stop_started = std::chrono::steady_clock::now();
  stop_source.request_stop();
  waiter.join();
  const auto stop_duration =
      std::chrono::duration_cast<std::chrono::milliseconds>(
          std::chrono::steady_clock::now() - stop_started);

  BOOST_TEST(!static_cast<bool>(failure));
  BOOST_TEST(cancelled.load());
  BOOST_TEST(stop_duration.count() < 500);
}

BOOST_AUTO_TEST_CASE(firo_rpc_wait_honors_stop_while_server_is_silent) {
  namespace asio = boost::asio;
  using tcp = asio::ip::tcp;

  asio::io_context server_context;
  tcp::acceptor acceptor(
      server_context,
      tcp::endpoint(asio::ip::make_address_v4("127.0.0.1"), 0U));
  std::promise<void> accepted;
  std::future<void> accepted_future = accepted.get_future();
  std::mutex server_mutex;
  std::condition_variable_any server_wakeup;
  std::jthread server([&](std::stop_token stop_token) {
    tcp::socket socket(server_context);
    boost::system::error_code accept_error;
    acceptor.accept(socket, accept_error);
    if (accept_error) {
      return;
    }
    accepted.set_value();
    std::unique_lock<std::mutex> lock(server_mutex);
    server_wakeup.wait(lock, stop_token, [] { return false; });
  });

  bbp::FiroNodeConfig config;
  config.id = "silent-server-test";
  config.rpc_host = "127.0.0.1";
  config.rpc_port = acceptor.local_endpoint().port();
  config.rpc_user = "user";
  config.rpc_password = "password";

  bbp::FiroDriver driver(std::chrono::seconds(30));
  std::stop_source stop_source;
  std::atomic<bool> cancelled = false;
  std::exception_ptr failure;
  std::thread waiter([&] {
    try {
      driver.WaitReady(config, std::chrono::hours(24), stop_source.get_token());
    } catch (const bbp::SimulationCancelled&) {
      cancelled = true;
    } catch (...) {
      failure = std::current_exception();
    }
  });

  const bool connected = accepted_future.wait_for(std::chrono::seconds(1)) ==
                         std::future_status::ready;
  const auto stop_started = std::chrono::steady_clock::now();
  stop_source.request_stop();
  waiter.join();
  const auto stop_duration =
      std::chrono::duration_cast<std::chrono::milliseconds>(
          std::chrono::steady_clock::now() - stop_started);
  server.request_stop();
  server_wakeup.notify_all();
  if (!connected) {
    boost::system::error_code close_error;
    acceptor.close(close_error);
  }
  server.join();

  BOOST_TEST(connected);
  BOOST_TEST(!static_cast<bool>(failure));
  BOOST_TEST(cancelled.load());
  BOOST_TEST(stop_duration.count() < 500);
}

BOOST_AUTO_TEST_CASE(firo_reads_public_wallet_snapshot_from_wallet_rpc) {
  namespace asio = boost::asio;
  using tcp = asio::ip::tcp;

  asio::io_context server_context;
  tcp::acceptor acceptor(
      server_context,
      tcp::endpoint(asio::ip::make_address_v4("127.0.0.1"), 0U));
  const std::vector<std::string> responses = {
      R"({"result":{"balance":49.00000000,"unconfirmed_balance":1.25000000,"immature_balance":2.00000000,"txcount":4},"error":null,"id":"bbp"})",
      R"({"result":[{"category":"receive","amount":50.00000000,"confirmations":101,"time":10,"txid":"incoming","address":"receiver"},{"category":"send","amount":-1.00000000,"fee":-0.00001000,"confirmations":0,"time":20,"txid":"outgoing","address":"recipient","abandoned":false},{"category":"mint","amount":-0.50000000,"fee":-0.00002000,"confirmations":2,"time":30,"txid":"mint"},{"category":"znode","amount":0.75000000,"confirmations":5,"time":40,"txid":"reward"}],"error":null,"id":"bbp"})"};
  std::future<std::vector<std::string>> served =
      std::async(std::launch::async,
                 [&] { return ServeRpcResponses(acceptor, responses); });

  bbp::FiroNodeConfig config;
  config.id = "wallet-snapshot-test";
  config.rpc_host = "127.0.0.1";
  config.rpc_port = acceptor.local_endpoint().port();
  config.rpc_user = "user";
  config.rpc_password = "password";
  const bbp::FiroDriver driver(std::chrono::seconds(1));
  const bbp::ChainWalletSnapshot snapshot =
      driver.ReadWalletSnapshot(config, bbp::WalletMode::kPublic, 4U);
  const std::vector<std::string> methods = served.get();

  BOOST_TEST(snapshot.available_balance_satoshis == 4900000000ULL);
  BOOST_TEST(snapshot.unconfirmed_balance_satoshis == 125000000ULL);
  BOOST_TEST(snapshot.immature_balance_satoshis == 200000000ULL);
  BOOST_TEST(snapshot.transaction_count == 4U);
  BOOST_TEST(!snapshot.transaction_history_truncated);
  BOOST_REQUIRE_EQUAL(snapshot.transactions.size(), 4U);
  BOOST_CHECK(snapshot.transactions[0].direction ==
              bbp::ChainWalletTransactionDirection::kIncoming);
  BOOST_TEST(snapshot.transactions[0].amount_satoshis == 5000000000LL);
  BOOST_TEST(snapshot.transactions[0].confirmations == 101);
  BOOST_TEST(snapshot.transactions[0].txid == "incoming");
  BOOST_CHECK(snapshot.transactions[1].direction ==
              bbp::ChainWalletTransactionDirection::kOutgoing);
  BOOST_REQUIRE(snapshot.transactions[1].fee_satoshis.has_value());
  BOOST_TEST(*snapshot.transactions[1].fee_satoshis == -1000);
  BOOST_CHECK(snapshot.transactions[2].direction ==
              bbp::ChainWalletTransactionDirection::kOutgoing);
  BOOST_CHECK(snapshot.transactions[3].direction ==
              bbp::ChainWalletTransactionDirection::kIncoming);
  BOOST_REQUIRE_EQUAL(methods.size(), 2U);
  BOOST_TEST(methods[0] == "getwalletinfo");
  BOOST_TEST(methods[1] == "listtransactions");
}

BOOST_AUTO_TEST_CASE(firo_reads_private_spark_wallet_snapshot) {
  namespace asio = boost::asio;
  using tcp = asio::ip::tcp;

  asio::io_context server_context;
  tcp::acceptor acceptor(
      server_context,
      tcp::endpoint(asio::ip::make_address_v4("127.0.0.1"), 0U));
  const std::vector<std::string> responses = {
      R"({"result":{"balance":40.00000000,"unconfirmed_balance":0.00000000,"immature_balance":50.00000000,"txcount":2},"error":null,"id":"bbp"})",
      R"({"result":{"availableBalance":150000000,"unconfirmedBalance":25000000,"fullBalance":175000000},"error":null,"id":"bbp"})",
      R"({"result":[{"category":"mint","amount":-2.00000000,"fee":-0.00001000,"confirmations":1,"time":10,"txid":"mint-tx","address":"sr1-self"},{"category":"spend","amount":-0.50000000,"fee":-0.00002000,"confirmations":0,"time":20,"txid":"spend-tx","address":"sr1-recipient"}],"error":null,"id":"bbp"})"};
  std::future<std::vector<std::string>> served =
      std::async(std::launch::async,
                 [&] { return ServeRpcResponses(acceptor, responses); });

  bbp::FiroNodeConfig config;
  config.id = "private-wallet-snapshot-test";
  config.rpc_host = "127.0.0.1";
  config.rpc_port = acceptor.local_endpoint().port();
  config.rpc_user = "user";
  config.rpc_password = "password";
  const bbp::FiroDriver driver(std::chrono::seconds(1));
  const bbp::ChainWalletSnapshot snapshot =
      driver.ReadWalletSnapshot(config, bbp::WalletMode::kPrivate, 4U);
  const std::vector<std::string> methods = served.get();

  BOOST_TEST(snapshot.available_balance_satoshis == 150000000ULL);
  BOOST_TEST(snapshot.unconfirmed_balance_satoshis == 25000000ULL);
  BOOST_TEST(snapshot.immature_balance_satoshis == 0U);
  BOOST_TEST(snapshot.transaction_count == 2U);
  BOOST_REQUIRE_EQUAL(snapshot.transactions.size(), 2U);
  BOOST_CHECK(snapshot.transactions[0].direction ==
              bbp::ChainWalletTransactionDirection::kOutgoing);
  BOOST_CHECK(snapshot.transactions[1].direction ==
              bbp::ChainWalletTransactionDirection::kOutgoing);
  BOOST_REQUIRE_EQUAL(methods.size(), 3U);
  BOOST_TEST(methods[0] == "getwalletinfo");
  BOOST_TEST(methods[1] == "getsparkbalance");
  BOOST_TEST(methods[2] == "listtransactions");
}

BOOST_AUTO_TEST_CASE(firo_submits_private_spark_transfer) {
  namespace asio = boost::asio;
  using tcp = asio::ip::tcp;

  asio::io_context server_context;
  tcp::acceptor acceptor(
      server_context,
      tcp::endpoint(asio::ip::make_address_v4("127.0.0.1"), 0U));
  const std::vector<std::string> responses = {
      R"({"result":true,"error":null,"id":"bbp"})",
      R"({"result":"spark-spend-tx","error":null,"id":"bbp"})",
      R"({"result":["spark-spend-tx"],"error":null,"id":"bbp"})"};
  std::vector<boost::json::value> requests;
  std::future<std::vector<std::string>> served = std::async(
      std::launch::async,
      [&] { return ServeRpcResponses(acceptor, responses, &requests); });

  bbp::FiroNodeConfig config;
  config.id = "private-transfer-test";
  config.rpc_host = "127.0.0.1";
  config.rpc_port = acceptor.local_endpoint().port();
  config.rpc_user = "user";
  config.rpc_password = "password";
  const bbp::FiroDriver driver(std::chrono::seconds(1));

  const bbp::FiroWalletTransactionResult result = driver.SendWalletTransaction(
      config, bbp::WalletMode::kPrivate, "sr1-recipient", 50000000ULL, 1000ULL,
      std::chrono::seconds(1));
  const std::vector<std::string> methods = served.get();

  BOOST_REQUIRE_EQUAL(result.txids.size(), 1U);
  BOOST_TEST(result.txids[0] == "spark-spend-tx");
  BOOST_TEST(result.destination_amount == "0.50000000");
  BOOST_TEST(result.requested_fee_rate == "0.00001000");
  BOOST_TEST(result.mempool_size == 1U);
  BOOST_REQUIRE_EQUAL(methods.size(), 3U);
  BOOST_TEST(methods[0] == "settxfee");
  BOOST_TEST(methods[1] == "spendspark");
  BOOST_TEST(methods[2] == "getrawmempool");
  BOOST_REQUIRE_EQUAL(requests.size(), 3U);
  const boost::json::array& spend_params =
      requests[1].as_object().at("params").as_array();
  BOOST_REQUIRE_EQUAL(spend_params.size(), 1U);
  const boost::json::object& recipient =
      spend_params[0].as_object().at("sr1-recipient").as_object();
  BOOST_TEST(recipient.at("amount").as_string() == "0.50000000");
  BOOST_TEST(recipient.at("memo").as_string().empty());
  BOOST_TEST(!recipient.at("subtractFee").as_bool());
}

BOOST_AUTO_TEST_CASE(firo_rejects_empty_private_spark_txid) {
  namespace asio = boost::asio;
  using tcp = asio::ip::tcp;

  asio::io_context server_context;
  tcp::acceptor acceptor(
      server_context,
      tcp::endpoint(asio::ip::make_address_v4("127.0.0.1"), 0U));
  const std::vector<std::string> responses = {
      R"({"result":true,"error":null,"id":"bbp"})",
      R"({"result":"","error":null,"id":"bbp"})"};
  std::future<std::vector<std::string>> served =
      std::async(std::launch::async,
                 [&] { return ServeRpcResponses(acceptor, responses); });

  bbp::FiroNodeConfig config;
  config.id = "empty-private-transfer-test";
  config.rpc_host = "127.0.0.1";
  config.rpc_port = acceptor.local_endpoint().port();
  config.rpc_user = "user";
  config.rpc_password = "password";
  const bbp::FiroDriver driver(std::chrono::seconds(1));

  BOOST_CHECK_THROW(driver.SendWalletTransaction(
                        config, bbp::WalletMode::kPrivate, "sr1-recipient",
                        50000000ULL, 1000ULL, std::chrono::seconds(1)),
                    std::runtime_error);
  const std::vector<std::string> methods = served.get();
  BOOST_REQUIRE_EQUAL(methods.size(), 2U);
  BOOST_TEST(methods[0] == "settxfee");
  BOOST_TEST(methods[1] == "spendspark");
}

BOOST_AUTO_TEST_CASE(firo_counts_non_reward_transactions_in_generated_block) {
  namespace asio = boost::asio;
  using tcp = asio::ip::tcp;

  asio::io_context server_context;
  tcp::acceptor acceptor(
      server_context,
      tcp::endpoint(asio::ip::make_address_v4("127.0.0.1"), 0U));
  const std::vector<std::string> responses = {
      R"({"result":{"hash":"block-hash","tx":["reward","tx-1","tx-2"]},"error":null,"id":"bbp"})"};
  std::future<std::vector<std::string>> served =
      std::async(std::launch::async,
                 [&] { return ServeRpcResponses(acceptor, responses); });

  bbp::FiroNodeConfig config;
  config.id = "block-transaction-count-test";
  config.rpc_host = "127.0.0.1";
  config.rpc_port = acceptor.local_endpoint().port();
  config.rpc_user = "user";
  config.rpc_password = "password";
  const bbp::FiroDriver driver(std::chrono::seconds(1));

  BOOST_TEST(driver.ReadBlockNonRewardTransactionCount(config, "block-hash") ==
             2U);
  const std::vector<std::string> methods = served.get();
  BOOST_REQUIRE_EQUAL(methods.size(), 1U);
  BOOST_TEST(methods[0] == "getblock");
}
