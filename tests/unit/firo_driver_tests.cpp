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
    const std::vector<std::string>& responses) {
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

    http::response<http::string_body> response{http::status::ok, 11};
    response.set(http::field::content_type, "application/json");
    response.body() = body;
    response.prepare_payload();
    http::write(socket, response);
  }
  return methods;
}

}  // namespace

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
  for (const std::string& argument : process.argv) {
    BOOST_TEST(!argument.starts_with("-connect="));
  }

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
