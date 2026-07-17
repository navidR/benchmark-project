#include <unistd.h>

#include <algorithm>
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
#include <fstream>
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
    std::vector<boost::json::value>* requests = nullptr,
    const std::vector<unsigned>* response_statuses = nullptr) {
  namespace beast = boost::beast;
  namespace http = beast::http;
  std::vector<std::string> methods;
  std::size_t response_index = 0;
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

    const http::status status =
        response_statuses == nullptr
            ? http::status::ok
            : static_cast<http::status>(response_statuses->at(response_index));
    http::response<http::string_body> response{status, 11};
    response.set(http::field::content_type, "application/json");
    response.body() = body;
    response.prepare_payload();
    http::write(socket, response);
    ++response_index;
  }
  return methods;
}

}  // namespace

BOOST_AUTO_TEST_CASE(firo_reads_normalized_chain_metrics) {
  namespace asio = boost::asio;
  using tcp = asio::ip::tcp;

  asio::io_context server_context;
  tcp::acceptor acceptor(
      server_context,
      tcp::endpoint(asio::ip::make_address_v4("127.0.0.1"), 0U));
  const std::vector<std::string> responses = {
      R"({"result":{"blocks":12,"headers":12,"bestblockhash":"best-12","difficulty":2.5,"mediantime":1700000000,"verificationprogress":1.0,"chainwork":"000abc"},"error":null,"id":"bbp"})",
      R"({"result":{"version":14100500,"protocolversion":70015,"subversion":"/Firo:0.14.1/","connections":2},"error":null,"id":"bbp"})",
      R"({"result":{"size":3,"bytes":450},"error":null,"id":"bbp"})",
      R"({"result":{"hash":"best-12","time":1700000100},"error":null,"id":"bbp"})",
      R"({"result":12345.5,"error":null,"id":"bbp"})",
      R"({"result":[{"addr":"10.20.0.2:18168"},{"addr":"10.20.0.3:18168"}],"error":null,"id":"bbp"})"};
  std::vector<boost::json::value> requests;
  std::future<std::vector<std::string>> served = std::async(
      std::launch::async,
      [&] { return ServeRpcResponses(acceptor, responses, &requests); });

  bbp::FiroNodeConfig config;
  config.id = "metrics-test";
  config.rpc_host = "127.0.0.1";
  config.rpc_port = acceptor.local_endpoint().port();
  config.rpc_user = "user";
  config.rpc_password = "password";
  const bbp::FiroDriver driver(std::chrono::seconds(1));

  const bbp::FiroMetrics metrics = driver.ReadMetrics(config);
  const std::vector<std::string> methods = served.get();

  BOOST_TEST(metrics.version == 14100500U);
  BOOST_TEST(metrics.protocol_version == 70015U);
  BOOST_TEST(metrics.subversion == "/Firo:0.14.1/");
  BOOST_TEST(metrics.height == 12U);
  BOOST_REQUIRE(metrics.headers.has_value());
  BOOST_TEST(*metrics.headers == 12U);
  BOOST_TEST(metrics.best_hash == "best-12");
  BOOST_TEST(metrics.peer_count == 2U);
  BOOST_REQUIRE_EQUAL(metrics.peer_addresses.size(), 2U);
  BOOST_TEST(metrics.mempool_tx_count == 3U);
  BOOST_TEST(metrics.mempool_bytes == 450U);
  BOOST_CHECK(metrics.sync_status == bbp::ChainSyncStatus::kSynced);
  BOOST_REQUIRE(metrics.verification_progress.has_value());
  BOOST_TEST(*metrics.verification_progress == 1.0);
  BOOST_REQUIRE(metrics.difficulty.has_value());
  BOOST_TEST(*metrics.difficulty == 2.5);
  BOOST_REQUIRE(metrics.hashrate_estimate.has_value());
  BOOST_TEST(*metrics.hashrate_estimate == 12345.5);
  BOOST_REQUIRE(metrics.last_block_time.has_value());
  BOOST_TEST(*metrics.last_block_time == 1700000100U);
  BOOST_REQUIRE(metrics.median_time.has_value());
  BOOST_TEST(*metrics.median_time == 1700000000U);
  BOOST_REQUIRE(metrics.chainwork.has_value());
  BOOST_TEST(*metrics.chainwork == "000abc");
  BOOST_TEST(!metrics.reorg_count.has_value());
  BOOST_REQUIRE_EQUAL(methods.size(), 6U);
  BOOST_TEST(methods[0] == "getblockchaininfo");
  BOOST_TEST(methods[1] == "getnetworkinfo");
  BOOST_TEST(methods[2] == "getmempoolinfo");
  BOOST_TEST(methods[3] == "getblockheader");
  BOOST_TEST(methods[4] == "getnetworkhashps");
  BOOST_TEST(methods[5] == "getpeerinfo");
  BOOST_REQUIRE_EQUAL(requests[3].as_object().at("params").as_array().size(),
                      2U);
  BOOST_TEST(requests[3].as_object().at("params").as_array()[0].as_string() ==
             "best-12");
  BOOST_TEST(requests[3].as_object().at("params").as_array()[1].as_bool());
  BOOST_TEST(requests[4].as_object().at("params").as_array().empty());
}

BOOST_AUTO_TEST_CASE(firo_rejects_out_of_range_verification_progress) {
  namespace asio = boost::asio;
  using tcp = asio::ip::tcp;

  asio::io_context server_context;
  tcp::acceptor acceptor(
      server_context,
      tcp::endpoint(asio::ip::make_address_v4("127.0.0.1"), 0U));
  const std::vector<std::string> responses = {
      R"({"result":{"blocks":1,"headers":1,"bestblockhash":"best-1","difficulty":1,"mediantime":10,"verificationprogress":1.1,"chainwork":"01"},"error":null,"id":"bbp"})",
      R"({"result":{"version":1,"protocolversion":2,"subversion":"/Firo:test/","connections":0},"error":null,"id":"bbp"})",
      R"({"result":{"size":0,"bytes":0},"error":null,"id":"bbp"})",
      R"({"result":{"time":11},"error":null,"id":"bbp"})",
      R"({"result":0,"error":null,"id":"bbp"})",
      R"({"result":[],"error":null,"id":"bbp"})"};
  std::future<std::vector<std::string>> served =
      std::async(std::launch::async,
                 [&] { return ServeRpcResponses(acceptor, responses); });

  bbp::FiroNodeConfig config;
  config.id = "invalid-progress-test";
  config.rpc_host = "127.0.0.1";
  config.rpc_port = acceptor.local_endpoint().port();
  config.rpc_user = "user";
  config.rpc_password = "password";
  const bbp::FiroDriver driver(std::chrono::seconds(1));

  BOOST_CHECK_THROW(driver.ReadMetrics(config), std::runtime_error);
  BOOST_REQUIRE_EQUAL(served.get().size(), 6U);
}

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

BOOST_AUTO_TEST_CASE(
    firo_creates_public_wallet_address_with_exact_rpc_payload) {
  namespace asio = boost::asio;
  using tcp = asio::ip::tcp;

  asio::io_context server_context;
  tcp::acceptor acceptor(
      server_context,
      tcp::endpoint(asio::ip::make_address_v4("127.0.0.1"), 0U));
  const std::vector<std::string> responses = {
      R"({"result":"public-wallet-address","error":null,"id":"bbp"})"};
  std::vector<boost::json::value> requests;
  std::future<std::vector<std::string>> served = std::async(
      std::launch::async,
      [&] { return ServeRpcResponses(acceptor, responses, &requests); });

  bbp::FiroNodeConfig config;
  config.id = "public-address-test";
  config.rpc_host = "127.0.0.1";
  config.rpc_port = acceptor.local_endpoint().port();
  config.rpc_user = "user";
  config.rpc_password = "password";
  const bbp::FiroDriver driver(std::chrono::seconds(1));

  const std::string address =
      driver.CreateWalletAddress(config, bbp::WalletMode::kPublic);
  const std::string funding_address = driver.CreateWalletFundingAddress(
      config, bbp::WalletMode::kPublic, address);
  const std::vector<std::string> methods = served.get();

  BOOST_TEST(address == "public-wallet-address");
  BOOST_TEST(funding_address == address);
  BOOST_REQUIRE_EQUAL(methods.size(), 1U);
  BOOST_TEST(methods[0] == "getnewaddress");
  BOOST_REQUIRE_EQUAL(requests.size(), 1U);
  BOOST_TEST(requests[0].as_object().at("params").as_array().empty());
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
  config.rpc_authentication = bbp::RpcAuthenticationMode::kCookieFile;
  config.rpc_cookie_file = config.log_dir / ".bbp-rpc-cookie";
  config.wallet_enabled = true;
  config.connect_peers = {"127.0.0.1:18169"};
  config.extra_args = bbp::ChainExtraArgs({"-dbcache=64", "-maxmempool=128"});

  const bbp::FiroDriver driver(std::chrono::milliseconds(100));
  const bbp::ProcessSpec process = driver.RenderProcess(config);
  bool dandelion_disabled = false;
  bool transaction_index_enabled = false;
  bool regtest_selected = false;
  bool wallet_disabled = false;
  bool cookie_authentication = false;
  bool inline_credentials = false;
  bool exact_extra_arguments = false;
  for (const std::string& argument : process.argv) {
    BOOST_TEST(!argument.starts_with("-connect="));
    if (argument == "-dandelion=0") {
      dandelion_disabled = true;
    }
    if (argument == "-txindex=1") {
      transaction_index_enabled = true;
    }
    if (argument == "-regtest") {
      regtest_selected = true;
    }
    if (argument == "-disablewallet") {
      wallet_disabled = true;
    }
    if (argument == "-rpccookiefile=" + config.rpc_cookie_file.string()) {
      cookie_authentication = true;
    }
    if (argument.starts_with("-rpcuser=") ||
        argument.starts_with("-rpcpassword=")) {
      inline_credentials = true;
    }
  }
  exact_extra_arguments =
      process.argv.size() >= 2U &&
      process.argv[process.argv.size() - 2U] == "-dbcache=64" &&
      process.argv.back() == "-maxmempool=128";
  BOOST_TEST(dandelion_disabled);
  BOOST_TEST(transaction_index_enabled);
  BOOST_TEST(regtest_selected);
  BOOST_TEST(!wallet_disabled);
  BOOST_TEST(cookie_authentication);
  BOOST_TEST(!inline_credentials);
  BOOST_TEST(exact_extra_arguments);

  config.wallet_enabled = false;
  const bbp::ProcessSpec wallet_disabled_process = driver.RenderProcess(config);
  BOOST_TEST(std::count(wallet_disabled_process.argv.begin(),
                        wallet_disabled_process.argv.end(),
                        "-disablewallet") == 1U);

  config.network = static_cast<bbp::ChainNetwork>(999);
  BOOST_CHECK_THROW(driver.RenderProcess(config), std::runtime_error);

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

BOOST_AUTO_TEST_CASE(firo_waits_for_peer_verack_before_reporting_ready) {
  namespace asio = boost::asio;
  using tcp = asio::ip::tcp;

  asio::io_context server_context;
  tcp::acceptor acceptor(
      server_context,
      tcp::endpoint(asio::ip::make_address_v4("127.0.0.1"), 0U));
  const std::vector<std::string> responses = {
      R"({"result":[{"addr":"10.20.0.2:18168","version":70028,"bytesrecv_per_msg":{"version":126}}],"error":null,"id":"bbp"})",
      R"({"result":[{"addr":"10.20.0.2:18168","version":70028,"bytesrecv_per_msg":{"version":126,"verack":24}}],"error":null,"id":"bbp"})"};
  std::future<std::vector<std::string>> served =
      std::async(std::launch::async,
                 [&] { return ServeRpcResponses(acceptor, responses); });

  bbp::FiroNodeConfig config;
  config.id = "peer-handshake-test";
  config.rpc_host = "127.0.0.1";
  config.rpc_port = acceptor.local_endpoint().port();
  config.rpc_user = "user";
  config.rpc_password = "password";
  const bbp::FiroDriver driver(std::chrono::seconds(1));

  driver.WaitForPeerAddress(config, "10.20.0.2:18168", std::chrono::seconds(1));
  const std::vector<std::string> methods = served.get();
  BOOST_REQUIRE_EQUAL(methods.size(), 2U);
  BOOST_TEST(methods[0] == "getpeerinfo");
  BOOST_TEST(methods[1] == "getpeerinfo");
}

BOOST_AUTO_TEST_CASE(firo_peer_count_wait_requires_completed_handshakes) {
  namespace asio = boost::asio;
  using tcp = asio::ip::tcp;

  asio::io_context server_context;
  tcp::acceptor acceptor(
      server_context,
      tcp::endpoint(asio::ip::make_address_v4("127.0.0.1"), 0U));
  const std::vector<std::string> responses = {
      R"({"result":[{"addr":"10.20.0.2:18168","version":70028,"bytesrecv_per_msg":{"version":126}}],"error":null,"id":"bbp"})",
      R"({"result":[{"addr":"10.20.0.2:18168","version":70028,"bytesrecv_per_msg":{"version":126,"verack":24}}],"error":null,"id":"bbp"})"};
  std::future<std::vector<std::string>> served =
      std::async(std::launch::async,
                 [&] { return ServeRpcResponses(acceptor, responses); });

  bbp::FiroNodeConfig config;
  config.id = "peer-count-handshake-test";
  config.rpc_host = "127.0.0.1";
  config.rpc_port = acceptor.local_endpoint().port();
  config.rpc_user = "user";
  config.rpc_password = "password";
  const bbp::FiroDriver driver(std::chrono::seconds(1));

  driver.WaitForPeerCount(config, 1U, std::chrono::seconds(1));
  BOOST_REQUIRE_EQUAL(served.get().size(), 2U);
}

BOOST_AUTO_TEST_CASE(firo_connectivity_uses_only_handshake_complete_peers) {
  namespace asio = boost::asio;
  using tcp = asio::ip::tcp;

  asio::io_context server_context;
  tcp::acceptor acceptor(
      server_context,
      tcp::endpoint(asio::ip::make_address_v4("127.0.0.1"), 0U));
  const std::vector<std::string> responses = {
      R"({"result":[{"addr":"10.20.0.2:18168","version":70028,"bytesrecv_per_msg":{"version":126}}],"error":null,"id":"bbp"})",
      R"({"result":[{"addr":"10.20.0.2:18168","version":70028,"bytesrecv_per_msg":{"version":126,"verack":24}}],"error":null,"id":"bbp"})"};
  std::future<std::vector<std::string>> served =
      std::async(std::launch::async,
                 [&] { return ServeRpcResponses(acceptor, responses); });

  bbp::FiroNodeConfig config;
  config.id = "peer-connectivity-test";
  config.rpc_host = "127.0.0.1";
  config.rpc_port = acceptor.local_endpoint().port();
  config.rpc_user = "user";
  config.rpc_password = "password";
  const bbp::FiroDriver driver(std::chrono::seconds(1));
  const std::vector<std::string> candidates = {"10.20.0.2:18168"};

  BOOST_TEST(driver.ConnectedPeerAddresses(config, candidates).empty());
  const std::vector<std::string> connected =
      driver.ConnectedPeerAddresses(config, candidates);
  BOOST_REQUIRE_EQUAL(connected.size(), 1U);
  BOOST_TEST(connected[0] == candidates[0]);
  BOOST_REQUIRE_EQUAL(served.get().size(), 2U);
}

BOOST_AUTO_TEST_CASE(firo_absence_wait_observes_pre_verack_socket) {
  namespace asio = boost::asio;
  using tcp = asio::ip::tcp;

  asio::io_context server_context;
  tcp::acceptor acceptor(
      server_context,
      tcp::endpoint(asio::ip::make_address_v4("127.0.0.1"), 0U));
  const std::vector<std::string> responses = {
      R"({"result":[{"addr":"10.20.0.2:18168","version":70028,"bytesrecv_per_msg":{"version":126}}],"error":null,"id":"bbp"})",
      R"({"result":[],"error":null,"id":"bbp"})"};
  std::future<std::vector<std::string>> served =
      std::async(std::launch::async,
                 [&] { return ServeRpcResponses(acceptor, responses); });

  bbp::FiroNodeConfig config;
  config.id = "peer-absence-test";
  config.rpc_host = "127.0.0.1";
  config.rpc_port = acceptor.local_endpoint().port();
  config.rpc_user = "user";
  config.rpc_password = "password";
  const bbp::FiroDriver driver(std::chrono::seconds(1));

  driver.WaitForPeerAddressAbsent(config, "10.20.0.2:18168",
                                  std::chrono::seconds(1));
  BOOST_REQUIRE_EQUAL(served.get().size(), 2U);
}

BOOST_AUTO_TEST_CASE(firo_rpc_cookie_cleanup_is_safe_and_idempotent) {
  const std::filesystem::path test_dir =
      std::filesystem::temp_directory_path() /
      ("bbp-firo-cookie-cleanup-" + std::to_string(getpid()));
  std::filesystem::remove_all(test_dir);
  std::filesystem::create_directories(test_dir / "logs");

  bbp::FiroNodeConfig config;
  config.id = "cookie-cleanup-test";
  config.log_dir = test_dir / "logs";
  config.rpc_authentication = bbp::RpcAuthenticationMode::kCookieFile;
  config.rpc_cookie_file = config.log_dir / ".bbp-rpc-cookie";
  const bbp::FiroDriver driver(std::chrono::milliseconds(100));

  const std::filesystem::path target = test_dir / "must-remain";
  {
    std::ofstream output(target);
    output << "not-a-cookie";
  }
  std::filesystem::create_symlink(target, config.rpc_cookie_file);
  driver.CleanupRpcCredentials(config);
  BOOST_TEST(std::filesystem::exists(target));
  BOOST_TEST(!std::filesystem::exists(config.rpc_cookie_file));

  {
    std::ofstream output(config.rpc_cookie_file);
    output << "user:password";
  }
  driver.CleanupRpcCredentials(config);
  driver.CleanupRpcCredentials(config);
  BOOST_TEST(!std::filesystem::exists(config.rpc_cookie_file));

  config.rpc_cookie_file = test_dir / "outside-cookie";
  BOOST_CHECK_THROW(driver.CleanupRpcCredentials(config), std::runtime_error);
  config.rpc_cookie_file = config.log_dir / ".bbp-rpc-cookie";
  config.rpc_user = "inline";
  BOOST_CHECK_THROW(driver.CleanupRpcCredentials(config), std::runtime_error);

  std::filesystem::remove_all(test_dir);
}

BOOST_AUTO_TEST_CASE(firo_rpc_authentication_failures_do_not_expose_cookie) {
  namespace asio = boost::asio;
  using tcp = asio::ip::tcp;

  asio::io_context server_context;
  tcp::acceptor acceptor(
      server_context,
      tcp::endpoint(asio::ip::make_address_v4("127.0.0.1"), 0U));
  const std::vector<std::string> responses = {
      R"({"result":null,"error":{"code":-32600,"message":"authorization failed"},"id":"bbp"})"};
  const std::vector<unsigned> statuses = {401U};
  std::future<std::vector<std::string>> served =
      std::async(std::launch::async, [&] {
        return ServeRpcResponses(acceptor, responses, nullptr, &statuses);
      });

  const std::filesystem::path test_dir =
      std::filesystem::temp_directory_path() /
      ("bbp-firo-cookie-auth-failure-" + std::to_string(getpid()));
  std::filesystem::remove_all(test_dir);
  std::filesystem::create_directories(test_dir);
  const std::filesystem::path cookie = test_dir / ".cookie";
  const std::string secret = "cookie-auth-failure-secret";
  {
    std::ofstream output(cookie);
    output << "__cookie__:" << secret;
  }
  std::filesystem::permissions(
      cookie,
      std::filesystem::perms::owner_read | std::filesystem::perms::owner_write,
      std::filesystem::perm_options::replace);

  bbp::FiroNodeConfig config;
  config.id = "cookie-auth-failure-test";
  config.rpc_host = "127.0.0.1";
  config.rpc_port = acceptor.local_endpoint().port();
  config.rpc_authentication = bbp::RpcAuthenticationMode::kCookieFile;
  config.rpc_cookie_file = cookie;
  const bbp::FiroDriver driver(std::chrono::seconds(1));
  try {
    driver.SetNetworkActive(config, true);
    BOOST_FAIL("Firo RPC authentication failure was accepted");
  } catch (const std::exception& error) {
    BOOST_TEST(std::string(error.what()).find(secret) == std::string::npos);
  }
  BOOST_REQUIRE_EQUAL(served.get().size(), 1U);
  std::filesystem::remove_all(test_dir);
}

BOOST_AUTO_TEST_CASE(firo_observes_mempool_transaction_with_exact_rpc_payload) {
  namespace asio = boost::asio;
  using tcp = asio::ip::tcp;

  asio::io_context server_context;
  tcp::acceptor acceptor(
      server_context,
      tcp::endpoint(asio::ip::make_address_v4("127.0.0.1"), 0U));
  const std::vector<std::string> responses = {
      R"({"result":25,"error":null,"id":"bbp"})",
      R"({"result":["observed-tx","other-tx"],"error":null,"id":"bbp"})",
      R"({"result":{"txid":"observed-tx","hex":"00"},"error":null,"id":"bbp"})"};
  std::vector<boost::json::value> requests;
  std::future<std::vector<std::string>> served = std::async(
      std::launch::async,
      [&] { return ServeRpcResponses(acceptor, responses, &requests); });

  bbp::FiroNodeConfig config;
  config.id = "mempool-observation-test";
  config.rpc_host = "127.0.0.1";
  config.rpc_port = acceptor.local_endpoint().port();
  config.rpc_user = "user";
  config.rpc_password = "password";
  const bbp::FiroDriver driver(std::chrono::seconds(1));

  const bbp::ChainTransactionObservation observation =
      driver.ObserveTransaction(config, "observed-tx");
  const std::vector<std::string> methods = served.get();

  BOOST_CHECK(observation.state == bbp::ChainTransactionState::kMempool);
  BOOST_TEST(observation.observed_height == 25U);
  BOOST_TEST(observation.mempool_size == 2U);
  BOOST_TEST(observation.block_hash.empty());
  BOOST_TEST(!observation.confirmation_height.has_value());
  BOOST_TEST(observation.confirmations == 0U);
  BOOST_REQUIRE_EQUAL(methods.size(), 3U);
  BOOST_TEST(methods[0] == "getblockcount");
  BOOST_TEST(methods[1] == "getrawmempool");
  BOOST_TEST(methods[2] == "getrawtransaction");
  const boost::json::array& params =
      requests[2].as_object().at("params").as_array();
  BOOST_REQUIRE_EQUAL(params.size(), 2U);
  BOOST_TEST(params[0].as_string() == "observed-tx");
  BOOST_TEST(params[1].as_bool());
}

BOOST_AUTO_TEST_CASE(firo_observes_confirmed_transaction) {
  namespace asio = boost::asio;
  using tcp = asio::ip::tcp;

  asio::io_context server_context;
  tcp::acceptor acceptor(
      server_context,
      tcp::endpoint(asio::ip::make_address_v4("127.0.0.1"), 0U));
  const std::vector<std::string> responses = {
      R"({"result":30,"error":null,"id":"bbp"})",
      R"({"result":[],"error":null,"id":"bbp"})",
      R"({"result":{"txid":"confirmed-tx","blockhash":"block-29","height":29,"confirmations":2},"error":null,"id":"bbp"})"};
  std::future<std::vector<std::string>> served =
      std::async(std::launch::async,
                 [&] { return ServeRpcResponses(acceptor, responses); });

  bbp::FiroNodeConfig config;
  config.id = "confirmed-observation-test";
  config.rpc_host = "127.0.0.1";
  config.rpc_port = acceptor.local_endpoint().port();
  config.rpc_user = "user";
  config.rpc_password = "password";
  const bbp::FiroDriver driver(std::chrono::seconds(1));

  const bbp::ChainTransactionObservation observation =
      driver.ObserveTransaction(config, "confirmed-tx");
  static_cast<void>(served.get());

  BOOST_CHECK(observation.state == bbp::ChainTransactionState::kConfirmed);
  BOOST_TEST(observation.observed_height == 30U);
  BOOST_TEST(observation.mempool_size == 0U);
  BOOST_TEST(observation.block_hash == "block-29");
  BOOST_REQUIRE(observation.confirmation_height.has_value());
  BOOST_TEST(*observation.confirmation_height == 29U);
  BOOST_TEST(observation.confirmations == 2U);
}

BOOST_AUTO_TEST_CASE(firo_waits_through_temporary_transaction_not_found) {
  namespace asio = boost::asio;
  using tcp = asio::ip::tcp;

  asio::io_context server_context;
  tcp::acceptor acceptor(
      server_context,
      tcp::endpoint(asio::ip::make_address_v4("127.0.0.1"), 0U));
  const std::vector<std::string> responses = {
      R"({"result":10,"error":null,"id":"bbp"})",
      R"({"result":[],"error":null,"id":"bbp"})",
      R"({"result":null,"error":{"code":-5,"message":"No such mempool or blockchain transaction"},"id":"bbp"})",
      R"({"result":10,"error":null,"id":"bbp"})",
      R"({"result":["late-tx"],"error":null,"id":"bbp"})",
      R"({"result":{"txid":"late-tx"},"error":null,"id":"bbp"})"};
  const std::vector<unsigned> response_statuses = {200U, 200U, 500U,
                                                   200U, 200U, 200U};
  std::future<std::vector<std::string>> served =
      std::async(std::launch::async, [&] {
        return ServeRpcResponses(acceptor, responses, nullptr,
                                 &response_statuses);
      });

  bbp::FiroNodeConfig config;
  config.id = "late-observation-test";
  config.rpc_host = "127.0.0.1";
  config.rpc_port = acceptor.local_endpoint().port();
  config.rpc_user = "user";
  config.rpc_password = "password";
  const bbp::FiroDriver driver(std::chrono::seconds(1));

  const bbp::ChainTransactionObservation observation =
      driver.WaitForTransaction(config, "late-tx", std::chrono::seconds(1));
  const std::vector<std::string> methods = served.get();

  BOOST_CHECK(observation.state == bbp::ChainTransactionState::kMempool);
  BOOST_TEST(observation.observed_height == 10U);
  BOOST_TEST(observation.mempool_size == 1U);
  BOOST_REQUIRE_EQUAL(methods.size(), 6U);
}

BOOST_AUTO_TEST_CASE(firo_mempool_wait_accepts_transaction_mined_during_poll) {
  namespace asio = boost::asio;
  using tcp = asio::ip::tcp;

  asio::io_context server_context;
  tcp::acceptor acceptor(
      server_context,
      tcp::endpoint(asio::ip::make_address_v4("127.0.0.1"), 0U));
  const std::vector<std::string> responses = {
      R"({"result":30,"error":null,"id":"bbp"})",
      R"({"result":["racing-tx"],"error":null,"id":"bbp"})",
      R"({"result":{"txid":"racing-tx","blockhash":"block-31","height":31,"confirmations":1},"error":null,"id":"bbp"})"};
  std::future<std::vector<std::string>> served =
      std::async(std::launch::async,
                 [&] { return ServeRpcResponses(acceptor, responses); });

  bbp::FiroNodeConfig config;
  config.id = "racing-observation-test";
  config.rpc_host = "127.0.0.1";
  config.rpc_port = acceptor.local_endpoint().port();
  config.rpc_user = "user";
  config.rpc_password = "password";
  const bbp::FiroDriver driver(std::chrono::seconds(1));

  BOOST_TEST(driver.WaitForMempoolTransaction(config, "racing-tx",
                                              std::chrono::seconds(1)) == 1U);
  static_cast<void>(served.get());
}

BOOST_AUTO_TEST_CASE(firo_rejects_invalid_transaction_confirmation_numbers) {
  const auto rejects = [](const std::string& transaction_response) {
    namespace asio = boost::asio;
    using tcp = asio::ip::tcp;

    asio::io_context server_context;
    tcp::acceptor acceptor(
        server_context,
        tcp::endpoint(asio::ip::make_address_v4("127.0.0.1"), 0U));
    const std::vector<std::string> responses = {
        R"({"result":30,"error":null,"id":"bbp"})",
        R"({"result":[],"error":null,"id":"bbp"})", transaction_response};
    std::future<std::vector<std::string>> served =
        std::async(std::launch::async,
                   [&] { return ServeRpcResponses(acceptor, responses); });

    bbp::FiroNodeConfig config;
    config.id = "invalid-confirmation-test";
    config.rpc_host = "127.0.0.1";
    config.rpc_port = acceptor.local_endpoint().port();
    config.rpc_user = "user";
    config.rpc_password = "password";
    const bbp::FiroDriver driver(std::chrono::seconds(1));
    bool rejected = false;
    try {
      static_cast<void>(driver.ObserveTransaction(config, "invalid-tx"));
    } catch (const std::exception&) {
      rejected = true;
    }
    static_cast<void>(served.get());
    return rejected;
  };

  BOOST_TEST(rejects(
      R"({"result":{"txid":"invalid-tx","blockhash":"block","height":-1,"confirmations":1},"error":null,"id":"bbp"})"));
  BOOST_TEST(rejects(
      R"({"result":{"txid":"invalid-tx","blockhash":"block","height":1,"confirmations":-1},"error":null,"id":"bbp"})"));
  BOOST_TEST(rejects(
      R"({"result":{"txid":"invalid-tx","blockhash":"block","height":18446744073709551615,"confirmations":2},"error":null,"id":"bbp"})"));
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

BOOST_AUTO_TEST_CASE(firo_submits_public_transfer_with_exact_rpc_payload) {
  namespace asio = boost::asio;
  using tcp = asio::ip::tcp;

  asio::io_context server_context;
  tcp::acceptor acceptor(
      server_context,
      tcp::endpoint(asio::ip::make_address_v4("127.0.0.1"), 0U));
  const std::vector<std::string> responses = {
      R"({"result":true,"error":null,"id":"bbp"})",
      R"({"result":"public-transfer-tx","error":null,"id":"bbp"})",
      R"({"result":202,"error":null,"id":"bbp"})",
      R"({"result":["public-transfer-tx"],"error":null,"id":"bbp"})",
      R"({"result":{"txid":"public-transfer-tx"},"error":null,"id":"bbp"})"};
  std::vector<boost::json::value> requests;
  std::future<std::vector<std::string>> served = std::async(
      std::launch::async,
      [&] { return ServeRpcResponses(acceptor, responses, &requests); });

  bbp::FiroNodeConfig config;
  config.id = "public-transfer-test";
  config.rpc_host = "127.0.0.1";
  config.rpc_port = acceptor.local_endpoint().port();
  config.rpc_user = "user";
  config.rpc_password = "password";
  const bbp::FiroDriver driver(std::chrono::seconds(1));

  const bbp::FiroWalletTransactionResult result = driver.SendWalletTransaction(
      config, bbp::WalletMode::kPublic, "public-destination", 50000000ULL,
      1000ULL, std::chrono::seconds(1));
  const std::vector<std::string> methods = served.get();

  BOOST_REQUIRE_EQUAL(result.txids.size(), 1U);
  BOOST_TEST(result.txids[0] == "public-transfer-tx");
  BOOST_TEST(result.destination_amount == "0.50000000");
  BOOST_TEST(result.requested_fee_rate == "0.00001000");
  BOOST_TEST(result.mempool_size == 1U);
  BOOST_REQUIRE_EQUAL(methods.size(), 5U);
  BOOST_TEST(methods[0] == "settxfee");
  BOOST_TEST(methods[1] == "sendtoaddress");
  BOOST_TEST(methods[2] == "getblockcount");
  BOOST_TEST(methods[3] == "getrawmempool");
  BOOST_TEST(methods[4] == "getrawtransaction");
  BOOST_REQUIRE_EQUAL(requests.size(), 5U);
  const boost::json::array& fee_params =
      requests[0].as_object().at("params").as_array();
  BOOST_REQUIRE_EQUAL(fee_params.size(), 1U);
  BOOST_TEST(fee_params[0].as_string() == "0.00001000");
  const boost::json::array& send_params =
      requests[1].as_object().at("params").as_array();
  BOOST_REQUIRE_EQUAL(send_params.size(), 5U);
  BOOST_TEST(send_params[0].as_string() == "public-destination");
  BOOST_TEST(send_params[1].as_string() == "0.50000000");
  BOOST_TEST(send_params[2].as_string().empty());
  BOOST_TEST(send_params[3].as_string().empty());
  BOOST_TEST(!send_params[4].as_bool());
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
      R"({"result":500,"error":null,"id":"bbp"})",
      R"({"result":["spark-spend-tx"],"error":null,"id":"bbp"})",
      R"({"result":{"txid":"spark-spend-tx"},"error":null,"id":"bbp"})"};
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
  BOOST_REQUIRE_EQUAL(methods.size(), 5U);
  BOOST_TEST(methods[0] == "settxfee");
  BOOST_TEST(methods[1] == "spendspark");
  BOOST_TEST(methods[2] == "getblockcount");
  BOOST_TEST(methods[3] == "getrawmempool");
  BOOST_TEST(methods[4] == "getrawtransaction");
  BOOST_REQUIRE_EQUAL(requests.size(), 5U);
  const boost::json::array& spend_params =
      requests[1].as_object().at("params").as_array();
  BOOST_REQUIRE_EQUAL(spend_params.size(), 1U);
  const boost::json::object& recipient =
      spend_params[0].as_object().at("sr1-recipient").as_object();
  BOOST_TEST(recipient.at("amount").as_string() == "0.50000000");
  BOOST_TEST(recipient.at("memo").as_string().empty());
  BOOST_TEST(!recipient.at("subtractFee").as_bool());
  const boost::json::array& observe_params =
      requests[4].as_object().at("params").as_array();
  BOOST_REQUIRE_EQUAL(observe_params.size(), 2U);
  BOOST_TEST(observe_params[0].as_string() == "spark-spend-tx");
  BOOST_TEST(observe_params[1].as_bool());
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
