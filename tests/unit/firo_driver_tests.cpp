#include <unistd.h>

#include <algorithm>
#include <atomic>
#include <boost/asio/ip/tcp.hpp>
#include <boost/beast/core.hpp>
#include <boost/beast/http.hpp>
#include <boost/json/parse.hpp>
#include <boost/json/serialize.hpp>
#include <boost/test/unit_test.hpp>
#include <chrono>
#include <condition_variable>
#include <exception>
#include <filesystem>
#include <fstream>
#include <future>
#include <limits>
#include <memory>
#include <mutex>
#include <stop_token>
#include <thread>
#include <vector>

#include "bbp/drivers/firo_driver.h"
#include "bbp/run_report.h"
#include "bbp/simulation_cancelled.h"
#include "bbp/simulator/transaction_load.h"
#include "bbp/util.h"

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

std::vector<std::string> ServeDelayedRpcResponses(
    boost::asio::ip::tcp::acceptor& acceptor,
    const std::vector<std::string>& responses,
    const std::vector<std::chrono::milliseconds>& response_delays,
    const std::vector<unsigned>* response_statuses = nullptr) {
  namespace beast = boost::beast;
  namespace http = beast::http;
  if (responses.size() != response_delays.size()) {
    throw std::runtime_error("delayed RPC response fixture size mismatch");
  }
  std::vector<std::string> methods;
  for (std::size_t response_index = 0U; response_index < responses.size();
       ++response_index) {
    boost::asio::ip::tcp::socket socket(acceptor.get_executor());
    acceptor.accept(socket);
    beast::flat_buffer buffer;
    http::request<http::string_body> request;
    http::read(socket, buffer, request);
    const boost::json::value request_json = boost::json::parse(request.body());
    methods.emplace_back(request_json.as_object().at("method").as_string());
    std::this_thread::sleep_for(response_delays.at(response_index));

    const http::status status =
        response_statuses == nullptr
            ? http::status::ok
            : static_cast<http::status>(response_statuses->at(response_index));
    http::response<http::string_body> response{status, 11};
    response.set(http::field::content_type, "application/json");
    response.body() = responses.at(response_index);
    response.prepare_payload();
    beast::error_code write_error;
    http::write(socket, response, write_error);
  }
  return methods;
}

std::uint64_t JsonNonNegativeInteger(const boost::json::value& value) {
  if (value.is_uint64()) {
    return value.as_uint64();
  }
  if (value.is_int64() && value.as_int64() >= 0) {
    return static_cast<std::uint64_t>(value.as_int64());
  }
  throw std::runtime_error("expected a non-negative JSON integer");
}

}  // namespace

BOOST_AUTO_TEST_CASE(firo_builds_isolated_manual_gui_command) {
  const std::filesystem::path test_dir =
      std::filesystem::temp_directory_path() /
      ("bbp-firo-gui-command-" + std::to_string(getpid()));
  std::filesystem::remove_all(test_dir);
  const std::filesystem::path bin_dir = test_dir / "Firo bin";
  const std::filesystem::path run_root = test_dir / "run root";
  const std::filesystem::path node_data = run_root / "nodes" / "firo-1";
  std::filesystem::create_directories(bin_dir);
  std::filesystem::create_directories(node_data);
  const std::filesystem::path qt_binary = bin_dir / "firo-qt";
  const std::filesystem::path daemon_binary = bin_dir / "firod";
  bbp::WriteText(qt_binary, "test fixture\n");
  bbp::WriteText(daemon_binary, "test fixture\n");
  std::filesystem::permissions(qt_binary,
                               std::filesystem::perms::owner_read |
                                   std::filesystem::perms::owner_write |
                                   std::filesystem::perms::owner_exec,
                               std::filesystem::perm_options::replace);
  std::filesystem::permissions(daemon_binary,
                               std::filesystem::perms::owner_read |
                                   std::filesystem::perms::owner_write |
                                   std::filesystem::perms::owner_exec,
                               std::filesystem::perm_options::replace);

  bbp::FiroNodeConfig config;
  config.id = "firo-1";
  config.binary = daemon_binary;
  config.data_dir = node_data;
  config.p2p_host = "10.77.0.2";
  config.p2p_port = 19168U;

  const bbp::FiroDriver driver(std::chrono::milliseconds(100));
  const std::optional<bbp::OperatorConnectionCommand> connection =
      driver.BuildOperatorConnectionCommand(config, run_root);
  BOOST_REQUIRE(connection);
  BOOST_TEST(connection->executable == std::filesystem::canonical(qt_binary));
  BOOST_TEST(connection->data_dir ==
             std::filesystem::canonical(run_root / "operator" / "firo-qt"));
  BOOST_TEST(connection->data_dir != std::filesystem::canonical(node_data));
  BOOST_TEST(connection->peer_address == "10.77.0.2");
  BOOST_TEST(connection->peer_port == 19168U);
  const std::vector<std::string> expected_arguments = {
      "-regtest",
      "-datadir=" + connection->data_dir.string(),
      "-connect=10.77.0.2:19168",
      "-dns=0",
      "-dnsseed=0",
      "-forcednsseed=0",
      "-maxconnections=1",
      "-listen=0",
      "-discover=0",
      "-listenonion=0",
      "-torsetup=0",
      "-upnp=0",
  };
  BOOST_TEST(connection->arguments == expected_arguments,
             boost::test_tools::per_element());
  BOOST_CHECK(std::ranges::find(connection->arguments, "-disablewallet") ==
              connection->arguments.end());
  BOOST_TEST(connection->ShellCommand().starts_with(
      "'" + std::filesystem::canonical(qt_binary).string() + "' '-regtest'"));
  const std::filesystem::perms permissions =
      std::filesystem::status(connection->data_dir).permissions();
  BOOST_CHECK((permissions & std::filesystem::perms::owner_all) ==
              std::filesystem::perms::owner_all);
  BOOST_CHECK((permissions & std::filesystem::perms::group_all) ==
              std::filesystem::perms::none);
  BOOST_CHECK((permissions & std::filesystem::perms::others_all) ==
              std::filesystem::perms::none);

  config.p2p_port = 0U;
  BOOST_CHECK_THROW(driver.BuildOperatorConnectionCommand(config, run_root),
                    std::runtime_error);
  config.p2p_port = 19168U;
  config.p2p_host = "0.0.0.0";
  BOOST_CHECK_THROW(driver.BuildOperatorConnectionCommand(config, run_root),
                    std::runtime_error);
  config.p2p_host = "ff02::1";
  BOOST_CHECK_THROW(driver.BuildOperatorConnectionCommand(config, run_root),
                    std::runtime_error);
  config.p2p_host = "not-an-address";
  BOOST_CHECK_THROW(driver.BuildOperatorConnectionCommand(config, run_root),
                    std::runtime_error);
  config.p2p_host = "10.77.0.2";
  BOOST_CHECK_THROW(
      driver.BuildOperatorConnectionCommand(config, test_dir / "missing"),
      std::runtime_error);

  std::filesystem::permissions(qt_binary, std::filesystem::perms::owner_read,
                               std::filesystem::perm_options::replace);
  BOOST_CHECK_THROW(driver.BuildOperatorConnectionCommand(config, run_root),
                    std::runtime_error);
  std::filesystem::remove_all(test_dir);
}

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
      R"({"result":{"txid":"invalid-tx","blockhash":"block","confirmations":1},"error":null,"id":"bbp"})"));
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

BOOST_AUTO_TEST_CASE(firo_rpc_stop_honors_stop_while_server_is_silent) {
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
  config.id = "silent-stop-server-test";
  config.rpc_host = "127.0.0.1";
  config.rpc_port = acceptor.local_endpoint().port();
  config.rpc_user = "user";
  config.rpc_password = "password";

  bbp::FiroDriver driver(std::chrono::seconds(30));
  std::stop_source stop_source;
  std::atomic<bool> cancelled = false;
  std::exception_ptr failure;
  std::thread stopper([&] {
    try {
      driver.Stop(config, stop_source.get_token());
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
  stopper.join();
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

BOOST_AUTO_TEST_CASE(firo_load_submission_returns_before_observation) {
  namespace asio = boost::asio;
  using tcp = asio::ip::tcp;

  asio::io_context server_context;
  tcp::acceptor acceptor(
      server_context,
      tcp::endpoint(asio::ip::make_address_v4("127.0.0.1"), 0U));
  const std::vector<std::string> responses = {
      R"({"result":true,"error":null,"id":"bbp"})",
      R"({"result":"load-transfer-tx","error":null,"id":"bbp"})"};
  std::vector<boost::json::value> requests;
  std::future<std::vector<std::string>> served = std::async(
      std::launch::async,
      [&] { return ServeRpcResponses(acceptor, responses, &requests); });

  bbp::FiroNodeConfig config;
  config.id = "load-transfer-test";
  config.rpc_host = "127.0.0.1";
  config.rpc_port = acceptor.local_endpoint().port();
  config.rpc_user = "user";
  config.rpc_password = "password";
  const bbp::FiroDriver driver(std::chrono::seconds(1));

  BOOST_TEST(driver.SupportsWalletTransactionMode(bbp::WalletMode::kPublic));
  BOOST_TEST(driver.SupportsWalletTransactionMode(bbp::WalletMode::kPrivate));
  const bbp::FiroWalletTransactionResult result =
      driver.SubmitWalletTransaction(config, bbp::WalletMode::kPublic,
                                     "load-destination", 25000000ULL, 1000ULL,
                                     std::chrono::seconds(1));
  const std::vector<std::string> methods = served.get();

  BOOST_REQUIRE_EQUAL(result.txids.size(), 1U);
  BOOST_TEST(result.txids[0] == "load-transfer-tx");
  BOOST_TEST(result.destination_amount == "0.25000000");
  BOOST_TEST(result.requested_fee_rate == "0.00001000");
  BOOST_TEST(result.mempool_size == 0U);
  BOOST_REQUIRE_EQUAL(methods.size(), 2U);
  BOOST_TEST(methods[0] == "settxfee");
  BOOST_TEST(methods[1] == "sendtoaddress");
  BOOST_REQUIRE_EQUAL(requests.size(), 2U);
}

BOOST_AUTO_TEST_CASE(firo_wallet_submission_rejects_multiple_transaction_ids) {
  namespace asio = boost::asio;
  using tcp = asio::ip::tcp;

  asio::io_context server_context;
  tcp::acceptor acceptor(
      server_context,
      tcp::endpoint(asio::ip::make_address_v4("127.0.0.1"), 0U));
  const std::vector<std::string> responses = {
      R"({"result":true,"error":null,"id":"bbp"})",
      R"({"result":["unexpected-1","unexpected-2"],"error":null,"id":"bbp"})"};
  std::future<std::vector<std::string>> served =
      std::async(std::launch::async,
                 [&] { return ServeRpcResponses(acceptor, responses); });

  bbp::FiroNodeConfig config;
  config.id = "multiple-transfer-test";
  config.rpc_host = "127.0.0.1";
  config.rpc_port = acceptor.local_endpoint().port();
  config.rpc_user = "user";
  config.rpc_password = "password";
  const bbp::FiroDriver driver(std::chrono::seconds(1));

  BOOST_CHECK_THROW(driver.SubmitWalletTransaction(
                        config, bbp::WalletMode::kPublic, "destination",
                        25000000ULL, 1000ULL, std::chrono::seconds(1)),
                    std::runtime_error);
  const std::vector<std::string> methods = served.get();
  BOOST_REQUIRE_EQUAL(methods.size(), 2U);
  BOOST_TEST(methods[0] == "settxfee");
  BOOST_TEST(methods[1] == "sendtoaddress");
}

BOOST_AUTO_TEST_CASE(
    firo_load_submit_propagate_mine_confirm_reaches_report_once) {
  namespace asio = boost::asio;
  using tcp = asio::ip::tcp;
  using namespace std::chrono_literals;

  asio::io_context server_context;
  tcp::acceptor acceptor(
      server_context,
      tcp::endpoint(asio::ip::make_address_v4("127.0.0.1"), 0U));
  const std::vector<std::string> responses = {
      R"({"result":true,"error":null,"id":"bbp"})",
      R"({"result":"late-confirmation-tx","error":null,"id":"bbp"})",
      R"({"result":200,"error":null,"id":"bbp"})",
      R"({"result":["late-confirmation-tx"],"error":null,"id":"bbp"})",
      R"({"result":{"txid":"late-confirmation-tx"},"error":null,"id":"bbp"})",
      R"({"result":["block-201"],"error":null,"id":"bbp"})",
      R"({"result":201,"error":null,"id":"bbp"})",
      R"({"result":[],"error":null,"id":"bbp"})",
      R"({"result":{"txid":"late-confirmation-tx","blockhash":"block-201","height":201,"confirmations":1},"error":null,"id":"bbp"})"};
  std::vector<boost::json::value> requests;
  std::future<std::vector<std::string>> served = std::async(
      std::launch::async,
      [&] { return ServeRpcResponses(acceptor, responses, &requests); });

  bbp::FiroNodeConfig config;
  config.id = "load-confirmation-node";
  config.rpc_host = "127.0.0.1";
  config.rpc_port = acceptor.local_endpoint().port();
  config.rpc_user = "user";
  config.rpc_password = "password";
  const bbp::FiroDriver driver(1s);

  auto accounting = std::make_shared<bbp::TransactionLoadAccounting>();
  const bbp::ChainWalletTransactionResult submitted =
      driver.SubmitWalletTransaction(config, bbp::WalletMode::kPublic,
                                     "load-destination", 25'000'000U, 1'000U,
                                     1s);
  BOOST_REQUIRE_EQUAL(submitted.txids.size(), 1U);
  const bbp::TransactionLoadSnapshot submitted_progress =
      accounting->RecordOutcome(bbp::TransactionLoadOutcome::kSubmitted, 10us);
  bbp::TransactionLoadConfirmation confirmation(
      accounting, {{submitted.txids.front(), config.id}});

  const bbp::ChainTransactionObservation propagated =
      driver.ObserveTransaction(config, submitted.txids.front());
  BOOST_CHECK(propagated.state == bbp::ChainTransactionState::kMempool);
  BOOST_TEST(!confirmation.RecordObservation(
      submitted.txids.front(), config.id,
      propagated.state == bbp::ChainTransactionState::kConfirmed));
  const bbp::TransactionLoadSnapshot propagated_progress =
      confirmation.RecordPropagated(false);
  BOOST_TEST(accounting->Snapshot(1s).confirmed == 0U);

  const std::vector<std::string> blocks =
      driver.GenerateBlocks(config, 1U, "mining-address");
  BOOST_REQUIRE_EQUAL(blocks.size(), 1U);
  BOOST_TEST(blocks.front() == "block-201");
  const bbp::ChainTransactionObservation confirmed =
      driver.ObserveTransaction(config, submitted.txids.front());
  BOOST_CHECK(confirmed.state == bbp::ChainTransactionState::kConfirmed);
  const std::optional<bbp::TransactionLoadSnapshot> confirmed_progress =
      confirmation.RecordObservation(submitted.txids.front(), config.id, true);
  BOOST_REQUIRE(confirmed_progress);
  BOOST_TEST(!confirmation.RecordObservation(submitted.txids.front(), config.id,
                                             true));

  const bbp::TransactionLoadSnapshot snapshot = accounting->Snapshot(1s);
  BOOST_TEST(snapshot.attempted == 1U);
  BOOST_TEST(snapshot.submitted == 1U);
  BOOST_TEST(snapshot.propagated == 1U);
  BOOST_TEST(snapshot.confirmed == 1U);
  BOOST_TEST(snapshot.confirmed_per_second == 1.0);
  BOOST_TEST(snapshot.InvariantsHold());

  const std::filesystem::path report_root =
      std::filesystem::temp_directory_path() /
      ("bbp-load-late-confirmation-report-" + std::to_string(getpid()));
  std::filesystem::remove_all(report_root);
  std::filesystem::create_directories(report_root);
  bbp::WriteText(report_root / "resolved-scenario.json",
                 R"({"run_id":"late-confirmation","chain":"firo","nodes":1})");
  bbp::AppendLine(
      report_root / "events.jsonl",
      R"({"run_id":"late-confirmation","node_id":"sim","event":"run_started"})");
  const auto append_progress = [&](const bbp::TransactionLoadSnapshot& value) {
    boost::json::object progress_detail;
    progress_detail["workload_index"] = 1U;
    progress_detail["workload_count"] = 1U;
    progress_detail["revision"] = value.revision;
    progress_detail["attempted"] = value.attempted;
    progress_detail["submitted"] = value.submitted;
    progress_detail["rejected"] = value.rejected;
    progress_detail["timed_out"] = value.timed_out;
    progress_detail["backpressured"] = value.backpressured;
    progress_detail["dropped"] = value.dropped;
    progress_detail["cancelled"] = value.cancelled;
    progress_detail["propagated"] = value.propagated;
    progress_detail["confirmed"] = value.confirmed;
    progress_detail["failed"] = value.failed;
    boost::json::object progress_event;
    progress_event["run_id"] = "late-confirmation";
    progress_event["node_id"] = "sim";
    progress_event["event"] = "transaction_load_progress";
    progress_event["detail"] = boost::json::serialize(progress_detail);
    bbp::AppendLine(report_root / "events.jsonl",
                    boost::json::serialize(progress_event));
  };
  append_progress(submitted_progress);
  append_progress(propagated_progress);
  append_progress(*confirmed_progress);
  boost::json::object detail;
  detail["workload_index"] = 1U;
  detail["workload_count"] = 1U;
  detail["revision"] = snapshot.revision;
  detail["attempted"] = snapshot.attempted;
  detail["submitted"] = snapshot.submitted;
  detail["rejected"] = snapshot.rejected;
  detail["timed_out"] = snapshot.timed_out;
  detail["backpressured"] = snapshot.backpressured;
  detail["dropped"] = snapshot.dropped;
  detail["cancelled"] = snapshot.cancelled;
  detail["propagated"] = snapshot.propagated;
  detail["confirmed"] = snapshot.confirmed;
  detail["failed"] = snapshot.failed;
  detail["confirmed_per_second"] = snapshot.confirmed_per_second;
  detail["accounting_invariants_hold"] = snapshot.InvariantsHold();
  boost::json::object event;
  event["run_id"] = "late-confirmation";
  event["node_id"] = "sim";
  event["event"] = "transaction_load_completed";
  event["detail"] = boost::json::serialize(detail);
  bbp::AppendLine(report_root / "events.jsonl", boost::json::serialize(event));
  bbp::AppendLine(
      report_root / "events.jsonl",
      R"({"run_id":"late-confirmation","node_id":"sim","event":"run_finished"})");

  const boost::json::object report = bbp::BuildRunReport(report_root);
  BOOST_TEST(JsonNonNegativeInteger(
                 report.at("transaction_load_completed_count")) == 1U);
  const boost::json::array& live =
      report.at("transaction_load_live").as_array();
  BOOST_REQUIRE_EQUAL(live.size(), 1U);
  const boost::json::object& live_load = live.front().as_object();
  BOOST_TEST(JsonNonNegativeInteger(live_load.at("workload_index")) == 1U);
  BOOST_TEST(JsonNonNegativeInteger(live_load.at("revision")) == 3U);
  BOOST_TEST(JsonNonNegativeInteger(live_load.at("confirmed")) == 1U);
  BOOST_TEST(live_load.at("completed").as_bool());
  const boost::json::array& summaries =
      report.at("transaction_load_summaries").as_array();
  BOOST_REQUIRE_EQUAL(summaries.size(), 1U);
  const boost::json::object& stored =
      summaries.front().as_object().at("detail").as_object();
  BOOST_TEST(JsonNonNegativeInteger(stored.at("confirmed")) == 1U);
  BOOST_TEST(stored.at("confirmed_per_second").as_double() == 1.0);
  BOOST_TEST(stored.at("accounting_invariants_hold").as_bool());
  std::filesystem::remove_all(report_root);

  const std::vector<std::string> methods = served.get();
  const std::vector<std::string> expected_methods = {
      "settxfee",      "sendtoaddress",     "getblockcount",
      "getrawmempool", "getrawtransaction", "generatetoaddress",
      "getblockcount", "getrawmempool",     "getrawtransaction"};
  BOOST_TEST(methods == expected_methods, boost::test_tools::per_element());
}

BOOST_AUTO_TEST_CASE(firo_load_submission_classifies_rpc_rejection) {
  namespace asio = boost::asio;
  using tcp = asio::ip::tcp;

  asio::io_context server_context;
  tcp::acceptor acceptor(
      server_context,
      tcp::endpoint(asio::ip::make_address_v4("127.0.0.1"), 0U));
  const std::vector<std::string> responses = {
      R"({"result":true,"error":null,"id":"bbp"})",
      R"({"result":null,"error":{"code":-6,"message":"Insufficient funds"},"id":"bbp"})"};
  const std::vector<unsigned> statuses = {200U, 500U};
  std::future<std::vector<std::string>> served =
      std::async(std::launch::async, [&] {
        return ServeRpcResponses(acceptor, responses, nullptr, &statuses);
      });

  bbp::FiroNodeConfig config;
  config.id = "load-rejection-test";
  config.rpc_host = "127.0.0.1";
  config.rpc_port = acceptor.local_endpoint().port();
  config.rpc_user = "user";
  config.rpc_password = "password";
  const bbp::FiroDriver driver(std::chrono::seconds(1));

  BOOST_CHECK_THROW(driver.SubmitWalletTransaction(
                        config, bbp::WalletMode::kPublic, "load-destination",
                        25000000ULL, 1000ULL, std::chrono::seconds(1)),
                    bbp::ChainTransactionRejected);
  const std::vector<std::string> methods = served.get();
  BOOST_REQUIRE_EQUAL(methods.size(), 2U);
  BOOST_TEST(methods[0] == "settxfee");
  BOOST_TEST(methods[1] == "sendtoaddress");
}

BOOST_AUTO_TEST_CASE(
    firo_load_submission_shares_one_wall_clock_deadline_across_rpcs) {
  namespace asio = boost::asio;
  using tcp = asio::ip::tcp;

  asio::io_context server_context;
  tcp::acceptor acceptor(
      server_context,
      tcp::endpoint(asio::ip::make_address_v4("127.0.0.1"), 0U));
  const std::vector<std::string> responses = {
      R"({"result":true,"error":null,"id":"bbp"})",
      R"({"result":"late-success","error":null,"id":"bbp"})"};
  const std::vector<std::chrono::milliseconds> delays = {
      std::chrono::milliseconds(650), std::chrono::milliseconds(650)};
  std::future<std::vector<std::string>> served = std::async(
      std::launch::async,
      [&] { return ServeDelayedRpcResponses(acceptor, responses, delays); });

  bbp::FiroNodeConfig config;
  config.id = "load-deadline-test";
  config.rpc_host = "127.0.0.1";
  config.rpc_port = acceptor.local_endpoint().port();
  config.rpc_user = "user";
  config.rpc_password = "password";
  const bbp::FiroDriver driver(std::chrono::seconds(5));

  const auto started = std::chrono::steady_clock::now();
  bool timed_out = false;
  bool accepted_late_success = false;
  try {
    static_cast<void>(driver.SubmitWalletTransaction(
        config, bbp::WalletMode::kPublic, "load-destination", 25000000ULL,
        1000ULL, std::chrono::seconds(1)));
    accepted_late_success = true;
  } catch (const bbp::ChainTransactionTimedOut&) {
    timed_out = true;
  }
  const auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(
      std::chrono::steady_clock::now() - started);
  const std::vector<std::string> methods = served.get();

  BOOST_TEST(timed_out);
  BOOST_TEST(!accepted_late_success);
  BOOST_TEST(elapsed.count() >= 850);
  BOOST_TEST(elapsed.count() < 1250);
  const std::vector<std::string> expected_methods = {"settxfee",
                                                     "sendtoaddress"};
  BOOST_TEST(methods == expected_methods, boost::test_tools::per_element());
}

BOOST_AUTO_TEST_CASE(firo_load_submission_preserves_exact_rpc_error_classes) {
  namespace asio = boost::asio;
  using tcp = asio::ip::tcp;

  const auto classify = [](std::int64_t code) {
    asio::io_context server_context;
    tcp::acceptor acceptor(
        server_context,
        tcp::endpoint(asio::ip::make_address_v4("127.0.0.1"), 0U));
    const std::vector<std::string> responses = {
        R"({"result":true,"error":null,"id":"bbp"})",
        std::string(R"({"result":null,"error":{"code":)") +
            std::to_string(code) +
            R"(,"message":"classified failure"},"id":"bbp"})"};
    const std::vector<unsigned> statuses = {200U, 500U};
    std::future<std::vector<std::string>> served =
        std::async(std::launch::async, [&] {
          return ServeRpcResponses(acceptor, responses, nullptr, &statuses);
        });

    bbp::FiroNodeConfig config;
    config.id = "load-error-class-test";
    config.rpc_host = "127.0.0.1";
    config.rpc_port = acceptor.local_endpoint().port();
    config.rpc_user = "user";
    config.rpc_password = "password";
    const bbp::FiroDriver driver(std::chrono::seconds(2));
    std::string classification = "success";
    try {
      static_cast<void>(driver.SubmitWalletTransaction(
          config, bbp::WalletMode::kPublic, "load-destination", 25000000ULL,
          1000ULL, std::chrono::seconds(1)));
    } catch (const bbp::ChainTransactionRejected&) {
      classification = "policy_rejection";
    } catch (const bbp::ChainTransactionRpcWarmup&) {
      classification = "warmup";
    } catch (const bbp::ChainTransactionRpcMethodUnavailable&) {
      classification = "method_unavailable";
    } catch (const bbp::ChainTransactionInternalRpcFailure&) {
      classification = "internal_rpc";
    }
    const std::vector<std::string> methods = served.get();
    const std::vector<std::string> expected_methods = {"settxfee",
                                                       "sendtoaddress"};
    BOOST_TEST(methods == expected_methods, boost::test_tools::per_element());
    return classification;
  };

  BOOST_TEST(classify(-6) == "policy_rejection");
  BOOST_TEST(classify(-28) == "warmup");
  BOOST_TEST(classify(-32601) == "method_unavailable");
  BOOST_TEST(classify(-32603) == "internal_rpc");
  BOOST_TEST(classify(-1) == "internal_rpc");
}

BOOST_AUTO_TEST_CASE(firo_load_submission_classifies_transport_failure) {
  namespace asio = boost::asio;
  using tcp = asio::ip::tcp;

  asio::io_context server_context;
  tcp::acceptor acceptor(
      server_context,
      tcp::endpoint(asio::ip::make_address_v4("127.0.0.1"), 0U));
  const std::uint16_t refused_port = acceptor.local_endpoint().port();
  boost::system::error_code close_error;
  acceptor.close(close_error);
  BOOST_REQUIRE(!close_error);

  bbp::FiroNodeConfig config;
  config.id = "load-transport-test";
  config.rpc_host = "127.0.0.1";
  config.rpc_port = refused_port;
  config.rpc_user = "user";
  config.rpc_password = "password";
  const bbp::FiroDriver driver(std::chrono::seconds(2));
  BOOST_CHECK_THROW(driver.SubmitWalletTransaction(
                        config, bbp::WalletMode::kPublic, "load-destination",
                        25000000ULL, 1000ULL, std::chrono::seconds(1)),
                    bbp::ChainTransactionTransportFailure);
}

BOOST_AUTO_TEST_CASE(firo_load_submission_preserves_cancellation_class) {
  namespace asio = boost::asio;
  namespace beast = boost::beast;
  namespace http = beast::http;
  using tcp = asio::ip::tcp;

  asio::io_context server_context;
  tcp::acceptor acceptor(
      server_context,
      tcp::endpoint(asio::ip::make_address_v4("127.0.0.1"), 0U));
  std::promise<void> request_received;
  std::future<void> request_received_future = request_received.get_future();
  std::mutex server_mutex;
  std::condition_variable_any server_wakeup;
  std::jthread server([&](std::stop_token stop_token) {
    tcp::socket socket(server_context);
    acceptor.accept(socket);
    beast::flat_buffer buffer;
    http::request<http::string_body> request;
    http::read(socket, buffer, request);
    request_received.set_value();
    std::unique_lock<std::mutex> lock(server_mutex);
    server_wakeup.wait(lock, stop_token, [] { return false; });
  });

  bbp::FiroNodeConfig config;
  config.id = "load-cancellation-test";
  config.rpc_host = "127.0.0.1";
  config.rpc_port = acceptor.local_endpoint().port();
  config.rpc_user = "user";
  config.rpc_password = "password";
  const bbp::FiroDriver driver(std::chrono::seconds(30));
  std::stop_source stop_source;
  std::atomic<bool> cancelled = false;
  std::exception_ptr failure;
  std::thread submitter([&] {
    try {
      static_cast<void>(driver.SubmitWalletTransaction(
          config, bbp::WalletMode::kPublic, "load-destination", 25000000ULL,
          1000ULL, std::chrono::seconds(20), stop_source.get_token()));
    } catch (const bbp::SimulationCancelled&) {
      cancelled = true;
    } catch (...) {
      failure = std::current_exception();
    }
  });

  BOOST_REQUIRE(request_received_future.wait_for(std::chrono::seconds(1)) ==
                std::future_status::ready);
  const auto stop_started = std::chrono::steady_clock::now();
  stop_source.request_stop();
  submitter.join();
  const auto stop_elapsed =
      std::chrono::duration_cast<std::chrono::milliseconds>(
          std::chrono::steady_clock::now() - stop_started);
  server.request_stop();
  server_wakeup.notify_all();
  server.join();

  BOOST_TEST(cancelled.load());
  BOOST_TEST(!static_cast<bool>(failure));
  BOOST_TEST(stop_elapsed.count() < 500);
}

BOOST_AUTO_TEST_CASE(firo_many_node_observation_shares_one_deadline) {
  namespace asio = boost::asio;
  using tcp = asio::ip::tcp;

  asio::io_context server_context;
  tcp::acceptor acceptor(
      server_context,
      tcp::endpoint(asio::ip::make_address_v4("127.0.0.1"), 0U));
  std::vector<std::string> responses;
  std::vector<std::chrono::milliseconds> delays;
  for (std::size_t node = 0U; node < 3U; ++node) {
    static_cast<void>(node);
    responses.push_back(R"({"result":1,"error":null,"id":"bbp"})");
    responses.push_back(R"({"result":[],"error":null,"id":"bbp"})");
    responses.push_back(
        R"({"result":null,"error":{"code":-5,"message":"not found"},"id":"bbp"})");
    delays.push_back(std::chrono::milliseconds(400));
    delays.push_back(std::chrono::milliseconds(0));
    delays.push_back(std::chrono::milliseconds(0));
  }
  responses.push_back(R"({"result":1,"error":null,"id":"bbp"})");
  delays.push_back(std::chrono::milliseconds(400));
  std::future<std::vector<std::string>> served = std::async(
      std::launch::async,
      [&] { return ServeDelayedRpcResponses(acceptor, responses, delays); });

  bbp::FiroNodeConfig config;
  config.rpc_host = "127.0.0.1";
  config.rpc_port = acceptor.local_endpoint().port();
  config.rpc_user = "user";
  config.rpc_password = "password";
  const bbp::FiroDriver driver(std::chrono::seconds(5));
  const auto started = std::chrono::steady_clock::now();
  const auto deadline = started + std::chrono::milliseconds(1500);
  std::size_t completed = 0U;
  std::size_t timed_out = 0U;
  for (std::size_t node = 0U; node < 6U; ++node) {
    config.id = "deadline-node-" + std::to_string(node + 1U);
    try {
      static_cast<void>(
          driver.ObserveTransactionUntil(config, "load-txid", deadline));
      ++completed;
    } catch (const bbp::ChainTransactionTimedOut&) {
      ++timed_out;
    }
  }
  const auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(
      std::chrono::steady_clock::now() - started);
  const std::vector<std::string> methods = served.get();

  BOOST_TEST(completed == 3U);
  BOOST_TEST(timed_out == 3U);
  BOOST_TEST(elapsed.count() >= 1350);
  BOOST_TEST(elapsed.count() < 1800);
  BOOST_TEST(methods.size() == 10U);
  BOOST_TEST(methods.back() == "getblockcount");
}

BOOST_AUTO_TEST_CASE(
    firo_partial_fanout_failure_reconciles_actual_balance_before_retry) {
  namespace asio = boost::asio;
  using tcp = asio::ip::tcp;

  asio::io_context server_context;
  tcp::acceptor acceptor(
      server_context,
      tcp::endpoint(asio::ip::make_address_v4("127.0.0.1"), 0U));
  const std::vector<std::string> responses = {
      R"({"result":{"balance":0.00003500,"unconfirmed_balance":0.0,"immature_balance":0.0,"txcount":0},"error":null,"id":"bbp"})",
      R"({"result":[],"error":null,"id":"bbp"})",
      R"({"result":true,"error":null,"id":"bbp"})",
      R"({"result":"fanout-one","error":null,"id":"bbp"})",
      R"({"result":true,"error":null,"id":"bbp"})",
      R"({"result":null,"error":{"code":-6,"message":"Insufficient funds"},"id":"bbp"})",
      R"({"result":{"balance":0.00002334,"unconfirmed_balance":0.0,"immature_balance":0.0,"txcount":1},"error":null,"id":"bbp"})",
      R"({"result":[],"error":null,"id":"bbp"})",
      R"({"result":true,"error":null,"id":"bbp"})",
      R"({"result":"fanout-three","error":null,"id":"bbp"})"};
  const std::vector<unsigned> statuses = {200U, 200U, 200U, 200U, 200U,
                                          500U, 200U, 200U, 200U, 200U};
  std::future<std::vector<std::string>> served =
      std::async(std::launch::async, [&] {
        return ServeRpcResponses(acceptor, responses, nullptr, &statuses);
      });

  bbp::FiroNodeConfig config;
  config.id = "load-reconciliation-test";
  config.rpc_host = "127.0.0.1";
  config.rpc_port = acceptor.local_endpoint().port();
  config.rpc_user = "user";
  config.rpc_password = "password";
  const bbp::FiroDriver driver(std::chrono::seconds(1));
  bbp::WalletTransactionsWorkload workload;
  workload.strategy = bbp::WalletTransferStrategy::kEqualFanout;
  workload.transaction_count = 6U;
  workload.amount = bbp::AmountDistribution{
      .kind = bbp::ValueDistributionKind::kFixed,
      .minimum_satoshis = 100U,
      .maximum_satoshis = 2'000U,
  };
  workload.fee_satoshis = 10U;
  workload.fee_reserve_satoshis = 100U;
  workload.sender_wallets = {1U};
  workload.receiver_wallets = {2U, 3U, 4U};
  bbp::WalletTransactionLoadPlanner planner(4U, workload);
  const bbp::ChainWalletSnapshot initial =
      driver.ReadWalletSnapshot(config, bbp::WalletMode::kPublic, 1U);
  BOOST_REQUIRE_EQUAL(initial.available_balance_satoshis, 3'500U);
  bbp::TransactionLoadBalanceReservations reservations(
      {initial.available_balance_satoshis, 0U, 0U, 0U}, 100U, 6U);
  const auto first = reservations.PlanAndReserve(
      &planner, 1U, 6U,
      [](const std::vector<bbp::WalletTransactionPlanEntry>&) { return true; });
  BOOST_REQUIRE(first.admitted);
  BOOST_REQUIRE_EQUAL(first.plans.size(), 3U);

  const bbp::ChainWalletTransactionResult submitted_first =
      driver.SubmitWalletTransaction(
          config, bbp::WalletMode::kPublic, "load-destination-1",
          first.plans.at(0).amount_satoshis, 10U, std::chrono::seconds(1));
  BOOST_REQUIRE_EQUAL(submitted_first.txids.size(), 1U);
  reservations.Settle(1U, std::nullopt, false);
  BOOST_CHECK_THROW(
      driver.SubmitWalletTransaction(
          config, bbp::WalletMode::kPublic, "load-destination-2",
          first.plans.at(1).amount_satoshis, 10U, std::chrono::seconds(1)),
      bbp::ChainTransactionRejected);
  const bbp::ChainWalletSnapshot actual =
      driver.ReadWalletSnapshot(config, bbp::WalletMode::kPublic, 1U);
  BOOST_TEST(actual.available_balance_satoshis == 2'334U);
  reservations.Settle(2U, actual.available_balance_satoshis, false);
  BOOST_TEST(reservations.available_balances().front() == 1'168U);
  BOOST_TEST(reservations.outstanding_size() == 1U);
  const bbp::ChainWalletTransactionResult submitted_third =
      driver.SubmitWalletTransaction(
          config, bbp::WalletMode::kPublic, "load-destination-3",
          first.plans.at(2).amount_satoshis, 10U, std::chrono::seconds(1));
  BOOST_REQUIRE_EQUAL(submitted_third.txids.size(), 1U);
  reservations.Settle(3U, std::nullopt, false);

  const auto retry = reservations.PlanAndReserve(
      &planner, 4U, 3U,
      [](const std::vector<bbp::WalletTransactionPlanEntry>&) {
        return false;
      });
  BOOST_REQUIRE(retry.has_plan());
  BOOST_TEST(!retry.admitted);
  BOOST_REQUIRE_EQUAL(retry.plans.size(), 3U);
  BOOST_TEST(retry.plans.front().amount_satoshis == 289U);
  BOOST_TEST(reservations.available_balances().front() == 1'168U);
  BOOST_TEST(reservations.outstanding_size() == 0U);

  const std::vector<std::string> methods = served.get();
  const std::vector<std::string> expected_methods = {
      "getwalletinfo", "listtransactions", "settxfee",      "sendtoaddress",
      "settxfee",      "sendtoaddress",    "getwalletinfo", "listtransactions",
      "settxfee",      "sendtoaddress"};
  BOOST_TEST(methods == expected_methods, boost::test_tools::per_element());
}

BOOST_AUTO_TEST_CASE(firo_reserves_the_maximum_standard_transaction_fee) {
  const bbp::FiroDriver driver(std::chrono::seconds(1));
  BOOST_TEST(driver.WalletTransactionFeeReserveSatoshis(
                 bbp::WalletMode::kPublic, 1000U) == 100000U);
  BOOST_TEST(driver.WalletTransactionFeeReserveSatoshis(
                 bbp::WalletMode::kPrivate, 1000U) == 100000U);
  BOOST_CHECK_THROW(
      driver.WalletTransactionFeeReserveSatoshis(
          bbp::WalletMode::kPublic, std::numeric_limits<std::uint64_t>::max()),
      std::runtime_error);
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
  std::vector<boost::json::value> requests;
  std::future<std::vector<std::string>> served = std::async(
      std::launch::async,
      [&] { return ServeRpcResponses(acceptor, responses, &requests); });

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
  BOOST_REQUIRE_EQUAL(requests.size(), 1U);
  const boost::json::array& parameters =
      requests[0].as_object().at("params").as_array();
  BOOST_REQUIRE_EQUAL(parameters.size(), 2U);
  BOOST_REQUIRE(parameters[0].is_string());
  BOOST_TEST(parameters[0].as_string() == "block-hash");
  BOOST_REQUIRE(parameters[1].is_bool());
  BOOST_TEST(parameters[1].as_bool());
  BOOST_CHECK(!parameters[1].is_int64());
  BOOST_CHECK(!parameters[1].is_uint64());
}

BOOST_AUTO_TEST_CASE(firo_finds_spendable_output_with_boolean_getblock_mode) {
  namespace asio = boost::asio;
  using tcp = asio::ip::tcp;

  asio::io_context server_context;
  tcp::acceptor acceptor(
      server_context,
      tcp::endpoint(asio::ip::make_address_v4("127.0.0.1"), 0U));
  const std::vector<std::string> responses = {
      R"({"result":{"hash":"funding-block","tx":["funding-tx"]},"error":null,"id":"bbp"})",
      R"({"result":{"value":40.00000000,"confirmations":101,"scriptPubKey":{"hex":"51","addresses":["funding-address"]}},"error":null,"id":"bbp"})"};
  std::vector<boost::json::value> requests;
  std::future<std::vector<std::string>> served = std::async(
      std::launch::async,
      [&] { return ServeRpcResponses(acceptor, responses, &requests); });

  bbp::FiroNodeConfig config;
  config.id = "spendable-output-getblock-mode-test";
  config.rpc_host = "127.0.0.1";
  config.rpc_port = acceptor.local_endpoint().port();
  config.rpc_user = "user";
  config.rpc_password = "password";
  const bbp::FiroDriver driver(std::chrono::seconds(1));

  const bbp::FiroUtxo output = driver.FindSpendableOutput(
      config, {"funding-block"}, "funding-address", 3999000000ULL, 101U);
  const std::vector<std::string> methods = served.get();

  BOOST_TEST(output.txid == "funding-tx");
  BOOST_TEST(output.vout == 0U);
  BOOST_TEST(output.amount_satoshis == 4000000000ULL);
  BOOST_REQUIRE_EQUAL(methods.size(), 2U);
  BOOST_TEST(methods[0] == "getblock");
  BOOST_TEST(methods[1] == "gettxout");
  BOOST_REQUIRE_EQUAL(requests.size(), 2U);
  const boost::json::array& parameters =
      requests[0].as_object().at("params").as_array();
  BOOST_REQUIRE_EQUAL(parameters.size(), 2U);
  BOOST_TEST(parameters[0].as_string() == "funding-block");
  BOOST_REQUIRE(parameters[1].is_bool());
  BOOST_TEST(parameters[1].as_bool());
  BOOST_CHECK(!parameters[1].is_int64());
  BOOST_CHECK(!parameters[1].is_uint64());
}
