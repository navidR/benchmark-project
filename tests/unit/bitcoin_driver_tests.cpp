#include <unistd.h>

#include <algorithm>
#include <boost/asio/ip/tcp.hpp>
#include <boost/beast/core.hpp>
#include <boost/beast/http.hpp>
#include <boost/json/parse.hpp>
#include <boost/json/serialize.hpp>
#include <boost/test/unit_test.hpp>
#include <cstdlib>
#include <filesystem>
#include <future>
#include <optional>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "bbp/drivers/bitcoin_driver.h"
#include "bbp/http_client.h"
#include "bbp/process.h"

namespace {

bool HasArgument(const bbp::ProcessSpec& process, const std::string& expected) {
  return std::find(process.argv.begin(), process.argv.end(), expected) !=
         process.argv.end();
}

std::filesystem::path TestRoot() {
  return std::filesystem::temp_directory_path() /
         ("bbp-bitcoin-driver-" + std::to_string(getpid()));
}

bbp::ChainNodeConfig TestConfig() {
  const std::filesystem::path root = TestRoot();
  bbp::ChainNodeConfig config;
  config.id = "bitcoin-1";
  config.binary = "/opt/bitcoin/bin/bitcoind";
  config.data_dir = root / "data";
  config.log_dir = root;
  config.p2p_port = 18444U;
  config.rpc_port = 18443U;
  config.rpc_authentication = bbp::RpcAuthenticationMode::kCookieFile;
  config.rpc_cookie_file = root / ".bbp-rpc-cookie";
  config.rpc_host = "10.210.1.2";
  config.rpc_bind = "10.210.1.2";
  config.rpc_allow_ips = {"10.210.1.1"};
  config.p2p_bind = "10.210.1.2";
  config.wallet_enabled = false;
  return config;
}

boost::json::value ServeRpcResponse(boost::asio::ip::tcp::acceptor& acceptor,
                                    const std::string& response_body) {
  namespace beast = boost::beast;
  namespace http = beast::http;

  boost::asio::ip::tcp::socket socket(acceptor.get_executor());
  acceptor.accept(socket);
  beast::flat_buffer buffer;
  http::request<http::string_body> request;
  http::read(socket, buffer, request);
  const boost::json::value request_json = boost::json::parse(request.body());

  http::response<http::string_body> response{http::status::ok, 11};
  response.set(http::field::content_type, "application/json");
  response.body() = response_body;
  response.prepare_payload();
  http::write(socket, response);
  return request_json;
}

std::vector<boost::json::value> ServeRpcResponses(
    boost::asio::ip::tcp::acceptor& acceptor,
    const std::vector<std::string>& response_bodies) {
  std::vector<boost::json::value> requests;
  requests.reserve(response_bodies.size());
  for (const std::string& response_body : response_bodies) {
    requests.push_back(ServeRpcResponse(acceptor, response_body));
  }
  return requests;
}

boost::json::value CallBitcoinRpc(const bbp::HttpClient& client,
                                  const bbp::RpcEndpoint& endpoint,
                                  std::string_view path,
                                  std::string_view method,
                                  const boost::json::array& params) {
  boost::json::object request;
  request["jsonrpc"] = "1.0";
  request["id"] = "item12-real-bitcoin";
  request["method"] = method;
  request["params"] = params;
  const bbp::HttpResponse response =
      client.PostJson(endpoint, path, boost::json::serialize(request));
  if (response.status != 200) {
    throw std::runtime_error(
        "real Bitcoin RPC " + std::string(method) + " returned HTTP status " +
        std::to_string(response.status) + ": " + response.body);
  }
  const boost::json::value parsed = boost::json::parse(response.body);
  if (!parsed.is_object()) {
    throw std::runtime_error("real Bitcoin RPC returned a non-object");
  }
  const boost::json::object& object = parsed.as_object();
  const boost::json::value* error = object.if_contains("error");
  if (error == nullptr || !error->is_null()) {
    throw std::runtime_error("real Bitcoin RPC " + std::string(method) +
                             " failed: " + response.body);
  }
  const boost::json::value* result = object.if_contains("result");
  if (result == nullptr) {
    throw std::runtime_error("real Bitcoin RPC response omitted result");
  }
  return *result;
}

std::uint16_t UnusedLoopbackPort() {
  boost::asio::io_context context;
  boost::asio::ip::tcp::acceptor acceptor(
      context, boost::asio::ip::tcp::endpoint(
                   boost::asio::ip::make_address_v4("127.0.0.1"), 0U));
  return acceptor.local_endpoint().port();
}

class BitcoinDaemonGuard {
 public:
  BitcoinDaemonGuard(const bbp::BitcoinDriver& driver,
                     const bbp::ChainNodeConfig& config,
                     bbp::ChildProcess process)
      : driver_(driver), config_(config), process_(std::move(process)) {}

  BitcoinDaemonGuard(const BitcoinDaemonGuard&) = delete;
  BitcoinDaemonGuard& operator=(const BitcoinDaemonGuard&) = delete;

  ~BitcoinDaemonGuard() {
    if (process_.running()) {
      try {
        driver_.Stop(config_);
        static_cast<void>(process_.WaitForExit(std::chrono::seconds(10)));
      } catch (const std::exception&) {
      }
    }
    if (process_.running()) {
      try {
        process_.Terminate(std::chrono::seconds(5));
      } catch (const std::exception&) {
      }
    }
    try {
      driver_.CleanupRpcCredentials(config_);
    } catch (const std::exception&) {
    }
  }

  pid_t pid() const { return process_.pid(); }

  bool StopAndReap() {
    driver_.Stop(config_);
    const bool stopped = process_.WaitForExit(std::chrono::seconds(10));
    if (!stopped) {
      process_.Terminate(std::chrono::seconds(5));
    }
    driver_.CleanupRpcCredentials(config_);
    return stopped && !process_.running();
  }

 private:
  const bbp::BitcoinDriver& driver_;
  bbp::ChainNodeConfig config_;
  bbp::ChildProcess process_;
};

}  // namespace

BOOST_AUTO_TEST_CASE(bitcoin_driver_renders_owned_regtest_process) {
  const std::filesystem::path root = TestRoot();
  std::filesystem::remove_all(root);
  bbp::BitcoinDriver driver(std::chrono::seconds(1));
  const bbp::ProcessSpec process = driver.RenderProcess(TestConfig());

  BOOST_TEST(process.binary ==
             std::filesystem::path("/opt/bitcoin/bin/bitcoind"));
  BOOST_TEST(process.cwd == root / "data");
  BOOST_TEST(process.stdout_path == root / "stdout.log");
  BOOST_TEST(process.stderr_path == root / "stderr.log");
  BOOST_CHECK(HasArgument(process, "-regtest"));
  BOOST_CHECK(HasArgument(process, "-server=1"));
  BOOST_CHECK(HasArgument(
      process, "-rpccookiefile=" + (root / ".bbp-rpc-cookie").string()));
  BOOST_CHECK(HasArgument(process, "-rpccookieperms=owner"));
  BOOST_CHECK(HasArgument(process, "-rpcbind=10.210.1.2"));
  BOOST_CHECK(HasArgument(process, "-rpcallowip=10.210.1.1"));
  BOOST_CHECK(HasArgument(process, "-rpcport=18443"));
  BOOST_CHECK(HasArgument(process, "-bind=10.210.1.2"));
  BOOST_CHECK(HasArgument(process, "-port=18444"));
  BOOST_CHECK(HasArgument(process, "-disablewallet=1"));
  BOOST_CHECK(HasArgument(process, "-txindex=1"));
  BOOST_CHECK(HasArgument(process, "-dnsseed=0"));
  BOOST_CHECK(HasArgument(process, "-fixedseeds=0"));
  BOOST_CHECK(HasArgument(process, "-discover=0"));
  BOOST_CHECK(HasArgument(process, "-listenonion=0"));
  BOOST_CHECK(HasArgument(process, "-natpmp=0"));

  std::filesystem::remove_all(root);
}

BOOST_AUTO_TEST_CASE(bitcoin_driver_rejects_inline_rpc_credentials) {
  const std::filesystem::path root = TestRoot();
  std::filesystem::remove_all(root);
  bbp::ChainNodeConfig config = TestConfig();
  config.rpc_user = "unsafe";
  config.rpc_password = "unsafe";
  bbp::BitcoinDriver driver(std::chrono::seconds(1));
  BOOST_CHECK_EXCEPTION(driver.RenderProcess(config), std::runtime_error,
                        [](const std::runtime_error& error) {
                          const std::string message = error.what();
                          return message.find("Bitcoin Core") !=
                                     std::string::npos &&
                                 message.find("Firo") == std::string::npos;
                        });
  std::filesystem::remove_all(root);
}

BOOST_AUTO_TEST_CASE(bitcoin_driver_reports_unsupported_wallet_boundary) {
  bbp::BitcoinDriver driver(std::chrono::seconds(1));
  BOOST_CHECK_EXCEPTION(
      driver.CreateWalletAddress(TestConfig(), bbp::ChainWalletMode::kPublic),
      bbp::UnsupportedChainOperation, [](const std::runtime_error& error) {
        return std::string(error.what()).find("Bitcoin Core") !=
               std::string::npos;
      });
}

BOOST_AUTO_TEST_CASE(
    item12_prefixed_bitcoin_observes_confirmed_transaction_without_top_level_height) {
  namespace asio = boost::asio;
  using tcp = asio::ip::tcp;

  asio::io_context server_context;
  tcp::acceptor acceptor(
      server_context,
      tcp::endpoint(asio::ip::make_address_v4("127.0.0.1"), 0U));
  const std::vector<std::string> responses = {
      R"({"result":30,"error":null,"id":"bbp"})",
      R"({"result":[],"error":null,"id":"bbp"})",
      R"({"result":{"txid":"confirmed-bitcoin-tx","blockhash":"bitcoin-block-29","confirmations":2,"time":1700000000,"blocktime":1700000000},"error":null,"id":"bbp"})",
      R"({"result":{"hash":"bitcoin-block-29","height":29,"confirmations":2},"error":null,"id":"bbp"})"};
  std::future<std::vector<boost::json::value>> served =
      std::async(std::launch::async,
                 [&] { return ServeRpcResponses(acceptor, responses); });

  bbp::ChainNodeConfig config;
  config.id = "confirmed-bitcoin-observation-test";
  config.rpc_host = "127.0.0.1";
  config.rpc_port = acceptor.local_endpoint().port();
  config.rpc_user = "user";
  config.rpc_password = "password";
  const bbp::BitcoinDriver driver(std::chrono::seconds(1));

  const bbp::ChainTransactionObservation observation =
      driver.ObserveTransaction(config, "confirmed-bitcoin-tx");
  const std::vector<boost::json::value> requests = served.get();

  BOOST_CHECK(observation.state == bbp::ChainTransactionState::kConfirmed);
  BOOST_TEST(observation.observed_height == 30U);
  BOOST_TEST(observation.mempool_size == 0U);
  BOOST_TEST(observation.block_hash == "bitcoin-block-29");
  BOOST_REQUIRE(observation.confirmation_height.has_value());
  BOOST_TEST(*observation.confirmation_height == 29U);
  BOOST_TEST(observation.confirmations == 2U);
  BOOST_REQUIRE_EQUAL(requests.size(), 4U);
  const std::vector<std::string> expected_methods = {
      "getblockcount", "getrawmempool", "getrawtransaction", "getblockheader"};
  for (std::size_t index = 0U; index < expected_methods.size(); ++index) {
    BOOST_TEST(requests[index].as_object().at("method").as_string() ==
               expected_methods[index]);
  }
  const boost::json::array& transaction_params =
      requests[2].as_object().at("params").as_array();
  BOOST_REQUIRE_EQUAL(transaction_params.size(), 2U);
  BOOST_TEST(transaction_params[0].as_string() == "confirmed-bitcoin-tx");
  BOOST_REQUIRE(transaction_params[1].is_bool());
  BOOST_TEST(transaction_params[1].as_bool());
  const boost::json::array& header_params =
      requests[3].as_object().at("params").as_array();
  BOOST_REQUIRE_EQUAL(header_params.size(), 2U);
  BOOST_TEST(header_params[0].as_string() == "bitcoin-block-29");
  BOOST_REQUIRE(header_params[1].is_bool());
  BOOST_TEST(header_params[1].as_bool());
}

BOOST_AUTO_TEST_CASE(bitcoin_rejects_invalid_confirmation_header_schemas) {
  const auto rejection_message = [](const std::string& header_response) {
    namespace asio = boost::asio;
    using tcp = asio::ip::tcp;

    asio::io_context server_context;
    tcp::acceptor acceptor(
        server_context,
        tcp::endpoint(asio::ip::make_address_v4("127.0.0.1"), 0U));
    const std::vector<std::string> responses = {
        R"({"result":30,"error":null,"id":"bbp"})",
        R"({"result":[],"error":null,"id":"bbp"})",
        R"({"result":{"txid":"invalid-bitcoin-tx","blockhash":"bitcoin-block-29","confirmations":2,"time":1700000000,"blocktime":1700000000},"error":null,"id":"bbp"})",
        header_response};
    std::future<std::vector<boost::json::value>> served =
        std::async(std::launch::async,
                   [&] { return ServeRpcResponses(acceptor, responses); });

    bbp::ChainNodeConfig config;
    config.id = "invalid-bitcoin-header-test";
    config.rpc_host = "127.0.0.1";
    config.rpc_port = acceptor.local_endpoint().port();
    config.rpc_user = "user";
    config.rpc_password = "password";
    const bbp::BitcoinDriver driver(std::chrono::seconds(1));
    std::string message;
    try {
      static_cast<void>(
          driver.ObserveTransaction(config, "invalid-bitcoin-tx"));
    } catch (const std::exception& error) {
      message = error.what();
    }
    BOOST_REQUIRE_EQUAL(served.get().size(), 4U);
    return message;
  };

  BOOST_TEST(rejection_message(R"({"result":[],"error":null,"id":"bbp"})")
                 .find("getblockheader returned a non-object") !=
             std::string::npos);
  BOOST_TEST(
      rejection_message(
          R"({"result":{"hash":"bitcoin-block-29"},"error":null,"id":"bbp"})")
          .find("missing Bitcoin Core RPC uint64 field: height") !=
      std::string::npos);
  BOOST_TEST(
      rejection_message(
          R"({"result":{"hash":"bitcoin-block-29","height":-1},"error":null,"id":"bbp"})")
          .find("missing Bitcoin Core RPC uint64 field: height") !=
      std::string::npos);
  BOOST_TEST(
      rejection_message(
          R"({"result":{"hash":"different-block","height":29},"error":null,"id":"bbp"})")
          .find("getblockheader returned a different block hash") !=
      std::string::npos);
}

BOOST_AUTO_TEST_CASE(bitcoin_rejects_invalid_transaction_confirmations) {
  const auto rejection_message = [](const std::string& transaction_response) {
    namespace asio = boost::asio;
    using tcp = asio::ip::tcp;

    asio::io_context server_context;
    tcp::acceptor acceptor(
        server_context,
        tcp::endpoint(asio::ip::make_address_v4("127.0.0.1"), 0U));
    const std::vector<std::string> responses = {
        R"({"result":30,"error":null,"id":"bbp"})",
        R"({"result":[],"error":null,"id":"bbp"})", transaction_response};
    std::future<std::vector<boost::json::value>> served =
        std::async(std::launch::async,
                   [&] { return ServeRpcResponses(acceptor, responses); });

    bbp::ChainNodeConfig config;
    config.id = "invalid-bitcoin-confirmations-test";
    config.rpc_host = "127.0.0.1";
    config.rpc_port = acceptor.local_endpoint().port();
    config.rpc_user = "user";
    config.rpc_password = "password";
    const bbp::BitcoinDriver driver(std::chrono::seconds(1));
    std::string message;
    try {
      static_cast<void>(
          driver.ObserveTransaction(config, "invalid-bitcoin-tx"));
    } catch (const std::exception& error) {
      message = error.what();
    }
    BOOST_REQUIRE_EQUAL(served.get().size(), 3U);
    return message;
  };

  BOOST_TEST(
      rejection_message(
          R"({"result":{"txid":"invalid-bitcoin-tx","blockhash":"bitcoin-block-29","confirmations":0},"error":null,"id":"bbp"})")
          .find("returned zero confirmations") != std::string::npos);
  BOOST_TEST(
      rejection_message(
          R"({"result":{"txid":"invalid-bitcoin-tx","blockhash":"bitcoin-block-29","confirmations":-1},"error":null,"id":"bbp"})")
          .find("missing Bitcoin Core RPC uint64 field: confirmations") !=
      std::string::npos);
}

BOOST_AUTO_TEST_CASE(
    bitcoin_driver_uses_integer_getblock_verbosity_for_transaction_count) {
  namespace asio = boost::asio;
  using tcp = asio::ip::tcp;

  asio::io_context server_context;
  tcp::acceptor acceptor(
      server_context,
      tcp::endpoint(asio::ip::make_address_v4("127.0.0.1"), 0U));
  std::future<boost::json::value> served = std::async(std::launch::async, [&] {
    return ServeRpcResponse(
        acceptor,
        R"({"result":{"hash":"block-hash","tx":["reward","tx-1","tx-2"]},"error":null,"id":"bbp"})");
  });

  bbp::ChainNodeConfig config;
  config.id = "bitcoin-block-transaction-count-test";
  config.rpc_host = "127.0.0.1";
  config.rpc_port = acceptor.local_endpoint().port();
  config.rpc_user = "user";
  config.rpc_password = "password";
  const bbp::BitcoinDriver driver(std::chrono::seconds(1));

  BOOST_TEST(driver.ReadBlockNonRewardTransactionCount(config, "block-hash") ==
             2U);
  const boost::json::value request = served.get();
  BOOST_TEST(request.as_object().at("method").as_string() == "getblock");
  const boost::json::array& parameters =
      request.as_object().at("params").as_array();
  BOOST_REQUIRE_EQUAL(parameters.size(), 2U);
  BOOST_TEST(parameters[0].as_string() == "block-hash");
  BOOST_REQUIRE(parameters[1].is_int64());
  BOOST_TEST(parameters[1].as_int64() == 1);
  BOOST_CHECK(!parameters[1].is_bool());
}

BOOST_AUTO_TEST_CASE(
    bitcoin_real_regtest_observes_confirmed_transaction_through_driver) {
  const char* binary_environment = std::getenv("BBP_REAL_BITCOIND");
  if (binary_environment == nullptr || *binary_environment == '\0') {
    BOOST_TEST_MESSAGE(
        "BBP_REAL_BITCOIND is unset; real Bitcoin integration not requested");
    return;
  }
  const char* root_environment = std::getenv("BBP_REAL_BITCOIN_ROOT");
  BOOST_REQUIRE_MESSAGE(
      root_environment != nullptr && *root_environment != '\0',
      "BBP_REAL_BITCOIN_ROOT must name a fresh retained artifact root");
  const std::filesystem::path root = root_environment;
  if (std::filesystem::exists(root)) {
    BOOST_REQUIRE(std::filesystem::is_directory(root));
    BOOST_REQUIRE(std::filesystem::directory_iterator(root) ==
                  std::filesystem::directory_iterator());
  }

  bbp::ChainNodeConfig config;
  config.id = "item12-real-bitcoin";
  config.binary = binary_environment;
  config.data_dir = root / "data";
  config.log_dir = root / "logs";
  config.p2p_port = UnusedLoopbackPort();
  config.rpc_port = UnusedLoopbackPort();
  config.rpc_authentication = bbp::RpcAuthenticationMode::kCookieFile;
  config.rpc_cookie_file = config.log_dir / ".bbp-rpc-cookie";
  config.rpc_host = "127.0.0.1";
  config.rpc_bind = "127.0.0.1";
  config.listen = false;
  config.wallet_enabled = true;
  config.extra_args = bbp::ChainExtraArgs({"-acceptnonstdtxn=1"});

  const bbp::BitcoinDriver driver(std::chrono::seconds(10));
  bbp::ChildProcess process =
      bbp::ChildProcess::Spawn(driver.RenderProcess(config), std::nullopt);
  BitcoinDaemonGuard daemon(driver, config, std::move(process));
  driver.WaitReady(config, std::chrono::seconds(30));

  const bbp::HttpClient client(std::chrono::seconds(30));
  const bbp::RpcEndpoint endpoint = driver.Endpoint(config);
  const boost::json::value maturity_blocks_value =
      CallBitcoinRpc(client, endpoint, "/", "generatetodescriptor",
                     boost::json::array{101, "raw(51)"});
  BOOST_REQUIRE(maturity_blocks_value.is_array());
  const boost::json::array& maturity_blocks = maturity_blocks_value.as_array();
  BOOST_REQUIRE_EQUAL(maturity_blocks.size(), 101U);
  BOOST_REQUIRE(maturity_blocks.front().is_string());
  const std::string funding_block_hash(maturity_blocks.front().as_string());

  const boost::json::value funding_block =
      CallBitcoinRpc(client, endpoint, "/", "getblock",
                     boost::json::array{funding_block_hash, 2});
  BOOST_REQUIRE(funding_block.is_object());
  const boost::json::array& funding_transactions =
      funding_block.as_object().at("tx").as_array();
  BOOST_REQUIRE_EQUAL(funding_transactions.size(), 1U);
  const boost::json::object& coinbase =
      funding_transactions.front().as_object();
  const std::string coinbase_txid(coinbase.at("txid").as_string());
  const boost::json::array& coinbase_outputs = coinbase.at("vout").as_array();
  std::optional<std::uint64_t> spendable_output;
  for (std::size_t index = 0U; index < coinbase_outputs.size(); ++index) {
    const boost::json::object& output = coinbase_outputs[index].as_object();
    const boost::json::object& script = output.at("scriptPubKey").as_object();
    if (script.at("hex").as_string() == "51") {
      spendable_output = index;
      break;
    }
  }
  BOOST_REQUIRE(spendable_output.has_value());

  boost::json::object input;
  input["txid"] = coinbase_txid;
  input["vout"] = *spendable_output;
  boost::json::object data_output;
  data_output["data"] =
      "0000000000000000000000000000000000000000000000000000000000000000";
  const boost::json::value raw_transaction = CallBitcoinRpc(
      client, endpoint, "/", "createrawtransaction",
      boost::json::array{boost::json::array{std::move(input)},
                         boost::json::array{std::move(data_output)}});
  BOOST_REQUIRE(raw_transaction.is_string());
  const boost::json::value txid_value =
      CallBitcoinRpc(client, endpoint, "/", "sendrawtransaction",
                     boost::json::array{raw_transaction.as_string(), 0});
  BOOST_REQUIRE(txid_value.is_string());
  const std::string txid(txid_value.as_string());
  BOOST_REQUIRE(!txid.empty());

  const boost::json::value confirmation_blocks_value =
      CallBitcoinRpc(client, endpoint, "/", "generatetodescriptor",
                     boost::json::array{1, "raw(51)"});
  BOOST_REQUIRE(confirmation_blocks_value.is_array());
  const boost::json::array& confirmation_blocks =
      confirmation_blocks_value.as_array();
  BOOST_REQUIRE_EQUAL(confirmation_blocks.size(), 1U);
  BOOST_REQUIRE(confirmation_blocks.front().is_string());
  const std::string confirmation_block_hash(
      confirmation_blocks.front().as_string());
  const boost::json::value transaction =
      CallBitcoinRpc(client, endpoint, "/", "getrawtransaction",
                     boost::json::array{txid, true});
  BOOST_REQUIRE(transaction.is_object());
  const boost::json::object& transaction_object = transaction.as_object();
  BOOST_CHECK(transaction_object.if_contains("height") == nullptr);
  BOOST_REQUIRE(transaction_object.if_contains("blockhash") != nullptr);
  BOOST_TEST(transaction_object.at("blockhash").as_string() ==
             confirmation_block_hash);

  const bbp::ChainTransactionObservation observation =
      driver.ObserveTransaction(config, txid);
  BOOST_CHECK(observation.state == bbp::ChainTransactionState::kConfirmed);
  BOOST_TEST(observation.observed_height == 102U);
  BOOST_TEST(observation.mempool_size == 0U);
  BOOST_TEST(observation.block_hash == confirmation_block_hash);
  BOOST_REQUIRE(observation.confirmation_height.has_value());
  BOOST_TEST(*observation.confirmation_height == 102U);
  BOOST_TEST(observation.confirmations == 1U);
  BOOST_TEST(driver.ReadBlockNonRewardTransactionCount(
                 config, confirmation_block_hash) == 1U);
  BOOST_TEST(driver.PeerAddresses(config).empty());

  BOOST_TEST_MESSAGE("item12 real Bitcoin pid="
                     << daemon.pid() << " txid=" << txid
                     << " block=" << observation.block_hash
                     << " height=" << *observation.confirmation_height
                     << " root=" << root.string());
  BOOST_CHECK(daemon.StopAndReap());
  BOOST_CHECK(!std::filesystem::exists(config.rpc_cookie_file));
}
