#include <unistd.h>

#include <algorithm>
#include <boost/asio/ip/tcp.hpp>
#include <boost/beast/core.hpp>
#include <boost/beast/http.hpp>
#include <boost/json/parse.hpp>
#include <boost/test/unit_test.hpp>
#include <filesystem>
#include <future>
#include <string>

#include "bbp/drivers/bitcoin_driver.h"

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
