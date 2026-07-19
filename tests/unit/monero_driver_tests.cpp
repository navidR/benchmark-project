#include <unistd.h>

#include <algorithm>
#include <atomic>
#include <boost/asio/ip/tcp.hpp>
#include <boost/beast/core.hpp>
#include <boost/beast/http.hpp>
#include <boost/json/parse.hpp>
#include <boost/test/unit_test.hpp>
#include <condition_variable>
#include <filesystem>
#include <future>
#include <mutex>
#include <optional>
#include <stop_token>
#include <string>
#include <thread>
#include <vector>

#include "bbp/drivers/monero_driver.h"
#include "bbp/simulation_cancelled.h"

namespace {

constexpr std::string_view kMiningAddress =
    "42ey1afDFnn4886T7196doS9GPMzexD9gXpsZJDwVjeRVdFCSoHnv7KPbBeGpzJBzHRCAs9"
    "UxqeoyFQMYbqSWYTfJJQAWDm";
constexpr std::string_view kHashA =
    "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
constexpr std::string_view kHashB =
    "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
constexpr std::string_view kHashC =
    "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc";

struct RecordedRequest {
  std::string path;
  boost::json::value body;
  std::string authorization;
};

std::vector<RecordedRequest> ServeDigestResponses(
    boost::asio::ip::tcp::acceptor& acceptor,
    const std::vector<std::string>& responses) {
  namespace beast = boost::beast;
  namespace http = beast::http;
  std::vector<RecordedRequest> requests;
  requests.reserve(responses.size());
  for (const std::string& response_body : responses) {
    boost::asio::ip::tcp::socket socket(acceptor.get_executor());
    acceptor.accept(socket);
    beast::flat_buffer buffer;
    {
      http::request<http::string_body> request;
      http::read(socket, buffer, request);
      BOOST_CHECK(request.find(http::field::authorization) == request.end());
      http::response<http::string_body> response{http::status::unauthorized,
                                                 11};
      response.insert(
          http::field::www_authenticate,
          "Digest qop=\"auth\",algorithm=MD5-sess,realm=\"monero-rpc\","
          "nonce=\"monero-test-nonce\",stale=false");
      response.insert(http::field::www_authenticate,
                      "Digest qop=\"auth\",algorithm=MD5,realm=\"monero-rpc\","
                      "nonce=\"monero-test-nonce\",stale=false");
      response.keep_alive(true);
      response.prepare_payload();
      http::write(socket, response);
    }
    {
      http::request<http::string_body> request;
      http::read(socket, buffer, request);
      const std::string authorization(request.at(http::field::authorization));
      BOOST_TEST(authorization.find("Digest algorithm=MD5") == 0U);
      BOOST_TEST(authorization.find("monero-test-password") ==
                 std::string::npos);
      requests.push_back(RecordedRequest{
          .path = std::string(request.target()),
          .body = boost::json::parse(request.body()),
          .authorization = authorization,
      });
      http::response<http::string_body> response{http::status::ok, 11};
      response.set(http::field::content_type, "application/json");
      response.body() = response_body;
      response.prepare_payload();
      http::write(socket, response);
    }
  }
  return requests;
}

std::filesystem::path TestRoot() {
  return std::filesystem::temp_directory_path() /
         ("bbp-monero-driver-" + std::to_string(getpid()));
}

bbp::ChainNodeConfig TestConfig(std::uint16_t rpc_port = 19081U) {
  const std::filesystem::path root = TestRoot();
  bbp::ChainNodeConfig config;
  config.id = "monero-1";
  config.binary = "/opt/monero/bin/monerod";
  config.data_dir = root / "data";
  config.log_dir = root;
  config.p2p_port = 18080U;
  config.rpc_port = rpc_port;
  config.rpc_authentication = bbp::RpcAuthenticationMode::kDigest;
  config.rpc_user = "bbp-monero-1";
  config.rpc_password = "monero-test-password";
  config.rpc_host = "10.210.1.2";
  config.rpc_bind = "10.210.1.2";
  config.rpc_allow_ips = {"10.210.1.1"};
  config.p2p_host = "10.210.1.2";
  config.p2p_bind = "10.210.1.2";
  config.connect_peers = {"10.210.1.6:18080"};
  return config;
}

bool HasArgument(const bbp::ProcessSpec& process, const std::string& expected) {
  return std::find(process.argv.begin(), process.argv.end(), expected) !=
         process.argv.end();
}

std::string JsonRpcResult(std::string result) {
  return "{\"jsonrpc\":\"2.0\",\"id\":\"bbp\",\"result\":" + result + "}";
}

std::string PeerLimitResponse(std::string_view field, std::uint32_t value,
                              std::string_view status = "OK") {
  return "{\"status\":\"" + std::string(status) + "\",\"" + std::string(field) +
         "\":" + std::to_string(value) + "}";
}

struct NetworkControlResult {
  std::vector<RecordedRequest> requests;
  std::optional<std::string> error;
};

NetworkControlResult RunNetworkControl(
    const std::vector<std::string>& responses, bool active) {
  namespace asio = boost::asio;
  using tcp = asio::ip::tcp;
  asio::io_context context;
  tcp::acceptor acceptor(
      context, tcp::endpoint(asio::ip::make_address_v4("127.0.0.1"), 0U));
  std::future<std::vector<RecordedRequest>> served =
      std::async(std::launch::async,
                 [&] { return ServeDigestResponses(acceptor, responses); });
  bbp::ChainNodeConfig config = TestConfig(acceptor.local_endpoint().port());
  config.rpc_host = "127.0.0.1";
  const bbp::MoneroDriver driver(std::chrono::seconds(1));

  std::optional<std::string> error;
  try {
    driver.SetNetworkActive(config, active);
  } catch (const std::exception& caught) {
    error = caught.what();
  } catch (...) {
    error = "unknown exception";
  }
  return NetworkControlResult{.requests = served.get(),
                              .error = std::move(error)};
}

void CheckPeerLimitRequest(const RecordedRequest& request,
                           std::string_view path, std::string_view field,
                           bool set, std::uint32_t value) {
  BOOST_TEST(request.path == path);
  const boost::json::object& body = request.body.as_object();
  BOOST_TEST(body.at("set").as_bool() == set);
  BOOST_TEST(body.at(field).to_number<std::uint64_t>() == value);
}

bbp::ChainTransactionObservation ObserveTransactionResponse(
    const std::string& transaction_response) {
  namespace asio = boost::asio;
  using tcp = asio::ip::tcp;
  asio::io_context context;
  tcp::acceptor acceptor(
      context, tcp::endpoint(asio::ip::make_address_v4("127.0.0.1"), 0U));
  const std::vector<std::string> responses = {
      R"({"status":"OK","pool_stats":{"txs_total":0,"bytes_total":0}})",
      transaction_response,
      JsonRpcResult("{\"status\":\"OK\",\"height\":1}"),
  };
  std::future<std::vector<RecordedRequest>> served =
      std::async(std::launch::async,
                 [&] { return ServeDigestResponses(acceptor, responses); });
  bbp::ChainNodeConfig config = TestConfig(acceptor.local_endpoint().port());
  config.rpc_host = "127.0.0.1";
  const bbp::MoneroDriver driver(std::chrono::seconds(1));
  try {
    bbp::ChainTransactionObservation observation =
        driver.ObserveTransaction(config, std::string(kHashA));
    static_cast<void>(served.get());
    return observation;
  } catch (...) {
    static_cast<void>(served.get());
    throw;
  }
}

std::vector<std::string> GenerateBlocksResponse(
    std::vector<std::string> responses, std::uint32_t count) {
  namespace asio = boost::asio;
  using tcp = asio::ip::tcp;
  asio::io_context context;
  tcp::acceptor acceptor(
      context, tcp::endpoint(asio::ip::make_address_v4("127.0.0.1"), 0U));
  std::future<std::vector<RecordedRequest>> served =
      std::async(std::launch::async,
                 [&] { return ServeDigestResponses(acceptor, responses); });
  bbp::ChainNodeConfig config = TestConfig(acceptor.local_endpoint().port());
  config.rpc_host = "127.0.0.1";
  const bbp::MoneroDriver driver(std::chrono::seconds(1));
  try {
    std::vector<std::string> hashes =
        driver.GenerateBlocks(config, count, std::string(kMiningAddress));
    static_cast<void>(served.get());
    return hashes;
  } catch (...) {
    static_cast<void>(served.get());
    throw;
  }
}

std::vector<std::string> PeerAddressesResponse(const std::string& response) {
  namespace asio = boost::asio;
  using tcp = asio::ip::tcp;
  asio::io_context context;
  tcp::acceptor acceptor(
      context, tcp::endpoint(asio::ip::make_address_v4("127.0.0.1"), 0U));
  const std::vector<std::string> responses = {response};
  std::future<std::vector<RecordedRequest>> served =
      std::async(std::launch::async,
                 [&] { return ServeDigestResponses(acceptor, responses); });
  bbp::ChainNodeConfig config = TestConfig(acceptor.local_endpoint().port());
  config.rpc_host = "127.0.0.1";
  const bbp::MoneroDriver driver(std::chrono::seconds(1));
  try {
    std::vector<std::string> addresses = driver.PeerAddresses(config);
    static_cast<void>(served.get());
    return addresses;
  } catch (...) {
    static_cast<void>(served.get());
    throw;
  }
}

std::uint64_t ReadBlockResponse(const std::string& response) {
  namespace asio = boost::asio;
  using tcp = asio::ip::tcp;
  asio::io_context context;
  tcp::acceptor acceptor(
      context, tcp::endpoint(asio::ip::make_address_v4("127.0.0.1"), 0U));
  const std::vector<std::string> responses = {response};
  std::future<std::vector<RecordedRequest>> served =
      std::async(std::launch::async,
                 [&] { return ServeDigestResponses(acceptor, responses); });
  bbp::ChainNodeConfig config = TestConfig(acceptor.local_endpoint().port());
  config.rpc_host = "127.0.0.1";
  const bbp::MoneroDriver driver(std::chrono::seconds(1));
  try {
    const std::uint64_t count =
        driver.ReadBlockNonRewardTransactionCount(config, std::string(kHashA));
    static_cast<void>(served.get());
    return count;
  } catch (...) {
    static_cast<void>(served.get());
    throw;
  }
}

}  // namespace

BOOST_AUTO_TEST_CASE(monero_driver_renders_owned_fakechain_process) {
  const std::filesystem::path root = TestRoot();
  std::filesystem::remove_all(root);
  const bbp::ChainNodeConfig config = TestConfig();
  const bbp::MoneroDriver driver(std::chrono::seconds(1));
  const bbp::ProcessSpec process = driver.RenderProcess(config);

  BOOST_TEST(process.binary ==
             std::filesystem::path("/opt/monero/bin/monerod"));
  BOOST_TEST(process.cwd == root / "data");
  BOOST_TEST(process.stdout_path == root / "stdout.log");
  BOOST_TEST(process.stderr_path == root / "stderr.log");
  BOOST_CHECK(HasArgument(process, "--regtest"));
  BOOST_CHECK(HasArgument(process, "--keep-fakechain"));
  BOOST_CHECK(HasArgument(process, "--fixed-difficulty=1"));
  BOOST_CHECK(HasArgument(process, "--data-dir=" + (root / "data").string()));
  BOOST_CHECK(
      HasArgument(process, "--log-file=" + (root / "monerod.log").string()));
  BOOST_CHECK(HasArgument(process, "--non-interactive"));
  BOOST_CHECK(HasArgument(process, "--no-igd"));
  BOOST_CHECK(HasArgument(process, "--disable-dns-checkpoints"));
  BOOST_CHECK(HasArgument(process, "--check-updates=disabled"));
  BOOST_CHECK(HasArgument(process, "--rpc-ssl=disabled"));
  BOOST_CHECK(HasArgument(process, "--no-zmq"));
  BOOST_CHECK(HasArgument(process, "--p2p-bind-ip=10.210.1.2"));
  BOOST_CHECK(HasArgument(process, "--p2p-bind-port=18080"));
  BOOST_CHECK(HasArgument(process, "--rpc-bind-ip=10.210.1.2"));
  BOOST_CHECK(HasArgument(process, "--rpc-bind-port=19081"));
  BOOST_CHECK(HasArgument(process, "--confirm-external-bind"));
  BOOST_CHECK(HasArgument(process, "--allow-local-ip"));
  BOOST_CHECK(HasArgument(process, "--max-connections-per-ip=16"));
  BOOST_CHECK(HasArgument(process, "--out-peers=16"));
  BOOST_CHECK(HasArgument(process, "--in-peers=16"));
  BOOST_CHECK(HasArgument(process, "--add-exclusive-node=10.210.1.6:18080"));
  BOOST_REQUIRE_EQUAL(process.environment.size(), 1U);
  BOOST_TEST(process.environment.front().first == "RPC_LOGIN");
  BOOST_TEST(process.environment.front().second ==
             "bbp-monero-1:monero-test-password");
  BOOST_CHECK(std::none_of(process.argv.begin(), process.argv.end(),
                           [](const std::string& argument) {
                             return argument.starts_with("--rpc-login") ||
                                    argument.find("monero-test-password") !=
                                        std::string::npos;
                           }));

  std::filesystem::remove_all(root);
}

BOOST_AUTO_TEST_CASE(monero_driver_validates_before_filesystem_mutation) {
  const std::filesystem::path root = TestRoot();
  std::filesystem::remove_all(root);
  const bbp::MoneroDriver driver(std::chrono::seconds(1));

  bbp::ChainNodeConfig config = TestConfig();
  config.rpc_bind.clear();
  BOOST_CHECK_THROW(driver.RenderProcess(config), std::runtime_error);
  BOOST_CHECK(!std::filesystem::exists(root));

  config = TestConfig();
  config.connect_peers = {"not-an-ip:18080"};
  BOOST_CHECK_THROW(driver.RenderProcess(config), std::runtime_error);
  BOOST_CHECK(!std::filesystem::exists(root));
}

BOOST_AUTO_TEST_CASE(monero_driver_rejects_unsafe_rpc_credentials) {
  const std::filesystem::path root = TestRoot();
  std::filesystem::remove_all(root);
  const bbp::MoneroDriver driver(std::chrono::seconds(1));
  const auto reject_without_password = [&](bbp::ChainNodeConfig config) {
    const std::string password = config.rpc_password;
    BOOST_CHECK_EXCEPTION(driver.RenderProcess(config), std::runtime_error,
                          [&](const std::runtime_error& error) {
                            return std::string(error.what()).find(password) ==
                                   std::string::npos;
                          });
    BOOST_CHECK(!std::filesystem::exists(root));
  };

  bbp::ChainNodeConfig config = TestConfig();
  config.rpc_cookie_file = root / "must-not-exist";
  reject_without_password(config);
  config = TestConfig();
  config.rpc_user = "unsafe:user";
  reject_without_password(config);
  config = TestConfig();
  config.rpc_password = "unsafe:password";
  reject_without_password(config);
  config = TestConfig();
  config.rpc_user = "unsafe\nuser";
  reject_without_password(config);
  config = TestConfig();
  config.rpc_password.assign(257U, 'p');
  reject_without_password(config);
  std::filesystem::remove_all(root);
}

BOOST_AUTO_TEST_CASE(monero_driver_reads_normalized_metrics) {
  namespace asio = boost::asio;
  using tcp = asio::ip::tcp;
  asio::io_context context;
  tcp::acceptor acceptor(
      context, tcp::endpoint(asio::ip::make_address_v4("127.0.0.1"), 0U));
  const std::vector<std::string> responses = {
      JsonRpcResult(
          "{\"status\":\"OK\",\"height\":12,\"target_height\":15,"
          "\"difficulty\":120,\"target\":120,"
          "\"outgoing_connections_count\":1,"
          "\"incoming_connections_count\":1,\"nettype\":\"fakechain\","
          "\"top_block_hash\":\"" +
          std::string(kHashA) +
          "\",\"wide_cumulative_difficulty\":\"0x000abc\","
          "\"busy_syncing\":true,\"version\":\"0.18.3.4\","
          "\"synchronized\":false}"),
      JsonRpcResult("{\"status\":\"OK\",\"connections\":["
                    "{\"address\":\"10.210.1.6:18080\",\"state\":"
                    "\"synchronizing\"},"
                    "{\"address\":\"10.210.1.10:18080\",\"state\":"
                    "\"normal\"}]}"),
      R"({"status":"OK","pool_stats":{"txs_total":3,"bytes_total":450}})",
      JsonRpcResult("{\"status\":\"OK\",\"version\":196624,\"release\":true,"
                    "\"current_height\":12,\"target_height\":15}"),
      JsonRpcResult("{\"status\":\"OK\",\"block_header\":{\"major_version\":16,"
                    "\"timestamp\":1700000000,\"hash\":\"" +
                    std::string(kHashA) + "\"}}")};
  std::future<std::vector<RecordedRequest>> served =
      std::async(std::launch::async,
                 [&] { return ServeDigestResponses(acceptor, responses); });

  bbp::ChainNodeConfig config = TestConfig(acceptor.local_endpoint().port());
  config.rpc_host = "127.0.0.1";
  const bbp::MoneroDriver driver(std::chrono::seconds(1));
  const bbp::ChainMetrics metrics = driver.ReadMetrics(config);
  const std::vector<RecordedRequest> requests = served.get();

  BOOST_TEST(metrics.version == 196624U);
  BOOST_TEST(metrics.protocol_version == 16U);
  BOOST_TEST(metrics.subversion == "0.18.3.4");
  BOOST_TEST(metrics.height == 12U);
  BOOST_REQUIRE(metrics.headers.has_value());
  BOOST_TEST(*metrics.headers == 15U);
  BOOST_TEST(metrics.best_hash == kHashA);
  BOOST_TEST(metrics.peer_count == 2U);
  BOOST_REQUIRE_EQUAL(metrics.peer_addresses.size(), 2U);
  BOOST_TEST(metrics.mempool_tx_count == 3U);
  BOOST_TEST(metrics.mempool_bytes == 450U);
  BOOST_REQUIRE(metrics.initial_block_download.has_value());
  BOOST_TEST(*metrics.initial_block_download);
  BOOST_CHECK(metrics.sync_status == bbp::ChainSyncStatus::kSyncing);
  BOOST_REQUIRE(metrics.verification_progress.has_value());
  BOOST_TEST(*metrics.verification_progress == 0.8);
  BOOST_REQUIRE(metrics.difficulty.has_value());
  BOOST_TEST(*metrics.difficulty == 120.0);
  BOOST_REQUIRE(metrics.hashrate_estimate.has_value());
  BOOST_TEST(*metrics.hashrate_estimate == 1.0);
  BOOST_REQUIRE(metrics.last_block_time.has_value());
  BOOST_TEST(*metrics.last_block_time == 1700000000U);
  BOOST_REQUIRE(metrics.chainwork.has_value());
  BOOST_TEST(*metrics.chainwork == "0x000abc");
  BOOST_REQUIRE_EQUAL(requests.size(), 5U);
  BOOST_TEST(requests[0].path == "/json_rpc");
  BOOST_TEST(requests[0].body.as_object().at("method").as_string() ==
             "get_info");
  BOOST_TEST(requests[1].body.as_object().at("method").as_string() ==
             "get_connections");
  BOOST_TEST(requests[2].path == "/get_transaction_pool_stats");
  BOOST_TEST(requests[3].body.as_object().at("method").as_string() ==
             "get_version");
  BOOST_TEST(requests[4].body.as_object().at("method").as_string() ==
             "get_last_block_header");
}

BOOST_AUTO_TEST_CASE(monero_driver_generates_and_reads_blocks) {
  namespace asio = boost::asio;
  using tcp = asio::ip::tcp;
  asio::io_context context;
  tcp::acceptor acceptor(
      context, tcp::endpoint(asio::ip::make_address_v4("127.0.0.1"), 0U));
  const std::vector<std::string> responses = {
      JsonRpcResult("{\"status\":\"OK\",\"height\":3,\"blocks\":[\"" +
                    std::string(kHashA) + "\",\"" + std::string(kHashB) +
                    "\"]}"),
      JsonRpcResult(
          "{\"status\":\"OK\",\"block_header\":{\"height\":3,\"hash\":\"" +
          std::string(kHashB) + "\"}}"),
      JsonRpcResult("{\"status\":\"OK\",\"tx_hashes\":[\"" +
                    std::string(kHashB) + "\",\"" + std::string(kHashC) +
                    "\"]}")};
  std::future<std::vector<RecordedRequest>> served =
      std::async(std::launch::async,
                 [&] { return ServeDigestResponses(acceptor, responses); });
  bbp::ChainNodeConfig config = TestConfig(acceptor.local_endpoint().port());
  config.rpc_host = "127.0.0.1";
  const bbp::MoneroDriver driver(std::chrono::seconds(1));

  const std::vector<std::string> hashes =
      driver.GenerateBlocks(config, 2U, std::string(kMiningAddress));
  const std::uint64_t transactions =
      driver.ReadBlockNonRewardTransactionCount(config, std::string(kHashA));
  const std::vector<RecordedRequest> requests = served.get();

  BOOST_REQUIRE_EQUAL(hashes.size(), 2U);
  BOOST_TEST(transactions == 2U);
  const boost::json::object& generate_params =
      requests[0].body.as_object().at("params").as_object();
  BOOST_TEST(
      generate_params.at("amount_of_blocks").to_number<std::uint64_t>() == 2U);
  BOOST_TEST(generate_params.at("wallet_address").as_string() ==
             kMiningAddress);
  BOOST_TEST(generate_params.at("starting_nonce").to_number<std::uint64_t>() ==
             0U);
  BOOST_TEST(requests[1].body.as_object().at("method").as_string() ==
             "get_block_header_by_hash");
  const boost::json::object& header_params =
      requests[1].body.as_object().at("params").as_object();
  BOOST_TEST(header_params.at("hash").as_string() == kHashB);
  BOOST_TEST(!header_params.at("fill_pow_hash").as_bool());
  BOOST_TEST(requests[2].body.as_object().at("method").as_string() ==
             "get_block");
}

BOOST_AUTO_TEST_CASE(monero_driver_rejects_inexact_generated_block_results) {
  const bbp::MoneroDriver driver(std::chrono::seconds(1));
  BOOST_CHECK_THROW(
      driver.GenerateBlocks(TestConfig(), 0U, std::string(kMiningAddress)),
      std::runtime_error);
  BOOST_CHECK_THROW(
      GenerateBlocksResponse(
          {JsonRpcResult("{\"status\":\"OK\",\"height\":3}")}, 2U),
      std::runtime_error);
  BOOST_CHECK_THROW(
      GenerateBlocksResponse(
          {JsonRpcResult("{\"status\":\"OK\",\"height\":3,\"blocks\":[\"" +
                         std::string(kHashA) + "\"]}")},
          2U),
      std::runtime_error);
  BOOST_CHECK_THROW(
      GenerateBlocksResponse(
          {JsonRpcResult("{\"status\":\"OK\",\"height\":3,\"blocks\":[\"" +
                         std::string(kHashA) + "\",\"" + std::string(kHashA) +
                         "\"]}")},
          2U),
      std::runtime_error);
  BOOST_CHECK_THROW(
      GenerateBlocksResponse(
          {JsonRpcResult("{\"status\":\"OK\",\"height\":3,\"blocks\":[\"" +
                         std::string(kHashA) + "\",\"" + std::string(kHashB) +
                         "\",\"" + std::string(kHashC) + "\"]}")},
          2U),
      std::runtime_error);
  BOOST_CHECK_THROW(
      GenerateBlocksResponse(
          {JsonRpcResult(R"({"status":"OK","height":3,"blocks":{}})")}, 2U),
      std::runtime_error);
  BOOST_CHECK_THROW(
      GenerateBlocksResponse(
          {JsonRpcResult("{\"status\":\"OK\",\"height\":3,\"blocks\":[\"" +
                         std::string(kHashA) + "\",7]}")},
          2U),
      std::runtime_error);
  BOOST_CHECK_THROW(
      GenerateBlocksResponse(
          {JsonRpcResult("{\"status\":\"OK\",\"height\":3,\"blocks\":[\"" +
                         std::string(kHashA) + "\",\"not-a-hash\"]}")},
          2U),
      std::runtime_error);
  BOOST_CHECK_THROW(GenerateBlocksResponse(
                        {JsonRpcResult("{\"status\":\"OK\",\"blocks\":[\"" +
                                       std::string(kHashA) + "\"]}")},
                        1U),
                    std::runtime_error);
  BOOST_CHECK_THROW(
      GenerateBlocksResponse(
          {JsonRpcResult("{\"status\":\"OK\",\"height\":-1,\"blocks\":[\"" +
                         std::string(kHashA) + "\"]}")},
          1U),
      std::runtime_error);
  BOOST_CHECK_THROW(
      GenerateBlocksResponse(
          {JsonRpcResult("{\"status\":\"OK\",\"height\":\"3\",\"blocks\":[\"" +
                         std::string(kHashA) + "\"]}")},
          1U),
      std::runtime_error);

  const std::string valid_generate =
      JsonRpcResult("{\"status\":\"OK\",\"height\":3,\"blocks\":[\"" +
                    std::string(kHashA) + "\"]}");
  BOOST_CHECK_THROW(
      GenerateBlocksResponse(
          {valid_generate, JsonRpcResult(R"({"status":"OK"})")}, 1U),
      std::runtime_error);
  BOOST_CHECK_THROW(
      GenerateBlocksResponse(
          {valid_generate,
           JsonRpcResult(R"({"status":"OK","block_header":"not-an-object"})")},
          1U),
      std::runtime_error);
  BOOST_CHECK_THROW(
      GenerateBlocksResponse(
          {valid_generate,
           JsonRpcResult(
               "{\"status\":\"OK\",\"block_header\":{\"height\":3,\"hash\":\"" +
               std::string(kHashB) + "\"}}")},
          1U),
      std::runtime_error);
  BOOST_CHECK_THROW(
      GenerateBlocksResponse(
          {valid_generate,
           JsonRpcResult(
               "{\"status\":\"OK\",\"block_header\":{\"height\":4,\"hash\":\"" +
               std::string(kHashA) + "\"}}")},
          1U),
      std::runtime_error);
}

BOOST_AUTO_TEST_CASE(monero_driver_bans_and_unbans_startup_peer) {
  namespace asio = boost::asio;
  using tcp = asio::ip::tcp;
  asio::io_context context;
  tcp::acceptor acceptor(
      context, tcp::endpoint(asio::ip::make_address_v4("127.0.0.1"), 0U));
  const std::vector<std::string> responses = {
      JsonRpcResult("{\"status\":\"OK\",\"connections\":[]}"),
      JsonRpcResult("{\"status\":\"OK\"}"),
      JsonRpcResult("{\"status\":\"OK\",\"connections\":[{\"address\":"
                    "\"10.210.1.6:18080\",\"state\":\"normal\"}]}"),
      JsonRpcResult("{\"status\":\"OK\",\"connections\":[{\"address\":"
                    "\"10.210.1.6:18080\",\"state\":\"normal\"}]}"),
      JsonRpcResult("{\"status\":\"OK\"}"),
      JsonRpcResult("{\"status\":\"OK\",\"connections\":[]}")};
  std::future<std::vector<RecordedRequest>> served =
      std::async(std::launch::async,
                 [&] { return ServeDigestResponses(acceptor, responses); });
  bbp::ChainNodeConfig config = TestConfig(acceptor.local_endpoint().port());
  config.rpc_host = "127.0.0.1";
  const bbp::MoneroDriver driver(std::chrono::seconds(1));

  driver.ConnectPeer(config, "10.210.1.6:18080");
  driver.WaitForPeerAddress(config, "10.210.1.6:18080",
                            std::chrono::seconds(1));
  driver.DisconnectPeer(config, "10.210.1.6:18080");
  driver.WaitForPeerAddressAbsent(config, "10.210.1.6:18080",
                                  std::chrono::seconds(1));
  const std::vector<RecordedRequest> requests = served.get();

  BOOST_REQUIRE_EQUAL(requests.size(), 6U);
  const boost::json::object& unban = requests[1]
                                         .body.as_object()
                                         .at("params")
                                         .as_object()
                                         .at("bans")
                                         .as_array()
                                         .front()
                                         .as_object();
  BOOST_TEST(!unban.at("ban").as_bool());
  BOOST_TEST(unban.at("host").as_string() == "10.210.1.6");
  const boost::json::object& ban = requests[4]
                                       .body.as_object()
                                       .at("params")
                                       .as_object()
                                       .at("bans")
                                       .as_array()
                                       .front()
                                       .as_object();
  BOOST_TEST(ban.at("ban").as_bool());
  BOOST_TEST(ban.at("seconds").to_number<std::uint64_t>() == 3600U);
}

BOOST_AUTO_TEST_CASE(monero_driver_changes_peer_limits_with_exact_readback) {
  const NetworkControlResult disabled = RunNetworkControl(
      {
          PeerLimitResponse("out_peers", 7U),
          PeerLimitResponse("in_peers", 5U),
          PeerLimitResponse("out_peers", 0U),
          PeerLimitResponse("in_peers", 0U),
      },
      false);
  BOOST_REQUIRE(!disabled.error.has_value());
  BOOST_REQUIRE_EQUAL(disabled.requests.size(), 4U);
  CheckPeerLimitRequest(disabled.requests[0], "/out_peers", "out_peers", false,
                        0U);
  CheckPeerLimitRequest(disabled.requests[1], "/in_peers", "in_peers", false,
                        0U);
  CheckPeerLimitRequest(disabled.requests[2], "/out_peers", "out_peers", true,
                        0U);
  CheckPeerLimitRequest(disabled.requests[3], "/in_peers", "in_peers", true,
                        0U);

  const NetworkControlResult enabled = RunNetworkControl(
      {
          PeerLimitResponse("out_peers", 0U),
          PeerLimitResponse("in_peers", 0U),
          PeerLimitResponse("out_peers", 16U),
          PeerLimitResponse("in_peers", 16U),
      },
      true);
  BOOST_REQUIRE(!enabled.error.has_value());
  BOOST_REQUIRE_EQUAL(enabled.requests.size(), 4U);
  CheckPeerLimitRequest(enabled.requests[2], "/out_peers", "out_peers", true,
                        16U);
  CheckPeerLimitRequest(enabled.requests[3], "/in_peers", "in_peers", true,
                        16U);
}

BOOST_AUTO_TEST_CASE(monero_driver_restores_both_limits_after_first_failure) {
  const NetworkControlResult result = RunNetworkControl(
      {
          PeerLimitResponse("out_peers", 7U),
          PeerLimitResponse("in_peers", 5U),
          PeerLimitResponse("out_peers", 0U, "BUSY"),
          PeerLimitResponse("in_peers", 5U),
          PeerLimitResponse("out_peers", 7U),
      },
      false);
  BOOST_REQUIRE(result.error.has_value());
  BOOST_TEST(result.error->find("BUSY") != std::string::npos);
  BOOST_TEST(result.error->find("rollback failed") == std::string::npos);
  BOOST_REQUIRE_EQUAL(result.requests.size(), 5U);
  CheckPeerLimitRequest(result.requests[2], "/out_peers", "out_peers", true,
                        0U);
  CheckPeerLimitRequest(result.requests[3], "/in_peers", "in_peers", true, 5U);
  CheckPeerLimitRequest(result.requests[4], "/out_peers", "out_peers", true,
                        7U);
}

BOOST_AUTO_TEST_CASE(monero_driver_restores_exact_limits_in_reverse_order) {
  const NetworkControlResult result = RunNetworkControl(
      {
          PeerLimitResponse("out_peers", 7U),
          PeerLimitResponse("in_peers", 5U),
          PeerLimitResponse("out_peers", 0U),
          PeerLimitResponse("in_peers", 4U),
          PeerLimitResponse("in_peers", 5U),
          PeerLimitResponse("out_peers", 7U),
      },
      false);
  BOOST_REQUIRE(result.error.has_value());
  BOOST_TEST(result.error->find("did not apply") != std::string::npos);
  BOOST_TEST(result.error->find("rollback failed") == std::string::npos);
  BOOST_REQUIRE_EQUAL(result.requests.size(), 6U);
  CheckPeerLimitRequest(result.requests[3], "/in_peers", "in_peers", true, 0U);
  CheckPeerLimitRequest(result.requests[4], "/in_peers", "in_peers", true, 5U);
  CheckPeerLimitRequest(result.requests[5], "/out_peers", "out_peers", true,
                        7U);
}

BOOST_AUTO_TEST_CASE(monero_driver_surfaces_original_and_rollback_failures) {
  const NetworkControlResult result = RunNetworkControl(
      {
          PeerLimitResponse("out_peers", 7U),
          PeerLimitResponse("in_peers", 5U),
          PeerLimitResponse("out_peers", 0U),
          PeerLimitResponse("in_peers", 0U, "BUSY"),
          PeerLimitResponse("in_peers", 5U, "ROLLBACK_BUSY"),
          PeerLimitResponse("out_peers", 7U),
      },
      false);
  BOOST_REQUIRE(result.error.has_value());
  BOOST_TEST(result.error->find("returned status: BUSY") != std::string::npos);
  BOOST_TEST(result.error->find("rollback failed") != std::string::npos);
  BOOST_TEST(result.error->find("ROLLBACK_BUSY") != std::string::npos);
  BOOST_REQUIRE_EQUAL(result.requests.size(), 6U);
  CheckPeerLimitRequest(result.requests[4], "/in_peers", "in_peers", true, 5U);
  CheckPeerLimitRequest(result.requests[5], "/out_peers", "out_peers", true,
                        7U);
}

BOOST_AUTO_TEST_CASE(monero_driver_rejects_malformed_limit_readback) {
  const NetworkControlResult result =
      RunNetworkControl({R"({"status":"OK","out_peers":"seven"})"}, false);
  BOOST_REQUIRE(result.error.has_value());
  BOOST_TEST(result.error->find("not uint64") != std::string::npos);
  BOOST_REQUIRE_EQUAL(result.requests.size(), 1U);
  CheckPeerLimitRequest(result.requests[0], "/out_peers", "out_peers", false,
                        0U);
}

BOOST_AUTO_TEST_CASE(monero_driver_observes_confirmed_transaction) {
  namespace asio = boost::asio;
  using tcp = asio::ip::tcp;
  asio::io_context context;
  tcp::acceptor acceptor(
      context, tcp::endpoint(asio::ip::make_address_v4("127.0.0.1"), 0U));
  const std::vector<std::string> responses = {
      R"({"status":"OK","pool_stats":{"txs_total":0,"bytes_total":0}})",
      "{\"status\":\"OK\",\"txs\":[{\"tx_hash\":\"" + std::string(kHashA) +
          "\",\"in_pool\":false,\"block_height\":10,"
          "\"confirmations\":2}]}",
      JsonRpcResult("{\"status\":\"OK\",\"height\":12}"),
      JsonRpcResult("{\"status\":\"OK\",\"block_header\":{\"height\":10,"
                    "\"hash\":\"" +
                    std::string(kHashB) + "\"}}")};
  std::future<std::vector<RecordedRequest>> served =
      std::async(std::launch::async,
                 [&] { return ServeDigestResponses(acceptor, responses); });
  bbp::ChainNodeConfig config = TestConfig(acceptor.local_endpoint().port());
  config.rpc_host = "127.0.0.1";
  const bbp::MoneroDriver driver(std::chrono::seconds(1));

  const bbp::ChainTransactionObservation observation =
      driver.ObserveTransaction(config, std::string(kHashA));
  const std::vector<RecordedRequest> requests = served.get();

  BOOST_CHECK(observation.state == bbp::ChainTransactionState::kConfirmed);
  BOOST_TEST(observation.observed_height == 12U);
  BOOST_REQUIRE(observation.confirmation_height.has_value());
  BOOST_TEST(*observation.confirmation_height == 10U);
  BOOST_TEST(observation.confirmations == 2U);
  BOOST_TEST(observation.block_hash == kHashB);
  BOOST_TEST(requests[1].path == "/get_transactions");
  BOOST_TEST(requests[3].body.as_object().at("method").as_string() ==
             "get_block_header_by_height");
}

BOOST_AUTO_TEST_CASE(monero_driver_accepts_omitted_empty_rpc_arrays) {
  namespace asio = boost::asio;
  using tcp = asio::ip::tcp;
  asio::io_context context;
  tcp::acceptor acceptor(
      context, tcp::endpoint(asio::ip::make_address_v4("127.0.0.1"), 0U));
  const std::vector<std::string> responses = {
      JsonRpcResult("{\"status\":\"OK\"}"),
      JsonRpcResult("{\"status\":\"OK\"}"),
      R"({"status":"OK","pool_stats":{"txs_total":0,"bytes_total":0}})",
      "{\"status\":\"OK\",\"missed_tx\":[\"" + std::string(kHashA) + "\"]}",
      JsonRpcResult("{\"status\":\"OK\",\"height\":1}")};
  std::future<std::vector<RecordedRequest>> served =
      std::async(std::launch::async,
                 [&] { return ServeDigestResponses(acceptor, responses); });
  bbp::ChainNodeConfig config = TestConfig(acceptor.local_endpoint().port());
  config.rpc_host = "127.0.0.1";
  const bbp::MoneroDriver driver(std::chrono::seconds(1));

  BOOST_TEST(driver.PeerAddresses(config).empty());
  BOOST_TEST(driver.ReadBlockNonRewardTransactionCount(
                 config, std::string(kHashB)) == 0U);
  const bbp::ChainTransactionObservation observation =
      driver.ObserveTransaction(config, std::string(kHashA));
  static_cast<void>(served.get());

  BOOST_CHECK(observation.state == bbp::ChainTransactionState::kUnknown);
  BOOST_TEST(observation.observed_height == 1U);
  BOOST_TEST(observation.mempool_size == 0U);
}

BOOST_AUTO_TEST_CASE(monero_driver_rejects_malformed_peer_arrays) {
  BOOST_CHECK_THROW(PeerAddressesResponse(
                        JsonRpcResult(R"({"status":"OK","connections":{}})")),
                    std::runtime_error);
  BOOST_CHECK_THROW(PeerAddressesResponse(
                        JsonRpcResult(R"({"status":"OK","connections":[7]})")),
                    std::runtime_error);
  BOOST_CHECK_THROW(
      PeerAddressesResponse(JsonRpcResult(
          R"({"status":"OK","connections":[{"address":"not-an-endpoint","state":"normal"}]})")),
      std::runtime_error);
  BOOST_CHECK_THROW(
      PeerAddressesResponse(JsonRpcResult(
          R"({"status":"OK","connections":[{"address":"10.210.1.6:18080","state":"normal"},{"address":"10.210.1.6:18080","state":"normal"}]})")),
      std::runtime_error);
  BOOST_CHECK_THROW(
      PeerAddressesResponse(JsonRpcResult(
          R"({"status":"OK","connections":[{"address":"10.210.1.6:18080"}]})")),
      std::runtime_error);
  BOOST_CHECK_THROW(
      PeerAddressesResponse(JsonRpcResult(
          R"({"status":"OK","connections":[{"address":"10.210.1.6:18080","state":7}]})")),
      std::runtime_error);
  BOOST_CHECK_THROW(
      PeerAddressesResponse(JsonRpcResult(
          R"({"status":"OK","connections":[{"address":"10.210.1.6:18080","state":"unknown"}]})")),
      std::runtime_error);
}

BOOST_AUTO_TEST_CASE(monero_peer_readiness_requires_completed_handshake) {
  namespace asio = boost::asio;
  using tcp = asio::ip::tcp;
  asio::io_context context;
  tcp::acceptor acceptor(
      context, tcp::endpoint(asio::ip::make_address_v4("127.0.0.1"), 0U));
  const std::vector<std::string> responses = {
      JsonRpcResult(
          R"({"status":"OK","connections":[{"address":"10.210.1.6:18080","state":"before_handshake"}]})"),
      JsonRpcResult(
          R"({"status":"OK","connections":[{"address":"10.210.1.6:18080","state":"synchronizing"}]})"),
  };
  std::future<std::vector<RecordedRequest>> served =
      std::async(std::launch::async,
                 [&] { return ServeDigestResponses(acceptor, responses); });
  bbp::ChainNodeConfig config = TestConfig(acceptor.local_endpoint().port());
  config.rpc_host = "127.0.0.1";
  const bbp::MoneroDriver driver(std::chrono::seconds(1));

  driver.WaitForPeerAddress(config, "10.210.1.6:18080",
                            std::chrono::seconds(1));
  const std::vector<RecordedRequest> requests = served.get();
  BOOST_REQUIRE_EQUAL(requests.size(), 2U);
}

BOOST_AUTO_TEST_CASE(
    monero_connectivity_filters_handshakes_but_absence_observes_sockets) {
  namespace asio = boost::asio;
  using tcp = asio::ip::tcp;
  asio::io_context context;
  tcp::acceptor acceptor(
      context, tcp::endpoint(asio::ip::make_address_v4("127.0.0.1"), 0U));
  const std::vector<std::string> responses = {
      JsonRpcResult(
          R"({"status":"OK","connections":[{"address":"10.210.1.6:18080","state":"before_handshake"}]})"),
      JsonRpcResult(
          R"({"status":"OK","connections":[{"address":"10.210.1.6:18080","state":"standby"}]})"),
      JsonRpcResult(
          R"({"status":"OK","connections":[{"address":"10.210.1.6:18080","state":"before_handshake"}]})"),
      JsonRpcResult(R"({"status":"OK","connections":[]})"),
  };
  std::future<std::vector<RecordedRequest>> served =
      std::async(std::launch::async,
                 [&] { return ServeDigestResponses(acceptor, responses); });
  bbp::ChainNodeConfig config = TestConfig(acceptor.local_endpoint().port());
  config.rpc_host = "127.0.0.1";
  const bbp::MoneroDriver driver(std::chrono::seconds(1));
  const std::vector<std::string> candidates = {"10.210.1.6:18080"};

  BOOST_TEST(driver.ConnectedPeerAddresses(config, candidates).empty());
  BOOST_TEST(driver.ConnectedPeerAddresses(config, candidates) == candidates,
             boost::test_tools::per_element());
  driver.WaitForPeerAddressAbsent(config, candidates.front(),
                                  std::chrono::seconds(1));
  const std::vector<RecordedRequest> requests = served.get();
  BOOST_REQUIRE_EQUAL(requests.size(), 4U);
}

BOOST_AUTO_TEST_CASE(monero_driver_rejects_malformed_block_hash_arrays) {
  BOOST_CHECK_THROW(
      ReadBlockResponse(JsonRpcResult(R"({"status":"OK","tx_hashes":{}})")),
      std::runtime_error);
  BOOST_CHECK_THROW(
      ReadBlockResponse(JsonRpcResult(R"({"status":"OK","tx_hashes":[7]})")),
      std::runtime_error);
  BOOST_CHECK_THROW(ReadBlockResponse(JsonRpcResult(
                        R"({"status":"OK","tx_hashes":["not-a-hash"]})")),
                    std::runtime_error);
  BOOST_CHECK_THROW(
      ReadBlockResponse(JsonRpcResult("{\"status\":\"OK\",\"tx_hashes\":[\"" +
                                      std::string(kHashA) + "\",\"" +
                                      std::string(kHashA) + "\"]}")),
      std::runtime_error);
}

BOOST_AUTO_TEST_CASE(monero_driver_normalizes_empty_transaction_results) {
  const bbp::ChainTransactionObservation omitted =
      ObserveTransactionResponse(R"({"status":"OK"})");
  BOOST_CHECK(omitted.state == bbp::ChainTransactionState::kUnknown);
  BOOST_TEST(omitted.observed_height == 1U);

  const bbp::ChainTransactionObservation explicit_empty =
      ObserveTransactionResponse(R"({"status":"OK","txs":[],"missed_tx":[]})");
  BOOST_CHECK(explicit_empty.state == bbp::ChainTransactionState::kUnknown);
  BOOST_TEST(explicit_empty.observed_height == 1U);
}

BOOST_AUTO_TEST_CASE(monero_driver_rejects_conflicting_transaction_results) {
  BOOST_CHECK_THROW(
      ObserveTransactionResponse("{\"status\":\"OK\",\"missed_tx\":[\"" +
                                 std::string(kHashB) + "\"]}"),
      std::runtime_error);
  BOOST_CHECK_THROW(
      ObserveTransactionResponse("{\"status\":\"OK\",\"missed_tx\":[\"" +
                                 std::string(kHashA) + "\",\"" +
                                 std::string(kHashA) + "\"]}"),
      std::runtime_error);
  BOOST_CHECK_THROW(
      ObserveTransactionResponse("{\"status\":\"OK\",\"txs\":[{\"tx_hash\":\"" +
                                 std::string(kHashA) +
                                 "\",\"in_pool\":true}],\"missed_tx\":[\"" +
                                 std::string(kHashA) + "\"]}"),
      std::runtime_error);
  BOOST_CHECK_THROW(
      ObserveTransactionResponse(
          "{\"status\":\"OK\",\"txs\":[{\"tx_hash\":\"" + std::string(kHashA) +
          "\",\"in_pool\":true},{\"tx_hash\":\"" + std::string(kHashB) +
          "\",\"in_pool\":true}]}"),
      std::runtime_error);
  BOOST_CHECK_THROW(
      ObserveTransactionResponse(R"({"status":"OK","txs":{},"missed_tx":[]})"),
      std::runtime_error);
  BOOST_CHECK_THROW(
      ObserveTransactionResponse(R"({"status":"OK","txs":[],"missed_tx":{}})"),
      std::runtime_error);
  BOOST_CHECK_THROW(
      ObserveTransactionResponse(R"({"status":"OK","txs":[],"missed_tx":[7]})"),
      std::runtime_error);
  BOOST_CHECK_THROW(
      ObserveTransactionResponse(
          R"({"status":"OK","txs":[],"missed_tx":["not-a-hash"]})"),
      std::runtime_error);
  BOOST_CHECK_THROW(
      ObserveTransactionResponse(R"({"status":"OK","txs":[7],"missed_tx":[]})"),
      std::runtime_error);
  BOOST_CHECK_THROW(ObserveTransactionResponse(
                        R"({"status":"OK","txs":[{}],"missed_tx":[]})"),
                    std::runtime_error);
  BOOST_CHECK_THROW(
      ObserveTransactionResponse("{\"status\":\"OK\",\"txs\":[{\"tx_hash\":\"" +
                                 std::string(kHashA) +
                                 "\",\"in_pool\":\"yes\"}],\"missed_tx\":[]}"),
      std::runtime_error);
}

BOOST_AUTO_TEST_CASE(monero_rpc_wait_honors_stop_while_server_is_silent) {
  namespace asio = boost::asio;
  namespace beast = boost::beast;
  namespace http = beast::http;
  using tcp = asio::ip::tcp;

  asio::io_context server_context;
  tcp::acceptor acceptor(
      server_context,
      tcp::endpoint(asio::ip::make_address_v4("127.0.0.1"), 0U));
  std::promise<void> request_received;
  std::future<void> request_seen = request_received.get_future();
  std::mutex server_mutex;
  std::condition_variable_any server_wakeup;
  std::jthread server([&](std::stop_token stop_token) {
    tcp::socket socket(server_context);
    boost::system::error_code accept_error;
    acceptor.accept(socket, accept_error);
    if (accept_error) {
      return;
    }
    beast::flat_buffer buffer;
    http::request<http::string_body> request;
    boost::system::error_code read_error;
    http::read(socket, buffer, request, read_error);
    if (read_error) {
      return;
    }
    request_received.set_value();
    std::unique_lock<std::mutex> lock(server_mutex);
    server_wakeup.wait(lock, stop_token, [] { return false; });
  });

  bbp::ChainNodeConfig config = TestConfig(acceptor.local_endpoint().port());
  config.rpc_host = "127.0.0.1";
  const bbp::MoneroDriver driver(std::chrono::seconds(30));
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

  const bool received = request_seen.wait_for(std::chrono::seconds(1)) ==
                        std::future_status::ready;
  const auto cancellation_started = std::chrono::steady_clock::now();
  stop_source.request_stop();
  waiter.join();
  const auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(
      std::chrono::steady_clock::now() - cancellation_started);
  server.request_stop();
  server_wakeup.notify_all();
  if (!received) {
    boost::system::error_code close_error;
    acceptor.close(close_error);
  }
  server.join();

  BOOST_TEST(received);
  BOOST_TEST(!static_cast<bool>(failure));
  BOOST_TEST(cancelled.load());
  BOOST_TEST(elapsed.count() < 500);
}

BOOST_AUTO_TEST_CASE(
    monero_driver_reports_supported_boundaries_and_cancellation) {
  const bbp::MoneroDriver driver(std::chrono::seconds(1));
  BOOST_CHECK_EXCEPTION(
      driver.CreateWalletAddress(TestConfig(), bbp::ChainWalletMode::kPublic),
      bbp::UnsupportedChainOperation, [](const std::runtime_error& error) {
        return std::string(error.what()).find("Monero") != std::string::npos;
      });
  BOOST_CHECK_THROW(driver.ConnectPeer(TestConfig(), "10.210.1.10:18080"),
                    bbp::UnsupportedChainOperation);

  std::stop_source stopped;
  stopped.request_stop();
  BOOST_CHECK_THROW(driver.WaitReady(TestConfig(), std::chrono::seconds(1),
                                     stopped.get_token()),
                    bbp::SimulationCancelled);
}
