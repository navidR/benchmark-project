#include <unistd.h>

#include <boost/asio/ip/address.hpp>
#include <boost/asio/ip/tcp.hpp>
#include <boost/beast/core/flat_buffer.hpp>
#include <boost/beast/core/tcp_stream.hpp>
#include <boost/beast/http/read.hpp>
#include <boost/beast/http/write.hpp>
#include <boost/json/object.hpp>
#include <boost/json/parse.hpp>
#include <boost/json/serialize.hpp>
#include <boost/test/unit_test.hpp>
#include <chrono>
#include <cstdlib>
#include <filesystem>
#include <memory>
#include <stdexcept>
#include <string>
#include <string_view>
#include <thread>
#include <vector>

#include "bbp/mcp_endpoint.h"
#include "bbp/mcp_live_application.h"
#include "bbp/mcp_registry.h"
#include "bbp/scenario_service.h"
#include "bbp/simulation_command_queue.h"
#include "bbp/util.h"

namespace bbp {
namespace {

namespace asio = boost::asio;
namespace beast = boost::beast;
namespace http = beast::http;
using tcp = asio::ip::tcp;

class EndpointTestDirectory {
 public:
  EndpointTestDirectory() {
    std::string pattern = (std::filesystem::temp_directory_path() /
                           "bbp-mcp-endpoint-test-XXXXXX")
                              .string();
    std::vector<char> writable(pattern.begin(), pattern.end());
    writable.push_back('\0');
    char* created = mkdtemp(writable.data());
    if (created == nullptr) {
      throw std::runtime_error("mkdtemp failed for MCP endpoint test");
    }
    path_ = created;
  }

  ~EndpointTestDirectory() { std::filesystem::remove_all(path_); }

  const std::filesystem::path& path() const { return path_; }

 private:
  std::filesystem::path path_;
};

http::response<http::string_body> Exchange(
    std::uint16_t port, std::string_view token, http::verb method,
    std::string body, std::string_view session_id = {},
    std::string_view accept = "application/json, text/event-stream") {
  asio::io_context io_context;
  beast::tcp_stream stream(io_context);
  stream.connect(tcp::endpoint(asio::ip::address_v4::loopback(), port));
  http::request<http::string_body> request{method, "/mcp", 11};
  request.set(http::field::host, "127.0.0.1:" + std::to_string(port));
  request.set(http::field::origin,
              "http://127.0.0.1:" + std::to_string(port));
  request.set(http::field::content_type, "application/json");
  request.set(http::field::accept, accept);
  request.set(http::field::authorization, "Bearer " + std::string(token));
  if (!session_id.empty()) {
    request.set("Mcp-Session-Id", session_id);
    request.set("MCP-Protocol-Version", kMcpProtocolVersion);
  }
  request.body() = std::move(body);
  request.prepare_payload();
  http::write(stream, request);
  beast::flat_buffer buffer;
  http::response<http::string_body> response;
  http::read(stream, buffer, response);
  return response;
}

http::response<http::string_body> Initialize(std::uint16_t port,
                                             std::string_view token) {
  return Exchange(
      port, token, http::verb::post,
      boost::json::serialize(boost::json::object{
      {"jsonrpc", "2.0"},
      {"id", 1U},
      {"method", "initialize"},
      {"params",
       boost::json::object{
           {"protocolVersion", kMcpProtocolVersion},
           {"capabilities", boost::json::object{}},
      {"clientInfo", boost::json::object{{"name", "endpoint-test"},
                                              {"version", "1"}}}}}}));
}

boost::json::object EndpointLiveScenario() {
  return boost::json::object{
      {"chain", "firo"},
      {"chain_daemon", "/bin/true"},
      {"run_id", "endpoint-live-run"},
      {"nodes", 1U},
      {"block_production", boost::json::object{{"enabled", false}}}};
}

}  // namespace

BOOST_AUTO_TEST_CASE(
    mcp_endpoint_publishes_usable_client_configuration_and_cleans_credentials) {
  EndpointTestDirectory temporary;
  const std::filesystem::path state_directory = temporary.path();
  McpEndpoint endpoint(McpEndpointConfig{.state_directory = state_directory,
                                         .run_id = "endpoint-test",
                                         .server = {},
                                         .dispatcher = {},
                                         .allowed_operations = {},
                                         .allowed_information_families = {},
                                         .read_only = false});
  endpoint.Start();
  BOOST_REQUIRE(endpoint.running());
  const McpEndpointPublication publication = endpoint.publication();
  BOOST_TEST(publication.endpoint.starts_with("http://127.0.0.1:"));
  BOOST_TEST(publication.endpoint.ends_with("/mcp"));

  const std::string token_with_newline = ReadText(publication.token_file);
  BOOST_REQUIRE_EQUAL(token_with_newline.size(), 65U);
  BOOST_TEST(token_with_newline.back() == '\n');
  const std::string token = token_with_newline.substr(0U, 64U);
  const boost::json::value parsed =
      boost::json::parse(ReadText(publication.client_config_file));
  BOOST_REQUIRE(parsed.is_object());
  const boost::json::object& config = parsed.as_object();
  BOOST_TEST(config.at("endpoint").as_string() == publication.endpoint);
  BOOST_TEST(config.at("protocol_version").as_string() == kMcpProtocolVersion);
  const boost::json::array& supported_versions =
      config.at("supported_protocol_versions").as_array();
  BOOST_REQUIRE_EQUAL(supported_versions.size(), 2U);
  BOOST_TEST(supported_versions[0].as_string() == "2025-11-25");
  BOOST_TEST(supported_versions[1].as_string() == "2025-06-18");
  BOOST_REQUIRE_EQUAL(supported_versions.size(),
                      kMcpSupportedProtocolVersions.size());
  for (std::size_t index = 0U; index < supported_versions.size(); ++index) {
    BOOST_TEST(supported_versions[index].as_string() ==
               kMcpSupportedProtocolVersions[index]);
  }
  BOOST_TEST(config.at("authentication").as_object().at("token").as_string() ==
             token);
  const std::string_view codex_config =
      config.at("codex_config_toml").as_string();
  BOOST_TEST(codex_config.find("[mcp_servers.bbp]") != std::string_view::npos);
  BOOST_TEST(config.at("opencode_config")
                 .as_object()
                 .at("mcp")
                 .as_object()
                 .at("bbp")
                 .as_object()
                 .at("type")
                 .as_string() == "remote");
  BOOST_CHECK((std::filesystem::status(publication.token_file).permissions() &
               (std::filesystem::perms::group_all |
                std::filesystem::perms::others_all)) ==
              std::filesystem::perms::none);
  BOOST_CHECK(
      (std::filesystem::status(publication.client_config_file).permissions() &
       (std::filesystem::perms::group_all |
        std::filesystem::perms::others_all)) == std::filesystem::perms::none);

  BOOST_REQUIRE_NE(publication.port, 0U);
  BOOST_TEST(Initialize(publication.port, token).result() == http::status::ok);

  endpoint.StopAdmissionAndDrain();
  BOOST_TEST(!endpoint.running());
  BOOST_TEST(std::filesystem::is_regular_file(publication.token_file));
  BOOST_TEST(std::filesystem::is_regular_file(publication.client_config_file));
  BOOST_TEST(endpoint.DispatcherStats().active_workers == 0U);
  BOOST_CHECK_THROW(endpoint.Start(), std::runtime_error);

  endpoint.Stop();
  BOOST_TEST(!endpoint.running());
  BOOST_TEST(!std::filesystem::exists(publication.token_file));
  BOOST_TEST(!std::filesystem::exists(publication.client_config_file));
  BOOST_TEST(
      std::filesystem::is_directory(state_directory / kMcpEndpointDirectory));
}

BOOST_AUTO_TEST_CASE(
    mcp_endpoint_forwards_typed_operation_updates_to_authenticated_sse) {
  EndpointTestDirectory temporary;
  McpEndpoint endpoint(McpEndpointConfig{
      .state_directory = temporary.path(),
      .run_id = "endpoint-notification-test",
      .server = {},
      .dispatcher = {},
      .allowed_operations = {McpOperationKind::kValidateScenario,
                             McpOperationKind::kGetOperation,
                             McpOperationKind::kCancelOperation},
      .allowed_information_families = {},
      .read_only = false});
  endpoint.Start();
  const McpEndpointPublication publication = endpoint.publication();
  const std::string token_with_newline = ReadText(publication.token_file);
  const std::string token =
      token_with_newline.substr(0U, token_with_newline.size() - 1U);

  const auto initialized = Initialize(publication.port, token);
  BOOST_REQUIRE(initialized.result() == http::status::ok);
  const std::string session_id(initialized.at("Mcp-Session-Id"));
  const auto marked = Exchange(
      publication.port, token, http::verb::post,
      boost::json::serialize(
          boost::json::object{{"jsonrpc", "2.0"},
                              {"method", "notifications/initialized"},
                              {"params", boost::json::object{}}}),
      session_id);
  BOOST_REQUIRE(marked.result() == http::status::accepted);

  const auto submitted = Exchange(
      publication.port, token, http::verb::post,
      boost::json::serialize(boost::json::object{
          {"jsonrpc", "2.0"},
          {"id", 2U},
          {"method", "tools/call"},
          {"params",
           boost::json::object{
               {"name", "scenario.validate"},
               {"arguments",
                boost::json::object{{"scenario", boost::json::object{}}}}}}}),
      session_id);
  BOOST_REQUIRE(submitted.result() == http::status::ok);
  const std::string operation_id(
      boost::json::parse(submitted.body())
          .as_object()
          .at("result")
          .as_object()
          .at("structuredContent")
          .as_object()
          .at("operation_id")
          .as_string());

  std::string terminal_state;
  const auto deadline =
      std::chrono::steady_clock::now() + std::chrono::seconds(2);
  std::uint64_t request_id = 3U;
  while (std::chrono::steady_clock::now() < deadline) {
    const auto operation = Exchange(
        publication.port, token, http::verb::post,
        boost::json::serialize(boost::json::object{
            {"jsonrpc", "2.0"},
            {"id", request_id++},
            {"method", "tools/call"},
            {"params",
             boost::json::object{
                 {"name", "operation.get"},
                 {"arguments",
                  boost::json::object{{"operation_id", operation_id}}}}}}),
        session_id);
    BOOST_REQUIRE(operation.result() == http::status::ok);
    terminal_state =
        boost::json::parse(operation.body())
            .as_object()
            .at("result")
            .as_object()
            .at("structuredContent")
            .as_object()
            .at("state")
            .as_string();
    if (terminal_state == "succeeded") {
      break;
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(1));
  }
  BOOST_REQUIRE(terminal_state == "succeeded");

  const auto events =
      Exchange(publication.port, token, http::verb::get, {}, session_id,
               "text/event-stream");
  BOOST_REQUIRE(events.result() == http::status::ok);
  BOOST_TEST(events.body().find(
                 "\"method\":\"bbp/notifications/operation_updated\"") !=
             std::string::npos);
  BOOST_TEST(events.body().find("\"operation_id\":\"" + operation_id + "\"") !=
             std::string::npos);
  BOOST_TEST(events.body().find("\"state\":\"succeeded\"") !=
             std::string::npos);
  BOOST_TEST(endpoint.ProtocolStats().notifications_enqueued >= 2U);

  endpoint.StopAdmissionAndDrain();
  endpoint.Stop();
  BOOST_TEST(!std::filesystem::exists(publication.token_file));
  BOOST_TEST(!std::filesystem::exists(publication.client_config_file));
}

BOOST_AUTO_TEST_CASE(
    mcp_endpoint_publishes_authoritative_run_scoped_lifecycle_evidence) {
  EndpointTestDirectory temporary;
  const auto options =
      std::make_shared<Options>(ParseAndValidateScenario(EndpointLiveScenario()));
  auto command_queue = std::make_shared<SimulationCommandQueue>();
  McpEndpoint* endpoint_pointer = nullptr;
  McpLiveApplication application(McpLiveApplication::Config{
      .run_id = "endpoint-live-run",
      .run_root = temporary.path(),
      .retained_run = std::nullopt,
      .options = options,
      .command_queue = command_queue,
      .request_run_stop = [] {},
      .run_started = {},
      .run_stopping = {},
      .run_stopped = {},
      .publish_evidence =
          [&](McpEvidenceRecord record) {
            BOOST_REQUIRE(endpoint_pointer != nullptr);
            endpoint_pointer->PublishEvidence(std::move(record));
          },
      .close_run_subscriptions =
          [&](std::string_view run_id) {
            BOOST_REQUIRE(endpoint_pointer != nullptr);
            endpoint_pointer->CloseRunSubscriptions(run_id);
          }});
  McpEndpoint endpoint(
      McpEndpointConfig{
          .state_directory = temporary.path(),
          .run_id = "endpoint-live-run",
          .server = {},
          .dispatcher = {},
          .allowed_operations = application.SupportedOperations(),
          .allowed_information_families =
              application.SupportedInformationFamilies(),
          .read_only = false},
      application.OperationFactory(), application.ResourceReader());
  endpoint_pointer = &endpoint;
  endpoint.Start();
  const McpEndpointPublication publication = endpoint.publication();
  const std::string token_with_newline = ReadText(publication.token_file);
  const std::string token =
      token_with_newline.substr(0U, token_with_newline.size() - 1U);
  const auto initialized = Initialize(publication.port, token);
  BOOST_REQUIRE(initialized.result() == http::status::ok);
  const std::string session_id(initialized.at("Mcp-Session-Id"));
  BOOST_REQUIRE(
      Exchange(
          publication.port, token, http::verb::post,
          boost::json::serialize(
              boost::json::object{{"jsonrpc", "2.0"},
                                  {"method", "notifications/initialized"},
                                  {"params", boost::json::object{}}}),
          session_id)
          .result() == http::status::accepted);

  const auto created = Exchange(
      publication.port, token, http::verb::post,
      boost::json::serialize(boost::json::object{
          {"jsonrpc", "2.0"},
          {"id", 2U},
          {"method", "tools/call"},
          {"params",
           boost::json::object{
               {"name", "subscription.create"},
               {"arguments",
                boost::json::object{
                    {"run_id", "endpoint-live-run"},
                    {"families", boost::json::array{"lifecycle"}}}}}}}),
      session_id);
  BOOST_REQUIRE(created.result() == http::status::ok);
  const boost::json::object created_result =
      boost::json::parse(created.body())
          .as_object()
          .at("result")
          .as_object()
          .at("structuredContent")
          .as_object();
  const std::string subscription_id(
      created_result.at("subscription_id").as_string());
  BOOST_TEST(created_result.at("run_id").as_string() == "endpoint-live-run");

  application.MarkRunStarted();
  application.MarkRunStopping();
  application.MarkRunStopped();
  const auto polled = Exchange(
      publication.port, token, http::verb::post,
      boost::json::serialize(boost::json::object{
          {"jsonrpc", "2.0"},
          {"id", 3U},
          {"method", "tools/call"},
          {"params",
           boost::json::object{
               {"name", "subscription.poll"},
               {"arguments",
                boost::json::object{{"subscription_id", subscription_id},
                                    {"cursor", "0"},
                                    {"limit", 8U}}}}}}),
      session_id);
  BOOST_REQUIRE(polled.result() == http::status::ok);
  const boost::json::object page =
      boost::json::parse(polled.body())
          .as_object()
          .at("result")
          .as_object()
          .at("structuredContent")
          .as_object();
  BOOST_TEST(page.at("run_id").as_string() == "endpoint-live-run");
  BOOST_TEST(!page.at("active").as_bool());
  const boost::json::array& items = page.at("items").as_array();
  BOOST_REQUIRE_EQUAL(items.size(), 3U);
  BOOST_TEST(items.front().as_object().at("kind").as_string() ==
             "run_started");
  BOOST_TEST(items.back().as_object().at("kind").as_string() ==
             "run_stopped");
  for (const boost::json::value& item : items) {
    BOOST_TEST(item.as_object().at("run_id").as_string() ==
               "endpoint-live-run");
  }

  endpoint.StopAdmissionAndDrain();
  application.Shutdown();
  endpoint.Stop();
}

BOOST_AUTO_TEST_CASE(mcp_endpoint_replaces_stale_publication_files) {
  EndpointTestDirectory temporary;
  const std::filesystem::path state_directory = temporary.path();
  const std::filesystem::path publication_directory =
      state_directory / kMcpEndpointDirectory;
  std::filesystem::create_directories(publication_directory);
  WriteText(publication_directory / kMcpTokenFile, "stale-token\n");
  WriteText(publication_directory / kMcpClientConfigFile, "stale-client\n");

  McpEndpoint endpoint(McpEndpointConfig{.state_directory = state_directory,
                                         .run_id = "endpoint-replacement-test",
                                         .server = {},
                                         .dispatcher = {},
                                         .allowed_operations = {},
                                         .allowed_information_families = {},
                                         .read_only = false});
  endpoint.Start();
  const McpEndpointPublication publication = endpoint.publication();
  BOOST_TEST(ReadText(publication.token_file) != "stale-token\n");
  BOOST_TEST(ReadText(publication.client_config_file) != "stale-client\n");
  endpoint.Stop();

  BOOST_TEST(std::filesystem::is_directory(publication_directory));
  BOOST_TEST(!std::filesystem::exists(publication.token_file));
  BOOST_TEST(!std::filesystem::exists(publication.client_config_file));
}

BOOST_AUTO_TEST_CASE(
    mcp_endpoint_removes_stale_publications_before_listener_failure) {
  EndpointTestDirectory temporary;
  const std::filesystem::path state_directory = temporary.path();
  const std::filesystem::path publication_directory =
      state_directory / kMcpEndpointDirectory;
  std::filesystem::create_directories(publication_directory);
  WriteText(publication_directory / kMcpTokenFile, "stale-token\n");
  WriteText(publication_directory / kMcpClientConfigFile, "stale-client\n");
  WriteText(publication_directory / ".token.tmp", "stale-temp-token\n");
  WriteText(publication_directory / ".client.json.tmp", "stale-temp-client\n");

  McpEndpoint endpoint(McpEndpointConfig{
      .state_directory = state_directory,
      .run_id = "endpoint-failure-test",
      .server = McpServerConfig{.bind_address = "invalid-address"},
      .dispatcher = {},
      .allowed_operations = {},
      .allowed_information_families = {},
      .read_only = false});
  BOOST_CHECK_THROW(endpoint.Start(), std::runtime_error);

  BOOST_TEST(!std::filesystem::exists(publication_directory / kMcpTokenFile));
  BOOST_TEST(
      !std::filesystem::exists(publication_directory / kMcpClientConfigFile));
  BOOST_TEST(!std::filesystem::exists(publication_directory / ".token.tmp"));
  BOOST_TEST(
      !std::filesystem::exists(publication_directory / ".client.json.tmp"));
}

BOOST_AUTO_TEST_CASE(mcp_endpoint_refuses_a_replaced_cleanup_path) {
  EndpointTestDirectory temporary;
  const std::filesystem::path state_directory = temporary.path();
  const std::filesystem::path publication_directory =
      state_directory / kMcpEndpointDirectory;
  const std::filesystem::path displaced_directory =
      state_directory / "mcp-displaced";
  McpEndpoint endpoint(McpEndpointConfig{.state_directory = state_directory,
                                         .run_id = "endpoint-replaced-test",
                                         .server = {},
                                         .dispatcher = {},
                                         .allowed_operations = {},
                                         .allowed_information_families = {},
                                         .read_only = false});
  endpoint.Start();

  std::filesystem::rename(publication_directory, displaced_directory);
  std::filesystem::create_directory(publication_directory);
  BOOST_CHECK_THROW(endpoint.Stop(), std::runtime_error);
  BOOST_TEST(std::filesystem::is_directory(publication_directory));

  BOOST_REQUIRE(std::filesystem::remove(publication_directory));
  std::filesystem::rename(displaced_directory, publication_directory);
  endpoint.Stop();
  BOOST_TEST(std::filesystem::is_directory(publication_directory));
}

}  // namespace bbp
