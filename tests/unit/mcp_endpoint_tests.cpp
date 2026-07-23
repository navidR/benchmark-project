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
#include <cstdlib>
#include <filesystem>
#include <stdexcept>
#include <string>
#include <string_view>
#include <vector>

#include "bbp/mcp_endpoint.h"
#include "bbp/mcp_registry.h"
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

http::response<http::string_body> Initialize(std::uint16_t port,
                                             std::string_view token) {
  asio::io_context io_context;
  beast::tcp_stream stream(io_context);
  stream.connect(tcp::endpoint(asio::ip::address_v4::loopback(), port));
  http::request<http::string_body> request{http::verb::post, "/mcp", 11};
  request.set(http::field::host, "127.0.0.1:" + std::to_string(port));
  request.set(http::field::content_type, "application/json");
  request.set(http::field::accept, "application/json, text/event-stream");
  request.set(http::field::authorization, "Bearer " + std::string(token));
  request.body() = boost::json::serialize(boost::json::object{
      {"jsonrpc", "2.0"},
      {"id", 1U},
      {"method", "initialize"},
      {"params",
       boost::json::object{
           {"protocolVersion", kMcpProtocolVersion},
           {"capabilities", boost::json::object{}},
           {"clientInfo", boost::json::object{{"name", "endpoint-test"},
                                              {"version", "1"}}}}}});
  request.prepare_payload();
  http::write(stream, request);
  beast::flat_buffer buffer;
  http::response<http::string_body> response;
  http::read(stream, buffer, response);
  return response;
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
