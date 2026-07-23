#include <atomic>
#include <boost/asio/connect.hpp>
#include <boost/asio/ip/tcp.hpp>
#include <boost/asio/write.hpp>
#include <boost/beast/core.hpp>
#include <boost/beast/http.hpp>
#include <boost/json/object.hpp>
#include <boost/json/serialize.hpp>
#include <boost/test/unit_test.hpp>
#include <chrono>
#include <string>
#include <thread>
#include <vector>

#include "bbp/mcp_registry.h"
#include "bbp/mcp_server.h"

namespace bbp {
namespace {

namespace asio = boost::asio;
namespace beast = boost::beast;
namespace http = beast::http;
using tcp = asio::ip::tcp;

constexpr std::string_view kServerToken =
    "abcdef0123456789abcdef0123456789abcdef0123456789";

http::response<http::string_body> SendRequest(
    std::uint16_t port, http::request<http::string_body> request) {
  asio::io_context io_context;
  beast::tcp_stream stream(io_context);
  stream.connect(tcp::endpoint(asio::ip::address_v4::loopback(), port));
  http::write(stream, request);
  beast::flat_buffer buffer;
  http::response<http::string_body> response;
  http::read(stream, buffer, response);
  boost::system::error_code ignored;
  stream.socket().shutdown(tcp::socket::shutdown_both, ignored);
  return response;
}

http::request<http::string_body> InitializeRequest(std::uint16_t port,
                                                   bool authenticate) {
  http::request<http::string_body> request{http::verb::post, "/mcp", 11};
  request.set(http::field::host, "127.0.0.1:" + std::to_string(port));
  request.set(http::field::content_type, "application/json");
  request.set(http::field::accept, "application/json, text/event-stream");
  request.set(http::field::origin, "http://127.0.0.1:" + std::to_string(port));
  if (authenticate) {
    request.set(http::field::authorization,
                "Bearer " + std::string(kServerToken));
  }
  request.body() = boost::json::serialize(boost::json::object{
      {"jsonrpc", "2.0"},
      {"id", 1U},
      {"method", "initialize"},
      {"params",
       boost::json::object{
           {"protocolVersion", kMcpProtocolVersion},
           {"capabilities", boost::json::object{}},
           {"clientInfo", boost::json::object{{"name", "transport-test"},
                                              {"version", "1"}}}}}});
  request.prepare_payload();
  return request;
}

McpServer MakeServer(
    std::chrono::milliseconds timeout = std::chrono::seconds(5),
    std::size_t worker_count = 2U,
    std::size_t pending_connection_capacity = 4U) {
  return McpServer(
      McpServerConfig{
          .bind_address = "127.0.0.1",
          .port = 0U,
          .worker_count = worker_count,
          .pending_connection_capacity = pending_connection_capacity,
          .request_timeout = timeout},
      McpProtocolConfig{.bearer_token = std::string(kServerToken),
                        .endpoint_path = "/mcp",
                        .endpoint_port = 0U,
                        .allowed_operations = {},
                        .allowed_information_families = {},
                        .read_only = false},
      [](std::string_view name, const boost::json::object&, std::string_view,
         std::stop_token) { return boost::json::object{{"tool", name}}; },
      [](std::string_view uri, std::string_view, std::stop_token) {
        return boost::json::object{{"uri", uri}};
      });
}

}  // namespace

BOOST_AUTO_TEST_CASE(mcp_server_serves_authenticated_loopback_http) {
  McpServer server = MakeServer();
  server.Start();
  const std::uint16_t port = server.port();
  BOOST_TEST(port != 0U);
  BOOST_TEST(server.endpoint() ==
             "http://127.0.0.1:" + std::to_string(port) + "/mcp");

  const auto unauthorized = SendRequest(port, InitializeRequest(port, false));
  BOOST_TEST(unauthorized.result() == http::status::unauthorized);

  const auto initialized = SendRequest(port, InitializeRequest(port, true));
  BOOST_TEST(initialized.result() == http::status::ok);
  BOOST_CHECK(static_cast<bool>(initialized.find("Mcp-Session-Id") !=
                                initialized.end()));

  auto wrong_origin = InitializeRequest(port, true);
  wrong_origin.set(http::field::origin,
                   "http://127.0.0.1:" +
                       std::to_string(static_cast<unsigned int>(port) + 1U));
  BOOST_TEST(SendRequest(port, std::move(wrong_origin)).result() ==
             http::status::forbidden);

  server.Stop();
  const McpServerStats stats = server.Stats();
  BOOST_TEST(!stats.running);
  BOOST_TEST(stats.accepted_connections == 3U);
  BOOST_TEST(stats.completed_connections == 3U);
}

BOOST_AUTO_TEST_CASE(mcp_server_shutdown_cancels_silent_connections) {
  McpServer server = MakeServer(std::chrono::seconds(30));
  server.Start();
  asio::io_context io_context;
  tcp::socket silent(io_context);
  silent.connect(
      tcp::endpoint(asio::ip::address_v4::loopback(), server.port()));

  const auto observation_deadline =
      std::chrono::steady_clock::now() + std::chrono::seconds(1);
  while (server.Stats().active_connections == 0U &&
         std::chrono::steady_clock::now() < observation_deadline) {
    std::this_thread::sleep_for(std::chrono::milliseconds(1));
  }
  BOOST_REQUIRE(server.Stats().active_connections == 1U);

  const auto started = std::chrono::steady_clock::now();
  server.Stop();
  const auto elapsed = std::chrono::steady_clock::now() - started;
  BOOST_TEST(elapsed < std::chrono::milliseconds(500));
  BOOST_TEST(server.Stats().active_connections == 0U);

  boost::system::error_code ignored;
  silent.close(ignored);
}

BOOST_AUTO_TEST_CASE(mcp_server_rejects_non_loopback_binding) {
  McpServer server(McpServerConfig{.bind_address = "0.0.0.0"},
                   McpProtocolConfig{.bearer_token = std::string(kServerToken),
                                     .allowed_operations = {},
                                     .allowed_information_families = {},
                                     .read_only = false},
                   {}, {});
  BOOST_CHECK_THROW(server.Start(), std::runtime_error);
}

BOOST_AUTO_TEST_CASE(mcp_server_enforces_body_and_request_time_bounds) {
  McpServer server = MakeServer(std::chrono::milliseconds(75));
  server.Start();

  asio::io_context io_context;
  beast::tcp_stream oversized(io_context);
  oversized.connect(
      tcp::endpoint(asio::ip::address_v4::loopback(), server.port()));
  const std::string header =
      "POST /mcp HTTP/1.1\r\nHost: 127.0.0.1\r\nContent-Type: "
      "application/json\r\nContent-Length: " +
      std::to_string(kMcpMaximumRequestBytes + 1U) + "\r\n\r\n";
  asio::write(oversized.socket(), asio::buffer(header));
  beast::flat_buffer response_buffer;
  http::response<http::string_body> response;
  http::read(oversized, response_buffer, response);
  BOOST_TEST(response.result() == http::status::payload_too_large);

  tcp::socket silent(io_context);
  silent.connect(
      tcp::endpoint(asio::ip::address_v4::loopback(), server.port()));
  const auto deadline =
      std::chrono::steady_clock::now() + std::chrono::milliseconds(500);
  while (server.Stats().completed_connections < 2U &&
         std::chrono::steady_clock::now() < deadline) {
    std::this_thread::sleep_for(std::chrono::milliseconds(1));
  }
  BOOST_TEST(server.Stats().completed_connections == 2U);

  server.Stop();
  boost::system::error_code ignored;
  silent.close(ignored);
}

BOOST_AUTO_TEST_CASE(mcp_server_bounds_pending_connections_and_backpressure) {
  McpServer server = MakeServer(std::chrono::seconds(30), 1U, 1U);
  server.Start();
  asio::io_context io_context;
  tcp::socket active(io_context);
  active.connect(
      tcp::endpoint(asio::ip::address_v4::loopback(), server.port()));

  const auto active_deadline =
      std::chrono::steady_clock::now() + std::chrono::seconds(1);
  while (server.Stats().active_connections == 0U &&
         std::chrono::steady_clock::now() < active_deadline) {
    std::this_thread::sleep_for(std::chrono::milliseconds(1));
  }
  BOOST_REQUIRE(server.Stats().active_connections == 1U);

  tcp::socket queued(io_context);
  queued.connect(
      tcp::endpoint(asio::ip::address_v4::loopback(), server.port()));
  const auto queue_deadline =
      std::chrono::steady_clock::now() + std::chrono::seconds(1);
  while (server.Stats().queued_connections == 0U &&
         std::chrono::steady_clock::now() < queue_deadline) {
    std::this_thread::sleep_for(std::chrono::milliseconds(1));
  }
  BOOST_REQUIRE(server.Stats().queued_connections == 1U);

  beast::tcp_stream rejected(io_context);
  rejected.connect(
      tcp::endpoint(asio::ip::address_v4::loopback(), server.port()));
  beast::flat_buffer rejected_buffer;
  http::response<http::string_body> rejected_response;
  http::read(rejected, rejected_buffer, rejected_response);
  BOOST_TEST(rejected_response.result() == http::status::service_unavailable);

  server.Stop();
  const McpServerStats stats = server.Stats();
  BOOST_TEST(stats.maximum_queued_connections == 1U);
  BOOST_TEST(stats.queued_connections == 0U);
  BOOST_TEST(stats.active_connections == 0U);
  BOOST_TEST(stats.rejected_connections == 1U);

  boost::system::error_code ignored;
  active.close(ignored);
  queued.close(ignored);
}

BOOST_AUTO_TEST_CASE(mcp_server_request_deadline_bounds_slow_drip_clients) {
  McpServer server = MakeServer(std::chrono::milliseconds(75), 1U, 1U);
  server.Start();
  asio::io_context io_context;
  tcp::socket slow(io_context);
  slow.connect(tcp::endpoint(asio::ip::address_v4::loopback(), server.port()));

  const auto active_deadline =
      std::chrono::steady_clock::now() + std::chrono::seconds(1);
  while (server.Stats().active_connections == 0U &&
         std::chrono::steady_clock::now() < active_deadline) {
    std::this_thread::sleep_for(std::chrono::milliseconds(1));
  }
  BOOST_REQUIRE(server.Stats().active_connections == 1U);

  boost::system::error_code write_error;
  constexpr std::string_view kPartialRequest = "POST /mcp HTTP/1.1\r\n";
  for (const char byte : kPartialRequest) {
    asio::write(slow, asio::buffer(&byte, 1U), write_error);
    if (write_error) {
      break;
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(20));
  }
  const auto completion_deadline =
      std::chrono::steady_clock::now() + std::chrono::milliseconds(250);
  while (server.Stats().completed_connections == 0U &&
         std::chrono::steady_clock::now() < completion_deadline) {
    std::this_thread::sleep_for(std::chrono::milliseconds(1));
  }
  BOOST_TEST(server.Stats().completed_connections == 1U);

  server.Stop();
  boost::system::error_code ignored;
  slow.close(ignored);
}

BOOST_AUTO_TEST_CASE(mcp_server_dispatches_concurrent_authenticated_clients) {
  McpServer server = MakeServer(std::chrono::seconds(5), 4U, 16U);
  server.Start();
  const std::uint16_t port = server.port();
  std::atomic<std::size_t> initialized = 0U;
  std::vector<std::jthread> clients;
  for (std::size_t index = 0U; index < 8U; ++index) {
    clients.emplace_back([&initialized, port] {
      const auto response = SendRequest(port, InitializeRequest(port, true));
      if (response.result() == http::status::ok &&
          response.find("Mcp-Session-Id") != response.end()) {
        initialized.fetch_add(1U, std::memory_order_relaxed);
      }
    });
  }
  for (std::jthread& client : clients) {
    client.join();
  }
  BOOST_TEST(initialized.load(std::memory_order_relaxed) == 8U);
  BOOST_TEST(server.protocol().Stats().sessions == 8U);

  server.Stop();
  BOOST_TEST(server.Stats().active_connections == 0U);
  BOOST_TEST(server.Stats().queued_connections == 0U);
}

}  // namespace bbp
