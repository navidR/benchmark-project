#include <atomic>
#include <boost/beast/http.hpp>
#include <boost/json/object.hpp>
#include <boost/json/parse.hpp>
#include <boost/json/serialize.hpp>
#include <boost/test/unit_test.hpp>
#include <chrono>
#include <condition_variable>
#include <cstddef>
#include <exception>
#include <memory>
#include <mutex>
#include <optional>
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>

#include "bbp/mcp_operation_service.h"
#include "bbp/mcp_protocol.h"
#include "bbp/mcp_registry.h"

namespace bbp {
namespace {

namespace http = boost::beast::http;
using namespace std::chrono_literals;

constexpr std::string_view kTestToken =
    "0123456789abcdef0123456789abcdef0123456789abcdef";

http::request<http::string_body> ProtocolRequest(
    http::verb method, std::string body = {}, std::string_view session = {},
    std::string_view token = kTestToken,
    std::string_view protocol_version = kMcpProtocolVersion) {
  http::request<http::string_body> request{method, "/mcp", 11};
  request.set(http::field::host, "127.0.0.1:43123");
  request.set(http::field::authorization, "Bearer " + std::string(token));
  request.set(http::field::accept, "application/json, text/event-stream");
  request.set(http::field::content_type, "application/json");
  if (!session.empty()) {
    request.set("Mcp-Session-Id", session);
    request.set("MCP-Protocol-Version", protocol_version);
  }
  request.body() = std::move(body);
  request.prepare_payload();
  return request;
}

std::string InitializeBody(
    std::uint64_t id, std::string_view protocol_version = kMcpProtocolVersion) {
  return boost::json::serialize(boost::json::object{
      {"jsonrpc", "2.0"},
      {"id", id},
      {"method", "initialize"},
      {"params", boost::json::object{
                     {"protocolVersion", protocol_version},
                     {"capabilities", boost::json::object{}},
                     {"clientInfo", boost::json::object{{"name", "bbp-test"},
                                                        {"version", "1"}}}}}});
}

std::string InitializedBody() {
  return boost::json::serialize(
      boost::json::object{{"jsonrpc", "2.0"},
                          {"method", "notifications/initialized"},
                          {"params", boost::json::object{}}});
}

std::string CancelledBody(const boost::json::value& request_id) {
  return boost::json::serialize(boost::json::object{
      {"jsonrpc", "2.0"},
      {"method", "notifications/cancelled"},
      {"params", boost::json::object{{"requestId", request_id},
                                     {"reason", "test cancellation"}}}});
}

std::string RequestBody(std::uint64_t id, std::string_view method,
                        boost::json::object params = {}) {
  return boost::json::serialize(
      boost::json::object{{"jsonrpc", "2.0"},
                          {"id", id},
                          {"method", method},
                          {"params", std::move(params)}});
}

std::vector<std::string> SseEventIds(std::string_view body) {
  std::vector<std::string> ids;
  while (!body.empty()) {
    const std::size_t line_end = body.find('\n');
    const std::string_view line = body.substr(0U, line_end);
    if (line.starts_with("id: ")) {
      ids.emplace_back(line.substr(4U));
    }
    if (line_end == std::string_view::npos) {
      break;
    }
    body.remove_prefix(line_end + 1U);
  }
  return ids;
}

std::string Initialize(
    McpProtocol* protocol, bool check_resource_subscription = false,
    std::string_view protocol_version = kMcpProtocolVersion) {
  const auto response = protocol->Handle(
      ProtocolRequest(http::verb::post, InitializeBody(1U, protocol_version)));
  BOOST_REQUIRE(response.result() == http::status::ok);
  const auto protocol_field = response.find("MCP-Protocol-Version");
  BOOST_REQUIRE(protocol_field != response.end());
  BOOST_TEST(protocol_field->value() == protocol_version);
  const boost::json::object initialized =
      boost::json::parse(response.body()).as_object();
  BOOST_TEST(
      initialized.at("result").as_object().at("protocolVersion").as_string() ==
      protocol_version);
  if (check_resource_subscription) {
    BOOST_TEST(initialized.at("result")
                   .as_object()
                   .at("capabilities")
                   .as_object()
                   .at("resources")
                   .as_object()
                   .at("subscribe")
                   .as_bool() == false);
  }
  const auto field = response.find("Mcp-Session-Id");
  BOOST_REQUIRE(static_cast<bool>(field != response.end()));
  const std::string session(field->value());
  BOOST_REQUIRE_EQUAL(session.size(), 48U);
  return session;
}

void MarkInitialized(McpProtocol* protocol, std::string_view session,
                     std::string_view protocol_version = kMcpProtocolVersion) {
  const auto response =
      protocol->Handle(ProtocolRequest(http::verb::post, InitializedBody(),
                                       session, kTestToken, protocol_version));
  BOOST_REQUIRE(response.result() == http::status::accepted);
}

const boost::json::object& ParsedBody(const std::string& body,
                                      boost::json::value* storage) {
  *storage = boost::json::parse(body);
  BOOST_REQUIRE(storage->is_object());
  return storage->as_object();
}

template <typename Predicate>
bool WaitFor(Predicate predicate,
             std::chrono::milliseconds timeout = std::chrono::seconds(2)) {
  const auto deadline = std::chrono::steady_clock::now() + timeout;
  while (std::chrono::steady_clock::now() < deadline) {
    if (predicate()) {
      return true;
    }
    std::this_thread::sleep_for(1ms);
  }
  return predicate();
}

McpProtocol MakeProtocol(std::size_t* tool_calls = nullptr) {
  return McpProtocol(
      McpProtocolConfig{.bearer_token = std::string(kTestToken),
                        .endpoint_path = "/mcp",
                        .endpoint_port = 43123U,
                        .allowed_operations = {},
                        .allowed_information_families = {},
                        .read_only = false},
      [tool_calls](std::string_view name, const boost::json::object& arguments,
                   std::string_view session, std::stop_token) {
        if (tool_calls != nullptr) {
          ++*tool_calls;
        }
        return boost::json::object{
            {"tool", name}, {"session", session}, {"arguments", arguments}};
      },
      [](std::string_view uri, std::string_view, std::stop_token) {
        return boost::json::object{{"uri", uri}, {"available", true}};
      });
}

}  // namespace

BOOST_AUTO_TEST_CASE(mcp_protocol_enforces_auth_origin_and_json_rpc_shape) {
  McpProtocol protocol = MakeProtocol();

  auto unauthorized = ProtocolRequest(http::verb::post, InitializeBody(1U));
  unauthorized.set(http::field::authorization, "Bearer wrong-token");
  BOOST_TEST(protocol.Handle(unauthorized).result() ==
             http::status::unauthorized);

  auto foreign_origin = ProtocolRequest(http::verb::post, InitializeBody(1U));
  foreign_origin.set(http::field::origin, "https://attacker.example");
  BOOST_TEST(protocol.Handle(foreign_origin).result() ==
             http::status::forbidden);

  auto malformed = ProtocolRequest(http::verb::post, "{");
  const auto malformed_response = protocol.Handle(malformed);
  BOOST_REQUIRE(malformed_response.result() == http::status::ok);
  boost::json::value parsed;
  const boost::json::object& object =
      ParsedBody(malformed_response.body(), &parsed);
  BOOST_TEST(object.at("error").as_object().at("code").as_int64() == -32700);

  auto false_content_type =
      ProtocolRequest(http::verb::post, InitializeBody(2U));
  false_content_type.set(http::field::content_type,
                         "not-application/json-value");
  BOOST_TEST(protocol.Handle(false_content_type).result() ==
             http::status::unsupported_media_type);

  auto oversized_initialize = boost::json::parse(InitializeBody(3U));
  oversized_initialize.as_object()
      .at("params")
      .as_object()
      .at("clientInfo")
      .as_object()["name"] = std::string(kMcpMaximumClientNameBytes + 1U, 'x');
  const auto invalid_client = protocol.Handle(ProtocolRequest(
      http::verb::post, boost::json::serialize(oversized_initialize)));
  BOOST_REQUIRE(invalid_client.result() == http::status::ok);
  const boost::json::object& invalid_client_object =
      ParsedBody(invalid_client.body(), &parsed);
  BOOST_TEST(
      invalid_client_object.at("error").as_object().at("code").as_int64() ==
      -32602);
  BOOST_TEST(protocol.Stats().sessions == 0U);

  const McpProtocolStats stats = protocol.Stats();
  BOOST_TEST(stats.authentication_failures == 1U);
  BOOST_TEST(stats.origin_failures == 1U);
  BOOST_TEST(stats.malformed_requests == 1U);
}

BOOST_AUTO_TEST_CASE(mcp_protocol_requires_initialize_notification_order) {
  McpProtocol protocol = MakeProtocol();
  const std::string session = Initialize(&protocol);

  const auto early = protocol.Handle(ProtocolRequest(
      http::verb::post, RequestBody(2U, "tools/list"), session));
  boost::json::value parsed;
  const boost::json::object& early_object = ParsedBody(early.body(), &parsed);
  BOOST_TEST(early_object.at("error").as_object().at("code").as_int64() ==
             -32002);

  MarkInitialized(&protocol, session);
  const auto response = protocol.Handle(ProtocolRequest(
      http::verb::post, RequestBody(3U, "tools/list"), session));
  BOOST_REQUIRE(response.result() == http::status::ok);
  const boost::json::object& object = ParsedBody(response.body(), &parsed);
  const boost::json::array& tools =
      object.at("result").as_object().at("tools").as_array();
  BOOST_TEST(tools.size() ==
             static_cast<std::size_t>(McpOperationKind::kCount));

  const McpProtocolStats stats = protocol.Stats();
  BOOST_TEST(stats.sessions == 1U);
  BOOST_TEST(stats.initialized_sessions == 1U);
}

BOOST_AUTO_TEST_CASE(
    mcp_protocol_negotiates_and_retains_each_supported_session_version) {
  constexpr std::string_view kStableVersion = "2025-06-18";
  constexpr std::string_view kNewestVersion = "2025-11-25";
  constexpr std::string_view kUnsupportedVersion = "2024-11-05";
  McpProtocol protocol = MakeProtocol();

  const std::string stable_session =
      Initialize(&protocol, false, kStableVersion);
  const std::string newest_session =
      Initialize(&protocol, false, kNewestVersion);
  BOOST_TEST(stable_session != newest_session);
  MarkInitialized(&protocol, stable_session, kStableVersion);
  MarkInitialized(&protocol, newest_session, kNewestVersion);

  const auto stable_ping = protocol.Handle(
      ProtocolRequest(http::verb::post, RequestBody(2U, "ping"), stable_session,
                      kTestToken, kStableVersion));
  BOOST_TEST(stable_ping.result() == http::status::ok);
  const auto newest_ping = protocol.Handle(
      ProtocolRequest(http::verb::post, RequestBody(3U, "ping"), newest_session,
                      kTestToken, kNewestVersion));
  BOOST_TEST(newest_ping.result() == http::status::ok);
  const auto require_common_tool_contract = [&](std::string_view session,
                                                std::string_view version,
                                                std::uint64_t request_id) {
    const auto response = protocol.Handle(
        ProtocolRequest(http::verb::post, RequestBody(request_id, "tools/list"),
                        session, kTestToken, version));
    BOOST_REQUIRE(response.result() == http::status::ok);
    const boost::json::value parsed = boost::json::parse(response.body());
    const boost::json::array& tools =
        parsed.as_object().at("result").as_object().at("tools").as_array();
    BOOST_REQUIRE(!tools.empty());
    for (const boost::json::value& value : tools) {
      const boost::json::object& tool = value.as_object();
      BOOST_TEST(tool.at("outputSchema").as_object().at("type").as_string() ==
                 "object");
      BOOST_TEST(tool.if_contains("execution") == nullptr);
    }
  };
  require_common_tool_contract(stable_session, kStableVersion, 8U);
  require_common_tool_contract(newest_session, kNewestVersion, 9U);

  const auto mismatched_post = protocol.Handle(
      ProtocolRequest(http::verb::post, RequestBody(4U, "ping"), stable_session,
                      kTestToken, kNewestVersion));
  BOOST_TEST(mismatched_post.result() == http::status::bad_request);
  BOOST_TEST(mismatched_post.body().find(
                 "not negotiated for this MCP session") != std::string::npos);
  const auto mismatched_get = protocol.Handle(ProtocolRequest(
      http::verb::get, {}, newest_session, kTestToken, kStableVersion));
  BOOST_TEST(mismatched_get.result() == http::status::bad_request);
  const auto mismatched_delete = protocol.Handle(ProtocolRequest(
      http::verb::delete_, {}, stable_session, kTestToken, kNewestVersion));
  BOOST_TEST(mismatched_delete.result() == http::status::bad_request);

  const auto stable_still_active = protocol.Handle(
      ProtocolRequest(http::verb::post, RequestBody(5U, "ping"), stable_session,
                      kTestToken, kStableVersion));
  BOOST_TEST(stable_still_active.result() == http::status::ok);

  const auto fallback_response = protocol.Handle(ProtocolRequest(
      http::verb::post, InitializeBody(6U, kUnsupportedVersion)));
  BOOST_REQUIRE(fallback_response.result() == http::status::ok);
  BOOST_TEST(fallback_response.at("MCP-Protocol-Version") == kNewestVersion);
  const boost::json::object fallback_body =
      boost::json::parse(fallback_response.body()).as_object();
  BOOST_TEST(fallback_body.at("result")
                 .as_object()
                 .at("protocolVersion")
                 .as_string() == kNewestVersion);
  const std::string fallback_session(fallback_response.at("Mcp-Session-Id"));
  MarkInitialized(&protocol, fallback_session, kNewestVersion);
  const auto fallback_ping = protocol.Handle(
      ProtocolRequest(http::verb::post, RequestBody(7U, "ping"),
                      fallback_session, kTestToken, kNewestVersion));
  BOOST_TEST(fallback_ping.result() == http::status::ok);
}

BOOST_AUTO_TEST_CASE(
    mcp_protocol_distinguishes_missing_unknown_and_client_response_posts) {
  McpProtocol protocol = MakeProtocol();
  const std::string session = Initialize(&protocol);
  MarkInitialized(&protocol, session);

  auto missing_post =
      ProtocolRequest(http::verb::post, RequestBody(2U, "ping"));
  missing_post.set("MCP-Protocol-Version", kMcpProtocolVersion);
  BOOST_TEST(protocol.Handle(missing_post).result() ==
             http::status::bad_request);
  auto missing_get = ProtocolRequest(http::verb::get);
  missing_get.set("MCP-Protocol-Version", kMcpProtocolVersion);
  BOOST_TEST(protocol.Handle(missing_get).result() ==
             http::status::bad_request);
  auto missing_delete = ProtocolRequest(http::verb::delete_);
  missing_delete.set("MCP-Protocol-Version", kMcpProtocolVersion);
  BOOST_TEST(protocol.Handle(missing_delete).result() ==
             http::status::bad_request);

  constexpr std::string_view kUnknownSession = "unknown-session";
  BOOST_TEST(protocol.Handle(ProtocolRequest(
                 http::verb::post, RequestBody(3U, "ping"), kUnknownSession))
                 .result() == http::status::not_found);
  BOOST_TEST(
      protocol
          .Handle(ProtocolRequest(http::verb::post, InitializeBody(4U),
                                  kUnknownSession))
          .result() == http::status::not_found);
  BOOST_TEST(
      protocol.Handle(ProtocolRequest(http::verb::get, {}, kUnknownSession))
          .result() == http::status::not_found);
  BOOST_TEST(protocol
                 .Handle(
                     ProtocolRequest(http::verb::delete_, {}, kUnknownSession))
                 .result() == http::status::not_found);

  const std::string result_response = boost::json::serialize(
      boost::json::object{{"jsonrpc", "2.0"},
                          {"id", 41U},
                          {"result", boost::json::object{{"accepted", true}}}});
  const auto accepted_result = protocol.Handle(ProtocolRequest(
      http::verb::post, result_response, session));
  BOOST_TEST(accepted_result.result() == http::status::accepted);
  BOOST_TEST(accepted_result.body().empty());

  const std::string error_response = boost::json::serialize(
      boost::json::object{{"jsonrpc", "2.0"},
                          {"id", "server-request"},
                          {"error", boost::json::object{{"code", -32001},
                                                        {"message", "no"}}}});
  const auto accepted_error = protocol.Handle(ProtocolRequest(
      http::verb::post, error_response, session));
  BOOST_TEST(accepted_error.result() == http::status::accepted);
  BOOST_TEST(accepted_error.body().empty());

  BOOST_REQUIRE(protocol
                    .Handle(
                        ProtocolRequest(http::verb::delete_, {}, session))
                    .result() == http::status::ok);
  BOOST_TEST(protocol
                 .Handle(
                     ProtocolRequest(http::verb::post, result_response, session))
                 .result() == http::status::not_found);
}

BOOST_AUTO_TEST_CASE(mcp_protocol_dispatches_registered_tools_and_resources) {
  std::size_t tool_calls = 0U;
  McpProtocol protocol = MakeProtocol(&tool_calls);
  const std::string session = Initialize(&protocol, true);
  MarkInitialized(&protocol, session);

  const boost::json::object arguments{{"run_id", "run-a"}};
  const auto tool_response = protocol.Handle(ProtocolRequest(
      http::verb::post,
      RequestBody(2U, "tools/call",
                  boost::json::object{{"name", "run.report"},
                                      {"arguments", arguments}}),
      session));
  boost::json::value parsed;
  const boost::json::object& tool_object =
      ParsedBody(tool_response.body(), &parsed);
  BOOST_TEST(tool_object.at("result").as_object().at("isError").as_bool() ==
             false);
  BOOST_TEST(tool_calls == 1U);

  const auto resource_response = protocol.Handle(ProtocolRequest(
      http::verb::post,
      RequestBody(3U, "resources/read",
                  boost::json::object{{"uri", "bbp:///capabilities"}}),
      session));
  const boost::json::object& resource_object =
      ParsedBody(resource_response.body(), &parsed);
  const std::string text(resource_object.at("result")
                             .as_object()
                             .at("contents")
                             .as_array()
                             .front()
                             .as_object()
                             .at("text")
                             .as_string());
  BOOST_TEST(
      boost::json::parse(text).as_object().at("protocol_version").as_string() ==
      kMcpProtocolVersion);
}

BOOST_AUTO_TEST_CASE(
    mcp_protocol_reports_resource_absence_separately_from_bad_params_and_faults) {
  const auto require_error = [](const auto& response, int expected_code,
                                std::string_view expected_uri = {}) {
    BOOST_REQUIRE(response.result() == http::status::ok);
    const boost::json::object error =
        boost::json::parse(response.body()).as_object().at("error").as_object();
    BOOST_TEST(error.at("code").as_int64() == expected_code);
    if (!expected_uri.empty()) {
      BOOST_TEST(error.at("data").as_object().at("uri").as_string() ==
                 expected_uri);
    }
  };

  McpProtocol restricted(
      McpProtocolConfig{
          .bearer_token = std::string(kTestToken),
          .endpoint_path = "/mcp",
          .endpoint_port = 43123U,
          .allowed_operations = {McpOperationKind::kQueryEvidence},
          .allowed_information_families = {
              McpInformationFamily::kCapabilities},
          .read_only = true},
      {}, {});
  const std::string restricted_session = Initialize(&restricted);
  MarkInitialized(&restricted, restricted_session);
  require_error(
      restricted.Handle(ProtocolRequest(
          http::verb::post,
          RequestBody(2U, "resources/read",
                      boost::json::object{{"uri", "bbp:///missing"}}),
          restricted_session)),
      -32002, "bbp:///missing");
  require_error(
      restricted.Handle(ProtocolRequest(
          http::verb::post,
          RequestBody(3U, "resources/read",
                      boost::json::object{{"uri", "bbp:///logs"}}),
          restricted_session)),
      -32002, "bbp:///logs");
  require_error(
      restricted.Handle(ProtocolRequest(
          http::verb::post,
          RequestBody(4U, "resources/read",
                      boost::json::object{{"uri", 7U}}),
          restricted_session)),
      -32602);

  McpProtocol failing(
      McpProtocolConfig{.bearer_token = std::string(kTestToken),
                        .endpoint_path = "/mcp",
                        .endpoint_port = 43123U,
                        .allowed_operations = {},
                        .allowed_information_families = {},
                        .read_only = false},
      {},
      [](std::string_view, std::string_view,
         std::stop_token) -> boost::json::value {
        throw std::invalid_argument("resource callback failed");
      });
  const std::string failing_session = Initialize(&failing);
  MarkInitialized(&failing, failing_session);
  require_error(
      failing.Handle(ProtocolRequest(
          http::verb::post,
          RequestBody(5U, "resources/read",
                      boost::json::object{{"uri", "bbp:///logs"}}),
          failing_session)),
      -32603);

  McpProtocol unavailable(
      McpProtocolConfig{.bearer_token = std::string(kTestToken),
                        .endpoint_path = "/mcp",
                        .endpoint_port = 43123U,
                        .allowed_operations = {},
                        .allowed_information_families = {},
                        .read_only = false},
      {},
      [](std::string_view, std::string_view,
         std::stop_token) -> boost::json::value {
        throw McpOperationFailure(
            "run_not_active",
            "the requested resource has no active managed run", true);
      });
  const std::string unavailable_session = Initialize(&unavailable);
  MarkInitialized(&unavailable, unavailable_session);
  require_error(
      unavailable.Handle(ProtocolRequest(
          http::verb::post,
          RequestBody(6U, "resources/read",
                      boost::json::object{{"uri", "bbp:///logs"}}),
          unavailable_session)),
      -32002, "bbp:///logs");
}

BOOST_AUTO_TEST_CASE(
    mcp_protocol_contains_non_standard_application_callback_exceptions) {
  McpProtocol protocol(
      McpProtocolConfig{.bearer_token = std::string(kTestToken),
                        .endpoint_path = "/mcp",
                        .endpoint_port = 43123U,
                        .allowed_operations = {},
                        .allowed_information_families = {},
                        .read_only = false},
      [](std::string_view, const boost::json::object&, std::string_view,
         std::stop_token) -> boost::json::value {
        throw 1;
      },
      [](std::string_view, std::string_view,
         std::stop_token) -> boost::json::value {
        throw 2;
      });
  const std::string session = Initialize(&protocol);
  MarkInitialized(&protocol, session);

  const auto require_internal_error = [](const auto& response,
                                         std::uint64_t expected_id) {
    BOOST_REQUIRE(response.result() == http::status::ok);
    const boost::json::object object =
        boost::json::parse(response.body()).as_object();
    BOOST_REQUIRE(object.at("id").is_int64());
    BOOST_TEST(object.at("id").as_int64() ==
               static_cast<std::int64_t>(expected_id));
    BOOST_TEST(object.at("error").as_object().at("code").as_int64() == -32603);
  };

  const auto tool_error = protocol.Handle(ProtocolRequest(
      http::verb::post,
      RequestBody(
          2U, "tools/call",
          boost::json::object{
              {"name", "run.report"},
              {"arguments", boost::json::object{{"run_id", "run-a"}}}}),
      session));
  BOOST_REQUIRE(tool_error.result() == http::status::ok);
  const boost::json::object structured_error =
      boost::json::parse(tool_error.body())
          .as_object()
          .at("result")
          .as_object()
          .at("structuredContent")
          .as_object();
  BOOST_TEST(structured_error.at("result_family").as_string() == "error");
  BOOST_TEST(structured_error.at("code").as_string() == "tool_error");
  BOOST_TEST(structured_error.at("retryable").as_bool() == false);
  BOOST_TEST(structured_error.at("diagnostics").as_array().empty());
  require_internal_error(
      protocol.Handle(ProtocolRequest(
          http::verb::post,
          RequestBody(3U, "resources/read",
                      boost::json::object{{"uri", "bbp:///logs"}}),
          session)),
      3U);

  McpProtocol session_callback_protocol(
      McpProtocolConfig{.bearer_token = std::string(kTestToken),
                        .endpoint_path = "/mcp",
                        .endpoint_port = 43123U,
                        .allowed_operations = {},
                        .allowed_information_families = {},
                        .read_only = false},
      {}, {}, [](std::string_view, bool opened, std::stop_token) {
        if (opened) {
          throw 3;
        }
      });
  require_internal_error(
      session_callback_protocol.Handle(
          ProtocolRequest(http::verb::post, InitializeBody(4U))),
      4U);
  BOOST_TEST(session_callback_protocol.Stats().sessions == 0U);
}

BOOST_AUTO_TEST_CASE(mcp_protocol_preserves_typed_tool_failures) {
  McpProtocol protocol(
      McpProtocolConfig{.bearer_token = std::string(kTestToken),
                        .endpoint_path = "/mcp",
                        .endpoint_port = 43123U,
                        .allowed_operations = {},
                        .allowed_information_families = {},
                        .read_only = false},
      [](std::string_view, const boost::json::object& arguments,
         std::string_view, std::stop_token) -> boost::json::value {
        if (arguments.at("failure").as_string() == "cancelled") {
          throw McpOperationCancelled(
              "cancelled with evidence",
              boost::json::array{
                  boost::json::object{{"node_id", "node-2"}}});
        }
        throw McpOperationFailure(
            "capacity_reached", "configured capacity reached", true,
            boost::json::array{
                boost::json::object{{"limit", 10U}, {"requested", 11U}}});
      },
      {});
  const std::string session = Initialize(&protocol);
  MarkInitialized(&protocol, session);

  const auto invoke = [&](std::uint64_t id, std::string_view failure) {
    return protocol.Handle(ProtocolRequest(
        http::verb::post,
        RequestBody(
            id, "tools/call",
            boost::json::object{
                {"name", "run.report"},
                {"arguments", boost::json::object{{"failure", failure}}}}),
        session));
  };
  const auto structured_error = [](const auto& response) {
    BOOST_REQUIRE(response.result() == http::status::ok);
    const boost::json::object result =
        boost::json::parse(response.body()).as_object().at("result").as_object();
    BOOST_TEST(result.at("isError").as_bool());
    return result.at("structuredContent").as_object();
  };

  const boost::json::object failed = structured_error(invoke(2U, "failed"));
  BOOST_TEST(failed.at("code").as_string() == "capacity_reached");
  BOOST_TEST(failed.at("retryable").as_bool());
  BOOST_TEST(failed.at("diagnostics")
                 .as_array()
                 .front()
                 .as_object()
                 .at("requested")
                 .as_int64() == 11);

  const boost::json::object cancelled =
      structured_error(invoke(3U, "cancelled"));
  BOOST_TEST(cancelled.at("code").as_string() == "cancelled");
  BOOST_TEST(!cancelled.at("retryable").as_bool());
  BOOST_TEST(cancelled.at("diagnostics")
                 .as_array()
                 .front()
                 .as_object()
                 .at("node_id")
                 .as_string() == "node-2");
}

BOOST_AUTO_TEST_CASE(
    mcp_protocol_malformed_notification_has_no_json_rpc_response_body) {
  McpProtocol protocol = MakeProtocol();
  const std::string session = Initialize(&protocol);
  MarkInitialized(&protocol, session);
  const std::string malformed_notification = boost::json::serialize(
      boost::json::object{
          {"jsonrpc", "2.0"},
          {"method", "notifications/cancelled"},
          {"params",
           boost::json::array{
               boost::json::object{{"requestId", 7U}}}}});

  const auto response = protocol.Handle(ProtocolRequest(
      http::verb::post, malformed_notification, session));
  BOOST_TEST(response.result() == http::status::accepted);
  BOOST_TEST(response.body().empty());
}

BOOST_AUTO_TEST_CASE(
    mcp_protocol_cancelled_notification_stops_the_matching_request) {
  std::atomic<bool> request_started = false;
  std::atomic<bool> inspect_nonmatching_cancellation = false;
  std::atomic<bool> nonmatching_cancellation_checked = false;
  std::atomic<bool> cancelled_by_nonmatching_request = false;
  std::atomic<bool> cancellation_seen = false;
  McpProtocol protocol(
      McpProtocolConfig{.bearer_token = std::string(kTestToken),
                        .endpoint_path = "/mcp",
                        .endpoint_port = 43123U,
                        .allowed_operations = {},
                        .allowed_information_families = {},
                        .read_only = false},
      {},
      [&](std::string_view uri, std::string_view, std::stop_token stop_token) {
        request_started.store(true, std::memory_order_release);
        const auto deadline = std::chrono::steady_clock::now() + 2s;
        while (!inspect_nonmatching_cancellation.load(
                   std::memory_order_acquire) &&
               std::chrono::steady_clock::now() < deadline) {
          std::this_thread::yield();
        }
        cancelled_by_nonmatching_request.store(stop_token.stop_requested(),
                                               std::memory_order_release);
        nonmatching_cancellation_checked.store(true, std::memory_order_release);
        while (!stop_token.stop_requested() &&
               std::chrono::steady_clock::now() < deadline) {
          std::this_thread::yield();
        }
        cancellation_seen.store(stop_token.stop_requested(),
                                std::memory_order_release);
        return boost::json::object{{"uri", uri}};
      });
  const std::string session = Initialize(&protocol);
  MarkInitialized(&protocol, session);

  std::optional<http::response<http::string_body>> cancelled_response;
  std::jthread reader([&] {
    cancelled_response = protocol.Handle(ProtocolRequest(
        http::verb::post,
        RequestBody(7U, "resources/read",
                    boost::json::object{{"uri", "bbp:///logs"}}),
        session));
  });
  BOOST_REQUIRE(WaitFor(
      [&] { return request_started.load(std::memory_order_acquire); }));

  const auto nonmatching = protocol.Handle(ProtocolRequest(
      http::verb::post, CancelledBody(boost::json::value(8U)), session));
  BOOST_TEST(nonmatching.result() == http::status::accepted);
  inspect_nonmatching_cancellation.store(true, std::memory_order_release);
  BOOST_REQUIRE(WaitFor([&] {
    return nonmatching_cancellation_checked.load(std::memory_order_acquire);
  }));
  BOOST_TEST(
      !cancelled_by_nonmatching_request.load(std::memory_order_acquire));

  const auto cancelled = protocol.Handle(ProtocolRequest(
      http::verb::post, CancelledBody(boost::json::value(7U)), session));
  BOOST_TEST(cancelled.result() == http::status::accepted);
  reader.join();
  BOOST_TEST(cancellation_seen.load(std::memory_order_acquire));
  BOOST_REQUIRE(cancelled_response.has_value());
  BOOST_TEST(cancelled_response->result() == http::status::accepted);
  BOOST_TEST(cancelled_response->body().empty());
}

BOOST_AUTO_TEST_CASE(
    mcp_protocol_transport_stop_suppresses_the_request_payload) {
  bool stopped_handler_called = false;
  McpProtocol protocol(
      McpProtocolConfig{.bearer_token = std::string(kTestToken),
                        .endpoint_path = "/mcp",
                        .endpoint_port = 43123U,
                        .allowed_operations = {},
                        .allowed_information_families = {},
                        .read_only = false},
      [&](std::string_view, const boost::json::object&, std::string_view,
          std::stop_token stop_token) {
        stopped_handler_called = stop_token.stop_requested();
        return boost::json::object{{"late", true}};
      },
      {});
  const std::string session = Initialize(&protocol);
  MarkInitialized(&protocol, session);
  std::stop_source transport;
  transport.request_stop();

  const auto response = protocol.Handle(
      ProtocolRequest(
          http::verb::post,
          RequestBody(
              2U, "tools/call",
              boost::json::object{
                  {"name", "run.report"},
                  {"arguments", boost::json::object{{"run_id", "run-a"}}}}),
          session),
      transport.get_token());
  BOOST_TEST(stopped_handler_called);
  BOOST_TEST(response.result() == http::status::accepted);
  BOOST_TEST(response.body().empty());
}

BOOST_AUTO_TEST_CASE(
    mcp_protocol_filters_read_only_discovery_and_rejects_hidden_mutations) {
  std::size_t tool_calls = 0U;
  McpProtocol protocol(
      McpProtocolConfig{
          .bearer_token = std::string(kTestToken),
          .endpoint_path = "/mcp",
          .endpoint_port = 43123U,
          .allowed_operations = {McpOperationKind::kReportRun,
                                 McpOperationKind::kGetOperation},
          .allowed_information_families = {McpInformationFamily::kCapabilities,
                                           McpInformationFamily::kSchemas},
          .read_only = true},
      [&tool_calls](std::string_view, const boost::json::object&,
                    std::string_view, std::stop_token) {
        ++tool_calls;
        return boost::json::object{};
      },
      {});
  const std::string session = Initialize(&protocol);
  MarkInitialized(&protocol, session);

  boost::json::value parsed;
  const auto tools_response = protocol.Handle(ProtocolRequest(
      http::verb::post, RequestBody(2U, "tools/list"), session));
  const boost::json::array& tools = ParsedBody(tools_response.body(), &parsed)
                                        .at("result")
                                        .as_object()
                                        .at("tools")
                                        .as_array();
  BOOST_REQUIRE_EQUAL(tools.size(), 2U);
  BOOST_TEST(tools[0].as_object().at("name").as_string() == "run.report");
  BOOST_TEST(tools[1].as_object().at("name").as_string() == "operation.get");

  const auto resources_response = protocol.Handle(ProtocolRequest(
      http::verb::post, RequestBody(3U, "resources/list"), session));
  const boost::json::array& resources =
      ParsedBody(resources_response.body(), &parsed)
          .at("result")
          .as_object()
          .at("resources")
          .as_array();
  BOOST_REQUIRE_EQUAL(resources.size(), 2U);
  BOOST_TEST(resources[0].as_object().at("uri").as_string() ==
             "bbp:///capabilities");
  BOOST_TEST(resources[1].as_object().at("uri").as_string() ==
             "bbp:///schemas");

  const auto capabilities_response = protocol.Handle(ProtocolRequest(
      http::verb::post,
      RequestBody(4U, "resources/read",
                  boost::json::object{{"uri", "bbp:///capabilities"}}),
      session));
  const boost::json::object& capabilities_result =
      ParsedBody(capabilities_response.body(), &parsed)
          .at("result")
          .as_object();
  const std::string capabilities_text(capabilities_result.at("contents")
                                          .as_array()
                                          .front()
                                          .as_object()
                                          .at("text")
                                          .as_string());
  const boost::json::object capabilities =
      boost::json::parse(capabilities_text).as_object();
  BOOST_TEST(capabilities.at("access_mode").as_string() == "read_only");
  BOOST_TEST(capabilities.at("operations").as_array().size() == 2U);

  const auto hidden_resource_response = protocol.Handle(ProtocolRequest(
      http::verb::post,
      RequestBody(5U, "resources/read",
                  boost::json::object{{"uri", "bbp:///artifacts"}}),
      session));
  BOOST_TEST(hidden_resource_response.body().find(
                 "resource is unavailable in the current endpoint") !=
             std::string::npos);

  const auto mutation_response = protocol.Handle(ProtocolRequest(
      http::verb::post,
      RequestBody(
          6U, "tools/call",
          boost::json::object{
              {"name", "run.stop"},
              {"arguments", boost::json::object{{"run_id", "retained"}}}}),
      session));
  BOOST_TEST(mutation_response.body().find("retained read-only run") !=
             std::string::npos);
  BOOST_TEST(tool_calls == 0U);
}

BOOST_AUTO_TEST_CASE(
    mcp_protocol_filters_family_selectors_in_discovery_and_dispatch) {
  std::size_t tool_calls = 0U;
  McpProtocol protocol(
      McpProtocolConfig{
          .bearer_token = std::string(kTestToken),
          .endpoint_path = "/mcp",
          .endpoint_port = 43123U,
          .allowed_operations = {McpOperationKind::kQueryEvidence},
          .allowed_information_families = {
              McpInformationFamily::kCapabilities},
          .read_only = true},
      [&tool_calls](std::string_view, const boost::json::object&,
                    std::string_view, std::stop_token) {
        ++tool_calls;
        return boost::json::object{};
      },
      {});
  const std::string session = Initialize(&protocol);
  MarkInitialized(&protocol, session);

  boost::json::value parsed;
  const auto tools_response = protocol.Handle(ProtocolRequest(
      http::verb::post, RequestBody(2U, "tools/list"), session));
  const boost::json::object& evidence_tool =
      ParsedBody(tools_response.body(), &parsed)
          .at("result")
          .as_object()
          .at("tools")
          .as_array()
          .front()
          .as_object();
  const boost::json::array& family_names =
      evidence_tool.at("inputSchema")
          .as_object()
          .at("properties")
          .as_object()
          .at("families")
          .as_object()
          .at("items")
          .as_object()
          .at("enum")
          .as_array();
  BOOST_REQUIRE_EQUAL(family_names.size(), 1U);
  BOOST_TEST(family_names.front().as_string() == "capabilities");

  const boost::json::array& output_choices =
      evidence_tool.at("outputSchema").as_object().at("oneOf").as_array();
  const boost::json::object* operation_schema = nullptr;
  for (const boost::json::value& choice : output_choices) {
    const boost::json::object& properties =
        choice.as_object().at("properties").as_object();
    if (properties.at("result_family").as_object().at("const").as_string() ==
        "operation") {
      operation_schema = &choice.as_object();
      break;
    }
  }
  BOOST_REQUIRE(operation_schema != nullptr);
  const boost::json::object& operation_properties =
      operation_schema->at("properties").as_object();
  const boost::json::array& operation_names =
      operation_properties.at("operation").as_object().at("enum").as_array();
  BOOST_REQUIRE_EQUAL(operation_names.size(), 1U);
  BOOST_TEST(operation_names.front().as_string() == "evidence.query");
  const boost::json::array& terminal_families =
      operation_properties.at("terminal_result_family")
          .as_object()
          .at("enum")
          .as_array();
  BOOST_REQUIRE_EQUAL(terminal_families.size(), 2U);
  BOOST_TEST(terminal_families[0].as_string() == "evidence_page");
  BOOST_TEST(terminal_families[1].as_string() == "error");

  const auto hidden_response = protocol.Handle(ProtocolRequest(
      http::verb::post,
      RequestBody(
          3U, "tools/call",
          boost::json::object{
              {"name", "evidence.query"},
              {"arguments",
               boost::json::object{
                   {"run_id", "retained"},
                   {"families", boost::json::array{"events"}}}}}),
      session));
  BOOST_TEST(hidden_response.body().find(
                 "information family is unavailable in the current endpoint") !=
             std::string::npos);
  BOOST_TEST(tool_calls == 0U);

  const auto allowed_response = protocol.Handle(ProtocolRequest(
      http::verb::post,
      RequestBody(
          4U, "tools/call",
          boost::json::object{
              {"name", "evidence.query"},
              {"arguments",
               boost::json::object{
                   {"run_id", "retained"},
                   {"families", boost::json::array{"capabilities"}}}}}),
      session));
  BOOST_TEST(allowed_response.body().find("\"isError\":false") !=
             std::string::npos);
  BOOST_TEST(tool_calls == 1U);

  const auto capabilities_response = protocol.Handle(ProtocolRequest(
      http::verb::post,
      RequestBody(5U, "resources/read",
                  boost::json::object{{"uri", "bbp:///capabilities"}}),
      session));
  const std::string capabilities_text(
      ParsedBody(capabilities_response.body(), &parsed)
          .at("result")
          .as_object()
          .at("contents")
          .as_array()
          .front()
          .as_object()
          .at("text")
          .as_string());
  const boost::json::value capability_document =
      boost::json::parse(capabilities_text);
  const boost::json::array& result_families =
      capability_document.as_object().at("result_families").as_array();
  BOOST_REQUIRE_EQUAL(result_families.size(), 3U);
  BOOST_TEST(result_families[0].as_object().at("name").as_string() ==
             "evidence_page");
  BOOST_TEST(result_families[1].as_object().at("name").as_string() ==
             "operation");
  BOOST_TEST(result_families[2].as_object().at("name").as_string() == "error");
}

BOOST_AUTO_TEST_CASE(
    mcp_protocol_rejects_invalid_information_family_allowlists) {
  const auto duplicate = [] {
    return std::make_unique<McpProtocol>(
        McpProtocolConfig{
            .bearer_token = std::string(kTestToken),
            .endpoint_path = "/mcp",
            .endpoint_port = 43123U,
            .allowed_operations = {},
            .allowed_information_families = {
                McpInformationFamily::kCapabilities,
                McpInformationFamily::kCapabilities},
            .read_only = false},
        McpToolHandler{}, McpResourceHandler{});
  };
  BOOST_CHECK_THROW(duplicate(), std::runtime_error);

  const auto invalid = [] {
    return std::make_unique<McpProtocol>(
        McpProtocolConfig{
            .bearer_token = std::string(kTestToken),
            .endpoint_path = "/mcp",
            .endpoint_port = 43123U,
            .allowed_operations = {},
            .allowed_information_families = {
                McpInformationFamily::kCount},
            .read_only = false},
        McpToolHandler{}, McpResourceHandler{});
  };
  BOOST_CHECK_THROW(invalid(), std::runtime_error);
}

BOOST_AUTO_TEST_CASE(
    mcp_protocol_session_lifecycle_is_transactional_and_cleanup_gated) {
  std::atomic<std::size_t> opened = 0U;
  std::atomic<std::size_t> closed = 0U;
  std::atomic<bool> reject_close = false;
  McpProtocol protocol(
      McpProtocolConfig{.bearer_token = std::string(kTestToken),
                        .endpoint_path = "/mcp",
                        .endpoint_port = 43123U,
                        .allowed_operations = {},
                        .allowed_information_families = {},
                        .read_only = false},
      {}, {}, [&](std::string_view, bool is_opened, std::stop_token) {
        if (is_opened) {
          opened.fetch_add(1U, std::memory_order_relaxed);
          return;
        }
        if (reject_close.load(std::memory_order_acquire)) {
          throw std::runtime_error("owned work is still active");
        }
        closed.fetch_add(1U, std::memory_order_relaxed);
      });
  const std::string session = Initialize(&protocol);
  MarkInitialized(&protocol, session);
  BOOST_TEST(opened.load(std::memory_order_relaxed) == 1U);

  reject_close.store(true, std::memory_order_release);
  const auto rejected =
      protocol.Handle(ProtocolRequest(http::verb::delete_, {}, session));
  BOOST_TEST(rejected.result() == http::status::service_unavailable);
  BOOST_TEST(protocol.Stats().sessions == 0U);
  BOOST_TEST(protocol.Stats().terminated_sessions == 1U);
  BOOST_TEST(closed.load(std::memory_order_relaxed) == 0U);

  reject_close.store(false, std::memory_order_release);
  BOOST_REQUIRE(WaitFor(
      [&] { return closed.load(std::memory_order_relaxed) == 1U; }, 2s));
  const auto already_removed =
      protocol.Handle(ProtocolRequest(http::verb::delete_, {}, session));
  BOOST_TEST(already_removed.result() == http::status::not_found);
  BOOST_TEST(protocol.Stats().sessions == 0U);
  BOOST_TEST(protocol.Stats().terminated_sessions == 1U);
  BOOST_TEST(closed.load(std::memory_order_relaxed) == 1U);
}

BOOST_AUTO_TEST_CASE(mcp_protocol_bounds_sessions_and_retained_notifications) {
  McpProtocol protocol = MakeProtocol();
  std::string first_session;
  for (std::size_t index = 0U; index < kMcpMaximumSessions; ++index) {
    const auto response = protocol.Handle(
        ProtocolRequest(http::verb::post, InitializeBody(index + 1U)));
    BOOST_REQUIRE(response.result() == http::status::ok);
    const std::string session(response.at("Mcp-Session-Id"));
    if (index == 0U) {
      first_session = session;
      MarkInitialized(&protocol, session);
    }
  }
  const auto rejected = protocol.Handle(ProtocolRequest(
      http::verb::post, InitializeBody(kMcpMaximumSessions + 1U)));
  BOOST_TEST(rejected.result() == http::status::service_unavailable);

  for (std::size_t index = 0U; index < kMcpMaximumNotificationsPerSession + 1U;
       ++index) {
    protocol.EnqueueNotification(
        first_session, kMcpOperationUpdatedNotification,
        boost::json::object{{"operation_id", index},
                            {"progress_completed", index}});
  }
  const McpProtocolStats stats = protocol.Stats();
  BOOST_TEST(stats.sessions == kMcpMaximumSessions);
  BOOST_TEST(stats.maximum_sessions == kMcpMaximumSessions);
  BOOST_TEST(stats.rejected_sessions == 1U);
  BOOST_TEST(stats.notifications_enqueued ==
             kMcpMaximumNotificationsPerSession + 1U);
  BOOST_TEST(stats.notifications_dropped == 1U);

  auto get = ProtocolRequest(http::verb::get, {}, first_session);
  get.set(http::field::accept, "text/event-stream");
  const auto events = protocol.Handle(get);
  BOOST_REQUIRE(events.result() == http::status::ok);
  BOOST_TEST(events.body().find("event: message") != std::string::npos);

  const auto deleted =
      protocol.Handle(ProtocolRequest(http::verb::delete_, {}, first_session));
  BOOST_TEST(deleted.result() == http::status::ok);
  BOOST_TEST(protocol.Handle(get).result() == http::status::not_found);
}

BOOST_AUTO_TEST_CASE(
    mcp_protocol_expires_only_sessions_without_initialized_notification) {
  std::atomic<std::size_t> opened = 0U;
  std::atomic<std::size_t> closed = 0U;
  McpProtocol* protocol_pointer = nullptr;
  McpProtocol protocol(
      McpProtocolConfig{.bearer_token = std::string(kTestToken),
                        .endpoint_path = "/mcp",
                        .endpoint_port = 43123U,
                        .uninitialized_session_timeout = 100ms,
                        .allowed_operations = {},
                        .allowed_information_families = {},
                        .read_only = false},
      {}, {}, [&](std::string_view, bool is_opened, std::stop_token) {
        if (is_opened) {
          opened.fetch_add(1U, std::memory_order_relaxed);
          return;
        }
        static_cast<void>(protocol_pointer->Stats());
        closed.fetch_add(1U, std::memory_order_relaxed);
      });
  protocol_pointer = &protocol;

  const std::string initialized = Initialize(&protocol);
  MarkInitialized(&protocol, initialized);
  const std::string abandoned = Initialize(&protocol);
  BOOST_TEST(protocol.Stats().sessions == 2U);

  BOOST_REQUIRE(
      WaitFor([&] { return protocol.Stats().expired_sessions == 1U; }, 500ms));
  const std::string replacement = Initialize(&protocol);
  BOOST_TEST(!replacement.empty());
  MarkInitialized(&protocol, replacement);

  const McpProtocolStats stats = protocol.Stats();
  BOOST_TEST(stats.sessions == 2U);
  BOOST_TEST(stats.initialized_sessions == 2U);
  BOOST_TEST(stats.terminated_sessions == 1U);
  BOOST_TEST(stats.expired_sessions == 1U);
  BOOST_TEST(opened.load(std::memory_order_relaxed) == 3U);
  BOOST_REQUIRE(WaitFor(
      [&] { return closed.load(std::memory_order_relaxed) == 1U; }, 500ms));
  BOOST_TEST(closed.load(std::memory_order_relaxed) == 1U);

  const auto initialized_ping = protocol.Handle(
      ProtocolRequest(http::verb::post, RequestBody(9U, "ping"), initialized));
  BOOST_TEST(initialized_ping.result() == http::status::ok);
  const auto abandoned_ping = protocol.Handle(
      ProtocolRequest(http::verb::post, InitializedBody(), abandoned));
  BOOST_TEST(abandoned_ping.result() == http::status::not_found);
}

BOOST_AUTO_TEST_CASE(
    mcp_protocol_starts_initialization_deadline_after_open_callback) {
  std::mutex callback_mutex;
  std::condition_variable callback_changed;
  bool callback_entered = false;
  bool release_open = false;
  std::atomic<bool> opening = false;
  std::atomic<std::size_t> overlapping_closes = 0U;
  std::atomic<std::size_t> closed = 0U;
  McpProtocol protocol(
      McpProtocolConfig{.bearer_token = std::string(kTestToken),
                        .endpoint_path = "/mcp",
                        .endpoint_port = 43123U,
                        .uninitialized_session_timeout = 5ms,
                        .allowed_operations = {},
                        .allowed_information_families = {},
                        .read_only = false},
      {}, {}, [&](std::string_view, bool is_opened, std::stop_token) {
        if (is_opened) {
          opening.store(true, std::memory_order_release);
          std::unique_lock<std::mutex> lock(callback_mutex);
          callback_entered = true;
          callback_changed.notify_all();
          callback_changed.wait(lock, [&] { return release_open; });
          opening.store(false, std::memory_order_release);
          return;
        }
        if (opening.load(std::memory_order_acquire)) {
          overlapping_closes.fetch_add(1U, std::memory_order_relaxed);
        }
        closed.fetch_add(1U, std::memory_order_relaxed);
      });

  std::optional<http::response<http::string_body>> response;
  std::exception_ptr failure;
  std::jthread initializer([&] {
    try {
      response = protocol.Handle(
          ProtocolRequest(http::verb::post, InitializeBody(1U)));
    } catch (...) {
      failure = std::current_exception();
    }
  });

  bool entered = false;
  {
    std::unique_lock<std::mutex> lock(callback_mutex);
    entered = callback_changed.wait_for(lock, 500ms,
                                        [&] { return callback_entered; });
  }
  std::this_thread::sleep_for(20ms);
  BOOST_TEST(closed.load(std::memory_order_relaxed) == 0U);
  {
    std::lock_guard<std::mutex> lock(callback_mutex);
    release_open = true;
  }
  callback_changed.notify_all();
  initializer.join();

  BOOST_REQUIRE(entered);
  BOOST_REQUIRE(failure == nullptr);
  BOOST_REQUIRE(response.has_value());
  BOOST_TEST(response->result() == http::status::ok);
  BOOST_REQUIRE(
      WaitFor([&] { return protocol.Stats().expired_sessions == 1U; }, 500ms));
  BOOST_TEST(overlapping_closes.load(std::memory_order_relaxed) == 0U);
  BOOST_REQUIRE(WaitFor(
      [&] { return closed.load(std::memory_order_relaxed) == 1U; }, 500ms));
  BOOST_TEST(closed.load(std::memory_order_relaxed) == 1U);
}

BOOST_AUTO_TEST_CASE(
    mcp_protocol_expiry_reclaims_capacity_when_cleanup_throws) {
  std::atomic<std::size_t> close_attempts = 0U;
  McpProtocol protocol(
      McpProtocolConfig{.bearer_token = std::string(kTestToken),
                        .endpoint_path = "/mcp",
                        .endpoint_port = 43123U,
                        .uninitialized_session_timeout = 1s,
                        .allowed_operations = {},
                        .allowed_information_families = {},
                        .read_only = false},
      {}, {}, [&](std::string_view, bool is_opened, std::stop_token) {
        if (!is_opened) {
          close_attempts.fetch_add(1U, std::memory_order_relaxed);
          throw std::runtime_error("cleanup still busy");
        }
      });

  for (std::size_t index = 0U; index < kMcpMaximumSessions; ++index) {
    static_cast<void>(Initialize(&protocol));
  }
  const auto full = protocol.Handle(ProtocolRequest(
      http::verb::post, InitializeBody(kMcpMaximumSessions + 1U)));
  BOOST_TEST(full.result() == http::status::service_unavailable);

  BOOST_REQUIRE(WaitFor([&] {
    const McpProtocolStats stats = protocol.Stats();
    return stats.sessions == 0U &&
           stats.expired_sessions == kMcpMaximumSessions;
  }));
  static_cast<void>(Initialize(&protocol));
  BOOST_REQUIRE(WaitFor(
      [&] {
        const McpProtocolStats stats = protocol.Stats();
        return stats.expired_sessions == kMcpMaximumSessions + 1U &&
               stats.failed_session_cleanups == kMcpMaximumSessions + 1U;
      },
      3s));
  BOOST_REQUIRE(WaitFor(
      [&] {
        return close_attempts.load(std::memory_order_relaxed) ==
               2U * (kMcpMaximumSessions + 1U) + 1U;
      },
      500ms));
  const std::string replacement = Initialize(&protocol);
  MarkInitialized(&protocol, replacement);
  BOOST_TEST(protocol.Stats().sessions == 1U);
}

BOOST_AUTO_TEST_CASE(
    mcp_protocol_shutdown_cancels_an_active_session_cleanup_callback) {
  std::atomic<bool> cleanup_entered = false;
  auto protocol = std::make_unique<McpProtocol>(
      McpProtocolConfig{.bearer_token = std::string(kTestToken),
                        .endpoint_path = "/mcp",
                        .endpoint_port = 43123U,
                        .uninitialized_session_timeout = 5ms,
                        .allowed_operations = {},
                        .allowed_information_families = {},
                        .read_only = false},
      McpToolHandler{}, McpResourceHandler{},
      [&](std::string_view, bool is_opened, std::stop_token stop_token) {
        if (is_opened) {
          return;
        }
        cleanup_entered.store(true, std::memory_order_release);
        while (!stop_token.stop_requested()) {
          std::this_thread::sleep_for(1ms);
        }
      });
  static_cast<void>(Initialize(protocol.get()));
  BOOST_REQUIRE(WaitFor(
      [&] { return cleanup_entered.load(std::memory_order_acquire); }, 500ms));

  const auto started = std::chrono::steady_clock::now();
  protocol.reset();
  const auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(
      std::chrono::steady_clock::now() - started);
  BOOST_TEST(elapsed.count() < 500);
}

BOOST_AUTO_TEST_CASE(
    mcp_protocol_sse_resume_is_stream_scoped_bounded_and_versioned) {
  McpProtocol protocol = MakeProtocol();
  const std::string newest_session =
      Initialize(&protocol, false, "2025-11-25");
  MarkInitialized(&protocol, newest_session, "2025-11-25");
  protocol.EnqueueNotification(
      newest_session, kMcpOperationUpdatedNotification,
      boost::json::object{{"operation_id", "operation-a"},
                          {"progress_completed", 1}});

  auto newest_get =
      ProtocolRequest(http::verb::get, {}, newest_session, kTestToken,
                      "2025-11-25");
  newest_get.set(http::field::accept, "text/event-stream");
  const auto first = protocol.Handle(newest_get);
  BOOST_REQUIRE(first.result() == http::status::ok);
  const std::vector<std::string> first_ids = SseEventIds(first.body());
  BOOST_REQUIRE_EQUAL(first_ids.size(), 2U);
  BOOST_TEST(first_ids[0].ends_with(".0"));
  BOOST_TEST(first_ids[1].ends_with(".1"));
  BOOST_TEST(first.body().find("data:\n\n") != std::string::npos);
  BOOST_TEST(first.body().find("retry: 1000\n\n") != std::string::npos);

  protocol.EnqueueNotification(
      newest_session, kMcpOperationUpdatedNotification,
      boost::json::object{{"operation_id", "operation-a"},
                          {"progress_completed", 2}});
  const auto second = protocol.Handle(newest_get);
  BOOST_REQUIRE(second.result() == http::status::ok);
  const std::vector<std::string> second_ids = SseEventIds(second.body());
  BOOST_REQUIRE_EQUAL(second_ids.size(), 2U);
  BOOST_TEST(second_ids[0].ends_with(".0"));
  BOOST_TEST(second_ids[1].ends_with(".2"));
  BOOST_TEST(first_ids[0].substr(0U, 32U) !=
             second_ids[0].substr(0U, 32U));
  BOOST_TEST(second.body().find("\"progress_completed\":1") ==
             std::string::npos);

  protocol.EnqueueNotification(
      newest_session, kMcpOperationUpdatedNotification,
      boost::json::object{{"operation_id", "operation-a"},
                          {"progress_completed", 3}});
  newest_get.set("Last-Event-ID", first_ids[1]);
  const auto resumed_first = protocol.Handle(newest_get);
  BOOST_REQUIRE(resumed_first.result() == http::status::ok);
  const std::vector<std::string> resumed_ids =
      SseEventIds(resumed_first.body());
  BOOST_REQUIRE_EQUAL(resumed_ids.size(), 1U);
  BOOST_TEST(resumed_ids.front().substr(0U, 32U) ==
             first_ids[0].substr(0U, 32U));
  BOOST_TEST(resumed_ids.front().ends_with(".3"));
  BOOST_TEST(resumed_first.body().find("\"progress_completed\":2") ==
             std::string::npos);

  newest_get.set("Last-Event-ID", second_ids[1]);
  const auto resumed_second = protocol.Handle(newest_get);
  BOOST_REQUIRE(resumed_second.result() == http::status::ok);
  BOOST_TEST(SseEventIds(resumed_second.body()).empty());
  BOOST_TEST(resumed_second.body() == "retry: 1000\n\n");

  newest_get.set("Last-Event-ID",
                 first_ids[0].substr(0U, 33U) + "2");
  BOOST_TEST(protocol.Handle(newest_get).result() ==
             http::status::bad_request);

  newest_get.erase("Last-Event-ID");
  for (std::size_t index = 2U;
       index <= kMcpMaximumSubscriptionsPerSession; ++index) {
    BOOST_REQUIRE(protocol.Handle(newest_get).result() == http::status::ok);
  }
  newest_get.set("Last-Event-ID", first_ids[1]);
  BOOST_TEST(protocol.Handle(newest_get).result() ==
             http::status::bad_request);

  const std::string stable_session =
      Initialize(&protocol, false, "2025-06-18");
  MarkInitialized(&protocol, stable_session, "2025-06-18");
  protocol.EnqueueNotification(
      stable_session, kMcpOperationUpdatedNotification,
      boost::json::object{{"operation_id", "operation-b"},
                          {"progress_completed", 1}});
  auto stable_get =
      ProtocolRequest(http::verb::get, {}, stable_session, kTestToken,
                      "2025-06-18");
  stable_get.set(http::field::accept, "text/event-stream");
  const auto stable = protocol.Handle(stable_get);
  BOOST_REQUIRE(stable.result() == http::status::ok);
  const std::vector<std::string> stable_ids = SseEventIds(stable.body());
  BOOST_REQUIRE_EQUAL(stable_ids.size(), 1U);
  BOOST_TEST(stable_ids.front().ends_with(".1"));
  BOOST_TEST(stable.body().find("data:\n\n") == std::string::npos);
  BOOST_TEST(stable.body().find("retry:") == std::string::npos);
  stable_get.set("Last-Event-ID", stable_ids.front());
  BOOST_TEST(protocol.Handle(stable_get).body() == ": bbp keepalive\n\n");
}

}  // namespace bbp
