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
    std::string_view token = kTestToken) {
  http::request<http::string_body> request{method, "/mcp", 11};
  request.set(http::field::host, "127.0.0.1:43123");
  request.set(http::field::authorization, "Bearer " + std::string(token));
  request.set(http::field::accept, "application/json, text/event-stream");
  request.set(http::field::content_type, "application/json");
  if (!session.empty()) {
    request.set("Mcp-Session-Id", session);
    request.set("MCP-Protocol-Version", kMcpProtocolVersion);
  }
  request.body() = std::move(body);
  request.prepare_payload();
  return request;
}

std::string InitializeBody(std::uint64_t id) {
  return boost::json::serialize(boost::json::object{
      {"jsonrpc", "2.0"},
      {"id", id},
      {"method", "initialize"},
      {"params", boost::json::object{
                     {"protocolVersion", kMcpProtocolVersion},
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

std::string RequestBody(std::uint64_t id, std::string_view method,
                        boost::json::object params = {}) {
  return boost::json::serialize(
      boost::json::object{{"jsonrpc", "2.0"},
                          {"id", id},
                          {"method", method},
                          {"params", std::move(params)}});
}

std::string Initialize(McpProtocol* protocol) {
  const auto response =
      protocol->Handle(ProtocolRequest(http::verb::post, InitializeBody(1U)));
  BOOST_REQUIRE(response.result() == http::status::ok);
  const auto field = response.find("Mcp-Session-Id");
  BOOST_REQUIRE(static_cast<bool>(field != response.end()));
  const std::string session(field->value());
  BOOST_REQUIRE_EQUAL(session.size(), 48U);
  return session;
}

void MarkInitialized(McpProtocol* protocol, std::string_view session) {
  const auto response = protocol->Handle(
      ProtocolRequest(http::verb::post, InitializedBody(), session));
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
                        .endpoint_port = 43123U},
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

BOOST_AUTO_TEST_CASE(mcp_protocol_dispatches_registered_tools_and_resources) {
  std::size_t tool_calls = 0U;
  McpProtocol protocol = MakeProtocol(&tool_calls);
  const std::string session = Initialize(&protocol);
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
    mcp_protocol_session_lifecycle_is_transactional_and_cleanup_gated) {
  std::atomic<std::size_t> opened = 0U;
  std::atomic<std::size_t> closed = 0U;
  std::atomic<bool> reject_close = false;
  McpProtocol protocol(
      McpProtocolConfig{.bearer_token = std::string(kTestToken),
                        .endpoint_path = "/mcp",
                        .endpoint_port = 43123U},
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
        first_session, "notifications/progress",
        boost::json::object{{"progressToken", index}, {"progress", index}});
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
                        .uninitialized_session_timeout = 100ms},
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
                        .uninitialized_session_timeout = 5ms},
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
                        .uninitialized_session_timeout = 100ms},
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
                        .uninitialized_session_timeout = 5ms},
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

BOOST_AUTO_TEST_CASE(mcp_protocol_reconnect_resumes_after_event_cursor) {
  McpProtocol protocol = MakeProtocol();
  const std::string session = Initialize(&protocol);
  MarkInitialized(&protocol, session);
  protocol.EnqueueNotification(
      session, "notifications/progress",
      boost::json::object{{"progressToken", "operation-a"}, {"progress", 1}});
  protocol.EnqueueNotification(
      session, "notifications/progress",
      boost::json::object{{"progressToken", "operation-a"}, {"progress", 2}});

  auto get = ProtocolRequest(http::verb::get, {}, session);
  get.set(http::field::accept, "text/event-stream");
  const auto initial = protocol.Handle(get);
  BOOST_REQUIRE(initial.result() == http::status::ok);
  BOOST_TEST(initial.body().find("id: 1\n") != std::string::npos);
  BOOST_TEST(initial.body().find("id: 2\n") != std::string::npos);

  get.set("Last-Event-ID", "1");
  const auto resumed = protocol.Handle(get);
  BOOST_REQUIRE(resumed.result() == http::status::ok);
  BOOST_TEST(resumed.body().find("id: 1\n") == std::string::npos);
  BOOST_TEST(resumed.body().find("id: 2\n") != std::string::npos);

  get.set("Last-Event-ID", "2");
  const auto caught_up = protocol.Handle(get);
  BOOST_REQUIRE(caught_up.result() == http::status::ok);
  BOOST_TEST(caught_up.body() == ": bbp keepalive\n\n");
}

}  // namespace bbp
