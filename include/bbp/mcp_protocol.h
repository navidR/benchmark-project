#pragma once

#include <boost/beast/http/message.hpp>
#include <boost/beast/http/string_body.hpp>
#include <boost/json/value.hpp>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <memory>
#include <stop_token>
#include <string>
#include <string_view>

namespace bbp {

namespace mcp_http = boost::beast::http;

inline constexpr std::size_t kMcpMaximumRequestBytes = 1024U * 1024U;
inline constexpr std::size_t kMcpMaximumClientNameBytes = 128U;

struct McpProtocolStats {
  std::size_t sessions = 0U;
  std::size_t maximum_sessions = 0U;
  std::uint64_t initialized_sessions = 0U;
  std::uint64_t terminated_sessions = 0U;
  std::uint64_t expired_sessions = 0U;
  std::uint64_t failed_session_cleanups = 0U;
  std::uint64_t rejected_sessions = 0U;
  std::uint64_t requests = 0U;
  std::uint64_t malformed_requests = 0U;
  std::uint64_t authentication_failures = 0U;
  std::uint64_t origin_failures = 0U;
  std::uint64_t notifications_enqueued = 0U;
  std::uint64_t notifications_dropped = 0U;
};

struct McpProtocolConfig {
  std::string bearer_token;
  std::string endpoint_path = "/mcp";
  std::uint16_t endpoint_port = 0U;
  std::chrono::milliseconds uninitialized_session_timeout =
      std::chrono::seconds(30);
};

using McpToolHandler = std::function<boost::json::value(
    std::string_view, const boost::json::object&, std::string_view,
    std::stop_token)>;
using McpResourceHandler = std::function<boost::json::value(
    std::string_view, std::string_view, std::stop_token)>;
// Automatic expiry may retry opened=false; close handling must be idempotent.
using McpSessionHandler =
    std::function<void(std::string_view, bool /* opened */)>;

class McpProtocol {
 public:
  McpProtocol(McpProtocolConfig config, McpToolHandler tool_handler,
              McpResourceHandler resource_handler,
              McpSessionHandler session_handler = {});
  ~McpProtocol();

  McpProtocol(const McpProtocol&) = delete;
  McpProtocol& operator=(const McpProtocol&) = delete;

  mcp_http::response<mcp_http::string_body> Handle(
      const mcp_http::request<mcp_http::string_body>& request,
      std::stop_token stop_token = {});

  void EnqueueNotification(std::string_view session_id, std::string_view method,
                           boost::json::value params);
  void BroadcastNotification(std::string_view method,
                             const boost::json::value& params);
  void SetEndpointPort(std::uint16_t endpoint_port);
  McpProtocolStats Stats() const;

 private:
  struct Impl;
  std::unique_ptr<Impl> impl_;
};

}  // namespace bbp
