#pragma once

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>

#include "bbp/mcp_protocol.h"

namespace bbp {

inline constexpr std::size_t kMcpHttpWorkerCount = 4U;
inline constexpr std::size_t kMcpMaximumPendingConnections = 16U;

struct McpServerConfig {
  std::string bind_address = "127.0.0.1";
  std::uint16_t port = 0U;
  std::size_t worker_count = kMcpHttpWorkerCount;
  std::size_t pending_connection_capacity = kMcpMaximumPendingConnections;
  std::chrono::milliseconds request_timeout = std::chrono::seconds(5);
};

struct McpServerStats {
  bool running = false;
  std::size_t active_connections = 0U;
  std::size_t queued_connections = 0U;
  std::size_t maximum_queued_connections = 0U;
  std::uint64_t accepted_connections = 0U;
  std::uint64_t rejected_connections = 0U;
  std::uint64_t completed_connections = 0U;
};

class McpServer {
 public:
  McpServer(McpServerConfig config, McpProtocolConfig protocol_config,
            McpToolHandler tool_handler, McpResourceHandler resource_handler);
  ~McpServer();

  McpServer(const McpServer&) = delete;
  McpServer& operator=(const McpServer&) = delete;

  void Start();
  void Stop();
  std::uint16_t port() const;
  std::string endpoint() const;
  McpServerStats Stats() const;
  McpProtocol& protocol();

 private:
  struct Impl;
  std::unique_ptr<Impl> impl_;
};

}  // namespace bbp
