#pragma once

#include <cstdint>
#include <filesystem>
#include <memory>
#include <string>
#include <string_view>
#include <vector>

#include "bbp/mcp_dispatcher.h"
#include "bbp/mcp_server.h"

namespace bbp {

inline constexpr std::string_view kMcpEndpointDirectory = "mcp";
inline constexpr std::string_view kMcpTokenFile = "token";
inline constexpr std::string_view kMcpClientConfigFile = "client.json";

struct McpEndpointConfig {
  std::filesystem::path state_directory;
  std::string run_id;
  McpServerConfig server;
  McpDispatcherConfig dispatcher;
  std::vector<McpOperationKind> allowed_operations;
  std::vector<McpInformationFamily> allowed_information_families;
  bool read_only = false;
};

struct McpEndpointPublication {
  std::string endpoint;
  std::uint16_t port = 0U;
  std::filesystem::path token_file;
  std::filesystem::path client_config_file;
};

// Owns the authenticated loopback listener and its short-lived credential
// publication. Stop removes and verifies every published credential before it
// returns; the private publication directory remains for the next BBP process.
class McpEndpoint {
 public:
  explicit McpEndpoint(McpEndpointConfig config,
                       McpApplicationOperationFactory operation_factory = {},
                       McpApplicationResourceReader resource_reader = {});
  ~McpEndpoint();

  McpEndpoint(const McpEndpoint&) = delete;
  McpEndpoint& operator=(const McpEndpoint&) = delete;

  void Start();
  // Stops listener admission and drains/cancels dispatcher work while retaining
  // the published client files for final application cleanup.
  void StopAdmissionAndDrain();
  void Stop();
  bool running() const;
  McpEndpointPublication publication() const;
  McpServerStats ServerStats() const;
  McpProtocolStats ProtocolStats() const;
  McpOperationServiceStats DispatcherStats() const;
  McpDispatcher& dispatcher();

 private:
  struct Impl;
  std::unique_ptr<Impl> impl_;
};

}  // namespace bbp
