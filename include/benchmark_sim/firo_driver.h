#pragma once

#include "benchmark_sim/http_client.h"
#include "benchmark_sim/process.h"

#include <cstdint>
#include <filesystem>
#include <optional>
#include <string>
#include <vector>

#include <boost/json/array.hpp>
#include <boost/json/value.hpp>

namespace bsim {

struct FiroMetrics {
  uint64_t height = 0;
  std::string best_hash;
  uint64_t peer_count = 0;
  uint64_t mempool_tx_count = 0;
  uint64_t mempool_bytes = 0;
  std::optional<bool> initial_block_download;
  std::optional<double> difficulty;
  uint64_t rpc_latency_ms = 0;
};

struct FiroNodeConfig {
  std::string id;
  std::filesystem::path binary;
  std::filesystem::path data_dir;
  std::filesystem::path log_dir;
  uint16_t p2p_port = 0;
  uint16_t rpc_port = 0;
  std::string rpc_user;
  std::string rpc_password;
  std::string rpc_host = "127.0.0.1";
  std::string rpc_bind = "127.0.0.1";
  std::vector<std::string> rpc_allow_ips = {"127.0.0.1"};
  std::string p2p_bind;
  bool listen = true;
  std::vector<std::string> connect_peers;
};

class FiroDriver {
 public:
  explicit FiroDriver(std::chrono::milliseconds rpc_timeout)
      : http_(rpc_timeout) {}

  ProcessSpec RenderProcess(const FiroNodeConfig& config) const;
  RpcEndpoint Endpoint(const FiroNodeConfig& config) const;
  void WaitReady(const FiroNodeConfig& config,
                 std::chrono::seconds timeout) const;
  void WaitForHeight(const FiroNodeConfig& config, uint64_t height,
                     std::chrono::seconds timeout) const;
  FiroMetrics ReadMetrics(const FiroNodeConfig& config) const;
  std::vector<std::string> GenerateBlocks(const FiroNodeConfig& config,
                                          uint32_t count,
                                          const std::string& address) const;
  void Stop(const FiroNodeConfig& config) const;

 private:
  boost::json::value RpcCall(const FiroNodeConfig& config,
                             std::string_view method,
                             const boost::json::array& params) const;

  HttpClient http_;
};

}  // namespace bsim
