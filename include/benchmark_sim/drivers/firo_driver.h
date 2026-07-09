#pragma once

#include <boost/json/array.hpp>
#include <boost/json/value.hpp>
#include <cstdint>
#include <filesystem>
#include <optional>
#include <string>
#include <vector>

#include "benchmark_sim/http_client.h"
#include "benchmark_sim/process.h"

namespace bsim {

struct FiroMetrics {
  uint64_t version = 0;
  uint64_t protocol_version = 0;
  std::string subversion;
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

struct FiroUtxo {
  std::string txid;
  uint32_t vout = 0;
  uint64_t amount_satoshis = 0;
  std::string amount;
  std::string script_pub_key;
  std::string block_hash;
  uint64_t confirmations = 0;
};

struct FiroRawTransactionResult {
  FiroUtxo utxo;
  std::string raw_hex;
  std::string signed_hex;
  std::string txid;
  std::string destination_amount;
  std::string change_amount;
  std::string fee;
  uint64_t mempool_size = 0;
};

class FiroDriver {
 public:
  explicit FiroDriver(std::chrono::milliseconds rpc_timeout)
      : http_(rpc_timeout) {}

  ProcessSpec RenderProcess(const FiroNodeConfig& config) const;
  std::filesystem::path LogPath(const FiroNodeConfig& config) const;
  RpcEndpoint Endpoint(const FiroNodeConfig& config) const;
  void WaitReady(const FiroNodeConfig& config,
                 std::chrono::seconds timeout) const;
  void WaitForHeight(const FiroNodeConfig& config, uint64_t height,
                     std::chrono::seconds timeout) const;
  void WaitForPeerCount(const FiroNodeConfig& config, uint64_t peer_count,
                        std::chrono::seconds timeout) const;
  void WaitForPeerAddress(const FiroNodeConfig& config,
                          const std::string& address,
                          std::chrono::seconds timeout) const;
  void WaitForPeerAddressAbsent(const FiroNodeConfig& config,
                                const std::string& address,
                                std::chrono::seconds timeout) const;
  FiroMetrics ReadMetrics(const FiroNodeConfig& config) const;
  std::vector<std::string> PeerAddresses(const FiroNodeConfig& config) const;
  std::vector<std::string> GenerateBlocks(const FiroNodeConfig& config,
                                          uint32_t count,
                                          const std::string& address) const;
  FiroUtxo FindSpendableOutput(const FiroNodeConfig& config,
                               const std::vector<std::string>& block_hashes,
                               const std::string& source_address,
                               uint64_t minimum_amount_satoshis,
                               uint64_t minimum_confirmations) const;
  FiroRawTransactionResult SendRawTransaction(
      const FiroNodeConfig& config, const FiroUtxo& utxo,
      const std::string& source_address, const std::string& source_private_key,
      const std::string& destination_address, uint64_t amount_satoshis,
      uint64_t fee_satoshis, std::chrono::seconds timeout) const;
  uint64_t WaitForMempoolTransaction(const FiroNodeConfig& config,
                                     const std::string& txid,
                                     std::chrono::seconds timeout) const;
  void ConnectPeer(const FiroNodeConfig& config,
                   const std::string& address) const;
  void DisconnectPeer(const FiroNodeConfig& config,
                      const std::string& address) const;
  void Stop(const FiroNodeConfig& config) const;

 private:
  boost::json::value RpcCall(const FiroNodeConfig& config,
                             std::string_view method,
                             const boost::json::array& params) const;

  HttpClient http_;
};

}  // namespace bsim
