#pragma once

#include <chrono>
#include <cstdint>
#include <filesystem>
#include <optional>
#include <stdexcept>
#include <stop_token>
#include <string>
#include <string_view>
#include <vector>

#include "bbp/drivers/chain_wallet_snapshot.h"
#include "bbp/http_client.h"
#include "bbp/log_tail.h"
#include "bbp/mining_difficulty.h"
#include "bbp/process.h"

namespace bbp {

struct ChainMetrics {
  uint64_t version = 0;
  uint64_t protocol_version = 0;
  std::string subversion;
  uint64_t height = 0;
  std::string best_hash;
  uint64_t peer_count = 0;
  std::vector<std::string> peer_addresses;
  uint64_t mempool_tx_count = 0;
  uint64_t mempool_bytes = 0;
  std::optional<bool> initial_block_download;
  std::optional<double> difficulty;
  uint64_t rpc_latency_ms = 0;
};

struct ChainNodeConfig {
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
  std::string p2p_host = "127.0.0.1";
  std::string p2p_bind;
  bool listen = true;
  bool wallet_enabled = false;
  std::vector<std::string> connect_peers;
};

struct ChainUtxo {
  std::string txid;
  uint32_t vout = 0;
  uint64_t amount_satoshis = 0;
  std::string amount;
  std::string script_pub_key;
  std::string block_hash;
  uint64_t confirmations = 0;
};

struct ChainRawTransactionResult {
  ChainUtxo utxo;
  std::string raw_hex;
  std::string signed_hex;
  std::string txid;
  std::string destination_amount;
  std::string change_amount;
  std::string fee;
  uint64_t mempool_size = 0;
};

enum class ChainWalletMode {
  kPublic,
  kPrivate,
};

enum class ChainLogSource {
  kDaemon,
  kStdout,
  kStderr,
};

enum class ChainLogVerbosityChange {
  kIncrease,
  kDecrease,
};

std::string_view ChainLogSourceName(ChainLogSource source);

class UnsupportedChainOperation : public std::runtime_error {
 public:
  UnsupportedChainOperation(std::string_view chain, std::string_view operation);
};

struct ChainWalletTransactionResult {
  std::vector<std::string> txids;
  std::string destination_amount;
  std::string requested_fee_rate;
  uint64_t mempool_size = 0;
};

class ChainDriver {
 public:
  virtual ~ChainDriver() = default;

  virtual ProcessSpec RenderProcess(const ChainNodeConfig& config) const = 0;
  virtual std::optional<LogTailChunk> ReadLogTail(
      const ChainNodeConfig& config, ChainLogSource source,
      const LogTailCursor& cursor, std::uint64_t max_bytes) const = 0;
  virtual RpcEndpoint Endpoint(const ChainNodeConfig& config) const = 0;
  virtual void WaitReady(const ChainNodeConfig& config,
                         std::chrono::seconds timeout,
                         std::stop_token stop_token = {}) const = 0;
  virtual void WaitForHeight(const ChainNodeConfig& config, uint64_t height,
                             std::chrono::seconds timeout,
                             std::stop_token stop_token = {}) const = 0;
  virtual void WaitForPeerCount(const ChainNodeConfig& config,
                                uint64_t peer_count,
                                std::chrono::seconds timeout,
                                std::stop_token stop_token = {}) const = 0;
  virtual void WaitForPeerAddress(const ChainNodeConfig& config,
                                  const std::string& address,
                                  std::chrono::seconds timeout,
                                  std::stop_token stop_token = {}) const = 0;
  virtual void WaitForPeerAddressAbsent(
      const ChainNodeConfig& config, const std::string& address,
      std::chrono::seconds timeout, std::stop_token stop_token = {}) const = 0;
  virtual ChainMetrics ReadMetrics(const ChainNodeConfig& config,
                                   std::stop_token stop_token = {}) const = 0;
  virtual std::vector<std::string> PeerAddresses(
      const ChainNodeConfig& config, std::stop_token stop_token = {}) const = 0;
  virtual std::vector<std::string> ConnectedPeerAddresses(
      const ChainNodeConfig& config,
      const std::vector<std::string>& candidate_addresses,
      std::stop_token stop_token = {}) const = 0;
  virtual std::vector<std::string> GenerateBlocks(
      const ChainNodeConfig& config, uint32_t count, const std::string& address,
      std::stop_token stop_token = {}) const = 0;
  virtual std::uint64_t ReadBlockNonRewardTransactionCount(
      const ChainNodeConfig& config, const std::string& block_hash,
      std::stop_token stop_token = {}) const = 0;
  virtual std::string CreateWalletAddress(
      const ChainNodeConfig& config, ChainWalletMode wallet_mode,
      std::stop_token stop_token = {}) const = 0;
  virtual uint64_t WaitForWalletBalance(
      const ChainNodeConfig& config, ChainWalletMode wallet_mode,
      uint64_t minimum_balance_satoshis, uint64_t minimum_confirmations,
      std::chrono::seconds timeout, std::stop_token stop_token = {}) const = 0;
  virtual ChainWalletSnapshot ReadWalletSnapshot(
      const ChainNodeConfig& config, ChainWalletMode wallet_mode,
      std::uint32_t transaction_limit,
      std::stop_token stop_token = {}) const = 0;
  virtual ChainUtxo FindSpendableOutput(
      const ChainNodeConfig& config,
      const std::vector<std::string>& block_hashes,
      const std::string& source_address, uint64_t minimum_amount_satoshis,
      uint64_t minimum_confirmations,
      std::stop_token stop_token = {}) const = 0;
  virtual ChainRawTransactionResult SendRawTransaction(
      const ChainNodeConfig& config, const ChainUtxo& utxo,
      const std::string& source_address, const std::string& source_private_key,
      const std::string& destination_address, uint64_t amount_satoshis,
      uint64_t fee_satoshis, std::chrono::seconds timeout,
      std::stop_token stop_token = {}) const = 0;
  virtual ChainWalletTransactionResult SendWalletTransaction(
      const ChainNodeConfig& config, ChainWalletMode wallet_mode,
      const std::string& destination_address, uint64_t amount_satoshis,
      uint64_t fee_satoshis, std::chrono::seconds timeout,
      std::stop_token stop_token = {}) const = 0;
  virtual uint64_t WaitForMempoolTransaction(
      const ChainNodeConfig& config, const std::string& txid,
      std::chrono::seconds timeout, std::stop_token stop_token = {}) const = 0;
  virtual void ConnectPeer(const ChainNodeConfig& config,
                           const std::string& address,
                           std::stop_token stop_token = {}) const = 0;
  virtual void DisconnectPeer(const ChainNodeConfig& config,
                              const std::string& address,
                              std::stop_token stop_token = {}) const = 0;
  virtual void ChangeLogVerbosity(const ChainNodeConfig& config,
                                  ChainLogVerbosityChange change,
                                  std::stop_token stop_token = {}) const = 0;
  virtual void SetMiningDifficulty(const ChainNodeConfig& config,
                                   MiningDifficulty difficulty,
                                   std::stop_token stop_token = {}) const = 0;
  virtual void StartMining(const ChainNodeConfig& config,
                           const std::string& reward_address,
                           std::stop_token stop_token = {}) const = 0;
  virtual void StopMining(const ChainNodeConfig& config,
                          std::stop_token stop_token = {}) const = 0;
  virtual void SetNetworkActive(const ChainNodeConfig& config, bool active,
                                std::stop_token stop_token = {}) const = 0;
  virtual void Stop(const ChainNodeConfig& config,
                    std::stop_token stop_token = {}) const = 0;
};

}  // namespace bbp
