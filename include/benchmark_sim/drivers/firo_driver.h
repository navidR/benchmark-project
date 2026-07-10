#pragma once

#include <boost/json/array.hpp>
#include <boost/json/value.hpp>
#include <string>

#include "benchmark_sim/drivers/chain_driver.h"
#include "benchmark_sim/http_client.h"

namespace bsim {

using FiroMetrics = ChainMetrics;
using FiroNodeConfig = ChainNodeConfig;
using FiroUtxo = ChainUtxo;
using FiroRawTransactionResult = ChainRawTransactionResult;
using FiroWalletTransactionResult = ChainWalletTransactionResult;
using WalletMode = ChainWalletMode;

class FiroDriver final : public ChainDriver {
 public:
  explicit FiroDriver(std::chrono::milliseconds rpc_timeout)
      : http_(rpc_timeout) {}

  ProcessSpec RenderProcess(const FiroNodeConfig& config) const override;
  std::optional<LogTailChunk> ReadLogTail(
      const FiroNodeConfig& config, ChainLogSource source,
      const LogTailCursor& cursor, std::uint64_t max_bytes) const override;
  RpcEndpoint Endpoint(const FiroNodeConfig& config) const override;
  void WaitReady(const FiroNodeConfig& config,
                 std::chrono::seconds timeout) const override;
  void WaitForHeight(const FiroNodeConfig& config, uint64_t height,
                     std::chrono::seconds timeout) const override;
  void WaitForPeerCount(const FiroNodeConfig& config, uint64_t peer_count,
                        std::chrono::seconds timeout) const override;
  void WaitForPeerAddress(const FiroNodeConfig& config,
                          const std::string& address,
                          std::chrono::seconds timeout) const override;
  void WaitForPeerAddressAbsent(const FiroNodeConfig& config,
                                const std::string& address,
                                std::chrono::seconds timeout) const override;
  FiroMetrics ReadMetrics(const FiroNodeConfig& config) const override;
  std::vector<std::string> PeerAddresses(
      const FiroNodeConfig& config) const override;
  std::vector<std::string> GenerateBlocks(
      const FiroNodeConfig& config, uint32_t count,
      const std::string& address) const override;
  std::string CreateWalletAddress(const FiroNodeConfig& config,
                                  WalletMode wallet_mode) const override;
  uint64_t WaitForWalletBalance(const FiroNodeConfig& config,
                                WalletMode wallet_mode,
                                uint64_t minimum_balance_satoshis,
                                uint64_t minimum_confirmations,
                                std::chrono::seconds timeout) const override;
  FiroUtxo FindSpendableOutput(const FiroNodeConfig& config,
                               const std::vector<std::string>& block_hashes,
                               const std::string& source_address,
                               uint64_t minimum_amount_satoshis,
                               uint64_t minimum_confirmations) const override;
  FiroRawTransactionResult SendRawTransaction(
      const FiroNodeConfig& config, const FiroUtxo& utxo,
      const std::string& source_address, const std::string& source_private_key,
      const std::string& destination_address, uint64_t amount_satoshis,
      uint64_t fee_satoshis, std::chrono::seconds timeout) const override;
  FiroWalletTransactionResult SendWalletTransaction(
      const FiroNodeConfig& config, WalletMode wallet_mode,
      const std::string& destination_address, uint64_t amount_satoshis,
      uint64_t fee_satoshis, std::chrono::seconds timeout) const override;
  uint64_t WaitForMempoolTransaction(
      const FiroNodeConfig& config, const std::string& txid,
      std::chrono::seconds timeout) const override;
  void ConnectPeer(const FiroNodeConfig& config,
                   const std::string& address) const override;
  void DisconnectPeer(const FiroNodeConfig& config,
                      const std::string& address) const override;
  void Stop(const FiroNodeConfig& config) const override;

 private:
  boost::json::value RpcCall(const FiroNodeConfig& config,
                             std::string_view method,
                             const boost::json::array& params) const;

  HttpClient http_;
};

}  // namespace bsim
