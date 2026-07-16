#pragma once

#include <boost/json/array.hpp>
#include <boost/json/value.hpp>
#include <stop_token>
#include <string>

#include "bbp/drivers/chain_driver.h"
#include "bbp/http_client.h"

namespace bbp {

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
  void WaitReady(const FiroNodeConfig& config, std::chrono::seconds timeout,
                 std::stop_token stop_token = {}) const override;
  void WaitForHeight(const FiroNodeConfig& config, uint64_t height,
                     std::chrono::seconds timeout,
                     std::stop_token stop_token = {}) const override;
  void WaitForPeerCount(const FiroNodeConfig& config, uint64_t peer_count,
                        std::chrono::seconds timeout,
                        std::stop_token stop_token = {}) const override;
  void WaitForPeerAddress(const FiroNodeConfig& config,
                          const std::string& address,
                          std::chrono::seconds timeout,
                          std::stop_token stop_token = {}) const override;
  void WaitForPeerAddressAbsent(const FiroNodeConfig& config,
                                const std::string& address,
                                std::chrono::seconds timeout,
                                std::stop_token stop_token = {}) const override;
  FiroMetrics ReadMetrics(const FiroNodeConfig& config,
                          std::stop_token stop_token = {}) const override;
  std::vector<std::string> PeerAddresses(
      const FiroNodeConfig& config,
      std::stop_token stop_token = {}) const override;
  std::vector<std::string> ConnectedPeerAddresses(
      const FiroNodeConfig& config,
      const std::vector<std::string>& candidate_addresses,
      std::stop_token stop_token = {}) const override;
  std::vector<std::string> GenerateBlocks(
      const FiroNodeConfig& config, uint32_t count, const std::string& address,
      std::stop_token stop_token = {}) const override;
  std::uint64_t ReadBlockNonRewardTransactionCount(
      const FiroNodeConfig& config, const std::string& block_hash,
      std::stop_token stop_token = {}) const override;
  std::string CreateWalletAddress(
      const FiroNodeConfig& config, WalletMode wallet_mode,
      std::stop_token stop_token = {}) const override;
  std::string CreateWalletFundingAddress(
      const FiroNodeConfig& config, WalletMode wallet_mode,
      const std::string& wallet_address,
      std::stop_token stop_token = {}) const override;
  ChainWalletFundingResult PrepareWalletFunding(
      const FiroNodeConfig& config, WalletMode wallet_mode,
      const std::string& wallet_address, std::uint64_t minimum_balance_satoshis,
      std::uint64_t minimum_confirmations, std::chrono::seconds timeout,
      std::stop_token stop_token = {}) const override;
  uint64_t WaitForWalletBalance(const FiroNodeConfig& config,
                                WalletMode wallet_mode,
                                uint64_t minimum_balance_satoshis,
                                uint64_t minimum_confirmations,
                                std::chrono::seconds timeout,
                                std::stop_token stop_token = {}) const override;
  ChainWalletSnapshot ReadWalletSnapshot(
      const FiroNodeConfig& config, WalletMode wallet_mode,
      std::uint32_t transaction_limit,
      std::stop_token stop_token = {}) const override;
  FiroUtxo FindSpendableOutput(const FiroNodeConfig& config,
                               const std::vector<std::string>& block_hashes,
                               const std::string& source_address,
                               uint64_t minimum_amount_satoshis,
                               uint64_t minimum_confirmations,
                               std::stop_token stop_token = {}) const override;
  FiroRawTransactionResult SendRawTransaction(
      const FiroNodeConfig& config, const FiroUtxo& utxo,
      const std::string& source_address, const std::string& source_private_key,
      const std::string& destination_address, uint64_t amount_satoshis,
      uint64_t fee_satoshis, std::chrono::seconds timeout,
      std::stop_token stop_token = {}) const override;
  FiroWalletTransactionResult SendWalletTransaction(
      const FiroNodeConfig& config, WalletMode wallet_mode,
      const std::string& destination_address, uint64_t amount_satoshis,
      uint64_t fee_satoshis, std::chrono::seconds timeout,
      std::stop_token stop_token = {}) const override;
  ChainTransactionObservation ObserveTransaction(
      const FiroNodeConfig& config, const std::string& txid,
      std::stop_token stop_token = {}) const override;
  ChainTransactionObservation WaitForTransaction(
      const FiroNodeConfig& config, const std::string& txid,
      std::chrono::seconds timeout,
      std::stop_token stop_token = {}) const override;
  uint64_t WaitForMempoolTransaction(
      const FiroNodeConfig& config, const std::string& txid,
      std::chrono::seconds timeout,
      std::stop_token stop_token = {}) const override;
  void ConnectPeer(const FiroNodeConfig& config, const std::string& address,
                   std::stop_token stop_token = {}) const override;
  void DisconnectPeer(const FiroNodeConfig& config, const std::string& address,
                      std::stop_token stop_token = {}) const override;
  void ChangeLogVerbosity(const FiroNodeConfig& config,
                          ChainLogVerbosityChange change,
                          std::stop_token stop_token = {}) const override;
  void SetMiningDifficulty(const FiroNodeConfig& config,
                           MiningDifficulty difficulty,
                           std::stop_token stop_token = {}) const override;
  void StartMining(const FiroNodeConfig& config,
                   const std::string& reward_address,
                   std::stop_token stop_token = {}) const override;
  void StopMining(const FiroNodeConfig& config,
                  std::stop_token stop_token = {}) const override;
  void SetNetworkActive(const FiroNodeConfig& config, bool active,
                        std::stop_token stop_token = {}) const override;
  void Stop(const FiroNodeConfig& config,
            std::stop_token stop_token = {}) const override;

 private:
  boost::json::value RpcCall(const FiroNodeConfig& config,
                             std::string_view method,
                             const boost::json::array& params,
                             std::stop_token stop_token = {}) const;

  HttpClient http_;
};

}  // namespace bbp
