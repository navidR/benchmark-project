#pragma once

#include <memory>

#include "bbp/drivers/chain_driver.h"

namespace bbp {

class BitcoinDriver final : public ChainDriver {
 public:
  explicit BitcoinDriver(std::chrono::milliseconds rpc_timeout);

  ProcessSpec RenderProcess(const ChainNodeConfig& config) const override;
  std::optional<LogTailChunk> ReadLogTail(
      const ChainNodeConfig& config, ChainLogSource source,
      const LogTailCursor& cursor, std::uint64_t max_bytes) const override;
  RpcEndpoint Endpoint(const ChainNodeConfig& config) const override;
  void WaitReady(const ChainNodeConfig& config, std::chrono::seconds timeout,
                 std::stop_token stop_token = {}) const override;
  void WaitForHeight(const ChainNodeConfig& config, std::uint64_t height,
                     std::chrono::seconds timeout,
                     std::stop_token stop_token = {}) const override;
  void WaitForPeerCount(const ChainNodeConfig& config, std::uint64_t peer_count,
                        std::chrono::seconds timeout,
                        std::stop_token stop_token = {}) const override;
  void WaitForPeerAddress(const ChainNodeConfig& config,
                          const std::string& address,
                          std::chrono::seconds timeout,
                          std::stop_token stop_token = {}) const override;
  void WaitForPeerAddressAbsent(const ChainNodeConfig& config,
                                const std::string& address,
                                std::chrono::seconds timeout,
                                std::stop_token stop_token = {}) const override;
  ChainMetrics ReadMetrics(const ChainNodeConfig& config,
                           std::stop_token stop_token = {}) const override;
  std::vector<std::string> PeerAddresses(
      const ChainNodeConfig& config,
      std::stop_token stop_token = {}) const override;
  std::vector<std::string> ConnectedPeerAddresses(
      const ChainNodeConfig& config,
      const std::vector<std::string>& candidate_addresses,
      std::stop_token stop_token = {}) const override;
  std::vector<std::string> GenerateBlocks(
      const ChainNodeConfig& config, std::uint32_t count,
      const std::string& address,
      std::stop_token stop_token = {}) const override;
  std::uint64_t ReadBlockNonRewardTransactionCount(
      const ChainNodeConfig& config, const std::string& block_hash,
      std::stop_token stop_token = {}) const override;
  std::string CreateWalletAddress(
      const ChainNodeConfig& config, ChainWalletMode wallet_mode,
      std::stop_token stop_token = {}) const override;
  std::string CreateWalletFundingAddress(
      const ChainNodeConfig& config, ChainWalletMode wallet_mode,
      const std::string& wallet_address,
      std::stop_token stop_token = {}) const override;
  ChainWalletFundingResult PrepareWalletFunding(
      const ChainNodeConfig& config, ChainWalletMode wallet_mode,
      const std::string& wallet_address, std::uint64_t minimum_balance_satoshis,
      std::uint64_t minimum_confirmations, std::chrono::seconds timeout,
      std::stop_token stop_token = {}) const override;
  std::uint64_t WaitForWalletBalance(
      const ChainNodeConfig& config, ChainWalletMode wallet_mode,
      std::uint64_t minimum_balance_satoshis,
      std::uint64_t minimum_confirmations, std::chrono::seconds timeout,
      std::stop_token stop_token = {}) const override;
  ChainWalletSnapshot ReadWalletSnapshot(
      const ChainNodeConfig& config, ChainWalletMode wallet_mode,
      std::uint32_t transaction_limit,
      std::stop_token stop_token = {}) const override;
  ChainUtxo FindSpendableOutput(const ChainNodeConfig& config,
                                const std::vector<std::string>& block_hashes,
                                const std::string& source_address,
                                std::uint64_t minimum_amount_satoshis,
                                std::uint64_t minimum_confirmations,
                                std::stop_token stop_token = {}) const override;
  ChainRawTransactionResult SendRawTransaction(
      const ChainNodeConfig& config, const ChainUtxo& utxo,
      const std::string& source_address, const std::string& source_private_key,
      const std::string& destination_address, std::uint64_t amount_satoshis,
      std::uint64_t fee_satoshis, std::chrono::seconds timeout,
      std::stop_token stop_token = {}) const override;
  ChainWalletTransactionResult SendWalletTransaction(
      const ChainNodeConfig& config, ChainWalletMode wallet_mode,
      const std::string& destination_address, std::uint64_t amount_satoshis,
      std::uint64_t fee_satoshis, std::chrono::seconds timeout,
      std::stop_token stop_token = {}) const override;
  ChainTransactionObservation ObserveTransaction(
      const ChainNodeConfig& config, const std::string& txid,
      std::stop_token stop_token = {}) const override;
  ChainTransactionObservation WaitForTransaction(
      const ChainNodeConfig& config, const std::string& txid,
      std::chrono::seconds timeout,
      std::stop_token stop_token = {}) const override;
  std::uint64_t WaitForMempoolTransaction(
      const ChainNodeConfig& config, const std::string& txid,
      std::chrono::seconds timeout,
      std::stop_token stop_token = {}) const override;
  void ConnectPeer(const ChainNodeConfig& config, const std::string& address,
                   std::stop_token stop_token = {}) const override;
  void DisconnectPeer(const ChainNodeConfig& config, const std::string& address,
                      std::stop_token stop_token = {}) const override;
  void ChangeLogVerbosity(const ChainNodeConfig& config,
                          ChainLogVerbosityChange change,
                          std::stop_token stop_token = {}) const override;
  void SetMiningDifficulty(const ChainNodeConfig& config,
                           MiningDifficulty difficulty,
                           std::stop_token stop_token = {}) const override;
  void StartMining(const ChainNodeConfig& config,
                   const std::string& reward_address,
                   std::stop_token stop_token = {}) const override;
  void StopMining(const ChainNodeConfig& config,
                  std::stop_token stop_token = {}) const override;
  void SetNetworkActive(const ChainNodeConfig& config, bool active,
                        std::stop_token stop_token = {}) const override;
  void Stop(const ChainNodeConfig& config,
            std::stop_token stop_token = {}) const override;
  void CleanupRpcCredentials(const ChainNodeConfig& config) const override;

 private:
  std::unique_ptr<ChainDriver> bitcoin_family_rpc_;
};

}  // namespace bbp
