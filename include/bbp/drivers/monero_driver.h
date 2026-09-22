#pragma once

#include <boost/json/object.hpp>
#include <memory>

#include "bbp/drivers/chain_driver.h"

namespace bbp {

class MoneroDriver final : public ChainDriver {
 public:
  explicit MoneroDriver(std::chrono::milliseconds rpc_timeout);

  ProcessSpec RenderProcess(const ChainNodeConfig& config) const override;
  std::vector<ProcessSpec> RenderCompanionProcesses(
      const ChainNodeConfig& config) const override;
  bool SupportsWalletTransactionMode(ChainWalletMode mode) const override;
  std::uint64_t WalletTransactionFeeReserveSatoshis(
      ChainWalletMode mode, std::uint64_t requested_fee_rate_satoshis) const override;
  bool ValidateTargetAddress(const ChainNodeConfig& config,
                             const std::string& address,
                             std::stop_token stop_token = {}) const override;
  ChainWalletTransactionResult SubmitWalletTransaction(
      const ChainNodeConfig& config, ChainWalletMode wallet_mode,
      const std::string& destination_address, std::uint64_t amount_satoshis,
      std::uint64_t fee_satoshis, std::chrono::seconds timeout,
      std::stop_token stop_token = {}) const override;
  std::optional<LogTailChunk> ReadLogTail(
      const ChainNodeConfig& config, ChainLogSource source,
      const LogTailCursor& cursor, std::uint64_t max_bytes) const override;
  RpcEndpoint Endpoint(const ChainNodeConfig& config) const override;
  void WaitReady(const ChainNodeConfig& config, std::chrono::seconds timeout,
                 std::stop_token stop_token = {}) const override;
  void WaitForHeight(const ChainNodeConfig& config, std::uint64_t height,
                     std::chrono::seconds timeout,
                     std::stop_token stop_token = {}) const override;
  std::uint64_t WaitForPeerCount(
      const ChainNodeConfig& config, std::uint64_t peer_count,
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
  ChainBlockSummary ReadChainTip(
      const ChainNodeConfig& config,
      std::stop_token stop_token = {}) const override;
  ChainBlockSummary ReadBlockSummary(
      const ChainNodeConfig& config, std::uint64_t height,
      std::stop_token stop_token = {}) const override;
  ChainBlockDetail ReadBlockDetail(
      const ChainNodeConfig& config, const std::string& hash,
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
      std::stop_token stop_token = {},
      const ChainRawTransactionBroadcastControl* broadcast_control =
          nullptr) const override;
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
  static void ValidateDigestConfiguration(const ChainNodeConfig& config);
  boost::json::object WalletRpcCall(
      const ChainNodeConfig& config, std::string_view method,
      const boost::json::object& params, std::stop_token stop_token = {},
      std::optional<std::chrono::steady_clock::time_point> deadline = {}) const;
  void InitializeWallet(const ChainNodeConfig& config,
                        std::chrono::steady_clock::time_point deadline,
                        std::stop_token stop_token) const;
  boost::json::object JsonRpcCall(const ChainNodeConfig& config,
                                  std::string_view method,
                                  const boost::json::object& params,
                                  std::stop_token stop_token = {}) const;
  boost::json::object PlainRpcCall(const ChainNodeConfig& config,
                                   std::string_view path,
                                   const boost::json::object& request,
                                   std::stop_token stop_token = {}) const;
  std::vector<std::string> HandshakeCompletePeerAddresses(
      const ChainNodeConfig& config, std::stop_token stop_token = {}) const;
  void SetPeerBan(const ChainNodeConfig& config, const std::string& host,
                  bool ban, std::stop_token stop_token) const;

  HttpClient http_;
  HttpClient block_http_{std::chrono::seconds(120)};
  HttpClient wallet_http_{std::chrono::seconds(60)};
};

}  // namespace bbp
