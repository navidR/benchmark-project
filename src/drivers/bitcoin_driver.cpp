#include "bbp/drivers/bitcoin_driver.h"

#include <utility>

#include "bbp/drivers/firo_driver.h"
#include "bbp/simulation_cancelled.h"
#include "bbp/util.h"

namespace bbp {
namespace {

std::string Arg(std::string key, const std::string& value) {
  key += '=';
  key += value;
  return key;
}

[[noreturn]] void Unsupported(std::stop_token stop_token,
                              std::string_view operation) {
  if (stop_token.stop_requested()) {
    throw SimulationCancelled();
  }
  throw UnsupportedChainOperation("Bitcoin Core", operation);
}

}  // namespace

BitcoinDriver::BitcoinDriver(std::chrono::milliseconds rpc_timeout)
    : bitcoin_family_rpc_(std::make_unique<FiroDriver>(
          rpc_timeout, "Bitcoin Core",
          BitcoinFamilyGetBlockVerbosityEncoding::kInteger)) {}

ProcessSpec BitcoinDriver::RenderProcess(const ChainNodeConfig& config) const {
  if (config.network != ChainNetwork::kRegtest) {
    throw std::runtime_error("Bitcoin driver supports only regtest network");
  }
  EnsureDirectory(config.data_dir);
  EnsureDirectory(config.log_dir);
  bitcoin_family_rpc_->CleanupRpcCredentials(config);

  ProcessSpec spec;
  spec.binary = config.binary;
  spec.cwd = config.data_dir;
  spec.stdout_path = config.log_dir / "stdout.log";
  spec.stderr_path = config.log_dir / "stderr.log";
  spec.argv = {
      "-regtest",
      Arg("-datadir", config.data_dir.string()),
      "-server=1",
      Arg("-rpccookiefile", config.rpc_cookie_file.string()),
      "-rpccookieperms=owner",
      Arg("-rpcbind", config.rpc_bind),
      Arg("-rpcport", std::to_string(config.rpc_port)),
      Arg("-port", std::to_string(config.p2p_port)),
      Arg("-listen", config.listen ? "1" : "0"),
      "-dnsseed=0",
      "-fixedseeds=0",
      "-listenonion=0",
      "-discover=0",
      "-natpmp=0",
      "-txindex=1",
      "-debug=net",
  };
  if (!config.wallet_enabled) {
    spec.argv.push_back("-disablewallet=1");
  }
  for (const std::string& allow_ip : config.rpc_allow_ips) {
    spec.argv.push_back(Arg("-rpcallowip", allow_ip));
  }
  if (!config.p2p_bind.empty()) {
    spec.argv.push_back(Arg("-bind", config.p2p_bind));
  }
  spec.argv.insert(spec.argv.end(), config.extra_args.arguments().begin(),
                   config.extra_args.arguments().end());
  return spec;
}

std::optional<LogTailChunk> BitcoinDriver::ReadLogTail(
    const ChainNodeConfig& config, ChainLogSource source,
    const LogTailCursor& cursor, std::uint64_t max_bytes) const {
  if (config.network != ChainNetwork::kRegtest) {
    throw std::runtime_error("Bitcoin driver supports only regtest network");
  }
  std::filesystem::path path;
  switch (source) {
    case ChainLogSource::kDaemon:
      path = config.data_dir / "regtest" / "debug.log";
      break;
    case ChainLogSource::kStdout:
      path = config.log_dir / "stdout.log";
      break;
    case ChainLogSource::kStderr:
      path = config.log_dir / "stderr.log";
      break;
  }
  return TailLogFile(path, cursor, max_bytes);
}

RpcEndpoint BitcoinDriver::Endpoint(const ChainNodeConfig& config) const {
  return bitcoin_family_rpc_->Endpoint(config);
}

void BitcoinDriver::WaitReady(const ChainNodeConfig& config,
                              std::chrono::seconds timeout,
                              std::stop_token stop_token) const {
  bitcoin_family_rpc_->WaitReady(config, timeout, stop_token);
}

void BitcoinDriver::WaitForHeight(const ChainNodeConfig& config,
                                  std::uint64_t height,
                                  std::chrono::seconds timeout,
                                  std::stop_token stop_token) const {
  bitcoin_family_rpc_->WaitForHeight(config, height, timeout, stop_token);
}

void BitcoinDriver::WaitForPeerCount(const ChainNodeConfig& config,
                                     std::uint64_t peer_count,
                                     std::chrono::seconds timeout,
                                     std::stop_token stop_token) const {
  bitcoin_family_rpc_->WaitForPeerCount(config, peer_count, timeout,
                                        stop_token);
}

void BitcoinDriver::WaitForPeerAddress(const ChainNodeConfig& config,
                                       const std::string& address,
                                       std::chrono::seconds timeout,
                                       std::stop_token stop_token) const {
  bitcoin_family_rpc_->WaitForPeerAddress(config, address, timeout, stop_token);
}

void BitcoinDriver::WaitForPeerAddressAbsent(const ChainNodeConfig& config,
                                             const std::string& address,
                                             std::chrono::seconds timeout,
                                             std::stop_token stop_token) const {
  bitcoin_family_rpc_->WaitForPeerAddressAbsent(config, address, timeout,
                                                stop_token);
}

ChainMetrics BitcoinDriver::ReadMetrics(const ChainNodeConfig& config,
                                        std::stop_token stop_token) const {
  return bitcoin_family_rpc_->ReadMetrics(config, stop_token);
}

std::vector<std::string> BitcoinDriver::PeerAddresses(
    const ChainNodeConfig& config, std::stop_token stop_token) const {
  return bitcoin_family_rpc_->PeerAddresses(config, stop_token);
}

std::vector<std::string> BitcoinDriver::ConnectedPeerAddresses(
    const ChainNodeConfig& config,
    const std::vector<std::string>& candidate_addresses,
    std::stop_token stop_token) const {
  return bitcoin_family_rpc_->ConnectedPeerAddresses(
      config, candidate_addresses, stop_token);
}

std::vector<std::string> BitcoinDriver::GenerateBlocks(
    const ChainNodeConfig& config, std::uint32_t count,
    const std::string& address, std::stop_token stop_token) const {
  return bitcoin_family_rpc_->GenerateBlocks(config, count, address,
                                             stop_token);
}

std::uint64_t BitcoinDriver::ReadBlockNonRewardTransactionCount(
    const ChainNodeConfig& config, const std::string& block_hash,
    std::stop_token stop_token) const {
  return bitcoin_family_rpc_->ReadBlockNonRewardTransactionCount(
      config, block_hash, stop_token);
}

std::string BitcoinDriver::CreateWalletAddress(
    const ChainNodeConfig&, ChainWalletMode, std::stop_token stop_token) const {
  Unsupported(stop_token, "wallet initialization");
}

std::string BitcoinDriver::CreateWalletFundingAddress(
    const ChainNodeConfig&, ChainWalletMode, const std::string&,
    std::stop_token stop_token) const {
  Unsupported(stop_token, "wallet funding");
}

ChainWalletFundingResult BitcoinDriver::PrepareWalletFunding(
    const ChainNodeConfig&, ChainWalletMode, const std::string&, std::uint64_t,
    std::uint64_t, std::chrono::seconds, std::stop_token stop_token) const {
  Unsupported(stop_token, "wallet funding");
}

std::uint64_t BitcoinDriver::WaitForWalletBalance(
    const ChainNodeConfig&, ChainWalletMode, std::uint64_t, std::uint64_t,
    std::chrono::seconds, std::stop_token stop_token) const {
  Unsupported(stop_token, "wallet balance polling");
}

ChainWalletSnapshot BitcoinDriver::ReadWalletSnapshot(
    const ChainNodeConfig&, ChainWalletMode, std::uint32_t,
    std::stop_token stop_token) const {
  Unsupported(stop_token, "wallet metrics");
}

ChainUtxo BitcoinDriver::FindSpendableOutput(const ChainNodeConfig&,
                                             const std::vector<std::string>&,
                                             const std::string&, std::uint64_t,
                                             std::uint64_t,
                                             std::stop_token stop_token) const {
  Unsupported(stop_token, "raw transaction funding");
}

ChainRawTransactionResult BitcoinDriver::SendRawTransaction(
    const ChainNodeConfig&, const ChainUtxo&, const std::string&,
    const std::string&, const std::string&, std::uint64_t, std::uint64_t,
    std::chrono::seconds, std::stop_token stop_token) const {
  Unsupported(stop_token, "raw transaction submission");
}

ChainWalletTransactionResult BitcoinDriver::SendWalletTransaction(
    const ChainNodeConfig&, ChainWalletMode, const std::string&, std::uint64_t,
    std::uint64_t, std::chrono::seconds, std::stop_token stop_token) const {
  Unsupported(stop_token, "wallet transaction submission");
}

ChainTransactionObservation BitcoinDriver::ObserveTransaction(
    const ChainNodeConfig& config, const std::string& txid,
    std::stop_token stop_token) const {
  return bitcoin_family_rpc_->ObserveTransaction(config, txid, stop_token);
}

ChainTransactionObservation BitcoinDriver::WaitForTransaction(
    const ChainNodeConfig& config, const std::string& txid,
    std::chrono::seconds timeout, std::stop_token stop_token) const {
  return bitcoin_family_rpc_->WaitForTransaction(config, txid, timeout,
                                                 stop_token);
}

std::uint64_t BitcoinDriver::WaitForMempoolTransaction(
    const ChainNodeConfig& config, const std::string& txid,
    std::chrono::seconds timeout, std::stop_token stop_token) const {
  return bitcoin_family_rpc_->WaitForMempoolTransaction(config, txid, timeout,
                                                        stop_token);
}

void BitcoinDriver::ConnectPeer(const ChainNodeConfig& config,
                                const std::string& address,
                                std::stop_token stop_token) const {
  bitcoin_family_rpc_->ConnectPeer(config, address, stop_token);
}

void BitcoinDriver::DisconnectPeer(const ChainNodeConfig& config,
                                   const std::string& address,
                                   std::stop_token stop_token) const {
  bitcoin_family_rpc_->DisconnectPeer(config, address, stop_token);
}

void BitcoinDriver::ChangeLogVerbosity(const ChainNodeConfig&,
                                       ChainLogVerbosityChange,
                                       std::stop_token stop_token) const {
  Unsupported(stop_token, "runtime log verbosity adjustment");
}

void BitcoinDriver::SetMiningDifficulty(const ChainNodeConfig&,
                                        MiningDifficulty,
                                        std::stop_token stop_token) const {
  Unsupported(stop_token, "regtest mining difficulty adjustment");
}

void BitcoinDriver::StartMining(const ChainNodeConfig&, const std::string&,
                                std::stop_token stop_token) const {
  Unsupported(stop_token, "native continuous mining");
}

void BitcoinDriver::StopMining(const ChainNodeConfig&,
                               std::stop_token stop_token) const {
  Unsupported(stop_token, "persistent mining stop");
}

void BitcoinDriver::SetNetworkActive(const ChainNodeConfig& config, bool active,
                                     std::stop_token stop_token) const {
  bitcoin_family_rpc_->SetNetworkActive(config, active, stop_token);
}

void BitcoinDriver::Stop(const ChainNodeConfig& config,
                         std::stop_token stop_token) const {
  bitcoin_family_rpc_->Stop(config, stop_token);
}

void BitcoinDriver::CleanupRpcCredentials(const ChainNodeConfig& config) const {
  bitcoin_family_rpc_->CleanupRpcCredentials(config);
}

}  // namespace bbp
