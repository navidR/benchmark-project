#include "bbp/drivers/chain_driver.h"

#include <stdexcept>

namespace bbp {

std::string_view ChainSyncStatusName(ChainSyncStatus status) {
  switch (status) {
    case ChainSyncStatus::kUnknown:
      return "unknown";
    case ChainSyncStatus::kSyncing:
      return "syncing";
    case ChainSyncStatus::kSynced:
      return "synced";
  }
  throw std::runtime_error("unknown chain sync status");
}

UnsupportedChainOperation::UnsupportedChainOperation(std::string_view chain,
                                                     std::string_view operation)
    : std::runtime_error(std::string(chain) + " does not support " +
                         std::string(operation) + " functionality.") {}

std::string_view ChainLogSourceName(ChainLogSource source) {
  switch (source) {
    case ChainLogSource::kDaemon:
      return "daemon_log";
    case ChainLogSource::kStdout:
      return "stdout";
    case ChainLogSource::kStderr:
      return "stderr";
  }
  throw std::runtime_error("unknown chain log source");
}

std::string_view ChainTransactionStateName(ChainTransactionState state) {
  switch (state) {
    case ChainTransactionState::kUnknown:
      return "unknown";
    case ChainTransactionState::kMempool:
      return "mempool";
    case ChainTransactionState::kConfirmed:
      return "confirmed";
  }
  throw std::runtime_error("unknown chain transaction state");
}

std::string_view ChainWalletTransactionDirectionName(
    ChainWalletTransactionDirection direction) {
  switch (direction) {
    case ChainWalletTransactionDirection::kIncoming:
      return "incoming";
    case ChainWalletTransactionDirection::kOutgoing:
      return "outgoing";
    case ChainWalletTransactionDirection::kInternal:
      return "internal";
  }
  throw std::runtime_error("unknown chain wallet transaction direction");
}

std::optional<ChainWalletTransactionDirection>
ChainWalletTransactionDirectionFromName(std::string_view name) {
  if (name == "incoming") {
    return ChainWalletTransactionDirection::kIncoming;
  }
  if (name == "outgoing") {
    return ChainWalletTransactionDirection::kOutgoing;
  }
  if (name == "internal") {
    return ChainWalletTransactionDirection::kInternal;
  }
  return std::nullopt;
}

std::string ChainDriver::CreateWalletFundingAddress(
    const ChainNodeConfig&, ChainWalletMode, const std::string& wallet_address,
    std::stop_token) const {
  return wallet_address;
}

ChainWalletFundingResult ChainDriver::PrepareWalletFunding(
    const ChainNodeConfig&, ChainWalletMode, const std::string&, std::uint64_t,
    std::uint64_t, std::chrono::seconds, std::stop_token) const {
  return {};
}

}  // namespace bbp
