#include "bbp/drivers/chain_driver.h"

#include <stdexcept>

namespace bbp {

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

}  // namespace bbp
