#pragma once

#include <cstdint>
#include <optional>
#include <string>
#include <string_view>

namespace bbp {

enum class ChainWalletTransactionDirection {
  kIncoming,
  kOutgoing,
  kInternal,
};

struct ChainWalletTransaction {
  std::string txid;
  std::string address;
  ChainWalletTransactionDirection direction =
      ChainWalletTransactionDirection::kInternal;
  std::int64_t amount_satoshis = 0;
  std::optional<std::int64_t> fee_satoshis;
  std::int64_t confirmations = 0;
  std::string block_hash;
  std::uint64_t timestamp = 0;
  std::optional<bool> abandoned;
};

std::string_view ChainWalletTransactionDirectionName(
    ChainWalletTransactionDirection direction);

}  // namespace bbp
