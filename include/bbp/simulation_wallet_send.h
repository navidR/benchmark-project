#pragma once

#include <cstdint>

namespace bbp {

struct SimulationWalletSend {
  std::uint32_t sender_wallet_index = 0;
  std::uint32_t receiver_wallet_index = 0;
  std::uint64_t amount_satoshis = 0;
  std::uint64_t fee_satoshis = 0;
  std::uint32_t timeout_sec = 30;

  bool operator==(const SimulationWalletSend&) const = default;
};

}  // namespace bbp
