#pragma once

#include <cstdint>
#include <string>

#include "bbp/simulator/constants.h"

namespace bbp {

struct SendRawTransactionWorkload {
  std::uint32_t funding_node = 1;
  std::uint32_t submit_node = 1;
  std::string source_address;
  std::string source_private_key;
  std::string destination_address;
  std::uint32_t funding_blocks = kDefaultCoinbaseSpendableConfirmations;
  std::uint64_t amount_satoshis = 0;
  std::uint64_t fee_satoshis = 0;
  std::uint32_t timeout_sec = 30;
};

}  // namespace bbp
