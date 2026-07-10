#pragma once

#include <cstdint>

namespace bbp {

inline constexpr const char* kRunMarkerFile = ".bbp-run";
inline constexpr std::uint64_t kMaxLogTailBytes = 4096;
inline constexpr std::uint32_t kDefaultCoinbaseSpendableConfirmations = 101;

}  // namespace bbp
