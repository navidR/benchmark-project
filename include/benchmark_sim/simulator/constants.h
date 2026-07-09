#pragma once

#include <cstdint>

namespace bsim {

inline constexpr const char* kDefaultRewardAddress =
    "TTJW6FsYqLbSiF3ZUwMXRghgQuXK7XTodR";
inline constexpr const char* kRunMarkerFile = ".benchmark-sim-run";
inline constexpr std::uint64_t kMaxLogTailBytes = 4096;
inline constexpr std::uint32_t kMaxFiroNodes = 16;
inline constexpr std::uint32_t kFiroCoinbaseSpendableConfirmations = 101;

}  // namespace bsim
