#pragma once

#include <cstdint>

namespace bbp {

inline constexpr std::uint32_t kBlockGenerationMinimumSyncTimeoutSeconds = 1U;
inline constexpr std::uint32_t kBlockGenerationMaximumSyncTimeoutSeconds =
    3600U;

struct BlockGenerationWorkload {
  std::uint32_t node = 1;
  std::uint32_t count = 0;
  std::uint32_t sync_timeout_sec = 30;
};

}  // namespace bbp
