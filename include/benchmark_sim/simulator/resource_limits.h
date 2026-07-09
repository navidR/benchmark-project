#pragma once

#include <cstdint>
#include <optional>

namespace bsim {

struct ResourceLimits {
  std::uint64_t memory_high_bytes = 0;
  std::uint64_t memory_max_bytes = 0;
  std::optional<std::uint64_t> cpu_quota_us;
  std::uint64_t cpu_period_us = 0;
  std::uint64_t pids_max = 0;
};

}  // namespace bsim
