#pragma once

#include <cstdint>
#include <optional>
#include <vector>

#include "bbp/cgroup.h"

namespace bbp {

struct ResourceLimits {
  std::uint64_t memory_high_bytes = 0;
  std::uint64_t memory_max_bytes = 0;
  std::optional<std::uint64_t> cpu_quota_us;
  std::uint64_t cpu_period_us = 0;
  std::uint64_t cpu_weight = 100;
  std::uint64_t io_weight = 100;
  std::vector<IoLimit> io_limits;
  std::uint64_t pids_max = 0;
};

}  // namespace bbp
