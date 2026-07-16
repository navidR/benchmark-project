#pragma once

#include <cstdint>
#include <optional>
#include <vector>

#include "bbp/cgroup.h"

namespace bbp {

struct ResourceLimitPatch {
  std::optional<std::uint64_t> memory_high_bytes;
  std::optional<std::uint64_t> memory_max_bytes;
  bool cpu_quota_present = false;
  std::optional<std::uint64_t> cpu_quota_us;
  std::optional<std::uint64_t> cpu_period_us;
  std::optional<std::uint64_t> cpu_weight;
  std::optional<std::uint64_t> io_weight;
  bool io_limits_present = false;
  std::vector<IoLimit> io_limits;
  std::optional<std::uint64_t> pids_max;
};

}  // namespace bbp
