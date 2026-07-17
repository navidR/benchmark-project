#pragma once

#include <cstdint>
#include <optional>
#include <string_view>
#include <vector>

#include "bbp/cgroup.h"
#include "bbp/simulator/resource_limits.h"

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

  bool operator==(const ResourceLimitPatch&) const = default;
};

bool ResourceLimitPatchIsEmpty(const ResourceLimitPatch& patch);
void ValidateResourceLimitPatch(
    const ResourceLimitPatch& patch,
    std::string_view context = "runtime resource update");
ResourceLimitPatch ResolveOperatorResourceLimitPatch(
    const ResourceLimits& current, const ResourceLimitPatch& requested);

}  // namespace bbp
