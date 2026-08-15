#include "simulator_resource_limit_application.h"

#include <algorithm>
#include <map>
#include <optional>
#include <stdexcept>
#include <string>
#include <vector>

#include "bbp/cgroup.h"
#include "bbp/simulator/resource_limits.h"

namespace bbp::simulator_app_internal {

void VerifyResourceLimits(const Cgroup& cgroup,
                          const ResourceLimits& expected) {
  const CgroupMetrics actual = cgroup.ReadMetrics();
  const auto finite_io_limits = [](const std::vector<IoLimit>& limits) {
    std::map<BlockDeviceId, IoLimit> result;
    for (const IoLimit& limit : limits) {
      if (limit.read_bytes_per_sec || limit.write_bytes_per_sec ||
          limit.read_operations_per_sec || limit.write_operations_per_sec) {
        result.emplace(limit.device, limit);
      }
    }
    return result;
  };
  if (actual.memory_high_limit_bytes != expected.memory_high_bytes ||
      actual.memory_max_limit_bytes != expected.memory_max_bytes ||
      actual.cpu_quota_us != expected.cpu_quota_us ||
      actual.cpu_period_us != expected.cpu_period_us ||
      actual.cpu_weight != expected.cpu_weight ||
      actual.io_weight != expected.io_weight ||
      finite_io_limits(actual.io_limits) !=
          finite_io_limits(expected.io_limits) ||
      actual.pids_max_limit != expected.pids_max) {
    throw std::runtime_error(
        "resource limit read-back verification failed for " +
        cgroup.path().string());
  }
}

void WriteResourceLimits(const Cgroup& cgroup, const ResourceLimits& previous,
                         const ResourceLimits& next) {
  if (next.memory_max_bytes != previous.memory_max_bytes &&
      next.memory_max_bytes > previous.memory_max_bytes) {
    cgroup.SetMemoryMax(next.memory_max_bytes);
  }
  if (next.memory_high_bytes != previous.memory_high_bytes) {
    cgroup.SetMemoryHigh(next.memory_high_bytes);
  }
  if (next.memory_max_bytes != previous.memory_max_bytes &&
      next.memory_max_bytes <= previous.memory_max_bytes) {
    cgroup.SetMemoryMax(next.memory_max_bytes);
  }
  if (next.cpu_quota_us != previous.cpu_quota_us ||
      next.cpu_period_us != previous.cpu_period_us) {
    cgroup.SetCpuMax(next.cpu_quota_us, next.cpu_period_us);
  }
  if (next.cpu_weight != previous.cpu_weight) {
    cgroup.SetCpuWeight(next.cpu_weight);
  }
  if (next.io_weight != previous.io_weight) {
    cgroup.SetIoWeight(next.io_weight);
  }
  for (const IoLimit& previous_limit : previous.io_limits) {
    const auto next_limit =
        std::find_if(next.io_limits.begin(), next.io_limits.end(),
                     [&](const IoLimit& candidate) {
                       return candidate.device == previous_limit.device;
                     });
    if (next_limit == next.io_limits.end()) {
      cgroup.SetIoMax(IoLimit{
          .device = previous_limit.device,
          .read_bytes_per_sec = std::nullopt,
          .write_bytes_per_sec = std::nullopt,
          .read_operations_per_sec = std::nullopt,
          .write_operations_per_sec = std::nullopt,
      });
    }
  }
  for (const IoLimit& next_limit : next.io_limits) {
    const auto previous_limit =
        std::find_if(previous.io_limits.begin(), previous.io_limits.end(),
                     [&](const IoLimit& candidate) {
                       return candidate.device == next_limit.device;
                     });
    if (previous_limit == previous.io_limits.end() ||
        *previous_limit != next_limit) {
      cgroup.SetIoMax(next_limit);
    }
  }
  if (next.pids_max != previous.pids_max) {
    cgroup.SetPidsMax(next.pids_max);
  }
  VerifyResourceLimits(cgroup, next);
}

}  // namespace bbp::simulator_app_internal
