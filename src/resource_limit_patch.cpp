#include "bbp/simulator/resource_limit_patch.h"

#include <algorithm>
#include <set>
#include <stdexcept>
#include <string>

namespace bbp {
namespace {

void RequirePositive(std::uint64_t value, std::string_view field,
                     std::string_view context) {
  if (value == 0U) {
    throw std::runtime_error(std::string(context) + " " + std::string(field) +
                             " must be greater than zero");
  }
}

void RequireWeight(std::uint64_t value, std::string_view field,
                   std::string_view context) {
  if (value < 1U || value > 10000U) {
    throw std::runtime_error(std::string(context) + " " + std::string(field) +
                             " must be in 1..10000");
  }
}

bool IoLimitFinite(const IoLimit& limit) {
  return limit.read_bytes_per_sec || limit.write_bytes_per_sec ||
         limit.read_operations_per_sec || limit.write_operations_per_sec;
}

void ValidateIoLimit(const IoLimit& limit, std::string_view context) {
  const auto validate = [&](const std::optional<std::uint64_t>& value,
                            std::string_view field) {
    if (value) {
      RequirePositive(*value, field, context);
    }
  };
  validate(limit.read_bytes_per_sec, "io read_bytes_per_sec");
  validate(limit.write_bytes_per_sec, "io write_bytes_per_sec");
  validate(limit.read_operations_per_sec, "io read_operations_per_sec");
  validate(limit.write_operations_per_sec, "io write_operations_per_sec");
}

}  // namespace

bool ResourceLimitPatchIsEmpty(const ResourceLimitPatch& patch) {
  return !patch.memory_high_bytes && !patch.memory_max_bytes &&
         !patch.cpu_quota_present && !patch.cpu_period_us &&
         !patch.cpu_weight && !patch.io_weight && !patch.io_limits_present &&
         !patch.pids_max;
}

void ValidateResourceLimitPatch(const ResourceLimitPatch& patch,
                                std::string_view context) {
  if (ResourceLimitPatchIsEmpty(patch)) {
    throw std::runtime_error(std::string(context) + " has no limit fields");
  }
  if (patch.memory_max_bytes) {
    RequirePositive(*patch.memory_max_bytes, "memory_max_bytes", context);
  }
  if (patch.cpu_quota_us) {
    RequirePositive(*patch.cpu_quota_us, "cpu_quota_us", context);
  }
  if (patch.cpu_period_us) {
    RequirePositive(*patch.cpu_period_us, "cpu_period_us", context);
  }
  if (patch.cpu_weight) {
    RequireWeight(*patch.cpu_weight, "cpu_weight", context);
  }
  if (patch.io_weight) {
    RequireWeight(*patch.io_weight, "io_weight", context);
  }
  if (patch.pids_max) {
    RequirePositive(*patch.pids_max, "pids_max", context);
  }
  if (patch.memory_high_bytes && patch.memory_max_bytes &&
      *patch.memory_high_bytes > *patch.memory_max_bytes) {
    throw std::runtime_error(std::string(context) +
                             " memory_high_bytes must be less than or equal "
                             "to memory_max_bytes");
  }

  std::set<BlockDeviceId> devices;
  for (const IoLimit& limit : patch.io_limits) {
    ValidateIoLimit(limit, context);
    if (!devices.insert(limit.device).second) {
      throw std::runtime_error(std::string(context) +
                               " contains a duplicate block device: " +
                               BlockDeviceIdText(limit.device));
    }
  }
  if (!patch.io_limits_present && !patch.io_limits.empty()) {
    throw std::runtime_error(std::string(context) +
                             " has io limits without io_limits_present");
  }
}

ResourceLimitPatch ResolveOperatorResourceLimitPatch(
    const ResourceLimits& current, const ResourceLimitPatch& requested) {
  ValidateResourceLimitPatch(requested, "operator resource update");
  if (!requested.io_limits_present) {
    return requested;
  }
  if (requested.io_limits.size() != 1U) {
    throw std::runtime_error(
        "operator io resource update requires exactly one block device");
  }

  ResourceLimitPatch resolved = requested;
  resolved.io_limits = current.io_limits;
  const IoLimit& mutation = requested.io_limits.front();
  const auto existing =
      std::find_if(resolved.io_limits.begin(), resolved.io_limits.end(),
                   [&](const IoLimit& candidate) {
                     return candidate.device == mutation.device;
                   });
  if (IoLimitFinite(mutation)) {
    if (existing == resolved.io_limits.end()) {
      resolved.io_limits.push_back(mutation);
    } else {
      *existing = mutation;
    }
  } else if (existing != resolved.io_limits.end()) {
    resolved.io_limits.erase(existing);
  }
  return resolved;
}

}  // namespace bbp
