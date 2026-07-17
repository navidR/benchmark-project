#include "bbp/simulation_policy.h"

#include <stdexcept>

namespace bbp {

std::string_view CleanupPolicyName(CleanupPolicy policy) {
  switch (policy) {
    case CleanupPolicy::kAutomatic:
      return "automatic";
    case CleanupPolicy::kRetainCgroups:
      return "retain_cgroups";
  }
  throw std::runtime_error("unknown cleanup policy");
}

std::optional<CleanupPolicy> CleanupPolicyFromName(std::string_view name) {
  if (name == "automatic") {
    return CleanupPolicy::kAutomatic;
  }
  if (name == "retain_cgroups") {
    return CleanupPolicy::kRetainCgroups;
  }
  return std::nullopt;
}

std::string_view PrivilegeModeName(PrivilegeMode mode) {
  switch (mode) {
    case PrivilegeMode::kDirect:
      return "direct";
  }
  throw std::runtime_error("unknown privilege mode");
}

std::optional<PrivilegeMode> PrivilegeModeFromName(std::string_view name) {
  if (name == "direct") {
    return PrivilegeMode::kDirect;
  }
  return std::nullopt;
}

std::string_view LogRetentionPolicyName(LogRetentionPolicy policy) {
  switch (policy) {
    case LogRetentionPolicy::kPreserve:
      return "preserve";
  }
  throw std::runtime_error("unknown log retention policy");
}

std::optional<LogRetentionPolicy> LogRetentionPolicyFromName(
    std::string_view name) {
  if (name == "preserve") {
    return LogRetentionPolicy::kPreserve;
  }
  return std::nullopt;
}

}  // namespace bbp
