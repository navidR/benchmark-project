#pragma once

#include <optional>
#include <string_view>

namespace bbp {

enum class CleanupPolicy {
  kAutomatic,
  kRetainCgroups,
};

enum class PrivilegeMode {
  kDirect,
};

enum class LogRetentionPolicy {
  kPreserve,
};

[[nodiscard]] std::string_view CleanupPolicyName(CleanupPolicy policy);
[[nodiscard]] std::optional<CleanupPolicy> CleanupPolicyFromName(
    std::string_view name);

[[nodiscard]] std::string_view PrivilegeModeName(PrivilegeMode mode);
[[nodiscard]] std::optional<PrivilegeMode> PrivilegeModeFromName(
    std::string_view name);

[[nodiscard]] std::string_view LogRetentionPolicyName(
    LogRetentionPolicy policy);
[[nodiscard]] std::optional<LogRetentionPolicy> LogRetentionPolicyFromName(
    std::string_view name);

}  // namespace bbp
