#pragma once

#include <cstdint>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

namespace bbp {

struct ProfileSwitchWorkload;
struct ResourceLimitPatch;
struct ResourceLimits;
struct ResourcePressureWorkload;
enum class WorkloadKind;

namespace simulator_app_internal {

std::string ResourceLimitUpdateDetail(
    const ResourceLimitPatch& patch, const ResourceLimits& previous,
    const ResourceLimits& current,
    std::optional<std::uint32_t> workload_index = std::nullopt,
    std::optional<std::uint32_t> workload_count = std::nullopt,
    std::optional<std::uint32_t> node = std::nullopt,
    std::optional<std::uint64_t> operator_sequence = std::nullopt);
std::string ProfileRollbackFailureDetail(
    WorkloadKind kind, std::string_view profile,
    std::string_view original_error,
    const std::vector<std::string>& rollback_errors);
std::string ResourceProfileUpdateDetail(const ProfileSwitchWorkload& workload,
                                        std::uint32_t node,
                                        std::string_view previous_profile,
                                        const ResourceLimits& previous,
                                        const ResourceLimits& current,
                                        std::uint32_t workload_index,
                                        std::uint32_t workload_count);
std::string ResourcePressureDetail(const ResourcePressureWorkload& workload,
                                   const ResourceLimits& previous_limits,
                                   const ResourceLimits& pressure_limits,
                                   const ResourceLimits& current_limits,
                                   std::uint32_t workload_index,
                                   std::uint32_t workload_count);

}  // namespace simulator_app_internal
}  // namespace bbp
