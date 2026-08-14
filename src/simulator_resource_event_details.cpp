#include "simulator_resource_event_details.h"

#include <boost/json/array.hpp>
#include <boost/json/object.hpp>
#include <boost/json/serialize.hpp>
#include <utility>

#include "bbp/simulator/options.h"
#include "bbp/simulator/workload_kind.h"
#include "simulator_scenario_serialization.h"

namespace bbp::simulator_app_internal {

std::string ResourceLimitUpdateDetail(
    const ResourceLimitPatch& patch, const ResourceLimits& previous,
    const ResourceLimits& current, std::optional<uint32_t> workload_index,
    std::optional<uint32_t> workload_count, std::optional<uint32_t> node,
    std::optional<std::uint64_t> operator_sequence) {
  boost::json::object detail;
  if (workload_index) {
    detail["workload_index"] = *workload_index;
  }
  if (workload_count) {
    detail["workload_count"] = *workload_count;
  }
  if (node) {
    detail["node"] = *node;
  }
  if (operator_sequence) {
    detail["operator_command_sequence"] = *operator_sequence;
  }
  detail["requested"] = ResourceLimitPatchJson(patch);
  detail["previous"] = ResourceLimitsJson(previous);
  detail["current"] = ResourceLimitsJson(current);
  return boost::json::serialize(detail);
}

std::string ProfileRollbackFailureDetail(
    WorkloadKind kind, std::string_view profile,
    std::string_view original_error,
    const std::vector<std::string>& rollback_errors) {
  boost::json::object detail;
  detail["action"] = WorkloadKindName(kind);
  detail["profile"] = profile;
  detail["original_error"] = original_error;
  boost::json::array errors;
  for (const std::string& error : rollback_errors) {
    errors.emplace_back(error);
  }
  detail["rollback_errors"] = std::move(errors);
  return boost::json::serialize(detail);
}

std::string ResourceProfileUpdateDetail(const ProfileSwitchWorkload& workload,
                                        uint32_t node,
                                        std::string_view previous_profile,
                                        const ResourceLimits& previous,
                                        const ResourceLimits& current,
                                        uint32_t workload_index,
                                        uint32_t workload_count) {
  boost::json::object detail;
  detail["workload_index"] = workload_index;
  detail["workload_count"] = workload_count;
  detail["node"] = node;
  detail["profile"] = workload.profile;
  if (previous_profile.empty()) {
    detail["previous_profile"] = nullptr;
  } else {
    detail["previous_profile"] = previous_profile;
  }
  detail["previous"] = ResourceLimitsJson(previous);
  detail["current"] = ResourceLimitsJson(current);
  detail["kernel_verified"] = true;
  return boost::json::serialize(detail);
}

std::string ResourcePressureDetail(const ResourcePressureWorkload& workload,
                                   const ResourceLimits& previous_limits,
                                   const ResourceLimits& pressure_limits,
                                   const ResourceLimits& current_limits,
                                   uint32_t workload_index,
                                   uint32_t workload_count) {
  boost::json::object detail;
  detail["workload_index"] = workload_index;
  detail["workload_count"] = workload_count;
  detail["node"] = workload.node;
  detail["duration_ms"] = workload.duration_ms;
  detail["requested"] = ResourceLimitPatchJson(workload.patch);
  detail["previous"] = ResourceLimitsJson(previous_limits);
  detail["pressure"] = ResourceLimitsJson(pressure_limits);
  detail["current"] = ResourceLimitsJson(current_limits);
  return boost::json::serialize(detail);
}

}  // namespace bbp::simulator_app_internal
