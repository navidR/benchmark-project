#include "simulator_resource_limit_decoding.h"

#include <cstdint>
#include <optional>
#include <set>
#include <stdexcept>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "bbp/scenario_fields.h"
#include "simulator_json_field_decoding.h"

namespace bbp::simulator_app_internal {
namespace {

std::optional<uint64_t> JsonRequiredNullablePositiveUint64(
    const boost::json::object& object, const char* field) {
  const boost::json::value* value = object.if_contains(field);
  if (value == nullptr) {
    throw std::runtime_error("missing io.max field: " + std::string(field));
  }
  if (value->is_null()) {
    return std::nullopt;
  }
  const uint64_t parsed = JsonUint64Value(*value, field);
  RequireNonZero(parsed, field);
  return parsed;
}

}  // namespace

void RequireNonZero(uint64_t value, std::string_view field) {
  if (value == 0U) {
    throw std::runtime_error(std::string(field) + " must be greater than zero");
  }
}

void RequireCgroupWeight(uint64_t value, std::string_view field) {
  if (value < 1U || value > 10000U) {
    throw std::runtime_error(std::string(field) + " must be in 1..10000");
  }
}

std::vector<IoLimit> ParseIoLimits(const boost::json::value& value,
                                   std::string_view field) {
  if (!value.is_array()) {
    throw std::runtime_error(std::string(field) + " must be a JSON array");
  }
  std::vector<IoLimit> limits;
  std::set<BlockDeviceId> devices;
  for (const boost::json::value& entry : value.as_array()) {
    if (!entry.is_object()) {
      throw std::runtime_error(std::string(field) +
                               " entries must be JSON objects");
    }
    const boost::json::object& object = entry.as_object();
    RejectUnsupportedFields(object,
                            ScenarioObjectFields(ScenarioObjectKind::kIoLimit),
                            std::string(field) + " entry");
    IoLimit limit;
    limit.device = ParseBlockDeviceId(JsonStringField(object, "device"));
    if (!devices.insert(limit.device).second) {
      throw std::runtime_error(std::string(field) +
                               " contains a duplicate block device: " +
                               BlockDeviceIdText(limit.device));
    }
    limit.read_bytes_per_sec =
        JsonRequiredNullablePositiveUint64(object, "read_bytes_per_sec");
    limit.write_bytes_per_sec =
        JsonRequiredNullablePositiveUint64(object, "write_bytes_per_sec");
    limit.read_operations_per_sec =
        JsonRequiredNullablePositiveUint64(object, "read_operations_per_sec");
    limit.write_operations_per_sec =
        JsonRequiredNullablePositiveUint64(object, "write_operations_per_sec");
    limits.push_back(std::move(limit));
  }
  return limits;
}

ResourceLimitPatch ParseResourceLimitPatchObject(
    const boost::json::object& object) {
  ResourceLimitPatch patch;
  patch.memory_high_bytes =
      JsonOptionalUint64FieldValue(object, "memory_high_bytes");
  patch.memory_max_bytes =
      JsonOptionalUint64FieldValue(object, "memory_max_bytes");
  const boost::json::value* quota = object.if_contains("cpu_quota_us");
  if (quota != nullptr) {
    patch.cpu_quota_present = true;
    if (!quota->is_null()) {
      patch.cpu_quota_us = JsonUint64Value(*quota, "cpu_quota_us");
    }
  }
  patch.cpu_period_us = JsonOptionalUint64FieldValue(object, "cpu_period_us");
  patch.cpu_weight = JsonOptionalUint64FieldValue(object, "cpu_weight");
  patch.io_weight = JsonOptionalUint64FieldValue(object, "io_weight");
  const boost::json::value* io_limits = object.if_contains("io_max");
  if (io_limits != nullptr) {
    patch.io_limits_present = true;
    patch.io_limits = ParseIoLimits(*io_limits, "io_max");
  }
  patch.pids_max = JsonOptionalUint64FieldValue(object, "pids_max");

  ValidateResourceLimitPatch(patch);
  return patch;
}

}  // namespace bbp::simulator_app_internal
