#include "simulator_resource_profile_decoding.h"

#include <boost/json/value.hpp>
#include <charconv>
#include <cstdint>
#include <limits>
#include <optional>
#include <sstream>
#include <stdexcept>
#include <string>
#include <string_view>
#include <system_error>
#include <utility>

#include "bbp/scenario_fields.h"
#include "bbp/simulator/options.h"
#include "simulator_json_field_decoding.h"
#include "simulator_resource_limit_decoding.h"

namespace bbp::simulator_app_internal {

ResourceLimits InitialResourceLimits(const Options& options) {
  return ResourceLimits{
      .memory_high_bytes = options.memory_high_bytes,
      .memory_max_bytes = options.memory_max_bytes,
      .cpu_quota_us = options.cpu_quota_requested
                          ? std::optional<uint64_t>(options.cpu_quota_us)
                          : std::nullopt,
      .cpu_period_us = options.cpu_period_us,
      .cpu_weight = options.cpu_weight,
      .io_weight = options.io_weight,
      .io_limits = options.io_limits,
      .pids_max = options.pids_max,
  };
}

ResourceLimits InitialResourceLimits(const Options& options,
                                     uint32_t node_index) {
  const auto node_limits = options.node_resource_limits.find(node_index);
  return node_limits == options.node_resource_limits.end()
             ? InitialResourceLimits(options)
             : node_limits->second;
}

ResourceLimits ApplyResourceLimitPatch(const ResourceLimits& current,
                                       const ResourceLimitPatch& patch,
                                       const std::string& node_id) {
  ResourceLimits next = current;
  if (patch.memory_high_bytes) {
    next.memory_high_bytes = *patch.memory_high_bytes;
  }
  if (patch.memory_max_bytes) {
    next.memory_max_bytes = *patch.memory_max_bytes;
  }
  if (patch.cpu_quota_present) {
    next.cpu_quota_us = patch.cpu_quota_us;
  }
  if (patch.cpu_period_us) {
    next.cpu_period_us = *patch.cpu_period_us;
  }
  if (patch.cpu_weight) {
    next.cpu_weight = *patch.cpu_weight;
  }
  if (patch.io_weight) {
    next.io_weight = *patch.io_weight;
  }
  if (patch.io_limits_present) {
    next.io_limits = patch.io_limits;
  }
  if (patch.pids_max) {
    next.pids_max = *patch.pids_max;
  }
  if (next.memory_high_bytes > next.memory_max_bytes) {
    throw std::runtime_error("runtime resource update for " + node_id +
                             " would make memory_high_bytes exceed "
                             "memory_max_bytes");
  }
  RequireNonZero(next.memory_max_bytes, "memory_max_bytes");
  RequireNonZero(next.cpu_period_us, "cpu_period_us");
  if (next.cpu_quota_us) {
    RequireNonZero(*next.cpu_quota_us, "cpu_quota_us");
  }
  RequireCgroupWeight(next.cpu_weight, "cpu_weight");
  RequireCgroupWeight(next.io_weight, "io_weight");
  RequireNonZero(next.pids_max, "pids_max");
  return next;
}

namespace {

uint64_t ParsePositiveUint64Text(std::string_view text,
                                 std::string_view field) {
  uint64_t value = 0U;
  const char* const begin = text.data();
  const char* const end = begin + text.size();
  const auto [next, error] = std::from_chars(begin, end, value);
  if (text.empty() || error != std::errc() || next != end || value == 0U) {
    throw std::runtime_error(std::string(field) + " must be a positive uint64");
  }
  return value;
}

uint64_t ParseBinaryByteSize(const boost::json::value& value,
                             std::string_view field) {
  if (!value.is_string()) {
    throw std::runtime_error(std::string(field) +
                             " must be a binary byte-size string");
  }
  const std::string text(value.as_string());
  const std::pair<std::string_view, uint64_t> suffixes[] = {
      {"TiB", 1ULL << 40U},
      {"GiB", 1ULL << 30U},
      {"MiB", 1ULL << 20U},
      {"KiB", 1ULL << 10U},
      {"B", 1U}};
  for (const auto& [suffix, multiplier] : suffixes) {
    if (text.size() <= suffix.size() ||
        std::string_view(text).substr(text.size() - suffix.size()) != suffix) {
      continue;
    }
    const std::string_view magnitude(text.data(), text.size() - suffix.size());
    const uint64_t parsed = ParsePositiveUint64Text(magnitude, field);
    if (parsed > std::numeric_limits<uint64_t>::max() / multiplier) {
      throw std::runtime_error(std::string(field) + " overflows uint64 bytes");
    }
    return parsed * multiplier;
  }
  throw std::runtime_error(std::string(field) +
                           " must use B, KiB, MiB, GiB, or TiB");
}

void ApplyCpuMaxAlias(const boost::json::value& value, std::string_view field,
                      boost::json::object* canonical) {
  if (!value.is_string()) {
    throw std::runtime_error(std::string(field) + " must be a string");
  }
  std::istringstream input(std::string(value.as_string()));
  std::string quota_text;
  std::string period_text;
  std::string extra;
  if (!(input >> quota_text >> period_text) || (input >> extra)) {
    throw std::runtime_error(std::string(field) +
                             " must contain exactly quota-or-max and period");
  }
  if (quota_text == "max") {
    (*canonical)["cpu_quota_us"] = nullptr;
  } else {
    (*canonical)["cpu_quota_us"] = ParsePositiveUint64Text(quota_text, field);
  }
  (*canonical)["cpu_period_us"] = ParsePositiveUint64Text(period_text, field);
}

ResourceLimits ParseResourceProfile(const boost::json::object& object,
                                    const ResourceLimits& defaults,
                                    std::string_view profile_name) {
  RejectUnsupportedFields(
      object, ScenarioObjectFields(ScenarioObjectKind::kResourceProfile),
      "scenario resource profile " + std::string(profile_name));
  boost::json::object canonical = object;
  const bool memory_high_alias = object.if_contains("memory_high") != nullptr;
  const bool memory_max_alias = object.if_contains("memory_max") != nullptr;
  if (memory_high_alias && object.if_contains("memory_high_bytes") != nullptr) {
    throw std::runtime_error("resource profile " + std::string(profile_name) +
                             " specifies both memory_high and "
                             "memory_high_bytes");
  }
  if (memory_max_alias && object.if_contains("memory_max_bytes") != nullptr) {
    throw std::runtime_error("resource profile " + std::string(profile_name) +
                             " specifies both memory_max and "
                             "memory_max_bytes");
  }
  if (memory_high_alias) {
    canonical["memory_high_bytes"] = ParseBinaryByteSize(
        object.at("memory_high"), "resource profile memory_high");
    canonical.erase("memory_high");
  }
  if (memory_max_alias) {
    canonical["memory_max_bytes"] = ParseBinaryByteSize(
        object.at("memory_max"), "resource profile memory_max");
    canonical.erase("memory_max");
  }

  const bool cpu_quota_alias = object.if_contains("cpu_quota") != nullptr;
  const bool cpu_max_alias = object.if_contains("cpu_max") != nullptr;
  if (cpu_quota_alias && cpu_max_alias) {
    throw std::runtime_error("resource profile " + std::string(profile_name) +
                             " specifies both cpu_quota and cpu_max");
  }
  if ((cpu_quota_alias || cpu_max_alias) &&
      (object.if_contains("cpu_quota_us") != nullptr ||
       object.if_contains("cpu_period_us") != nullptr)) {
    throw std::runtime_error("resource profile " + std::string(profile_name) +
                             " CPU aliases conflict with cpu_quota_us or "
                             "cpu_period_us");
  }
  if (cpu_quota_alias || cpu_max_alias) {
    const char* const alias = cpu_quota_alias ? "cpu_quota" : "cpu_max";
    ApplyCpuMaxAlias(object.at(alias), "resource profile " + std::string(alias),
                     &canonical);
    canonical.erase(alias);
  }

  ResourceLimitPatch patch = ParseResourceLimitPatchObject(canonical);
  if (patch.memory_max_bytes && !patch.memory_high_bytes &&
      defaults.memory_high_bytes > *patch.memory_max_bytes) {
    patch.memory_high_bytes = *patch.memory_max_bytes;
  }
  return ApplyResourceLimitPatch(
      defaults, patch, "resource profile " + std::string(profile_name));
}

}  // namespace

void ParseResourceProfiles(const boost::json::object& scenario,
                           Options* options) {
  const boost::json::value* value = scenario.if_contains("resource_profiles");
  if (value == nullptr) {
    return;
  }
  if (!value->is_object()) {
    throw std::runtime_error("scenario resource_profiles must be an object");
  }
  const ResourceLimits defaults = InitialResourceLimits(*options);
  for (const auto& [name_json, profile_value] : value->as_object()) {
    const std::string name(name_json);
    RequireSafeScenarioIdentifier(name, "resource profile name");
    if (!profile_value.is_object()) {
      throw std::runtime_error("scenario resource profile " + name +
                               " must be an object");
    }
    options->resource_profiles.emplace(
        name, ParseResourceProfile(profile_value.as_object(), defaults, name));
  }
}

}  // namespace bbp::simulator_app_internal
