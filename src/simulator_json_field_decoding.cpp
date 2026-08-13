#include "simulator_json_field_decoding.h"

#include <algorithm>
#include <cmath>
#include <limits>
#include <stdexcept>

#include "bbp/util.h"

namespace bbp::simulator_app_internal {

uint32_t JsonUint32Field(const boost::json::object& object, const char* field) {
  const boost::json::value* value = object.if_contains(field);
  if (value == nullptr) {
    throw std::runtime_error("missing or invalid uint32 JSON field: " +
                             std::string(field));
  }
  if (value->is_uint64() &&
      value->as_uint64() <= std::numeric_limits<uint32_t>::max()) {
    return static_cast<uint32_t>(value->as_uint64());
  }
  if (value->is_int64() && value->as_int64() >= 0 &&
      static_cast<uint64_t>(value->as_int64()) <=
          std::numeric_limits<uint32_t>::max()) {
    return static_cast<uint32_t>(value->as_int64());
  }
  throw std::runtime_error("missing or invalid uint32 JSON field: " +
                           std::string(field));
}

uint32_t JsonOptionalUint32Field(const boost::json::object& object,
                                 const char* field, uint32_t default_value) {
  const boost::json::value* value = object.if_contains(field);
  if (value == nullptr) {
    return default_value;
  }
  if (value->is_uint64() &&
      value->as_uint64() <= std::numeric_limits<uint32_t>::max()) {
    return static_cast<uint32_t>(value->as_uint64());
  }
  if (value->is_int64() && value->as_int64() >= 0 &&
      static_cast<uint64_t>(value->as_int64()) <=
          std::numeric_limits<uint32_t>::max()) {
    return static_cast<uint32_t>(value->as_int64());
  }
  throw std::runtime_error("invalid uint32 JSON field: " + std::string(field));
}

uint32_t JsonOptionalNullableUint32Field(const boost::json::object& object,
                                         const char* field,
                                         uint32_t default_value) {
  const boost::json::value* value = object.if_contains(field);
  if (value != nullptr && value->is_null()) {
    return default_value;
  }
  return JsonOptionalUint32Field(object, field, default_value);
}

uint64_t JsonOptionalUint64Field(const boost::json::object& object,
                                 const char* field, uint64_t default_value) {
  const boost::json::value* value = object.if_contains(field);
  if (value == nullptr) {
    return default_value;
  }
  if (value->is_uint64()) {
    return value->as_uint64();
  }
  if (value->is_int64() && value->as_int64() >= 0) {
    return static_cast<uint64_t>(value->as_int64());
  }
  throw std::runtime_error("invalid uint64 JSON field: " + std::string(field));
}

double JsonOptionalDoubleField(const boost::json::object& object,
                               const char* field, double default_value) {
  const boost::json::value* value = object.if_contains(field);
  if (value == nullptr) {
    return default_value;
  }
  if (value->is_double()) {
    return value->as_double();
  }
  if (value->is_uint64()) {
    return static_cast<double>(value->as_uint64());
  }
  if (value->is_int64()) {
    return static_cast<double>(value->as_int64());
  }
  throw std::runtime_error("invalid numeric JSON field: " + std::string(field));
}

std::optional<double> JsonOptionalNullableDoubleField(
    const boost::json::object& object, const char* field) {
  const boost::json::value* value = object.if_contains(field);
  if (value == nullptr || value->is_null()) {
    return std::nullopt;
  }
  return JsonOptionalDoubleField(object, field, 0.0);
}

uint32_t JsonPercentBasisPoints(const boost::json::object& object,
                                const char* field) {
  const double percent = JsonOptionalDoubleField(object, field, 0.0);
  if (!std::isfinite(percent) || percent < 0.0 || percent > 100.0) {
    throw std::runtime_error(std::string(field) + " must be in 0..100");
  }
  const double scaled = percent * 100.0;
  const double integral = std::round(scaled);
  if (std::fabs(scaled - integral) > 1e-9) {
    throw std::runtime_error(std::string(field) +
                             " must use at most 0.01 percent resolution");
  }
  return static_cast<uint32_t>(integral);
}

uint64_t JsonUint64Field(const boost::json::object& object, const char* field) {
  const boost::json::value* value = object.if_contains(field);
  if (value == nullptr) {
    throw std::runtime_error("missing or invalid uint64 JSON field: " +
                             std::string(field));
  }
  if (value->is_uint64()) {
    return value->as_uint64();
  }
  if (value->is_int64() && value->as_int64() >= 0) {
    return static_cast<uint64_t>(value->as_int64());
  }
  throw std::runtime_error("missing or invalid uint64 JSON field: " +
                           std::string(field));
}

uint64_t JsonUint64Value(const boost::json::value& value,
                         std::string_view field) {
  if (value.is_uint64()) {
    return value.as_uint64();
  }
  if (value.is_int64() && value.as_int64() >= 0) {
    return static_cast<uint64_t>(value.as_int64());
  }
  throw std::runtime_error("invalid uint64 JSON field: " + std::string(field));
}

uint32_t JsonUint32Value(const boost::json::value& value,
                         std::string_view field) {
  const uint64_t parsed = JsonUint64Value(value, field);
  if (parsed > std::numeric_limits<uint32_t>::max()) {
    throw std::runtime_error("invalid uint32 JSON field: " +
                             std::string(field));
  }
  return static_cast<uint32_t>(parsed);
}

std::optional<uint64_t> JsonOptionalUint64FieldValue(
    const boost::json::object& object, const char* field) {
  const boost::json::value* value = object.if_contains(field);
  if (value == nullptr) {
    return std::nullopt;
  }
  return JsonUint64Value(*value, field);
}

bool JsonOptionalBoolField(const boost::json::object& object, const char* field,
                           bool default_value) {
  const boost::json::value* value = object.if_contains(field);
  if (value == nullptr) {
    return default_value;
  }
  if (!value->is_bool()) {
    throw std::runtime_error("invalid bool JSON field: " + std::string(field));
  }
  return value->as_bool();
}

std::filesystem::path JsonOptionalPathField(
    const boost::json::object& object, const char* field,
    const std::filesystem::path& default_value) {
  const boost::json::value* value = object.if_contains(field);
  if (value == nullptr) {
    return default_value;
  }
  if (!value->is_string()) {
    throw std::runtime_error("invalid path JSON field: " + std::string(field));
  }
  return std::filesystem::path(std::string(value->as_string()));
}

std::string JsonStringField(const boost::json::object& object,
                            const char* field) {
  const boost::json::value* value = object.if_contains(field);
  if (value == nullptr || !value->is_string()) {
    throw std::runtime_error("missing or invalid string JSON field: " +
                             std::string(field));
  }
  return std::string(value->as_string());
}

std::string JsonOptionalStringField(const boost::json::object& object,
                                    const char* field,
                                    std::string_view default_value) {
  const boost::json::value* value = object.if_contains(field);
  if (value == nullptr) {
    return std::string(default_value);
  }
  if (!value->is_string()) {
    throw std::runtime_error("invalid string JSON field: " +
                             std::string(field));
  }
  return std::string(value->as_string());
}

void RejectUnsupportedFields(const boost::json::object& object,
                             std::span<const std::string_view> allowed_fields,
                             std::string_view context,
                             std::string_view additional_allowed_field) {
  for (const auto& member : object) {
    if (member.key() != additional_allowed_field &&
        std::find(allowed_fields.begin(), allowed_fields.end(), member.key()) ==
            allowed_fields.end()) {
      throw std::runtime_error(
          std::string(context) +
          " has unsupported field: " + std::string(member.key()));
    }
  }
}

uint64_t JsonAmountField(const boost::json::object& object, const char* field) {
  const boost::json::value* value = object.if_contains(field);
  if (value == nullptr) {
    throw std::runtime_error("missing or invalid fixed-8 amount JSON field: " +
                             std::string(field));
  }
  return JsonFixed8Amount(*value, field);
}

uint64_t JsonOptionalAmountField(const boost::json::object& object,
                                 const char* field, uint64_t default_value) {
  const boost::json::value* value = object.if_contains(field);
  if (value == nullptr) {
    return default_value;
  }
  return JsonFixed8Amount(*value, field);
}

}  // namespace bbp::simulator_app_internal
