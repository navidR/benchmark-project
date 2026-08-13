#pragma once

#include <boost/json/object.hpp>
#include <boost/json/value.hpp>
#include <cstdint>
#include <filesystem>
#include <optional>
#include <span>
#include <string>
#include <string_view>

namespace bbp::simulator_app_internal {

std::uint32_t JsonUint32Field(const boost::json::object& object,
                              const char* field);
std::uint32_t JsonOptionalUint32Field(const boost::json::object& object,
                                      const char* field,
                                      std::uint32_t default_value);
std::uint32_t JsonOptionalNullableUint32Field(const boost::json::object& object,
                                              const char* field,
                                              std::uint32_t default_value);
std::uint64_t JsonOptionalUint64Field(const boost::json::object& object,
                                      const char* field,
                                      std::uint64_t default_value);
double JsonOptionalDoubleField(const boost::json::object& object,
                               const char* field, double default_value);
std::optional<double> JsonOptionalNullableDoubleField(
    const boost::json::object& object, const char* field);
std::uint32_t JsonPercentBasisPoints(const boost::json::object& object,
                                     const char* field);
std::uint64_t JsonUint64Field(const boost::json::object& object,
                              const char* field);
std::uint64_t JsonUint64Value(const boost::json::value& value,
                              std::string_view field);
std::uint32_t JsonUint32Value(const boost::json::value& value,
                              std::string_view field);
std::optional<std::uint64_t> JsonOptionalUint64FieldValue(
    const boost::json::object& object, const char* field);
bool JsonOptionalBoolField(const boost::json::object& object, const char* field,
                           bool default_value);
std::filesystem::path JsonOptionalPathField(
    const boost::json::object& object, const char* field,
    const std::filesystem::path& default_value);
std::string JsonStringField(const boost::json::object& object,
                            const char* field);
std::string JsonOptionalStringField(const boost::json::object& object,
                                    const char* field,
                                    std::string_view default_value);
void RejectUnsupportedFields(
    const boost::json::object& object,
    std::span<const std::string_view> allowed_fields, std::string_view context,
    std::string_view additional_allowed_field = std::string_view());
std::uint64_t JsonAmountField(const boost::json::object& object,
                              const char* field);
std::uint64_t JsonOptionalAmountField(const boost::json::object& object,
                                      const char* field,
                                      std::uint64_t default_value);

}  // namespace bbp::simulator_app_internal
