#pragma once

#include <boost/json/value.hpp>
#include <chrono>
#include <cstdint>
#include <filesystem>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

namespace bbp {

std::string ReadText(const std::filesystem::path& path);
void WriteText(const std::filesystem::path& path, std::string_view text);
void AppendLine(const std::filesystem::path& path, std::string_view text);
void EnsureDirectory(const std::filesystem::path& path);
void RequireSafeRunId(std::string_view run_id);
void RequireExecutable(const std::filesystem::path& path);

std::string JsonEscape(std::string_view value);
std::string NowIso8601();
uint64_t NowUnixMillis();
std::string MakeRunId();
uint64_t ParseFixed8Amount(std::string_view text, std::string_view field);
std::string FormatFixed8Amount(uint64_t amount);
std::string JsonFixed8AmountText(const boost::json::value& value,
                                 std::string_view field);
uint64_t JsonFixed8Amount(const boost::json::value& value,
                          std::string_view field);

std::vector<std::string> SplitWhitespace(std::string_view text);
std::string JsonString(const boost::json::value& value, std::string_view field);
uint64_t JsonUint(const boost::json::value& value, std::string_view field);
std::optional<bool> JsonOptionalBool(const boost::json::value& value,
                                     std::string_view field);
std::optional<double> JsonOptionalDouble(const boost::json::value& value,
                                         std::string_view field);

}  // namespace bbp
