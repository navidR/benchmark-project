#include "bbp/util.h"

#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

#include <array>
#include <boost/json/object.hpp>
#include <boost/json/serialize.hpp>
#include <boost/json/string.hpp>
#include <boost/json/value.hpp>
#include <cctype>
#include <cerrno>
#include <cstring>
#include <ctime>
#include <fstream>
#include <iomanip>
#include <limits>
#include <mutex>
#include <random>
#include <sstream>
#include <stdexcept>

namespace bbp {
namespace {

std::mutex append_line_mutex;

std::runtime_error IoError(const std::filesystem::path& path,
                           std::string_view action) {
  return std::runtime_error(std::string(action) + " failed for " +
                            path.string() + ": " + std::strerror(errno));
}

void WriteFdAll(int fd, std::string_view text,
                const std::filesystem::path& path) {
  const char* data = text.data();
  size_t remaining = text.size();
  while (remaining != 0) {
    ssize_t n = write(fd, data, remaining);
    if (n < 0) {
      if (errno == EINTR) {
        continue;
      }
      throw IoError(path, "write");
    }
    data += n;
    remaining -= static_cast<size_t>(n);
  }
}

}  // namespace

std::string ReadText(const std::filesystem::path& path) {
  std::ifstream input(path);
  if (!input) {
    throw std::runtime_error("open failed for " + path.string());
  }
  std::ostringstream buffer;
  buffer << input.rdbuf();
  return buffer.str();
}

void WriteText(const std::filesystem::path& path, std::string_view text) {
  int fd = open(path.c_str(), O_WRONLY | O_CREAT | O_TRUNC | O_CLOEXEC, 0644);
  if (fd < 0) {
    throw IoError(path, "open");
  }
  try {
    WriteFdAll(fd, text, path);
  } catch (...) {
    close(fd);
    throw;
  }
  if (close(fd) != 0) {
    throw IoError(path, "close");
  }
}

void AppendLine(const std::filesystem::path& path, std::string_view text) {
  std::lock_guard<std::mutex> lock(append_line_mutex);
  int fd = open(path.c_str(), O_WRONLY | O_CREAT | O_APPEND | O_CLOEXEC, 0644);
  if (fd < 0) {
    throw IoError(path, "open");
  }
  try {
    WriteFdAll(fd, text, path);
    WriteFdAll(fd, "\n", path);
  } catch (...) {
    close(fd);
    throw;
  }
  if (close(fd) != 0) {
    throw IoError(path, "close");
  }
}

void EnsureDirectory(const std::filesystem::path& path) {
  std::error_code ec;
  std::filesystem::create_directories(path, ec);
  if (ec) {
    throw std::runtime_error("create_directories failed for " + path.string() +
                             ": " + ec.message());
  }
}

void RequireSafeRunId(std::string_view run_id) {
  if (run_id.empty() || run_id.size() > 32) {
    throw std::runtime_error("run id must be 1..32 characters");
  }
  for (char c : run_id) {
    const bool ok = (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') ||
                    (c >= '0' && c <= '9') || c == '-' || c == '_';
    if (!ok) {
      throw std::runtime_error("run id contains unsafe character");
    }
  }
}

void RequireExecutable(const std::filesystem::path& path) {
  if (path.empty() || !std::filesystem::exists(path)) {
    throw std::runtime_error("binary does not exist: " + path.string());
  }
  if (access(path.c_str(), X_OK) != 0) {
    throw IoError(path, "access(X_OK)");
  }
}

std::string JsonEscape(std::string_view value) {
  boost::json::string json(value.data(), value.size());
  std::string serialized = boost::json::serialize(json);
  return serialized.substr(1, serialized.size() - 2);
}

std::string NowIso8601() {
  auto now = std::chrono::system_clock::now();
  std::time_t tt = std::chrono::system_clock::to_time_t(now);
  std::tm tm{};
  gmtime_r(&tt, &tm);
  std::ostringstream out;
  out << std::put_time(&tm, "%Y-%m-%dT%H:%M:%SZ");
  return out.str();
}

uint64_t NowUnixMillis() {
  auto now = std::chrono::system_clock::now().time_since_epoch();
  return static_cast<uint64_t>(
      std::chrono::duration_cast<std::chrono::milliseconds>(now).count());
}

std::string MakeRunId() {
  auto millis = NowUnixMillis();
  std::random_device rd;
  uint32_t suffix = rd();
  std::ostringstream out;
  out << "bbp" << millis << "-" << std::hex << (suffix & 0xffff);
  return out.str();
}

uint64_t ParseFixed8Amount(std::string_view text, std::string_view field) {
  constexpr uint64_t kScale = 100000000ULL;
  if (text.empty()) {
    throw std::runtime_error("invalid fixed-8 amount JSON field: " +
                             std::string(field));
  }

  uint64_t whole = 0;
  uint64_t fraction = 0;
  uint32_t fraction_digits = 0;
  bool seen_dot = false;
  bool seen_digit = false;
  for (char c : text) {
    if (c == '.') {
      if (seen_dot) {
        throw std::runtime_error("invalid fixed-8 amount JSON field: " +
                                 std::string(field));
      }
      seen_dot = true;
      continue;
    }
    if (c < '0' || c > '9') {
      throw std::runtime_error("invalid fixed-8 amount JSON field: " +
                               std::string(field));
    }
    seen_digit = true;
    const uint64_t digit = static_cast<uint64_t>(c - '0');
    if (!seen_dot) {
      if (whole > (std::numeric_limits<uint64_t>::max() - digit) / 10ULL) {
        throw std::runtime_error("fixed-8 amount overflows uint64: " +
                                 std::string(field));
      }
      whole = whole * 10ULL + digit;
    } else if (fraction_digits < 8U) {
      fraction = fraction * 10ULL + digit;
      ++fraction_digits;
    } else if (digit != 0ULL) {
      throw std::runtime_error("fixed-8 amount has more than 8 decimals: " +
                               std::string(field));
    }
  }
  if (!seen_digit) {
    throw std::runtime_error("invalid fixed-8 amount JSON field: " +
                             std::string(field));
  }
  while (fraction_digits < 8U) {
    fraction *= 10ULL;
    ++fraction_digits;
  }
  if (whole > (std::numeric_limits<uint64_t>::max() - fraction) / kScale) {
    throw std::runtime_error("fixed-8 amount overflows uint64: " +
                             std::string(field));
  }
  return whole * kScale + fraction;
}

std::string FormatFixed8Amount(uint64_t amount) {
  constexpr uint64_t kScale = 100000000ULL;
  std::ostringstream out;
  out << (amount / kScale) << "." << std::setw(8) << std::setfill('0')
      << (amount % kScale);
  return out.str();
}

std::string JsonFixed8AmountText(const boost::json::value& value,
                                 std::string_view field) {
  std::string text;
  if (value.is_string()) {
    text = std::string(value.as_string());
  } else if (value.is_uint64()) {
    text = std::to_string(value.as_uint64());
  } else if (value.is_int64() && value.as_int64() >= 0) {
    text = std::to_string(value.as_int64());
  } else if (value.is_double()) {
    std::ostringstream out;
    out << std::fixed << std::setprecision(8) << value.as_double();
    text = out.str();
  } else {
    throw std::runtime_error("invalid fixed-8 amount JSON field: " +
                             std::string(field));
  }
  return FormatFixed8Amount(ParseFixed8Amount(text, field));
}

uint64_t JsonFixed8Amount(const boost::json::value& value,
                          std::string_view field) {
  return ParseFixed8Amount(JsonFixed8AmountText(value, field), field);
}

std::vector<std::string> SplitWhitespace(std::string_view text) {
  std::istringstream in{std::string(text)};
  std::vector<std::string> words;
  std::string word;
  while (in >> word) {
    words.push_back(word);
  }
  return words;
}

std::string JsonString(const boost::json::value& value,
                       std::string_view field) {
  const auto& object = value.as_object();
  const boost::json::value* found = object.if_contains(field);
  if (found == nullptr || !found->is_string()) {
    throw std::runtime_error("missing JSON string field: " +
                             std::string(field));
  }
  return std::string(found->as_string());
}

uint64_t JsonUint(const boost::json::value& value, std::string_view field) {
  const auto& object = value.as_object();
  const boost::json::value* found = object.if_contains(field);
  if (found == nullptr) {
    throw std::runtime_error("missing JSON uint field: " + std::string(field));
  }
  if (found->is_uint64()) {
    return found->as_uint64();
  }
  if (found->is_int64() && found->as_int64() >= 0) {
    return static_cast<uint64_t>(found->as_int64());
  }
  throw std::runtime_error("JSON field is not unsigned integer: " +
                           std::string(field));
}

std::optional<bool> JsonOptionalBool(const boost::json::value& value,
                                     std::string_view field) {
  const auto& object = value.as_object();
  const boost::json::value* found = object.if_contains(field);
  if (found == nullptr) {
    return std::nullopt;
  }
  if (!found->is_bool()) {
    throw std::runtime_error("JSON field is not boolean: " +
                             std::string(field));
  }
  return found->as_bool();
}

std::optional<double> JsonOptionalDouble(const boost::json::value& value,
                                         std::string_view field) {
  const auto& object = value.as_object();
  const boost::json::value* found = object.if_contains(field);
  if (found == nullptr) {
    return std::nullopt;
  }
  if (found->is_double()) {
    return found->as_double();
  }
  if (found->is_int64()) {
    return static_cast<double>(found->as_int64());
  }
  if (found->is_uint64()) {
    return static_cast<double>(found->as_uint64());
  }
  throw std::runtime_error("JSON field is not numeric: " + std::string(field));
}

}  // namespace bbp
