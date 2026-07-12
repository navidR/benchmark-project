#pragma once

#include <string_view>

namespace bbp {

enum class LogLevel {
  kTrace,
  kDebug,
  kInfo,
  kWarning,
  kError,
  kFatal,
};

LogLevel ParseLogLevel(std::string_view value);
std::string_view LogLevelName(LogLevel level);

}  // namespace bbp
