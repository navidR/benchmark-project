#include "bbp/log_level.h"

#include <stdexcept>
#include <string>

namespace bbp {

LogLevel ParseLogLevel(std::string_view value) {
  if (value == "trace") {
    return LogLevel::kTrace;
  }
  if (value == "debug") {
    return LogLevel::kDebug;
  }
  if (value == "info") {
    return LogLevel::kInfo;
  }
  if (value == "warning") {
    return LogLevel::kWarning;
  }
  if (value == "error") {
    return LogLevel::kError;
  }
  if (value == "fatal") {
    return LogLevel::kFatal;
  }
  throw std::runtime_error("invalid log level: " + std::string(value));
}

std::string_view LogLevelName(LogLevel level) {
  switch (level) {
    case LogLevel::kTrace:
      return "trace";
    case LogLevel::kDebug:
      return "debug";
    case LogLevel::kInfo:
      return "info";
    case LogLevel::kWarning:
      return "warning";
    case LogLevel::kError:
      return "error";
    case LogLevel::kFatal:
      return "fatal";
  }
  throw std::logic_error("unknown log level");
}

}  // namespace bbp
