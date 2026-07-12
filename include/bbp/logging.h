#pragma once

#include <boost/log/trivial.hpp>
#include <filesystem>

#include "bbp/log_level.h"

namespace bbp {

void InitLogging();
void SetMinimumLogLevel(LogLevel level);
void SetConsoleLoggingEnabled(bool enabled);
void AttachRunLogFile(const std::filesystem::path& run_root);

}  // namespace bbp

#define BBP_LOG(severity) BOOST_LOG_TRIVIAL(severity)
