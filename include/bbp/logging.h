#pragma once

#include <boost/log/trivial.hpp>
#include <chrono>
#include <filesystem>
#include <optional>
#include <stop_token>

#include "bbp/log_level.h"

namespace bbp {

void InitLogging();
void SetMinimumLogLevel(LogLevel level);
void SetConsoleLoggingEnabled(bool enabled);
void AttachRunLogFile(const std::filesystem::path& run_root);
void AttachRunLogFileAt(const std::filesystem::path& run_root, int run_root_fd);
void DetachRunLogFile(const std::filesystem::path& run_root,
                      std::optional<std::chrono::steady_clock::time_point>
                          deadline = std::nullopt,
                      std::stop_token stop_token = {});

}  // namespace bbp

#define BBP_LOG(severity) BOOST_LOG_TRIVIAL(severity)
