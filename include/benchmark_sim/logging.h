#pragma once

#include <boost/log/trivial.hpp>
#include <filesystem>

namespace bsim {

void InitLogging();
void SetConsoleLoggingEnabled(bool enabled);
void AttachRunLogFile(const std::filesystem::path& run_root);

}  // namespace bsim

#define BSIM_LOG(severity) BOOST_LOG_TRIVIAL(severity)
