#pragma once

#include <filesystem>
#include <string>

#include "benchmark_sim/result.h"

namespace bsim {

Result<std::string> BuildRunReportJson(const std::filesystem::path& run_root);

}  // namespace bsim
