#pragma once

#include <filesystem>
#include <string>

namespace bsim {

std::string BuildRunReportJson(const std::filesystem::path& run_root);

}  // namespace bsim
