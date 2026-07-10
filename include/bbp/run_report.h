#pragma once

#include <filesystem>
#include <string>

namespace bbp {

std::string BuildRunReportJson(const std::filesystem::path& run_root);

}  // namespace bbp
