#include "benchmark_sim/log_view.h"

#include <deque>
#include <fstream>

namespace bsim {
namespace {

constexpr const char* kRunLogFile = "simulator.log";

}  // namespace

std::filesystem::path RunLogPath(const std::filesystem::path& run_root) {
  return run_root / kRunLogFile;
}

Result<std::vector<std::string>> ReadRecentLogLines(
    const std::filesystem::path& log_path, std::size_t max_lines) {
  if (max_lines == 0U) {
    return Ok(std::vector<std::string>{});
  }
  std::error_code ec;
  if (!std::filesystem::exists(log_path, ec)) {
    if (ec) {
      return Error<std::vector<std::string>>(
          "stat log file failed: " + log_path.string() + ": " + ec.message());
    }
    return Ok(std::vector<std::string>{});
  }
  std::ifstream input(log_path);
  if (!input) {
    return Error<std::vector<std::string>>("open log file failed: " +
                                           log_path.string());
  }
  std::deque<std::string> lines;
  std::string line;
  while (std::getline(input, line)) {
    lines.push_back(line);
    if (lines.size() > max_lines) {
      lines.pop_front();
    }
  }
  return Ok(std::vector<std::string>{lines.begin(), lines.end()});
}

}  // namespace bsim
