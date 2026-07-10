#include "bbp/log_view.h"

#include <deque>
#include <fstream>

namespace bbp {
namespace {

constexpr const char* kRunLogFile = "simulator.log";

}  // namespace

std::filesystem::path RunLogPath(const std::filesystem::path& run_root) {
  return run_root / kRunLogFile;
}

std::vector<std::string> ReadRecentLogLines(
    const std::filesystem::path& log_path, std::size_t max_lines) {
  if (max_lines == 0U) {
    return {};
  }
  std::ifstream input(log_path);
  if (!input) {
    return {};
  }
  std::deque<std::string> lines;
  std::string line;
  while (std::getline(input, line)) {
    lines.push_back(line);
    if (lines.size() > max_lines) {
      lines.pop_front();
    }
  }
  return {lines.begin(), lines.end()};
}

}  // namespace bbp
