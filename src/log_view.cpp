#include "bbp/log_view.h"

#include <algorithm>
#include <array>
#include <deque>
#include <fstream>
#include <sstream>

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
  std::ifstream input(log_path, std::ios::binary);
  if (!input) {
    return {};
  }
  input.seekg(0, std::ios::end);
  const std::streampos end_position = input.tellg();
  if (end_position <= 0) {
    return {};
  }

  constexpr std::size_t kReadBlockBytes = 8192U;
  std::array<char, kReadBlockBytes> buffer{};
  std::streamoff begin = static_cast<std::streamoff>(end_position);
  std::size_t newline_count = 0;
  std::string tail;
  while (begin > 0 && newline_count <= max_lines) {
    const std::streamoff bytes =
        std::min<std::streamoff>(begin, kReadBlockBytes);
    begin -= bytes;
    input.seekg(begin);
    if (!input) {
      return {};
    }
    input.read(buffer.data(), bytes);
    if (input.gcount() != bytes) {
      return {};
    }
    newline_count += static_cast<std::size_t>(
        std::count(buffer.begin(), buffer.begin() + bytes, '\n'));
    tail.insert(0, buffer.data(), static_cast<std::size_t>(bytes));
  }
  if (begin > 0) {
    const std::size_t first_newline = tail.find('\n');
    if (first_newline == std::string::npos) {
      return {};
    }
    tail.erase(0, first_newline + 1U);
  }

  std::deque<std::string> lines;
  std::istringstream tail_input(std::move(tail));
  std::string line;
  while (std::getline(tail_input, line)) {
    lines.push_back(line);
    if (lines.size() > max_lines) {
      lines.pop_front();
    }
  }
  return {lines.begin(), lines.end()};
}

}  // namespace bbp
