#include "bbp/conntrack.h"

#include <fcntl.h>
#include <unistd.h>

#include <cerrno>
#include <charconv>
#include <sstream>
#include <stdexcept>
#include <string>
#include <system_error>

#include "bbp/util.h"

namespace bbp {
namespace {

std::uint32_t ReadLimit(const std::filesystem::path& path) {
  std::istringstream input(ReadText(path));
  std::string text;
  std::string extra;
  input >> text;
  std::uint32_t value = 0U;
  const auto [end, error] =
      std::from_chars(text.data(), text.data() + text.size(), value);
  if (error != std::errc{} || end != text.data() + text.size() || value == 0U ||
      (input >> extra)) {
    throw std::runtime_error("invalid connection tracking limit in " +
                             path.string());
  }
  return value;
}

}  // namespace

std::uint32_t EnsureConntrackCapacity(const std::filesystem::path& limit_path) {
  const std::uint32_t before = ReadLimit(limit_path);
  if (before >= kMinimumConntrackCapacity) {
    return before;
  }

  const std::string setting = std::to_string(kMinimumConntrackCapacity) + "\n";
  // Sysctls consume a single write; do not create or truncate a missing path.
  const int fd = open(limit_path.c_str(), O_WRONLY | O_CLOEXEC);
  if (fd < 0) {
    throw std::system_error(errno, std::generic_category(),
                            "open " + limit_path.string());
  }
  ssize_t written;
  do {
    written = write(fd, setting.data(), setting.size());
  } while (written < 0 && errno == EINTR);
  const int write_error = errno;
  const int closed = close(fd);
  const int close_error = errno;
  if (written < 0) {
    throw std::system_error(write_error, std::generic_category(),
                            "write " + limit_path.string());
  }
  if (static_cast<std::size_t>(written) != setting.size()) {
    throw std::runtime_error("incomplete write to " + limit_path.string());
  }
  if (closed != 0) {
    throw std::system_error(close_error, std::generic_category(),
                            "close " + limit_path.string());
  }

  const std::uint32_t after = ReadLimit(limit_path);
  if (after < kMinimumConntrackCapacity) {
    throw std::runtime_error("connection tracking limit read-back below " +
                             std::to_string(kMinimumConntrackCapacity));
  }
  return after;
}

}  // namespace bbp
