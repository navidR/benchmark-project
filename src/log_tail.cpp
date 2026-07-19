#include "bbp/log_tail.h"

#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

#include <algorithm>
#include <cerrno>
#include <cstring>
#include <limits>
#include <stdexcept>
#include <string>

namespace bbp {
namespace {

constexpr std::uint64_t kBoundaryBytes = 64;

std::runtime_error LogIoError(const std::filesystem::path& path,
                              std::string_view operation) {
  return std::runtime_error(std::string(operation) + " failed for " +
                            path.string() + ": " + std::strerror(errno));
}

std::string ReadAt(int fd, std::uint64_t offset, std::uint64_t byte_count,
                   const std::filesystem::path& path) {
  if (offset > static_cast<std::uint64_t>(std::numeric_limits<off_t>::max()) ||
      byte_count >
          static_cast<std::uint64_t>(std::numeric_limits<std::size_t>::max())) {
    throw std::runtime_error("log file offset exceeds platform limits: " +
                             path.string());
  }

  std::string text(static_cast<std::size_t>(byte_count), '\0');
  std::size_t total = 0;
  while (total < text.size()) {
    const std::size_t remaining = text.size() - total;
    const std::size_t request =
        std::min(remaining,
                 static_cast<std::size_t>(std::numeric_limits<ssize_t>::max()));
    const std::uint64_t position = offset + total;
    const ssize_t read_count =
        pread(fd, text.data() + total, request, static_cast<off_t>(position));
    if (read_count < 0) {
      if (errno == EINTR) {
        continue;
      }
      throw LogIoError(path, "read log file");
    }
    if (read_count == 0) {
      break;
    }
    total += static_cast<std::size_t>(read_count);
  }
  text.resize(total);
  return text;
}

std::string ReadBoundary(int fd, std::uint64_t offset,
                         const std::filesystem::path& path) {
  const std::uint64_t boundary_size = std::min(offset, kBoundaryBytes);
  return ReadAt(fd, offset - boundary_size, boundary_size, path);
}

}  // namespace

std::optional<LogTailChunk> TailLogFile(const std::filesystem::path& path,
                                        const LogTailCursor& cursor,
                                        std::uint64_t max_bytes) {
  if (max_bytes == 0U) {
    throw std::runtime_error("max log tail bytes must be greater than zero");
  }

  const int fd = open(path.c_str(), O_RDONLY | O_CLOEXEC);
  if (fd < 0) {
    if (errno == ENOENT) {
      return std::nullopt;
    }
    throw LogIoError(path, "open log file");
  }

  bool descriptor_open = true;
  try {
    struct stat status {};
    if (fstat(fd, &status) != 0) {
      throw LogIoError(path, "inspect log file");
    }
    if (status.st_size < 0) {
      throw std::runtime_error("log file has a negative size: " +
                               path.string());
    }

    const std::uint64_t file_size = static_cast<std::uint64_t>(status.st_size);
    const std::uint64_t device = static_cast<std::uint64_t>(status.st_dev);
    const std::uint64_t inode = static_cast<std::uint64_t>(status.st_ino);
    LogTailChunk chunk;
    chunk.start_offset = cursor.offset;
    if (cursor.initialized) {
      const bool identity_changed =
          cursor.device != device || cursor.inode != inode;
      bool boundary_changed = false;
      if (!identity_changed && cursor.offset <= file_size &&
          !cursor.boundary.empty()) {
        boundary_changed =
            ReadBoundary(fd, cursor.offset, path) != cursor.boundary;
      }
      if (identity_changed || cursor.offset > file_size || boundary_changed) {
        chunk.start_offset = 0;
        chunk.offset_reset = true;
      }
    }

    const std::uint64_t available = file_size - chunk.start_offset;
    const std::uint64_t requested = std::min(available, max_bytes);
    chunk.text = ReadAt(fd, chunk.start_offset, requested, path);
    chunk.next_offset =
        chunk.start_offset + static_cast<std::uint64_t>(chunk.text.size());
    chunk.truncated = chunk.next_offset < file_size;
    chunk.next_cursor.offset = chunk.next_offset;
    chunk.next_cursor.device = device;
    chunk.next_cursor.inode = inode;
    chunk.next_cursor.boundary = ReadBoundary(fd, chunk.next_offset, path);
    chunk.next_cursor.initialized = true;

    const int close_result = close(fd);
    descriptor_open = false;
    if (close_result != 0) {
      throw LogIoError(path, "close log file");
    }
    return chunk;
  } catch (...) {
    if (descriptor_open) {
      close(fd);
    }
    throw;
  }
}

}  // namespace bbp
