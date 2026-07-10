#pragma once

#include <cstdint>
#include <filesystem>
#include <optional>
#include <string>

namespace bbp {

struct LogTailCursor {
  std::uint64_t offset = 0;
  std::uint64_t device = 0;
  std::uint64_t inode = 0;
  std::string boundary;
  bool initialized = false;
};

struct LogTailChunk {
  std::uint64_t start_offset = 0;
  std::uint64_t next_offset = 0;
  bool truncated = false;
  bool offset_reset = false;
  std::string text;
  LogTailCursor next_cursor;
};

std::optional<LogTailChunk> TailLogFile(const std::filesystem::path& path,
                                        const LogTailCursor& cursor,
                                        std::uint64_t max_bytes);

}  // namespace bbp
