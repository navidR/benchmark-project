#pragma once

#include <cstdint>
#include <filesystem>
#include <string>

namespace bsim {

struct LogTailChunk {
  std::uint64_t start_offset = 0;
  std::uint64_t next_offset = 0;
  bool truncated = false;
  bool offset_reset = false;
  std::string text;
};

LogTailChunk TailLogFile(const std::filesystem::path& path,
                         std::uint64_t offset, std::uint64_t max_bytes);

}  // namespace bsim
