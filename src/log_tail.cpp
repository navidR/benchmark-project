#include "benchmark_sim/log_tail.h"

#include <algorithm>
#include <fstream>
#include <stdexcept>

namespace bsim {

LogTailChunk TailLogFile(const std::filesystem::path& path,
                         std::uint64_t offset, std::uint64_t max_bytes) {
  if (max_bytes == 0U) {
    throw std::runtime_error("max log tail bytes must be greater than zero");
  }

  const std::uint64_t file_size =
      static_cast<std::uint64_t>(std::filesystem::file_size(path));
  LogTailChunk chunk;
  chunk.start_offset = offset;
  if (chunk.start_offset > file_size) {
    chunk.start_offset = 0;
    chunk.offset_reset = true;
  }

  const std::uint64_t available = file_size - chunk.start_offset;
  const std::uint64_t bytes_to_read = std::min(available, max_bytes);
  chunk.truncated = bytes_to_read < available;
  chunk.next_offset = chunk.start_offset + bytes_to_read;

  std::ifstream input(path, std::ios::binary);
  if (!input) {
    throw std::runtime_error("open log file failed: " + path.string());
  }
  input.seekg(static_cast<std::streamoff>(chunk.start_offset));
  if (!input) {
    throw std::runtime_error("seek log file failed: " + path.string());
  }

  chunk.text.resize(static_cast<std::size_t>(bytes_to_read));
  if (bytes_to_read != 0U) {
    input.read(chunk.text.data(), static_cast<std::streamsize>(bytes_to_read));
    if (input.gcount() != static_cast<std::streamsize>(bytes_to_read)) {
      throw std::runtime_error("read log file failed: " + path.string());
    }
  }
  return chunk;
}

}  // namespace bsim
