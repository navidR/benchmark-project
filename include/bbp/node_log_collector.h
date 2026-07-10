#pragma once

#include <array>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <exception>
#include <functional>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

#include "bbp/drivers/chain_driver.h"

namespace bbp {

class NodeLogCollector {
 public:
  using ChunkHandler = std::function<void(const ChainNodeConfig&,
                                          ChainLogSource, const LogTailChunk&)>;

  NodeLogCollector(const ChainDriver& driver,
                   std::vector<ChainNodeConfig> nodes,
                   std::chrono::milliseconds interval,
                   std::uint64_t maximum_chunk_bytes, ChunkHandler handler);
  NodeLogCollector(const NodeLogCollector&) = delete;
  NodeLogCollector& operator=(const NodeLogCollector&) = delete;
  ~NodeLogCollector();

  void Start();
  void Stop();

 private:
  void Run();
  void PollOnce();

  const ChainDriver& driver_;
  std::vector<ChainNodeConfig> nodes_;
  std::chrono::milliseconds interval_;
  std::uint64_t maximum_chunk_bytes_;
  ChunkHandler handler_;
  std::vector<std::array<LogTailCursor, 3>> cursors_;
  std::vector<std::array<std::string, 3>> last_errors_;
  std::mutex mutex_;
  std::condition_variable wakeup_;
  std::thread thread_;
  std::exception_ptr handler_failure_;
  bool started_ = false;
  bool stopping_ = false;
};

}  // namespace bbp
