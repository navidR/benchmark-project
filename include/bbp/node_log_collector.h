#pragma once

#include <array>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <exception>
#include <functional>
#include <map>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

#include "bbp/drivers/chain_driver.h"
#include "bbp/node_config_snapshot.h"

namespace bbp {

class NodeLogCollector {
 public:
  using NodeProvider = std::function<NodeConfigSnapshot()>;
  using ChunkHandler = std::function<void(const ChainNodeConfig&,
                                          ChainLogSource, const LogTailChunk&)>;

  NodeLogCollector(const ChainDriver& driver,
                   std::vector<ChainNodeConfig> nodes,
                   std::chrono::milliseconds interval,
                   std::uint64_t maximum_chunk_bytes, ChunkHandler handler);
  NodeLogCollector(const ChainDriver& driver, NodeProvider node_provider,
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
  NodeProvider node_provider_;
  std::chrono::milliseconds interval_;
  std::uint64_t maximum_chunk_bytes_;
  ChunkHandler handler_;
  std::map<std::string, std::array<LogTailCursor, 3>> cursors_;
  std::map<std::string, std::array<std::string, 3>> last_errors_;
  std::mutex mutex_;
  std::condition_variable wakeup_;
  std::thread thread_;
  std::exception_ptr handler_failure_;
  bool started_ = false;
  bool stopping_ = false;
};

}  // namespace bbp
