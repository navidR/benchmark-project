#include "bbp/node_log_collector.h"

#include <algorithm>
#include <stdexcept>
#include <utility>

#include "bbp/logging.h"

namespace bbp {
namespace {

constexpr std::array<ChainLogSource, 3> kLogSources = {
    ChainLogSource::kDaemon,
    ChainLogSource::kStdout,
    ChainLogSource::kStderr,
};

std::size_t SourceIndex(ChainLogSource source) {
  switch (source) {
    case ChainLogSource::kDaemon:
      return 0;
    case ChainLogSource::kStdout:
      return 1;
    case ChainLogSource::kStderr:
      return 2;
  }
  throw std::runtime_error("unknown chain log source");
}

}  // namespace

NodeLogCollector::NodeLogCollector(const ChainDriver& driver,
                                   std::vector<ChainNodeConfig> nodes,
                                   std::chrono::milliseconds interval,
                                   std::uint64_t maximum_chunk_bytes,
                                   ChunkHandler handler)
    : driver_(driver),
      nodes_(std::move(nodes)),
      interval_(interval),
      maximum_chunk_bytes_(maximum_chunk_bytes),
      handler_(std::move(handler)),
      cursors_(nodes_.size()),
      last_errors_(nodes_.size()) {
  if (interval_ <= std::chrono::milliseconds::zero()) {
    throw std::runtime_error("node log collection interval must be positive");
  }
  if (maximum_chunk_bytes_ == 0U) {
    throw std::runtime_error("node log chunk size must be positive");
  }
  if (!handler_) {
    throw std::runtime_error("node log collector requires a chunk handler");
  }
}

NodeLogCollector::~NodeLogCollector() {
  try {
    Stop();
  } catch (const std::exception& error) {
    BBP_LOG(error) << "node log collector shutdown failed: " << error.what();
  } catch (...) {
    BBP_LOG(error) << "node log collector shutdown failed";
  }
}

void NodeLogCollector::Start() {
  std::lock_guard<std::mutex> lock(mutex_);
  if (started_) {
    throw std::runtime_error("node log collector is already started");
  }
  started_ = true;
  thread_ = std::thread(&NodeLogCollector::Run, this);
}

void NodeLogCollector::Stop() {
  {
    std::lock_guard<std::mutex> lock(mutex_);
    if (!started_) {
      return;
    }
    stopping_ = true;
  }
  wakeup_.notify_all();
  if (thread_.joinable()) {
    thread_.join();
  }
  PollOnce();
  started_ = false;
  if (handler_failure_) {
    std::rethrow_exception(handler_failure_);
  }
}

void NodeLogCollector::Run() {
  while (true) {
    PollOnce();
    std::unique_lock<std::mutex> lock(mutex_);
    if (handler_failure_ ||
        wakeup_.wait_for(lock, interval_, [this] { return stopping_; })) {
      return;
    }
  }
}

void NodeLogCollector::PollOnce() {
  for (std::size_t node_index = 0; node_index < nodes_.size(); ++node_index) {
    for (ChainLogSource source : kLogSources) {
      const std::size_t source_index = SourceIndex(source);
      std::optional<LogTailChunk> chunk;
      try {
        chunk = driver_.ReadLogTail(nodes_[node_index], source,
                                    cursors_[node_index][source_index],
                                    maximum_chunk_bytes_);
        last_errors_[node_index][source_index].clear();
      } catch (const std::exception& error) {
        const std::string message = error.what();
        if (message != last_errors_[node_index][source_index]) {
          BBP_LOG(warning)
              << "cannot read " << ChainLogSourceName(source) << " for "
              << nodes_[node_index].id << ": " << message;
          last_errors_[node_index][source_index] = message;
        }
        continue;
      }
      if (!chunk) {
        continue;
      }
      cursors_[node_index][source_index] = chunk->next_cursor;
      if (chunk->text.empty() && !chunk->truncated && !chunk->offset_reset) {
        continue;
      }
      try {
        handler_(nodes_[node_index], source, *chunk);
      } catch (...) {
        handler_failure_ = std::current_exception();
        std::lock_guard<std::mutex> lock(mutex_);
        stopping_ = true;
        wakeup_.notify_all();
        return;
      }
    }
  }
}

}  // namespace bbp
