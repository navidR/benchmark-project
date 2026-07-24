#include "bbp/node_log_collector.h"

#include <algorithm>
#include <set>
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
    : NodeLogCollector(
          driver,
          [nodes = std::move(nodes)] { return NodeConfigSnapshot(nodes); },
          interval, maximum_chunk_bytes, std::move(handler)) {}

NodeLogCollector::NodeLogCollector(const ChainDriver& driver,
                                   NodeProvider node_provider,
                                   std::chrono::milliseconds interval,
                                   std::uint64_t maximum_chunk_bytes,
                                   ChunkHandler handler)
    : driver_(driver),
      node_provider_(std::move(node_provider)),
      interval_(interval),
      maximum_chunk_bytes_(maximum_chunk_bytes),
      handler_(std::move(handler)) {
  if (interval_ <= std::chrono::milliseconds::zero()) {
    throw std::runtime_error("node log collection interval must be positive");
  }
  if (maximum_chunk_bytes_ == 0U) {
    throw std::runtime_error("node log chunk size must be positive");
  }
  if (!handler_) {
    throw std::runtime_error("node log collector requires a chunk handler");
  }
  if (!node_provider_) {
    throw std::runtime_error("node log collector requires a node provider");
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
  if (!handler_failure_) {
    PollOnce();
  }
  started_ = false;
  if (handler_failure_) {
    std::rethrow_exception(handler_failure_);
  }
}

void NodeLogCollector::Run() {
  while (true) {
    try {
      PollOnce();
    } catch (...) {
      handler_failure_ = std::current_exception();
      return;
    }
    std::unique_lock<std::mutex> lock(mutex_);
    if (handler_failure_ ||
        wakeup_.wait_for(lock, interval_, [this] { return stopping_; })) {
      return;
    }
  }
}

void NodeLogCollector::PollOnce() {
  const NodeConfigSnapshot snapshot = node_provider_();
  const std::vector<ChainNodeConfig>& nodes = snapshot.nodes();
  std::set<std::string> active_node_ids;
  for (const ChainNodeConfig& node : nodes) {
    if (node.id.empty() || !active_node_ids.insert(node.id).second) {
      throw std::runtime_error(
          "node log collector provider returned an empty or duplicate node "
          "id");
    }
    std::array<LogTailCursor, 3>& node_cursors = cursors_[node.id];
    std::array<std::string, 3>& node_errors = last_errors_[node.id];
    for (ChainLogSource source : kLogSources) {
      const std::size_t source_index = SourceIndex(source);
      std::optional<LogTailChunk> chunk;
      try {
        chunk = driver_.ReadLogTail(node, source, node_cursors[source_index],
                                    maximum_chunk_bytes_);
        node_errors[source_index].clear();
      } catch (const std::exception& error) {
        const std::string message = error.what();
        if (message != node_errors[source_index]) {
          BBP_LOG(warning) << "cannot read " << ChainLogSourceName(source)
                           << " for " << node.id << ": " << message;
          node_errors[source_index] = message;
        }
        continue;
      }
      if (!chunk) {
        continue;
      }
      node_cursors[source_index] = chunk->next_cursor;
      if (chunk->text.empty() && !chunk->truncated && !chunk->offset_reset) {
        continue;
      }
      try {
        handler_(node, source, *chunk);
      } catch (...) {
        handler_failure_ = std::current_exception();
        std::lock_guard<std::mutex> lock(mutex_);
        stopping_ = true;
        wakeup_.notify_all();
        return;
      }
    }
  }
  std::erase_if(cursors_, [&](const auto& item) {
    return !active_node_ids.contains(item.first);
  });
  std::erase_if(last_errors_, [&](const auto& item) {
    return !active_node_ids.contains(item.first);
  });
}

}  // namespace bbp
