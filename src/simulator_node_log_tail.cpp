#include "simulator_node_log_tail.h"

#include <boost/json/object.hpp>
#include <boost/json/serialize.hpp>
#include <optional>
#include <stdexcept>
#include <string>
#include <string_view>

#include "bbp/drivers/chain_driver.h"
#include "bbp/log_tail.h"
#include "bbp/logging.h"
#include "bbp/runtime_node_inventory.h"
#include "bbp/simulation_event_kind.h"
#include "bbp/simulator/constants.h"
#include "bbp/simulator/options.h"
#include "simulator_event_writing.h"

namespace bbp::simulator_app_internal {
namespace {

std::string LogTailDetail(std::string_view kind, const LogTailChunk& chunk) {
  boost::json::object detail;
  detail["kind"] = kind;
  detail["start_offset"] = chunk.start_offset;
  detail["next_offset"] = chunk.next_offset;
  detail["truncated"] = chunk.truncated;
  detail["offset_reset"] = chunk.offset_reset;
  detail["text"] = chunk.text;
  return boost::json::serialize(detail);
}

SimulationEventKind LogTailEventKind(ChainLogSource source) {
  switch (source) {
    case ChainLogSource::kDaemon:
      return SimulationEventKind::kDaemonLogTail;
    case ChainLogSource::kStdout:
      return SimulationEventKind::kStdoutTail;
    case ChainLogSource::kStderr:
      return SimulationEventKind::kStderrTail;
  }
  throw std::runtime_error("unknown chain log source");
}

void WriteLogTailEvent(const std::filesystem::path& events_path,
                       const Options& options, const ChainDriver& driver,
                       const NodeRuntime& node, ChainLogSource source,
                       LogTailCursor* cursor) {
  std::optional<LogTailChunk> chunk;
  try {
    chunk = driver.ReadLogTail(node.config, source, *cursor, kMaxLogTailBytes);
  } catch (const std::exception& error) {
    BBP_LOG(warning) << "cannot read " << ChainLogSourceName(source) << " for "
                     << node.config.id << ": " << error.what();
    return;
  }
  if (!chunk) {
    return;
  }
  *cursor = chunk->next_cursor;
  if (chunk->text.empty() && !chunk->truncated && !chunk->offset_reset) {
    return;
  }
  WriteLogTailChunkEvent(events_path, options, node.config, source, *chunk);
}

void WriteNodeLogTail(const std::filesystem::path& events_path,
                      const Options& options, const ChainDriver& driver,
                      NodeRuntime& node) {
  WriteLogTailEvent(events_path, options, driver, node, ChainLogSource::kStdout,
                    &node.stdout_log_cursor);
  WriteLogTailEvent(events_path, options, driver, node, ChainLogSource::kStderr,
                    &node.stderr_log_cursor);
  WriteLogTailEvent(events_path, options, driver, node, ChainLogSource::kDaemon,
                    &node.daemon_log_cursor);
}

}  // namespace

void WriteLogTailChunkEvent(const std::filesystem::path& events_path,
                            const Options& options,
                            const ChainNodeConfig& config,
                            ChainLogSource source, const LogTailChunk& chunk) {
  const std::string_view kind = ChainLogSourceName(source);
  WriteEvent(events_path, options.run_id, config.id, LogTailEventKind(source),
             LogTailDetail(kind, chunk));
}

void WriteNodeLogTails(const std::filesystem::path& events_path,
                       const Options& options, const ChainDriver& driver,
                       const RuntimeNodeSnapshot& nodes) {
  for (NodeRuntime& node : nodes) {
    WriteNodeLogTail(events_path, options, driver, node);
  }
}

}  // namespace bbp::simulator_app_internal
