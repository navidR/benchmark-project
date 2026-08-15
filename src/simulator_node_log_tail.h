#pragma once

#include <filesystem>

namespace bbp {

class ChainDriver;
class RuntimeNodeSnapshot;
enum class ChainLogSource;
struct ChainNodeConfig;
struct LogTailChunk;
struct Options;

namespace simulator_app_internal {

void WriteLogTailChunkEvent(const std::filesystem::path& events_path,
                            const Options& options,
                            const ChainNodeConfig& config,
                            ChainLogSource source, const LogTailChunk& chunk);

void WriteNodeLogTails(const std::filesystem::path& events_path,
                       const Options& options, const ChainDriver& driver,
                       const RuntimeNodeSnapshot& nodes);

}  // namespace simulator_app_internal
}  // namespace bbp
