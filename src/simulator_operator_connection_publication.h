#pragma once

#include <filesystem>

namespace bbp {

class ChainDriver;
class RuntimeNodeSnapshot;
struct Options;

namespace simulator_app_internal {

void PublishOperatorConnectionCommand(const Options& options,
                                      const std::filesystem::path& run_root,
                                      const std::filesystem::path& events_path,
                                      const ChainDriver& driver,
                                      const RuntimeNodeSnapshot& nodes,
                                      bool* resolved);

}  // namespace simulator_app_internal
}  // namespace bbp
