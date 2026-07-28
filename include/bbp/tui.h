#pragma once

#include <cstdint>
#include <filesystem>
#include <functional>
#include <memory>
#include <mutex>
#include <stop_token>
#include <string>

namespace bbp {

class FiroQtLauncherService;
class SimulationCommandQueue;

struct TuiMcpConnectionInfo {
  std::string endpoint;
  std::filesystem::path token_file;
  std::filesystem::path client_config_file;
};

struct TuiRunSnapshot {
  std::uint64_t generation = 0;
  std::filesystem::path run_root;
  std::shared_ptr<SimulationCommandQueue> command_queue;
  std::shared_ptr<FiroQtLauncherService> firo_qt_launcher_service = {};
  std::shared_ptr<std::timed_mutex> publication_mutex;
  std::shared_ptr<void> read_lease;
};

using TuiRunSnapshotProvider = std::function<TuiRunSnapshot()>;

int RunTuiReport(const std::filesystem::path& run_root, bool once,
                 std::uint32_t refresh_ms,
                 const TuiMcpConnectionInfo& mcp_connection,
                 SimulationCommandQueue* command_queue = nullptr,
                 std::stop_token stop_token = {});
int RunTuiReport(TuiRunSnapshotProvider snapshot_provider, bool once,
                 std::uint32_t refresh_ms,
                 const TuiMcpConnectionInfo& mcp_connection,
                 std::stop_token stop_token = {});

}  // namespace bbp
