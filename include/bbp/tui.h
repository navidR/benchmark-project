#pragma once

#include <cstdint>
#include <filesystem>
#include <functional>
#include <memory>
#include <mutex>
#include <stop_token>
#include <string>

#include "bbp/run_report.h"

namespace bbp {

#ifdef BBP_FIRO_GUI_LAUNCHER
class OperatorConnectionLauncher;
#endif
class SimulationCommandQueue;
class ChainViewService;
class PoolViewService;

struct TuiMcpConnectionInfo {
  std::string endpoint;
  std::filesystem::path token_file;
  std::filesystem::path client_config_file;
};

struct TuiRunSnapshot {
  std::uint64_t generation = 0;
  std::filesystem::path run_root;
  std::shared_ptr<SimulationCommandQueue> command_queue;
#ifdef BBP_FIRO_GUI_LAUNCHER
  std::shared_ptr<OperatorConnectionLauncher> operator_connection_launcher = {};
#endif
  std::shared_ptr<std::timed_mutex> publication_mutex;
  std::shared_ptr<void> read_lease;
  std::shared_ptr<ChainViewService> chain_view = {};
  std::shared_ptr<PoolViewService> pool_view = {};
};

using TuiRunSnapshotProvider = std::function<TuiRunSnapshot()>;

// An initial reader, when supplied, must belong to run_root.
int RunTuiReport(const std::filesystem::path& run_root, bool once,
                 std::uint32_t refresh_ms,
                 const TuiMcpConnectionInfo& mcp_connection,
                 SimulationCommandQueue* command_queue = nullptr,
                 std::stop_token stop_token = {},
                 std::unique_ptr<IncrementalRunReport> initial_report = {});
int RunTuiReport(TuiRunSnapshotProvider snapshot_provider, bool once,
                 std::uint32_t refresh_ms,
                 const TuiMcpConnectionInfo& mcp_connection,
                 std::stop_token stop_token = {});

}  // namespace bbp
