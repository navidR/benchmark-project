#pragma once

#include <cstdint>
#include <filesystem>
#include <functional>
#include <memory>
#include <mutex>
#include <stop_token>

namespace bbp {

class SimulationCommandQueue;

struct TuiRunSnapshot {
  std::uint64_t generation = 0;
  std::filesystem::path run_root;
  std::shared_ptr<SimulationCommandQueue> command_queue;
  std::shared_ptr<std::timed_mutex> publication_mutex;
};

using TuiRunSnapshotProvider = std::function<TuiRunSnapshot()>;

int RunTuiReport(const std::filesystem::path& run_root, bool once,
                 std::uint32_t refresh_ms,
                 SimulationCommandQueue* command_queue = nullptr,
                 std::stop_token stop_token = {});
int RunTuiReport(TuiRunSnapshotProvider snapshot_provider, bool once,
                 std::uint32_t refresh_ms, std::stop_token stop_token = {});

}  // namespace bbp
