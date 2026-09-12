#pragma once

#include <atomic>
#include <filesystem>
#include <memory>
#include <mutex>
#include <stop_token>

#include "simulator_metrics_sampling.h"

namespace bbp {

class ChainDriver;
class NodeLogCollector;
class PeriodicMetricsCollector;
class RunProcessState;
class RuntimeNodeInventory;
class RuntimeWalletRegistry;
struct Options;

namespace simulator_app_internal {

// Retained callbacks copy this context and borrow run state through the
// caller's collector shutdown, including the final log poll.
struct LiveTelemetryCollectorsContext {
  const Options& options;
  const std::filesystem::path& events_path;
  const std::filesystem::path& metrics_path;
  const std::filesystem::path& wallet_metrics_path;
  const ChainDriver& driver;
  RuntimeNodeInventory& node_inventory;
  RuntimeWalletRegistry& runtime_wallet_registry;
  RunProcessState& run_process_state;
  MetricsSnapshotSynchronization metrics_synchronization;
  const std::stop_source& metrics_rpc_stop_source;
  const std::atomic<bool>& wallets_initialized;
  const std::unique_ptr<PeriodicMetricsCollector>& metrics_collector;
  std::stop_token stop_token;
  std::unique_lock<std::timed_mutex> (*acquire_runtime_publication_lock)(
      std::stop_token);
};

std::unique_ptr<NodeLogCollector> MakeLiveNodeLogCollector(
    const LiveTelemetryCollectorsContext& context);
std::unique_ptr<PeriodicMetricsCollector> MakeLivePeriodicMetricsCollector(
    const LiveTelemetryCollectorsContext& context);

}  // namespace simulator_app_internal
}  // namespace bbp
