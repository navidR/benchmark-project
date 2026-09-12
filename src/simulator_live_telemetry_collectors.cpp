#include "simulator_live_telemetry_collectors.h"

#include <atomic>
#include <boost/json/object.hpp>
#include <boost/json/serialize.hpp>
#include <cstdint>
#include <memory>
#include <mutex>
#include <optional>
#include <string_view>

#include "bbp/drivers/chain_driver.h"
#include "bbp/logging.h"
#include "bbp/node_log_collector.h"
#include "bbp/periodic_metrics_collector.h"
#include "bbp/runtime_node_inventory.h"
#include "bbp/runtime_wallet_registry.h"
#include "bbp/simulation_event_kind.h"
#include "bbp/simulation_registry.h"
#include "bbp/simulator/constants.h"
#include "bbp/simulator/node_runtime.h"
#include "bbp/simulator/options.h"
#include "simulator_event_writing.h"
#include "simulator_node_log_tail.h"

namespace bbp::simulator_app_internal {

std::unique_ptr<NodeLogCollector> MakeLiveNodeLogCollector(
    const LiveTelemetryCollectorsContext& context) {
  return std::make_unique<NodeLogCollector>(
      context.driver,
      [context] {
        std::unique_lock<std::timed_mutex> publication_lock =
            context.acquire_runtime_publication_lock({});
        return context.node_inventory.ConfigSnapshot();
      },
      context.options.metrics_interval, kMaxLogTailBytes,
      [context](const ChainNodeConfig& config, ChainLogSource source,
                const LogTailChunk& chunk) {
        std::unique_lock<std::timed_mutex> publication_lock =
            context.acquire_runtime_publication_lock({});
        WriteLogTailChunkEvent(context.events_path, context.options, config,
                               source, chunk);
      });
}

std::unique_ptr<PeriodicMetricsCollector> MakeLivePeriodicMetricsCollector(
    const LiveTelemetryCollectorsContext& context) {
  return std::make_unique<PeriodicMetricsCollector>(
      context.options.metrics_sample_count, context.options.metrics_interval,
      [context](std::uint32_t sample) {
        RuntimeNodeSnapshot nodes;
        std::optional<RuntimeWalletSnapshot> wallet_snapshot;
        {
          std::unique_lock<std::timed_mutex> publication_lock =
              context.acquire_runtime_publication_lock(
                  context.metrics_rpc_stop_source.get_token());
          nodes = context.node_inventory.Snapshot();
          if (context.wallets_initialized.load(std::memory_order_acquire)) {
            wallet_snapshot.emplace(context.runtime_wallet_registry.Snapshot());
          }
        }
        WriteMetricsSnapshot(
            context.metrics_path, context.options, context.driver, nodes,
            context.run_process_state, context.metrics_synchronization,
            [&](const NodeRuntime& node, std::string_view error) {
              boost::json::object detail;
              detail["sample"] = sample;
              detail["error"] = error;
              WriteEvent(context.events_path, context.options.run_id,
                         node.config.id,
                         SimulationEventKind::kMetricsNodeUnavailable,
                         boost::json::serialize(detail));
              BBP_LOG(warning) << "metrics sample " << sample << " skipped "
                               << node.config.id << ": " << error;
            },
            [&] { return context.metrics_collector->StopRequested(); },
            context.metrics_rpc_stop_source.get_token(),
            wallet_snapshot ? &wallet_snapshot->registry().topology()
                            : nullptr);
        if (context.metrics_collector->StopRequested()) {
          return;
        }
        if (wallet_snapshot) {
          WriteWalletMetricsSnapshot(
              context.wallet_metrics_path, context.options, context.driver,
              nodes, wallet_snapshot->registry(),
              [&](std::uint32_t wallet_index, const NodeRuntime& node,
                  std::string_view error) {
                boost::json::object detail;
                detail["sample"] = sample;
                detail["wallet_index"] = wallet_index;
                detail["error"] = error;
                WriteEvent(context.events_path, context.options.run_id,
                           node.config.id,
                           SimulationEventKind::kWalletMetricsUnavailable,
                           boost::json::serialize(detail));
                BBP_LOG(warning) << "wallet metrics sample " << sample
                                 << " skipped #" << wallet_index << " on "
                                 << node.config.id << ": " << error;
              },
              context.metrics_rpc_stop_source.get_token());
        }
        if (context.metrics_collector->StopRequested()) {
          return;
        }
        boost::json::object detail;
        detail["sample"] = sample;
        detail["sample_count"] = context.options.metrics_sample_count;
        detail["interval_ms"] = context.options.metrics_interval.count();
        WriteEvent(context.events_path, context.options.run_id, "sim",
                   SimulationEventKind::kMetricsSample,
                   boost::json::serialize(detail));
      },
      [stop_token = context.stop_token] {
        return stop_token.stop_requested();
      });
}

}  // namespace bbp::simulator_app_internal
