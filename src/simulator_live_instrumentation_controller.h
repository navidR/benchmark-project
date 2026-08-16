#pragma once

#include <chrono>
#include <filesystem>
#include <functional>
#include <memory>
#include <mutex>
#include <optional>
#include <set>
#include <stop_token>
#include <string>
#include <string_view>
#include <vector>

#include "simulator_metrics_sampling.h"
#include "simulator_perf_counter_transactions.h"

namespace bbp {

class ChainDriver;
class RunProcessState;
class RuntimeNodeInventory;
class RuntimeWalletRegistry;
struct McpLiveInstrumentationService;
struct Options;
struct SimulationCommand;

namespace simulator_app_internal {

struct LiveInstrumentationRegistry;
class LiveInstrumentationController;

using LiveInstrumentationMeasurementCollector =
    std::function<std::vector<std::string>(const std::set<std::string>&,
                                           std::stop_token)>;

struct LiveInstrumentationControllerDeleter {
  void operator()(LiveInstrumentationController* controller) const noexcept;
};

using LiveInstrumentationControllerPtr =
    std::unique_ptr<LiveInstrumentationController,
                    LiveInstrumentationControllerDeleter>;

std::shared_ptr<LiveInstrumentationRegistry> MakeLiveInstrumentationRegistry();

LiveInstrumentationControllerPtr MakeLiveInstrumentationController(
    const Options& options, std::filesystem::path metrics_path,
    std::filesystem::path events_path, ChainDriver& driver,
    RuntimeNodeInventory& node_inventory,
    RuntimeWalletRegistry& wallet_registry, RunProcessState& run_process_state,
    std::timed_mutex& node_mutation_mutex,
    MetricsSnapshotSynchronization metrics_synchronization,
    std::shared_ptr<LiveInstrumentationRegistry> registry,
    std::optional<NodePerfCounterTransactionBackend> transaction_backend =
        std::nullopt,
    LiveInstrumentationMeasurementCollector measurement_collector = {});

std::shared_ptr<McpLiveInstrumentationService> MakeLiveInstrumentationService(
    LiveInstrumentationController& controller);

void ShutdownLiveInstrumentation(LiveInstrumentationController& controller,
                                 bool run_failed);

void RequireNoLiveInstrumentationCommandConflict(
    const LiveInstrumentationRegistry& registry,
    const SimulationCommand& command);

void ValidateLiveInstrumentationDuration(std::chrono::milliseconds duration,
                                         std::string_view field);

#ifdef BBP_ENABLE_TEST_HOOKS
void ApplyLiveInstrumentationPerfMutationForTest(
    LiveInstrumentationController& controller, std::string_view node_id,
    PerfCounterKind counter);
void SampleLiveInstrumentationNowForTest(
    LiveInstrumentationController& controller);
void ExpireLiveInstrumentationNowForTest(
    LiveInstrumentationController& controller);
void SetLiveInstrumentationExpiredWithoutWorkerWakeForTest(
    LiveInstrumentationController& controller);
#endif

}  // namespace simulator_app_internal
}  // namespace bbp
