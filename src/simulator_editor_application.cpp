#include "simulator_editor_application.h"

#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

#include <algorithm>
#include <atomic>
#include <boost/json/array.hpp>
#include <boost/json/object.hpp>
#include <cerrno>
#include <chrono>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <exception>
#include <filesystem>
#include <functional>
#include <limits>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <stdexcept>
#include <stop_token>
#include <string>
#include <string_view>
#include <system_error>
#include <thread>
#include <utility>
#include <vector>

#include "bbp/cgroup.h"
#include "bbp/drivers/chain_driver_registry.h"
#include "bbp/logging.h"
#include "bbp/mcp_endpoint.h"
#include "bbp/mcp_host_application.h"
#include "bbp/mcp_live_application.h"
#include "bbp/network.h"
#include "bbp/network_allocation_lock.h"
#include "bbp/operator_connection.h"
#include "bbp/run_ownership.h"
#include "bbp/runtime_node_inventory.h"
#include "bbp/runtime_node_resource_manifest.h"
#include "bbp/scenario_service.h"
#include "bbp/signal_stop_monitor.h"
#include "bbp/simulation_cancelled.h"
#include "bbp/simulation_command_queue.h"
#include "bbp/simulator/node_runtime.h"
#include "bbp/simulator/options.h"
#include "bbp/tui.h"
#include "bbp/util.h"
#include "simulator_cancellable_waiting.h"
#include "simulator_combined_stop_token.h"
#include "simulator_managed_run_root.h"
#include "simulator_network_launch_planning.h"
#include "simulator_node_process_state.h"
#include "simulator_offline_run_cleanup.h"
#include "simulator_resolved_scenario_persistence.h"
#include "simulator_retained_run_registry.h"
#include "simulator_source_scenario_persistence.h"
#include "simulator_stop_coordination.h"
#include "simulator_workload_service_shutdown_diagnostic.h"

namespace bbp::simulator_app_internal {
namespace {

enum class EditorRunState {
  kStarting,
  kActive,
  kStopping,
  kStopped,
  kFailed,
};

bool IsTerminalEditorRunState(EditorRunState state) {
  return state == EditorRunState::kStopped || state == EditorRunState::kFailed;
}

struct EditorTuiReadLeaseState {
  std::mutex mutex;
  std::condition_variable_any drained;
  std::size_t readers = 0U;
};

class EditorTuiReadLease {
 public:
  explicit EditorTuiReadLease(std::shared_ptr<EditorTuiReadLeaseState> state)
      : state_(std::move(state)) {
    std::lock_guard<std::mutex> lock(state_->mutex);
    ++state_->readers;
  }

  ~EditorTuiReadLease() {
    std::lock_guard<std::mutex> lock(state_->mutex);
    --state_->readers;
    state_->drained.notify_all();
  }

  EditorTuiReadLease(const EditorTuiReadLease&) = delete;
  EditorTuiReadLease& operator=(const EditorTuiReadLease&) = delete;

 private:
  std::shared_ptr<EditorTuiReadLeaseState> state_;
};

[[maybe_unused]] std::string_view EditorRunStateName(EditorRunState state) {
  switch (state) {
    case EditorRunState::kStarting:
      return "starting";
    case EditorRunState::kActive:
      return "active";
    case EditorRunState::kStopping:
      return "stopping";
    case EditorRunState::kStopped:
      return "stopped";
    case EditorRunState::kFailed:
      return "failed";
  }
  throw std::logic_error("unknown editor run state");
}

struct EditorRunContext {
  std::uint64_t generation = 0U;
  std::shared_ptr<Options> options;
  std::optional<boost::json::object> source_scenario;
  std::shared_ptr<SimulationCommandQueue> command_queue;
#ifdef BBP_FIRO_GUI_LAUNCHER
  std::shared_ptr<OperatorConnectionLauncher> operator_connection_launcher;
#endif
  std::shared_ptr<RuntimeNodeInventory> node_inventory;
  std::shared_ptr<EditorTuiReadLeaseState> tui_read_lease_state =
      std::make_shared<EditorTuiReadLeaseState>();
  std::atomic<RunStopTick> run_stop_tick{kRunStopNotObserved};
  std::stop_source simulation_stop_source;
  std::shared_ptr<McpLiveApplication> mcp_application;
  std::shared_ptr<ReservedManagedRunRoot> reserved_run_root;
  std::function<void(std::stop_token)> run_root_prepared;
  std::jthread worker;
  mutable std::mutex mutex;
  std::condition_variable_any state_changed;
  EditorRunState state = EditorRunState::kStarting;
  bool reached_active = false;
  bool host_stop_requested = false;
  bool retained_run_root_available = false;
  std::exception_ptr failure;
  int result = 1;
};

struct EditorRunSnapshot {
  std::uint64_t generation = 0U;
  std::string run_id;
  std::filesystem::path run_root;
  std::string chain;
  std::uint32_t node_count = 0U;
  std::uint32_t node_capacity = 0U;
  boost::json::value network_allocation = nullptr;
  std::uint32_t available_node_capacity = 0U;
  EditorRunState state = EditorRunState::kStarting;
  std::shared_ptr<SimulationCommandQueue> command_queue;
#ifdef BBP_FIRO_GUI_LAUNCHER
  std::shared_ptr<OperatorConnectionLauncher> operator_connection_launcher;
#endif
  std::shared_ptr<McpLiveApplication> mcp_application;
  std::shared_ptr<void> tui_read_lease;
};

BenchmarkHeadlessResult RunPreparedBenchmark(
    const std::shared_ptr<EditorRunContext>& context,
    const EditorApplicationDependencies& dependencies) {
  Options& options = *context->options;
  const std::filesystem::path run_root = BenchmarkRunRoot(options);
  const std::stop_token setup_stop_token =
      context->simulation_stop_source.get_token();
  std::unique_ptr<NetworkAllocationLock> network_allocation_lock;
  bool run_prepared = false;
  try {
    ThrowIfStopRequested(setup_stop_token);
    if (context->reserved_run_root) {
      const RunOwnership& ownership = context->reserved_run_root->ownership();
      if (ownership.run_id != options.run_id ||
          ownership.run_root != run_root.lexically_normal()) {
        throw std::runtime_error(
            "reserved replay destination does not match the launched run");
      }
      options.run_ownership = ownership;
      context->reserved_run_root->Adopt();
      run_prepared = true;
      PrepareManagedRunRoot(&options, MakeNodeVethConfig,
                            context->reserved_run_root);
    } else {
      PrepareManagedRunRoot(&options, MakeNodeVethConfig);
      run_prepared = true;
    }
    context->retained_run_root_available = true;
    if (context->run_root_prepared) {
      context->run_root_prepared(setup_stop_token);
    }
    if (options.isolate_network) {
      network_allocation_lock =
          std::make_unique<NetworkAllocationLock>(setup_stop_token);
      RequireRunNetworkInterfacesAvailable(options, setup_stop_token);
      options.network_address_plan = SimulationNetworkAddressPlan::Allocate(
          options.run_id, options.node_capacity,
          ListIpv4Routes(setup_stop_token), ListIpv4Addresses(setup_stop_token),
          options.network_address_pool);
    }
    ThrowIfStopRequested(setup_stop_token);
    WriteScenarioFiles(options, run_root, ChainDriverSpecFor(options.chain),
                       context->reserved_run_root
                           ? context->reserved_run_root->descriptor()
                           : -1);
    if (context->source_scenario) {
      WriteSourceScenarioFile(
          *context->source_scenario, options.run_id, run_root,
          context->reserved_run_root
              ? std::optional<int>(context->reserved_run_root->descriptor())
              : std::nullopt,
          &options);
    }
    if (context->reserved_run_root &&
        LoadRunOwnershipAt(options.run_id, run_root.lexically_normal(),
                           context->reserved_run_root->descriptor()) !=
            RequireRunOwnership(options)) {
      throw std::runtime_error(
          "reserved replay destination identity changed during publication");
    }
    ThrowIfStopRequested(setup_stop_token);
  } catch (...) {
    const std::exception_ptr setup_failure = std::current_exception();
    if (run_prepared) {
      try {
        RemovePreparedRunRoot(options, context->reserved_run_root);
        context->retained_run_root_available = false;
      } catch (...) {
        throw std::runtime_error(
            "run setup failed: " +
            dependencies.exception_message(setup_failure) +
            "; setup cleanup also failed: " +
            dependencies.exception_message(std::current_exception()));
      }
    }
    if (setup_stop_token.stop_requested()) {
      throw SimulationCancelled();
    }
    std::rethrow_exception(setup_failure);
  }

  return dependencies.run_benchmark_headless(
      options, *context->command_queue, *context->mcp_application,
      *context->node_inventory, context->simulation_stop_source,
      context->run_stop_tick, {});
}

class EditorRunController {
 public:
  explicit EditorRunController(
#ifdef BBP_ENABLE_TEST_HOOKS
      const std::function<void()>& run_cleanup_root_removed_test_hook,
#endif
      EditorApplicationDependencies dependencies)
      :
#ifdef BBP_ENABLE_TEST_HOOKS
        run_cleanup_root_removed_test_hook_(run_cleanup_root_removed_test_hook),
#endif
        dependencies_(dependencies) {
  }

  ~EditorRunController() {
    try {
      Shutdown();
    } catch (...) {
    }
  }

  EditorRunController(const EditorRunController&) = delete;
  EditorRunController& operator=(const EditorRunController&) = delete;

  void SetEvidenceCallbacks(
      std::function<void(McpEvidenceRecord)> publish_evidence,
      std::function<void(std::string_view)> close_run_subscriptions) {
    std::lock_guard<std::timed_mutex> transition_lock(transition_mutex_);
    std::lock_guard<std::mutex> lock(mutex_);
    if (active_) {
      throw std::logic_error(
          "MCP evidence callbacks must be configured before launching a run");
    }
    publish_evidence_ = std::move(publish_evidence);
    close_run_subscriptions_ = std::move(close_run_subscriptions);
  }

  std::shared_ptr<EditorRunContext> LaunchScenario(
      const boost::json::object& scenario,
      const std::filesystem::path& benchmark_root, std::stop_token stop_token) {
    boost::json::object hosted_scenario = scenario;
    Options options = PrepareHostedScenario(&hosted_scenario, benchmark_root);
    return Launch(std::move(options), std::move(hosted_scenario), stop_token);
  }

  std::shared_ptr<EditorRunContext> ReplayScenario(
      std::string_view source_run_id,
      std::optional<std::string> destination_run_id,
      const std::filesystem::path& benchmark_root, std::stop_token stop_token) {
    RequireSafeRunId(source_run_id);
    if (destination_run_id) {
      RequireSafeRunId(*destination_run_id);
      if (*destination_run_id == source_run_id) {
        throw std::invalid_argument(
            "replay destination run id must differ from its source");
      }
    }
    std::unique_lock<std::timed_mutex> transition_lock =
        AcquireTransitionLock(stop_token);
    if (CurrentRun(false)) {
      throw McpOperationFailure(
          "run_already_active",
          "a managed run is already starting, active, or stopping", true);
    }

    const std::filesystem::path source_root =
        CleanupRunRoot(benchmark_root, source_run_id);
    const boost::json::object source_scenario =
        LoadRetainedSourceScenario(source_root, source_run_id, stop_token);

    const auto launch_destination = [&](std::string_view destination) {
      boost::json::object replay_scenario = source_scenario;
      replay_scenario["run_id"] = destination;
      Options options = PrepareHostedScenario(&replay_scenario, benchmark_root);
      return LaunchLocked(std::move(options), std::move(replay_scenario),
                          stop_token, true);
    };
    if (destination_run_id) {
      return launch_destination(*destination_run_id);
    }

    constexpr std::size_t kMaximumGeneratedRunIdAttempts = 32U;
    for (std::size_t attempt = 0U; attempt < kMaximumGeneratedRunIdAttempts;
         ++attempt) {
      if (stop_token.stop_requested()) {
        throw McpOperationCancelled();
      }
      const std::string candidate = MakeRunId();
      if (candidate == source_run_id) {
        continue;
      }
      try {
        return launch_destination(candidate);
      } catch (const McpOperationFailure& failure) {
        if (failure.code() != "run_replay_destination_exists") {
          throw;
        }
      }
    }
    throw McpOperationFailure("run_replay_id_unavailable",
                              "BBP could not allocate a fresh replay run id",
                              true);
  }

  std::shared_ptr<EditorRunContext> LaunchOptions(Options options) {
    return Launch(std::move(options), std::nullopt, {});
  }

  EditorRunSnapshot WaitUntilActive(
      const std::shared_ptr<EditorRunContext>& context,
      std::stop_token stop_token) const {
    std::unique_lock<std::mutex> lock(context->mutex);
    const bool ready = context->state_changed.wait(lock, stop_token, [&] {
      return context->reached_active ||
             IsTerminalEditorRunState(context->state);
    });
    if (!ready) {
      throw McpOperationCancelled();
    }
    if (context->reached_active) {
      return SnapshotLocked(*context);
    }
    if (context->failure) {
      throw McpOperationFailure(
          "run_launch_failed",
          "managed run startup failed: " +
              dependencies_.exception_message(context->failure),
          false);
    }
    throw McpOperationFailure("run_launch_cancelled",
                              "managed run stopped before startup completed",
                              false);
  }

  EditorRunSnapshot StopRun(std::string_view run_id,
                            std::chrono::seconds timeout,
                            std::stop_token stop_token) {
    const auto deadline = std::chrono::steady_clock::now() + timeout;
    std::shared_ptr<EditorRunContext> context;
    {
      std::unique_lock<std::timed_mutex> transition_lock(transition_mutex_,
                                                         std::defer_lock);
      constexpr auto kTransitionPollInterval = std::chrono::milliseconds(25);
      while (!transition_lock.try_lock_until(std::min(
          deadline,
          std::chrono::steady_clock::now() + kTransitionPollInterval))) {
        if (stop_token.stop_requested()) {
          throw McpOperationCancelled();
        }
        if (std::chrono::steady_clock::now() >= deadline) {
          throw McpOperationFailure(
              "run_stop_timeout",
              "managed run stop could not start before the timeout", true);
        }
      }
      if (stop_token.stop_requested()) {
        throw McpOperationCancelled();
      }
      if (std::chrono::steady_clock::now() >= deadline) {
        throw McpOperationFailure(
            "run_stop_timeout",
            "managed run stop could not start before the timeout", true);
      }
      {
        std::lock_guard<std::mutex> lock(mutex_);
        context = active_;
      }
      if (!context) {
        throw McpOperationFailure("run_not_active",
                                  "there is no active managed run", false);
      }
      {
        std::lock_guard<std::mutex> lock(context->mutex);
        if (context->options->run_id != run_id) {
          throw McpOperationFailure(
              "run_id_mismatch",
              "requested run does not match the active managed run", false);
        }
        if (context->state == EditorRunState::kStopping ||
            IsTerminalEditorRunState(context->state)) {
          throw McpOperationFailure(
              "run_not_active", "the managed run is no longer active", false);
        }
        context->host_stop_requested = true;
      }
      context->mcp_application->MarkRunStopping();
      RequestStop(context);
    }

    std::unique_lock<std::mutex> lock(context->mutex);
    const bool stopped = context->state_changed.wait_until(
        lock, stop_token, deadline,
        [&] { return IsTerminalEditorRunState(context->state); });
    if (!stopped) {
      if (stop_token.stop_requested()) {
        throw McpOperationCancelled();
      }
      throw McpOperationFailure(
          "run_stop_timeout",
          "managed run cleanup did not finish before the timeout", true);
    }
    if (context->state == EditorRunState::kFailed) {
      if (context->failure) {
        try {
          std::rethrow_exception(context->failure);
        } catch (const WorkloadServiceShutdownTimeout& error) {
          throw McpOperationFailure("workload_service_shutdown_timeout",
                                    error.what(), false,
                                    boost::json::array{error.Diagnostic()});
        } catch (...) {
        }
      }
      const std::string detail =
          context->failure ? dependencies_.exception_message(context->failure)
                           : "unknown managed-run failure";
      throw McpOperationFailure(
          "run_stop_failed",
          "managed run failed during bounded shutdown: " + detail, false);
    }
    return SnapshotLocked(*context);
  }

  McpRunCleanupResult CleanRun(const std::filesystem::path& benchmark_root,
                               std::string_view run_id,
                               std::chrono::seconds timeout,
                               bool remove_retained_artifacts,
                               std::stop_token stop_token) {
    dependencies_.require_safe_output_directory(benchmark_root);
    RequireSafeRunId(run_id);
    const auto deadline = std::chrono::steady_clock::now() + timeout;
    std::stop_source deadline_stop_source;
    std::jthread deadline_timer(
        [deadline, &deadline_stop_source](std::stop_token timer_stop_token) {
          try {
            WaitUntil(deadline, timer_stop_token);
          } catch (const SimulationCancelled&) {
            return;
          }
          if (!timer_stop_token.stop_requested()) {
            deadline_stop_source.request_stop();
          }
        });
    CombinedStopToken operation_stop_tokens(stop_token,
                                            deadline_stop_source.get_token());
    const std::stop_token operation_stop_token =
        operation_stop_tokens.get_token();
    std::unique_lock<std::timed_mutex> transition_lock(transition_mutex_,
                                                       std::defer_lock);
    constexpr auto kTransitionPollInterval = std::chrono::milliseconds(25);
    while (!transition_lock.try_lock_until(
        std::min(deadline,
                 std::chrono::steady_clock::now() + kTransitionPollInterval))) {
      if (stop_token.stop_requested()) {
        throw McpOperationCancelled();
      }
      if (std::chrono::steady_clock::now() >= deadline) {
        throw McpOperationFailure(
            "run_clean_timeout",
            "run cleanup could not start before the timeout", true);
      }
    }

    try {
      if (stop_token.stop_requested()) {
        throw McpOperationCancelled();
      }
      if (std::chrono::steady_clock::now() >= deadline) {
        throw McpOperationFailure(
            "run_clean_timeout",
            "run cleanup could not start before the timeout", true);
      }

      std::shared_ptr<EditorRunContext> context;
      {
        std::lock_guard<std::mutex> lock(mutex_);
        if (shutdown_) {
          throw McpOperationFailure(
              "application_stopping",
              "BBP is shutting down and cannot clean a retained run", false);
        }
        context = active_;
      }
      if (context) {
        std::lock_guard<std::mutex> lock(context->mutex);
        if (!IsTerminalEditorRunState(context->state)) {
          throw McpOperationFailure(
              "run_cleanup_requires_stop",
              "run.clean requires the managed run to be stopped first", true);
        }
      }
      if (context) {
        ReapTerminalRun(deadline, operation_stop_token);
      }
      if (stop_token.stop_requested()) {
        throw McpOperationCancelled();
      }
      if (std::chrono::steady_clock::now() >= deadline) {
        throw McpOperationFailure(
            "run_clean_timeout",
            "run cleanup could not start before the timeout", true);
      }

      const std::filesystem::path run_root =
          CleanupRunRoot(benchmark_root, run_id);
      const std::optional<OwnedRunRootCleanupReceipt> durable_receipt =
          TryLoadOwnedRunRootCleanupReceipt(run_id, run_root, deadline,
                                            operation_stop_token);
      auto receipt = cleanup_receipts_.find(run_id);
      if (receipt != cleanup_receipts_.end() &&
          receipt->second.ownership.run_root != run_root) {
        throw McpOperationFailure(
            "run_cleanup_identity_reused",
            "run cleanup receipt does not match the configured run root",
            false);
      }
      if (durable_receipt) {
        if (receipt != cleanup_receipts_.end() &&
            (receipt->second.ownership != durable_receipt->ownership ||
             receipt->second.root_identity != durable_receipt->root_identity)) {
          throw McpOperationFailure(
              "run_cleanup_identity_reused",
              "durable and in-memory cleanup receipts disagree", false);
        }
        if (receipt == cleanup_receipts_.end()) {
          const auto inserted = cleanup_receipts_.emplace(
              std::string(run_id),
              RunCleanupReceipt{
                  .ownership = durable_receipt->ownership,
                  .root_identity = durable_receipt->root_identity,
                  .result = SuccessfulCleanupResult(run_id),
                  .external_cleanup_complete = true,
                  .complete = false,
              });
          if (!inserted.second) {
            throw std::logic_error(
                "durable cleanup receipt insertion lost serialization");
          }
          receipt = inserted.first;
        }
      }
      bool durable_receipt_available = durable_receipt.has_value();
      std::optional<OwnedRunRootIdentity> initial_identity;
      try {
        initial_identity =
            InspectRunRootIdentity(run_root, deadline, operation_stop_token);
      } catch (const std::exception& error) {
        if (stop_token.stop_requested()) {
          throw McpOperationCancelled();
        }
        if (std::chrono::steady_clock::now() >= deadline) {
          throw McpOperationFailure(
              "run_clean_timeout",
              "run cleanup root verification exceeded the timeout", true);
        }
        throw McpOperationFailure(
            "run_cleanup_unverified",
            "run cleanup could not verify the retained root identity: " +
                std::string(error.what()),
            false);
      }
      if (durable_receipt_available && !initial_identity) {
        const std::filesystem::path quarantine =
            OwnedRunRootCleanupQuarantinePath(receipt->second.ownership);
        if (!InspectRunRootIdentity(quarantine, deadline,
                                    operation_stop_token)) {
          receipt->second.complete = true;
          return receipt->second.result;
        }
      }
      if (receipt != cleanup_receipts_.end() && !receipt->second.complete &&
          !remove_retained_artifacts) {
        throw McpOperationFailure(
            "run_cleanup_state_uncertain",
            "an incomplete artifact-removal cleanup must be resumed with "
            "remove_retained_artifacts enabled",
            false);
      }
      const auto require_removed_artifacts_absent =
          [&](const std::filesystem::path& quarantine,
              std::string_view failure_detail) {
            if (InspectRunRootIdentity(run_root) ||
                InspectRunRootIdentity(quarantine)) {
              throw McpOperationFailure("run_cleanup_identity_reused",
                                        std::string(failure_detail), false);
            }
          };
      const auto complete_prepared_artifact_removal = [&] {
        const std::filesystem::path quarantine =
            OwnedRunRootCleanupQuarantinePath(receipt->second.ownership);
        const std::optional<OwnedRunRootIdentity> quarantined_identity =
            InspectRunRootIdentity(quarantine, deadline, operation_stop_token);
        if ((initial_identity &&
             *initial_identity != receipt->second.root_identity) ||
            (quarantined_identity &&
             *quarantined_identity != receipt->second.root_identity)) {
          throw McpOperationFailure(
              "run_cleanup_identity_reused",
              "an incomplete cleanup found a foreign public or quarantined "
              "run root",
              false);
        }
        if (initial_identity && quarantined_identity) {
          throw McpOperationFailure(
              "run_cleanup_identity_reused",
              "an incomplete cleanup found both public and quarantined run "
              "roots",
              false);
        }
        if (!initial_identity && !quarantined_identity) {
          if (!durable_receipt_available) {
            throw McpOperationFailure(
                "run_cleanup_state_uncertain",
                "a prepared run cleanup lost its ownership root before "
                "completion was verified",
                false);
          }
          receipt->second.complete = true;
          return receipt->second.result;
        }
        DetachRunLogFile(run_root, deadline, operation_stop_token);
        if (!durable_receipt_available) {
          WriteOwnedRunRootCleanupReceipt(receipt->second.ownership,
                                          receipt->second.root_identity,
                                          deadline, operation_stop_token);
          durable_receipt_available = true;
        }
        RemoveOwnedRunRoot(receipt->second.ownership, deadline,
                           operation_stop_token, receipt->second.root_identity);
#ifdef BBP_ENABLE_TEST_HOOKS
        if (run_cleanup_root_removed_test_hook_) {
          run_cleanup_root_removed_test_hook_();
        }
#endif
        require_removed_artifacts_absent(
            quarantine,
            "a public or quarantined root appeared while prepared cleanup "
            "completed");
        receipt->second.complete = true;
        return receipt->second.result;
      };
      if (receipt != cleanup_receipts_.end() && !receipt->second.complete &&
          receipt->second.external_cleanup_complete) {
        return complete_prepared_artifact_removal();
      }
      if (receipt != cleanup_receipts_.end() && receipt->second.complete) {
        const std::filesystem::path quarantine =
            OwnedRunRootCleanupQuarantinePath(receipt->second.ownership);
        if (initial_identity || InspectRunRootIdentity(quarantine, deadline,
                                                       operation_stop_token)) {
          throw McpOperationFailure(
              "run_cleanup_identity_reused",
              "a run root reappeared after its verified cleanup completed",
              false);
        }
        return receipt->second.result;
      }
      if (!initial_identity) {
        if (receipt == cleanup_receipts_.end()) {
          throw McpOperationFailure(
              "run_cleanup_unverified",
              "run cleanup cannot verify ownership of an absent run root",
              false);
        }
        throw McpOperationFailure(
            "run_cleanup_state_uncertain",
            "a prepared run cleanup lost its ownership root before "
            "completion was verified",
            false);
      }

      RunOwnership ownership;
      try {
        ownership = LoadRunOwnership(std::string(run_id), run_root,
                                     operation_stop_token);
      } catch (const std::exception& error) {
        if (stop_token.stop_requested()) {
          throw McpOperationCancelled();
        }
        if (std::chrono::steady_clock::now() >= deadline) {
          throw McpOperationFailure(
              "run_clean_timeout",
              "run cleanup ownership verification exceeded the timeout", true);
        }
        throw McpOperationFailure("run_cleanup_unverified",
                                  "run cleanup could not verify ownership: " +
                                      std::string(error.what()),
                                  false);
      }
      const std::optional<OwnedRunRootIdentity> confirmed_identity =
          InspectRunRootIdentity(run_root, deadline, operation_stop_token);
      if (!confirmed_identity || *confirmed_identity != *initial_identity ||
          ownership.run_root != run_root) {
        throw McpOperationFailure(
            "run_cleanup_identity_reused",
            "run identity changed while cleanup ownership was verified", false);
      }
      if (receipt != cleanup_receipts_.end() &&
          (receipt->second.ownership != ownership ||
           receipt->second.root_identity != *initial_identity)) {
        throw McpOperationFailure(
            "run_cleanup_identity_reused",
            "run cleanup refuses a replaced or foreign run identity", false);
      }
      if (receipt == cleanup_receipts_.end() && remove_retained_artifacts) {
        RunCleanupReceipt prepared{
            .ownership = ownership,
            .root_identity = *initial_identity,
            .result = SuccessfulCleanupResult(run_id),
            .external_cleanup_complete = false,
            .complete = false,
        };
        const auto inserted =
            cleanup_receipts_.emplace(std::string(run_id), std::move(prepared));
        if (!inserted.second) {
          throw std::logic_error(
              "run cleanup receipt insertion lost controller serialization");
        }
        receipt = inserted.first;
      }

      Options cleanup_options;
      cleanup_options.output_dir = run_root.parent_path();
      cleanup_options.run_id = std::string(run_id);
      const RunOwnership* expected_ownership =
          receipt == cleanup_receipts_.end() ? &ownership
                                             : &receipt->second.ownership;
      McpRunCleanupResult result = CleanupRun(
          std::move(cleanup_options), deadline, operation_stop_token,
          remove_retained_artifacts, expected_ownership, *initial_identity,
          receipt == cleanup_receipts_.end()
              ? nullptr
              : &receipt->second.external_cleanup_complete);
      if (remove_retained_artifacts) {
        if (receipt == cleanup_receipts_.end()) {
          throw std::logic_error(
              "artifact-removing cleanup completed without a receipt");
        }
#ifdef BBP_ENABLE_TEST_HOOKS
        if (run_cleanup_root_removed_test_hook_) {
          run_cleanup_root_removed_test_hook_();
        }
#endif
        const std::filesystem::path quarantine =
            OwnedRunRootCleanupQuarantinePath(receipt->second.ownership);
        require_removed_artifacts_absent(
            quarantine,
            "a public or quarantined root appeared during verified cleanup");
        receipt->second.complete = true;
      } else {
        const std::optional<OwnedRunRootIdentity> retained_identity =
            InspectRunRootIdentity(run_root, deadline, operation_stop_token);
        if (!retained_identity || *retained_identity != *initial_identity) {
          throw McpOperationFailure(
              "run_cleanup_identity_reused",
              "retained run identity changed before ownership was rechecked",
              false);
        }
        RunOwnership retained_ownership;
        try {
          retained_ownership = LoadRunOwnership(std::string(run_id), run_root,
                                                operation_stop_token);
        } catch (...) {
          const std::string detail =
              dependencies_.exception_message(std::current_exception());
          if (stop_token.stop_requested()) {
            throw McpOperationCancelled();
          }
          if (std::chrono::steady_clock::now() >= deadline) {
            throw McpOperationFailure(
                "run_clean_timeout",
                "retained run ownership recheck exceeded the timeout: " +
                    detail,
                true);
          }
          throw McpOperationFailure(
              "run_cleanup_identity_reused",
              "retained run ownership changed before cleanup completion: " +
                  detail,
              false);
        }
        const std::optional<OwnedRunRootIdentity> final_identity =
            InspectRunRootIdentity(run_root, deadline, operation_stop_token);
        if (!final_identity || *final_identity != *initial_identity ||
            retained_ownership != ownership ||
            InspectRunRootIdentity(OwnedRunRootCleanupQuarantinePath(ownership),
                                   deadline, operation_stop_token)) {
          throw McpOperationFailure(
              "run_cleanup_identity_reused",
              "retained run identity changed before cleanup completion", false);
        }
      }
      return result;
    } catch (const McpOperationCancelled&) {
      if (stop_token.stop_requested()) {
        throw;
      }
      if (std::chrono::steady_clock::now() >= deadline) {
        throw McpOperationFailure(
            "run_clean_timeout",
            "run cleanup did not finish before the timeout", true);
      }
      throw;
    } catch (const McpOperationFailure&) {
      throw;
    } catch (const OwnedRunRootIdentityMismatch& error) {
      throw McpOperationFailure(
          "run_cleanup_identity_reused",
          "run cleanup refused a replaced root: " + std::string(error.what()),
          false);
    } catch (const CgroupOwnershipMismatch& error) {
      throw McpOperationFailure(
          "run_cleanup_unverified",
          "run cleanup refused unverified cgroup ownership: " +
              std::string(error.what()),
          false);
    } catch (...) {
      const std::string detail =
          dependencies_.exception_message(std::current_exception());
      if (stop_token.stop_requested()) {
        throw McpOperationCancelled();
      }
      if (std::chrono::steady_clock::now() >= deadline) {
        throw McpOperationFailure(
            "run_clean_timeout",
            "run cleanup did not finish before the timeout: " + detail, true);
      }
      throw McpOperationFailure("run_clean_failed",
                                "run cleanup failed: " + detail, true);
    }
  }

  std::optional<EditorRunSnapshot> CurrentRun(
      bool include_terminal, bool acquire_tui_read_lease = false) const {
    std::shared_ptr<EditorRunContext> context;
    {
      std::lock_guard<std::mutex> lock(mutex_);
      context = active_;
    }
    if (!context) {
      return std::nullopt;
    }
    std::lock_guard<std::mutex> lock(context->mutex);
    if (!include_terminal && IsTerminalEditorRunState(context->state)) {
      return std::nullopt;
    }
    return SnapshotLocked(*context, acquire_tui_read_lease);
  }

  std::uint64_t RunMembershipRevision() const noexcept {
    return run_membership_revision_.load(std::memory_order_acquire);
  }

  void RequestActiveRunStop() {
    std::shared_ptr<EditorRunContext> context;
    {
      std::lock_guard<std::mutex> lock(mutex_);
      context = active_;
    }
    if (!context) {
      return;
    }
    {
      std::lock_guard<std::mutex> lock(context->mutex);
      context->host_stop_requested = true;
    }
    context->mcp_application->MarkRunStopping();
    RequestStop(context);
  }

  int WaitForInitialRun(const std::shared_ptr<EditorRunContext>& context,
                        std::stop_token application_stop_token) const {
    std::unique_lock<std::mutex> lock(context->mutex);
    const bool completed = context->state_changed.wait(
        lock, application_stop_token,
        [&] { return IsTerminalEditorRunState(context->state); });
    if (!completed) {
      return 0;
    }
    if (context->host_stop_requested) {
      lock.unlock();
      WaitForApplicationStop(application_stop_token);
      return 0;
    }
    if (context->failure) {
      std::rethrow_exception(context->failure);
    }
    return context->result;
  }

  void Shutdown() {
    std::shared_ptr<EditorRunContext> context;
    std::lock_guard<std::timed_mutex> transition_lock(transition_mutex_);
    JoinRetiredWorkers();
    {
      std::lock_guard<std::mutex> lock(mutex_);
      if (shutdown_ && !active_) {
        return;
      }
      shutdown_ = true;
      context = active_;
    }
    if (context) {
      RequestStop(context);
    }
    bool application_shutdown_succeeded = false;
    try {
      JoinAndShutdown(context, true, &application_shutdown_succeeded);
    } catch (...) {
      if (application_shutdown_succeeded) {
        std::lock_guard<std::mutex> lock(mutex_);
        if (active_ == context) {
          active_.reset();
        }
      }
      throw;
    }
    std::lock_guard<std::mutex> lock(mutex_);
    if (active_ == context) {
      active_.reset();
    }
  }

 private:
  struct RunCleanupReceipt {
    RunOwnership ownership;
    OwnedRunRootIdentity root_identity;
    McpRunCleanupResult result;
    bool external_cleanup_complete = false;
    bool complete = false;
  };

  static Options PrepareHostedScenario(
      boost::json::object* hosted_scenario,
      const std::filesystem::path& benchmark_root) {
    const boost::json::value* simulation =
        hosted_scenario->if_contains("simulation");
    const bool has_nested_output =
        simulation != nullptr && simulation->is_object() &&
        simulation->as_object().if_contains("output_dir") != nullptr;
    if (hosted_scenario->if_contains("output_dir") == nullptr &&
        !has_nested_output) {
      (*hosted_scenario)["output_dir"] = benchmark_root.string();
    }

    Options options = ParseAndValidateScenario(*hosted_scenario);
    const std::filesystem::path requested_root =
        std::filesystem::weakly_canonical(
            std::filesystem::absolute(options.output_dir))
            .lexically_normal();
    const std::filesystem::path configured_root =
        benchmark_root.lexically_normal();
    if (requested_root != configured_root) {
      throw McpOperationFailure(
          "run_output_root_mismatch",
          "managed runs must use the editor host benchmark root", false);
    }
    options.output_dir = configured_root;
    return options;
  }

  static std::filesystem::path CleanupRunRoot(
      const std::filesystem::path& benchmark_root, std::string_view run_id) {
    std::error_code error;
    const std::filesystem::path absolute_root =
        std::filesystem::absolute(benchmark_root, error);
    if (error) {
      throw std::runtime_error("resolve editor host benchmark root failed: " +
                               error.message());
    }
    const std::filesystem::path canonical_root =
        std::filesystem::weakly_canonical(absolute_root, error);
    if (error) {
      throw std::runtime_error(
          "canonicalize editor host benchmark root failed: " + error.message());
    }
    return (canonical_root / run_id).lexically_normal();
  }

  static std::optional<OwnedRunRootIdentity> InspectRunRootIdentity(
      const std::filesystem::path& run_root,
      std::optional<std::chrono::steady_clock::time_point> deadline =
          std::nullopt,
      std::stop_token stop_token = {}) {
    const auto require_active = [&] {
      if (stop_token.stop_requested()) {
        throw std::runtime_error("retained run root inspection was cancelled");
      }
      if (deadline && std::chrono::steady_clock::now() >= *deadline) {
        throw std::runtime_error(
            "retained run root inspection deadline expired");
      }
    };
    require_active();
    const int descriptor =
        open(run_root.c_str(),
             O_RDONLY | O_DIRECTORY | O_CLOEXEC | O_NOFOLLOW | O_NONBLOCK);
    if (descriptor < 0) {
      const int open_error = errno;
      require_active();
      if (open_error == ENOENT) {
        return std::nullopt;
      }
      throw std::runtime_error(
          "open retained run root without following links failed: " +
          std::error_code(open_error, std::generic_category()).message());
    }

    struct stat opened{};
    const int inspect_result = fstat(descriptor, &opened);
    const int inspect_error = inspect_result == 0 ? 0 : errno;
    struct stat linked{};
    const int link_result =
        inspect_result == 0
            ? fstatat(AT_FDCWD, run_root.c_str(), &linked, AT_SYMLINK_NOFOLLOW)
            : -1;
    const int link_error = link_result == 0 ? 0 : errno;
    const int close_result = close(descriptor);
    const int close_error = close_result == 0 ? 0 : errno;
    require_active();
    if (inspect_result != 0) {
      throw std::runtime_error(
          "inspect retained run root failed: " +
          std::error_code(inspect_error, std::generic_category()).message());
    }
    if (link_result != 0) {
      throw std::runtime_error(
          "reinspect retained run root link failed: " +
          std::error_code(link_error, std::generic_category()).message());
    }
    if (close_result != 0) {
      throw std::runtime_error(
          "close retained run root failed: " +
          std::error_code(close_error, std::generic_category()).message());
    }
    if (!S_ISDIR(linked.st_mode) || opened.st_dev != linked.st_dev ||
        opened.st_ino != linked.st_ino) {
      throw std::runtime_error(
          "retained run root identity changed during no-follow inspection");
    }
    require_active();
    return OwnedRunRootIdentity{
        .device = static_cast<std::uintmax_t>(opened.st_dev),
        .inode = static_cast<std::uintmax_t>(opened.st_ino),
    };
  }

  static McpRunCleanupResult SuccessfulCleanupResult(std::string_view run_id) {
    return McpRunCleanupResult{
        .run_id = std::string(run_id),
        .verified_owned = true,
        .processes_remaining = 0U,
        .network_resources_remaining = 0U,
        .cgroups_remaining = 0U,
        .credentials_remaining = 0U,
        .complete = true,
    };
  }

  static EditorRunSnapshot SnapshotLocked(const EditorRunContext& context,
                                          bool acquire_tui_read_lease = false) {
    const RuntimeNodeSnapshot nodes = context.node_inventory->Snapshot();
    const std::uint32_t node_count = static_cast<std::uint32_t>(nodes.size());
    const std::uint32_t node_capacity = nodes.capacity();
    return EditorRunSnapshot{
        .generation = context.generation,
        .run_id = context.options->run_id,
        .run_root = BenchmarkRunRoot(*context.options),
        .chain = std::string(ChainKindName(context.options->chain)),
        .node_count = node_count,
        .node_capacity = node_capacity,
        .network_allocation =
            nodes.network_address_plan()
                ? boost::json::value(
                      nodes.network_address_plan()->ToSerialized())
                : boost::json::value(nullptr),
        .available_node_capacity =
            node_count <= node_capacity ? node_capacity - node_count : 0U,
        .state = context.state,
        .command_queue = context.command_queue,
#ifdef BBP_FIRO_GUI_LAUNCHER
        .operator_connection_launcher = context.operator_connection_launcher,
#endif
        .mcp_application = context.mcp_application,
        .tui_read_lease = acquire_tui_read_lease
                              ? std::make_shared<EditorTuiReadLease>(
                                    context.tui_read_lease_state)
                              : std::shared_ptr<void>{}};
  }

  static void RequestStop(const std::shared_ptr<EditorRunContext>& context) {
    RecordRunStop(context->run_stop_tick, std::chrono::steady_clock::now());
    context->simulation_stop_source.request_stop();
    context->command_queue->Close();
  }

  static void RequestHostedStop(
      const std::shared_ptr<EditorRunContext>& context) {
    {
      std::lock_guard<std::mutex> lock(context->mutex);
      context->host_stop_requested = true;
      if (!IsTerminalEditorRunState(context->state)) {
        context->state = EditorRunState::kStopping;
        context->state_changed.notify_all();
      }
    }
    RequestStop(context);
  }

  static void WaitForApplicationStop(std::stop_token stop_token) {
    std::condition_variable_any stopped;
    std::mutex mutex;
    std::unique_lock<std::mutex> lock(mutex);
    static_cast<void>(stopped.wait(lock, stop_token, [] { return false; }));
  }

  static void WaitForTuiReadLeaseDrain(
      const std::shared_ptr<EditorRunContext>& context,
      std::optional<std::chrono::steady_clock::time_point> deadline,
      std::stop_token stop_token) {
    const std::shared_ptr<EditorTuiReadLeaseState> state =
        context->tui_read_lease_state;
    std::unique_lock<std::mutex> lock(state->mutex);
    const auto drained = [&] { return state->readers == 0U; };
    if (!deadline) {
      if (!state->drained.wait(lock, stop_token, drained)) {
        throw McpOperationCancelled();
      }
      return;
    }
    if (!state->drained.wait_until(lock, stop_token, *deadline, drained)) {
      if (stop_token.stop_requested()) {
        throw McpOperationCancelled();
      }
      throw std::runtime_error(
          "TUI did not release the terminal run before the cleanup deadline");
    }
  }

  void JoinAndShutdown(
      const std::shared_ptr<EditorRunContext>& context,
      bool propagate_worker_failure = true,
      bool* application_shutdown_succeeded = nullptr,
      std::optional<std::chrono::steady_clock::time_point> deadline =
          std::nullopt,
      std::stop_token stop_token = {},
      std::vector<std::jthread>* retired_workers = nullptr) const {
    if (application_shutdown_succeeded != nullptr) {
      *application_shutdown_succeeded = false;
    }
    if (!context) {
      if (application_shutdown_succeeded != nullptr) {
        *application_shutdown_succeeded = true;
      }
      return;
    }
    if (stop_token.stop_requested()) {
      throw McpOperationCancelled();
    }
    if (deadline && std::chrono::steady_clock::now() >= *deadline) {
      throw std::runtime_error(
          "managed run reaping did not start before the deadline");
    }
    if (context->worker.joinable()) {
      if (deadline || stop_token.stop_possible()) {
        {
          std::lock_guard<std::mutex> lock(context->mutex);
          if (!IsTerminalEditorRunState(context->state)) {
            throw std::logic_error(
                "cancelable managed run reaping requires terminal state");
          }
        }
        if (retired_workers == nullptr) {
          throw std::logic_error(
              "cancelable managed run reaping requires retired-worker "
              "storage");
        }
        retired_workers->push_back(std::move(context->worker));
      } else {
        context->worker.join();
      }
    }
    if (stop_token.stop_requested()) {
      throw McpOperationCancelled();
    }
    if (deadline && std::chrono::steady_clock::now() >= *deadline) {
      throw std::runtime_error(
          "managed run worker did not join before the deadline");
    }
    std::exception_ptr worker_failure;
    {
      std::lock_guard<std::mutex> lock(context->mutex);
      worker_failure = context->failure;
    }
    std::exception_ptr shutdown_failure;
    try {
      if (deadline) {
        context->mcp_application->Shutdown(*deadline, stop_token);
      } else if (stop_token.stop_possible()) {
        context->mcp_application->Shutdown(stop_token);
      } else {
        context->mcp_application->Shutdown();
      }
    } catch (...) {
      shutdown_failure = std::current_exception();
    }
    if (!shutdown_failure && application_shutdown_succeeded != nullptr) {
      *application_shutdown_succeeded = true;
    }
    if (propagate_worker_failure && worker_failure && shutdown_failure) {
      throw std::runtime_error(
          "managed run failed: " +
          dependencies_.exception_message(worker_failure) +
          "; MCP application shutdown also failed: " +
          dependencies_.exception_message(shutdown_failure));
    }
    if (propagate_worker_failure && worker_failure) {
      std::rethrow_exception(worker_failure);
    }
    if (shutdown_failure) {
      std::rethrow_exception(shutdown_failure);
    }
  }

  void JoinRetiredWorkers() {
    for (std::jthread& worker : retired_workers_) {
      if (worker.joinable()) {
        worker.join();
      }
    }
    retired_workers_.clear();
  }

  void ReapTerminalRun(std::optional<std::chrono::steady_clock::time_point>
                           deadline = std::nullopt,
                       std::stop_token stop_token = {}) {
    std::shared_ptr<EditorRunContext> context;
    {
      std::lock_guard<std::mutex> lock(mutex_);
      if (!active_) {
        return;
      }
      {
        std::lock_guard<std::mutex> run_lock(active_->mutex);
        if (!IsTerminalEditorRunState(active_->state)) {
          return;
        }
      }
      context = active_;
    }
    WaitForTuiReadLeaseDrain(context, deadline, stop_token);
    JoinAndShutdown(
        context, false, nullptr, deadline, stop_token,
        deadline || stop_token.stop_possible() ? &retired_workers_ : nullptr);
    std::lock_guard<std::mutex> lock(mutex_);
    if (active_ == context) {
      active_.reset();
    }
  }

  std::unique_lock<std::timed_mutex> AcquireTransitionLock(
      std::stop_token stop_token) {
    std::unique_lock<std::timed_mutex> transition_lock(transition_mutex_,
                                                       std::defer_lock);
    while (!transition_lock.try_lock_for(std::chrono::milliseconds(25))) {
      if (stop_token.stop_requested()) {
        throw McpOperationCancelled();
      }
    }
    if (stop_token.stop_requested()) {
      throw McpOperationCancelled();
    }
    return transition_lock;
  }

  std::shared_ptr<EditorRunContext> Launch(
      Options options, std::optional<boost::json::object> source_scenario,
      std::stop_token stop_token) {
    std::unique_lock<std::timed_mutex> transition_lock =
        AcquireTransitionLock(stop_token);
    return LaunchLocked(std::move(options), std::move(source_scenario),
                        stop_token);
  }

  std::shared_ptr<EditorRunContext> LaunchLocked(
      Options options, std::optional<boost::json::object> source_scenario,
      std::stop_token stop_token, bool reserve_replay_destination = false) {
    dependencies_.require_safe_output_directory(options.output_dir);
    if (stop_token.stop_requested()) {
      throw McpOperationCancelled();
    }
    JoinRetiredWorkers();
    ReapTerminalRun(std::nullopt, stop_token);
    std::function<void(McpEvidenceRecord)> publish_evidence;
    std::function<void(std::string_view)> close_run_subscriptions;
    {
      std::lock_guard<std::mutex> lock(mutex_);
      if (shutdown_) {
        throw McpOperationFailure(
            "application_stopping",
            "BBP is shutting down and cannot launch another run", false);
      }
      if (active_) {
        throw McpOperationFailure(
            "run_already_active",
            "a managed run is already starting, active, or stopping", true);
      }
      publish_evidence = publish_evidence_;
      close_run_subscriptions = close_run_subscriptions_;
    }

    const std::filesystem::path cleanup_run_root =
        CleanupRunRoot(options.output_dir, options.run_id);
    const std::optional<OwnedRunRootCleanupReceipt> durable_receipt =
        TryLoadOwnedRunRootCleanupReceipt(options.run_id, cleanup_run_root,
                                          std::nullopt, stop_token);
    auto receipt = cleanup_receipts_.find(options.run_id);
    bool retire_in_memory_receipt = false;
    if (durable_receipt) {
      if (receipt != cleanup_receipts_.end() &&
          (receipt->second.ownership != durable_receipt->ownership ||
           receipt->second.root_identity != durable_receipt->root_identity)) {
        throw McpOperationFailure(
            "run_cleanup_identity_reused",
            "durable cleanup receipt disagrees with controller state", false);
      }
      const std::filesystem::path quarantine =
          OwnedRunRootCleanupQuarantinePath(durable_receipt->ownership);
      if (InspectRunRootIdentity(cleanup_run_root, std::nullopt, stop_token) ||
          InspectRunRootIdentity(quarantine, std::nullopt, stop_token)) {
        throw McpOperationFailure(
            "run_cleanup_state_uncertain",
            "an incomplete durable cleanup prevents reuse of this run id",
            false);
      }
      if (receipt != cleanup_receipts_.end()) {
        retire_in_memory_receipt = true;
      }
      if (stop_token.stop_requested()) {
        throw McpOperationCancelled();
      }
    } else if (receipt != cleanup_receipts_.end()) {
      if (InspectRunRootIdentity(cleanup_run_root, std::nullopt, stop_token) ||
          InspectRunRootIdentity(
              OwnedRunRootCleanupQuarantinePath(receipt->second.ownership),
              std::nullopt, stop_token)) {
        throw McpOperationFailure(
            "run_cleanup_identity_reused",
            "a retained run identity must not be replaced by run.launch",
            false);
      }
      if (!receipt->second.complete) {
        throw McpOperationFailure(
            "run_cleanup_state_uncertain",
            "an incomplete cleanup receipt prevents reuse of this run id",
            false);
      }
      retire_in_memory_receipt = true;
    }

    std::shared_ptr<ReservedManagedRunRoot> reserved_run_root;
    if (reserve_replay_destination) {
      reserved_run_root = ReserveManagedReplayRunRoot(options);
    }

    auto context = std::make_shared<EditorRunContext>();
    context->generation = next_generation_++;
    context->options = std::make_shared<Options>(std::move(options));
    context->source_scenario = std::move(source_scenario);
    context->command_queue = std::make_shared<SimulationCommandQueue>();
    context->node_inventory =
        std::make_shared<RuntimeNodeInventory>(context->options->node_capacity);
    context->reserved_run_root = std::move(reserved_run_root);
    if (durable_receipt || retire_in_memory_receipt) {
      const std::string reused_run_id = context->options->run_id;
      context->run_root_prepared =
          [this, durable_receipt, retire_in_memory_receipt,
           reused_run_id](std::stop_token preparation_stop_token) {
            if (durable_receipt) {
              RemoveOwnedRunRootCleanupReceipt(*durable_receipt, std::nullopt,
                                               preparation_stop_token);
            }
            if (retire_in_memory_receipt) {
              cleanup_receipts_.erase(reused_run_id);
            }
          };
    }
    const std::weak_ptr<EditorRunContext> weak_context(context);
#ifdef BBP_FIRO_GUI_LAUNCHER
    std::unique_ptr<ChainDriver> launcher_driver =
        CreateChainDriver(context->options->chain);
    context->operator_connection_launcher =
        launcher_driver->CreateOperatorConnectionLauncher(
            [weak_context](std::string_view node_id,
                           std::stop_token stop_token) {
              ThrowIfStopRequested(stop_token);
              const std::shared_ptr<EditorRunContext> run = weak_context.lock();
              if (!run || !run->node_inventory || !run->options) {
                throw std::runtime_error(
                    "managed run operator launcher authority is unavailable");
              }

              const RuntimeNodeSnapshot snapshot =
                  run->node_inventory->Snapshot();
              const auto selected =
                  std::find_if(snapshot.begin(), snapshot.end(),
                               [node_id](const NodeRuntime& node) {
                                 return node.config.id == node_id;
                               });
              if (selected == snapshot.end()) {
                throw std::runtime_error(
                    "operator launcher references an unknown active node: " +
                    std::string(node_id));
              }

              ChainNodeConfig config;
              {
                auto process_guard = LockNodeProcessState(*selected);
                RequireNodeRunning(*selected, process_guard,
                                   "operator launcher");
                config = selected->config;
              }
              ThrowIfStopRequested(stop_token);

              std::unique_ptr<ChainDriver> driver =
                  CreateChainDriver(run->options->chain);
              std::optional<OperatorConnectionCommand> command =
                  driver->BuildOperatorConnectionCommand(
                      config, BenchmarkRunRoot(*run->options));
              if (!command) {
                throw std::runtime_error(
                    "the active chain has no operator launcher command");
              }
              ThrowIfStopRequested(stop_token);
              return OperatorConnectionLauncherAuthority{
                  .inventory_generation = snapshot.generation(),
                  .node_id = config.id,
                  .command = std::move(*command),
              };
            });
#endif
    context->mcp_application = std::make_shared<
        McpLiveApplication>(McpLiveApplication::Config{
        .run_id = context->options->run_id,
        .run_root = BenchmarkRunRoot(*context->options),
        .retained_run = std::nullopt,
        .options = context->options,
        .command_queue = context->command_queue,
#ifdef BBP_FIRO_GUI_LAUNCHER
        .operator_connection_launcher = context->operator_connection_launcher,
#endif
        .node_inventory_snapshot =
            [weak_context] {
              const std::shared_ptr<EditorRunContext> run = weak_context.lock();
              if (!run || !run->node_inventory) {
                throw std::runtime_error(
                    "managed run node inventory is unavailable");
              }
              const RuntimeNodeSnapshot snapshot =
                  run->node_inventory->Snapshot();
              McpLiveNodeInventorySnapshot result{
                  .generation = snapshot.generation(),
                  .node_ids = {},
                  .node_capacity = snapshot.capacity(),
                  .network_allocation =
                      snapshot.network_address_plan()
                          ? boost::json::value(
                                snapshot.network_address_plan()->ToSerialized())
                          : boost::json::value(nullptr)};
              result.node_ids.reserve(snapshot.size());
              for (const NodeRuntime& node : snapshot) {
                result.node_ids.push_back(node.config.id);
              }
              return result;
            },
        .publication_mutex = dependencies_.runtime_publication_mutex(),
        .request_run_stop =
            [weak_context] {
              if (const std::shared_ptr<EditorRunContext> run =
                      weak_context.lock()) {
                RequestHostedStop(run);
              }
            },
        .run_started =
            [weak_context] {
              if (const std::shared_ptr<EditorRunContext> run =
                      weak_context.lock()) {
                std::lock_guard<std::mutex> lock(run->mutex);
                if (run->state == EditorRunState::kStarting) {
                  run->reached_active = true;
                  run->state = EditorRunState::kActive;
                  run->state_changed.notify_all();
                }
              }
            },
        .run_stopping =
            [weak_context] {
              if (const std::shared_ptr<EditorRunContext> run =
                      weak_context.lock()) {
                std::lock_guard<std::mutex> lock(run->mutex);
                if (!IsTerminalEditorRunState(run->state)) {
                  run->state = EditorRunState::kStopping;
                  run->state_changed.notify_all();
                }
              }
            },
        .run_stopped = {},
        .publish_evidence = std::move(publish_evidence),
        .close_run_subscriptions = std::move(close_run_subscriptions)});
    {
      std::lock_guard<std::mutex> lock(mutex_);
      if (active_) {
        throw std::logic_error(
            "managed run membership changed during serialized launch");
      }
      active_ = context;
      run_membership_revision_.fetch_add(1U, std::memory_order_release);
    }
    try {
      context->worker = std::jthread([this, context] {
        const auto finalize_lifecycle = [&context]() -> std::exception_ptr {
          try {
            context->mcp_application->MarkRunStopping();
            context->mcp_application->MarkRunStopped();
            return {};
          } catch (...) {
            return std::current_exception();
          }
        };
        const auto publish_terminal_summary =
            [&context](std::string_view state) -> std::exception_ptr {
          if (!context->retained_run_root_available) {
            return {};
          }
          try {
            const RuntimeNodeSnapshot nodes =
                context->node_inventory->Snapshot();
            if (nodes.size() > std::numeric_limits<std::uint32_t>::max()) {
              throw std::overflow_error(
                  "terminal retained run node count exceeds uint32");
            }
            Options final_options = *context->options;
            final_options.node_capacity = nodes.capacity();
            final_options.network_address_plan = nodes.network_address_plan();
            WriteRetainedRunRegistrySummary(
                final_options, state, static_cast<std::uint32_t>(nodes.size()));
            return {};
          } catch (...) {
            return std::current_exception();
          }
        };
        const auto log_summary_failure =
            [this](std::exception_ptr failure) {
              if (failure) {
                BBP_LOG(error)
                    << "retained run registry summary publication failed: "
                    << dependencies_.exception_message(failure);
              }
            };
        const auto append_failure = [this](std::exception_ptr primary,
                                           std::exception_ptr additional,
                                           std::string_view context) {
          if (!additional) {
            return primary;
          }
          if (!primary) {
            return additional;
          }
          return std::make_exception_ptr(
              std::runtime_error(dependencies_.exception_message(primary) +
                                 "; " + std::string(context) + ": " +
                                 dependencies_.exception_message(additional)));
        };
        try {
          const BenchmarkHeadlessResult completion =
              RunPreparedBenchmark(context, dependencies_);
          context->result = completion.result;
          const std::exception_ptr summary_failure = publish_terminal_summary(
              completion.terminal_outcome ==
                      BenchmarkTerminalOutcome::kCancelled
                  ? "cancelled"
                  : "finished");
          log_summary_failure(summary_failure);
          std::lock_guard<std::mutex> lock(context->mutex);
          if (!IsTerminalEditorRunState(context->state)) {
            context->state = EditorRunState::kStopped;
            run_membership_revision_.fetch_add(1U, std::memory_order_release);
          }
          context->state_changed.notify_all();
        } catch (const SimulationCancelled&) {
          std::exception_ptr failure = finalize_lifecycle();
          const std::exception_ptr summary_failure =
              publish_terminal_summary(failure ? "failed" : "cancelled");
          if (failure) {
            failure = append_failure(
                failure, summary_failure,
                "retained run registry summary publication failed");
          } else {
            log_summary_failure(summary_failure);
          }
          std::lock_guard<std::mutex> lock(context->mutex);
          const bool membership_was_visible =
              !IsTerminalEditorRunState(context->state);
          if (failure) {
            context->failure = failure;
            context->state = EditorRunState::kFailed;
          } else {
            context->result = 0;
            context->state = EditorRunState::kStopped;
          }
          if (membership_was_visible) {
            run_membership_revision_.fetch_add(1U, std::memory_order_release);
          }
          context->state_changed.notify_all();
        } catch (...) {
          std::exception_ptr failure = std::current_exception();
          failure = append_failure(failure, finalize_lifecycle(),
                                   "terminal launcher cleanup also failed");
          failure = append_failure(
              failure, publish_terminal_summary("failed"),
              "retained run registry summary publication failed");
          std::lock_guard<std::mutex> lock(context->mutex);
          const bool membership_was_visible =
              !IsTerminalEditorRunState(context->state);
          context->failure = failure;
          context->state = EditorRunState::kFailed;
          if (membership_was_visible) {
            run_membership_revision_.fetch_add(1U, std::memory_order_release);
          }
          context->state_changed.notify_all();
        }
      });
    } catch (...) {
      std::lock_guard<std::mutex> lock(mutex_);
      if (active_ == context) {
        active_.reset();
        run_membership_revision_.fetch_add(1U, std::memory_order_release);
      }
      throw;
    }
    return context;
  }

#ifdef BBP_ENABLE_TEST_HOOKS
  const std::function<void()>& run_cleanup_root_removed_test_hook_;
#endif
  const EditorApplicationDependencies dependencies_;
  mutable std::timed_mutex transition_mutex_;
  mutable std::mutex mutex_;
  std::vector<std::jthread> retired_workers_;
  std::map<std::string, RunCleanupReceipt, std::less<>> cleanup_receipts_;
  std::shared_ptr<EditorRunContext> active_;
  std::function<void(McpEvidenceRecord)> publish_evidence_;
  std::function<void(std::string_view)> close_run_subscriptions_;
  std::uint64_t next_generation_ = 1U;
  std::atomic<std::uint64_t> run_membership_revision_{0U};
  bool shutdown_ = false;
};

std::optional<McpHostedRunSnapshot> McpSnapshot(
    const EditorRunController& controller) {
  const std::optional<EditorRunSnapshot> snapshot =
      controller.CurrentRun(false);
  if (!snapshot) {
    return std::nullopt;
  }
  return McpHostedRunSnapshot{
      .generation = snapshot->generation,
      .run_id = snapshot->run_id,
      .state = std::string(EditorRunStateName(snapshot->state)),
      .chain = snapshot->chain,
      .node_count = snapshot->node_count,
      .node_capacity = snapshot->node_capacity,
      .available_node_capacity = snapshot->available_node_capacity,
      .application = snapshot->mcp_application,
      .network_allocation = snapshot->network_allocation,
  };
}

TuiRunSnapshot TuiSnapshot(
    const EditorRunController& controller,
    std::shared_ptr<std::timed_mutex> (*runtime_publication_mutex)()) {
  const std::optional<EditorRunSnapshot> snapshot =
      controller.CurrentRun(false, true);
  if (!snapshot || snapshot->state == EditorRunState::kStarting) {
    return {};
  }
  return TuiRunSnapshot{
      .generation = snapshot->generation,
      .run_root = snapshot->run_root,
      .command_queue = snapshot->command_queue,
#ifdef BBP_FIRO_GUI_LAUNCHER
      .operator_connection_launcher = snapshot->operator_connection_launcher,
#endif
      .publication_mutex = runtime_publication_mutex(),
      .read_lease = snapshot->tui_read_lease,
  };
}

void WaitForApplicationStop(std::stop_token stop_token) {
  std::condition_variable_any stopped;
  std::mutex mutex;
  std::unique_lock<std::mutex> lock(mutex);
  static_cast<void>(stopped.wait(lock, stop_token, [] { return false; }));
}

}  // namespace

int RunEditorApplication(
    Options options, const std::filesystem::path& state_directory,
#ifdef BBP_ENABLE_TEST_HOOKS
    const std::function<void()>& run_cleanup_root_removed_test_hook,
#endif
    EditorApplicationDependencies dependencies) {
  SignalStopMonitor signal_monitor;
  std::stop_source application_stop_source;
  EnsureDirectory(options.output_dir);
  const std::filesystem::path benchmark_root =
      std::filesystem::canonical(options.output_dir);
  dependencies.require_safe_output_directory(benchmark_root);
  options.output_dir = benchmark_root;
  EditorRunController run_controller(
#ifdef BBP_ENABLE_TEST_HOOKS
      run_cleanup_root_removed_test_hook,
#endif
      dependencies);
  McpHostApplication host_application(McpHostApplication::Config{
      .host_id = options.run_id,
      .snapshot_run = [&] { return McpSnapshot(run_controller); },
      .snapshot_run_membership_revision =
          [&] { return run_controller.RunMembershipRevision(); },
      .snapshot_retained_runs =
          [benchmark_root](std::string_view active_run_id,
                           std::stop_token stop_token) {
            return DiscoverRetainedRuns(benchmark_root, active_run_id,
                                        stop_token);
          },
      .launch_run =
          [&](const boost::json::object& scenario, std::stop_token stop_token) {
            const std::shared_ptr<EditorRunContext> run =
                run_controller.LaunchScenario(scenario, benchmark_root,
                                              stop_token);
            const EditorRunSnapshot snapshot =
                run_controller.WaitUntilActive(run, stop_token);
            return McpRunLifecycleResult{
                .run_id = snapshot.run_id,
                .state = std::string(EditorRunStateName(snapshot.state)),
                .node_count = snapshot.node_count,
            };
          },
      .replay_run =
          [&](std::string_view source_run_id,
              std::optional<std::string> destination_run_id,
              std::stop_token stop_token) {
            const std::shared_ptr<EditorRunContext> run =
                run_controller.ReplayScenario(source_run_id,
                                              std::move(destination_run_id),
                                              benchmark_root, stop_token);
            const EditorRunSnapshot snapshot =
                run_controller.WaitUntilActive(run, stop_token);
            return McpRunLifecycleResult{
                .run_id = snapshot.run_id,
                .state = std::string(EditorRunStateName(snapshot.state)),
                .node_count = snapshot.node_count,
            };
          },
      .stop_run =
          [&](std::string_view run_id, std::chrono::seconds timeout,
              std::stop_token stop_token) {
            const EditorRunSnapshot snapshot =
                run_controller.StopRun(run_id, timeout, stop_token);
            return McpRunLifecycleResult{
                .run_id = snapshot.run_id,
                .state = std::string(EditorRunStateName(snapshot.state)),
                .node_count = snapshot.node_count,
            };
          },
      .clean_run =
          [&, benchmark_root](
              std::string_view run_id, std::chrono::seconds timeout,
              bool remove_retained_artifacts, std::stop_token stop_token) {
            return run_controller.CleanRun(benchmark_root, run_id, timeout,
                                           remove_retained_artifacts,
                                           stop_token);
          },
  });
  McpEndpoint mcp_endpoint(
      McpEndpointConfig{
          .state_directory = state_directory,
          .run_id = options.run_id,
          .server = {},
          .dispatcher = {},
          .allowed_operations = host_application.SupportedOperations(),
          .allowed_information_families =
              host_application.SupportedInformationFamilies(),
          .read_only = false,
      },
      host_application.OperationFactory(), host_application.ResourceReader());
  run_controller.SetEvidenceCallbacks(
      [&mcp_endpoint](McpEvidenceRecord record) {
        mcp_endpoint.PublishEvidence(std::move(record));
      },
      [&mcp_endpoint](std::string_view run_id) {
        mcp_endpoint.CloseRunSubscriptions(run_id);
      });
  std::stop_callback stop_on_signal(signal_monitor.GetToken(), [&] {
    application_stop_source.request_stop();
  });

  int result = 1;
  std::exception_ptr application_failure;
  try {
    mcp_endpoint.Start();
    const McpEndpointPublication publication = mcp_endpoint.publication();
    BBP_LOG(info) << "MCP endpoint listening at " << publication.endpoint
                  << "; client_config="
                  << publication.client_config_file.string();
    const TuiMcpConnectionInfo mcp_connection{
        .endpoint = publication.endpoint,
        .token_file = publication.token_file,
        .client_config_file = publication.client_config_file,
    };

    std::shared_ptr<EditorRunContext> initial_run;
    if (options.initial_run_requested) {
      initial_run = run_controller.LaunchOptions(options);
    }
    if (options.no_tui) {
      if (initial_run) {
        result = run_controller.WaitForInitialRun(
            initial_run, application_stop_source.get_token());
      } else {
        WaitForApplicationStop(application_stop_source.get_token());
        result = 0;
      }
    } else {
      SetConsoleLoggingEnabled(false);
      result = RunTuiReport(
          [&] {
            return TuiSnapshot(run_controller,
                               dependencies.runtime_publication_mutex);
          },
          false, options.tui_refresh_ms, mcp_connection,
          application_stop_source.get_token());
      SetConsoleLoggingEnabled(true);
    }
  } catch (...) {
    SetConsoleLoggingEnabled(true);
    application_failure = std::current_exception();
  }

  std::exception_ptr cleanup_failure;
  const auto capture_cleanup_failure = [&](auto&& action) {
    try {
      action();
    } catch (...) {
      if (!cleanup_failure) {
        cleanup_failure = std::current_exception();
      }
    }
  };
  bool endpoint_drained = false;
  capture_cleanup_failure([&] {
    mcp_endpoint.StopAdmissionAndDrain();
    endpoint_drained = true;
  });
  if (endpoint_drained) {
    capture_cleanup_failure([&] { host_application.Shutdown(); });
    capture_cleanup_failure([&] { run_controller.Shutdown(); });
    capture_cleanup_failure([&] { mcp_endpoint.Stop(); });
  }

  if (application_failure) {
    std::rethrow_exception(application_failure);
  }
  if (cleanup_failure) {
    std::rethrow_exception(cleanup_failure);
  }
  if (signal_monitor.ReceivedSignal() != 0) {
    BBP_LOG(info) << "graceful shutdown completed after signal "
                  << signal_monitor.ReceivedSignal();
  }
  return result;
}

#ifdef BBP_ENABLE_TEST_HOOKS
McpRunCleanupResult CleanEditorRetainedRunForTest(
    const std::filesystem::path& benchmark_root, std::string_view run_id,
    std::chrono::seconds timeout, bool remove_retained_artifacts,
    std::stop_token stop_token,
    const std::function<void()>& run_cleanup_root_removed_test_hook,
    EditorApplicationDependencies dependencies) {
  EditorRunController controller(run_cleanup_root_removed_test_hook,
                                 dependencies);
  return controller.CleanRun(benchmark_root, run_id, timeout,
                             remove_retained_artifacts, stop_token);
}
#endif

}  // namespace bbp::simulator_app_internal
