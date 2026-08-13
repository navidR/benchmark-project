#pragma once

#include <chrono>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <functional>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <stop_token>
#include <string>
#include <string_view>
#include <vector>

#include "bbp/mcp_dispatcher.h"
#include "bbp/simulation_command.h"

namespace bbp {

#ifdef BBP_FIRO_GUI_LAUNCHER
class OperatorConnectionLauncher;
#endif
class SimulationCommandQueue;
struct Options;

struct McpLiveNodeInventorySnapshot {
  std::uint64_t generation = 0U;
  std::vector<std::string> node_ids;
};

struct McpLiveWorkloadControl;

struct McpLiveWorkloadDrainResult {
  bool admission_closed = false;
  bool cancellation_requested = false;
  bool drained = false;
  bool timed_out = false;
  std::size_t active_callback_count = 0U;
  std::size_t active_worker_count = 0U;
  std::chrono::steady_clock::time_point first_deadline{};

  bool safe_to_destroy() const noexcept;
};

class McpLiveWorkloadWorkerLease {
 public:
  McpLiveWorkloadWorkerLease() noexcept = default;
  ~McpLiveWorkloadWorkerLease();

  McpLiveWorkloadWorkerLease(const McpLiveWorkloadWorkerLease&) = delete;
  McpLiveWorkloadWorkerLease& operator=(const McpLiveWorkloadWorkerLease&) =
      delete;
  McpLiveWorkloadWorkerLease(McpLiveWorkloadWorkerLease&& other) noexcept;
  McpLiveWorkloadWorkerLease& operator=(
      McpLiveWorkloadWorkerLease&& other) noexcept;

  explicit operator bool() const noexcept;
  std::stop_token stop_token() const noexcept;

 private:
  friend struct McpLiveWorkloadService;

  explicit McpLiveWorkloadWorkerLease(
      std::shared_ptr<McpLiveWorkloadControl> control) noexcept;
  void Release() noexcept;

  std::shared_ptr<McpLiveWorkloadControl> control_;
};

// Simulator-owned workload service. MCP supplies typed arguments and
// cancellation; the service owns validation and execution shared with the
// scenario/TUI paths. Lifecycle operations additionally own retained workload
// identity, state, and accounting; one-shot invocation owns only its terminal
// invocation identity, returns its complete typed result, and remains
// unregistered.
struct McpLiveWorkloadService {
  McpLiveWorkloadService();
  ~McpLiveWorkloadService();

  McpLiveWorkloadService(const McpLiveWorkloadService&) = delete;
  McpLiveWorkloadService& operator=(const McpLiveWorkloadService&) = delete;

  std::function<boost::json::object(
      McpOperationKind, const boost::json::object&, std::stop_token)>
      operation;
  std::function<boost::json::value(bool history, std::stop_token)> read;

  boost::json::object ExecuteOperation(McpOperationKind kind,
                                       const boost::json::object& arguments,
                                       std::stop_token stop_token);
  boost::json::value Read(bool history, std::stop_token stop_token);
  McpLiveWorkloadWorkerLease AcquireWorkerLease();
  McpLiveWorkloadDrainResult WaitUntilDrained(
      std::optional<std::chrono::steady_clock::time_point> deadline =
          std::nullopt,
      std::stop_token stop_token = {});

 private:
  friend class McpLiveApplication;

  McpLiveWorkloadDrainResult CloseAdmission(
      std::chrono::steady_clock::time_point deadline);
  McpLiveWorkloadDrainResult RequestCancellation();
  McpLiveWorkloadDrainResult DrainResult();

  std::shared_ptr<McpLiveWorkloadControl> control_;
};

// Simulator-owned instrumentation lifecycle service. MCP supplies typed
// arguments and cancellation; the service owns measurement windows, stable
// identities, sampling, evidence, and teardown shared with run reporting.
struct McpLiveInstrumentationService {
  std::function<boost::json::object(
      McpOperationKind, const boost::json::object&, std::stop_token)>
      operation;
  std::function<boost::json::value(McpInformationFamily, std::stop_token)> read;
};

// Simulator-owned role mutation service. MCP supplies typed arguments and
// cancellation; the simulator owns driver calls, transactional publication,
// evidence, and shared runtime state.
struct McpLiveRoleService {
  std::function<boost::json::object(
      McpOperationKind, const boost::json::object&, std::stop_token)>
      operation;
};

// Translates one shared simulation command into exactly one authoritative
// role-service operation and returns the generic role.assign/role.remove
// result after the same normalization and secret redaction used by MCP.
boost::json::object ExecuteAndNormalizeSimulationRoleMutation(
    const McpLiveRoleService& service, std::string_view run_id,
    SimulationCommandKind kind, const SimulationRoleMutationRequest& request,
    std::stop_token stop_token);

// Lifetime bridge between the embedded MCP endpoint and one simulator run.
// Config references remain valid until the endpoint has stopped all request
// and operation workers. mutex_ owns admission, shutdown, and MCP command
// outcome publication; report_mutex_ serializes production report snapshots.
class McpLiveApplication {
 public:
  struct RetainedRun {
    std::string chain;
    std::uint32_t node_count = 0U;
    std::string state;
    bool has_owned_artifacts = false;
  };

  struct Config {
    std::string run_id;
    std::filesystem::path run_root;
    std::optional<RetainedRun> retained_run;
    std::shared_ptr<const Options> options;
    std::shared_ptr<SimulationCommandQueue> command_queue;
#ifdef BBP_FIRO_GUI_LAUNCHER
    std::shared_ptr<OperatorConnectionLauncher> operator_connection_launcher =
        {};
#endif
    std::function<McpLiveNodeInventorySnapshot()> node_inventory_snapshot = {};
    std::shared_ptr<std::timed_mutex> publication_mutex;
    std::function<void()> request_run_stop;
    std::function<void()> run_started;
    std::function<void()> run_stopping;
    std::function<void()> run_stopped;
    std::function<void(McpEvidenceRecord)> publish_evidence = {};
    std::function<void(std::string_view)> close_run_subscriptions = {};
#ifdef BBP_ENABLE_TEST_HOOKS
    std::function<void()> request_admitted_test_hook = {};
#endif
  };

  explicit McpLiveApplication(Config config);
  ~McpLiveApplication();

  McpLiveApplication(const McpLiveApplication&) = delete;
  McpLiveApplication& operator=(const McpLiveApplication&) = delete;

  McpApplicationOperationFactory OperationFactory();
  McpApplicationResourceReader ResourceReader();
  std::vector<McpOperationKind> SupportedOperations() const;
  std::vector<McpInformationFamily> SupportedInformationFamilies() const;
  bool read_only() const;
  std::uint32_t current_node_count() const;
  void SetWorkloadService(std::shared_ptr<McpLiveWorkloadService> service);
  McpLiveWorkloadDrainResult CloseWorkloadService(
      std::chrono::steady_clock::time_point deadline);
  McpLiveWorkloadDrainResult RequestWorkloadServiceCancellation();
  McpLiveWorkloadDrainResult WaitForWorkloadServiceDrain(
      std::stop_token stop_token = {});
  McpLiveWorkloadDrainResult WaitForWorkloadServiceQuarantine(
      std::stop_token stop_token = {});
  void PublishWorkloadServiceShutdownTimeout(
      const McpLiveWorkloadDrainResult& result,
      std::chrono::milliseconds shutdown_bound) const noexcept;
  void SetInstrumentationService(
      std::shared_ptr<McpLiveInstrumentationService> service);
  void SetRoleService(std::shared_ptr<McpLiveRoleService> service);

  // Called exactly once by SimulationCommandProcessor. Successful node
  // mutations keep the shared live inventory synchronized across MCP, TUI,
  // and scheduled commands; only MCP-submitted outcomes consume pending state.
  void RecordCommandOutcome(const SimulationCommand& command,
                            const SimulationCommandOutcome& outcome);
  void MarkRunStarted();
  void MarkRunStopping();
  void MarkRunStopped();
  void Shutdown();
  void Shutdown(std::stop_token stop_token);
  void Shutdown(std::chrono::steady_clock::time_point deadline,
                std::stop_token stop_token);

 private:
  class ActiveRequest {
   public:
    explicit ActiveRequest(McpLiveApplication* application);
    ~ActiveRequest();

    ActiveRequest(const ActiveRequest&) = delete;
    ActiveRequest& operator=(const ActiveRequest&) = delete;

   private:
    McpLiveApplication* application_;
  };

  struct PendingCommand {
    bool completed = false;
    bool detached = false;
    std::optional<SimulationCommandOutcome> outcome;
  };

  void BeginRequest();
  void EndRequest();
  McpOperationPlan BuildOperation(McpOperationKind kind,
                                  const boost::json::object& arguments,
                                  std::string_view session_id);
  boost::json::value ReadResource(McpInformationFamily family,
                                  std::string_view session_id,
                                  std::stop_token stop_token);
  void RequireRun(const boost::json::object& arguments) const;
  std::uint64_t SubmitCommand(SimulationCommand command);
  void DetachPendingCommand(std::uint64_t sequence) noexcept;
  SimulationCommandOutcome WaitForCommand(
      std::uint64_t sequence, std::stop_token stop_token,
      const std::shared_ptr<SimulationCommandControl>& operation_control,
      std::optional<std::chrono::steady_clock::time_point>
          cancellation_deadline = std::nullopt,
      std::optional<std::chrono::steady_clock::time_point> terminal_deadline =
          std::nullopt,
      std::chrono::steady_clock::duration reconciliation_bound =
          kSimulationCommandCancellationReconciliation,
      McpOperationContext* progress_context = nullptr);
  boost::json::object ReportSnapshot(std::stop_token stop_token,
                                     bool include_artifacts = false);
  std::unique_lock<std::timed_mutex> AcquirePublicationLock(
      std::stop_token stop_token) const;
  std::string RunState(
      std::optional<std::uint32_t> live_node_count = std::nullopt) const;
  std::string CurrentChain() const;
  std::uint32_t NodeCount() const;
  McpLiveNodeInventorySnapshot LiveNodeInventory() const;
  std::shared_ptr<McpLiveWorkloadService> WorkloadService() const;
  std::shared_ptr<McpLiveInstrumentationService> InstrumentationService() const;
  std::shared_ptr<McpLiveRoleService> RoleService() const;
  void PublishEvidence(
      McpInformationFamily family, std::string kind, std::string message,
      std::optional<std::string> node_id = std::nullopt,
      std::optional<boost::json::value> data = std::nullopt) const noexcept;
  void CloseRunSubscriptions() const noexcept;
  void ShutdownImpl(
      std::optional<std::chrono::steady_clock::time_point> deadline,
      std::stop_token stop_token);

  Config config_;
  mutable std::mutex mutex_;
  std::condition_variable command_outcome_ready_;
  std::condition_variable_any requests_drained_;
  std::map<std::uint64_t, PendingCommand> pending_commands_;
  std::shared_ptr<McpLiveWorkloadService> workload_service_;
  std::shared_ptr<McpLiveWorkloadService> draining_workload_service_;
  std::optional<std::chrono::steady_clock::time_point>
      workload_shutdown_deadline_;
  std::shared_ptr<McpLiveInstrumentationService> instrumentation_service_;
  std::shared_ptr<McpLiveRoleService> role_service_;
  std::stop_source request_stop_source_;
  std::stop_source run_stop_source_;
  std::size_t active_requests_ = 0U;
  bool workload_admission_closed_ = false;
  bool run_started_ = false;
  bool stop_requested_ = false;
  bool run_stopped_ = false;
  bool shutdown_ = false;
  std::mutex report_mutex_;
};

}  // namespace bbp
