#pragma once

#include <chrono>
#include <condition_variable>
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

class SimulationCommandQueue;
struct Options;

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

  // Called exactly once by SimulationCommandProcessor for commands submitted
  // through this service. Outcomes for unrelated TUI/scenario commands are
  // deliberately ignored and therefore do not consume retained state.
  void RecordCommandOutcome(const SimulationCommand& command,
                            const SimulationCommandOutcome& outcome);
  void MarkRunStarted();
  void MarkRunStopping();
  void MarkRunStopped();
  void Shutdown();

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
  SimulationCommandOutcome WaitForCommand(
      std::uint64_t sequence, std::stop_token stop_token,
      const std::shared_ptr<SimulationCommandControl>& operation_control,
      std::optional<std::chrono::steady_clock::time_point>
          cancellation_deadline = std::nullopt,
      std::optional<std::chrono::steady_clock::time_point> terminal_deadline =
          std::nullopt);
  boost::json::object ReportSnapshot(std::stop_token stop_token);
  std::string RunState() const;
  std::string CurrentChain() const;
  std::uint32_t NodeCount() const;
  void PublishEvidence(McpInformationFamily family, std::string kind,
                       std::string message,
                       std::optional<std::string> node_id = std::nullopt,
                       std::optional<boost::json::value> data = std::nullopt)
      const noexcept;
  void CloseRunSubscriptions() const noexcept;

  Config config_;
  mutable std::mutex mutex_;
  std::condition_variable command_outcome_ready_;
  std::condition_variable requests_drained_;
  std::map<std::uint64_t, PendingCommand> pending_commands_;
  std::stop_source request_stop_source_;
  std::stop_source run_stop_source_;
  std::size_t active_requests_ = 0U;
  bool run_started_ = false;
  bool stop_requested_ = false;
  bool run_stopped_ = false;
  bool shutdown_ = false;
  std::mutex report_mutex_;
};

}  // namespace bbp
