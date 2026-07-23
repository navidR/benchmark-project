#pragma once

#include <condition_variable>
#include <cstdint>
#include <filesystem>
#include <functional>
#include <map>
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
    const Options* options = nullptr;
    SimulationCommandQueue* command_queue = nullptr;
    std::function<void()> request_run_stop;
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
                            std::optional<std::string_view> error);
  void MarkRunStarted();
  void MarkRunStopping();
  void MarkRunStopped();
  void Shutdown();

 private:
  struct PendingCommand {
    bool completed = false;
    std::optional<std::string> error;
  };

  McpOperationPlan BuildOperation(McpOperationKind kind,
                                  const boost::json::object& arguments,
                                  std::string_view session_id);
  boost::json::value ReadResource(McpInformationFamily family,
                                  std::string_view session_id,
                                  std::stop_token stop_token);
  void RequireRun(const boost::json::object& arguments) const;
  std::uint64_t SubmitCommand(SimulationCommand command);
  std::optional<std::string> WaitForCommand(std::uint64_t sequence,
                                            std::stop_token stop_token);
  boost::json::object ReportSnapshot(std::stop_token stop_token);
  std::string RunState() const;
  std::string CurrentChain() const;
  std::uint32_t NodeCount() const;

  Config config_;
  mutable std::mutex mutex_;
  std::condition_variable command_outcome_ready_;
  std::map<std::uint64_t, PendingCommand> pending_commands_;
  bool run_started_ = false;
  bool stop_requested_ = false;
  bool run_stopped_ = false;
  bool shutdown_ = false;
  std::timed_mutex report_mutex_;
};

}  // namespace bbp
