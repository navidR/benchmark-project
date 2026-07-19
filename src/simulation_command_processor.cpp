#include "bbp/simulation_command_processor.h"

#include <stdexcept>
#include <utility>

#include "bbp/logging.h"

namespace bbp {
namespace {

constexpr std::string_view kCancelledBeforeExecution =
    "simulation command processor stopped before execution";

}  // namespace

SimulationCommandProcessor::SimulationCommandProcessor(
    SimulationCommandQueue& queue, CommandHandler command_handler,
    FailureHandler failure_handler, OutcomeHandler outcome_handler)
    : queue_(queue),
      command_handler_(std::move(command_handler)),
      failure_handler_(std::move(failure_handler)),
      outcome_handler_(std::move(outcome_handler)) {
  if (!command_handler_) {
    throw std::runtime_error("simulation command processor requires a handler");
  }
  if (!failure_handler_) {
    throw std::runtime_error(
        "simulation command processor requires a failure handler");
  }
}

SimulationCommandProcessor::~SimulationCommandProcessor() { Stop(); }

void SimulationCommandProcessor::Start() {
  if (started_) {
    throw std::runtime_error("simulation command processor is already started");
  }
  started_ = true;
  thread_ = std::thread(&SimulationCommandProcessor::Run, this);
}

void SimulationCommandProcessor::Stop() {
  if (!started_) {
    return;
  }
  std::vector<SimulationCommand> cancelled = queue_.Cancel();
  if (thread_.joinable()) {
    thread_.join();
  }
  for (const SimulationCommand& command : cancelled) {
    ReportFailure(command, kCancelledBeforeExecution);
    ReportOutcome(command, kCancelledBeforeExecution);
  }
  started_ = false;
}

void SimulationCommandProcessor::Run() {
  while (const std::optional<SimulationCommand> command = queue_.WaitPop()) {
    try {
      command_handler_(*command);
      ReportOutcome(*command, std::nullopt);
    } catch (const std::exception& error) {
      ReportFailure(*command, error.what());
      ReportOutcome(*command, error.what());
    } catch (...) {
      ReportFailure(*command, "unknown exception");
      ReportOutcome(*command, "unknown exception");
    }
  }
}

void SimulationCommandProcessor::ReportFailure(const SimulationCommand& command,
                                               std::string_view detail) const {
  try {
    failure_handler_(command, detail);
  } catch (const std::exception& error) {
    BBP_LOG(error) << "simulation command failure handler failed for command "
                   << command.sequence << ": " << error.what();
  } catch (...) {
    BBP_LOG(error) << "simulation command failure handler failed for command "
                   << command.sequence;
  }
}

void SimulationCommandProcessor::ReportOutcome(
    const SimulationCommand& command,
    std::optional<std::string_view> error) const {
  if (!outcome_handler_) {
    return;
  }
  try {
    outcome_handler_(command, error);
  } catch (const std::exception& callback_error) {
    BBP_LOG(error) << "simulation command outcome handler failed for command "
                   << command.sequence << ": " << callback_error.what();
  } catch (...) {
    BBP_LOG(error) << "simulation command outcome handler failed for command "
                   << command.sequence;
  }
}

}  // namespace bbp
