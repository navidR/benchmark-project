#include "bbp/simulation_command_processor.h"

#include <stdexcept>
#include <utility>

#include "bbp/logging.h"
#include "bbp/simulation_cancelled.h"

namespace bbp {
namespace {

constexpr std::string_view kCancelledBeforeExecution =
    "simulation command processor stopped before execution";
constexpr std::string_view kOperationCancelledBeforeExecution =
    "simulation command operation cancelled before execution";

SimulationCommandCancellationCause CancellationCause(
    const SimulationCommand& command,
    SimulationCommandCancellationCause fallback) {
  if (!command.operation_control) {
    return fallback;
  }
  const SimulationCommandCancellationCause cause =
      command.operation_control->cancellation_cause.load(
          std::memory_order_acquire);
  return cause == SimulationCommandCancellationCause::kNone ? fallback : cause;
}

SimulationCommandOutcome CancellationOutcome(
    const SimulationCommand& command, std::string_view error,
    SimulationCommandCancellationCause fallback) {
  const SimulationCommandCancellationCause cause =
      CancellationCause(command, fallback);
  return SimulationCommandOutcome{
      .state = command.operation_control &&
                       command.operation_control->outcome_unconfirmed.load(
                           std::memory_order_acquire)
                   ? SimulationCommandOutcomeState::kOutcomeUnconfirmed
               : cause == SimulationCommandCancellationCause::kDeadline
                   ? SimulationCommandOutcomeState::kTimedOut
                   : SimulationCommandOutcomeState::kCancelled,
      .cancellation_cause = cause,
      .error = std::string(error),
      .node_lifecycle = std::nullopt,
  };
}

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
    ReportOutcome(
        command, CancellationOutcome(
                     command, kCancelledBeforeExecution,
                     SimulationCommandCancellationCause::kApplicationShutdown));
  }
  started_ = false;
}

void SimulationCommandProcessor::Run() {
  while (const std::optional<SimulationCommand> command = queue_.WaitPop()) {
    if (command->operation_control &&
        command->operation_control->stop_source.stop_requested()) {
      ReportOutcome(*command,
                    CancellationOutcome(
                        *command, kOperationCancelledBeforeExecution,
                        SimulationCommandCancellationCause::kClientCancel));
      continue;
    }
    try {
      command_handler_(*command);
      ReportOutcome(
          *command,
          SimulationCommandOutcome{
              .state = SimulationCommandOutcomeState::kSucceeded,
              .cancellation_cause = CancellationCause(
                  *command, SimulationCommandCancellationCause::kNone),
              .error = std::nullopt,
              .node_lifecycle = std::nullopt,
          });
    } catch (const SimulationCancelled& error) {
      ReportOutcome(*command, CancellationOutcome(
                                  *command, error.what(),
                                  SimulationCommandCancellationCause::kNone));
    } catch (const SimulationCommandOutcomeUnconfirmed& error) {
      ReportOutcome(
          *command,
          SimulationCommandOutcome{
              .state = SimulationCommandOutcomeState::kOutcomeUnconfirmed,
              .cancellation_cause = CancellationCause(
                  *command, SimulationCommandCancellationCause::kNone),
              .error = error.what(),
              .node_lifecycle = std::nullopt,
          });
    } catch (const std::exception& error) {
      if (command->operation_control &&
          command->operation_control->outcome_unconfirmed.load(
              std::memory_order_acquire)) {
        ReportOutcome(
            *command,
            SimulationCommandOutcome{
                .state = SimulationCommandOutcomeState::kOutcomeUnconfirmed,
                .cancellation_cause = CancellationCause(
                    *command, SimulationCommandCancellationCause::kNone),
                .error = error.what(),
                .node_lifecycle = std::nullopt,
            });
        continue;
      }
      ReportFailure(*command, error.what());
      ReportOutcome(
          *command,
          SimulationCommandOutcome{
              .state = SimulationCommandOutcomeState::kFailed,
              .cancellation_cause = CancellationCause(
                  *command, SimulationCommandCancellationCause::kNone),
              .error = error.what(),
              .node_lifecycle = std::nullopt,
          });
    } catch (...) {
      if (command->operation_control &&
          command->operation_control->outcome_unconfirmed.load(
              std::memory_order_acquire)) {
        ReportOutcome(
            *command,
            SimulationCommandOutcome{
                .state = SimulationCommandOutcomeState::kOutcomeUnconfirmed,
                .cancellation_cause = CancellationCause(
                    *command, SimulationCommandCancellationCause::kNone),
                .error = "unknown exception",
                .node_lifecycle = std::nullopt,
            });
        continue;
      }
      ReportFailure(*command, "unknown exception");
      ReportOutcome(
          *command,
          SimulationCommandOutcome{
              .state = SimulationCommandOutcomeState::kFailed,
              .cancellation_cause = CancellationCause(
                  *command, SimulationCommandCancellationCause::kNone),
              .error = "unknown exception",
              .node_lifecycle = std::nullopt,
          });
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
    const SimulationCommandOutcome& outcome) const {
  if (!outcome_handler_) {
    return;
  }
  try {
    outcome_handler_(command, outcome);
  } catch (const std::exception& callback_error) {
    BBP_LOG(error) << "simulation command outcome handler failed for command "
                   << command.sequence << ": " << callback_error.what();
  } catch (...) {
    BBP_LOG(error) << "simulation command outcome handler failed for command "
                   << command.sequence;
  }
}

}  // namespace bbp
