#include "benchmark_sim/simulation_command_processor.h"

#include <stdexcept>
#include <utility>

#include "benchmark_sim/logging.h"

namespace bsim {

SimulationCommandProcessor::SimulationCommandProcessor(
    SimulationCommandQueue& queue, CommandHandler command_handler,
    FailureHandler failure_handler)
    : queue_(queue),
      command_handler_(std::move(command_handler)),
      failure_handler_(std::move(failure_handler)) {
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
  queue_.Cancel();
  if (thread_.joinable()) {
    thread_.join();
  }
  started_ = false;
}

void SimulationCommandProcessor::Run() {
  while (const std::optional<SimulationCommand> command = queue_.WaitPop()) {
    try {
      command_handler_(*command);
    } catch (const std::exception& error) {
      ReportFailure(*command, error.what());
    } catch (...) {
      ReportFailure(*command, "unknown exception");
    }
  }
}

void SimulationCommandProcessor::ReportFailure(const SimulationCommand& command,
                                               std::string_view detail) const {
  try {
    failure_handler_(command, detail);
  } catch (const std::exception& error) {
    BSIM_LOG(error) << "simulation command failure handler failed for command "
                    << command.sequence << ": " << error.what();
  } catch (...) {
    BSIM_LOG(error) << "simulation command failure handler failed for command "
                    << command.sequence;
  }
}

}  // namespace bsim
