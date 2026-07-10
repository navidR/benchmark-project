#pragma once

#include <functional>
#include <string_view>
#include <thread>

#include "bbp/simulation_command.h"
#include "bbp/simulation_command_queue.h"

namespace bbp {

class SimulationCommandProcessor {
 public:
  using CommandHandler = std::function<void(const SimulationCommand&)>;
  using FailureHandler =
      std::function<void(const SimulationCommand&, std::string_view)>;

  SimulationCommandProcessor(SimulationCommandQueue& queue,
                             CommandHandler command_handler,
                             FailureHandler failure_handler);
  SimulationCommandProcessor(const SimulationCommandProcessor&) = delete;
  SimulationCommandProcessor& operator=(const SimulationCommandProcessor&) =
      delete;
  ~SimulationCommandProcessor();

  void Start();
  void Stop();

 private:
  void Run();
  void ReportFailure(const SimulationCommand& command,
                     std::string_view detail) const;

  SimulationCommandQueue& queue_;
  CommandHandler command_handler_;
  FailureHandler failure_handler_;
  std::thread thread_;
  bool started_ = false;
};

}  // namespace bbp
