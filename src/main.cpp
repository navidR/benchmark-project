#include <iostream>
#include <stdexcept>

#include "benchmark_sim/logging.h"
#include "benchmark_sim/simulator_app.h"

int main(int argc, char** argv) {
  try {
    bsim::InitLogging();
    bsim::SimulatorApp app;
    return app.Run(argc, argv);
  } catch (const std::exception& e) {
    std::cerr << "benchmark-sim: " << e.what() << "\n";
    return 1;
  }
}
