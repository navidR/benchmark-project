#include <stdexcept>

#include "bbp/logging.h"
#include "bbp/simulator_app.h"

int main(int argc, char** argv) {
  try {
    bbp::InitLogging();
    bbp::SimulatorApp app;
    return app.Run(argc, argv);
  } catch (const std::exception& e) {
    BBP_LOG(error) << "bbp: " << e.what();
    return 1;
  }
}
