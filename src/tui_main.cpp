#include <boost/program_options.hpp>
#include <cstdint>
#include <filesystem>
#include <iostream>
#include <stdexcept>

#include "benchmark_sim/logging.h"
#include "benchmark_sim/tui.h"

namespace {

struct Options {
  std::filesystem::path run_root;
  bool once = false;
  std::uint32_t refresh_ms = 1000;
};

Options ParseOptions(int argc, char** argv) {
  namespace po = boost::program_options;
  Options options;
  po::options_description desc("Allowed options");
  desc.add_options()("help", "show this help")(
      "run", po::value<std::filesystem::path>(&options.run_root),
      "run directory containing events.jsonl and metrics.jsonl")(
      "once", po::bool_switch(&options.once), "render one frame and exit")(
      "refresh-ms", po::value<std::uint32_t>(&options.refresh_ms),
      "milliseconds between report refreshes");

  po::variables_map vm;
  po::store(po::parse_command_line(argc, argv, desc), vm);
  po::notify(vm);

  if (vm.count("help") != 0U) {
    std::cout << "Usage: " << argv[0] << " --run <run-dir> [options]\n"
              << desc << "\n";
    std::exit(0);
  }
  if (options.run_root.empty()) {
    throw std::runtime_error("benchmark-tui requires --run <run-dir>");
  }
  if (options.refresh_ms == 0U) {
    throw std::runtime_error("--refresh-ms must be greater than zero");
  }
  return options;
}

}  // namespace

int main(int argc, char** argv) {
  try {
    bsim::InitLogging();
    const Options options = ParseOptions(argc, argv);
    return bsim::RunTuiReport(options.run_root, options.once,
                              options.refresh_ms);
  } catch (const std::exception& e) {
    BSIM_LOG(error) << "benchmark-tui: " << e.what();
    return 1;
  }
}
