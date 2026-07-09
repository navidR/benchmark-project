#include <boost/program_options.hpp>
#include <cstdint>
#include <exception>
#include <filesystem>
#include <utility>

#include "benchmark_sim/logging.h"
#include "benchmark_sim/result.h"
#include "benchmark_sim/tui.h"

namespace {

struct Options {
  std::filesystem::path run_root;
  bool once = false;
  std::uint32_t refresh_ms = 1000;
};

bsim::Result<Options> ParseOptions(int argc, char** argv) {
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
  try {
    po::store(po::parse_command_line(argc, argv, desc), vm);
    po::notify(vm);
  } catch (const std::exception& e) {
    return bsim::Error<Options>(e.what());
  }

  if (vm.count("help") != 0U) {
    BSIM_LOG(info) << "Usage: " << argv[0] << " --run <run-dir> [options]\n"
                   << desc;
    std::exit(0);
  }
  if (options.run_root.empty()) {
    return bsim::Error<Options>("benchmark-tui requires --run <run-dir>");
  }
  if (options.refresh_ms == 0U) {
    return bsim::Error<Options>("--refresh-ms must be greater than zero");
  }
  return bsim::Ok(std::move(options));
}

}  // namespace

int main(int argc, char** argv) {
  try {
    bsim::InitLogging();
    bsim::Result<Options> options_result = ParseOptions(argc, argv);
    if (!options_result) {
      BSIM_LOG(error) << "benchmark-tui: " << options_result.error();
      return 1;
    }
    const Options options = std::move(options_result).unsafe_value();
    return bsim::RunTuiReport(options.run_root, options.once,
                              options.refresh_ms);
  } catch (const std::exception& e) {
    BSIM_LOG(error) << "benchmark-tui: " << e.what();
    return 1;
  }
}
