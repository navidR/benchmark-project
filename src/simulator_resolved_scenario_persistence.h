#pragma once

#include <boost/json/object.hpp>
#include <filesystem>
#include <vector>

#include "bbp/simulator/scenario_workload.h"

namespace bbp {

struct ChainDriverSpec;
struct Options;

namespace simulator_app_internal {

std::vector<ScenarioWorkload> EffectiveWorkloads(const Options& options);
boost::json::object BuildResolvedScenarioDocument(
    const Options& options, const ChainDriverSpec& chain_spec);
void WriteScenarioFiles(const Options& options,
                        const std::filesystem::path& run_root,
                        const ChainDriverSpec& chain_spec,
                        int reserved_run_root_fd = -1);

}  // namespace simulator_app_internal
}  // namespace bbp
