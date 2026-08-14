#pragma once

#include <boost/json/object.hpp>
#include <cstdint>

namespace bbp {

struct Options;
struct ScenarioWorkload;

namespace simulator_app_internal {

Options ParseOptions(int argc, char** argv,
                     const boost::json::object* in_memory_scenario);
void ValidateScenarioWorkload(const ScenarioWorkload& workload,
                              std::uint32_t available_node_count,
                              const Options& options);

}  // namespace simulator_app_internal
}  // namespace bbp
