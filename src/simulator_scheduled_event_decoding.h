#pragma once

#include <boost/json/array.hpp>
#include <boost/program_options/variables_map.hpp>

namespace bbp {

struct Options;

namespace simulator_app_internal {

void ApplyScheduledScenarioEvents(
    const boost::json::array& events,
    const boost::program_options::variables_map& vm, Options& options);

}  // namespace simulator_app_internal
}  // namespace bbp
