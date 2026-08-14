#pragma once

#include <boost/json/object.hpp>
#include <boost/program_options/variables_map.hpp>

namespace bbp {

struct Options;

namespace simulator_app_internal {

void ApplyScenarioJson(const boost::json::object& scenario,
                       const boost::program_options::variables_map& vm,
                       Options& options);

}  // namespace simulator_app_internal
}  // namespace bbp
