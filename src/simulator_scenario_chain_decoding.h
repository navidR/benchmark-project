#pragma once

#include <boost/json/object.hpp>

namespace bbp {

struct Options;

namespace simulator_app_internal {

void ParseScenarioChains(const boost::json::object& scenario, Options* options);

}  // namespace simulator_app_internal
}  // namespace bbp
