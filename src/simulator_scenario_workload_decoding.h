#pragma once

#include <boost/json/array.hpp>
#include <boost/json/object.hpp>
#include <boost/program_options/variables_map.hpp>

#include "bbp/simulator/workload_kind.h"

namespace bbp {

struct Options;

namespace simulator_app_internal {

void RejectUnsupportedScenarioActionFields(const boost::json::object& object,
                                           WorkloadKind kind, bool scheduled);
void ApplyScenarioWorkloads(const boost::json::array& workloads,
                            const boost::program_options::variables_map& vm,
                            Options& options);

}  // namespace simulator_app_internal
}  // namespace bbp
