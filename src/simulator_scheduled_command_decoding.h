#pragma once

#include <boost/json/object.hpp>

namespace bbp {

struct Options;
struct SimulationCommand;
enum class SimulationCommandKind;

namespace simulator_app_internal {

SimulationCommand ParseScheduledSimulationCommand(
    const boost::json::object& object, SimulationCommandKind kind,
    const Options& options);

}  // namespace simulator_app_internal
}  // namespace bbp
