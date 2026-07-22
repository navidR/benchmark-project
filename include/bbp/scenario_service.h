#pragma once

#include <boost/json/object.hpp>

#include "bbp/simulation_command.h"
#include "bbp/simulator/options.h"

namespace bbp {

// Parses and validates an in-memory scenario through the same production
// parser and final validation used by the command-line scenario path.
Options ParseAndValidateScenario(const boost::json::object& scenario);

// Returns the same canonical resolved document that is written into a run.
boost::json::object ResolveScenario(const boost::json::object& scenario);

// Parses a live command object whose "kind" member names a registered
// SimulationCommandKind. Payload validation is shared with scheduled events.
SimulationCommand ParseAndValidateSimulationCommand(
    const boost::json::object& command, const Options& options);

}  // namespace bbp
