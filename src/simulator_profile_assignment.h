#pragma once

#include <boost/json/object.hpp>

namespace bbp {

struct Options;

namespace simulator_app_internal {

void ParseNetworkProfiles(const boost::json::object& scenario,
                          Options* options);
void ResolveNodeProfileAssignments(Options* options);
void ValidateProfileSwitchReferences(Options* options);

}  // namespace simulator_app_internal
}  // namespace bbp
