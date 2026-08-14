#pragma once

#include <boost/json/object.hpp>

namespace bbp {

struct Options;
struct ProfileSwitchWorkload;
enum class WorkloadKind;

namespace simulator_app_internal {

ProfileSwitchWorkload ParseProfileSwitchWorkload(
    const boost::json::object& object, const Options& options,
    WorkloadKind kind);
void ParseNetworkProfiles(const boost::json::object& scenario,
                          Options* options);
void ResolveNodeProfileAssignments(Options* options);
void ValidateProfileSwitchReferences(Options* options);

}  // namespace simulator_app_internal
}  // namespace bbp
