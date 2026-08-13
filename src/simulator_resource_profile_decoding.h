#pragma once

#include <boost/json/object.hpp>
#include <cstdint>
#include <string>
#include <string_view>

#include "bbp/simulator/resource_limit_patch.h"
#include "bbp/simulator/resource_limits.h"

namespace bbp {

struct Options;

namespace simulator_app_internal {

ResourceLimits InitialResourceLimits(const Options& options);
ResourceLimits InitialResourceLimits(const Options& options,
                                     std::uint32_t node_index);
ResourceLimits ApplyResourceLimitPatch(const ResourceLimits& current,
                                       const ResourceLimitPatch& patch,
                                       const std::string& node_id);
void RequireSafeScenarioIdentifier(std::string_view value,
                                   std::string_view field);
void ParseResourceProfiles(const boost::json::object& scenario,
                           Options* options);

}  // namespace simulator_app_internal
}  // namespace bbp
