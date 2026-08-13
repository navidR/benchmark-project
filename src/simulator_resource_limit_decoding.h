#pragma once

#include <boost/json/object.hpp>
#include <boost/json/value.hpp>
#include <cstdint>
#include <string_view>
#include <vector>

#include "bbp/cgroup.h"
#include "bbp/simulator/resource_limit_patch.h"

namespace bbp::simulator_app_internal {

void RequireNonZero(std::uint64_t value, std::string_view field);
void RequireCgroupWeight(std::uint64_t value, std::string_view field);
std::vector<IoLimit> ParseIoLimits(const boost::json::value& value,
                                   std::string_view field);
ResourceLimitPatch ParseResourceLimitPatchObject(
    const boost::json::object& object);

}  // namespace bbp::simulator_app_internal
