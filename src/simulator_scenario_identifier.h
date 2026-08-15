#pragma once

#include <string_view>

namespace bbp::simulator_app_internal {

void RequireSafeScenarioIdentifier(std::string_view value,
                                   std::string_view field);

}  // namespace bbp::simulator_app_internal
