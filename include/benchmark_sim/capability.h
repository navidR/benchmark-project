#pragma once

#include <string_view>

namespace bsim {

bool HasEffectiveCapability(int capability);
void RequireEffectiveCapability(int capability, std::string_view name);
void RequireNetworkSetupCapabilities();

}  // namespace bsim
