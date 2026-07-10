#pragma once

#include <string_view>

namespace bbp {

bool HasEffectiveCapability(int capability);
void RequireEffectiveCapability(int capability, std::string_view name);
void RequireNetworkSetupCapabilities();

}  // namespace bbp
