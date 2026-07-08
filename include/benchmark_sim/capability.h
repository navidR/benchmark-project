#pragma once

#include <cstdint>
#include <string_view>

namespace bsim {

uint64_t ParseEffectiveCapabilities(std::string_view proc_status);
uint64_t ReadEffectiveCapabilities();
bool HasCapability(uint64_t effective_capabilities, int capability);
void RequireEffectiveCapability(int capability, std::string_view name);
void RequireNetworkSetupCapabilities();

}  // namespace bsim
