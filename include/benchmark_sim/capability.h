#pragma once

#include <cstdint>
#include <string_view>

#include "benchmark_sim/result.h"

namespace bsim {

Result<uint64_t> ParseEffectiveCapabilities(std::string_view proc_status);
Result<uint64_t> ReadEffectiveCapabilities();
bool HasCapability(uint64_t effective_capabilities, int capability);
Status RequireEffectiveCapability(int capability, std::string_view name);
Status RequireNetworkSetupCapabilities();

}  // namespace bsim
