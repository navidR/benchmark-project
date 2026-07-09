#include "benchmark_sim/capability.h"

#include <linux/capability.h>

#include <sstream>
#include <string>

#include "benchmark_sim/util.h"

namespace bsim {

Result<uint64_t> ParseEffectiveCapabilities(std::string_view proc_status) {
  std::istringstream lines{std::string(proc_status)};
  std::string line;
  while (std::getline(lines, line)) {
    std::istringstream fields(line);
    std::string name;
    std::string value;
    fields >> name >> value;
    if (name == "CapEff:" && !value.empty()) {
      return Ok(std::stoull(value, nullptr, 16));
    }
  }
  return Error<uint64_t>("missing CapEff in /proc/self/status");
}

Result<uint64_t> ReadEffectiveCapabilities() {
  return ParseEffectiveCapabilities(ReadText("/proc/self/status"));
}

bool HasCapability(uint64_t effective_capabilities, int capability) {
  if (capability < 0 || capability >= 64) {
    return false;
  }
  return (effective_capabilities & (uint64_t{1} << capability)) != 0U;
}

Status RequireEffectiveCapability(int capability, std::string_view name) {
  const Result<uint64_t> capabilities = ReadEffectiveCapabilities();
  if (!capabilities) {
    return ErrorStatus(capabilities.error());
  }
  if (!HasCapability(capabilities.unsafe_value(), capability)) {
    return ErrorStatus("missing required capability: " + std::string(name));
  }
  return OkStatus();
}

Status RequireNetworkSetupCapabilities() {
  Status status = RequireEffectiveCapability(CAP_SYS_ADMIN, "CAP_SYS_ADMIN");
  if (!status) {
    return status;
  }
  status = RequireEffectiveCapability(CAP_NET_ADMIN, "CAP_NET_ADMIN");
  if (!status) {
    return status;
  }
  return OkStatus();
}

}  // namespace bsim
