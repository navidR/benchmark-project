#include "benchmark_sim/capability.h"

#include "benchmark_sim/util.h"

#include <linux/capability.h>

#include <sstream>
#include <stdexcept>
#include <string>

namespace bsim {

uint64_t ParseEffectiveCapabilities(std::string_view proc_status) {
  std::istringstream lines{std::string(proc_status)};
  std::string line;
  while (std::getline(lines, line)) {
    std::istringstream fields(line);
    std::string name;
    std::string value;
    fields >> name >> value;
    if (name == "CapEff:" && !value.empty()) {
      return std::stoull(value, nullptr, 16);
    }
  }
  throw std::runtime_error("missing CapEff in /proc/self/status");
}

uint64_t ReadEffectiveCapabilities() {
  return ParseEffectiveCapabilities(ReadText("/proc/self/status"));
}

bool HasCapability(uint64_t effective_capabilities, int capability) {
  if (capability < 0 || capability >= 64) {
    return false;
  }
  return (effective_capabilities & (uint64_t{1} << capability)) != 0U;
}

void RequireEffectiveCapability(int capability, std::string_view name) {
  if (!HasCapability(ReadEffectiveCapabilities(), capability)) {
    throw std::runtime_error("missing required capability: " +
                             std::string(name));
  }
}

void RequireNetworkSetupCapabilities() {
  RequireEffectiveCapability(CAP_SYS_ADMIN, "CAP_SYS_ADMIN");
  RequireEffectiveCapability(CAP_NET_ADMIN, "CAP_NET_ADMIN");
}

}  // namespace bsim
