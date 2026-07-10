#include "bbp/capability.h"

#include <sys/capability.h>

#include <cerrno>
#include <cstring>
#include <stdexcept>
#include <string>

namespace bbp {
namespace {

class ScopedCapabilities {
 public:
  explicit ScopedCapabilities(cap_t capabilities)
      : capabilities_(capabilities) {}

  ScopedCapabilities(const ScopedCapabilities&) = delete;
  ScopedCapabilities& operator=(const ScopedCapabilities&) = delete;

  ~ScopedCapabilities() {
    if (capabilities_ != nullptr) {
      cap_free(capabilities_);
    }
  }

  cap_t get() const { return capabilities_; }

 private:
  cap_t capabilities_;
};

std::string ErrnoMessage(std::string_view prefix, int error_number) {
  return std::string(prefix) + ": " + std::strerror(error_number);
}

}  // namespace

bool HasEffectiveCapability(int capability) {
  if (capability < 0 || capability >= cap_max_bits()) {
    return false;
  }
  ScopedCapabilities capabilities(cap_get_proc());
  if (capabilities.get() == nullptr) {
    throw std::runtime_error(ErrnoMessage("cap_get_proc failed", errno));
  }
  cap_flag_value_t value = CAP_CLEAR;
  if (cap_get_flag(capabilities.get(), capability, CAP_EFFECTIVE, &value) !=
      0) {
    throw std::runtime_error(ErrnoMessage("cap_get_flag failed", errno));
  }
  return value == CAP_SET;
}

void RequireEffectiveCapability(int capability, std::string_view name) {
  if (!HasEffectiveCapability(capability)) {
    throw std::runtime_error("missing required capability: " +
                             std::string(name));
  }
}

void RequireNetworkSetupCapabilities() {
  RequireEffectiveCapability(CAP_SYS_ADMIN, "CAP_SYS_ADMIN");
  RequireEffectiveCapability(CAP_NET_ADMIN, "CAP_NET_ADMIN");
}

}  // namespace bbp
