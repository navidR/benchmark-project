#include "bbp/capability.h"

#include <linux/capability.h>
#include <sys/syscall.h>
#include <unistd.h>

#include <array>
#include <cerrno>
#include <cstdint>
#include <cstring>
#include <stdexcept>
#include <string>

namespace bbp {
namespace {

std::string ErrnoMessage(std::string_view prefix, int error_number) {
  return std::string(prefix) + ": " + std::strerror(error_number);
}

}  // namespace

bool HasEffectiveCapability(int capability) {
  if (capability < 0 || capability > CAP_LAST_CAP) {
    return false;
  }

  __user_cap_header_struct header{};
  header.version = _LINUX_CAPABILITY_VERSION_3;
  header.pid = 0;
  std::array<__user_cap_data_struct, _LINUX_CAPABILITY_U32S_3> data{};
  if (::syscall(SYS_capget, &header, data.data()) == -1) {
    const int error_number = errno;
    throw std::runtime_error(ErrnoMessage("capget failed", error_number));
  }

  constexpr int kBitsPerWord = 32;
  const auto word = static_cast<std::size_t>(capability / kBitsPerWord);
  const auto bit = static_cast<unsigned int>(capability % kBitsPerWord);
  const std::uint32_t mask = std::uint32_t{1} << bit;
  return (data.at(word).effective & mask) != 0;
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
