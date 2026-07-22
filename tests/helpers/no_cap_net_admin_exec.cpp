#include <linux/capability.h>
#include <linux/securebits.h>
#include <sys/prctl.h>
#include <sys/syscall.h>
#include <unistd.h>

#include <array>
#include <cerrno>
#include <cstring>
#include <iostream>

namespace {

bool DropCapabilities() {
  const unsigned int securebits = SECBIT_NOROOT | SECBIT_NOROOT_LOCKED |
                                  SECBIT_NO_SETUID_FIXUP |
                                  SECBIT_NO_SETUID_FIXUP_LOCKED;
  if (prctl(PR_SET_SECUREBITS, securebits, 0L, 0L, 0L) != 0) {
    return false;
  }
  if (prctl(PR_CAPBSET_DROP, CAP_NET_ADMIN, 0L, 0L, 0L) != 0) {
    return false;
  }

  __user_cap_header_struct header{};
  header.version = _LINUX_CAPABILITY_VERSION_3;
  header.pid = 0;
  std::array<__user_cap_data_struct, _LINUX_CAPABILITY_U32S_3> capabilities{};
  if (syscall(SYS_capset, &header, capabilities.data()) != 0) {
    return false;
  }
  if (syscall(SYS_capget, &header, capabilities.data()) != 0) {
    return false;
  }
  constexpr std::size_t kBitsPerWord = 32U;
  const std::size_t word =
      static_cast<std::size_t>(CAP_NET_ADMIN) / kBitsPerWord;
  const unsigned int bit =
      static_cast<unsigned int>(CAP_NET_ADMIN) % kBitsPerWord;
  return (capabilities.at(word).effective & (1U << bit)) == 0U;
}

}  // namespace

int main(int argc, char** argv) {
  if (argc < 2) {
    std::cerr << "usage: bbp-no-cap-net-admin-exec COMMAND [ARG...]\n";
    return 2;
  }
  if (!DropCapabilities()) {
    std::cerr << "drop capabilities failed: " << std::strerror(errno) << '\n';
    return 3;
  }
  std::cerr << "effective_cap_net_admin=0\n";
  execv(argv[1], &argv[1]);
  std::cerr << "execv failed: " << std::strerror(errno) << '\n';
  return 4;
}
