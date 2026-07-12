#include "bbp/network_allocation_lock.h"

#include <fcntl.h>
#include <unistd.h>

#include <cerrno>
#include <cstring>
#include <stdexcept>
#include <string>

namespace bbp {
namespace {

constexpr char kNetworkAllocationLockPath[] =
    "/tmp/blockchain-benchmark-project-network-allocation.lock";

}  // namespace

NetworkAllocationLock::NetworkAllocationLock() {
  fd_ = open(kNetworkAllocationLockPath,
             O_CREAT | O_RDWR | O_CLOEXEC | O_NOFOLLOW, 0600);
  if (fd_ < 0) {
    throw std::runtime_error("open network allocation lock failed: " +
                             std::string(std::strerror(errno)));
  }
  struct flock lock = {};
  lock.l_type = F_WRLCK;
  lock.l_whence = SEEK_SET;
  // Process-scoped locks are not inherited by forked namespace helpers.
  int result = 0;
  do {
    result = fcntl(fd_, F_SETLKW, &lock);
  } while (result != 0 && errno == EINTR);
  if (result != 0) {
    const int error = errno;
    close(fd_);
    fd_ = -1;
    throw std::runtime_error("lock network allocation failed: " +
                             std::string(std::strerror(error)));
  }
}

NetworkAllocationLock::~NetworkAllocationLock() {
  if (fd_ >= 0) {
    close(fd_);
  }
}

}  // namespace bbp
