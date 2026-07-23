#include "bbp/network_allocation_lock.h"

#include <fcntl.h>
#include <unistd.h>

#include <cerrno>
#include <chrono>
#include <cstring>
#include <stdexcept>
#include <string>
#include <thread>

namespace bbp {
namespace {

constexpr char kNetworkAllocationLockPath[] =
    "/tmp/blockchain-benchmark-project-network-allocation.lock";
constexpr std::chrono::milliseconds kLockRetryInterval(10);

}  // namespace

NetworkAllocationLock::NetworkAllocationLock()
    : NetworkAllocationLock(std::stop_token{}) {}

NetworkAllocationLock::NetworkAllocationLock(std::stop_token stop_token) {
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
  while (true) {
    if (stop_token.stop_requested()) {
      close(fd_);
      fd_ = -1;
      throw std::runtime_error("lock network allocation cancelled");
    }
    if (fcntl(fd_, F_SETLK, &lock) == 0) {
      return;
    }
    const int error = errno;
    if (error == EINTR) {
      continue;
    }
    if (error != EACCES && error != EAGAIN) {
      close(fd_);
      fd_ = -1;
      throw std::runtime_error("lock network allocation failed: " +
                               std::string(std::strerror(error)));
    }
    std::this_thread::sleep_for(kLockRetryInterval);
  }
}

NetworkAllocationLock::~NetworkAllocationLock() {
  if (fd_ >= 0) {
    close(fd_);
  }
}

}  // namespace bbp
