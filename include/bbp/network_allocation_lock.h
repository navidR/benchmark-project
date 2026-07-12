#pragma once

namespace bbp {

class NetworkAllocationLock {
 public:
  NetworkAllocationLock();
  NetworkAllocationLock(const NetworkAllocationLock&) = delete;
  NetworkAllocationLock& operator=(const NetworkAllocationLock&) = delete;
  NetworkAllocationLock(NetworkAllocationLock&&) = delete;
  NetworkAllocationLock& operator=(NetworkAllocationLock&&) = delete;
  ~NetworkAllocationLock();

 private:
  int fd_ = -1;
};

}  // namespace bbp
