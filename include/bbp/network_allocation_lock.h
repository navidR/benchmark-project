#pragma once

#include <stop_token>

namespace bbp {

class NetworkAllocationLock {
 public:
  NetworkAllocationLock();
  explicit NetworkAllocationLock(std::stop_token stop_token);
  NetworkAllocationLock(const NetworkAllocationLock&) = delete;
  NetworkAllocationLock& operator=(const NetworkAllocationLock&) = delete;
  NetworkAllocationLock(NetworkAllocationLock&&) = delete;
  NetworkAllocationLock& operator=(NetworkAllocationLock&&) = delete;
  ~NetworkAllocationLock();

 private:
  int fd_ = -1;
};

}  // namespace bbp
