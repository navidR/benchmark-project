#pragma once

#include <filesystem>

namespace bbp {

class ApplicationInstanceLock {
 public:
  ApplicationInstanceLock();
  explicit ApplicationInstanceLock(std::filesystem::path state_directory);
  ~ApplicationInstanceLock();

  ApplicationInstanceLock(const ApplicationInstanceLock&) = delete;
  ApplicationInstanceLock& operator=(const ApplicationInstanceLock&) = delete;
  ApplicationInstanceLock(ApplicationInstanceLock&&) = delete;
  ApplicationInstanceLock& operator=(ApplicationInstanceLock&&) = delete;

  const std::filesystem::path& state_directory() const;

 private:
  std::filesystem::path state_directory_;
  int lock_fd_ = -1;
  bool process_guard_held_ = false;
};

}  // namespace bbp
