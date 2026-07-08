#pragma once

#include <chrono>
#include <filesystem>
#include <optional>
#include <string>
#include <vector>

#include <sys/types.h>

namespace bsim {

struct ProcessSpec {
  std::filesystem::path binary;
  std::vector<std::string> argv;
  std::filesystem::path cwd;
  std::filesystem::path stdout_path;
  std::filesystem::path stderr_path;
  std::optional<int> network_namespace_fd;
};

class ChildProcess {
 public:
  static ChildProcess Spawn(const ProcessSpec& spec,
                            const std::optional<std::filesystem::path>& cgroup);

  ChildProcess() = default;
  ChildProcess(const ChildProcess&) = delete;
  ChildProcess& operator=(const ChildProcess&) = delete;
  ChildProcess(ChildProcess&& other) noexcept;
  ChildProcess& operator=(ChildProcess&& other) noexcept;
  ~ChildProcess();

  pid_t pid() const { return pid_; }
  int pidfd() const { return pidfd_; }
  bool running() const;
  bool WaitForExit(std::chrono::milliseconds timeout);
  void Terminate(std::chrono::milliseconds graceful_timeout);
  void Kill();
  std::optional<int> exit_status() const { return exit_status_; }

 private:
  ChildProcess(pid_t pid, int pidfd) : pid_(pid), pidfd_(pidfd) {}

  pid_t pid_ = -1;
  int pidfd_ = -1;
  mutable std::optional<int> exit_status_;
};

}  // namespace bsim
