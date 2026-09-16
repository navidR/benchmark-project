#pragma once

#include <sys/types.h>

#include <chrono>
#include <filesystem>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "bbp/process_signal.h"

namespace bbp {

struct ProcessSpec {
  std::filesystem::path binary;
  std::vector<std::string> argv;
  std::filesystem::path cwd;
  std::filesystem::path stdout_path;
  std::filesystem::path stderr_path;
  std::vector<std::pair<std::string, std::string>> environment;
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
  ChildProcess& operator=(ChildProcess&& other);
  ~ChildProcess();

  pid_t pid() const { return pid_; }
  int pidfd() const { return pidfd_; }
  bool running() const;
  // Observes the owned leader without consuming stop/continue notifications.
  // "running" means alive, including a sleeping process, not RPC readiness.
  std::string_view ObservedState() const;
  bool WaitForExit(std::chrono::milliseconds timeout);
  bool RequestTerminate();
  bool RequestKill();
  // A successful kernel result reports delivery acceptance, not termination.
  // Observe lifecycle separately with running/WaitForExit/exit_status.
  // Group delivery requires Linux 6.9; unsupported kernels return EINVAL.
  ProcessSignalDelivery DeliverSignal(int signal, ProcessSignalScope scope);
  void Terminate(std::chrono::milliseconds graceful_timeout);
  void Kill();
  std::optional<int> exit_status() const { return exit_status_; }

 private:
  ChildProcess(pid_t pid, int pidfd) : pid_(pid), pidfd_(pidfd) {}

  pid_t pid_ = -1;
  int pidfd_ = -1;
  mutable std::optional<int> exit_status_;
};

}  // namespace bbp
