#include "benchmark_sim/process.h"

#include "benchmark_sim/util.h"

#include <fcntl.h>
#include <signal.h>
#include <sys/syscall.h>
#include <sys/wait.h>
#include <unistd.h>

#include <cerrno>
#include <cstring>
#include <stdexcept>
#include <thread>

namespace bsim {
namespace {

[[noreturn]] void ChildFail(const char* message) {
  const char* prefix = "child setup failed: ";
  write(STDERR_FILENO, prefix, std::strlen(prefix));
  write(STDERR_FILENO, message, std::strlen(message));
  write(STDERR_FILENO, "\n", 1);
  _exit(127);
}

int OpenLog(const std::filesystem::path& path) {
  int fd = open(path.c_str(), O_WRONLY | O_CREAT | O_TRUNC | O_CLOEXEC, 0644);
  if (fd < 0) {
    ChildFail("open log");
  }
  return fd;
}

int PidfdOpen(pid_t pid) {
#ifdef SYS_pidfd_open
  int fd = static_cast<int>(syscall(SYS_pidfd_open, pid, 0));
  if (fd >= 0 || errno == ENOSYS) {
    return fd;
  }
  return -1;
#else
  (void)pid;
  return -1;
#endif
}

int PidfdSendSignal(int pidfd, int sig) {
#ifdef SYS_pidfd_send_signal
  return static_cast<int>(syscall(SYS_pidfd_send_signal, pidfd, sig, nullptr, 0));
#else
  (void)pidfd;
  (void)sig;
  errno = ENOSYS;
  return -1;
#endif
}

void AttachPidToCgroup(const std::filesystem::path& cgroup, pid_t pid) {
  WriteText(cgroup / "cgroup.procs", std::to_string(pid));
}

}  // namespace

ChildProcess ChildProcess::Spawn(
    const ProcessSpec& spec,
    const std::optional<std::filesystem::path>& cgroup) {
  RequireExecutable(spec.binary);
  EnsureDirectory(spec.stdout_path.parent_path());
  EnsureDirectory(spec.stderr_path.parent_path());
  if (!spec.cwd.empty()) {
    EnsureDirectory(spec.cwd);
  }

  int gate[2];
  if (pipe2(gate, O_CLOEXEC) != 0) {
    throw std::runtime_error(std::string("pipe2 failed: ") + std::strerror(errno));
  }

  pid_t pid = fork();
  if (pid < 0) {
    close(gate[0]);
    close(gate[1]);
    throw std::runtime_error(std::string("fork failed: ") + std::strerror(errno));
  }

  if (pid == 0) {
    close(gate[1]);
    if (setpgid(0, 0) != 0) {
      ChildFail("setpgid");
    }
    if (!spec.cwd.empty() && chdir(spec.cwd.c_str()) != 0) {
      ChildFail("chdir");
    }

    int out_fd = OpenLog(spec.stdout_path);
    int err_fd = OpenLog(spec.stderr_path);
    if (dup2(out_fd, STDOUT_FILENO) < 0 || dup2(err_fd, STDERR_FILENO) < 0) {
      ChildFail("dup2");
    }
    close(out_fd);
    close(err_fd);

    char token = 0;
    ssize_t n = read(gate[0], &token, 1);
    close(gate[0]);
    if (n != 1 || token != 'x') {
      ChildFail("start gate");
    }

    std::vector<std::string> argv_storage;
    argv_storage.reserve(spec.argv.size() + 1);
    argv_storage.push_back(spec.binary.string());
    for (const auto& arg : spec.argv) {
      argv_storage.push_back(arg);
    }

    std::vector<char*> argv;
    argv.reserve(argv_storage.size() + 1);
    for (auto& arg : argv_storage) {
      argv.push_back(arg.data());
    }
    argv.push_back(nullptr);

    execv(spec.binary.c_str(), argv.data());
    ChildFail("execv");
  }

  close(gate[0]);
  try {
    if (cgroup) {
      AttachPidToCgroup(*cgroup, pid);
    }
    char token = 'x';
    if (write(gate[1], &token, 1) != 1) {
      throw std::runtime_error("failed to release child start gate");
    }
  } catch (...) {
    close(gate[1]);
    kill(pid, SIGKILL);
    waitpid(pid, nullptr, 0);
    throw;
  }
  close(gate[1]);

  return ChildProcess(pid, PidfdOpen(pid));
}

ChildProcess::ChildProcess(ChildProcess&& other) noexcept {
  *this = std::move(other);
}

ChildProcess& ChildProcess::operator=(ChildProcess&& other) noexcept {
  if (this == &other) {
    return *this;
  }
  if (pidfd_ >= 0) {
    close(pidfd_);
  }
  pid_ = other.pid_;
  pidfd_ = other.pidfd_;
  exit_status_ = other.exit_status_;
  other.pid_ = -1;
  other.pidfd_ = -1;
  other.exit_status_.reset();
  return *this;
}

ChildProcess::~ChildProcess() {
  if (pidfd_ >= 0) {
    close(pidfd_);
  }
}

bool ChildProcess::running() const {
  if (pid_ <= 0 || exit_status_) {
    return false;
  }
  int status = 0;
  pid_t rc = waitpid(pid_, &status, WNOHANG);
  if (rc == 0) {
    return true;
  }
  if (rc == pid_) {
    exit_status_ = status;
    return false;
  }
  if (errno == ECHILD) {
    exit_status_ = 0;
    return false;
  }
  return true;
}

bool ChildProcess::WaitForExit(std::chrono::milliseconds timeout) {
  const auto deadline = std::chrono::steady_clock::now() + timeout;
  while (std::chrono::steady_clock::now() < deadline) {
    if (!running()) {
      return true;
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
  }
  return !running();
}

void ChildProcess::Terminate(std::chrono::milliseconds graceful_timeout) {
  if (!running()) {
    return;
  }
  if (pidfd_ >= 0 && PidfdSendSignal(pidfd_, SIGTERM) == 0) {
    if (WaitForExit(graceful_timeout)) {
      return;
    }
  } else {
    kill(-pid_, SIGTERM);
    if (WaitForExit(graceful_timeout)) {
      return;
    }
  }
  Kill();
}

void ChildProcess::Kill() {
  if (!running()) {
    return;
  }
  if (pidfd_ >= 0 && PidfdSendSignal(pidfd_, SIGKILL) == 0) {
    WaitForExit(std::chrono::seconds(5));
    return;
  }
  kill(-pid_, SIGKILL);
  WaitForExit(std::chrono::seconds(5));
}

}  // namespace bsim
