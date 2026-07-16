#include "bbp/process.h"

#include <fcntl.h>
#include <sched.h>
#include <signal.h>
#include <sys/syscall.h>
#include <sys/wait.h>
#include <unistd.h>

#include <cerrno>
#include <cstring>
#include <stdexcept>
#include <thread>

#include "bbp/util.h"

namespace bbp {
namespace {

void WriteExactOrExit(int fd, const void* data, size_t size) {
  const char* cursor = static_cast<const char*>(data);
  size_t remaining = size;
  while (remaining != 0U) {
    const ssize_t written = write(fd, cursor, remaining);
    if (written < 0) {
      if (errno == EINTR) {
        continue;
      }
      _exit(127);
    }
    if (written == 0) {
      _exit(127);
    }
    cursor += written;
    remaining -= static_cast<size_t>(written);
  }
}

[[noreturn]] void ChildFail(const char* message) {
  constexpr char kPrefix[] = "child setup failed: ";
  constexpr char kNewline[] = "\n";
  WriteExactOrExit(STDERR_FILENO, kPrefix, sizeof(kPrefix) - 1U);
  WriteExactOrExit(STDERR_FILENO, message, std::strlen(message));
  WriteExactOrExit(STDERR_FILENO, kNewline, sizeof(kNewline) - 1U);
  _exit(127);
}

[[noreturn]] void ChildSetupFail(const char* message, int status_fd) {
  const int status = errno == 0 ? ECHILD : errno;
  WriteExactOrExit(status_fd, &status, sizeof(status));
  ChildFail(message);
}

void WriteSetupOkOrExit(int status_fd) {
  const int status = 0;
  WriteExactOrExit(status_fd, &status, sizeof(status));
}

int ReadSetupStatus(int fd) {
  int status = 0;
  char* cursor = reinterpret_cast<char*>(&status);
  size_t remaining = sizeof(status);
  while (remaining != 0U) {
    const ssize_t received = read(fd, cursor, remaining);
    if (received < 0) {
      if (errno == EINTR) {
        continue;
      }
      throw std::runtime_error(std::string("read child setup status failed: ") +
                               std::strerror(errno));
    }
    if (received == 0) {
      throw std::runtime_error("child exited before reporting setup status");
    }
    cursor += received;
    remaining -= static_cast<size_t>(received);
  }
  return status;
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
  return static_cast<int>(
      syscall(SYS_pidfd_send_signal, pidfd, sig, nullptr, 0));
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
    throw std::runtime_error(std::string("pipe2 failed: ") +
                             std::strerror(errno));
  }
  int setup_status[2];
  if (pipe2(setup_status, O_CLOEXEC) != 0) {
    close(gate[0]);
    close(gate[1]);
    throw std::runtime_error(std::string("pipe2 failed: ") +
                             std::strerror(errno));
  }

  pid_t pid = fork();
  if (pid < 0) {
    close(gate[0]);
    close(gate[1]);
    close(setup_status[0]);
    close(setup_status[1]);
    throw std::runtime_error(std::string("fork failed: ") +
                             std::strerror(errno));
  }

  if (pid == 0) {
    close(gate[1]);
    close(setup_status[0]);
    if (spec.network_namespace_fd &&
        setns(*spec.network_namespace_fd, CLONE_NEWNET) != 0) {
      ChildSetupFail("setns network namespace", setup_status[1]);
    }
    if (setpgid(0, 0) != 0) {
      ChildSetupFail("setpgid", setup_status[1]);
    }
    if (!spec.cwd.empty() && chdir(spec.cwd.c_str()) != 0) {
      ChildSetupFail("chdir", setup_status[1]);
    }

    int out_fd = OpenLog(spec.stdout_path);
    int err_fd = OpenLog(spec.stderr_path);
    if (dup2(out_fd, STDOUT_FILENO) < 0 || dup2(err_fd, STDERR_FILENO) < 0) {
      ChildSetupFail("dup2", setup_status[1]);
    }
    close(out_fd);
    close(err_fd);

    WriteSetupOkOrExit(setup_status[1]);
    close(setup_status[1]);

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
  close(setup_status[1]);
  try {
    const int setup = ReadSetupStatus(setup_status[0]);
    close(setup_status[0]);
    setup_status[0] = -1;
    if (setup != 0) {
      throw std::runtime_error(std::string("child setup failed: ") +
                               std::strerror(setup));
    }
    if (cgroup) {
      AttachPidToCgroup(*cgroup, pid);
    }
    char token = 'x';
    if (write(gate[1], &token, 1) != 1) {
      throw std::runtime_error("failed to release child start gate");
    }
  } catch (...) {
    close(gate[1]);
    if (setup_status[0] >= 0) {
      close(setup_status[0]);
    }
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

}  // namespace bbp
