#include "bbp/process.h"

#include <fcntl.h>
#include <sched.h>
#include <signal.h>
#include <sys/wait.h>
#include <unistd.h>

#include <array>
#include <cerrno>
#include <chrono>
#include <cstring>
#include <filesystem>
#include <optional>
#include <stdexcept>
#include <string>

#include <boost/test/unit_test.hpp>

namespace {

class UniqueFd {
 public:
  UniqueFd() = default;
  explicit UniqueFd(int fd) : fd_(fd) {}

  UniqueFd(const UniqueFd&) = delete;
  UniqueFd& operator=(const UniqueFd&) = delete;

  UniqueFd(UniqueFd&& other) noexcept { *this = std::move(other); }
  UniqueFd& operator=(UniqueFd&& other) noexcept {
    if (this == &other) {
      return *this;
    }
    Reset();
    fd_ = other.fd_;
    other.fd_ = -1;
    return *this;
  }

  ~UniqueFd() { Reset(); }

  int get() const { return fd_; }

  void Reset() {
    if (fd_ >= 0) {
      close(fd_);
      fd_ = -1;
    }
  }

 private:
  int fd_ = -1;
};

class NetworkNamespaceHelper {
 public:
  NetworkNamespaceHelper(pid_t pid, UniqueFd netns)
      : pid_(pid), netns_(std::move(netns)) {}

  NetworkNamespaceHelper(const NetworkNamespaceHelper&) = delete;
  NetworkNamespaceHelper& operator=(const NetworkNamespaceHelper&) = delete;
  NetworkNamespaceHelper(NetworkNamespaceHelper&& other) noexcept {
    *this = std::move(other);
  }
  NetworkNamespaceHelper& operator=(NetworkNamespaceHelper&& other) noexcept {
    if (this == &other) {
      return *this;
    }
    Stop();
    pid_ = other.pid_;
    netns_ = std::move(other.netns_);
    other.pid_ = -1;
    return *this;
  }

  ~NetworkNamespaceHelper() { Stop(); }

  pid_t pid() const { return pid_; }
  int netns_fd() const { return netns_.get(); }

 private:
  void Stop() {
    if (pid_ > 0) {
      kill(pid_, SIGKILL);
      int status = 0;
      while (waitpid(pid_, &status, 0) < 0 && errno == EINTR) {
      }
      pid_ = -1;
    }
  }

  pid_t pid_ = -1;
  UniqueFd netns_;
};

void WriteStatus(int fd, int status) {
  const char* cursor = reinterpret_cast<const char*>(&status);
  size_t remaining = sizeof(status);
  while (remaining != 0U) {
    const ssize_t written = write(fd, cursor, remaining);
    if (written < 0) {
      if (errno == EINTR) {
        continue;
      }
      _exit(127);
    }
    cursor += written;
    remaining -= static_cast<size_t>(written);
  }
}

int ReadStatus(int fd) {
  int status = 0;
  char* cursor = reinterpret_cast<char*>(&status);
  size_t remaining = sizeof(status);
  while (remaining != 0U) {
    const ssize_t received = read(fd, cursor, remaining);
    if (received < 0) {
      if (errno == EINTR) {
        continue;
      }
      throw std::runtime_error(std::string("read failed: ") +
                               std::strerror(errno));
    }
    if (received == 0) {
      throw std::runtime_error("helper exited before writing status");
    }
    cursor += received;
    remaining -= static_cast<size_t>(received);
  }
  return status;
}

std::optional<NetworkNamespaceHelper> StartHelper() {
  int pipe_fds[2];
  if (pipe2(pipe_fds, O_CLOEXEC) != 0) {
    throw std::runtime_error(std::string("pipe2 failed: ") +
                             std::strerror(errno));
  }
  UniqueFd read_end(pipe_fds[0]);
  UniqueFd write_end(pipe_fds[1]);

  const pid_t pid = fork();
  if (pid < 0) {
    throw std::runtime_error(std::string("fork failed: ") + std::strerror(errno));
  }

  if (pid == 0) {
    read_end.Reset();
    int status = 0;
    if (unshare(CLONE_NEWNET) != 0) {
      status = errno;
    }
    WriteStatus(write_end.get(), status);
    if (status != 0) {
      _exit(127);
    }
    while (true) {
      pause();
    }
  }

  write_end.Reset();
  const int status = ReadStatus(read_end.get());
  read_end.Reset();
  if (status == EPERM) {
    int wait_status = 0;
    while (waitpid(pid, &wait_status, 0) < 0 && errno == EINTR) {
    }
    return std::nullopt;
  }
  if (status != 0) {
    int wait_status = 0;
    while (waitpid(pid, &wait_status, 0) < 0 && errno == EINTR) {
    }
    throw std::runtime_error(std::string("unshare failed: ") +
                             std::strerror(status));
  }

  const std::string path = "/proc/" + std::to_string(pid) + "/ns/net";
  UniqueFd netns(open(path.c_str(), O_RDONLY | O_CLOEXEC));
  if (netns.get() < 0) {
    kill(pid, SIGKILL);
    int wait_status = 0;
    while (waitpid(pid, &wait_status, 0) < 0 && errno == EINTR) {
    }
    throw std::runtime_error("open failed for " + path + ": " +
                             std::strerror(errno));
  }

  return NetworkNamespaceHelper(pid, std::move(netns));
}

std::string ReadLink(const std::string& path) {
  std::array<char, 256> buffer{};
  const ssize_t size = readlink(path.c_str(), buffer.data(), buffer.size() - 1U);
  if (size < 0) {
    throw std::runtime_error("readlink failed for " + path + ": " +
                             std::strerror(errno));
  }
  return std::string(buffer.data(), static_cast<size_t>(size));
}

}  // namespace

BOOST_AUTO_TEST_CASE(child_process_enters_configured_network_namespace) {
  std::optional<NetworkNamespaceHelper> helper = StartHelper();
  if (!helper) {
    BOOST_TEST_MESSAGE("skipping child netns test: unshare requires privilege");
    return;
  }

  const std::filesystem::path run_dir =
      std::filesystem::temp_directory_path() /
      ("bbp-process-netns-" + std::to_string(getpid()));
  std::filesystem::create_directories(run_dir);

  bbp::ProcessSpec spec;
  spec.binary = "/bin/sleep";
  spec.argv = {"2"};
  spec.cwd = run_dir;
  spec.stdout_path = run_dir / "stdout.log";
  spec.stderr_path = run_dir / "stderr.log";
  spec.network_namespace_fd = helper->netns_fd();

  bbp::ChildProcess child = bbp::ChildProcess::Spawn(spec, std::nullopt);
  const std::string helper_netns =
      ReadLink("/proc/" + std::to_string(helper->pid()) + "/ns/net");
  const std::string child_netns =
      ReadLink("/proc/" + std::to_string(child.pid()) + "/ns/net");

  BOOST_TEST(child_netns == helper_netns);

  child.Terminate(std::chrono::seconds(1));
  std::filesystem::remove_all(run_dir);
}
