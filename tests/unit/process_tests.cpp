#include <fcntl.h>
#include <sched.h>
#include <signal.h>
#include <sys/wait.h>
#include <unistd.h>

#include <array>
#include <boost/test/unit_test.hpp>
#include <cerrno>
#include <charconv>
#include <chrono>
#include <cstring>
#include <filesystem>
#include <optional>
#include <stdexcept>
#include <string>
#include <thread>

#include "bbp/process.h"
#include "bbp/util.h"

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
    throw std::runtime_error(std::string("fork failed: ") +
                             std::strerror(errno));
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
  const ssize_t size =
      readlink(path.c_str(), buffer.data(), buffer.size() - 1U);
  if (size < 0) {
    throw std::runtime_error("readlink failed for " + path + ": " +
                             std::strerror(errno));
  }
  return std::string(buffer.data(), static_cast<size_t>(size));
}

bool WaitForFile(const std::filesystem::path& path,
                 std::chrono::milliseconds timeout) {
  const auto deadline = std::chrono::steady_clock::now() + timeout;
  while (std::chrono::steady_clock::now() < deadline) {
    if (std::filesystem::exists(path)) {
      return true;
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
  }
  return std::filesystem::exists(path);
}

pid_t ReadPid(const std::filesystem::path& path) {
  const std::string text = bbp::ReadText(path);
  pid_t pid = -1;
  const char* const begin = text.data();
  const char* const end = begin + text.size();
  const auto [next, error] = std::from_chars(begin, end, pid);
  if (error != std::errc() || next != end || pid <= 0) {
    throw std::runtime_error("process-tree helper wrote an invalid PID");
  }
  return pid;
}

std::optional<char> ProcessState(pid_t pid) {
  try {
    const std::string stat =
        bbp::ReadText("/proc/" + std::to_string(pid) + "/stat");
    const std::size_t name_end = stat.rfind(')');
    if (name_end == std::string::npos || name_end + 2U >= stat.size() ||
        stat[name_end + 1U] != ' ') {
      throw std::runtime_error("malformed process stat");
    }
    return stat[name_end + 2U];
  } catch (const std::filesystem::filesystem_error&) {
    return std::nullopt;
  } catch (const std::runtime_error&) {
    if (access(("/proc/" + std::to_string(pid)).c_str(), F_OK) != 0 &&
        errno == ENOENT) {
      return std::nullopt;
    }
    throw;
  }
}

bool WaitForProcessGoneOrZombie(pid_t pid, std::chrono::milliseconds timeout) {
  const auto deadline = std::chrono::steady_clock::now() + timeout;
  while (std::chrono::steady_clock::now() < deadline) {
    const std::optional<char> state = ProcessState(pid);
    if (!state || *state == 'Z') {
      return true;
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
  }
  const std::optional<char> state = ProcessState(pid);
  return !state || *state == 'Z';
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

BOOST_AUTO_TEST_CASE(child_process_does_not_inherit_blocked_signal_mask) {
  sigset_t blocked;
  sigset_t previous;
  sigemptyset(&blocked);
  sigaddset(&blocked, SIGTERM);
  BOOST_REQUIRE(pthread_sigmask(SIG_BLOCK, &blocked, &previous) == 0);

  const std::filesystem::path run_dir =
      std::filesystem::temp_directory_path() /
      ("bbp-process-signal-mask-" + std::to_string(getpid()));
  std::filesystem::create_directories(run_dir);

  bbp::ProcessSpec spec;
  spec.binary = "/bin/sleep";
  spec.argv = {"10"};
  spec.cwd = run_dir;
  spec.stdout_path = run_dir / "stdout.log";
  spec.stderr_path = run_dir / "stderr.log";

  bbp::ChildProcess child;
  try {
    child = bbp::ChildProcess::Spawn(spec, std::nullopt);
  } catch (...) {
    static_cast<void>(pthread_sigmask(SIG_SETMASK, &previous, nullptr));
    std::filesystem::remove_all(run_dir);
    throw;
  }
  BOOST_REQUIRE(pthread_sigmask(SIG_SETMASK, &previous, nullptr) == 0);

  child.Terminate(std::chrono::seconds(1));
  const std::optional<int> status = child.exit_status();
  BOOST_REQUIRE(status);
  BOOST_REQUIRE(WIFSIGNALED(*status));
  BOOST_TEST(WTERMSIG(*status) == SIGTERM);
  std::filesystem::remove_all(run_dir);
}

BOOST_AUTO_TEST_CASE(child_process_resolves_relative_binary_before_chdir) {
  const std::filesystem::path run_dir =
      std::filesystem::temp_directory_path() /
      ("bbp-process-relative-binary-" + std::to_string(getpid()));
  std::filesystem::create_directories(run_dir);

  bbp::ProcessSpec spec;
  spec.binary =
      std::filesystem::relative("/bin/true", std::filesystem::current_path());
  spec.cwd = run_dir;
  spec.stdout_path = run_dir / "stdout.log";
  spec.stderr_path = run_dir / "stderr.log";

  bbp::ChildProcess child = bbp::ChildProcess::Spawn(spec, std::nullopt);
  BOOST_REQUIRE(child.WaitForExit(std::chrono::seconds(1)));
  const std::optional<int> status = child.exit_status();
  BOOST_REQUIRE(status);
  BOOST_REQUIRE(WIFEXITED(*status));
  BOOST_TEST(WEXITSTATUS(*status) == 0);

  std::filesystem::remove_all(run_dir);
}

BOOST_AUTO_TEST_CASE(child_process_applies_validated_environment_overrides) {
  const std::filesystem::path run_dir =
      std::filesystem::temp_directory_path() /
      ("bbp-process-environment-" + std::to_string(getpid()));
  std::filesystem::remove_all(run_dir);

  bbp::ProcessSpec spec;
  spec.binary = "/usr/bin/env";
  spec.cwd = run_dir;
  spec.stdout_path = run_dir / "stdout.log";
  spec.stderr_path = run_dir / "stderr.log";
  spec.environment = {{"BBP_PROCESS_TEST_VALUE", "exact inherited value"}};

  bbp::ChildProcess child = bbp::ChildProcess::Spawn(spec, std::nullopt);
  BOOST_REQUIRE(child.WaitForExit(std::chrono::seconds(1)));
  BOOST_REQUIRE(child.exit_status().has_value());
  BOOST_TEST(*child.exit_status() == 0);
  BOOST_TEST(bbp::ReadText(spec.stdout_path)
                 .find("BBP_PROCESS_TEST_VALUE=exact inherited value\n") !=
             std::string::npos);

  spec.environment = {{"DUPLICATE", "first"}, {"DUPLICATE", "second"}};
  BOOST_CHECK_THROW(bbp::ChildProcess::Spawn(spec, std::nullopt),
                    std::runtime_error);
  spec.environment = {{"INVALID=NAME", "value"}};
  BOOST_CHECK_THROW(bbp::ChildProcess::Spawn(spec, std::nullopt),
                    std::runtime_error);

  const std::filesystem::path invalid_run_dir =
      std::filesystem::temp_directory_path() /
      ("bbp-process-invalid-environment-" + std::to_string(getpid()));
  std::filesystem::remove_all(invalid_run_dir);
  spec.cwd = invalid_run_dir;
  spec.stdout_path = invalid_run_dir / "stdout.log";
  spec.stderr_path = invalid_run_dir / "stderr.log";
  spec.environment = {
      {std::string("INVALID\0NAME", 12), "value"},
  };
  BOOST_CHECK_THROW(bbp::ChildProcess::Spawn(spec, std::nullopt),
                    std::runtime_error);
  BOOST_TEST(!std::filesystem::exists(invalid_run_dir));

  std::filesystem::remove_all(run_dir);
}

BOOST_AUTO_TEST_CASE(child_process_termination_signals_complete_process_group) {
  const std::filesystem::path run_dir =
      std::filesystem::temp_directory_path() /
      ("bbp-process-group-" + std::to_string(getpid()));
  std::filesystem::remove_all(run_dir);
  std::filesystem::create_directories(run_dir);
  const std::filesystem::path descendant_pid_path = run_dir / "descendant.pid";
  const std::filesystem::path leader_marker = run_dir / "leader.term";
  const std::filesystem::path descendant_marker = run_dir / "descendant.term";
  const std::filesystem::path helper =
      std::filesystem::canonical("/proc/self/exe").parent_path() /
      "bbp-process-tree-helper";

  bbp::ProcessSpec spec;
  spec.binary = helper;
  spec.argv = {descendant_pid_path.string(), leader_marker.string(),
               descendant_marker.string()};
  spec.cwd = run_dir;
  spec.stdout_path = run_dir / "stdout.log";
  spec.stderr_path = run_dir / "stderr.log";

  bbp::ChildProcess child = bbp::ChildProcess::Spawn(spec, std::nullopt);
  if (!WaitForFile(descendant_pid_path, std::chrono::seconds(2))) {
    child.Kill();
    std::filesystem::remove_all(run_dir);
    BOOST_FAIL("process-tree helper did not report its descendant");
  }
  const pid_t descendant = ReadPid(descendant_pid_path);
  BOOST_REQUIRE_EQUAL(getpgid(descendant), child.pid());

  child.Terminate(std::chrono::seconds(2));
  const bool leader_signalled =
      WaitForFile(leader_marker, std::chrono::seconds(1));
  const bool descendant_signalled =
      WaitForFile(descendant_marker, std::chrono::seconds(1));
  if (!descendant_signalled) {
    kill(descendant, SIGKILL);
  }
  BOOST_TEST(leader_signalled);
  BOOST_TEST(descendant_signalled);
  const std::optional<int> status = child.exit_status();
  BOOST_REQUIRE(status);
  BOOST_REQUIRE(WIFEXITED(*status));
  BOOST_TEST(WEXITSTATUS(*status) == 0);
  std::filesystem::remove_all(run_dir);
}

BOOST_AUTO_TEST_CASE(child_process_move_assignment_refuses_live_owner_loss) {
  const std::filesystem::path run_dir =
      std::filesystem::temp_directory_path() /
      ("bbp-process-move-" + std::to_string(getpid()));
  std::filesystem::remove_all(run_dir);
  std::filesystem::create_directories(run_dir);

  const auto spawn = [&](std::string_view name) {
    bbp::ProcessSpec spec;
    spec.binary = "/bin/sleep";
    spec.argv = {"10"};
    spec.cwd = run_dir;
    spec.stdout_path = run_dir / (std::string(name) + ".out");
    spec.stderr_path = run_dir / (std::string(name) + ".err");
    return bbp::ChildProcess::Spawn(spec, std::nullopt);
  };

  bbp::ChildProcess first = spawn("first");
  bbp::ChildProcess second = spawn("second");
  BOOST_CHECK_THROW(first = std::move(second), std::logic_error);
  BOOST_TEST(first.running());
  BOOST_TEST(second.running());
  first.Kill();
  second.Kill();
  std::filesystem::remove_all(run_dir);
}

BOOST_AUTO_TEST_CASE(child_process_timeout_force_kills_complete_process_group) {
  const std::filesystem::path run_dir =
      std::filesystem::temp_directory_path() /
      ("bbp-process-group-kill-" + std::to_string(getpid()));
  std::filesystem::remove_all(run_dir);
  std::filesystem::create_directories(run_dir);
  const std::filesystem::path descendant_pid_path = run_dir / "descendant.pid";
  const std::filesystem::path helper =
      std::filesystem::canonical("/proc/self/exe").parent_path() /
      "bbp-process-tree-helper";

  bbp::ProcessSpec spec;
  spec.binary = helper;
  spec.argv = {descendant_pid_path.string(), (run_dir / "leader.term").string(),
               (run_dir / "descendant.term").string(), "--ignore-term"};
  spec.cwd = run_dir;
  spec.stdout_path = run_dir / "stdout.log";
  spec.stderr_path = run_dir / "stderr.log";

  bbp::ChildProcess child = bbp::ChildProcess::Spawn(spec, std::nullopt);
  if (!WaitForFile(descendant_pid_path, std::chrono::seconds(2))) {
    child.Kill();
    std::filesystem::remove_all(run_dir);
    BOOST_FAIL("process-tree helper did not report its descendant");
  }
  const pid_t descendant = ReadPid(descendant_pid_path);
  BOOST_REQUIRE_EQUAL(getpgid(descendant), child.pid());

  child.Terminate(std::chrono::milliseconds(50));
  const bool descendant_stopped =
      WaitForProcessGoneOrZombie(descendant, std::chrono::seconds(1));
  if (!descendant_stopped) {
    kill(descendant, SIGKILL);
  }
  BOOST_TEST(descendant_stopped);
  const std::optional<int> status = child.exit_status();
  BOOST_REQUIRE(status);
  BOOST_REQUIRE(WIFSIGNALED(*status));
  BOOST_TEST(WTERMSIG(*status) == SIGKILL);
  std::filesystem::remove_all(run_dir);
}
