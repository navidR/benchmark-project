#include <fcntl.h>
#include <poll.h>
#include <sched.h>
#include <signal.h>
#include <sys/prctl.h>
#include <sys/resource.h>
#include <sys/wait.h>
#include <unistd.h>

#include <array>
#include <boost/test/unit_test.hpp>
#include <cerrno>
#include <charconv>
#include <chrono>
#include <cstring>
#include <filesystem>
#include <future>
#include <optional>
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>

#include "../../src/owned_process_spawn.h"
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

class Subreaper {
 public:
  Subreaper() {
    if (prctl(PR_GET_CHILD_SUBREAPER, &previous_) != 0 ||
        prctl(PR_SET_CHILD_SUBREAPER, 1) != 0) {
      throw std::runtime_error("set process test subreaper");
    }
  }
  ~Subreaper() { static_cast<void>(prctl(PR_SET_CHILD_SUBREAPER, previous_)); }

 private:
  int previous_ = 0;
};

class OwnedTestPid {
 public:
  explicit OwnedTestPid(pid_t pid) : pid_(pid) {}
  ~OwnedTestPid() {
    if (pid_ > 0) {
      static_cast<void>(kill(pid_, SIGKILL));
      while (waitpid(pid_, nullptr, 0) < 0 && errno == EINTR) {
      }
    }
  }
  int Wait() {
    const auto deadline =
        std::chrono::steady_clock::now() + std::chrono::seconds(2);
    while (std::chrono::steady_clock::now() < deadline) {
      int status = 0;
      if (waitpid(pid_, &status, WNOHANG) == pid_) {
        pid_ = -1;
        return status;
      }
      std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
    throw std::runtime_error(
        "owned test child did not exit within two seconds");
  }

 private:
  pid_t pid_;
};

int parent_death_ready = -1;
int parent_death_release = -1;

void DelayParentDeathInstallation() {
  const pid_t pid = getpid();
  if (write(parent_death_ready, &pid, sizeof(pid)) != sizeof(pid)) {
    _exit(125);
  }
  char token;
  if (read(parent_death_release, &token, 1) != 1) {
    _exit(126);
  }
}

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

BOOST_AUTO_TEST_CASE(
    child_process_nonblocking_signal_requests_share_one_shutdown_deadline) {
  const std::filesystem::path run_dir =
      std::filesystem::temp_directory_path() /
      ("bbp-process-common-deadline-" + std::to_string(getpid()));
  std::filesystem::remove_all(run_dir);
  std::filesystem::create_directories(run_dir);

  constexpr std::size_t kProcessCount = 6U;
  std::vector<bbp::ChildProcess> children;
  children.reserve(kProcessCount);
  for (std::size_t index = 0; index < kProcessCount; ++index) {
    bbp::ProcessSpec spec;
    spec.binary = "/bin/sleep";
    spec.argv = {"10"};
    spec.cwd = run_dir;
    spec.stdout_path = run_dir / (std::to_string(index) + ".out");
    spec.stderr_path = run_dir / (std::to_string(index) + ".err");
    children.push_back(bbp::ChildProcess::Spawn(spec, std::nullopt));
  }

  const auto request_started = std::chrono::steady_clock::now();
  for (bbp::ChildProcess& child : children) {
    BOOST_REQUIRE(child.RequestTerminate());
  }
  const auto request_elapsed =
      std::chrono::steady_clock::now() - request_started;
  BOOST_TEST(
      std::chrono::duration_cast<std::chrono::milliseconds>(request_elapsed)
          .count() < 2000);

  const auto common_deadline =
      std::chrono::steady_clock::now() + std::chrono::seconds(2);
  for (bbp::ChildProcess& child : children) {
    const auto now = std::chrono::steady_clock::now();
    if (now < common_deadline) {
      static_cast<void>(child.WaitForExit(
          std::chrono::duration_cast<std::chrono::milliseconds>(
              common_deadline - now)));
    }
  }
  for (bbp::ChildProcess& child : children) {
    if (child.running()) {
      static_cast<void>(child.RequestKill());
    }
  }
  const auto kill_deadline =
      std::chrono::steady_clock::now() + std::chrono::seconds(2);
  for (bbp::ChildProcess& child : children) {
    const auto now = std::chrono::steady_clock::now();
    if (now < kill_deadline) {
      static_cast<void>(child.WaitForExit(
          std::chrono::duration_cast<std::chrono::milliseconds>(kill_deadline -
                                                                now)));
    }
    BOOST_TEST(!child.running());
  }
  const auto total_elapsed = std::chrono::steady_clock::now() - request_started;
  BOOST_TEST(
      std::chrono::duration_cast<std::chrono::milliseconds>(total_elapsed)
          .count() < 5000);
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

BOOST_AUTO_TEST_CASE(child_process_survives_launching_thread_exit) {
  const auto root = std::filesystem::temp_directory_path() /
                    ("bbp-process-thread-" + std::to_string(getpid()));
  bbp::ProcessSpec spec;
  spec.binary = "/bin/sleep";
  spec.argv = {"30"};
  spec.stdout_path = root / "stdout.log";
  spec.stderr_path = root / "stderr.log";
  bbp::ChildProcess child =
      std::async(std::launch::async, [&] {
        return bbp::ChildProcess::Spawn(spec, std::nullopt);
      }).get();
  const bool exited = child.WaitForExit(std::chrono::milliseconds(50));
  child.Terminate(std::chrono::seconds(1));
  BOOST_TEST(!exited);
  std::filesystem::remove_all(root);
}

BOOST_AUTO_TEST_CASE(child_process_tree_dies_after_owner_kill_or_crash) {
  Subreaper subreaper;
  for (const int signal : {SIGKILL, SIGSEGV}) {
    const auto root = std::filesystem::temp_directory_path() /
                      ("bbp-parent-death-" + std::to_string(getpid()) + "-" +
                       std::to_string(signal));
    const auto descendant_path = root / "descendant.pid";
    bbp::ProcessSpec spec;
    spec.binary = std::filesystem::canonical("/proc/self/exe").parent_path() /
                  "bbp-process-tree-helper";
    spec.argv = {descendant_path.string(), (root / "leader.term").string(),
                 (root / "descendant.term").string(), "--ignore-term"};
    spec.stdout_path = root / "stdout.log";
    spec.stderr_path = root / "stderr.log";
    int ready[2];
    BOOST_REQUIRE(pipe2(ready, O_CLOEXEC) == 0);
    UniqueFd ready_read(ready[0]);
    UniqueFd ready_write(ready[1]);
    const pid_t owner_pid = fork();
    BOOST_REQUIRE(owner_pid >= 0);
    if (owner_pid == 0) {
      ready_read.Reset();
      static_cast<void>(::signal(SIGSEGV, SIG_DFL));
      const rlimit core_limit{0U, 0U};
      if (setrlimit(RLIMIT_CORE, &core_limit) != 0) {
        _exit(125);
      }
      const bbp::ChildProcess child =
          bbp::ChildProcess::Spawn(spec, std::nullopt);
      WriteStatus(ready_write.get(), child.pid());
      for (;;) {
        pause();
      }
    }
    OwnedTestPid owner_guard(owner_pid);
    ready_write.Reset();
    pollfd descriptor{.fd = ready_read.get(), .events = POLLIN, .revents = 0};
    BOOST_REQUIRE(poll(&descriptor, 1, 2000) == 1);
    const pid_t child_pid = ReadStatus(ready_read.get());
    OwnedTestPid child_guard(child_pid);
    BOOST_REQUIRE(WaitForFile(descendant_path, std::chrono::seconds(2)));
    OwnedTestPid descendant_guard(ReadPid(descendant_path));
    BOOST_REQUIRE(kill(owner_pid, signal) == 0);
    const int owner_status = owner_guard.Wait();
    BOOST_REQUIRE(WIFSIGNALED(owner_status));
    BOOST_TEST(WTERMSIG(owner_status) == signal);
    for (OwnedTestPid* child : {&child_guard, &descendant_guard}) {
      const int status = child->Wait();
      BOOST_REQUIRE(WIFSIGNALED(status));
      BOOST_TEST(WTERMSIG(status) == SIGKILL);
    }
    std::filesystem::remove_all(root);
  }
}

BOOST_AUTO_TEST_CASE(child_process_refuses_parent_death_before_installation) {
  Subreaper subreaper;
  int ready[2];
  int release[2];
  BOOST_REQUIRE(pipe2(ready, O_CLOEXEC) == 0);
  UniqueFd ready_read(ready[0]);
  UniqueFd ready_write(ready[1]);
  BOOST_REQUIRE(pipe2(release, O_CLOEXEC) == 0);
  UniqueFd release_read(release[0]);
  UniqueFd release_write(release[1]);
  const pid_t owner_pid = fork();
  BOOST_REQUIRE(owner_pid >= 0);
  if (owner_pid == 0) {
    ready_read.Reset();
    release_write.Reset();
    parent_death_ready = ready_write.get();
    parent_death_release = release_read.get();
    bbp::SetOwnedForkBeforeParentDeathHookForTest(DelayParentDeathInstallation);
    static_cast<void>(bbp::ForkOwnedProcess([] { _exit(99); }));
    _exit(124);
  }
  OwnedTestPid owner_guard(owner_pid);
  ready_write.Reset();
  release_read.Reset();
  pollfd descriptor{.fd = ready_read.get(), .events = POLLIN, .revents = 0};
  BOOST_REQUIRE(poll(&descriptor, 1, 2000) == 1);
  OwnedTestPid child_guard(ReadStatus(ready_read.get()));
  BOOST_REQUIRE(kill(owner_pid, SIGKILL) == 0);
  static_cast<void>(owner_guard.Wait());
  BOOST_REQUIRE(write(release_write.get(), "x", 1) == 1);
  const int status = child_guard.Wait();
  BOOST_REQUIRE(WIFEXITED(status));
  BOOST_TEST(WEXITSTATUS(status) == 127);
}

BOOST_AUTO_TEST_CASE(process_signal_names_numbers_and_realtime_bounds) {
  BOOST_TEST(bbp::ParseProcessSignal("SIGTERM") == SIGTERM);
  BOOST_TEST(bbp::ParseProcessSignal(std::to_string(SIGINT)) == SIGINT);
  BOOST_TEST(bbp::ParseProcessSignal("SIGPOLL") == SIGIO);
  BOOST_TEST(bbp::ParseProcessSignal("SIGRTMIN") == SIGRTMIN);
  BOOST_TEST(bbp::ParseProcessSignal("SIGRTMAX") == SIGRTMAX);
  BOOST_TEST(bbp::ParseProcessSignal("SIGRTMIN+1") == SIGRTMIN + 1);
  BOOST_TEST(bbp::ParseProcessSignal("SIGRTMAX-1") == SIGRTMAX - 1);
  BOOST_TEST(bbp::ParseProcessSignal(bbp::ProcessSignalName(SIGRTMAX - 1)) ==
             SIGRTMAX - 1);
  for (const std::string& value :
       {std::string{}, std::string("SIGUNKNOWN"), std::string("15x"),
        std::string(" 15"), std::string("-1"), std::string("0"),
        std::to_string(NSIG),
        "SIGRTMIN+" + std::to_string(SIGRTMAX - SIGRTMIN + 1),
        std::string("SIGRTMAX-999999999999999999999")}) {
    BOOST_CHECK_THROW(bbp::ParseProcessSignal(value), std::invalid_argument);
  }
}

BOOST_AUTO_TEST_CASE(child_process_signal_scope_excludes_foreign_processes) {
  Subreaper subreaper;
  const auto root = std::filesystem::temp_directory_path() /
                    ("bbp-process-signals-" + std::to_string(getpid()));
  const auto descendant_path = root / "descendant.pid";
  bbp::ProcessSpec spec;
  spec.binary = std::filesystem::canonical("/proc/self/exe").parent_path() /
                "bbp-process-tree-helper";
  spec.argv = {descendant_path.string(), (root / "leader.term").string(),
               (root / "descendant.term").string(), "--ignore-term"};
  spec.stdout_path = root / "stdout.log";
  spec.stderr_path = root / "stderr.log";
  bbp::ChildProcess child = bbp::ChildProcess::Spawn(spec, std::nullopt);
  try {
    BOOST_REQUIRE(WaitForFile(descendant_path, std::chrono::seconds(2)));
    const pid_t descendant = ReadPid(descendant_path);
    OwnedTestPid descendant_guard(descendant);
    const pid_t foreign = fork();
    BOOST_REQUIRE(foreign >= 0);
    if (foreign == 0) {
      const int joined = setpgid(0, child.pid());
      _exit(joined < 0 && errno == EPERM ? 0 : 1);
    }
    OwnedTestPid foreign_guard(foreign);
    const int foreign_status = foreign_guard.Wait();
    BOOST_REQUIRE(WIFEXITED(foreign_status));
    BOOST_TEST(WEXITSTATUS(foreign_status) == 0);

    const auto deliver = [&](int signal, bbp::ProcessSignalScope scope) {
      const auto result = child.DeliverSignal(signal, scope);
      BOOST_TEST(result.target_pid == child.pid());
      BOOST_TEST(result.process_group_id == child.pid());
      BOOST_TEST(result.signal == signal);
      BOOST_TEST(static_cast<int>(result.scope) == static_cast<int>(scope));
      BOOST_REQUIRE(result.kernel_result == 0);
      BOOST_TEST(result.error_number == 0);
    };
    const auto wait_stopped = [](pid_t pid, bool stopped) {
      const auto deadline =
          std::chrono::steady_clock::now() + std::chrono::seconds(2);
      do {
        const auto state = ProcessState(pid);
        if (state && *state != 'Z' && (*state == 'T') == stopped) {
          return true;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
      } while (std::chrono::steady_clock::now() < deadline);
      return false;
    };
    using Scope = bbp::ProcessSignalScope;
    BOOST_CHECK_THROW(child.DeliverSignal(0, Scope::kProcess),
                      std::invalid_argument);
    BOOST_CHECK_THROW(child.DeliverSignal(SIGTERM, static_cast<Scope>(99)),
                      std::invalid_argument);
    deliver(SIGWINCH, Scope::kProcess);
    BOOST_TEST(child.running());
    deliver(SIGSTOP, Scope::kProcess);
    BOOST_REQUIRE(wait_stopped(child.pid(), true));
    BOOST_REQUIRE(wait_stopped(descendant, false));
    deliver(SIGCONT, Scope::kProcess);
    BOOST_REQUIRE(wait_stopped(child.pid(), false));
    deliver(SIGSTOP, Scope::kProcessGroup);
    BOOST_REQUIRE(wait_stopped(child.pid(), true));
    BOOST_REQUIRE(wait_stopped(descendant, true));
    deliver(SIGCONT, Scope::kProcessGroup);
    BOOST_REQUIRE(wait_stopped(child.pid(), false));
    BOOST_REQUIRE(wait_stopped(descendant, false));
    deliver(SIGKILL, Scope::kProcessGroup);
    BOOST_REQUIRE(child.WaitForExit(std::chrono::seconds(2)));
    BOOST_REQUIRE(child.exit_status());
    BOOST_REQUIRE(WIFSIGNALED(*child.exit_status()));
    BOOST_TEST(WTERMSIG(*child.exit_status()) == SIGKILL);
    const int descendant_status = descendant_guard.Wait();
    BOOST_REQUIRE(WIFSIGNALED(descendant_status));
    BOOST_TEST(WTERMSIG(descendant_status) == SIGKILL);
    BOOST_CHECK_THROW(child.DeliverSignal(SIGCONT, Scope::kProcessGroup),
                      std::system_error);
  } catch (...) {
    child.Kill();
    throw;
  }
  std::filesystem::remove_all(root);
}

BOOST_AUTO_TEST_CASE(
    child_process_signal_requires_current_owner_and_reports_exit) {
  const auto root = std::filesystem::temp_directory_path() /
                    ("bbp-process-signal-owner-" + std::to_string(getpid()));
  bbp::ProcessSpec spec;
  spec.binary = "/bin/sleep";
  spec.argv = {"30"};
  spec.stdout_path = root / "stdout.log";
  spec.stderr_path = root / "stderr.log";
  bbp::ChildProcess previous = bbp::ChildProcess::Spawn(spec, std::nullopt);
  bbp::ChildProcess owner = std::move(previous);
  try {
    BOOST_CHECK_THROW(
        previous.DeliverSignal(SIGKILL, bbp::ProcessSignalScope::kProcess),
        std::system_error);
    BOOST_TEST(owner.running());
    const auto result =
        owner.DeliverSignal(SIGTERM, bbp::ProcessSignalScope::kProcess);
    BOOST_TEST(result.target_pid == owner.pid());
    BOOST_TEST(result.kernel_result == 0);
    BOOST_TEST(result.error_number == 0);
    BOOST_REQUIRE(owner.WaitForExit(std::chrono::seconds(2)));
    BOOST_REQUIRE(owner.exit_status());
    BOOST_REQUIRE(WIFSIGNALED(*owner.exit_status()));
    BOOST_TEST(WTERMSIG(*owner.exit_status()) == SIGTERM);
  } catch (...) {
    owner.Kill();
    throw;
  }
  std::filesystem::remove_all(root);
}
