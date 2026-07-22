#include <poll.h>
#include <pty.h>
#include <sys/wait.h>
#include <unistd.h>

#include <algorithm>
#include <cerrno>
#include <chrono>
#include <csignal>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <stdexcept>
#include <string>
#include <string_view>
#include <system_error>
#include <thread>
#include <vector>

namespace {

using namespace std::chrono_literals;

class PtyProcess {
 public:
  PtyProcess(const std::filesystem::path& command,
             std::vector<std::string> arguments, unsigned short rows,
             unsigned short cols) {
    struct winsize size {};
    size.ws_row = rows;
    size.ws_col = cols;
    pid_ = forkpty(&master_fd_, nullptr, nullptr, &size);
    if (pid_ < 0) {
      throw std::system_error(errno, std::generic_category(), "forkpty");
    }
    if (pid_ == 0) {
      static_cast<void>(setenv("TERM", "xterm", 1));
      static_cast<void>(setenv("ESCDELAY", "25", 1));
      std::vector<char*> argv;
      argv.reserve(arguments.size() + 2U);
      argv.push_back(const_cast<char*>(command.c_str()));
      for (std::string& argument : arguments) {
        argv.push_back(argument.data());
      }
      argv.push_back(nullptr);
      execv(command.c_str(), argv.data());
      _exit(127);
    }
  }

  PtyProcess(const PtyProcess&) = delete;
  PtyProcess& operator=(const PtyProcess&) = delete;

  ~PtyProcess() {
    if (pid_ > 0) {
      static_cast<void>(kill(pid_, SIGINT));
      const auto deadline = std::chrono::steady_clock::now() + 5s;
      while (pid_ > 0 && std::chrono::steady_clock::now() < deadline) {
        const pid_t result = waitpid(pid_, nullptr, WNOHANG);
        if (result == pid_ || (result < 0 && errno == ECHILD)) {
          pid_ = -1;
          break;
        }
        std::this_thread::sleep_for(10ms);
      }
      if (pid_ > 0) {
        static_cast<void>(kill(pid_, SIGKILL));
        static_cast<void>(waitpid(pid_, nullptr, 0));
      }
    }
    if (master_fd_ >= 0) {
      static_cast<void>(close(master_fd_));
    }
  }

  std::string ReadFor(std::chrono::milliseconds duration) const {
    const auto deadline = std::chrono::steady_clock::now() + duration;
    std::string output;
    while (std::chrono::steady_clock::now() < deadline) {
      const auto remaining =
          std::chrono::duration_cast<std::chrono::milliseconds>(
              deadline - std::chrono::steady_clock::now());
      const int timeout =
          static_cast<int>(std::min<std::int64_t>(remaining.count(), 50));
      pollfd descriptor{
          .fd = master_fd_,
          .events = POLLIN,
          .revents = 0,
      };
      const int poll_result = poll(&descriptor, 1, timeout);
      if (poll_result < 0) {
        if (errno == EINTR) {
          continue;
        }
        throw std::system_error(errno, std::generic_category(), "poll PTY");
      }
      if (poll_result == 0) {
        continue;
      }
      char buffer[8192];
      const ssize_t count = read(master_fd_, buffer, sizeof(buffer));
      if (count > 0) {
        output.append(buffer, static_cast<std::size_t>(count));
      } else if (count == 0 || (count < 0 && errno == EIO)) {
        break;
      } else if (errno != EINTR) {
        throw std::system_error(errno, std::generic_category(), "read PTY");
      }
    }
    return output;
  }

  std::string ReadUntil(std::string_view expected,
                        std::chrono::milliseconds timeout,
                        std::string_view context) const {
    const auto deadline = std::chrono::steady_clock::now() + timeout;
    std::string output;
    while (std::chrono::steady_clock::now() < deadline) {
      output += ReadFor(100ms);
      if (output.find(expected) != std::string::npos) {
        return output;
      }
    }
    throw std::runtime_error(std::string(context) +
                             " did not render: " + std::string(expected));
  }

  void Write(std::string_view input) const {
    std::size_t written = 0U;
    while (written < input.size()) {
      const ssize_t count =
          write(master_fd_, input.data() + written, input.size() - written);
      if (count > 0) {
        written += static_cast<std::size_t>(count);
      } else if (count < 0 && errno != EINTR) {
        throw std::system_error(errno, std::generic_category(), "write PTY");
      }
    }
  }

  bool Running() const {
    return pid_ > 0 && (kill(pid_, 0) == 0 || errno == EPERM);
  }

  int Wait(std::chrono::milliseconds timeout = 10s) {
    const auto deadline = std::chrono::steady_clock::now() + timeout;
    int status = 0;
    while (std::chrono::steady_clock::now() < deadline) {
      const pid_t result = waitpid(pid_, &status, WNOHANG);
      if (result == pid_) {
        pid_ = -1;
        if (WIFEXITED(status)) {
          return WEXITSTATUS(status);
        }
        return 128 + WTERMSIG(status);
      }
      if (result < 0 && errno != EINTR) {
        throw std::system_error(errno, std::generic_category(), "waitpid");
      }
      std::this_thread::sleep_for(10ms);
    }
    throw std::runtime_error("PTY child did not exit before the deadline");
  }

 private:
  pid_t pid_ = -1;
  int master_fd_ = -1;
};

class OwnedTemporaryDirectory {
 public:
  explicit OwnedTemporaryDirectory(std::string_view label) {
    const std::filesystem::path temporary_root =
        std::filesystem::temp_directory_path();
    for (unsigned int attempt = 0U; attempt < 100U; ++attempt) {
      root_ = temporary_root /
              ("bbp-tui-pty-" + std::string(label) + "-" +
               std::to_string(getpid()) + "-" + std::to_string(attempt));
      if (std::filesystem::create_directory(root_)) {
        return;
      }
    }
    throw std::runtime_error("could not create owned PTY test directory");
  }

  OwnedTemporaryDirectory(const OwnedTemporaryDirectory&) = delete;
  OwnedTemporaryDirectory& operator=(const OwnedTemporaryDirectory&) = delete;

  ~OwnedTemporaryDirectory() {
    std::error_code error;
    std::filesystem::remove_all(root_, error);
  }

  const std::filesystem::path& root() const { return root_; }

 private:
  std::filesystem::path root_;
};

class OwnedRunCopy {
 public:
  OwnedRunCopy(const std::filesystem::path& source, std::string_view label)
      : directory_(label), run_root_(directory_.root() / "run") {
    std::filesystem::copy(source, run_root_,
                          std::filesystem::copy_options::recursive);
  }

  const std::filesystem::path& run_root() const { return run_root_; }

  void AppendEvent(std::string_view event) const {
    std::ofstream stream(run_root_ / "events.jsonl", std::ios::app);
    if (!stream) {
      throw std::runtime_error("could not append PTY test event");
    }
    stream << event << '\n';
    if (!stream) {
      throw std::runtime_error("could not flush PTY test event");
    }
  }

 private:
  OwnedTemporaryDirectory directory_;
  std::filesystem::path run_root_;
};

std::string ReadFile(const std::filesystem::path& path) {
  std::ifstream stream(path);
  if (!stream) {
    return {};
  }
  return std::string(std::istreambuf_iterator<char>(stream),
                     std::istreambuf_iterator<char>());
}

std::string WaitForFileText(const std::filesystem::path& path,
                            std::string_view expected,
                            std::chrono::milliseconds timeout) {
  const auto deadline = std::chrono::steady_clock::now() + timeout;
  std::string contents;
  while (std::chrono::steady_clock::now() < deadline) {
    contents = ReadFile(path);
    if (contents.find(expected) != std::string::npos) {
      return contents;
    }
    std::this_thread::sleep_for(20ms);
  }
  throw std::runtime_error("timed out waiting for " + path.string() +
                           " to contain " + std::string(expected));
}

void RequireContains(std::string_view text, std::string_view expected,
                     std::string_view context) {
  if (text.find(expected) == std::string_view::npos) {
    throw std::runtime_error(std::string(context) +
                             " did not contain: " + std::string(expected));
  }
}

void RequireNotContains(std::string_view text, std::string_view unexpected,
                        std::string_view context) {
  if (text.find(unexpected) != std::string_view::npos) {
    throw std::runtime_error(
        std::string(context) +
        " unexpectedly contained: " + std::string(unexpected));
  }
}

void RequireExitZero(PtyProcess* process, std::string_view context) {
  const int result = process->Wait();
  if (result != 0) {
    throw std::runtime_error(std::string(context) + " exited " +
                             std::to_string(result));
  }
}

void CheckCanonicalExitModal(const std::filesystem::path& command,
                             const std::filesystem::path& run_root) {
  PtyProcess process(command, {"--run", run_root.string()}, 24, 80);
  static_cast<void>(process.ReadUntil("Blockchain Benchmark Project TUI", 3s,
                                      "canonical report"));
  process.Write("\x1b");
  const std::string popup =
      process.ReadUntil("Confirm exit", 3s, "canonical exit modal");
  RequireContains(popup, "Press y to exit; n or Esc cancels.",
                  "canonical exit modal");
  process.Write("n");
  if (process.ReadFor(500ms).empty()) {
    throw std::runtime_error("canonical cancel path did not refresh");
  }
  process.Write("\x1b");
  static_cast<void>(
      process.ReadUntil("Confirm exit", 3s, "canonical second exit modal"));
  process.Write("y");
  RequireExitZero(&process, "canonical exit modal");
}

void CorruptNextRefresh(const OwnedRunCopy& run) {
  run.AppendEvent("{malformed-event");
}

void CheckPaletteOnErrorFrame(const std::filesystem::path& command,
                              const std::filesystem::path& source_run) {
  OwnedRunCopy run(source_run, "palette");
  PtyProcess process(command, {"--run", run.run_root().string()}, 30, 100);
  static_cast<void>(process.ReadUntil("Blockchain Benchmark Project TUI", 3s,
                                      "palette report"));
  process.Write("c");
  static_cast<void>(
      process.ReadUntil("Live command", 3s, "palette before report error"));
  CorruptNextRefresh(run);
  static_cast<void>(
      process.ReadUntil("error:", 3s, "palette report-error frame"));
  process.Write("x");
  if (process.ReadFor(500ms).empty()) {
    throw std::runtime_error("palette was not interactive on the error frame");
  }
  process.Write("\x1bq");
  RequireExitZero(&process, "palette report-error frame");
}

void CheckCommandErrorOnErrorFrame(const std::filesystem::path& command,
                                   const std::filesystem::path& source_run) {
  OwnedRunCopy run(source_run, "command-error");
  PtyProcess process(command, {"--run", run.run_root().string()}, 30, 100);
  static_cast<void>(process.ReadUntil("Blockchain Benchmark Project TUI", 3s,
                                      "command-error report"));
  run.AppendEvent(
      "{\"run_id\":\"tui-fixture\",\"node_id\":\"firo-1\","
      "\"timestamp\":\"2026-07-22T00:00:00Z\","
      "\"event\":\"operator_command_failed\","
      "\"detail\":\"{\\\"sequence\\\":9001,"
      "\\\"kind\\\":\\\"kill\\\","
      "\\\"error\\\":\\\"forced command failure\\\"}\"}");
  static_cast<void>(process.ReadUntil("Command error", 3s,
                                      "command error before report error"));
  CorruptNextRefresh(run);
  static_cast<void>(
      process.ReadUntil("error:", 3s, "command-error report-error frame"));
  process.Write("\x1bq");
  RequireExitZero(&process, "command-error report-error frame");
}

pid_t ProcessStartedPid(std::string_view events) {
  constexpr std::string_view marker = "\\\"pid\\\":";
  const std::size_t process_event =
      events.rfind("\"event\":\"process_started\"");
  const std::size_t pid_field = events.find(marker, process_event);
  if (process_event == std::string_view::npos ||
      pid_field == std::string_view::npos) {
    throw std::runtime_error("process_started event did not contain a pid");
  }
  const std::size_t begin = pid_field + marker.size();
  const std::size_t end = events.find_first_not_of("0123456789", begin);
  const std::string text(events.substr(begin, end - begin));
  const long parsed = std::stol(text);
  if (parsed <= 0) {
    throw std::runtime_error("process_started pid was not positive");
  }
  return static_cast<pid_t>(parsed);
}

bool ProcessExists(pid_t pid) { return kill(pid, 0) == 0 || errno == EPERM; }

void WaitForProcessExit(pid_t pid, std::chrono::milliseconds timeout) {
  const auto deadline = std::chrono::steady_clock::now() + timeout;
  while (std::chrono::steady_clock::now() < deadline) {
    if (!ProcessExists(pid)) {
      return;
    }
    std::this_thread::sleep_for(20ms);
  }
  throw std::runtime_error("active-run daemon survived confirmed TUI exit");
}

void WriteActiveScenario(const std::filesystem::path& path) {
  std::ofstream stream(path);
  if (!stream) {
    throw std::runtime_error("could not create active-run scenario");
  }
  stream << R"({
  "simulation": {
    "duration": "120s",
    "metrics_interval": "100ms"
  },
  "nodes": [
    {
      "id": "firo-active",
      "chain": "firo",
      "role": "base"
    }
  ],
  "block_production": {
    "enabled": false
  },
  "ready_timeout_sec": 120
})";
  if (!stream) {
    throw std::runtime_error("could not write active-run scenario");
  }
}

void AppendMalformedEvent(const std::filesystem::path& events_path) {
  std::ofstream stream(events_path, std::ios::app);
  if (!stream) {
    throw std::runtime_error("could not append active-run malformed event");
  }
  stream << "{malformed-event\n";
  if (!stream) {
    throw std::runtime_error("could not flush active-run malformed event");
  }
}

void CheckActiveRunLifecycle(const std::filesystem::path& command,
                             const std::filesystem::path& daemon) {
  OwnedTemporaryDirectory directory("active");
  const std::filesystem::path scenario = directory.root() / "scenario.json";
  const std::filesystem::path benchmark_root = directory.root() / "runs";
  const std::string run_id = "tui-active-" + std::to_string(getpid());
  const std::filesystem::path run_root = benchmark_root / run_id;
  const std::filesystem::path events_path = run_root / "events.jsonl";
  WriteActiveScenario(scenario);

  PtyProcess process(
      command,
      {"--scenario", scenario.string(), "--node-binary", daemon.string(),
       "--benchmark-root", benchmark_root.string(), "--run-id", run_id,
       "--refresh-ms", "50"},
      30, 100);
  static_cast<void>(process.ReadUntil("Blockchain Benchmark Project TUI", 5s,
                                      "active benchmark"));
  const std::string startup_events =
      WaitForFileText(events_path, "\"event\":\"process_started\"", 10s);
  const pid_t daemon_pid = ProcessStartedPid(startup_events);
  if (!process.Running() || !ProcessExists(daemon_pid)) {
    throw std::runtime_error("active benchmark was not running before Esc");
  }

  std::this_thread::sleep_for(200ms);
  static_cast<void>(process.ReadFor(100ms));
  process.Write("c");
  static_cast<void>(
      process.ReadUntil("Live command", 3s, "active destructive palette"));
  process.Write("kill\n");
  static_cast<void>(process.ReadUntil("Confirm destructive action", 3s,
                                      "active destructive confirmation"));
  AppendMalformedEvent(events_path);
  static_cast<void>(process.ReadUntil(
      "error:", 3s, "active destructive confirmation error frame"));
  process.Write("n");
  static_cast<void>(process.ReadFor(100ms));

  static_cast<void>(process.ReadFor(100ms));
  process.Write("\x1b");
  static_cast<void>(
      process.ReadUntil("Confirm exit", 3s, "active-run exit modal"));
  process.Write("n");
  if (process.ReadFor(500ms).empty()) {
    throw std::runtime_error("active-run cancel path did not refresh");
  }
  std::this_thread::sleep_for(150ms);
  const std::string after_cancel = ReadFile(events_path);
  RequireNotContains(after_cancel, "\"event\":\"run_cancelled\"",
                     "active-run cancel path");
  RequireNotContains(after_cancel, "\"event\":\"run_finished\"",
                     "active-run cancel path");
  if (!process.Running() || !ProcessExists(daemon_pid)) {
    throw std::runtime_error("Esc,n stopped the active worker or its daemon");
  }

  process.Write("\x1b");
  static_cast<void>(
      process.ReadUntil("Confirm exit", 3s, "active-run confirmed exit modal"));
  process.Write("y");
  RequireExitZero(&process, "active RunBenchmarkWithTui exit");
  const std::string finished_events =
      WaitForFileText(events_path, "\"event\":\"run_finished\"", 3s);
  RequireContains(finished_events, "\"event\":\"run_cancelled\"",
                  "active-run confirmed exit");
  WaitForProcessExit(daemon_pid, 3s);
}

int RunIdleDaemon() {
  while (true) {
    pause();
  }
}

}  // namespace

int main(int argc, char** argv) {
  if (argc > 1 && argv[1][0] == '-') {
    return RunIdleDaemon();
  }
  if (argc != 4) {
    std::cerr << "usage: " << argv[0]
              << " BBP LIVE_RUN_ROOT COMPLETE_RUN_ROOT\n";
    return 2;
  }
  try {
    const std::filesystem::path command = argv[1];
    const std::filesystem::path live_run = argv[2];
    const std::filesystem::path complete_run = argv[3];
    CheckCanonicalExitModal(command, live_run);
    CheckPaletteOnErrorFrame(command, complete_run);
    CheckCommandErrorOnErrorFrame(command, complete_run);
    CheckActiveRunLifecycle(command, std::filesystem::canonical(argv[0]));
    std::cout << "canonical modal, shared error-frame overlays, and active "
                 "RunBenchmarkWithTui lifecycle checks passed\n";
    return 0;
  } catch (const std::exception& error) {
    std::cerr << "tui PTY regression failed: " << error.what() << '\n';
    return 1;
  }
}
