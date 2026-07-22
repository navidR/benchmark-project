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
#include <set>
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

std::size_t CountOccurrences(std::string_view text, std::string_view expected) {
  std::size_t count = 0U;
  std::size_t offset = 0U;
  while ((offset = text.find(expected, offset)) != std::string_view::npos) {
    ++count;
    offset += expected.size();
  }
  return count;
}

std::string WaitForFileOccurrences(const std::filesystem::path& path,
                                   std::string_view expected,
                                   std::size_t minimum_count,
                                   std::chrono::milliseconds timeout) {
  const auto deadline = std::chrono::steady_clock::now() + timeout;
  std::string contents;
  while (std::chrono::steady_clock::now() < deadline) {
    contents = ReadFile(path);
    if (CountOccurrences(contents, expected) >= minimum_count) {
      return contents;
    }
    std::this_thread::sleep_for(20ms);
  }
  throw std::runtime_error("timed out waiting for " + path.string() +
                           " occurrence count for " + std::string(expected));
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

std::vector<pid_t> EventProcessPids(std::string_view events) {
  constexpr std::string_view event_marker = "\"event\":\"process_started\"";
  constexpr std::string_view pid_marker = "\\\"pid\\\":";
  std::vector<pid_t> pids;
  std::size_t offset = 0U;
  while ((offset = events.find(event_marker, offset)) !=
         std::string_view::npos) {
    const std::size_t line_end = events.find('\n', offset);
    const std::size_t pid_field = events.find(pid_marker, offset);
    if (pid_field == std::string_view::npos ||
        (line_end != std::string_view::npos && pid_field >= line_end)) {
      throw std::runtime_error("process_started event did not contain a pid");
    }
    const std::size_t begin = pid_field + pid_marker.size();
    const std::size_t end = events.find_first_not_of("0123456789", begin);
    const long parsed =
        std::stol(std::string(events.substr(begin, end - begin)));
    if (parsed <= 0) {
      throw std::runtime_error("process_started pid was not positive");
    }
    pids.push_back(static_cast<pid_t>(parsed));
    offset = line_end == std::string_view::npos ? events.size() : line_end + 1U;
  }
  return pids;
}

std::vector<pid_t> MetricNamespacePids(std::string_view metrics) {
  constexpr std::string_view marker = "\"network_namespace_helper_pid\":";
  std::set<pid_t> unique_pids;
  std::size_t offset = 0U;
  while ((offset = metrics.find(marker, offset)) != std::string_view::npos) {
    const std::size_t begin = offset + marker.size();
    if (metrics.substr(begin, 4U) != "null") {
      const std::size_t end = metrics.find_first_not_of("0123456789", begin);
      const long parsed =
          std::stol(std::string(metrics.substr(begin, end - begin)));
      if (parsed > 0) {
        unique_pids.insert(static_cast<pid_t>(parsed));
      }
    }
    offset = begin;
  }
  return {unique_pids.begin(), unique_pids.end()};
}

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

std::string MarkerResourceId(const std::filesystem::path& run_root) {
  const std::string marker = ReadFile(run_root / ".bbp-run");
  constexpr std::string_view field = "\"resource_id\":\"";
  const std::size_t begin_field = marker.find(field);
  if (begin_field == std::string::npos) {
    throw std::runtime_error("run ownership marker has no resource id");
  }
  const std::size_t begin = begin_field + field.size();
  const std::size_t end = marker.find('"', begin);
  if (end == std::string::npos || end - begin != 32U) {
    throw std::runtime_error("run ownership marker resource id is invalid");
  }
  return marker.substr(begin, end - begin);
}

void RequireNoRpcCredentials(const std::filesystem::path& run_root) {
  for (const auto& entry :
       std::filesystem::recursive_directory_iterator(run_root)) {
    if (entry.path().filename() == ".bbp-rpc-cookie") {
      throw std::runtime_error("RPC credential survived run cleanup: " +
                               entry.path().string());
    }
  }
}

void RequireOwnedResourcesRemoved(const std::filesystem::path& run_root,
                                  std::uint32_t node_count,
                                  const std::vector<pid_t>& daemon_pids,
                                  const std::vector<pid_t>& namespace_pids) {
  for (const pid_t pid : daemon_pids) {
    WaitForProcessExit(pid, 5s);
  }
  for (const pid_t pid : namespace_pids) {
    WaitForProcessExit(pid, 5s);
  }
  const std::string resource_id = MarkerResourceId(run_root);
  const std::filesystem::path cgroup =
      std::filesystem::path("/sys/fs/cgroup/bbp") / resource_id;
  if (std::filesystem::exists(cgroup)) {
    throw std::runtime_error("owned run cgroup survived cleanup: " +
                             cgroup.string());
  }
  const std::string interface_token = resource_id.substr(0U, 8U);
  for (std::uint32_t node = 1U; node <= node_count; ++node) {
    for (const char suffix : {'h', 'p'}) {
      const std::filesystem::path interface =
          std::filesystem::path("/sys/class/net") /
          ("bbp" + interface_token + "n" + std::to_string(node) + suffix);
      if (std::filesystem::exists(interface)) {
        throw std::runtime_error("owned run interface survived cleanup: " +
                                 interface.string());
      }
    }
  }
  RequireNoRpcCredentials(run_root);
}

void CheckFiniteDirectLoadOption(const std::filesystem::path& command,
                                 const std::filesystem::path& daemon,
                                 const std::filesystem::path& benchmark_root) {
  const std::string run_id = "direct-load-finite-" + std::to_string(getpid());
  const std::filesystem::path run_root = benchmark_root / run_id;
  PtyProcess process(command,
                     {"--firod",
                      daemon.string(),
                      "--benchmark-root",
                      benchmark_root.string(),
                      "--run-id",
                      run_id,
                      "--nodes",
                      "2",
                      "--wallet-node-count",
                      "2",
                      "--transaction-load-strategy",
                      "random_bruteforce",
                      "--metrics-sample-count",
                      "1",
                      "--no-tui",
                      "--metrics-interval",
                      "100ms",
                      "--block-production-period-ms",
                      "100",
                      "--block-production-probability",
                      "1",
                      "--keep-artifacts"},
                     24, 80);
  const int result = process.Wait(60s);
  if (result != 0) {
    throw std::runtime_error("explicit finite direct load exited " +
                             std::to_string(result));
  }
  const std::string resolved = ReadFile(run_root / "resolved-scenario.json");
  RequireContains(resolved, "\"metrics_sample_count\":1",
                  "explicit finite direct load options");
  const std::string events = ReadFile(run_root / "events.jsonl");
  RequireContains(events, "\"event\":\"run_finished\"",
                  "explicit finite direct load");
  RequireNotContains(events, "\"event\":\"run_cancelled\"",
                     "explicit finite direct load");
  const std::vector<pid_t> daemon_pids = EventProcessPids(events);
  if (daemon_pids.size() != 2U) {
    throw std::runtime_error("finite direct load did not start two daemons");
  }
  RequireOwnedResourcesRemoved(
      run_root, 2U, daemon_pids,
      MetricNamespacePids(ReadFile(run_root / "metrics.jsonl")));
}

void CheckIndefiniteDirectLoadLifecycle(
    const std::filesystem::path& command, const std::filesystem::path& daemon,
    const std::filesystem::path& benchmark_root) {
  const std::string run_id =
      "direct-load-indefinite-" + std::to_string(getpid());
  const std::filesystem::path run_root = benchmark_root / run_id;
  const std::filesystem::path events_path = run_root / "events.jsonl";
  PtyProcess process(command,
                     {"--firod",
                      daemon.string(),
                      "--benchmark-root",
                      benchmark_root.string(),
                      "--run-id",
                      run_id,
                      "--nodes",
                      "2",
                      "--wallet-node-count",
                      "2",
                      "--transaction-load-strategy",
                      "random_bruteforce",
                      "--metrics-interval",
                      "100ms",
                      "--refresh-ms",
                      "50",
                      "--block-production-period-ms",
                      "100",
                      "--block-production-probability",
                      "1",
                      "--keep-artifacts"},
                     30, 100);
  static_cast<void>(process.ReadUntil("Blockchain Benchmark Project TUI", 10s,
                                      "indefinite direct load"));
  const std::string resolved = WaitForFileText(
      run_root / "resolved-scenario.json", "\"metrics_sample_count\":0", 10s);
  RequireContains(resolved, "\"transaction_count\":2",
                  "indefinite direct load options");
  std::string after_workload =
      WaitForFileText(events_path, "\\\"transaction_index\\\":2", 30s);
  const std::size_t metrics_before =
      CountOccurrences(after_workload, "\"event\":\"metrics_sample\"");
  const std::size_t blocks_before = CountOccurrences(
      after_workload, "\"event\":\"scheduled_block_produced\"");
  after_workload = WaitForFileOccurrences(
      events_path, "\"event\":\"metrics_sample\"", metrics_before + 2U, 20s);
  after_workload = WaitForFileOccurrences(
      events_path, "\"event\":\"scheduled_block_produced\"", blocks_before + 2U,
      20s);
  RequireNotContains(after_workload, "\"event\":\"run_cancelled\"",
                     "indefinite direct load after workload");
  RequireNotContains(after_workload, "\"event\":\"run_finished\"",
                     "indefinite direct load after workload");
  const std::vector<pid_t> daemon_pids = EventProcessPids(after_workload);
  if (!process.Running() || daemon_pids.size() != 2U) {
    throw std::runtime_error(
        "default direct load was not running after workload completion");
  }
  for (const pid_t pid : daemon_pids) {
    if (!ProcessExists(pid)) {
      throw std::runtime_error(
          "direct-load daemon stopped after workload completion");
    }
  }

  process.Write("\x1b");
  static_cast<void>(
      process.ReadUntil("Confirm exit", 3s, "direct-load exit modal"));
  process.Write("y");
  const int result = process.Wait(30s);
  if (result != 0) {
    throw std::runtime_error("explicit direct-load exit returned " +
                             std::to_string(result));
  }
  const std::string finished =
      WaitForFileText(events_path, "\"event\":\"run_finished\"", 5s);
  RequireContains(finished, "\"event\":\"run_cancelled\"",
                  "explicit direct-load exit");
  RequireContains(finished, "\"event\":\"transaction_load_completed\"",
                  "explicit direct-load exit");
  RequireOwnedResourcesRemoved(
      run_root, 2U, daemon_pids,
      MetricNamespacePids(ReadFile(run_root / "metrics.jsonl")));
}

void CheckDirectLoadLifecycle(const std::filesystem::path& command,
                              const std::filesystem::path& daemon,
                              const std::filesystem::path& benchmark_root) {
  std::filesystem::create_directories(benchmark_root);
  CheckFiniteDirectLoadOption(command, daemon, benchmark_root);
  CheckIndefiniteDirectLoadLifecycle(command, daemon, benchmark_root);
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
  if (argc == 5 && std::string_view(argv[1]) == "--direct-load-lifecycle") {
    try {
      CheckDirectLoadLifecycle(argv[2], argv[3], argv[4]);
      std::cout << "direct no-JSON finite option and indefinite active-run "
                   "lifecycle checks passed\n";
      return 0;
    } catch (const std::exception& error) {
      std::cerr << "direct-load lifecycle regression failed: " << error.what()
                << '\n';
      return 1;
    }
  }
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
