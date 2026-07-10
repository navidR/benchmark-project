#include "bbp/cgroup.h"

#include "bbp/util.h"

#include <signal.h>
#include <sys/wait.h>
#include <unistd.h>

#include <cerrno>
#include <cctype>
#include <filesystem>
#include <initializer_list>
#include <sstream>
#include <stdexcept>
#include <string>
#include <string_view>
#include <thread>

namespace bbp {
namespace {

constexpr std::string_view kCgroupRoot = "/sys/fs/cgroup";
constexpr std::string_view kSimulatorRootName = "bbp";

std::filesystem::path CgroupRoot() { return std::filesystem::path(kCgroupRoot); }

bool RunningInsideDocker() { return std::filesystem::exists("/.dockerenv"); }

struct IoStatTotals {
  uint64_t read_bytes = 0;
  uint64_t write_bytes = 0;
};

struct CpuMaxValue {
  std::optional<uint64_t> quota_us;
  uint64_t period_us = 0;
};

void WriteCgroupFile(const std::filesystem::path& dir, std::string_view file,
                     std::string_view value) {
  WriteText(dir / std::string(file), value);
}

uint64_t ParseSingleUint(const std::filesystem::path& path) {
  std::string text = ReadText(path);
  size_t pos = 0;
  while (pos < text.size() && std::isspace(static_cast<unsigned char>(text[pos]))) {
    ++pos;
  }
  return std::stoull(text.substr(pos));
}

std::optional<uint64_t> ParseMaxOrUint(const std::filesystem::path& path) {
  const std::string text = ReadText(path);
  const std::vector<std::string> fields = SplitWhitespace(text);
  if (fields.empty()) {
    throw std::runtime_error("empty cgroup max-or-uint file: " +
                             path.string());
  }
  if (fields.front() == "max") {
    return std::nullopt;
  }
  return std::stoull(fields.front());
}

CpuMaxValue ParseCpuMax(const std::filesystem::path& path) {
  const std::vector<std::string> fields = SplitWhitespace(ReadText(path));
  if (fields.size() != 2U) {
    throw std::runtime_error("invalid cpu.max format: " + path.string());
  }
  CpuMaxValue value;
  if (fields[0] != "max") {
    value.quota_us = std::stoull(fields[0]);
  }
  value.period_us = std::stoull(fields[1]);
  return value;
}

uint64_t ParseKeyValue(const std::filesystem::path& path, std::string_view key) {
  std::istringstream in(ReadText(path));
  std::string name;
  uint64_t value = 0;
  while (in >> name >> value) {
    if (name == key) {
      return value;
    }
  }
  return 0;
}

uint64_t ParseAssignmentUint(std::string_view token, std::string_view key) {
  if (!token.starts_with(key) || token.size() == key.size()) {
    return 0;
  }
  return std::stoull(std::string(token.substr(key.size())));
}

IoStatTotals ParseIoStat(const std::filesystem::path& path) {
  IoStatTotals totals;
  if (!std::filesystem::exists(path)) {
    return totals;
  }

  std::istringstream lines(ReadText(path));
  std::string line;
  while (std::getline(lines, line)) {
    std::istringstream fields(line);
    std::string token;
    fields >> token;
    while (fields >> token) {
      totals.read_bytes += ParseAssignmentUint(token, "rbytes=");
      totals.write_bytes += ParseAssignmentUint(token, "wbytes=");
    }
  }
  return totals;
}

uint64_t ParsePressureTotal(const std::filesystem::path& path,
                            std::string_view category) {
  if (!std::filesystem::exists(path)) {
    return 0;
  }

  std::istringstream lines(ReadText(path));
  std::string line;
  while (std::getline(lines, line)) {
    std::istringstream fields(line);
    std::string token;
    fields >> token;
    if (token != category) {
      continue;
    }
    while (fields >> token) {
      constexpr std::string_view kTotalPrefix = "total=";
      if (token.starts_with(kTotalPrefix)) {
        return std::stoull(std::string(token.substr(kTotalPrefix.size())));
      }
    }
  }
  return 0;
}

void EnableControllers(const std::filesystem::path& dir) {
  const auto controllers_file = dir / "cgroup.controllers";
  const auto subtree_file = dir / "cgroup.subtree_control";
  if (!std::filesystem::exists(controllers_file)) {
    throw std::runtime_error("missing cgroup.controllers at " + dir.string());
  }
  const std::string controllers = ReadText(controllers_file);
  std::string request;
  for (const std::string& controller : SplitWhitespace(controllers)) {
    if (controller == "cpu" || controller == "memory" || controller == "io" ||
        controller == "pids") {
      if (!request.empty()) {
        request += ' ';
      }
      request += '+';
      request += controller;
    }
  }
  if (!request.empty()) {
    WriteText(subtree_file, request);
  }
}

bool ContainsController(const std::vector<std::string>& controllers,
                        std::string_view required) {
  for (const std::string& controller : controllers) {
    if (controller == required) {
      return true;
    }
  }
  return false;
}

void RequireControllersAvailable(
    const std::filesystem::path& dir,
    std::initializer_list<std::string_view> required_controllers) {
  const auto controllers_file = dir / "cgroup.controllers";
  if (!std::filesystem::exists(controllers_file)) {
    throw std::runtime_error("missing cgroup.controllers at " + dir.string());
  }
  const std::vector<std::string> controllers =
      SplitWhitespace(ReadText(controllers_file));
  for (std::string_view required : required_controllers) {
    if (!ContainsController(controllers, required)) {
      throw std::runtime_error("required cgroup controller unavailable at " +
                               dir.string() + ": " + std::string(required));
    }
  }
}

bool CgroupProcsEmpty(const std::filesystem::path& dir) {
  return SplitWhitespace(ReadText(dir / "cgroup.procs")).empty();
}

void WaitForCgroupProcsEmpty(const std::filesystem::path& dir) {
  for (int attempt = 0; attempt < 50; ++attempt) {
    if (CgroupProcsEmpty(dir)) {
      return;
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(20));
  }
}

void KillCgroupIfSupported(const std::filesystem::path& dir) {
  if (std::filesystem::exists(dir / "cgroup.kill")) {
    WriteCgroupFile(dir, "cgroup.kill", "1");
    WaitForCgroupProcsEmpty(dir);
  }
}

bool ReadFrozenState(const std::filesystem::path& dir) {
  return ParseKeyValue(dir / "cgroup.events", "frozen") != 0U;
}

bool WaitForFrozenState(const Cgroup& cgroup, bool expected) {
  for (int attempt = 0; attempt < 50; ++attempt) {
    if (cgroup.Frozen() == expected) {
      return true;
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(20));
  }
  return false;
}

void MoveRootProcessesIntoContainerController(
    const std::filesystem::path& sim_root) {
  if (!RunningInsideDocker()) {
    return;
  }

  const std::filesystem::path root = CgroupRoot();
  const std::filesystem::path controller = sim_root / "controller";
  EnsureDirectory(controller);

  for (int attempt = 0; attempt < 20; ++attempt) {
    const std::vector<std::string> pids =
        SplitWhitespace(ReadText(root / "cgroup.procs"));
    if (pids.empty()) {
      return;
    }
    for (const std::string& pid : pids) {
      try {
        WriteText(controller / "cgroup.procs", pid);
      } catch (const std::exception&) {
        // A PID can exit between reading cgroup.procs and writing it.
      }
    }
    if (CgroupProcsEmpty(root)) {
      return;
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(20));
  }

  throw std::runtime_error(
      "could not delegate Docker cgroup root; root cgroup still has processes");
}

}  // namespace

Cgroup Cgroup::Create(const std::string& run_id, const std::string& node_id) {
  RequireSafeRunId(run_id);
  RequireSafeRunId(node_id);

  const std::filesystem::path root = CgroupRoot();
  if (!std::filesystem::exists(root / "cgroup.controllers")) {
    throw std::runtime_error("cgroup v2 is not mounted at " + root.string());
  }

  const std::filesystem::path sim_root = root / std::string(kSimulatorRootName);
  const std::filesystem::path run_root = sim_root / run_id;
  const std::filesystem::path node_root = run_root / node_id;

  RequireControllersAvailable(root, {"cpu", "io", "memory", "pids"});
  EnsureDirectory(sim_root);
  if (!CgroupProcsEmpty(root)) {
    MoveRootProcessesIntoContainerController(sim_root);
  }
  EnableControllers(root);
  RequireControllersAvailable(sim_root, {"cpu", "io", "memory", "pids"});
  EnsureDirectory(run_root);
  EnableControllers(sim_root);
  RequireControllersAvailable(run_root, {"cpu", "io", "memory", "pids"});
  EnsureDirectory(node_root);
  EnableControllers(run_root);

  return Cgroup(node_root);
}

void Cgroup::RemoveRun(const std::string& run_id) {
  RequireSafeRunId(run_id);
  const std::filesystem::path run_root =
      CgroupRoot() / std::string(kSimulatorRootName) / run_id;
  if (!std::filesystem::exists(run_root)) {
    return;
  }
  std::error_code ec;
  for (const auto& entry : std::filesystem::directory_iterator(run_root, ec)) {
    if (ec) {
      break;
    }
    if (entry.is_directory()) {
      KillCgroupIfSupported(entry.path());
      std::filesystem::remove(entry.path(), ec);
    }
  }
  ec.clear();
  std::filesystem::remove(run_root, ec);
  if (ec) {
    throw std::runtime_error("remove run cgroup failed for " +
                             run_root.string() + ": " + ec.message());
  }
}

CgroupFreezeProbe Cgroup::ProbeFreezeThaw() {
  CgroupFreezeProbe probe;
  probe.run_id = "freeze-" + std::to_string(getpid());
  probe.node_id = "node-1";

  Cgroup::RemoveRun(probe.run_id);
  Cgroup cgroup = Cgroup::Create(probe.run_id, probe.node_id);
  pid_t child = fork();
  if (child < 0) {
    cgroup.Remove();
    Cgroup::RemoveRun(probe.run_id);
    throw std::runtime_error(std::string("fork failed: ") +
                             std::strerror(errno));
  }
  if (child == 0) {
    for (;;) {
      pause();
    }
  }

  probe.child_pid = child;
  try {
    cgroup.AttachPid(child);
    cgroup.Freeze();
    if (!WaitForFrozenState(cgroup, true)) {
      throw std::runtime_error("cgroup did not report frozen after freeze");
    }
    probe.frozen_after_freeze = cgroup.Frozen();
    cgroup.Thaw();
    if (!WaitForFrozenState(cgroup, false)) {
      throw std::runtime_error("cgroup still reported frozen after thaw");
    }
    probe.frozen_after_thaw = cgroup.Frozen();
    kill(child, SIGKILL);
    waitpid(child, nullptr, 0);
    cgroup.Remove();
    Cgroup::RemoveRun(probe.run_id);
  } catch (...) {
    kill(child, SIGKILL);
    waitpid(child, nullptr, 0);
    try {
      cgroup.KillAll();
      cgroup.Remove();
      Cgroup::RemoveRun(probe.run_id);
    } catch (const std::exception&) {
    }
    throw;
  }
  return probe;
}

void Cgroup::AttachPid(pid_t pid) const {
  WriteCgroupFile(path_, "cgroup.procs", std::to_string(pid));
}

void Cgroup::SetMemoryMax(uint64_t bytes) const {
  WriteCgroupFile(path_, "memory.max", std::to_string(bytes));
}

void Cgroup::SetMemoryHigh(uint64_t bytes) const {
  WriteCgroupFile(path_, "memory.high", std::to_string(bytes));
}

void Cgroup::SetCpuMax(std::optional<uint64_t> quota_us,
                       uint64_t period_us) const {
  std::string value = quota_us ? std::to_string(*quota_us) : "max";
  value += " ";
  value += std::to_string(period_us);
  WriteCgroupFile(path_, "cpu.max", value);
}

void Cgroup::SetPidsMax(uint64_t n) const {
  WriteCgroupFile(path_, "pids.max", std::to_string(n));
}

CgroupMetrics Cgroup::ReadMetrics() const {
  CgroupMetrics metrics;
  metrics.cpu_usage_usec = ParseKeyValue(path_ / "cpu.stat", "usage_usec");
  metrics.cpu_throttled_usec =
      ParseKeyValue(path_ / "cpu.stat", "throttled_usec");
  metrics.cpu_pressure_some_total_usec =
      ParsePressureTotal(path_ / "cpu.pressure", "some");
  metrics.cpu_pressure_full_total_usec =
      ParsePressureTotal(path_ / "cpu.pressure", "full");
  metrics.memory_current = ParseSingleUint(path_ / "memory.current");
  if (std::filesystem::exists(path_ / "memory.peak")) {
    metrics.memory_peak = ParseSingleUint(path_ / "memory.peak");
  }
  metrics.memory_high_limit_bytes = ParseMaxOrUint(path_ / "memory.high");
  metrics.memory_max_limit_bytes = ParseMaxOrUint(path_ / "memory.max");
  const CpuMaxValue cpu_max = ParseCpuMax(path_ / "cpu.max");
  metrics.cpu_quota_us = cpu_max.quota_us;
  metrics.cpu_period_us = cpu_max.period_us;
  const IoStatTotals io = ParseIoStat(path_ / "io.stat");
  metrics.io_read_bytes = io.read_bytes;
  metrics.io_write_bytes = io.write_bytes;
  metrics.io_pressure_some_total_usec =
      ParsePressureTotal(path_ / "io.pressure", "some");
  metrics.io_pressure_full_total_usec =
      ParsePressureTotal(path_ / "io.pressure", "full");
  metrics.pids_current = ParseSingleUint(path_ / "pids.current");
  metrics.pids_max_limit = ParseMaxOrUint(path_ / "pids.max");
  metrics.pids_max_events = ParseKeyValue(path_ / "pids.events", "max");
  metrics.cgroup_populated = ParseKeyValue(path_ / "cgroup.events", "populated");
  metrics.cgroup_frozen = ParseKeyValue(path_ / "cgroup.events", "frozen");
  metrics.memory_low = ParseKeyValue(path_ / "memory.events", "low");
  metrics.memory_high = ParseKeyValue(path_ / "memory.events", "high");
  metrics.memory_max = ParseKeyValue(path_ / "memory.events", "max");
  metrics.oom = ParseKeyValue(path_ / "memory.events", "oom");
  metrics.oom_kill = ParseKeyValue(path_ / "memory.events", "oom_kill");
  metrics.oom_group_kill =
      ParseKeyValue(path_ / "memory.events", "oom_group_kill");
  return metrics;
}

void Cgroup::Freeze() const { WriteCgroupFile(path_, "cgroup.freeze", "1"); }

void Cgroup::Thaw() const { WriteCgroupFile(path_, "cgroup.freeze", "0"); }

bool Cgroup::Frozen() const { return ReadFrozenState(path_); }

void Cgroup::KillAll() const {
  if (std::filesystem::exists(path_ / "cgroup.kill")) {
    WriteCgroupFile(path_, "cgroup.kill", "1");
    WaitForCgroupProcsEmpty(path_);
  }
}

void Cgroup::Remove() const {
  std::error_code ec;
  std::filesystem::remove(path_, ec);
  if (ec) {
    throw std::runtime_error("remove cgroup failed for " + path_.string() +
                             ": " + ec.message());
  }
}

}  // namespace bbp
