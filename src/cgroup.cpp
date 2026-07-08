#include "benchmark_sim/cgroup.h"

#include "benchmark_sim/util.h"

#include <unistd.h>

#include <cerrno>
#include <filesystem>
#include <sstream>
#include <stdexcept>
#include <string>
#include <string_view>
#include <thread>

namespace bsim {
namespace {

constexpr std::string_view kCgroupRoot = "/sys/fs/cgroup";
constexpr std::string_view kSimulatorRootName = "benchmark-sim";

std::filesystem::path CgroupRoot() { return std::filesystem::path(kCgroupRoot); }

bool RunningInsideDocker() { return std::filesystem::exists("/.dockerenv"); }

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

bool CgroupProcsEmpty(const std::filesystem::path& dir) {
  return SplitWhitespace(ReadText(dir / "cgroup.procs")).empty();
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

  EnsureDirectory(sim_root);
  if (!CgroupProcsEmpty(root)) {
    MoveRootProcessesIntoContainerController(sim_root);
  }
  EnableControllers(root);
  EnsureDirectory(run_root);
  EnableControllers(sim_root);
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
  metrics.memory_current = ParseSingleUint(path_ / "memory.current");
  if (std::filesystem::exists(path_ / "memory.peak")) {
    metrics.memory_peak = ParseSingleUint(path_ / "memory.peak");
  }
  metrics.pids_current = ParseSingleUint(path_ / "pids.current");
  metrics.oom = ParseKeyValue(path_ / "memory.events", "oom");
  metrics.oom_kill = ParseKeyValue(path_ / "memory.events", "oom_kill");
  return metrics;
}

void Cgroup::Freeze() const { WriteCgroupFile(path_, "cgroup.freeze", "1"); }

void Cgroup::Thaw() const { WriteCgroupFile(path_, "cgroup.freeze", "0"); }

void Cgroup::KillAll() const {
  if (std::filesystem::exists(path_ / "cgroup.kill")) {
    WriteCgroupFile(path_, "cgroup.kill", "1");
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

}  // namespace bsim
