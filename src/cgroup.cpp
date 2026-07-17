#include "bbp/cgroup.h"

#include <signal.h>
#include <sys/wait.h>
#include <unistd.h>

#include <algorithm>
#include <cctype>
#include <cerrno>
#include <charconv>
#include <chrono>
#include <filesystem>
#include <initializer_list>
#include <limits>
#include <map>
#include <mutex>
#include <set>
#include <sstream>
#include <stdexcept>
#include <string>
#include <string_view>
#include <thread>
#include <vector>

#include "bbp/util.h"

namespace bbp {
namespace {

constexpr std::string_view kCgroupRoot = "/sys/fs/cgroup";
constexpr std::string_view kSimulatorRootName = "bbp";
constexpr std::string_view kRunOwnershipMarker = ".bbp-run";
constexpr std::string_view kRunOwnershipMarkerContents = "bbp run\n";

std::mutex prepared_runs_mutex;
std::set<std::string> prepared_runs;

struct CgroupPaths {
  std::filesystem::path root;
  std::filesystem::path simulator;
  std::filesystem::path run;
};

std::filesystem::path CgroupRoot() {
  return std::filesystem::path(kCgroupRoot);
}

CgroupPaths CgroupPathsForRun(const std::string& run_id) {
  const std::filesystem::path root = CgroupRoot();
  const std::filesystem::path simulator =
      root / std::string(kSimulatorRootName);
  return CgroupPaths{
      .root = root, .simulator = simulator, .run = simulator / run_id};
}

bool RunWasPrepared(const std::string& run_id) {
  std::lock_guard<std::mutex> lock(prepared_runs_mutex);
  return prepared_runs.contains(run_id);
}

void RecordPreparedRun(const std::string& run_id) {
  std::lock_guard<std::mutex> lock(prepared_runs_mutex);
  if (!prepared_runs.insert(run_id).second) {
    throw std::runtime_error(
        "run cgroup is already prepared by this process: " + run_id);
  }
}

void ForgetPreparedRun(const std::string& run_id) {
  std::lock_guard<std::mutex> lock(prepared_runs_mutex);
  prepared_runs.erase(run_id);
}

bool RunningInsideDocker() { return std::filesystem::exists("/.dockerenv"); }

struct IoStatTotals {
  uint64_t read_bytes = 0;
  uint64_t write_bytes = 0;
  uint64_t read_operations = 0;
  uint64_t write_operations = 0;
  uint64_t discard_bytes = 0;
  uint64_t discard_operations = 0;
};

struct CpuMaxValue {
  std::optional<uint64_t> quota_us;
  uint64_t period_us = 0;
};

void WriteCgroupFile(const std::filesystem::path& dir, std::string_view file,
                     std::string_view value) {
  WriteText(dir / std::string(file), value);
}

uint64_t ParseUint64(std::string_view text, std::string_view context) {
  if (text.empty()) {
    throw std::runtime_error("empty uint64 value in " + std::string(context));
  }
  uint64_t value = 0;
  const auto [end, error] =
      std::from_chars(text.data(), text.data() + text.size(), value);
  if (error != std::errc() || end != text.data() + text.size()) {
    throw std::runtime_error("invalid uint64 value in " + std::string(context) +
                             ": " + std::string(text));
  }
  return value;
}

void CheckedAdd(uint64_t value, uint64_t* total, std::string_view context) {
  if (value > std::numeric_limits<uint64_t>::max() - *total) {
    throw std::runtime_error("uint64 overflow while summing " +
                             std::string(context));
  }
  *total += value;
}

uint64_t ParseSingleUint(const std::filesystem::path& path) {
  const std::vector<std::string> fields = SplitWhitespace(ReadText(path));
  if (fields.size() != 1U) {
    throw std::runtime_error("invalid single uint64 cgroup file: " +
                             path.string());
  }
  return ParseUint64(fields.front(), path.string());
}

std::optional<uint64_t> ParseMaxOrUint(const std::filesystem::path& path) {
  const std::string text = ReadText(path);
  const std::vector<std::string> fields = SplitWhitespace(text);
  if (fields.empty()) {
    throw std::runtime_error("empty cgroup max-or-uint file: " + path.string());
  }
  if (fields.size() != 1U) {
    throw std::runtime_error("invalid cgroup max-or-uint file: " +
                             path.string());
  }
  if (fields.front() == "max") {
    return std::nullopt;
  }
  return ParseUint64(fields.front(), path.string());
}

CpuMaxValue ParseCpuMax(const std::filesystem::path& path) {
  const std::vector<std::string> fields = SplitWhitespace(ReadText(path));
  if (fields.size() != 2U) {
    throw std::runtime_error("invalid cpu.max format: " + path.string());
  }
  CpuMaxValue value;
  if (fields[0] != "max") {
    value.quota_us = ParseUint64(fields[0], path.string());
  }
  value.period_us = ParseUint64(fields[1], path.string());
  return value;
}

uint64_t ParseKeyValue(const std::filesystem::path& path,
                       std::string_view key) {
  std::istringstream lines(ReadText(path));
  std::string line;
  std::optional<uint64_t> result;
  while (std::getline(lines, line)) {
    const std::vector<std::string> fields = SplitWhitespace(line);
    if (fields.empty()) {
      continue;
    }
    if (fields.size() != 2U) {
      throw std::runtime_error("invalid key/value cgroup line in " +
                               path.string() + ": " + line);
    }
    const uint64_t value = ParseUint64(fields[1], path.string());
    if (fields[0] == key) {
      if (result) {
        throw std::runtime_error("duplicate cgroup key in " + path.string() +
                                 ": " + std::string(key));
      }
      result = value;
    }
  }
  return result.value_or(0U);
}

std::optional<uint64_t> ParseAssignmentUint(std::string_view token,
                                            std::string_view key,
                                            const std::filesystem::path& path) {
  if (!token.starts_with(key)) {
    return std::nullopt;
  }
  return ParseUint64(token.substr(key.size()), path.string());
}

IoStatTotals ParseIoStat(const std::filesystem::path& path) {
  IoStatTotals totals;
  if (!std::filesystem::exists(path)) {
    return totals;
  }

  std::istringstream lines(ReadText(path));
  std::string line;
  while (std::getline(lines, line)) {
    const std::vector<std::string> fields = SplitWhitespace(line);
    if (fields.empty()) {
      continue;
    }
    ParseBlockDeviceId(fields.front());
    std::set<std::string_view> seen;
    for (size_t index = 1; index < fields.size(); ++index) {
      const std::string_view token = fields[index];
      const auto parse = [&](std::string_view key, uint64_t* total,
                             std::string_view metric) {
        const std::optional<uint64_t> value =
            ParseAssignmentUint(token, key, path);
        if (!value) {
          return false;
        }
        if (!seen.insert(key).second) {
          throw std::runtime_error("duplicate io.stat field in " +
                                   path.string() + ": " + std::string(key));
        }
        CheckedAdd(*value, total, metric);
        return true;
      };
      if (parse("rbytes=", &totals.read_bytes, "io read bytes") ||
          parse("wbytes=", &totals.write_bytes, "io write bytes") ||
          parse("rios=", &totals.read_operations, "io read operations") ||
          parse("wios=", &totals.write_operations, "io write operations") ||
          parse("dbytes=", &totals.discard_bytes, "io discard bytes") ||
          parse("dios=", &totals.discard_operations, "io discard operations")) {
        continue;
      }
    }
  }
  return totals;
}

std::map<std::string, uint64_t> ParseMemoryStat(
    const std::filesystem::path& path) {
  std::map<std::string, uint64_t> result;
  std::istringstream lines(ReadText(path));
  std::string line;
  while (std::getline(lines, line)) {
    const std::vector<std::string> fields = SplitWhitespace(line);
    if (fields.empty()) {
      continue;
    }
    if (fields.size() != 2U || fields[0].empty()) {
      throw std::runtime_error("invalid memory.stat line in " + path.string() +
                               ": " + line);
    }
    const uint64_t value = ParseUint64(fields[1], path.string());
    if (!result.emplace(fields[0], value).second) {
      throw std::runtime_error("duplicate memory.stat key in " + path.string() +
                               ": " + fields[0]);
    }
  }
  return result;
}

std::vector<IoLimit> ParseIoMax(const std::filesystem::path& path) {
  std::vector<IoLimit> limits;
  if (!std::filesystem::exists(path)) {
    return limits;
  }
  std::set<BlockDeviceId> devices;
  std::istringstream lines(ReadText(path));
  std::string line;
  while (std::getline(lines, line)) {
    const std::vector<std::string> fields = SplitWhitespace(line);
    if (fields.empty()) {
      continue;
    }
    IoLimit limit;
    limit.device = ParseBlockDeviceId(fields.front());
    if (!devices.insert(limit.device).second) {
      throw std::runtime_error("duplicate device in " + path.string() + ": " +
                               fields.front());
    }
    std::set<std::string_view> seen;
    for (size_t index = 1; index < fields.size(); ++index) {
      const std::string_view token = fields[index];
      const auto assign = [&](std::string_view prefix,
                              std::optional<uint64_t>* output) {
        if (!token.starts_with(prefix)) {
          return false;
        }
        if (!seen.insert(prefix).second) {
          throw std::runtime_error("duplicate io.max field in " +
                                   path.string() + ": " + std::string(prefix));
        }
        const std::string_view value = token.substr(prefix.size());
        if (value == "max") {
          output->reset();
        } else {
          *output = ParseUint64(value, path.string());
        }
        return true;
      };
      if (!assign("rbps=", &limit.read_bytes_per_sec) &&
          !assign("wbps=", &limit.write_bytes_per_sec) &&
          !assign("riops=", &limit.read_operations_per_sec) &&
          !assign("wiops=", &limit.write_operations_per_sec)) {
        throw std::runtime_error("unknown io.max field in " + path.string() +
                                 ": " + std::string(token));
      }
    }
    limits.push_back(std::move(limit));
  }
  return limits;
}

uint64_t ParseIoWeight(const std::filesystem::path& path) {
  const std::vector<std::string> fields = SplitWhitespace(ReadText(path));
  if (fields.size() == 2U && fields[0] == "default") {
    return ParseUint64(fields[1], path.string());
  }
  if (fields.size() == 1U) {
    return ParseUint64(fields[0], path.string());
  }
  throw std::runtime_error("invalid io.weight format: " + path.string());
}

uint64_t ParsePressureTotal(const std::filesystem::path& path,
                            std::string_view category) {
  if (!std::filesystem::exists(path)) {
    return 0;
  }

  std::istringstream lines(ReadText(path));
  std::string line;
  std::optional<uint64_t> result;
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
        if (result) {
          throw std::runtime_error("duplicate pressure total in " +
                                   path.string() + ": " +
                                   std::string(category));
        }
        result = ParseUint64(token.substr(kTotalPrefix.size()), path.string());
      }
    }
  }
  return result.value_or(0U);
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
  throw std::runtime_error("cgroup still contains processes after kill: " +
                           dir.string());
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

void CreateCgroupDirectoryExclusive(const std::filesystem::path& path,
                                    std::string_view kind) {
  std::error_code ec;
  const bool created = std::filesystem::create_directory(path, ec);
  if (ec) {
    throw std::runtime_error("create " + std::string(kind) +
                             " cgroup failed for " + path.string() + ": " +
                             ec.message());
  }
  if (!created) {
    throw std::runtime_error("refusing to adopt pre-existing " +
                             std::string(kind) + " cgroup: " + path.string());
  }
}

std::vector<std::filesystem::path> DescendantCgroupsDeepestFirst(
    const std::filesystem::path& run_root) {
  std::vector<std::filesystem::path> paths;
  std::error_code ec;
  std::filesystem::recursive_directory_iterator iterator(
      run_root, std::filesystem::directory_options::none, ec);
  const std::filesystem::recursive_directory_iterator end;
  if (ec) {
    throw std::runtime_error("list run cgroup failed for " + run_root.string() +
                             ": " + ec.message());
  }
  while (iterator != end) {
    const bool is_directory = iterator->is_directory(ec);
    if (ec) {
      throw std::runtime_error("inspect run cgroup entry failed for " +
                               iterator->path().string() + ": " + ec.message());
    }
    if (is_directory) {
      paths.push_back(iterator->path());
    }
    iterator.increment(ec);
    if (ec) {
      throw std::runtime_error("list run cgroup failed for " +
                               run_root.string() + ": " + ec.message());
    }
  }
  const auto depth = [](const std::filesystem::path& path) {
    return static_cast<std::size_t>(std::distance(path.begin(), path.end()));
  };
  std::sort(paths.begin(), paths.end(), [&](const auto& lhs, const auto& rhs) {
    const std::size_t lhs_depth = depth(lhs);
    const std::size_t rhs_depth = depth(rhs);
    if (lhs_depth != rhs_depth) {
      return lhs_depth > rhs_depth;
    }
    return lhs.generic_string() < rhs.generic_string();
  });
  return paths;
}

void RemoveCgroupDirectory(const std::filesystem::path& path,
                           std::string_view kind) {
  std::error_code ec;
  const bool removed = std::filesystem::remove(path, ec);
  if (ec) {
    throw std::runtime_error("remove " + std::string(kind) +
                             " cgroup failed for " + path.string() + ": " +
                             ec.message());
  }
  if (!removed) {
    throw std::runtime_error(
        std::string(kind) +
        " cgroup disappeared during removal: " + path.string());
  }
}

void RemoveRunCgroup(const std::string& run_id) {
  const std::filesystem::path run_root = CgroupPathsForRun(run_id).run;
  if (!std::filesystem::exists(run_root)) {
    return;
  }
  KillCgroupIfSupported(run_root);
  for (const std::filesystem::path& path :
       DescendantCgroupsDeepestFirst(run_root)) {
    KillCgroupIfSupported(path);
    RemoveCgroupDirectory(path, "descendant");
  }
  RemoveCgroupDirectory(run_root, "run");
}

void RequireStaleRunOwnership(
    const std::string& run_id,
    const std::filesystem::path& owned_run_directory) {
  if (owned_run_directory.filename() != run_id) {
    throw std::runtime_error(
        "run directory does not match stale cgroup run id: " +
        owned_run_directory.string());
  }
  std::error_code ec;
  const std::filesystem::file_status run_status =
      std::filesystem::symlink_status(owned_run_directory, ec);
  if (ec || !std::filesystem::is_directory(run_status)) {
    throw std::runtime_error(
        "stale cgroup cleanup requires an owned run "
        "directory: " +
        owned_run_directory.string());
  }
  const std::filesystem::path marker =
      owned_run_directory / std::string(kRunOwnershipMarker);
  const std::filesystem::file_status marker_status =
      std::filesystem::symlink_status(marker, ec);
  if (ec || !std::filesystem::is_regular_file(marker_status) ||
      ReadText(marker) != kRunOwnershipMarkerContents) {
    throw std::runtime_error(
        "stale cgroup cleanup requires an exact simulator ownership marker: " +
        marker.string());
  }
}

}  // namespace

BlockDeviceId ParseBlockDeviceId(std::string_view text) {
  const size_t separator = text.find(':');
  if (separator == std::string_view::npos || separator == 0U ||
      separator + 1U >= text.size() ||
      text.find(':', separator + 1U) != std::string_view::npos) {
    throw std::runtime_error("invalid block device id: " + std::string(text));
  }
  const uint64_t major =
      ParseUint64(text.substr(0U, separator), "block device major");
  const uint64_t minor =
      ParseUint64(text.substr(separator + 1U), "block device minor");
  if (major > std::numeric_limits<uint32_t>::max() ||
      minor > std::numeric_limits<uint32_t>::max()) {
    throw std::runtime_error("block device id exceeds uint32: " +
                             std::string(text));
  }
  return BlockDeviceId{.major = static_cast<uint32_t>(major),
                       .minor = static_cast<uint32_t>(minor)};
}

std::string BlockDeviceIdText(const BlockDeviceId& device) {
  return std::to_string(device.major) + ":" + std::to_string(device.minor);
}

void Cgroup::PrepareRun(const std::string& run_id) {
  RequireSafeRunId(run_id);

  const CgroupPaths paths = CgroupPathsForRun(run_id);
  if (!std::filesystem::exists(paths.root / "cgroup.controllers")) {
    throw std::runtime_error("cgroup v2 is not mounted at " +
                             paths.root.string());
  }

  RequireControllersAvailable(paths.root, {"cpu", "io", "memory", "pids"});
  EnsureDirectory(paths.simulator);
  if (!CgroupProcsEmpty(paths.root)) {
    MoveRootProcessesIntoContainerController(paths.simulator);
  }
  EnableControllers(paths.root);
  RequireControllersAvailable(paths.simulator, {"cpu", "io", "memory", "pids"});
  EnableControllers(paths.simulator);
  CreateCgroupDirectoryExclusive(paths.run, "run");
  try {
    RequireControllersAvailable(paths.run, {"cpu", "io", "memory", "pids"});
    EnableControllers(paths.run);
    RecordPreparedRun(run_id);
  } catch (...) {
    std::error_code ignored;
    std::filesystem::remove(paths.run, ignored);
    throw;
  }
}

Cgroup Cgroup::Create(const std::string& run_id, const std::string& node_id) {
  RequireSafeRunId(run_id);
  RequireSafeRunId(node_id);

  const CgroupPaths paths = CgroupPathsForRun(run_id);
  if (!RunWasPrepared(run_id)) {
    throw std::runtime_error("refusing to adopt an unprepared run cgroup: " +
                             paths.run.string());
  }
  if (!std::filesystem::is_directory(paths.run)) {
    throw std::runtime_error("run cgroup was not prepared: " +
                             paths.run.string());
  }
  RequireControllersAvailable(paths.run, {"cpu", "io", "memory", "pids"});
  EnableControllers(paths.run);
  const std::filesystem::path node_root = paths.run / node_id;
  CreateCgroupDirectoryExclusive(node_root, "node");

  return Cgroup(node_root);
}

void Cgroup::RemoveRun(const std::string& run_id) {
  RequireSafeRunId(run_id);
  const std::filesystem::path run_root = CgroupPathsForRun(run_id).run;
  if (!RunWasPrepared(run_id)) {
    if (!std::filesystem::exists(run_root)) {
      return;
    }
    throw std::runtime_error("refusing to remove an unowned run cgroup: " +
                             run_root.string());
  }
  RemoveRunCgroup(run_id);
  ForgetPreparedRun(run_id);
}

void Cgroup::RemoveStaleRun(const std::string& run_id,
                            const std::filesystem::path& owned_run_directory) {
  RequireSafeRunId(run_id);
  RequireStaleRunOwnership(run_id, owned_run_directory);
  RemoveRunCgroup(run_id);
  ForgetPreparedRun(run_id);
}

CgroupFreezeProbe Cgroup::ProbeFreezeThaw() {
  CgroupFreezeProbe probe;
  probe.run_id = "freeze-" + std::to_string(getpid());
  probe.node_id = "node-1";

  Cgroup::PrepareRun(probe.run_id);
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

void Cgroup::SetCpuWeight(uint64_t weight) const {
  if (weight < 1U || weight > 10000U) {
    throw std::runtime_error("cpu.weight must be in 1..10000");
  }
  WriteCgroupFile(path_, "cpu.weight", std::to_string(weight));
  if (ParseSingleUint(path_ / "cpu.weight") != weight) {
    throw std::runtime_error("cpu.weight read-back verification failed for " +
                             path_.string());
  }
}

void Cgroup::SetIoMax(const IoLimit& limit) const {
  const auto require_positive = [](const std::optional<uint64_t>& value,
                                   std::string_view name) {
    if (value && *value == 0U) {
      throw std::runtime_error(std::string(name) +
                               " must be greater than zero");
    }
  };
  require_positive(limit.read_bytes_per_sec, "io.max rbps");
  require_positive(limit.write_bytes_per_sec, "io.max wbps");
  require_positive(limit.read_operations_per_sec, "io.max riops");
  require_positive(limit.write_operations_per_sec, "io.max wiops");

  const auto value_text = [](const std::optional<uint64_t>& value) {
    return value ? std::to_string(*value) : std::string("max");
  };
  const std::string value =
      BlockDeviceIdText(limit.device) +
      " rbps=" + value_text(limit.read_bytes_per_sec) +
      " wbps=" + value_text(limit.write_bytes_per_sec) +
      " riops=" + value_text(limit.read_operations_per_sec) +
      " wiops=" + value_text(limit.write_operations_per_sec);
  WriteCgroupFile(path_, "io.max", value);

  const std::vector<IoLimit> actual = ParseIoMax(path_ / "io.max");
  const auto found =
      std::find_if(actual.begin(), actual.end(), [&](const IoLimit& candidate) {
        return candidate.device == limit.device;
      });
  const bool unlimited =
      !limit.read_bytes_per_sec && !limit.write_bytes_per_sec &&
      !limit.read_operations_per_sec && !limit.write_operations_per_sec;
  if ((found == actual.end() && !unlimited) ||
      (found != actual.end() && *found != limit)) {
    throw std::runtime_error("io.max read-back verification failed for " +
                             path_.string() + " device " +
                             BlockDeviceIdText(limit.device));
  }
}

void Cgroup::SetIoWeight(uint64_t weight) const {
  if (weight < 1U || weight > 10000U) {
    throw std::runtime_error("io.weight must be in 1..10000");
  }
  WriteCgroupFile(path_, "io.weight", "default " + std::to_string(weight));
  if (ParseIoWeight(path_ / "io.weight") != weight) {
    throw std::runtime_error("io.weight read-back verification failed for " +
                             path_.string());
  }
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
  metrics.cpu_weight = ParseSingleUint(path_ / "cpu.weight");
  metrics.io_weight = ParseIoWeight(path_ / "io.weight");
  metrics.io_limits = ParseIoMax(path_ / "io.max");
  const IoStatTotals io = ParseIoStat(path_ / "io.stat");
  metrics.io_read_bytes = io.read_bytes;
  metrics.io_write_bytes = io.write_bytes;
  metrics.io_read_operations = io.read_operations;
  metrics.io_write_operations = io.write_operations;
  metrics.io_discard_bytes = io.discard_bytes;
  metrics.io_discard_operations = io.discard_operations;
  metrics.io_pressure_some_total_usec =
      ParsePressureTotal(path_ / "io.pressure", "some");
  metrics.io_pressure_full_total_usec =
      ParsePressureTotal(path_ / "io.pressure", "full");
  metrics.pids_current = ParseSingleUint(path_ / "pids.current");
  metrics.pids_max_limit = ParseMaxOrUint(path_ / "pids.max");
  metrics.pids_max_events = ParseKeyValue(path_ / "pids.events", "max");
  metrics.cgroup_populated =
      ParseKeyValue(path_ / "cgroup.events", "populated");
  metrics.cgroup_frozen = ParseKeyValue(path_ / "cgroup.events", "frozen");
  metrics.memory_low = ParseKeyValue(path_ / "memory.events", "low");
  metrics.memory_high = ParseKeyValue(path_ / "memory.events", "high");
  metrics.memory_max = ParseKeyValue(path_ / "memory.events", "max");
  metrics.oom = ParseKeyValue(path_ / "memory.events", "oom");
  metrics.oom_kill = ParseKeyValue(path_ / "memory.events", "oom_kill");
  metrics.oom_group_kill =
      ParseKeyValue(path_ / "memory.events", "oom_group_kill");
  metrics.memory_stat = ParseMemoryStat(path_ / "memory.stat");
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
