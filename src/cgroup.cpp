#include "bbp/cgroup.h"

#include <fcntl.h>
#include <linux/magic.h>
#include <signal.h>
#include <sys/file.h>
#include <sys/random.h>
#include <sys/stat.h>
#include <sys/syscall.h>
#include <sys/vfs.h>
#include <sys/wait.h>
#include <unistd.h>

#include <algorithm>
#include <array>
#include <boost/json/array.hpp>
#include <boost/json/object.hpp>
#include <boost/json/parse.hpp>
#include <boost/json/serialize.hpp>
#include <boost/json/value.hpp>
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

#include "bbp/run_ownership.h"
#include "bbp/util.h"

namespace bbp {
namespace {

constexpr std::string_view kCgroupRoot = "/sys/fs/cgroup";
constexpr std::string_view kSimulatorRootName = "bbp";
constexpr std::string_view kCgroupScopeStateFile =
    "/tmp/blockchain-benchmark-project-cgroup-scope-v1.json";
constexpr std::string_view kOwnedControllerPrefix = ".bbp-controller-v1-";
constexpr std::uint64_t kCgroupScopeStateVersion = 2U;

std::mutex prepared_runs_mutex;
std::set<std::string> prepared_runs;

struct CgroupPaths {
  std::filesystem::path root;
  std::filesystem::path simulator;
  std::filesystem::path run;
};

struct CgroupScopeConfig {
  std::filesystem::path root;
  std::string simulator_name;
  std::filesystem::path state_file;
  bool allow_root_process_move = false;
};

struct CgroupPathIdentity {
  std::uint64_t device = 0U;
  std::uint64_t inode = 0U;

  bool operator==(const CgroupPathIdentity&) const = default;
};

struct CgroupRunBinding {
  std::string run_id;
  std::filesystem::path run_root;
  std::string resource_id;
  CgroupPathIdentity run_root_identity;
  CgroupPathIdentity cgroup_identity;

  bool operator==(const CgroupRunBinding&) const = default;
};

struct CgroupScopeState {
  std::filesystem::path root;
  std::string simulator_name;
  std::string controller_name;
  bool simulator_preexisting = false;
  std::set<std::string> root_controllers_before;
  std::set<std::string> simulator_controllers_before;
  std::set<std::string> root_controllers_added;
  std::set<std::string> simulator_controllers_added;
  std::set<std::string> active_runs;
  std::map<std::string, CgroupRunBinding> run_bindings;
  std::optional<std::string> pending_run;
  bool pending_run_created = false;
};

class CgroupScopeLock {
 public:
  explicit CgroupScopeLock(const std::filesystem::path& root) {
    Open(root);
    int result = 0;
    do {
      result = flock(fd_, LOCK_EX);
    } while (result != 0 && errno == EINTR);
    if (result != 0) {
      ThrowLockFailure(root, errno);
    }
  }

  CgroupScopeLock(const std::filesystem::path& root,
                  std::chrono::steady_clock::time_point deadline,
                  std::stop_token stop_token) {
    Open(root);
    for (;;) {
      if (stop_token.stop_requested()) {
        Close();
        throw std::runtime_error("cgroup scope lock cancelled for " +
                                 root.string());
      }
      const auto now = std::chrono::steady_clock::now();
      if (now >= deadline) {
        Close();
        throw std::runtime_error("cgroup scope lock deadline expired for " +
                                 root.string());
      }
      if (flock(fd_, LOCK_EX | LOCK_NB) == 0) {
        return;
      }
      const int error = errno;
      if (error != EINTR && error != EAGAIN && error != EWOULDBLOCK) {
        ThrowLockFailure(root, error);
      }
      std::this_thread::sleep_for(std::min(
          std::chrono::milliseconds(5),
          std::chrono::duration_cast<std::chrono::milliseconds>(deadline -
                                                                now)));
    }
  }

  CgroupScopeLock(const CgroupScopeLock&) = delete;
  CgroupScopeLock& operator=(const CgroupScopeLock&) = delete;

  ~CgroupScopeLock() { Close(); }

 private:
  void Open(const std::filesystem::path& root) {
    fd_ = open(root.c_str(), O_RDONLY | O_DIRECTORY | O_CLOEXEC | O_NOFOLLOW);
    if (fd_ < 0) {
      throw std::runtime_error("open cgroup scope lock failed for " +
                               root.string() + ": " + std::strerror(errno));
    }
  }

  void Close() noexcept {
    if (fd_ >= 0) {
      static_cast<void>(close(fd_));
      fd_ = -1;
    }
  }

  [[noreturn]] void ThrowLockFailure(const std::filesystem::path& root,
                                     int error) {
    Close();
    throw std::runtime_error("lock cgroup scope failed for " + root.string() +
                             ": " + std::strerror(error));
  }

  int fd_ = -1;
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

CgroupPaths CgroupPathsForScope(const CgroupScopeConfig& config,
                                const std::string& run_id) {
  const std::filesystem::path simulator = config.root / config.simulator_name;
  return CgroupPaths{
      .root = config.root, .simulator = simulator, .run = simulator / run_id};
}

CgroupPathIdentity DirectoryIdentity(const std::filesystem::path& path,
                                     std::string_view description) {
  const int fd =
      open(path.c_str(), O_PATH | O_DIRECTORY | O_CLOEXEC | O_NOFOLLOW);
  if (fd < 0) {
    throw std::runtime_error("open " + std::string(description) +
                             " for ownership identity failed: " +
                             path.string() + ": " + std::strerror(errno));
  }
  struct stat status {};
  if (fstat(fd, &status) != 0 || !S_ISDIR(status.st_mode)) {
    const int error = errno;
    close(fd);
    throw std::runtime_error("read " + std::string(description) +
                             " ownership identity failed: " + path.string() +
                             ": " + std::strerror(error));
  }
  if (close(fd) != 0) {
    throw std::runtime_error("close " + std::string(description) +
                             " ownership identity failed: " + path.string() +
                             ": " + std::strerror(errno));
  }
  return CgroupPathIdentity{
      .device = static_cast<std::uint64_t>(status.st_dev),
      .inode = static_cast<std::uint64_t>(status.st_ino),
  };
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

CgroupScopeConfig ProductionCgroupScopeConfig() {
  return CgroupScopeConfig{
      .root = CgroupRoot(),
      .simulator_name = std::string(kSimulatorRootName),
      .state_file = std::filesystem::path(kCgroupScopeStateFile),
      .allow_root_process_move = RunningInsideDocker(),
  };
}

std::set<std::string> ControllerSet(const std::filesystem::path& path) {
  const std::vector<std::string> values = SplitWhitespace(ReadText(path));
  return std::set<std::string>(values.begin(), values.end());
}

std::set<std::string> DesiredControllers(
    const std::filesystem::path& directory) {
  const std::set<std::string> available =
      ControllerSet(directory / "cgroup.controllers");
  std::set<std::string> desired;
  for (std::string_view controller : {"cpu", "io", "memory", "pids"}) {
    if (available.contains(std::string(controller))) {
      desired.emplace(controller);
    }
  }
  return desired;
}

std::set<std::string> SetDifference(const std::set<std::string>& values,
                                    const std::set<std::string>& excluded) {
  std::set<std::string> difference;
  std::set_difference(values.begin(), values.end(), excluded.begin(),
                      excluded.end(),
                      std::inserter(difference, difference.end()));
  return difference;
}

std::string ControllerRequest(const std::set<std::string>& controllers,
                              char operation) {
  std::string request;
  for (const std::string& controller : controllers) {
    if (!request.empty()) {
      request += ' ';
    }
    request += operation;
    request += controller;
  }
  return request;
}

void SetControllers(const std::filesystem::path& directory,
                    const std::set<std::string>& controllers, char operation) {
  if (controllers.empty()) {
    return;
  }
  WriteText(directory / "cgroup.subtree_control",
            ControllerRequest(controllers, operation));
  const std::set<std::string> after =
      ControllerSet(directory / "cgroup.subtree_control");
  for (const std::string& controller : controllers) {
    const bool expected = operation == '+';
    if (after.contains(controller) != expected) {
      throw std::runtime_error("cgroup controller " + controller +
                               " read-back verification failed at " +
                               directory.string());
    }
  }
}

std::string RandomScopeToken() {
  std::array<unsigned char, 8U> bytes{};
  std::size_t offset = 0U;
  while (offset < bytes.size()) {
    const ssize_t count =
        getrandom(bytes.data() + offset, bytes.size() - offset, 0U);
    if (count < 0) {
      if (errno == EINTR) {
        continue;
      }
      throw std::runtime_error("getrandom for cgroup scope failed: " +
                               std::string(std::strerror(errno)));
    }
    if (count == 0) {
      throw std::runtime_error("getrandom for cgroup scope made no progress");
    }
    offset += static_cast<std::size_t>(count);
  }
  constexpr char kHex[] = "0123456789abcdef";
  std::string token;
  token.reserve(bytes.size() * 2U);
  for (const unsigned char byte : bytes) {
    token.push_back(kHex[byte >> 4U]);
    token.push_back(kHex[byte & 0x0fU]);
  }
  return token;
}

void WriteAll(int fd, std::string_view text,
              const std::filesystem::path& path) {
  while (!text.empty()) {
    const ssize_t count = write(fd, text.data(), text.size());
    if (count < 0) {
      if (errno == EINTR) {
        continue;
      }
      throw std::runtime_error("write cgroup scope state failed for " +
                               path.string() + ": " + std::strerror(errno));
    }
    if (count == 0) {
      throw std::runtime_error(
          "write cgroup scope state made no progress for " + path.string());
    }
    text.remove_prefix(static_cast<std::size_t>(count));
  }
}

std::string ReadOwnedScopeState(const std::filesystem::path& path) {
  const int fd = open(path.c_str(), O_RDONLY | O_CLOEXEC | O_NOFOLLOW);
  if (fd < 0) {
    throw std::runtime_error("open cgroup scope state failed for " +
                             path.string() + ": " + std::strerror(errno));
  }
  struct stat status {};
  if (fstat(fd, &status) != 0 || !S_ISREG(status.st_mode) ||
      status.st_uid != geteuid() || (status.st_mode & 0077U) != 0U) {
    close(fd);
    throw std::runtime_error(
        "cgroup scope state must be an owner-only regular file: " +
        path.string());
  }
  constexpr std::size_t kMaximumStateBytes = 64U * 1024U;
  std::string contents;
  std::array<char, 4096U> buffer{};
  try {
    for (;;) {
      const ssize_t count = read(fd, buffer.data(), buffer.size());
      if (count < 0) {
        if (errno == EINTR) {
          continue;
        }
        throw std::runtime_error("read cgroup scope state failed for " +
                                 path.string() + ": " + std::strerror(errno));
      }
      if (count == 0) {
        break;
      }
      const std::size_t received = static_cast<std::size_t>(count);
      if (received > kMaximumStateBytes - contents.size()) {
        throw std::runtime_error("cgroup scope state exceeds 64 KiB: " +
                                 path.string());
      }
      contents.append(buffer.data(), received);
    }
  } catch (...) {
    close(fd);
    throw;
  }
  if (close(fd) != 0) {
    throw std::runtime_error("close cgroup scope state failed for " +
                             path.string() + ": " + std::strerror(errno));
  }
  return contents;
}

boost::json::array StringSetJson(const std::set<std::string>& values) {
  boost::json::array result;
  for (const std::string& value : values) {
    result.emplace_back(value);
  }
  return result;
}

boost::json::object RunBindingsJson(
    const std::map<std::string, CgroupRunBinding>& bindings) {
  boost::json::object result;
  for (const auto& [cgroup_name, binding] : bindings) {
    result[cgroup_name] = boost::json::object{
        {"run_id", binding.run_id},
        {"run_root", binding.run_root.string()},
        {"resource_id", binding.resource_id},
        {"run_root_device", binding.run_root_identity.device},
        {"run_root_inode", binding.run_root_identity.inode},
        {"cgroup_device", binding.cgroup_identity.device},
        {"cgroup_inode", binding.cgroup_identity.inode},
    };
  }
  return result;
}

std::string SerializeCgroupScopeState(const CgroupScopeState& state) {
  boost::json::object object;
  object["version"] = kCgroupScopeStateVersion;
  object["root"] = state.root.string();
  object["simulator_name"] = state.simulator_name;
  object["controller_name"] = state.controller_name;
  object["simulator_preexisting"] = state.simulator_preexisting;
  object["root_controllers_before"] =
      StringSetJson(state.root_controllers_before);
  object["simulator_controllers_before"] =
      StringSetJson(state.simulator_controllers_before);
  object["root_controllers_added"] =
      StringSetJson(state.root_controllers_added);
  object["simulator_controllers_added"] =
      StringSetJson(state.simulator_controllers_added);
  object["active_runs"] = StringSetJson(state.active_runs);
  object["run_bindings"] = RunBindingsJson(state.run_bindings);
  if (state.pending_run) {
    object["pending_run"] = *state.pending_run;
  } else {
    object["pending_run"] = nullptr;
  }
  object["pending_run_created"] = state.pending_run_created;
  return boost::json::serialize(object) + "\n";
}

void WriteCgroupScopeState(const std::filesystem::path& path,
                           const CgroupScopeState& state) {
  const std::filesystem::path temporary = path.string() + ".tmp-" +
                                          std::to_string(getpid()) + "-" +
                                          RandomScopeToken();
  const int fd =
      open(temporary.c_str(),
           O_WRONLY | O_CREAT | O_EXCL | O_CLOEXEC | O_NOFOLLOW, 0600);
  if (fd < 0) {
    throw std::runtime_error("create cgroup scope state failed for " +
                             temporary.string() + ": " + std::strerror(errno));
  }
  try {
    WriteAll(fd, SerializeCgroupScopeState(state), temporary);
    if (fsync(fd) != 0) {
      throw std::runtime_error("fsync cgroup scope state failed for " +
                               temporary.string() + ": " +
                               std::strerror(errno));
    }
  } catch (...) {
    close(fd);
    unlink(temporary.c_str());
    throw;
  }
  if (close(fd) != 0) {
    const int error = errno;
    unlink(temporary.c_str());
    throw std::runtime_error("close cgroup scope state failed for " +
                             temporary.string() + ": " + std::strerror(error));
  }
  if (rename(temporary.c_str(), path.c_str()) != 0) {
    const int error = errno;
    unlink(temporary.c_str());
    throw std::runtime_error("publish cgroup scope state failed for " +
                             path.string() + ": " + std::strerror(error));
  }
}

const boost::json::value& RequiredScopeField(const boost::json::object& object,
                                             std::string_view field) {
  const boost::json::value* value = object.if_contains(field);
  if (value == nullptr) {
    throw std::runtime_error("cgroup scope state is missing field: " +
                             std::string(field));
  }
  return *value;
}

std::string ScopeString(const boost::json::object& object,
                        std::string_view field) {
  const boost::json::value& value = RequiredScopeField(object, field);
  if (!value.is_string()) {
    throw std::runtime_error("cgroup scope state field is not a string: " +
                             std::string(field));
  }
  return std::string(value.as_string());
}

std::uint64_t ScopeUint64(const boost::json::object& object,
                          std::string_view field) {
  const boost::json::value& value = RequiredScopeField(object, field);
  if (value.is_uint64()) {
    return value.as_uint64();
  }
  if (value.is_int64() && value.as_int64() >= 0) {
    return static_cast<std::uint64_t>(value.as_int64());
  }
  throw std::runtime_error("cgroup scope state field is not uint64: " +
                           std::string(field));
}

enum class ScopeSetKind { kRunIds, kControllers, kManagedControllers };

std::set<std::string> ScopeStringSet(const boost::json::object& object,
                                     std::string_view field,
                                     ScopeSetKind kind) {
  const boost::json::value& value = RequiredScopeField(object, field);
  if (!value.is_array()) {
    throw std::runtime_error("cgroup scope state field is not an array: " +
                             std::string(field));
  }
  std::set<std::string> result;
  for (const boost::json::value& entry : value.as_array()) {
    if (!entry.is_string()) {
      throw std::runtime_error("cgroup scope state array is not textual: " +
                               std::string(field));
    }
    const std::string text(entry.as_string());
    if (kind != ScopeSetKind::kRunIds) {
      if (text.empty() ||
          !std::all_of(text.begin(), text.end(), [](char character) {
            return (character >= 'a' && character <= 'z') ||
                   (character >= '0' && character <= '9') || character == '_';
          })) {
        throw std::runtime_error(
            "cgroup scope state contains an invalid controller: " + text);
      }
      if (kind == ScopeSetKind::kManagedControllers && text != "cpu" &&
          text != "io" && text != "memory" && text != "pids") {
        throw std::runtime_error(
            "cgroup scope state contains an unmanaged controller: " + text);
      }
    } else {
      RequireSafeRunId(text);
    }
    if (!result.insert(text).second) {
      throw std::runtime_error("cgroup scope state contains a duplicate: " +
                               text);
    }
  }
  return result;
}

std::map<std::string, CgroupRunBinding> ScopeRunBindings(
    const boost::json::object& object) {
  const boost::json::value& value = RequiredScopeField(object, "run_bindings");
  if (!value.is_object()) {
    throw std::runtime_error(
        "cgroup scope state run_bindings is not an object");
  }
  std::map<std::string, CgroupRunBinding> bindings;
  for (const auto& member : value.as_object()) {
    const std::string cgroup_name(member.key());
    if (cgroup_name.size() != 32U ||
        !std::all_of(cgroup_name.begin(), cgroup_name.end(),
                     [](char value) {
                       return (value >= '0' && value <= '9') ||
                              (value >= 'a' && value <= 'f');
                     }) ||
        !member.value().is_object()) {
      throw std::runtime_error(
          "cgroup scope state contains an invalid run binding");
    }
    const boost::json::object& binding_object = member.value().as_object();
    const std::set<std::string_view> fields = {
        "run_id",         "run_root",      "resource_id",  "run_root_device",
        "run_root_inode", "cgroup_device", "cgroup_inode",
    };
    for (const auto& field : binding_object) {
      if (!fields.contains(field.key())) {
        throw std::runtime_error(
            "cgroup scope run binding has an unsupported field");
      }
    }
    CgroupRunBinding binding{
        .run_id = ScopeString(binding_object, "run_id"),
        .run_root = ScopeString(binding_object, "run_root"),
        .resource_id = ScopeString(binding_object, "resource_id"),
        .run_root_identity =
            CgroupPathIdentity{
                .device = ScopeUint64(binding_object, "run_root_device"),
                .inode = ScopeUint64(binding_object, "run_root_inode"),
            },
        .cgroup_identity =
            CgroupPathIdentity{
                .device = ScopeUint64(binding_object, "cgroup_device"),
                .inode = ScopeUint64(binding_object, "cgroup_inode"),
            },
    };
    RequireSafeRunId(binding.run_id);
    if (!binding.run_root.is_absolute() ||
        binding.run_root.lexically_normal() != binding.run_root ||
        binding.resource_id != cgroup_name ||
        binding.run_root_identity.inode == 0U ||
        binding.cgroup_identity.inode == 0U) {
      throw std::runtime_error(
          "cgroup scope state run binding is inconsistent");
    }
    bindings.emplace(cgroup_name, std::move(binding));
  }
  return bindings;
}

CgroupScopeState LoadCgroupScopeState(const CgroupScopeConfig& config) {
  const boost::json::value parsed =
      boost::json::parse(ReadOwnedScopeState(config.state_file));
  if (!parsed.is_object()) {
    throw std::runtime_error("cgroup scope state is not a JSON object");
  }
  const boost::json::object& object = parsed.as_object();
  const std::set<std::string_view> supported = {
      "version",
      "root",
      "simulator_name",
      "controller_name",
      "simulator_preexisting",
      "root_controllers_before",
      "simulator_controllers_before",
      "root_controllers_added",
      "simulator_controllers_added",
      "active_runs",
      "run_bindings",
      "pending_run",
      "pending_run_created",
  };
  for (const auto& member : object) {
    const std::string_view key(member.key().data(), member.key().size());
    if (!supported.contains(key)) {
      throw std::runtime_error("cgroup scope state has unsupported field: " +
                               std::string(key));
    }
  }
  const boost::json::value& version = RequiredScopeField(object, "version");
  const std::uint64_t version_number =
      version.is_uint64() ? version.as_uint64()
      : version.is_int64() && version.as_int64() >= 0
          ? static_cast<std::uint64_t>(version.as_int64())
          : 0U;
  if (version_number != 1U && version_number != kCgroupScopeStateVersion) {
    throw std::runtime_error("cgroup scope state version is unsupported");
  }
  const boost::json::value& preexisting =
      RequiredScopeField(object, "simulator_preexisting");
  if (!preexisting.is_bool()) {
    throw std::runtime_error(
        "cgroup scope simulator_preexisting is not Boolean");
  }
  CgroupScopeState state;
  state.root = ScopeString(object, "root");
  state.simulator_name = ScopeString(object, "simulator_name");
  state.controller_name = ScopeString(object, "controller_name");
  state.simulator_preexisting = preexisting.as_bool();
  state.root_controllers_before = ScopeStringSet(
      object, "root_controllers_before", ScopeSetKind::kControllers);
  state.simulator_controllers_before = ScopeStringSet(
      object, "simulator_controllers_before", ScopeSetKind::kControllers);
  state.root_controllers_added = ScopeStringSet(
      object, "root_controllers_added", ScopeSetKind::kManagedControllers);
  state.simulator_controllers_added = ScopeStringSet(
      object, "simulator_controllers_added", ScopeSetKind::kManagedControllers);
  state.active_runs =
      ScopeStringSet(object, "active_runs", ScopeSetKind::kRunIds);
  state.run_bindings = object.if_contains("run_bindings") != nullptr
                           ? ScopeRunBindings(object)
                           : std::map<std::string, CgroupRunBinding>{};
  const boost::json::value& pending = RequiredScopeField(object, "pending_run");
  if (pending.is_string()) {
    state.pending_run = std::string(pending.as_string());
    RequireSafeRunId(*state.pending_run);
  } else if (!pending.is_null()) {
    throw std::runtime_error(
        "cgroup scope state pending_run is not a string or null");
  }
  const boost::json::value& pending_created =
      RequiredScopeField(object, "pending_run_created");
  if (!pending_created.is_bool()) {
    throw std::runtime_error(
        "cgroup scope state pending_run_created is not Boolean");
  }
  state.pending_run_created = pending_created.as_bool();
  if (!state.pending_run && state.pending_run_created) {
    throw std::runtime_error(
        "cgroup scope state marks a missing pending run as created");
  }
  for (const auto& [cgroup_name, binding] : state.run_bindings) {
    static_cast<void>(binding);
    const bool recoverable_pending_binding =
        state.pending_run_created && state.pending_run == cgroup_name;
    if (!state.active_runs.contains(cgroup_name) &&
        !recoverable_pending_binding) {
      throw std::runtime_error("cgroup scope state binding has no active run");
    }
  }
  if (state.root != config.root ||
      state.simulator_name != config.simulator_name ||
      !state.controller_name.starts_with(kOwnedControllerPrefix) ||
      state.controller_name.size() != kOwnedControllerPrefix.size() + 16U) {
    throw std::runtime_error(
        "cgroup scope state does not match the requested scope");
  }
  const std::set<std::string> desired = DesiredControllers(config.root);
  if (state.root_controllers_added !=
          SetDifference(desired, state.root_controllers_before) ||
      state.simulator_controllers_added !=
          SetDifference(desired, state.simulator_controllers_before)) {
    throw std::runtime_error("cgroup scope root controller state is invalid");
  }
  return state;
}

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

std::set<std::string> RequiredControllers() {
  return {"cpu", "io", "memory", "pids"};
}

std::string ControllerList(const std::set<std::string>& controllers) {
  std::string result;
  for (const std::string& controller : controllers) {
    if (!result.empty()) {
      result += ", ";
    }
    result += controller;
  }
  return result;
}

void RequireNativeCgroupRoot(const std::filesystem::path& root) {
  struct statfs filesystem_status {};
  if (statfs(root.c_str(), &filesystem_status) != 0) {
    throw std::runtime_error("inspect native cgroup root failed at " +
                             root.string() + ": " + std::strerror(errno));
  }
  if (filesystem_status.f_type != CGROUP2_SUPER_MAGIC) {
    throw std::runtime_error("cgroup v2 is not mounted at native root " +
                             root.string());
  }
  if (access(root.c_str(), W_OK) != 0 ||
      access((root / "cgroup.subtree_control").c_str(), W_OK) != 0) {
    throw std::runtime_error("native cgroup root is not writable at " +
                             root.string() + ": " + std::strerror(errno));
  }

  const std::set<std::string> required = RequiredControllers();
  const std::set<std::string> unavailable =
      SetDifference(required, ControllerSet(root / "cgroup.controllers"));
  if (!unavailable.empty()) {
    throw std::runtime_error(
        "required native cgroup root controllers unavailable at " +
        root.string() + ": " + ControllerList(unavailable));
  }
  const std::set<std::string> not_enabled =
      SetDifference(required, ControllerSet(root / "cgroup.subtree_control"));
  if (!not_enabled.empty()) {
    throw std::runtime_error(
        "required native cgroup root controllers not enabled at " +
        root.string() + ": " + ControllerList(not_enabled));
  }
}

bool CgroupProcsEmpty(const std::filesystem::path& dir) {
  return SplitWhitespace(ReadText(dir / "cgroup.procs")).empty();
}

std::vector<pid_t> CgroupPids(const std::filesystem::path& dir) {
  std::vector<pid_t> pids;
  for (const std::string& text :
       SplitWhitespace(ReadText(dir / "cgroup.procs"))) {
    pid_t pid = 0;
    const auto parsed =
        std::from_chars(text.data(), text.data() + text.size(), pid);
    if (parsed.ec != std::errc{} || parsed.ptr != text.data() + text.size() ||
        pid <= 0) {
      throw std::runtime_error("cgroup.procs contains an invalid PID: " + text);
    }
    pids.push_back(pid);
  }
  return pids;
}

int OpenPidfd(pid_t pid) {
#ifdef SYS_pidfd_open
  for (;;) {
    const int descriptor = static_cast<int>(syscall(SYS_pidfd_open, pid, 0U));
    if (descriptor >= 0 || errno == ESRCH) {
      return descriptor;
    }
    if (errno != EINTR) {
      throw std::runtime_error("pidfd_open failed for cgroup process " +
                               std::to_string(pid) + ": " +
                               std::strerror(errno));
    }
  }
#else
  static_cast<void>(pid);
  throw std::runtime_error(
      "pidfd_open is unavailable for cgroup process cleanup");
#endif
}

void SignalPidfd(int descriptor, pid_t pid) {
#ifdef SYS_pidfd_send_signal
  for (;;) {
    if (syscall(SYS_pidfd_send_signal, descriptor, SIGKILL, nullptr, 0U) == 0 ||
        errno == ESRCH) {
      return;
    }
    if (errno != EINTR) {
      throw std::runtime_error("pidfd_send_signal failed for cgroup process " +
                               std::to_string(pid) + ": " +
                               std::strerror(errno));
    }
  }
#else
  static_cast<void>(descriptor);
  static_cast<void>(pid);
  throw std::runtime_error(
      "pidfd_send_signal is unavailable for cgroup process cleanup");
#endif
}

void WaitForCgroupProcsEmpty(const std::filesystem::path& dir,
                             std::chrono::steady_clock::time_point deadline,
                             std::stop_token stop_token) {
  while (true) {
    if (stop_token.stop_requested()) {
      throw std::runtime_error("cgroup process wait cancelled: " +
                               dir.string());
    }
    const auto now = std::chrono::steady_clock::now();
    if (now >= deadline) {
      throw std::runtime_error("cgroup process wait deadline expired: " +
                               dir.string());
    }
    if (CgroupProcsEmpty(dir)) {
      return;
    }
    std::this_thread::sleep_for(std::min(
        std::chrono::milliseconds(20),
        std::chrono::duration_cast<std::chrono::milliseconds>(deadline - now)));
  }
}

void KillCgroupProcesses(const std::filesystem::path& dir,
                         std::chrono::steady_clock::time_point deadline,
                         std::stop_token stop_token,
                         bool force_pidfd_fallback = false) {
  if (!force_pidfd_fallback && std::filesystem::exists(dir / "cgroup.kill")) {
    WriteCgroupFile(dir, "cgroup.kill", "1");
    WaitForCgroupProcsEmpty(dir, deadline, stop_token);
    return;
  }
  while (true) {
    if (stop_token.stop_requested()) {
      throw std::runtime_error("cgroup process kill cancelled: " +
                               dir.string());
    }
    if (std::chrono::steady_clock::now() >= deadline) {
      throw std::runtime_error("cgroup process kill deadline expired: " +
                               dir.string());
    }
    const std::vector<pid_t> pids = CgroupPids(dir);
    if (pids.empty()) {
      return;
    }
    for (const pid_t pid : pids) {
      if (stop_token.stop_requested()) {
        throw std::runtime_error("cgroup process kill cancelled: " +
                                 dir.string());
      }
      if (std::chrono::steady_clock::now() >= deadline) {
        throw std::runtime_error("cgroup process kill deadline expired: " +
                                 dir.string());
      }
      const int pidfd = OpenPidfd(pid);
      if (pidfd < 0) {
        continue;
      }
      try {
        const std::vector<pid_t> current = CgroupPids(dir);
        if (std::find(current.begin(), current.end(), pid) != current.end()) {
          if (stop_token.stop_requested()) {
            throw std::runtime_error("cgroup process kill cancelled: " +
                                     dir.string());
          }
          if (std::chrono::steady_clock::now() >= deadline) {
            throw std::runtime_error("cgroup process kill deadline expired: " +
                                     dir.string());
          }
          SignalPidfd(pidfd, pid);
        }
      } catch (...) {
        close(pidfd);
        throw;
      }
      if (close(pidfd) != 0) {
        throw std::runtime_error("close pidfd failed for cgroup process " +
                                 std::to_string(pid) + ": " +
                                 std::strerror(errno));
      }
      if (stop_token.stop_requested()) {
        throw std::runtime_error("cgroup process kill cancelled: " +
                                 dir.string());
      }
      if (std::chrono::steady_clock::now() >= deadline) {
        throw std::runtime_error("cgroup process kill deadline expired: " +
                                 dir.string());
      }
    }
    const auto now = std::chrono::steady_clock::now();
    if (now < deadline) {
      std::this_thread::sleep_for(std::min(
          std::chrono::milliseconds(5),
          std::chrono::duration_cast<std::chrono::milliseconds>(deadline -
                                                                now)));
    }
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

void MoveCgroupProcesses(const std::filesystem::path& source,
                         const std::filesystem::path& destination,
                         std::string_view context) {
  for (int attempt = 0; attempt < 20; ++attempt) {
    const std::vector<std::string> pids =
        SplitWhitespace(ReadText(source / "cgroup.procs"));
    if (pids.empty()) {
      return;
    }
    for (const std::string& pid : pids) {
      try {
        WriteText(destination / "cgroup.procs", pid);
      } catch (const std::exception&) {
        // A PID can exit between reading cgroup.procs and writing it.
      }
    }
    if (CgroupProcsEmpty(source)) {
      return;
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(20));
  }

  throw std::runtime_error(
      std::string(context) +
      "; source cgroup still has processes: " + source.string());
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
    const std::filesystem::path& run_root,
    std::chrono::steady_clock::time_point deadline,
    std::stop_token stop_token) {
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
    if (stop_token.stop_requested()) {
      throw std::runtime_error("cgroup traversal cancelled for " +
                               run_root.string());
    }
    if (std::chrono::steady_clock::now() >= deadline) {
      throw std::runtime_error("cgroup traversal deadline expired for " +
                               run_root.string());
    }
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

void RemoveRunCgroup(const CgroupScopeConfig& config, const std::string& run_id,
                     std::chrono::steady_clock::time_point deadline,
                     std::stop_token stop_token,
                     bool force_pidfd_fallback = false) {
  const std::filesystem::path run_root =
      CgroupPathsForScope(config, run_id).run;
  if (!std::filesystem::exists(run_root)) {
    return;
  }
  for (const std::filesystem::path& path :
       DescendantCgroupsDeepestFirst(run_root, deadline, stop_token)) {
    KillCgroupProcesses(path, deadline, stop_token, force_pidfd_fallback);
    RemoveCgroupDirectory(path, "descendant");
  }
  KillCgroupProcesses(run_root, deadline, stop_token, force_pidfd_fallback);
  RemoveCgroupDirectory(run_root, "run");
}

bool ScopeStateExists(const CgroupScopeConfig& config) {
  struct stat status {};
  if (lstat(config.state_file.c_str(), &status) == 0) {
    return true;
  }
  if (errno == ENOENT) {
    return false;
  }
  throw std::runtime_error("inspect cgroup scope state failed for " +
                           config.state_file.string() + ": " +
                           std::strerror(errno));
}

bool IsResourceCgroupName(std::string_view name) {
  if (name.size() != 32U) {
    return false;
  }
  return std::all_of(name.begin(), name.end(), [](char character) {
    return (character >= '0' && character <= '9') ||
           (character >= 'a' && character <= 'f');
  });
}

void RecordExistingResourceCgroups(const std::filesystem::path& simulator,
                                   CgroupScopeState* state) {
  if (!std::filesystem::exists(simulator)) {
    return;
  }
  std::error_code error;
  std::filesystem::directory_iterator iterator(simulator, error);
  const std::filesystem::directory_iterator end;
  if (error) {
    throw std::runtime_error("list simulator cgroups failed for " +
                             simulator.string() + ": " + error.message());
  }
  while (iterator != end) {
    const std::string name = iterator->path().filename().string();
    if (IsResourceCgroupName(name) && iterator->is_directory(error)) {
      state->active_runs.insert(name);
    }
    if (error) {
      throw std::runtime_error("inspect simulator cgroup failed for " +
                               iterator->path().string() + ": " +
                               error.message());
    }
    iterator.increment(error);
    if (error) {
      throw std::runtime_error("list simulator cgroups failed for " +
                               simulator.string() + ": " + error.message());
    }
  }
}

CgroupScopeState NewCgroupScopeState(const CgroupScopeConfig& config) {
  const CgroupPaths paths = CgroupPathsForScope(config, "state-probe");
  std::error_code error;
  const std::filesystem::file_status simulator_status =
      std::filesystem::symlink_status(paths.simulator, error);
  const bool simulator_missing =
      error == std::errc::no_such_file_or_directory ||
      (!error &&
       simulator_status.type() == std::filesystem::file_type::not_found);
  if (error && error != std::errc::no_such_file_or_directory) {
    throw std::runtime_error("inspect simulator cgroup failed for " +
                             paths.simulator.string() + ": " + error.message());
  }
  const bool simulator_preexisting = !simulator_missing;
  if (simulator_preexisting &&
      !std::filesystem::is_directory(simulator_status)) {
    throw std::runtime_error("simulator cgroup path is not a real directory: " +
                             paths.simulator.string());
  }

  RequireControllersAvailable(paths.root, {"cpu", "io", "memory", "pids"});
  const std::set<std::string> desired = DesiredControllers(paths.root);
  CgroupScopeState state{
      .root = paths.root,
      .simulator_name = config.simulator_name,
      .controller_name =
          std::string(kOwnedControllerPrefix) + RandomScopeToken(),
      .simulator_preexisting = simulator_preexisting,
      .root_controllers_before =
          ControllerSet(paths.root / "cgroup.subtree_control"),
      .simulator_controllers_before =
          simulator_preexisting
              ? ControllerSet(paths.simulator / "cgroup.subtree_control")
              : std::set<std::string>{},
      .root_controllers_added = {},
      .simulator_controllers_added = {},
      .active_runs = {},
      .run_bindings = {},
      .pending_run = std::nullopt,
      .pending_run_created = false,
  };
  state.root_controllers_added =
      SetDifference(desired, state.root_controllers_before);
  state.simulator_controllers_added =
      SetDifference(desired, state.simulator_controllers_before);
  RecordExistingResourceCgroups(paths.simulator, &state);
  return state;
}

void StartCgroupScope(const CgroupScopeConfig& config,
                      const CgroupScopeState& state) {
  const CgroupPaths paths = CgroupPathsForScope(config, "state-probe");
  if (!config.allow_root_process_move &&
      !state.root_controllers_added.empty()) {
    throw std::runtime_error(
        "native cgroup root unexpectedly requires controller mutation at " +
        paths.root.string() + ": " +
        ControllerList(state.root_controllers_added));
  }
  if (!state.simulator_preexisting) {
    CreateCgroupDirectoryExclusive(paths.simulator, "simulator");
  }
  if (config.allow_root_process_move) {
    const std::filesystem::path controller =
        paths.simulator / state.controller_name;
    CreateCgroupDirectoryExclusive(controller, "controller");
    if (!CgroupProcsEmpty(paths.root)) {
      MoveCgroupProcesses(paths.root, controller,
                          "could not delegate cgroup root");
    }
    SetControllers(paths.root, state.root_controllers_added, '+');
  }
  RequireControllersAvailable(paths.simulator, {"cpu", "io", "memory", "pids"});
  SetControllers(paths.simulator, state.simulator_controllers_added, '+');
}

void RequireControllersPreserved(const std::filesystem::path& directory,
                                 const std::set<std::string>& before,
                                 const std::set<std::string>& added) {
  const std::set<std::string> after =
      ControllerSet(directory / "cgroup.subtree_control");
  for (const std::string& controller : before) {
    if (!after.contains(controller)) {
      throw std::runtime_error(
          "pre-existing cgroup controller disappeared at " +
          directory.string() + ": " + controller);
    }
  }
  for (const std::string& controller : added) {
    if (after.contains(controller)) {
      throw std::runtime_error(
          "BBP-added cgroup controller survived cleanup at " +
          directory.string() + ": " + controller);
    }
  }
}

void RemoveScopeStateFile(const CgroupScopeConfig& config) {
  if (unlink(config.state_file.c_str()) != 0) {
    throw std::runtime_error("remove cgroup scope state failed for " +
                             config.state_file.string() + ": " +
                             std::strerror(errno));
  }
  struct stat status {};
  if (lstat(config.state_file.c_str(), &status) == 0 || errno != ENOENT) {
    throw std::runtime_error(
        "cgroup scope state absence read-back failed for " +
        config.state_file.string());
  }
}

void RestoreCgroupScope(const CgroupScopeConfig& config,
                        const CgroupScopeState& state) {
  if (!state.active_runs.empty() || !state.run_bindings.empty() ||
      state.pending_run) {
    throw std::runtime_error(
        "refusing to restore cgroup scope while owned runs remain");
  }
  const CgroupPaths paths = CgroupPathsForScope(config, "state-probe");
  const std::filesystem::path controller =
      paths.simulator / state.controller_name;
  if (std::filesystem::exists(paths.simulator)) {
    SetControllers(paths.simulator, state.simulator_controllers_added, '-');
  } else if (state.simulator_preexisting) {
    throw std::runtime_error("pre-existing simulator cgroup disappeared: " +
                             paths.simulator.string());
  }
  SetControllers(paths.root, state.root_controllers_added, '-');
  if (std::filesystem::exists(controller)) {
    MoveCgroupProcesses(controller, paths.root,
                        "could not restore cgroup-root processes");
    RemoveCgroupDirectory(controller, "controller");
  }
  if (!state.simulator_preexisting &&
      std::filesystem::exists(paths.simulator)) {
    RemoveCgroupDirectory(paths.simulator, "simulator");
  }
  RequireControllersPreserved(paths.root, state.root_controllers_before,
                              state.root_controllers_added);
  if (state.simulator_preexisting) {
    RequireControllersPreserved(paths.simulator,
                                state.simulator_controllers_before,
                                state.simulator_controllers_added);
  }
  RemoveScopeStateFile(config);
}

std::string CurrentExceptionText() {
  try {
    throw;
  } catch (const std::exception& error) {
    return error.what();
  } catch (...) {
    return "unknown exception";
  }
}

void RecoverInterruptedCgroupPreparation(const CgroupScopeConfig& config,
                                         CgroupScopeState* state) {
  if (!state->pending_run) {
    return;
  }
  if (state->pending_run_created) {
    const auto binding = state->run_bindings.find(*state->pending_run);
    if (binding == state->run_bindings.end()) {
      throw std::runtime_error(
          "refusing recovery deletion for an unbound pending run cgroup");
    }
    const std::filesystem::path run_cgroup =
        CgroupPathsForScope(config, *state->pending_run).run;
    if (DirectoryIdentity(binding->second.run_root, "run root") !=
            binding->second.run_root_identity ||
        DirectoryIdentity(run_cgroup, "run cgroup") !=
            binding->second.cgroup_identity) {
      throw std::runtime_error(
          "refusing recovery deletion for a replaced pending run resource");
    }
    RemoveRunCgroup(config, *state->pending_run,
                    std::chrono::steady_clock::now() + std::chrono::seconds(10),
                    {});
  }
  state->run_bindings.erase(*state->pending_run);
  state->pending_run.reset();
  state->pending_run_created = false;
  WriteCgroupScopeState(config.state_file, *state);
}

void PrepareRunInScope(const CgroupScopeConfig& config,
                       const std::string& run_id,
                       const RunOwnership* ownership = nullptr) {
  RequireSafeRunId(run_id);
  std::optional<CgroupRunBinding> run_binding;
  if (ownership != nullptr) {
    const RunOwnership loaded =
        LoadRunOwnership(ownership->run_id, ownership->run_root);
    if (loaded != *ownership || ownership->cgroup_name != run_id ||
        ownership->resource_id != run_id) {
      throw std::runtime_error(
          "run cgroup ownership does not match the prepared resource");
    }
    run_binding = CgroupRunBinding{
        .run_id = ownership->run_id,
        .run_root = ownership->run_root,
        .resource_id = ownership->resource_id,
        .run_root_identity = DirectoryIdentity(ownership->run_root, "run root"),
        .cgroup_identity = {},
    };
  }
  if (!std::filesystem::exists(config.root / "cgroup.controllers")) {
    throw std::runtime_error("cgroup v2 is not mounted at " +
                             config.root.string());
  }
  CgroupScopeLock scope_lock(config.root);
  if (!config.allow_root_process_move) {
    RequireNativeCgroupRoot(config.root);
  }
  const CgroupPaths requested_paths = CgroupPathsForScope(config, run_id);
  const bool scope_state_exists = ScopeStateExists(config);
  if (!scope_state_exists && std::filesystem::exists(requested_paths.run)) {
    throw std::runtime_error(
        "refusing to adopt pre-existing run cgroup: " +
        requested_paths.run.string());
  }
  std::optional<CgroupScopeState> state;
  bool run_created = false;
  bool attempted_run = false;
  bool scope_published = false;
  try {
    if (scope_state_exists) {
      state = LoadCgroupScopeState(config);
      RecoverInterruptedCgroupPreparation(config, &*state);
      for (auto iterator = state->active_runs.begin();
           iterator != state->active_runs.end();) {
        if (!std::filesystem::exists(
                CgroupPathsForScope(config, *iterator).run)) {
          state->run_bindings.erase(*iterator);
          iterator = state->active_runs.erase(iterator);
        } else {
          ++iterator;
        }
      }
      if (state->active_runs.empty()) {
        WriteCgroupScopeState(config.state_file, *state);
        RestoreCgroupScope(config, *state);
        state.reset();
      }
    }
    if (!state) {
      state = NewCgroupScopeState(config);
      if (state->active_runs.contains(run_id)) {
        throw std::runtime_error(
            "refusing to adopt pre-existing run cgroup: " +
            CgroupPathsForScope(config, run_id).run.string());
      }
      WriteCgroupScopeState(config.state_file, *state);
      scope_published = true;
      StartCgroupScope(config, *state);
    }
    if (state->active_runs.contains(run_id)) {
      throw std::runtime_error(
          "run cgroup is already prepared in this cgroup scope: " + run_id);
    }
    attempted_run = true;
    state->pending_run = run_id;
    state->pending_run_created = false;
    WriteCgroupScopeState(config.state_file, *state);

    const CgroupPaths paths = CgroupPathsForScope(config, run_id);
    CreateCgroupDirectoryExclusive(paths.run, "run");
    run_created = true;
    if (run_binding) {
      run_binding->cgroup_identity = DirectoryIdentity(paths.run, "run cgroup");
      state->run_bindings.emplace(run_id, *run_binding);
    }
    state->pending_run_created = true;
    WriteCgroupScopeState(config.state_file, *state);
    RequireControllersAvailable(paths.run, {"cpu", "io", "memory", "pids"});
    EnableControllers(paths.run);
    state->active_runs.insert(run_id);
    state->pending_run.reset();
    state->pending_run_created = false;
    WriteCgroupScopeState(config.state_file, *state);
  } catch (...) {
    const std::string original = CurrentExceptionText();
    std::optional<std::string> rollback_error;
    try {
      if (state && (attempted_run || scope_published)) {
        if (run_created) {
          RemoveRunCgroup(
              config, run_id,
              std::chrono::steady_clock::now() + std::chrono::seconds(10), {});
        }
        if (attempted_run) {
          state->active_runs.erase(run_id);
          state->run_bindings.erase(run_id);
          if (state->pending_run == run_id) {
            state->pending_run.reset();
            state->pending_run_created = false;
          }
        }
        WriteCgroupScopeState(config.state_file, *state);
        if (state->active_runs.empty()) {
          RestoreCgroupScope(config, *state);
        }
      }
    } catch (...) {
      rollback_error = CurrentExceptionText();
    }
    std::string message = original;
    if (rollback_error) {
      message += "; cgroup scope rollback failed: " + *rollback_error;
    }
    throw std::runtime_error(message);
  }
}

void RemoveRunInScope(const CgroupScopeConfig& config,
                      const std::string& run_id,
                      const RunOwnership* expected_ownership,
                      std::chrono::steady_clock::time_point deadline,
                      std::stop_token stop_token) {
  RequireSafeRunId(run_id);
  CgroupScopeLock scope_lock(config.root, deadline, stop_token);
  if (!ScopeStateExists(config)) {
    if (std::filesystem::exists(CgroupPathsForScope(config, run_id).run)) {
      throw std::runtime_error(
          "refusing to remove a run cgroup without scope ownership state: " +
          run_id);
    }
    return;
  }
  CgroupScopeState state = LoadCgroupScopeState(config);
  if (!state.active_runs.contains(run_id) && state.pending_run != run_id) {
    throw std::runtime_error(
        "refusing to remove a run cgroup absent from scope ownership state: " +
        run_id);
  }
  const auto binding = state.run_bindings.find(run_id);
  if (expected_ownership != nullptr) {
    if (binding == state.run_bindings.end()) {
      throw std::runtime_error(
          "refusing stale cgroup cleanup for an unbound legacy scope entry");
    }
    const RunOwnership loaded = LoadRunOwnership(expected_ownership->run_id,
                                                 expected_ownership->run_root);
    const CgroupRunBinding expected{
        .run_id = expected_ownership->run_id,
        .run_root = expected_ownership->run_root,
        .resource_id = expected_ownership->resource_id,
        .run_root_identity =
            DirectoryIdentity(expected_ownership->run_root, "run root"),
        .cgroup_identity = binding->second.cgroup_identity,
    };
    if (loaded != *expected_ownership ||
        expected_ownership->cgroup_name != run_id ||
        binding->second != expected) {
      throw std::runtime_error(
          "stale cgroup scope binding does not match exact run ownership");
    }
  }
  const std::filesystem::path run_cgroup =
      CgroupPathsForScope(config, run_id).run;
  if (binding != state.run_bindings.end() &&
      std::filesystem::exists(run_cgroup) &&
      DirectoryIdentity(run_cgroup, "run cgroup") !=
          binding->second.cgroup_identity) {
    throw std::runtime_error(
        "refusing to remove a replaced run cgroup identity: " + run_id);
  }
  RemoveRunCgroup(config, run_id, deadline, stop_token);
  if (state.pending_run == run_id) {
    state.pending_run.reset();
    state.pending_run_created = false;
  }
  state.active_runs.erase(run_id);
  state.run_bindings.erase(run_id);
  WriteCgroupScopeState(config.state_file, state);
  if (state.active_runs.empty() && !state.pending_run) {
    RestoreCgroupScope(config, state);
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

Cgroup::Cgroup(std::filesystem::path path)
    : path_(std::filesystem::absolute(std::move(path)).lexically_normal()) {
  name_ = path_.filename().string();
  if (name_.empty() || name_ == "." || name_ == "..") {
    throw std::runtime_error("cgroup path has no safe directory name: " +
                             path_.string());
  }
  std::filesystem::path parent = path_.parent_path();
  if (parent.empty()) {
    parent = ".";
  }
  parent_fd_ =
      open(parent.c_str(), O_PATH | O_DIRECTORY | O_CLOEXEC | O_NOFOLLOW);
  if (parent_fd_ < 0) {
    throw std::runtime_error("open cgroup parent failed for " + path_.string() +
                             ": " + std::strerror(errno));
  }
  fd_ = openat(parent_fd_, name_.c_str(),
               O_PATH | O_DIRECTORY | O_CLOEXEC | O_NOFOLLOW);
  if (fd_ < 0) {
    const int error = errno;
    Close();
    throw std::runtime_error("open cgroup identity failed for " +
                             path_.string() + ": " + std::strerror(error));
  }
  struct stat status {};
  if (fstat(fd_, &status) != 0 || !S_ISDIR(status.st_mode)) {
    const int error = errno == 0 ? ENOTDIR : errno;
    Close();
    throw std::runtime_error("inspect cgroup identity failed for " +
                             path_.string() + ": " + std::strerror(error));
  }
  device_ = static_cast<std::uint64_t>(status.st_dev);
  inode_ = static_cast<std::uint64_t>(status.st_ino);
  try {
    RequireBoundIdentity();
  } catch (...) {
    Close();
    throw;
  }
}

Cgroup::Cgroup(Cgroup&& other) noexcept { *this = std::move(other); }

Cgroup& Cgroup::operator=(Cgroup&& other) noexcept {
  if (this == &other) {
    return *this;
  }
  Close();
  path_ = std::move(other.path_);
  name_ = std::move(other.name_);
  parent_fd_ = other.parent_fd_;
  fd_ = other.fd_;
  device_ = other.device_;
  inode_ = other.inode_;
  removed_ = other.removed_;
  other.parent_fd_ = -1;
  other.fd_ = -1;
  other.device_ = 0U;
  other.inode_ = 0U;
  other.removed_ = true;
  return *this;
}

Cgroup::~Cgroup() { Close(); }

void Cgroup::Close() noexcept {
  if (fd_ >= 0) {
    static_cast<void>(close(fd_));
    fd_ = -1;
  }
  if (parent_fd_ >= 0) {
    static_cast<void>(close(parent_fd_));
    parent_fd_ = -1;
  }
}

void Cgroup::RequireBoundIdentity() const {
  if (removed_) {
    throw std::runtime_error("cgroup was already removed: " + path_.string());
  }
  if (fd_ < 0 || parent_fd_ < 0) {
    throw std::runtime_error("cgroup has no acquired directory identity: " +
                             path_.string());
  }
  const auto exact_identity = [&](const struct stat& status) {
    return S_ISDIR(status.st_mode) &&
           static_cast<std::uint64_t>(status.st_dev) == device_ &&
           static_cast<std::uint64_t>(status.st_ino) == inode_;
  };
  struct stat opened {};
  if (fstat(fd_, &opened) != 0) {
    throw std::runtime_error("inspect acquired cgroup fd failed for " +
                             path_.string() + ": " + std::strerror(errno));
  }
  if (!exact_identity(opened)) {
    throw std::runtime_error("acquired cgroup fd identity changed for " +
                             path_.string());
  }
  struct stat parent_entry {};
  if (fstatat(parent_fd_, name_.c_str(), &parent_entry,
              AT_SYMLINK_NOFOLLOW) != 0) {
    throw std::runtime_error("acquired cgroup path disappeared for " +
                             path_.string() + ": " + std::strerror(errno));
  }
  if (!exact_identity(parent_entry)) {
    throw std::runtime_error(
        "refusing replaced cgroup directory identity: " + path_.string());
  }
  struct stat path_entry {};
  if (fstatat(AT_FDCWD, path_.c_str(), &path_entry, AT_SYMLINK_NOFOLLOW) != 0) {
    throw std::runtime_error("acquired cgroup pathname disappeared for " +
                             path_.string() + ": " + std::strerror(errno));
  }
  if (!exact_identity(path_entry)) {
    throw std::runtime_error(
        "refusing replaced cgroup pathname identity: " + path_.string());
  }
}

std::filesystem::path Cgroup::access_path() const {
  RequireBoundIdentity();
  return std::filesystem::path("/proc/self/fd") / std::to_string(fd_);
}

void Cgroup::PrepareRun(const std::string& run_id) {
  RequireSafeRunId(run_id);
  if (RunWasPrepared(run_id)) {
    throw std::runtime_error(
        "run cgroup is already prepared by this process: " + run_id);
  }
  PrepareRunInScope(ProductionCgroupScopeConfig(), run_id);
  try {
    RecordPreparedRun(run_id);
  } catch (...) {
    RemoveRunInScope(
        ProductionCgroupScopeConfig(), run_id, nullptr,
        std::chrono::steady_clock::now() + std::chrono::seconds(10), {});
    throw;
  }
}

void Cgroup::PrepareRun(const RunOwnership& ownership) {
  const std::string& run_id = ownership.cgroup_name;
  RequireSafeRunId(run_id);
  if (RunWasPrepared(run_id)) {
    throw std::runtime_error(
        "run cgroup is already prepared by this process: " + run_id);
  }
  PrepareRunInScope(ProductionCgroupScopeConfig(), run_id, &ownership);
  try {
    RecordPreparedRun(run_id);
  } catch (...) {
    RemoveRunInScope(
        ProductionCgroupScopeConfig(), run_id, &ownership,
        std::chrono::steady_clock::now() + std::chrono::seconds(10), {});
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
  RemoveRun(run_id,
            std::chrono::steady_clock::now() + std::chrono::seconds(10));
}

void Cgroup::RemoveRun(const std::string& run_id,
                       std::chrono::steady_clock::time_point deadline,
                       std::stop_token stop_token) {
  RequireSafeRunId(run_id);
  const std::filesystem::path run_root = CgroupPathsForRun(run_id).run;
  if (!RunWasPrepared(run_id)) {
    if (!std::filesystem::exists(run_root)) {
      return;
    }
    throw std::runtime_error("refusing to remove an unowned run cgroup: " +
                             run_root.string());
  }
  RemoveRunInScope(ProductionCgroupScopeConfig(), run_id, nullptr, deadline,
                   stop_token);
  ForgetPreparedRun(run_id);
}

void Cgroup::RemoveStaleRun(const RunOwnership& ownership) {
  const RunOwnership loaded =
      LoadRunOwnership(ownership.run_id, ownership.run_root);
  if (loaded != ownership) {
    throw std::runtime_error("stale cgroup ownership fields do not match");
  }
  RemoveRunInScope(
      ProductionCgroupScopeConfig(), ownership.cgroup_name, &ownership,
      std::chrono::steady_clock::now() + std::chrono::seconds(10), {});
  ForgetPreparedRun(ownership.cgroup_name);
}

#ifdef BBP_ENABLE_TEST_HOOKS
void PrepareCgroupRunInTestScope(const CgroupScopeTestConfig& config,
                                 const std::string& run_id) {
  PrepareRunInScope(
      CgroupScopeConfig{
          .root = config.root,
          .simulator_name = config.simulator_name,
          .state_file = config.state_file,
          .allow_root_process_move = config.allow_root_process_move,
      },
      run_id);
}

void PrepareCgroupRunInTestScope(const CgroupScopeTestConfig& config,
                                 const RunOwnership& ownership) {
  PrepareRunInScope(
      CgroupScopeConfig{
          .root = config.root,
          .simulator_name = config.simulator_name,
          .state_file = config.state_file,
          .allow_root_process_move = config.allow_root_process_move,
      },
      ownership.cgroup_name, &ownership);
}

void RemoveCgroupRunInTestScope(const CgroupScopeTestConfig& config,
                                const std::string& run_id) {
  RemoveRunInScope(
      CgroupScopeConfig{
          .root = config.root,
          .simulator_name = config.simulator_name,
          .state_file = config.state_file,
          .allow_root_process_move = config.allow_root_process_move,
      },
      run_id, nullptr,
      std::chrono::steady_clock::now() + std::chrono::seconds(10), {});
}

void RemoveStaleCgroupRunInTestScope(const CgroupScopeTestConfig& config,
                                     const RunOwnership& ownership) {
  RemoveRunInScope(
      CgroupScopeConfig{
          .root = config.root,
          .simulator_name = config.simulator_name,
          .state_file = config.state_file,
          .allow_root_process_move = config.allow_root_process_move,
      },
      ownership.cgroup_name, &ownership,
      std::chrono::steady_clock::now() + std::chrono::seconds(10), {});
}

void KillCgroupProcessesWithPidfdFallbackForTest(
    const std::filesystem::path& path,
    std::chrono::steady_clock::time_point deadline,
    std::stop_token stop_token) {
  KillCgroupProcesses(path, deadline, stop_token, true);
}
#endif

CgroupFreezeProbe Cgroup::ProbeFreezeThaw() {
  CgroupFreezeProbe probe;
  probe.run_id = "freeze-" + std::to_string(getpid());
  probe.node_id = "node-1";

  Cgroup::PrepareRun(probe.run_id);
  Cgroup cgroup = Cgroup::Create(probe.run_id, probe.node_id);
  pid_t child = fork();
  if (child < 0) {
    cgroup.Remove(std::chrono::steady_clock::now() + std::chrono::seconds(1));
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
    cgroup.Remove(std::chrono::steady_clock::now() + std::chrono::seconds(1));
    Cgroup::RemoveRun(probe.run_id);
  } catch (...) {
    kill(child, SIGKILL);
    waitpid(child, nullptr, 0);
    try {
      const auto deadline =
          std::chrono::steady_clock::now() + std::chrono::seconds(1);
      cgroup.KillAll(deadline);
      cgroup.Remove(deadline);
      Cgroup::RemoveRun(probe.run_id);
    } catch (const std::exception&) {
    }
    throw;
  }
  return probe;
}

void Cgroup::AttachPid(pid_t pid) const {
  const std::filesystem::path bound = access_path();
  WriteCgroupFile(bound, "cgroup.procs", std::to_string(pid));
  RequireBoundIdentity();
}

void Cgroup::SetMemoryMax(uint64_t bytes) const {
  const std::filesystem::path bound = access_path();
  WriteCgroupFile(bound, "memory.max", std::to_string(bytes));
  RequireBoundIdentity();
}

void Cgroup::SetMemoryHigh(uint64_t bytes) const {
  const std::filesystem::path bound = access_path();
  WriteCgroupFile(bound, "memory.high", std::to_string(bytes));
  RequireBoundIdentity();
}

void Cgroup::SetCpuMax(std::optional<uint64_t> quota_us,
                       uint64_t period_us) const {
  std::string value = quota_us ? std::to_string(*quota_us) : "max";
  value += " ";
  value += std::to_string(period_us);
  const std::filesystem::path bound = access_path();
  WriteCgroupFile(bound, "cpu.max", value);
  RequireBoundIdentity();
}

void Cgroup::SetCpuWeight(uint64_t weight) const {
  if (weight < 1U || weight > 10000U) {
    throw std::runtime_error("cpu.weight must be in 1..10000");
  }
  const std::filesystem::path bound = access_path();
  WriteCgroupFile(bound, "cpu.weight", std::to_string(weight));
  if (ParseSingleUint(bound / "cpu.weight") != weight) {
    throw std::runtime_error("cpu.weight read-back verification failed for " +
                             path_.string());
  }
  RequireBoundIdentity();
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
  const std::filesystem::path bound = access_path();
  WriteCgroupFile(bound, "io.max", value);

  const std::vector<IoLimit> actual = ParseIoMax(bound / "io.max");
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
  RequireBoundIdentity();
}

void Cgroup::SetIoWeight(uint64_t weight) const {
  if (weight < 1U || weight > 10000U) {
    throw std::runtime_error("io.weight must be in 1..10000");
  }
  const std::filesystem::path bound = access_path();
  WriteCgroupFile(bound, "io.weight", "default " + std::to_string(weight));
  if (ParseIoWeight(bound / "io.weight") != weight) {
    throw std::runtime_error("io.weight read-back verification failed for " +
                             path_.string());
  }
  RequireBoundIdentity();
}

void Cgroup::SetPidsMax(uint64_t n) const {
  const std::filesystem::path bound = access_path();
  WriteCgroupFile(bound, "pids.max", std::to_string(n));
  RequireBoundIdentity();
}

CgroupMetrics Cgroup::ReadMetrics() const {
  const std::filesystem::path bound = access_path();
  CgroupMetrics metrics;
  metrics.cpu_usage_usec = ParseKeyValue(bound / "cpu.stat", "usage_usec");
  metrics.cpu_throttled_usec =
      ParseKeyValue(bound / "cpu.stat", "throttled_usec");
  metrics.cpu_pressure_some_total_usec =
      ParsePressureTotal(bound / "cpu.pressure", "some");
  metrics.cpu_pressure_full_total_usec =
      ParsePressureTotal(bound / "cpu.pressure", "full");
  metrics.memory_current = ParseSingleUint(bound / "memory.current");
  if (std::filesystem::exists(bound / "memory.peak")) {
    metrics.memory_peak = ParseSingleUint(bound / "memory.peak");
  }
  metrics.memory_high_limit_bytes = ParseMaxOrUint(bound / "memory.high");
  metrics.memory_max_limit_bytes = ParseMaxOrUint(bound / "memory.max");
  const CpuMaxValue cpu_max = ParseCpuMax(bound / "cpu.max");
  metrics.cpu_quota_us = cpu_max.quota_us;
  metrics.cpu_period_us = cpu_max.period_us;
  metrics.cpu_weight = ParseSingleUint(bound / "cpu.weight");
  metrics.io_weight = ParseIoWeight(bound / "io.weight");
  metrics.io_limits = ParseIoMax(bound / "io.max");
  const IoStatTotals io = ParseIoStat(bound / "io.stat");
  metrics.io_read_bytes = io.read_bytes;
  metrics.io_write_bytes = io.write_bytes;
  metrics.io_read_operations = io.read_operations;
  metrics.io_write_operations = io.write_operations;
  metrics.io_discard_bytes = io.discard_bytes;
  metrics.io_discard_operations = io.discard_operations;
  metrics.io_pressure_some_total_usec =
      ParsePressureTotal(bound / "io.pressure", "some");
  metrics.io_pressure_full_total_usec =
      ParsePressureTotal(bound / "io.pressure", "full");
  metrics.pids_current = ParseSingleUint(bound / "pids.current");
  metrics.pids_max_limit = ParseMaxOrUint(bound / "pids.max");
  metrics.pids_max_events = ParseKeyValue(bound / "pids.events", "max");
  metrics.cgroup_populated =
      ParseKeyValue(bound / "cgroup.events", "populated");
  metrics.cgroup_frozen = ParseKeyValue(bound / "cgroup.events", "frozen");
  metrics.memory_low = ParseKeyValue(bound / "memory.events", "low");
  metrics.memory_high = ParseKeyValue(bound / "memory.events", "high");
  metrics.memory_max = ParseKeyValue(bound / "memory.events", "max");
  metrics.oom = ParseKeyValue(bound / "memory.events", "oom");
  metrics.oom_kill = ParseKeyValue(bound / "memory.events", "oom_kill");
  metrics.oom_group_kill =
      ParseKeyValue(bound / "memory.events", "oom_group_kill");
  metrics.memory_stat = ParseMemoryStat(bound / "memory.stat");
  RequireBoundIdentity();
  return metrics;
}

void Cgroup::Freeze() const {
  const std::filesystem::path bound = access_path();
  WriteCgroupFile(bound, "cgroup.freeze", "1");
  RequireBoundIdentity();
}

void Cgroup::Thaw() const {
  const std::filesystem::path bound = access_path();
  WriteCgroupFile(bound, "cgroup.freeze", "0");
  RequireBoundIdentity();
}

bool Cgroup::Frozen() const {
  const bool frozen = ReadFrozenState(access_path());
  RequireBoundIdentity();
  return frozen;
}

bool Cgroup::Empty() const {
  const std::filesystem::path bound = access_path();
  const bool empty =
      CgroupProcsEmpty(bound) &&
      ParseKeyValue(bound / "cgroup.events", "populated") == 0U;
  RequireBoundIdentity();
  return empty;
}

void Cgroup::KillAll(std::chrono::steady_clock::time_point deadline,
                     std::stop_token stop_token) const {
  if (stop_token.stop_requested()) {
    throw std::runtime_error("cgroup process kill cancelled: " +
                             path_.string());
  }
  if (std::chrono::steady_clock::now() >= deadline) {
    throw std::runtime_error("cgroup process kill deadline expired: " +
                             path_.string());
  }
  KillCgroupProcesses(access_path(), deadline, stop_token);
  RequireBoundIdentity();
  if (!Empty()) {
    throw std::runtime_error("owned cgroup remained populated after kill: " +
                             path_.string());
  }
}

void Cgroup::Remove(std::chrono::steady_clock::time_point deadline,
                    std::stop_token stop_token) const {
  if (stop_token.stop_requested()) {
    throw std::runtime_error("cgroup removal cancelled: " + path_.string());
  }
  if (std::chrono::steady_clock::now() >= deadline) {
    throw std::runtime_error("cgroup removal deadline expired: " +
                             path_.string());
  }
  if (removed_) {
    struct stat replacement {};
    if (fstatat(parent_fd_, name_.c_str(), &replacement,
                AT_SYMLINK_NOFOLLOW) == 0) {
      throw std::runtime_error(
          "refusing replacement at removed cgroup path: " + path_.string());
    }
    if (errno != ENOENT) {
      throw std::runtime_error("inspect removed cgroup path failed for " +
                               path_.string() + ": " + std::strerror(errno));
    }
    return;
  }
  const std::filesystem::path bound = access_path();
  WaitForCgroupProcsEmpty(bound, deadline, stop_token);
  if (!Empty()) {
    throw std::runtime_error("refusing to remove a populated owned cgroup: " +
                             path_.string());
  }
  RequireBoundIdentity();
  if (unlinkat(parent_fd_, name_.c_str(), AT_REMOVEDIR) != 0) {
    throw std::runtime_error("remove cgroup failed for " + path_.string() +
                             ": " + std::strerror(errno));
  }
  removed_ = true;
  struct stat remaining {};
  if (fstatat(parent_fd_, name_.c_str(), &remaining, AT_SYMLINK_NOFOLLOW) == 0) {
    throw std::runtime_error(
        "replacement appeared at removed cgroup path: " + path_.string());
  }
  if (errno != ENOENT) {
    throw std::runtime_error("verify removed cgroup path failed for " +
                             path_.string() + ": " + std::strerror(errno));
  }
}

}  // namespace bbp
