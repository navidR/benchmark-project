#include "bbp/cgroup.h"

#include <dirent.h>
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
constexpr std::uint64_t kCgroupScopeStateVersion = 3U;

std::mutex prepared_runs_mutex;
std::set<std::string> prepared_runs;

#ifdef BBP_ENABLE_TEST_HOOKS
std::function<void()> cgroup_create_after_directory_hook;
std::function<void()> cgroup_create_shared_allocation_hook;
std::function<void(CgroupRemovalTestPhase, const std::filesystem::path&)>
    cgroup_removal_identity_hook;
#endif

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

struct CgroupDirectoryIdentity {
  CgroupPathIdentity path;
  std::uint64_t mount_id = 0U;

  bool operator==(const CgroupDirectoryIdentity&) const = default;
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
  bool controller_expected = false;
  bool restoration_ready = false;
  std::optional<CgroupDirectoryIdentity> root_identity;
  std::optional<CgroupDirectoryIdentity> simulator_identity;
  std::optional<CgroupDirectoryIdentity> controller_identity;
  std::set<std::string> root_controllers_before;
  std::set<std::string> simulator_controllers_before;
  std::set<std::string> root_controllers_added;
  std::set<std::string> simulator_controllers_added;
  std::set<std::string> active_runs;
  std::map<std::string, CgroupRunBinding> run_bindings;
  std::optional<std::string> pending_run;
  bool pending_run_created = false;
  bool legacy_scope_identities = false;
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

  int descriptor() const { return fd_; }

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
  struct stat status{};
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

void RequireCgroupOperationActive(
    std::optional<std::chrono::steady_clock::time_point> deadline,
    std::stop_token stop_token, std::string_view operation) {
  if (stop_token.stop_requested()) {
    throw std::runtime_error(std::string(operation) + " was cancelled");
  }
  if (deadline && std::chrono::steady_clock::now() >= *deadline) {
    throw std::runtime_error(std::string(operation) + " deadline expired");
  }
}

std::string ReadOwnedScopeState(
    const std::filesystem::path& path,
    std::optional<std::chrono::steady_clock::time_point> deadline,
    std::stop_token stop_token) {
  RequireCgroupOperationActive(deadline, stop_token, "cgroup scope state read");
  const int fd =
      open(path.c_str(), O_RDONLY | O_NONBLOCK | O_CLOEXEC | O_NOFOLLOW);
  if (fd < 0) {
    throw std::runtime_error("open cgroup scope state failed for " +
                             path.string() + ": " + std::strerror(errno));
  }
  constexpr std::size_t kMaximumStateBytes = 64U * 1024U;
  std::string contents;
  std::array<char, 4096U> buffer{};
  try {
    RequireCgroupOperationActive(deadline, stop_token,
                                 "cgroup scope state read");
    struct stat status{};
    if (fstat(fd, &status) != 0 || !S_ISREG(status.st_mode) ||
        status.st_uid != geteuid() || (status.st_mode & 0077U) != 0U) {
      throw std::runtime_error(
          "cgroup scope state must be an owner-only regular file: " +
          path.string());
    }
    for (;;) {
      RequireCgroupOperationActive(deadline, stop_token,
                                   "cgroup scope state read");
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
    RequireCgroupOperationActive(deadline, stop_token,
                                 "cgroup scope state read");
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

boost::json::value ScopeDirectoryIdentityJson(
    const std::optional<CgroupDirectoryIdentity>& identity) {
  if (!identity) {
    return nullptr;
  }
  return boost::json::object{
      {"device", identity->path.device},
      {"inode", identity->path.inode},
      {"mount_id", identity->mount_id},
  };
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
  object["controller_expected"] = state.controller_expected;
  object["restoration_ready"] = state.restoration_ready;
  object["root_identity"] = ScopeDirectoryIdentityJson(state.root_identity);
  object["simulator_identity"] =
      ScopeDirectoryIdentityJson(state.simulator_identity);
  object["controller_identity"] =
      ScopeDirectoryIdentityJson(state.controller_identity);
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

void SyncCgroupScopeStateDirectory(const std::filesystem::path& path,
                                   std::string_view operation) {
  const std::filesystem::path parent = path.parent_path().empty()
                                           ? std::filesystem::path(".")
                                           : path.parent_path();
  const int descriptor =
      open(parent.c_str(), O_RDONLY | O_DIRECTORY | O_CLOEXEC);
  if (descriptor < 0) {
    throw std::runtime_error("open cgroup scope state directory failed after " +
                             std::string(operation) + " for " + path.string() +
                             ": " + std::strerror(errno));
  }
  if (fsync(descriptor) != 0) {
    const int error = errno;
    static_cast<void>(close(descriptor));
    throw std::runtime_error(
        "fsync cgroup scope state directory failed after " +
        std::string(operation) + " for " + path.string() + ": " +
        std::strerror(error));
  }
  if (close(descriptor) != 0) {
    throw std::runtime_error(
        "close cgroup scope state directory failed after " +
        std::string(operation) + " for " + path.string() + ": " +
        std::strerror(errno));
  }
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
  SyncCgroupScopeStateDirectory(path, "publish");
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

std::optional<CgroupDirectoryIdentity> ScopeDirectoryIdentity(
    const boost::json::object& object, std::string_view field) {
  const boost::json::value& value = RequiredScopeField(object, field);
  if (value.is_null()) {
    return std::nullopt;
  }
  if (!value.is_object()) {
    throw std::runtime_error(
        "cgroup scope directory identity is not an object: " +
        std::string(field));
  }
  const boost::json::object& identity = value.as_object();
  const std::set<std::string_view> supported = {"device", "inode", "mount_id"};
  for (const auto& member : identity) {
    if (!supported.contains(member.key())) {
      throw std::runtime_error(
          "cgroup scope directory identity has unsupported field: " +
          std::string(field));
    }
  }
  CgroupDirectoryIdentity result{
      .path =
          CgroupPathIdentity{
              .device = ScopeUint64(identity, "device"),
              .inode = ScopeUint64(identity, "inode"),
          },
      .mount_id = ScopeUint64(identity, "mount_id"),
  };
  if (result.path.inode == 0U || result.mount_id == 0U) {
    throw std::runtime_error(
        "cgroup scope directory identity contains a zero identity: " +
        std::string(field));
  }
  return result;
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

CgroupScopeState LoadCgroupScopeState(
    const CgroupScopeConfig& config,
    std::optional<std::chrono::steady_clock::time_point> deadline =
        std::nullopt,
    std::stop_token stop_token = {}) {
  const boost::json::value parsed = boost::json::parse(
      ReadOwnedScopeState(config.state_file, deadline, stop_token));
  RequireCgroupOperationActive(deadline, stop_token, "cgroup scope state read");
  if (!parsed.is_object()) {
    throw std::runtime_error("cgroup scope state is not a JSON object");
  }
  const boost::json::object& object = parsed.as_object();
  const boost::json::value& version = RequiredScopeField(object, "version");
  const std::uint64_t version_number =
      version.is_uint64() ? version.as_uint64()
      : version.is_int64() && version.as_int64() >= 0
          ? static_cast<std::uint64_t>(version.as_int64())
          : 0U;
  if (version_number != 1U && version_number != 2U &&
      version_number != kCgroupScopeStateVersion) {
    throw std::runtime_error("cgroup scope state version is unsupported");
  }
  std::set<std::string_view> supported = {
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
  if (version_number == kCgroupScopeStateVersion) {
    supported.insert("controller_expected");
    supported.insert("restoration_ready");
    supported.insert("root_identity");
    supported.insert("simulator_identity");
    supported.insert("controller_identity");
  }
  for (const auto& member : object) {
    const std::string_view key(member.key().data(), member.key().size());
    if (!supported.contains(key)) {
      throw std::runtime_error("cgroup scope state has unsupported field: " +
                               std::string(key));
    }
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
  if (version_number == kCgroupScopeStateVersion) {
    const boost::json::value& controller_expected =
        RequiredScopeField(object, "controller_expected");
    const boost::json::value& restoration_ready =
        RequiredScopeField(object, "restoration_ready");
    if (!controller_expected.is_bool()) {
      throw std::runtime_error(
          "cgroup scope controller_expected is not Boolean");
    }
    if (!restoration_ready.is_bool()) {
      throw std::runtime_error("cgroup scope restoration_ready is not Boolean");
    }
    state.controller_expected = controller_expected.as_bool();
    state.restoration_ready = restoration_ready.as_bool();
    state.root_identity = ScopeDirectoryIdentity(object, "root_identity");
    state.simulator_identity =
        ScopeDirectoryIdentity(object, "simulator_identity");
    state.controller_identity =
        ScopeDirectoryIdentity(object, "controller_identity");
  } else {
    state.controller_expected = config.allow_root_process_move;
    state.legacy_scope_identities = true;
  }
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
  const auto controllers_overlap = [](const std::set<std::string>& before,
                                      const std::set<std::string>& added) {
    return std::any_of(added.begin(), added.end(),
                       [&](const std::string& controller) {
                         return before.contains(controller);
                       });
  };
  if (controllers_overlap(state.root_controllers_before,
                          state.root_controllers_added) ||
      controllers_overlap(state.simulator_controllers_before,
                          state.simulator_controllers_added) ||
      (!state.simulator_preexisting &&
       !state.simulator_controllers_before.empty())) {
    throw std::runtime_error("cgroup scope controller state is inconsistent");
  }
  if (!state.legacy_scope_identities) {
    if (!state.root_identity) {
      throw std::runtime_error(
          "cgroup scope state is missing its root identity");
    }
    if (state.simulator_preexisting && !state.simulator_identity) {
      throw std::runtime_error(
          "cgroup scope state is missing its pre-existing simulator identity");
    }
    if (state.controller_identity && !state.controller_expected) {
      throw std::runtime_error(
          "cgroup scope state has an unexpected controller identity");
    }
    if (state.controller_identity && !state.simulator_identity) {
      throw std::runtime_error(
          "cgroup scope controller identity has no simulator identity");
    }
    if (!state.controller_expected && !state.root_controllers_added.empty()) {
      throw std::runtime_error(
          "cgroup scope without a controller cannot own root controllers");
    }
    const bool scope_in_use =
        !state.active_runs.empty() || state.pending_run.has_value();
    if (scope_in_use && !state.simulator_identity) {
      throw std::runtime_error(
          "cgroup scope in use is missing its simulator identity");
    }
    if (scope_in_use && state.controller_expected &&
        !state.controller_identity) {
      throw std::runtime_error(
          "cgroup scope in use is missing its controller identity");
    }
    if (state.restoration_ready &&
        (!state.active_runs.empty() || !state.run_bindings.empty() ||
         state.pending_run || state.pending_run_created)) {
      throw std::runtime_error(
          "restoration-ready cgroup scope still contains an owned run");
    }
    if (state.simulator_identity &&
        (state.simulator_identity->path.device !=
             state.root_identity->path.device ||
         state.simulator_identity->mount_id != state.root_identity->mount_id ||
         state.simulator_identity->path == state.root_identity->path)) {
      throw std::runtime_error(
          "cgroup scope simulator identity is outside its root identity");
    }
    if (state.controller_identity &&
        (state.controller_identity->path.device !=
             state.simulator_identity->path.device ||
         state.controller_identity->mount_id !=
             state.simulator_identity->mount_id ||
         state.controller_identity->path == state.simulator_identity->path ||
         state.controller_identity->path == state.root_identity->path)) {
      throw std::runtime_error(
          "cgroup scope controller identity is outside its simulator identity");
    }
  }
  RequireCgroupOperationActive(deadline, stop_token, "cgroup scope state read");
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

uint64_t ParseKeyValueText(std::string_view text, std::string_view key,
                           std::string_view context) {
  std::istringstream lines{std::string(text)};
  std::string line;
  std::optional<uint64_t> result;
  while (std::getline(lines, line)) {
    const std::vector<std::string> fields = SplitWhitespace(line);
    if (fields.empty()) {
      continue;
    }
    if (fields.size() != 2U) {
      throw std::runtime_error("invalid key/value cgroup line in " +
                               std::string(context) + ": " + line);
    }
    const uint64_t value = ParseUint64(fields[1], context);
    if (fields[0] == key) {
      if (result) {
        throw std::runtime_error("duplicate cgroup key in " +
                                 std::string(context) + ": " +
                                 std::string(key));
      }
      result = value;
    }
  }
  return result.value_or(0U);
}

uint64_t ParseKeyValue(const std::filesystem::path& path,
                       std::string_view key) {
  return ParseKeyValueText(ReadText(path), key, path.string());
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
  struct statfs filesystem_status{};
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

bool SameCgroupIdentity(const struct stat& first, const struct stat& second) {
  return first.st_dev == second.st_dev && first.st_ino == second.st_ino;
}

std::uint64_t CgroupMountId(int descriptor) {
  struct statx status{};
  if (statx(descriptor, "", AT_EMPTY_PATH | AT_NO_AUTOMOUNT, STATX_MNT_ID,
            &status) != 0) {
    throw std::runtime_error("inspect acquired cgroup mount identity failed: " +
                             std::string(std::strerror(errno)));
  }
  if ((status.stx_mask & STATX_MNT_ID) == 0U) {
    throw std::runtime_error(
        "kernel did not report an acquired cgroup mount identity");
  }
  return status.stx_mnt_id;
}

std::uint64_t CgroupMountIdAt(int parent, std::string_view name) {
  const std::string filename(name);
  struct statx status{};
  if (statx(parent, filename.c_str(), AT_SYMLINK_NOFOLLOW | AT_NO_AUTOMOUNT,
            STATX_MNT_ID, &status) != 0) {
    throw std::runtime_error("inspect linked cgroup mount identity failed: " +
                             std::string(std::strerror(errno)));
  }
  if ((status.stx_mask & STATX_MNT_ID) == 0U) {
    throw std::runtime_error(
        "kernel did not report a linked cgroup mount identity");
  }
  return status.stx_mnt_id;
}

class BoundCgroupDirectory {
 public:
  static std::optional<BoundCgroupDirectory> TryOpen(
      int parent_descriptor, std::string name,
      std::filesystem::path display_path,
      std::optional<CgroupPathIdentity> expected_identity = std::nullopt,
      std::optional<std::uint64_t> expected_mount_id = std::nullopt,
      bool require_parent_mount = true) {
    struct stat before{};
    if (fstatat(parent_descriptor, name.c_str(), &before,
                AT_SYMLINK_NOFOLLOW) != 0) {
      if (errno == ENOENT) {
        return std::nullopt;
      }
      throw std::runtime_error("inspect cgroup before acquisition failed for " +
                               display_path.string() + ": " +
                               std::strerror(errno));
    }
    if (!S_ISDIR(before.st_mode)) {
      throw CgroupOwnershipMismatch(
          "refusing a non-directory cgroup cleanup target: " +
          display_path.string());
    }

    BoundCgroupDirectory acquired(parent_descriptor, std::move(name),
                                  std::move(display_path), before,
                                  require_parent_mount);
    if (expected_identity &&
        (static_cast<std::uint64_t>(acquired.identity_.st_dev) !=
             expected_identity->device ||
         static_cast<std::uint64_t>(acquired.identity_.st_ino) !=
             expected_identity->inode)) {
      throw CgroupOwnershipMismatch(
          (expected_mount_id
               ? "refusing a replaced scope cgroup identity: "
               : "refusing to remove a replaced run cgroup identity: ") +
          acquired.display_path_.string());
    }
    if (expected_mount_id && acquired.mount_id_ != *expected_mount_id) {
      throw CgroupOwnershipMismatch(
          "refusing a replaced cgroup mount identity: " +
          acquired.display_path_.string());
    }
    return acquired;
  }

  BoundCgroupDirectory(const BoundCgroupDirectory&) = delete;
  BoundCgroupDirectory& operator=(const BoundCgroupDirectory&) = delete;

  BoundCgroupDirectory(BoundCgroupDirectory&& other) noexcept {
    *this = std::move(other);
  }

  BoundCgroupDirectory& operator=(BoundCgroupDirectory&& other) noexcept {
    if (this == &other) {
      return *this;
    }
    Close();
    parent_descriptor_ = other.parent_descriptor_;
    descriptor_ = other.descriptor_;
    name_ = std::move(other.name_);
    display_path_ = std::move(other.display_path_);
    identity_ = other.identity_;
    mount_id_ = other.mount_id_;
    removed_ = other.removed_;
    other.parent_descriptor_ = -1;
    other.descriptor_ = -1;
    other.removed_ = true;
    return *this;
  }

  ~BoundCgroupDirectory() { Close(); }

  int parent_descriptor() const { return parent_descriptor_; }
  int descriptor() const { return descriptor_; }
  const std::string& name() const { return name_; }
  const std::filesystem::path& display_path() const { return display_path_; }
  const struct stat& identity() const { return identity_; }
  std::uint64_t mount_id() const { return mount_id_; }
  CgroupDirectoryIdentity directory_identity() const {
    return CgroupDirectoryIdentity{
        .path =
            CgroupPathIdentity{
                .device = static_cast<std::uint64_t>(identity_.st_dev),
                .inode = static_cast<std::uint64_t>(identity_.st_ino),
            },
        .mount_id = mount_id_,
    };
  }

  void RequireLinkedIdentity() const {
    if (removed_) {
      throw CgroupOwnershipMismatch("acquired cgroup was already removed: " +
                                    display_path_.string());
    }
    struct stat opened{};
    if (fstat(descriptor_, &opened) != 0) {
      throw std::runtime_error("inspect acquired cgroup failed for " +
                               display_path_.string() + ": " +
                               std::strerror(errno));
    }
    if (!S_ISDIR(opened.st_mode) || !SameCgroupIdentity(opened, identity_) ||
        opened.st_uid != identity_.st_uid || identity_.st_uid != geteuid()) {
      throw CgroupOwnershipMismatch("acquired cgroup identity changed: " +
                                    display_path_.string());
    }
    struct stat linked{};
    if (fstatat(parent_descriptor_, name_.c_str(), &linked,
                AT_SYMLINK_NOFOLLOW) != 0) {
      if (errno == ENOENT) {
        throw CgroupOwnershipMismatch("acquired cgroup path disappeared: " +
                                      display_path_.string());
      }
      throw std::runtime_error("reinspect acquired cgroup failed for " +
                               display_path_.string() + ": " +
                               std::strerror(errno));
    }
    if (!S_ISDIR(linked.st_mode) || !SameCgroupIdentity(linked, identity_) ||
        linked.st_uid != identity_.st_uid ||
        CgroupMountId(descriptor_) != mount_id_ ||
        CgroupMountIdAt(parent_descriptor_, name_) != mount_id_) {
      throw CgroupOwnershipMismatch("refusing a replaced cgroup identity: " +
                                    display_path_.string());
    }
  }

  void MarkRemoved() { removed_ = true; }

 private:
  BoundCgroupDirectory(int parent_descriptor, std::string name,
                       std::filesystem::path display_path,
                       const struct stat& before, bool require_parent_mount)
      : name_(std::move(name)),
        display_path_(std::move(display_path)),
        identity_(before) {
    parent_descriptor_ = fcntl(parent_descriptor, F_DUPFD_CLOEXEC, 0);
    if (parent_descriptor_ < 0) {
      throw std::runtime_error("duplicate cgroup parent failed for " +
                               display_path_.string() + ": " +
                               std::strerror(errno));
    }
    descriptor_ = openat(parent_descriptor_, name_.c_str(),
                         O_RDONLY | O_DIRECTORY | O_CLOEXEC | O_NOFOLLOW);
    if (descriptor_ < 0) {
      const int error = errno;
      Close();
      if (error == ENOENT) {
        throw CgroupOwnershipMismatch(
            "cgroup disappeared during acquisition: " + display_path_.string());
      }
      throw std::runtime_error("open acquired cgroup failed for " +
                               display_path_.string() + ": " +
                               std::strerror(error));
    }
    struct stat opened{};
    if (fstat(descriptor_, &opened) != 0) {
      const int error = errno;
      Close();
      throw std::runtime_error("inspect opened cgroup failed for " +
                               display_path_.string() + ": " +
                               std::strerror(error));
    }
    if (!S_ISDIR(opened.st_mode) || !SameCgroupIdentity(before, opened) ||
        opened.st_uid != geteuid()) {
      Close();
      throw CgroupOwnershipMismatch(
          "cgroup ownership or identity changed during acquisition: " +
          display_path_.string());
    }
    try {
      mount_id_ = CgroupMountId(descriptor_);
      if ((require_parent_mount &&
           CgroupMountId(parent_descriptor_) != mount_id_) ||
          CgroupMountIdAt(parent_descriptor_, name_) != mount_id_) {
        throw CgroupOwnershipMismatch(
            "cgroup cleanup refuses a mount boundary: " +
            display_path_.string());
      }
    } catch (...) {
      Close();
      throw;
    }
    identity_ = opened;
  }

  void Close() noexcept {
    if (descriptor_ >= 0) {
      static_cast<void>(close(descriptor_));
      descriptor_ = -1;
    }
    if (parent_descriptor_ >= 0) {
      static_cast<void>(close(parent_descriptor_));
      parent_descriptor_ = -1;
    }
  }

  int parent_descriptor_ = -1;
  int descriptor_ = -1;
  std::string name_;
  std::filesystem::path display_path_;
  struct stat identity_{};
  std::uint64_t mount_id_ = 0U;
  bool removed_ = false;
};

BoundCgroupDirectory AcquireBoundScopeRoot(
    const CgroupScopeConfig& config, const CgroupScopeLock& scope_lock,
    const std::optional<CgroupDirectoryIdentity>& expected_identity) {
  if (!config.root.is_absolute() ||
      config.root.lexically_normal() != config.root ||
      config.root.filename().empty()) {
    throw std::runtime_error(
        "cgroup scope root is not a normalized directory: " +
        config.root.string());
  }
  const std::filesystem::path parent_path = config.root.parent_path();
  const int parent_descriptor = open(
      parent_path.c_str(), O_RDONLY | O_DIRECTORY | O_CLOEXEC | O_NOFOLLOW);
  if (parent_descriptor < 0) {
    throw std::runtime_error("open cgroup scope root parent failed for " +
                             parent_path.string() + ": " +
                             std::strerror(errno));
  }

  std::optional<BoundCgroupDirectory> root;
  try {
    root = BoundCgroupDirectory::TryOpen(
        parent_descriptor, config.root.filename().string(), config.root,
        expected_identity
            ? std::optional<CgroupPathIdentity>(expected_identity->path)
            : std::nullopt,
        expected_identity
            ? std::optional<std::uint64_t>(expected_identity->mount_id)
            : std::nullopt,
        false);
  } catch (...) {
    static_cast<void>(close(parent_descriptor));
    throw;
  }
  if (close(parent_descriptor) != 0) {
    throw std::runtime_error("close cgroup scope root parent failed for " +
                             parent_path.string() + ": " +
                             std::strerror(errno));
  }
  if (!root) {
    throw CgroupOwnershipMismatch("cgroup scope root disappeared: " +
                                  config.root.string());
  }

  struct stat locked{};
  if (fstat(scope_lock.descriptor(), &locked) != 0) {
    throw std::runtime_error("inspect locked cgroup scope root failed for " +
                             config.root.string() + ": " +
                             std::strerror(errno));
  }
  if (!SameCgroupIdentity(locked, root->identity()) ||
      CgroupMountId(scope_lock.descriptor()) != root->mount_id()) {
    throw CgroupOwnershipMismatch(
        "locked cgroup scope root was replaced during acquisition: " +
        config.root.string());
  }
  root->RequireLinkedIdentity();
  return std::move(*root);
}

void RequireAbsentCgroupChild(const BoundCgroupDirectory& parent,
                              std::string_view name,
                              const std::filesystem::path& display_path) {
  parent.RequireLinkedIdentity();
  const std::string child_name(name);
  struct stat status{};
  if (fstatat(parent.descriptor(), child_name.c_str(), &status,
              AT_SYMLINK_NOFOLLOW) == 0) {
    throw CgroupOwnershipMismatch(
        "a replacement appeared at an absent cgroup path: " +
        display_path.string());
  }
  if (errno != ENOENT) {
    throw std::runtime_error("inspect absent cgroup path failed for " +
                             display_path.string() + ": " +
                             std::strerror(errno));
  }
  parent.RequireLinkedIdentity();
}

struct BoundCgroupScope {
  BoundCgroupDirectory root;
  std::optional<BoundCgroupDirectory> simulator;
  std::optional<BoundCgroupDirectory> controller;
};

int OpenBoundCgroupFile(
    const BoundCgroupDirectory& cgroup, std::string_view file, int access_mode,
    std::optional<std::chrono::steady_clock::time_point> deadline,
    std::stop_token stop_token) {
  RequireCgroupOperationActive(deadline, stop_token, "cgroup file open");
  if (file.empty() || file == "." || file == ".." ||
      file.find('/') != std::string_view::npos) {
    throw std::runtime_error("invalid cgroup control filename: " +
                             std::string(file));
  }
  cgroup.RequireLinkedIdentity();
  const std::string filename(file);
  const std::filesystem::path path = cgroup.display_path() / filename;
  const int descriptor =
      openat(cgroup.descriptor(), filename.c_str(),
             access_mode | O_NONBLOCK | O_CLOEXEC | O_NOFOLLOW);
  if (descriptor < 0) {
    const int error = errno;
    cgroup.RequireLinkedIdentity();
    throw std::runtime_error("open cgroup file failed for " + path.string() +
                             ": " + std::strerror(error));
  }
  try {
    struct stat status{};
    if (fstat(descriptor, &status) != 0) {
      throw std::runtime_error("inspect cgroup control file failed for " +
                               path.string() + ": " + std::strerror(errno));
    }
    if (!S_ISREG(status.st_mode)) {
      throw std::runtime_error("cgroup control file is not regular: " +
                               path.string());
    }
    if (CgroupMountId(descriptor) != cgroup.mount_id()) {
      throw CgroupOwnershipMismatch(
          "cgroup control file crosses a mount boundary: " + path.string());
    }
    cgroup.RequireLinkedIdentity();
  } catch (...) {
    static_cast<void>(close(descriptor));
    throw;
  }
  return descriptor;
}

std::string ReadCgroupFileAt(
    const BoundCgroupDirectory& cgroup, std::string_view file,
    std::optional<std::chrono::steady_clock::time_point> deadline,
    std::stop_token stop_token) {
  RequireCgroupOperationActive(deadline, stop_token, "cgroup file read");
  const std::string filename(file);
  const std::filesystem::path path = cgroup.display_path() / filename;
  const int descriptor =
      OpenBoundCgroupFile(cgroup, file, O_RDONLY, deadline, stop_token);
  constexpr std::size_t kMaximumCgroupFileBytes = 1024U * 1024U;
  std::string contents;
  std::array<char, 4096U> buffer{};
  try {
    for (;;) {
      RequireCgroupOperationActive(deadline, stop_token, "cgroup file read");
      const ssize_t count = read(descriptor, buffer.data(), buffer.size());
      if (count < 0) {
        if (errno == EINTR) {
          continue;
        }
        throw std::runtime_error("read cgroup file failed for " +
                                 path.string() + ": " + std::strerror(errno));
      }
      if (count == 0) {
        break;
      }
      const std::size_t received = static_cast<std::size_t>(count);
      if (received > kMaximumCgroupFileBytes - contents.size()) {
        throw std::runtime_error("cgroup file exceeds 1 MiB: " + path.string());
      }
      contents.append(buffer.data(), received);
    }
    cgroup.RequireLinkedIdentity();
  } catch (...) {
    static_cast<void>(close(descriptor));
    throw;
  }
  if (close(descriptor) != 0) {
    throw std::runtime_error("close cgroup file failed for " + path.string() +
                             ": " + std::strerror(errno));
  }
  cgroup.RequireLinkedIdentity();
  return contents;
}

class CgroupFileWriteError final : public std::runtime_error {
 public:
  CgroupFileWriteError(const std::filesystem::path& path, int error)
      : std::runtime_error("write cgroup file failed for " + path.string() +
                           ": " + std::strerror(error)),
        error_(error) {}

  int error() const { return error_; }

 private:
  int error_;
};

void WriteCgroupFileAt(
    const BoundCgroupDirectory& cgroup, std::string_view file,
    std::string_view value,
    std::optional<std::chrono::steady_clock::time_point> deadline,
    std::stop_token stop_token) {
  RequireCgroupOperationActive(deadline, stop_token, "cgroup file write");
  const std::filesystem::path path = cgroup.display_path() / std::string(file);
  const int descriptor =
      OpenBoundCgroupFile(cgroup, file, O_WRONLY, deadline, stop_token);
  try {
    ssize_t count = -1;
    do {
      RequireCgroupOperationActive(deadline, stop_token, "cgroup file write");
      count = write(descriptor, value.data(), value.size());
    } while (count < 0 && errno == EINTR);
    if (count < 0) {
      throw CgroupFileWriteError(path, errno);
    }
    if (static_cast<std::size_t>(count) != value.size()) {
      throw std::runtime_error("short write to cgroup file " + path.string());
    }
    cgroup.RequireLinkedIdentity();
  } catch (...) {
    static_cast<void>(close(descriptor));
    throw;
  }
  if (close(descriptor) != 0) {
    throw std::runtime_error("close cgroup file failed for " + path.string() +
                             ": " + std::strerror(errno));
  }
  cgroup.RequireLinkedIdentity();
}

std::set<std::string> ControllerSetAt(
    const BoundCgroupDirectory& cgroup,
    std::optional<std::chrono::steady_clock::time_point> deadline,
    std::stop_token stop_token) {
  const std::vector<std::string> values = SplitWhitespace(
      ReadCgroupFileAt(cgroup, "cgroup.subtree_control", deadline, stop_token));
  return std::set<std::string>(values.begin(), values.end());
}

void SetControllersAt(
    const BoundCgroupDirectory& cgroup,
    const std::set<std::string>& controllers, char operation,
    std::optional<std::chrono::steady_clock::time_point> deadline,
    std::stop_token stop_token) {
  RequireCgroupOperationActive(deadline, stop_token,
                               "cgroup controller update");
  cgroup.RequireLinkedIdentity();
  if (controllers.empty()) {
    return;
  }
  if (operation != '+' && operation != '-') {
    throw std::runtime_error("invalid cgroup controller operation");
  }
  WriteCgroupFileAt(cgroup, "cgroup.subtree_control",
                    ControllerRequest(controllers, operation), deadline,
                    stop_token);
  const std::set<std::string> after =
      ControllerSetAt(cgroup, deadline, stop_token);
  for (const std::string& controller : controllers) {
    const bool expected = operation == '+';
    if (after.contains(controller) != expected) {
      throw std::runtime_error("cgroup controller " + controller +
                               " read-back verification failed at " +
                               cgroup.display_path().string());
    }
  }
}

std::vector<pid_t> BoundCgroupPids(
    const BoundCgroupDirectory& cgroup,
    std::optional<std::chrono::steady_clock::time_point> deadline,
    std::stop_token stop_token) {
  std::vector<pid_t> pids;
  for (const std::string& text : SplitWhitespace(
           ReadCgroupFileAt(cgroup, "cgroup.procs", deadline, stop_token))) {
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

void MoveCgroupProcesses(
    const BoundCgroupDirectory& source, const BoundCgroupDirectory& destination,
    std::string_view context,
    std::optional<std::chrono::steady_clock::time_point> deadline,
    std::stop_token stop_token) {
  for (int attempt = 0; attempt < 20; ++attempt) {
    RequireCgroupOperationActive(deadline, stop_token, context);
    source.RequireLinkedIdentity();
    destination.RequireLinkedIdentity();
    const std::vector<pid_t> pids =
        BoundCgroupPids(source, deadline, stop_token);
    if (pids.empty()) {
      source.RequireLinkedIdentity();
      destination.RequireLinkedIdentity();
      return;
    }
    for (const pid_t pid : pids) {
      RequireCgroupOperationActive(deadline, stop_token, context);
      try {
        WriteCgroupFileAt(destination, "cgroup.procs", std::to_string(pid),
                          deadline, stop_token);
      } catch (const CgroupFileWriteError& error) {
        if (error.error() != ESRCH) {
          throw;
        }
        source.RequireLinkedIdentity();
        destination.RequireLinkedIdentity();
      }
    }
    source.RequireLinkedIdentity();
    destination.RequireLinkedIdentity();
    const bool source_empty =
        BoundCgroupPids(source, deadline, stop_token).empty();
    destination.RequireLinkedIdentity();
    if (source_empty) {
      return;
    }
    const auto now = std::chrono::steady_clock::now();
    const auto delay =
        deadline
            ? std::min(std::chrono::milliseconds(20),
                       std::chrono::duration_cast<std::chrono::milliseconds>(
                           *deadline - now))
            : std::chrono::milliseconds(20);
    if (delay > std::chrono::milliseconds::zero()) {
      std::this_thread::sleep_for(delay);
    }
  }
  throw std::runtime_error(
      std::string(context) +
      "; source cgroup still has processes: " + source.display_path().string());
}

bool BoundCgroupPopulated(const BoundCgroupDirectory& cgroup,
                          std::chrono::steady_clock::time_point deadline,
                          std::stop_token stop_token) {
  return ParseKeyValueText(
             ReadCgroupFileAt(cgroup, "cgroup.events", deadline, stop_token),
             "populated",
             (cgroup.display_path() / "cgroup.events").string()) != 0U;
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

bool TryWriteBoundCgroupKill(const BoundCgroupDirectory& cgroup,
                             std::chrono::steady_clock::time_point deadline,
                             std::stop_token stop_token) {
  RequireCgroupOperationActive(deadline, stop_token, "cgroup process kill");
  cgroup.RequireLinkedIdentity();
  const int descriptor = openat(cgroup.descriptor(), "cgroup.kill",
                                O_WRONLY | O_CLOEXEC | O_NOFOLLOW | O_NONBLOCK);
  if (descriptor < 0) {
    if (errno == ENOENT) {
      return false;
    }
    throw std::runtime_error("open cgroup.kill failed for " +
                             cgroup.display_path().string() + ": " +
                             std::strerror(errno));
  }
  try {
    struct stat status{};
    if (fstat(descriptor, &status) != 0 || !S_ISREG(status.st_mode)) {
      throw std::runtime_error("cgroup.kill is not a regular control file: " +
                               cgroup.display_path().string());
    }
    if (CgroupMountId(descriptor) != cgroup.mount_id()) {
      throw CgroupOwnershipMismatch("cgroup.kill crosses a mount boundary: " +
                                    cgroup.display_path().string());
    }
    std::string_view value = "1";
    while (!value.empty()) {
      RequireCgroupOperationActive(deadline, stop_token, "cgroup process kill");
      const ssize_t count = write(descriptor, value.data(), value.size());
      if (count < 0) {
        if (errno == EINTR) {
          continue;
        }
        throw std::runtime_error("write cgroup.kill failed for " +
                                 cgroup.display_path().string() + ": " +
                                 std::strerror(errno));
      }
      if (count == 0) {
        throw std::runtime_error("write cgroup.kill made no progress for " +
                                 cgroup.display_path().string());
      }
      value.remove_prefix(static_cast<std::size_t>(count));
    }
  } catch (...) {
    static_cast<void>(close(descriptor));
    throw;
  }
  if (close(descriptor) != 0) {
    throw std::runtime_error("close cgroup.kill failed for " +
                             cgroup.display_path().string() + ": " +
                             std::strerror(errno));
  }
  cgroup.RequireLinkedIdentity();
  return true;
}

void WaitForBoundCgroupEmpty(const BoundCgroupDirectory& cgroup,
                             std::chrono::steady_clock::time_point deadline,
                             std::stop_token stop_token) {
  for (;;) {
    RequireCgroupOperationActive(deadline, stop_token, "cgroup process wait");
    cgroup.RequireLinkedIdentity();
    if (!BoundCgroupPopulated(cgroup, deadline, stop_token) &&
        BoundCgroupPids(cgroup, deadline, stop_token).empty()) {
      return;
    }
    const auto now = std::chrono::steady_clock::now();
    std::this_thread::sleep_for(std::min(
        std::chrono::milliseconds(20),
        std::chrono::duration_cast<std::chrono::milliseconds>(deadline - now)));
  }
}

void KillBoundCgroupProcesses(const BoundCgroupDirectory& cgroup,
                              std::chrono::steady_clock::time_point deadline,
                              std::stop_token stop_token,
                              bool force_pidfd_fallback = false) {
  RequireCgroupOperationActive(deadline, stop_token, "cgroup process kill");
  if (!force_pidfd_fallback &&
      TryWriteBoundCgroupKill(cgroup, deadline, stop_token)) {
    WaitForBoundCgroupEmpty(cgroup, deadline, stop_token);
    return;
  }
  for (;;) {
    RequireCgroupOperationActive(deadline, stop_token, "cgroup process kill");
    cgroup.RequireLinkedIdentity();
    const std::vector<pid_t> pids =
        BoundCgroupPids(cgroup, deadline, stop_token);
    if (pids.empty()) {
      if (BoundCgroupPopulated(cgroup, deadline, stop_token)) {
        throw std::runtime_error(
            "cgroup remained recursively populated without cgroup.kill: " +
            cgroup.display_path().string());
      }
      return;
    }
    for (const pid_t pid : pids) {
      RequireCgroupOperationActive(deadline, stop_token, "cgroup process kill");
      const int pidfd = OpenPidfd(pid);
      if (pidfd < 0) {
        continue;
      }
      try {
        const std::vector<pid_t> current =
            BoundCgroupPids(cgroup, deadline, stop_token);
        if (std::find(current.begin(), current.end(), pid) != current.end()) {
          RequireCgroupOperationActive(deadline, stop_token,
                                       "cgroup process kill");
          SignalPidfd(pidfd, pid);
        }
      } catch (...) {
        static_cast<void>(close(pidfd));
        throw;
      }
      if (close(pidfd) != 0) {
        throw std::runtime_error("close pidfd failed for cgroup process " +
                                 std::to_string(pid) + ": " +
                                 std::strerror(errno));
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
  if (stop_token.stop_requested()) {
    throw std::runtime_error("cgroup process kill cancelled: " + dir.string());
  }
  if (std::chrono::steady_clock::now() >= deadline) {
    throw std::runtime_error("cgroup process kill deadline expired: " +
                             dir.string());
  }
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

std::vector<std::string> BoundCgroupDirectoryNames(
    const BoundCgroupDirectory& cgroup,
    std::chrono::steady_clock::time_point deadline,
    std::stop_token stop_token) {
  RequireCgroupOperationActive(deadline, stop_token, "cgroup traversal");
  cgroup.RequireLinkedIdentity();
  const int duplicate = fcntl(cgroup.descriptor(), F_DUPFD_CLOEXEC, 0);
  if (duplicate < 0) {
    throw std::runtime_error("duplicate cgroup directory failed for " +
                             cgroup.display_path().string() + ": " +
                             std::strerror(errno));
  }
  DIR* raw_directory = fdopendir(duplicate);
  if (raw_directory == nullptr) {
    const int error = errno;
    static_cast<void>(close(duplicate));
    throw std::runtime_error("open cgroup directory stream failed for " +
                             cgroup.display_path().string() + ": " +
                             std::strerror(error));
  }
  std::unique_ptr<DIR, int (*)(DIR*)> directory(raw_directory, closedir);
  std::vector<std::string> names;
  for (;;) {
    RequireCgroupOperationActive(deadline, stop_token, "cgroup traversal");
    errno = 0;
    dirent* entry = readdir(directory.get());
    if (entry == nullptr) {
      if (errno != 0) {
        throw std::runtime_error("read cgroup directory failed for " +
                                 cgroup.display_path().string() + ": " +
                                 std::strerror(errno));
      }
      break;
    }
    const std::string_view name(entry->d_name);
    if (name == "." || name == "..") {
      continue;
    }
    names.emplace_back(name);
  }
  std::sort(names.begin(), names.end());
  cgroup.RequireLinkedIdentity();
  return names;
}

void RemoveBoundCgroupDirectory(BoundCgroupDirectory* cgroup,
                                std::chrono::steady_clock::time_point deadline,
                                std::stop_token stop_token) {
  RequireCgroupOperationActive(deadline, stop_token, "cgroup removal");
  cgroup->RequireLinkedIdentity();
  if (BoundCgroupPopulated(*cgroup, deadline, stop_token) ||
      !BoundCgroupPids(*cgroup, deadline, stop_token).empty()) {
    throw std::runtime_error("refusing to remove a populated cgroup: " +
                             cgroup->display_path().string());
  }
  RequireCgroupOperationActive(deadline, stop_token, "cgroup removal");
  cgroup->RequireLinkedIdentity();
  struct stat linked{};
  if (fstat(cgroup->descriptor(), &linked) != 0) {
    throw std::runtime_error("inspect acquired cgroup before removal failed: " +
                             cgroup->display_path().string() + ": " +
                             std::strerror(errno));
  }
  if (!SameCgroupIdentity(linked, cgroup->identity())) {
    throw CgroupOwnershipMismatch(
        "acquired cgroup identity changed before removal: " +
        cgroup->display_path().string());
  }
  // Linux has no compare-identity form of rmdir/unlinkat. The final linked
  // identity validation and this unlinkat therefore cannot be one atomic step.
  if (unlinkat(cgroup->parent_descriptor(), cgroup->name().c_str(),
               AT_REMOVEDIR) != 0) {
    throw std::runtime_error("remove acquired cgroup failed for " +
                             cgroup->display_path().string() + ": " +
                             std::strerror(errno));
  }
  cgroup->MarkRemoved();
  struct stat removed{};
  if (fstat(cgroup->descriptor(), &removed) != 0) {
    throw std::runtime_error("inspect removed cgroup identity failed for " +
                             cgroup->display_path().string() + ": " +
                             std::strerror(errno));
  }
  // Kernfs retains the pre-rmdir link count on an open cgroup descriptor,
  // while conventional filesystems can report zero for an unlinked inode.
  if (!S_ISDIR(removed.st_mode) ||
      !SameCgroupIdentity(removed, cgroup->identity()) ||
      (removed.st_nlink != 0U && removed.st_nlink != linked.st_nlink)) {
    throw CgroupOwnershipMismatch(
        "removed cgroup descriptor has an unexpected link count or identity: " +
        cgroup->display_path().string());
  }
  struct stat remaining{};
  if (fstatat(cgroup->parent_descriptor(), cgroup->name().c_str(), &remaining,
              AT_SYMLINK_NOFOLLOW) == 0) {
    throw CgroupOwnershipMismatch(
        "a replacement appeared at a removed cgroup path: " +
        cgroup->display_path().string());
  }
  if (errno != ENOENT) {
    throw std::runtime_error("verify removed cgroup path failed for " +
                             cgroup->display_path().string() + ": " +
                             std::strerror(errno));
  }
}

void RemoveBoundCgroupDescendants(
    BoundCgroupDirectory* parent, std::size_t depth,
    std::chrono::steady_clock::time_point deadline,
    std::stop_token stop_token) {
  constexpr std::size_t kMaximumCgroupDepth = 256U;
  if (depth > kMaximumCgroupDepth) {
    throw std::runtime_error(
        "cgroup cleanup exceeded its directory-depth bound");
  }
  for (const std::string& name :
       BoundCgroupDirectoryNames(*parent, deadline, stop_token)) {
    RequireCgroupOperationActive(deadline, stop_token, "cgroup traversal");
    struct stat status{};
    if (fstatat(parent->descriptor(), name.c_str(), &status,
                AT_SYMLINK_NOFOLLOW) != 0) {
      if (errno == ENOENT) {
        throw CgroupOwnershipMismatch(
            "a cgroup descendant disappeared during traversal: " +
            (parent->display_path() / name).string());
      }
      throw std::runtime_error("inspect cgroup descendant failed for " +
                               (parent->display_path() / name).string() + ": " +
                               std::strerror(errno));
    }
    if (!S_ISDIR(status.st_mode)) {
      continue;
    }
    std::optional<BoundCgroupDirectory> child = BoundCgroupDirectory::TryOpen(
        parent->descriptor(), name, parent->display_path() / name);
    if (!child) {
      throw CgroupOwnershipMismatch(
          "a cgroup descendant disappeared during acquisition: " +
          (parent->display_path() / name).string());
    }
#ifdef BBP_ENABLE_TEST_HOOKS
    if (cgroup_removal_identity_hook) {
      cgroup_removal_identity_hook(
          CgroupRemovalTestPhase::kAfterDescendantIdentityVerification,
          child->display_path());
    }
#endif
    child->RequireLinkedIdentity();
    RemoveBoundCgroupDescendants(&*child, depth + 1U, deadline, stop_token);
    KillBoundCgroupProcesses(*child, deadline, stop_token);
    RemoveBoundCgroupDirectory(&*child, deadline, stop_token);
  }
  parent->RequireLinkedIdentity();
}

std::optional<BoundCgroupDirectory> AcquireRunCgroup(
    const BoundCgroupDirectory& simulator, const std::string& run_id,
    std::optional<CgroupPathIdentity> expected_identity) {
  return BoundCgroupDirectory::TryOpen(simulator.descriptor(), run_id,
                                       simulator.display_path() / run_id,
                                       expected_identity);
}

void RemoveRunCgroup(BoundCgroupDirectory* run,
                     std::chrono::steady_clock::time_point deadline,
                     std::stop_token stop_token) {
  RequireCgroupOperationActive(deadline, stop_token, "run cgroup removal");
#ifdef BBP_ENABLE_TEST_HOOKS
  if (cgroup_removal_identity_hook) {
    cgroup_removal_identity_hook(
        CgroupRemovalTestPhase::kAfterRunIdentityVerification,
        run->display_path());
  }
#endif
  run->RequireLinkedIdentity();
  RemoveBoundCgroupDescendants(run, 0U, deadline, stop_token);
  KillBoundCgroupProcesses(*run, deadline, stop_token);
  RemoveBoundCgroupDirectory(run, deadline, stop_token);
}

bool ScopeStateExists(const CgroupScopeConfig& config) {
  struct stat status{};
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

void RequireScopeChildName(std::string_view name, std::string_view kind) {
  if (name.empty() || name == "." || name == ".." ||
      name.find('/') != std::string_view::npos) {
    throw std::runtime_error("invalid " + std::string(kind) +
                             " cgroup name: " + std::string(name));
  }
}

std::optional<BoundCgroupDirectory> AcquireBoundScopeChild(
    const BoundCgroupDirectory& parent, const std::string& name,
    const std::filesystem::path& display_path,
    const std::optional<CgroupDirectoryIdentity>& expected_identity) {
  RequireScopeChildName(name, "scope");
  return BoundCgroupDirectory::TryOpen(
      parent.descriptor(), name, display_path,
      expected_identity
          ? std::optional<CgroupPathIdentity>(expected_identity->path)
          : std::nullopt,
      expected_identity
          ? std::optional<std::uint64_t>(expected_identity->mount_id)
          : std::nullopt);
}

void CreateCgroupDirectoryExclusiveAt(const BoundCgroupDirectory& parent,
                                      const std::string& name,
                                      std::string_view kind) {
  RequireScopeChildName(name, kind);
  parent.RequireLinkedIdentity();
  if (mkdirat(parent.descriptor(), name.c_str(), 0777) != 0) {
    if (errno == EEXIST) {
      throw std::runtime_error(
          "refusing to adopt pre-existing " + std::string(kind) +
          " cgroup: " + (parent.display_path() / name).string());
    }
    throw std::runtime_error(
        "create " + std::string(kind) + " cgroup failed for " +
        (parent.display_path() / name).string() + ": " + std::strerror(errno));
  }
  parent.RequireLinkedIdentity();
}

std::set<std::string> ControllerFileSetAt(
    const BoundCgroupDirectory& cgroup, std::string_view file,
    std::optional<std::chrono::steady_clock::time_point> deadline =
        std::nullopt,
    std::stop_token stop_token = {}) {
  const std::vector<std::string> values =
      SplitWhitespace(ReadCgroupFileAt(cgroup, file, deadline, stop_token));
  return std::set<std::string>(values.begin(), values.end());
}

std::set<std::string> DesiredControllersAt(
    const BoundCgroupDirectory& cgroup,
    std::optional<std::chrono::steady_clock::time_point> deadline =
        std::nullopt,
    std::stop_token stop_token = {}) {
  const std::set<std::string> available =
      ControllerFileSetAt(cgroup, "cgroup.controllers", deadline, stop_token);
  std::set<std::string> desired;
  for (std::string_view controller : {"cpu", "io", "memory", "pids"}) {
    if (available.contains(std::string(controller))) {
      desired.emplace(controller);
    }
  }
  return desired;
}

void RequireControllersAvailableAt(
    const BoundCgroupDirectory& cgroup,
    std::initializer_list<std::string_view> required_controllers,
    std::optional<std::chrono::steady_clock::time_point> deadline =
        std::nullopt,
    std::stop_token stop_token = {}) {
  const std::set<std::string> available =
      ControllerFileSetAt(cgroup, "cgroup.controllers", deadline, stop_token);
  for (const std::string_view required : required_controllers) {
    if (!available.contains(std::string(required))) {
      throw std::runtime_error("required cgroup controller unavailable at " +
                               cgroup.display_path().string() + ": " +
                               std::string(required));
    }
  }
}

void EnableControllersAt(const BoundCgroupDirectory& cgroup,
                         std::optional<std::chrono::steady_clock::time_point>
                             deadline = std::nullopt,
                         std::stop_token stop_token = {}) {
  SetControllersAt(cgroup, DesiredControllersAt(cgroup, deadline, stop_token),
                   '+', deadline, stop_token);
}

void RequireControllersPreservedAt(
    const BoundCgroupDirectory& cgroup, const std::set<std::string>& before,
    const std::set<std::string>& added,
    std::optional<std::chrono::steady_clock::time_point> deadline,
    std::stop_token stop_token) {
  const std::set<std::string> after =
      ControllerSetAt(cgroup, deadline, stop_token);
  for (const std::string& controller : before) {
    if (!after.contains(controller)) {
      throw std::runtime_error(
          "pre-existing cgroup controller disappeared at " +
          cgroup.display_path().string() + ": " + controller);
    }
  }
  for (const std::string& controller : added) {
    if (after.contains(controller)) {
      throw std::runtime_error(
          "BBP-added cgroup controller survived cleanup at " +
          cgroup.display_path().string() + ": " + controller);
    }
  }
}

void ValidateScopeControllerPlan(
    const CgroupScopeState& state, const BoundCgroupDirectory& root,
    std::optional<std::chrono::steady_clock::time_point> deadline,
    std::stop_token stop_token) {
  const std::set<std::string> desired =
      DesiredControllersAt(root, deadline, stop_token);
  if (state.root_controllers_added !=
          SetDifference(desired, state.root_controllers_before) ||
      state.simulator_controllers_added !=
          SetDifference(desired, state.simulator_controllers_before)) {
    throw std::runtime_error("cgroup scope root controller state is invalid");
  }
}

void RequireBoundScopeHierarchy(const CgroupScopeConfig& config,
                                const CgroupScopeState& state,
                                const BoundCgroupScope& scope) {
  scope.root.RequireLinkedIdentity();
  if (!state.root_identity ||
      scope.root.directory_identity() != *state.root_identity) {
    throw CgroupOwnershipMismatch(
        "cgroup scope root identity no longer matches durable state");
  }
  const std::filesystem::path simulator_path =
      config.root / config.simulator_name;
  if (scope.simulator) {
    scope.simulator->RequireLinkedIdentity();
    if (!state.simulator_identity ||
        scope.simulator->directory_identity() != *state.simulator_identity) {
      throw CgroupOwnershipMismatch(
          "cgroup scope simulator identity no longer matches durable state");
    }
  } else {
    RequireAbsentCgroupChild(scope.root, config.simulator_name, simulator_path);
  }

  if (!state.controller_expected) {
    if (scope.controller || state.controller_identity) {
      throw CgroupOwnershipMismatch(
          "cgroup scope has an unexpected owned controller identity");
    }
    return;
  }
  if (!scope.simulator) {
    return;
  }
  const std::filesystem::path controller_path =
      simulator_path / state.controller_name;
  if (scope.controller) {
    scope.controller->RequireLinkedIdentity();
    if (!state.controller_identity ||
        scope.controller->directory_identity() != *state.controller_identity) {
      throw CgroupOwnershipMismatch(
          "cgroup scope controller identity no longer matches durable state");
    }
  } else {
    RequireAbsentCgroupChild(*scope.simulator, state.controller_name,
                             controller_path);
  }
}

BoundCgroupScope BindNewCgroupScope(const CgroupScopeConfig& config,
                                    const CgroupScopeLock& scope_lock) {
  RequireScopeChildName(config.simulator_name, "simulator");
  BoundCgroupDirectory root =
      AcquireBoundScopeRoot(config, scope_lock, std::nullopt);
  std::optional<BoundCgroupDirectory> simulator =
      AcquireBoundScopeChild(root, config.simulator_name,
                             config.root / config.simulator_name, std::nullopt);
  return BoundCgroupScope{
      .root = std::move(root),
      .simulator = std::move(simulator),
      .controller = std::nullopt,
  };
}

BoundCgroupScope BindCgroupScope(
    const CgroupScopeConfig& config, const CgroupScopeLock& scope_lock,
    CgroupScopeState* state,
    std::optional<std::chrono::steady_clock::time_point> deadline =
        std::nullopt,
    std::stop_token stop_token = {}) {
  RequireCgroupOperationActive(deadline, stop_token,
                               "cgroup scope identity binding");
  RequireScopeChildName(config.simulator_name, "simulator");
  const bool legacy = state->legacy_scope_identities;
  if (legacy && state->active_runs.empty() && !state->pending_run) {
    throw CgroupOwnershipMismatch(
        "refusing identity-less legacy cgroup scope restoration");
  }
  BoundCgroupDirectory root = AcquireBoundScopeRoot(
      config, scope_lock, legacy ? std::nullopt : state->root_identity);
  std::optional<BoundCgroupDirectory> simulator = AcquireBoundScopeChild(
      root, config.simulator_name, config.root / config.simulator_name,
      legacy ? std::nullopt : state->simulator_identity);
  const bool scope_in_use =
      !state->active_runs.empty() || state->pending_run.has_value();
  if (!legacy && !state->simulator_identity && simulator) {
    throw CgroupOwnershipMismatch(
        "refusing to adopt an identity-less simulator cgroup");
  }
  if (!simulator &&
      (legacy || state->simulator_preexisting || scope_in_use ||
       (state->simulator_identity && !state->restoration_ready))) {
    throw CgroupOwnershipMismatch(
        "expected cgroup scope simulator identity disappeared");
  }

  std::optional<BoundCgroupDirectory> controller;
  if (legacy && simulator) {
    controller = AcquireBoundScopeChild(
        *simulator, state->controller_name,
        simulator->display_path() / state->controller_name, std::nullopt);
    if (!controller && !state->root_controllers_added.empty()) {
      throw CgroupOwnershipMismatch(
          "legacy cgroup scope cannot prove its historical controller mode");
    }
    state->controller_expected = controller.has_value();
  } else if (state->controller_expected && simulator) {
    controller = AcquireBoundScopeChild(
        *simulator, state->controller_name,
        simulator->display_path() / state->controller_name,
        legacy ? std::nullopt : state->controller_identity);
    if (!legacy && !state->controller_identity && controller) {
      throw CgroupOwnershipMismatch(
          "refusing to adopt an identity-less scope controller cgroup");
    }
    if (!controller &&
        (legacy || scope_in_use ||
         (state->controller_identity && !state->restoration_ready))) {
      throw CgroupOwnershipMismatch(
          "expected cgroup scope controller identity disappeared");
    }
  }

  BoundCgroupScope scope{
      .root = std::move(root),
      .simulator = std::move(simulator),
      .controller = std::move(controller),
  };
  ValidateScopeControllerPlan(*state, scope.root, deadline, stop_token);
  if (legacy) {
    if (!scope.simulator || (state->controller_expected && !scope.controller)) {
      throw CgroupOwnershipMismatch(
          "legacy cgroup scope hierarchy is incomplete");
    }
    std::set<std::string> expected_runs = state->active_runs;
    if (state->pending_run_created) {
      expected_runs.insert(*state->pending_run);
    }
    for (const std::string& run_id : expected_runs) {
      const auto binding = state->run_bindings.find(run_id);
      const std::optional<CgroupPathIdentity> expected_identity =
          binding == state->run_bindings.end()
              ? std::nullopt
              : std::optional<CgroupPathIdentity>(
                    binding->second.cgroup_identity);
      if (!AcquireRunCgroup(*scope.simulator, run_id, expected_identity)) {
        throw CgroupOwnershipMismatch(
            "legacy cgroup scope run disappeared before identity migration: " +
            run_id);
      }
    }
    state->root_identity = scope.root.directory_identity();
    state->simulator_identity = scope.simulator->directory_identity();
    state->controller_identity =
        scope.controller ? std::optional<CgroupDirectoryIdentity>(
                               scope.controller->directory_identity())
                         : std::nullopt;
    state->restoration_ready = false;
    state->legacy_scope_identities = false;
    RequireCgroupOperationActive(deadline, stop_token,
                                 "cgroup scope identity migration");
    WriteCgroupScopeState(config.state_file, *state);
    RequireCgroupOperationActive(deadline, stop_token,
                                 "cgroup scope identity migration");
  }
  RequireBoundScopeHierarchy(config, *state, scope);
  return scope;
}

void RecordExistingResourceCgroups(const BoundCgroupDirectory& simulator,
                                   CgroupScopeState* state) {
  for (const std::string& name : BoundCgroupDirectoryNames(
           simulator, std::chrono::steady_clock::time_point::max(), {})) {
    if (!IsResourceCgroupName(name)) {
      continue;
    }
    struct stat status{};
    if (fstatat(simulator.descriptor(), name.c_str(), &status,
                AT_SYMLINK_NOFOLLOW) != 0) {
      if (errno == ENOENT) {
        throw CgroupOwnershipMismatch(
            "resource cgroup disappeared while recording scope state: " +
            (simulator.display_path() / name).string());
      }
      throw std::runtime_error("inspect simulator resource cgroup failed for " +
                               (simulator.display_path() / name).string() +
                               ": " + std::strerror(errno));
    }
    if (S_ISDIR(status.st_mode)) {
      state->active_runs.insert(name);
    }
  }
  simulator.RequireLinkedIdentity();
}

CgroupScopeState NewCgroupScopeState(const CgroupScopeConfig& config,
                                     const BoundCgroupScope& scope) {
  RequireControllersAvailableAt(scope.root, {"cpu", "io", "memory", "pids"});
  const std::set<std::string> desired = DesiredControllersAt(scope.root);
  CgroupScopeState state{
      .root = config.root,
      .simulator_name = config.simulator_name,
      .controller_name =
          std::string(kOwnedControllerPrefix) + RandomScopeToken(),
      .simulator_preexisting = scope.simulator.has_value(),
      .controller_expected = config.allow_root_process_move,
      .restoration_ready = false,
      .root_identity = scope.root.directory_identity(),
      .simulator_identity = scope.simulator
                                ? std::optional<CgroupDirectoryIdentity>(
                                      scope.simulator->directory_identity())
                                : std::nullopt,
      .controller_identity = std::nullopt,
      .root_controllers_before = ControllerSetAt(scope.root, std::nullopt, {}),
      .simulator_controllers_before =
          scope.simulator ? ControllerSetAt(*scope.simulator, std::nullopt, {})
                          : std::set<std::string>{},
      .root_controllers_added = {},
      .simulator_controllers_added = {},
      .active_runs = {},
      .run_bindings = {},
      .pending_run = std::nullopt,
      .pending_run_created = false,
      .legacy_scope_identities = false,
  };
  state.root_controllers_added =
      SetDifference(desired, state.root_controllers_before);
  state.simulator_controllers_added =
      SetDifference(desired, state.simulator_controllers_before);
  if (!config.allow_root_process_move &&
      !state.root_controllers_added.empty()) {
    throw std::runtime_error(
        "native cgroup root unexpectedly requires controller mutation at " +
        config.root.string() + ": " +
        ControllerList(state.root_controllers_added));
  }
  if (scope.simulator) {
    RecordExistingResourceCgroups(*scope.simulator, &state);
  }
  return state;
}

void StartCgroupScope(const CgroupScopeConfig& config, CgroupScopeState* state,
                      BoundCgroupScope* scope) {
  RequireBoundScopeHierarchy(config, *state, *scope);
  if (!scope->simulator) {
    CreateCgroupDirectoryExclusiveAt(scope->root, config.simulator_name,
                                     "simulator");
    scope->simulator = AcquireBoundScopeChild(
        scope->root, config.simulator_name, config.root / config.simulator_name,
        std::nullopt);
    if (!scope->simulator) {
      throw CgroupOwnershipMismatch(
          "new simulator cgroup disappeared during acquisition");
    }
    state->simulator_identity = scope->simulator->directory_identity();
    WriteCgroupScopeState(config.state_file, *state);
    RequireBoundScopeHierarchy(config, *state, *scope);
  }
  if (state->controller_expected) {
    CreateCgroupDirectoryExclusiveAt(*scope->simulator, state->controller_name,
                                     "controller");
    scope->controller = AcquireBoundScopeChild(
        *scope->simulator, state->controller_name,
        scope->simulator->display_path() / state->controller_name,
        std::nullopt);
    if (!scope->controller) {
      throw CgroupOwnershipMismatch(
          "new scope controller disappeared during acquisition");
    }
    state->controller_identity = scope->controller->directory_identity();
    WriteCgroupScopeState(config.state_file, *state);
    RequireBoundScopeHierarchy(config, *state, *scope);
    MoveCgroupProcesses(scope->root, *scope->controller,
                        "could not delegate cgroup root", std::nullopt, {});
    SetControllersAt(scope->root, state->root_controllers_added, '+',
                     std::nullopt, {});
  }
  RequireControllersAvailableAt(*scope->simulator,
                                {"cpu", "io", "memory", "pids"});
  SetControllersAt(*scope->simulator, state->simulator_controllers_added, '+',
                   std::nullopt, {});
  RequireBoundScopeHierarchy(config, *state, *scope);
}

void RemoveScopeStateFile(const CgroupScopeConfig& config) {
  if (unlink(config.state_file.c_str()) != 0) {
    throw std::runtime_error("remove cgroup scope state failed for " +
                             config.state_file.string() + ": " +
                             std::strerror(errno));
  }
  struct stat status{};
  if (lstat(config.state_file.c_str(), &status) == 0 || errno != ENOENT) {
    throw std::runtime_error(
        "cgroup scope state absence read-back failed for " +
        config.state_file.string());
  }
  SyncCgroupScopeStateDirectory(config.state_file, "removal");
}

void RequireNoCgroupDirectoryChildren(
    const BoundCgroupDirectory& cgroup,
    const std::optional<std::string>& allowed_child,
    std::chrono::steady_clock::time_point deadline,
    std::stop_token stop_token) {
  for (const std::string& name :
       BoundCgroupDirectoryNames(cgroup, deadline, stop_token)) {
    struct stat status{};
    if (fstatat(cgroup.descriptor(), name.c_str(), &status,
                AT_SYMLINK_NOFOLLOW) != 0) {
      if (errno == ENOENT) {
        throw CgroupOwnershipMismatch(
            "cgroup child disappeared during emptiness verification: " +
            (cgroup.display_path() / name).string());
      }
      throw std::runtime_error("inspect cgroup child failed for " +
                               (cgroup.display_path() / name).string() + ": " +
                               std::strerror(errno));
    }
    if (S_ISDIR(status.st_mode) && (!allowed_child || name != *allowed_child)) {
      throw std::runtime_error(
          "refusing to restore a cgroup scope with an unexpected child: " +
          (cgroup.display_path() / name).string());
    }
  }
  cgroup.RequireLinkedIdentity();
}

void RestoreCgroupScope(const CgroupScopeConfig& config,
                        CgroupScopeState* state, BoundCgroupScope* scope,
                        std::optional<std::chrono::steady_clock::time_point>
                            deadline = std::nullopt,
                        std::stop_token stop_token = {}) {
  if (!state->active_runs.empty() || !state->run_bindings.empty() ||
      state->pending_run) {
    throw std::runtime_error(
        "refusing to restore cgroup scope while owned runs remain");
  }
  if (state->legacy_scope_identities) {
    throw CgroupOwnershipMismatch(
        "refusing to restore a cgroup scope without durable identities");
  }
  RequireCgroupOperationActive(deadline, stop_token,
                               "cgroup scope restoration");
  const std::filesystem::path controller =
      config.root / config.simulator_name / state->controller_name;
  RequireBoundScopeHierarchy(config, *state, *scope);
#ifdef BBP_ENABLE_TEST_HOOKS
  if (cgroup_removal_identity_hook) {
    cgroup_removal_identity_hook(
        CgroupRemovalTestPhase::kAfterScopeHierarchyIdentityVerification,
        controller);
  }
#endif
  RequireBoundScopeHierarchy(config, *state, *scope);

  const std::chrono::steady_clock::time_point bound_deadline =
      deadline.value_or(std::chrono::steady_clock::time_point::max());
  const auto remove_scope_directory = [&](BoundCgroupDirectory* directory) {
    try {
      RemoveBoundCgroupDirectory(directory, bound_deadline, stop_token);
    } catch (...) {
      const std::exception_ptr removal_failure = std::current_exception();
      try {
        RequireBoundScopeHierarchy(config, *state, *scope);
      } catch (const CgroupOwnershipMismatch&) {
        throw;
      } catch (...) {
      }
      std::rethrow_exception(removal_failure);
    }
  };
  if (!state->restoration_ready) {
    if (scope->simulator) {
      SetControllersAt(*scope->simulator, state->simulator_controllers_added,
                       '-', deadline, stop_token);
    }
    RequireCgroupOperationActive(deadline, stop_token,
                                 "cgroup scope restoration");
    SetControllersAt(scope->root, state->root_controllers_added, '-', deadline,
                     stop_token);
    RequireCgroupOperationActive(deadline, stop_token,
                                 "cgroup scope restoration");
    if (scope->controller) {
      MoveCgroupProcesses(*scope->controller, scope->root,
                          "could not restore cgroup-root processes", deadline,
                          stop_token);
      if (!BoundCgroupPids(*scope->controller, deadline, stop_token).empty() ||
          BoundCgroupPopulated(*scope->controller, bound_deadline,
                               stop_token)) {
        throw std::runtime_error(
            "scope controller remained populated after process restoration: " +
            scope->controller->display_path().string());
      }
      RequireNoCgroupDirectoryChildren(*scope->controller, std::nullopt,
                                       bound_deadline, stop_token);
    }
    RequireControllersPreservedAt(scope->root, state->root_controllers_before,
                                  state->root_controllers_added, deadline,
                                  stop_token);
    if (scope->simulator) {
      RequireControllersPreservedAt(
          *scope->simulator, state->simulator_controllers_before,
          state->simulator_controllers_added, deadline, stop_token);
      if (!state->simulator_preexisting) {
        if (!BoundCgroupPids(*scope->simulator, deadline, stop_token).empty() ||
            BoundCgroupPopulated(*scope->simulator, bound_deadline,
                                 stop_token)) {
          throw std::runtime_error(
              "owned simulator cgroup remained populated during restoration: " +
              scope->simulator->display_path().string());
        }
        RequireNoCgroupDirectoryChildren(
            *scope->simulator,
            scope->controller
                ? std::optional<std::string>(state->controller_name)
                : std::nullopt,
            bound_deadline, stop_token);
      }
    }
    RequireBoundScopeHierarchy(config, *state, *scope);
    RequireCgroupOperationActive(deadline, stop_token,
                                 "cgroup restoration-ready checkpoint");
    state->restoration_ready = true;
    WriteCgroupScopeState(config.state_file, *state);
    RequireCgroupOperationActive(deadline, stop_token,
                                 "cgroup restoration-ready checkpoint");
    RequireBoundScopeHierarchy(config, *state, *scope);
  }

  RequireCgroupOperationActive(deadline, stop_token,
                               "cgroup scope restoration");
  if (scope->controller) {
    remove_scope_directory(&*scope->controller);
    scope->controller.reset();
    RequireBoundScopeHierarchy(config, *state, *scope);
  }
  RequireCgroupOperationActive(deadline, stop_token,
                               "cgroup scope restoration");
  if (!state->simulator_preexisting && scope->simulator) {
    remove_scope_directory(&*scope->simulator);
    scope->simulator.reset();
  }
  RequireBoundScopeHierarchy(config, *state, *scope);
  RequireControllersPreservedAt(scope->root, state->root_controllers_before,
                                state->root_controllers_added, deadline,
                                stop_token);
  if (state->simulator_preexisting) {
    if (!scope->simulator) {
      throw CgroupOwnershipMismatch(
          "pre-existing simulator disappeared during scope restoration");
    }
    RequireControllersPreservedAt(
        *scope->simulator, state->simulator_controllers_before,
        state->simulator_controllers_added, deadline, stop_token);
  }
  RequireCgroupOperationActive(deadline, stop_token,
                               "cgroup scope restoration");
  RequireBoundScopeHierarchy(config, *state, *scope);
  // State-file unlink is the restoration commit point. Once it is absent, the
  // scope is fully restored and cancellation belongs to the caller's next work.
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
                                         BoundCgroupScope* scope,
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
    if (DirectoryIdentity(binding->second.run_root, "run root") !=
        binding->second.run_root_identity) {
      throw std::runtime_error(
          "refusing recovery deletion for a replaced pending run resource");
    }
    if (!scope->simulator) {
      throw CgroupOwnershipMismatch(
          "pending run simulator identity disappeared before recovery");
    }
    std::optional<BoundCgroupDirectory> run =
        AcquireRunCgroup(*scope->simulator, *state->pending_run,
                         binding->second.cgroup_identity);
    if (!run) {
      throw CgroupOwnershipMismatch(
          "pending run cgroup disappeared before recovery");
    }
    RemoveRunCgroup(
        &*run, std::chrono::steady_clock::now() + std::chrono::seconds(10), {});
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
  CgroupScopeLock scope_lock(config.root);
  if (!config.allow_root_process_move) {
    RequireNativeCgroupRoot(config.root);
  }
  const bool scope_state_exists = ScopeStateExists(config);
  std::optional<CgroupScopeState> state;
  std::optional<BoundCgroupScope> scope;
  std::optional<BoundCgroupDirectory> created_run;
  bool attempted_run = false;
  bool scope_published = false;
  std::optional<CgroupPathIdentity> created_run_identity;
  try {
    if (scope_state_exists) {
      state = LoadCgroupScopeState(config);
      scope.emplace(BindCgroupScope(config, scope_lock, &*state));
      RecoverInterruptedCgroupPreparation(config, &*scope, &*state);
      for (auto iterator = state->active_runs.begin();
           iterator != state->active_runs.end();) {
        const auto binding = state->run_bindings.find(*iterator);
        const std::optional<CgroupPathIdentity> expected_identity =
            binding == state->run_bindings.end()
                ? std::nullopt
                : std::optional<CgroupPathIdentity>(
                      binding->second.cgroup_identity);
        std::optional<BoundCgroupDirectory> existing =
            AcquireRunCgroup(*scope->simulator, *iterator, expected_identity);
        if (!existing) {
          state->run_bindings.erase(*iterator);
          iterator = state->active_runs.erase(iterator);
        } else {
          ++iterator;
        }
      }
      if (state->active_runs.empty()) {
        WriteCgroupScopeState(config.state_file, *state);
        RestoreCgroupScope(config, &*state, &*scope);
        state.reset();
        scope.reset();
      }
    }
    if (!state) {
      scope.emplace(BindNewCgroupScope(config, scope_lock));
      state = NewCgroupScopeState(config, *scope);
      if (state->active_runs.contains(run_id)) {
        throw std::runtime_error(
            "refusing to adopt pre-existing run cgroup: " +
            CgroupPathsForScope(config, run_id).run.string());
      }
      WriteCgroupScopeState(config.state_file, *state);
      scope_published = true;
      StartCgroupScope(config, &*state, &*scope);
    }
    if (state->active_runs.contains(run_id)) {
      throw std::runtime_error(
          "run cgroup is already prepared in this cgroup scope: " + run_id);
    }
    attempted_run = true;
    state->pending_run = run_id;
    state->pending_run_created = false;
    WriteCgroupScopeState(config.state_file, *state);

    if (!scope->simulator) {
      throw CgroupOwnershipMismatch(
          "simulator cgroup disappeared before run creation");
    }
    CreateCgroupDirectoryExclusiveAt(*scope->simulator, run_id, "run");
    created_run = AcquireRunCgroup(*scope->simulator, run_id, std::nullopt);
    if (!created_run) {
      throw CgroupOwnershipMismatch(
          "new run cgroup disappeared during acquisition");
    }
    created_run_identity = created_run->directory_identity().path;
    if (run_binding) {
      run_binding->cgroup_identity = *created_run_identity;
      state->run_bindings.emplace(run_id, *run_binding);
    }
    state->pending_run_created = true;
    WriteCgroupScopeState(config.state_file, *state);
    RequireControllersAvailableAt(*created_run,
                                  {"cpu", "io", "memory", "pids"});
    EnableControllersAt(*created_run);
    state->active_runs.insert(run_id);
    state->pending_run.reset();
    state->pending_run_created = false;
    WriteCgroupScopeState(config.state_file, *state);
  } catch (...) {
    const std::string original = CurrentExceptionText();
    std::optional<std::string> rollback_error;
    try {
      if (state && scope && (attempted_run || scope_published)) {
        if (created_run) {
          RemoveRunCgroup(
              &*created_run,
              std::chrono::steady_clock::now() + std::chrono::seconds(10), {});
          created_run.reset();
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
          RestoreCgroupScope(config, &*state, &*scope);
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
      throw CgroupOwnershipMismatch(
          "refusing to remove a run cgroup without scope ownership state: " +
          run_id);
    }
    return;
  }
  const auto verify_ownership = [&](std::string_view operation, auto&& action) {
    try {
      return action();
    } catch (const CgroupOwnershipMismatch&) {
      throw;
    } catch (...) {
      if (stop_token.stop_requested() ||
          std::chrono::steady_clock::now() >= deadline) {
        throw;
      }
      throw CgroupOwnershipMismatch(std::string(operation) + ": " +
                                    CurrentExceptionText());
    }
  };
  CgroupScopeState state = verify_ownership(
      "cgroup scope ownership state could not be verified",
      [&] { return LoadCgroupScopeState(config, deadline, stop_token); });
  BoundCgroupScope scope = verify_ownership(
      "cgroup scope hierarchy identity could not be verified", [&] {
        return BindCgroupScope(config, scope_lock, &state, deadline,
                               stop_token);
      });
  if (!state.active_runs.contains(run_id) && state.pending_run != run_id) {
    std::optional<BoundCgroupDirectory> unexpected_run;
    if (scope.simulator) {
      unexpected_run =
          verify_ownership("absent run cgroup path could not be verified", [&] {
            return AcquireRunCgroup(*scope.simulator, run_id, std::nullopt);
          });
    }
    if (unexpected_run || state.run_bindings.contains(run_id)) {
      throw CgroupOwnershipMismatch(
          "refusing to remove a run cgroup absent from scope ownership "
          "state: " +
          run_id);
    }
    if (expected_ownership != nullptr) {
      const RunOwnership loaded = verify_ownership(
          "absent cgroup run ownership could not be verified", [&] {
            return LoadRunOwnership(expected_ownership->run_id,
                                    expected_ownership->run_root, stop_token);
          });
      if (loaded != *expected_ownership ||
          expected_ownership->cgroup_name != run_id) {
        throw CgroupOwnershipMismatch(
            "stale cgroup ownership fields do not match the absent run");
      }
    }
    if (state.active_runs.empty() && !state.pending_run) {
      RestoreCgroupScope(config, &state, &scope, deadline, stop_token);
    }
    return;
  }
  const auto binding = state.run_bindings.find(run_id);
  if (expected_ownership != nullptr) {
    if (binding == state.run_bindings.end()) {
      throw CgroupOwnershipMismatch(
          "refusing stale cgroup cleanup for an unbound legacy scope entry");
    }
    const RunOwnership loaded = verify_ownership(
        "bound cgroup run ownership could not be verified", [&] {
          return LoadRunOwnership(expected_ownership->run_id,
                                  expected_ownership->run_root, stop_token);
        });
    const CgroupRunBinding expected{
        .run_id = expected_ownership->run_id,
        .run_root = expected_ownership->run_root,
        .resource_id = expected_ownership->resource_id,
        .run_root_identity = verify_ownership(
            "bound cgroup run root identity could not be verified",
            [&] {
              return DirectoryIdentity(expected_ownership->run_root,
                                       "run root");
            }),
        .cgroup_identity = binding->second.cgroup_identity,
    };
    if (loaded != *expected_ownership ||
        expected_ownership->cgroup_name != run_id ||
        binding->second != expected) {
      throw CgroupOwnershipMismatch(
          "stale cgroup scope binding does not match exact run ownership");
    }
  }
  const std::optional<CgroupPathIdentity> expected_cgroup_identity =
      binding == state.run_bindings.end()
          ? std::nullopt
          : std::optional<CgroupPathIdentity>(binding->second.cgroup_identity);
  std::optional<BoundCgroupDirectory> run =
      verify_ownership("bound run cgroup identity could not be verified", [&] {
        if (!scope.simulator) {
          throw CgroupOwnershipMismatch(
              "run cgroup simulator identity disappeared");
        }
        return AcquireRunCgroup(*scope.simulator, run_id,
                                expected_cgroup_identity);
      });
  if (run) {
    RemoveRunCgroup(&*run, deadline, stop_token);
  }
  if (state.pending_run == run_id) {
    state.pending_run.reset();
    state.pending_run_created = false;
  }
  state.active_runs.erase(run_id);
  state.run_bindings.erase(run_id);
  WriteCgroupScopeState(config.state_file, state);
  if (state.active_runs.empty() && !state.pending_run) {
    RestoreCgroupScope(config, &state, &scope, deadline, stop_token);
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
  struct stat status{};
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
  struct stat opened{};
  if (fstat(fd_, &opened) != 0) {
    throw std::runtime_error("inspect acquired cgroup fd failed for " +
                             path_.string() + ": " + std::strerror(errno));
  }
  if (!exact_identity(opened)) {
    throw std::runtime_error("acquired cgroup fd identity changed for " +
                             path_.string());
  }
  struct stat parent_entry{};
  if (fstatat(parent_fd_, name_.c_str(), &parent_entry, AT_SYMLINK_NOFOLLOW) !=
      0) {
    throw std::runtime_error("acquired cgroup path disappeared for " +
                             path_.string() + ": " + std::strerror(errno));
  }
  if (!exact_identity(parent_entry)) {
    throw std::runtime_error("refusing replaced cgroup directory identity: " +
                             path_.string());
  }
  struct stat path_entry{};
  if (fstatat(AT_FDCWD, path_.c_str(), &path_entry, AT_SYMLINK_NOFOLLOW) != 0) {
    throw std::runtime_error("acquired cgroup pathname disappeared for " +
                             path_.string() + ": " + std::strerror(errno));
  }
  if (!exact_identity(path_entry)) {
    throw std::runtime_error("refusing replaced cgroup pathname identity: " +
                             path_.string());
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
  try {
#ifdef BBP_ENABLE_TEST_HOOKS
    if (cgroup_create_after_directory_hook) {
      cgroup_create_after_directory_hook();
    }
#endif
    return Cgroup(node_root);
  } catch (...) {
    const std::exception_ptr failure = std::current_exception();
    const std::string failure_text = CurrentExceptionText();
    if (rmdir(node_root.c_str()) != 0) {
      throw std::runtime_error(
          failure_text +
          "; failed to clean acquired cgroup after construction failure: " +
          std::strerror(errno));
    }
    std::rethrow_exception(failure);
  }
}

std::shared_ptr<Cgroup> Cgroup::CreateShared(const std::string& run_id,
                                             const std::string& node_id) {
  Cgroup acquired = Create(run_id, node_id);
  try {
#ifdef BBP_ENABLE_TEST_HOOKS
    if (cgroup_create_shared_allocation_hook) {
      cgroup_create_shared_allocation_hook();
    }
#endif
    return std::make_shared<Cgroup>(std::move(acquired));
  } catch (...) {
    const std::exception_ptr failure = std::current_exception();
    const std::string failure_text = CurrentExceptionText();
    try {
      acquired.Remove(std::chrono::steady_clock::now() +
                      std::chrono::seconds(10));
    } catch (...) {
      throw std::runtime_error(
          failure_text +
          "; failed to clean acquired cgroup after shared ownership failure: " +
          CurrentExceptionText());
    }
    std::rethrow_exception(failure);
  }
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
  RemoveStaleRun(ownership,
                 std::chrono::steady_clock::now() + std::chrono::seconds(10),
                 {});
}

void Cgroup::RemoveStaleRun(const RunOwnership& ownership,
                            std::chrono::steady_clock::time_point deadline,
                            std::stop_token stop_token) {
  RunOwnership loaded;
  try {
    loaded = LoadRunOwnership(ownership.run_id, ownership.run_root, stop_token);
  } catch (...) {
    if (stop_token.stop_requested() ||
        std::chrono::steady_clock::now() >= deadline) {
      throw;
    }
    throw CgroupOwnershipMismatch(
        "stale cgroup run ownership could not be verified: " +
        CurrentExceptionText());
  }
  if (loaded != ownership) {
    throw CgroupOwnershipMismatch("stale cgroup ownership fields do not match");
  }
  RemoveRunInScope(ProductionCgroupScopeConfig(), ownership.cgroup_name,
                   &ownership, deadline, stop_token);
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
  RemoveCgroupRunInTestScope(
      config, run_id,
      std::chrono::steady_clock::now() + std::chrono::seconds(10), {});
}

void RemoveCgroupRunInTestScope(const CgroupScopeTestConfig& config,
                                const std::string& run_id,
                                std::chrono::steady_clock::time_point deadline,
                                std::stop_token stop_token) {
  RemoveRunInScope(
      CgroupScopeConfig{
          .root = config.root,
          .simulator_name = config.simulator_name,
          .state_file = config.state_file,
          .allow_root_process_move = config.allow_root_process_move,
      },
      run_id, nullptr, deadline, stop_token);
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
  const std::filesystem::path absolute =
      std::filesystem::absolute(path).lexically_normal();
  const std::filesystem::path parent = absolute.parent_path();
  const int parent_descriptor =
      open(parent.c_str(), O_RDONLY | O_DIRECTORY | O_CLOEXEC | O_NOFOLLOW);
  if (parent_descriptor < 0) {
    throw std::runtime_error("open test cgroup parent failed for " +
                             absolute.string() + ": " + std::strerror(errno));
  }
  std::optional<BoundCgroupDirectory> cgroup;
  try {
    cgroup = BoundCgroupDirectory::TryOpen(
        parent_descriptor, absolute.filename().string(), absolute);
  } catch (...) {
    static_cast<void>(close(parent_descriptor));
    throw;
  }
  if (close(parent_descriptor) != 0) {
    throw std::runtime_error("close test cgroup parent failed for " +
                             absolute.string() + ": " + std::strerror(errno));
  }
  if (!cgroup) {
    throw std::runtime_error("test cgroup disappeared before acquisition: " +
                             absolute.string());
  }
  KillBoundCgroupProcesses(*cgroup, deadline, stop_token, true);
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
  const bool empty = CgroupProcsEmpty(bound) &&
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
    struct stat replacement{};
    if (fstatat(parent_fd_, name_.c_str(), &replacement, AT_SYMLINK_NOFOLLOW) ==
        0) {
      throw std::runtime_error("refusing replacement at removed cgroup path: " +
                               path_.string());
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
  struct stat remaining{};
  if (fstatat(parent_fd_, name_.c_str(), &remaining, AT_SYMLINK_NOFOLLOW) ==
      0) {
    throw std::runtime_error("replacement appeared at removed cgroup path: " +
                             path_.string());
  }
  if (errno != ENOENT) {
    throw std::runtime_error("verify removed cgroup path failed for " +
                             path_.string() + ": " + std::strerror(errno));
  }
}

#ifdef BBP_ENABLE_TEST_HOOKS
void SetCgroupCreateAfterDirectoryHookForTest(std::function<void()> hook) {
  cgroup_create_after_directory_hook = std::move(hook);
}

void SetCgroupCreateSharedAllocationHookForTest(std::function<void()> hook) {
  cgroup_create_shared_allocation_hook = std::move(hook);
}

void SetCgroupRemovalIdentityHookForTest(
    std::function<void(CgroupRemovalTestPhase, const std::filesystem::path&)>
        hook) {
  cgroup_removal_identity_hook = std::move(hook);
}
#endif

}  // namespace bbp
