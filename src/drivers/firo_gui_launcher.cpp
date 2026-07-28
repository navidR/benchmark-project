#include "bbp/drivers/firo_gui_launcher.h"

#include <arpa/inet.h>
#include <fcntl.h>
#include <linux/memfd.h>
#include <sys/stat.h>
#include <sys/syscall.h>
#include <unistd.h>

#include <algorithm>
#include <array>
#include <boost/json/value.hpp>
#include <cerrno>
#include <chrono>
#include <exception>
#include <limits>
#include <memory>
#include <stdexcept>
#include <system_error>
#include <utility>
#include <vector>

#include "bbp/drivers/firo_driver.h"
#include "bbp/simulation_cancelled.h"

namespace bbp {
namespace {

constexpr std::string_view kFiroQtLauncherDescriptorName =
    "bbp-firo-qt-launcher";
constexpr std::size_t kMaximumFiroQtLauncherCommandBytes = 1024U * 1024U;
constexpr int kRequiredFiroQtLauncherSeals =
    F_SEAL_SEAL | F_SEAL_SHRINK | F_SEAL_GROW | F_SEAL_WRITE;
#ifdef MFD_EXEC
constexpr unsigned int kFiroQtLauncherMemfdExecFlag = MFD_EXEC;
#else
// Linux UAPI value since 6.3. An older running kernel rejects it with EINVAL.
constexpr unsigned int kFiroQtLauncherMemfdExecFlag = 0x0010U;
#endif
#ifdef F_SEAL_EXEC
constexpr int kFiroQtLauncherExecSeal = F_SEAL_EXEC;
#else
// Linux UAPI value since 6.3. An older running kernel rejects it with EINVAL.
constexpr int kFiroQtLauncherExecSeal = 0x0020;
#endif

#ifdef BBP_ENABLE_TEST_HOOKS
FiroQtLauncherCleanupTestHook firo_qt_launcher_cleanup_test_hook;
#endif

class ScopedDescriptor {
 public:
  explicit ScopedDescriptor(int descriptor) : descriptor_(descriptor) {}
  ~ScopedDescriptor() {
    if (descriptor_ >= 0) {
      static_cast<void>(close(descriptor_));
    }
  }

  ScopedDescriptor(const ScopedDescriptor&) = delete;
  ScopedDescriptor& operator=(const ScopedDescriptor&) = delete;

  [[nodiscard]] int get() const { return descriptor_; }
  [[nodiscard]] int release() { return std::exchange(descriptor_, -1); }

 private:
  int descriptor_ = -1;
};

[[noreturn]] void ThrowSystemError(std::string_view operation);

std::string RequiredLauncherString(const boost::json::object& object,
                                   std::string_view field) {
  const boost::json::value* value = object.if_contains(field);
  if (value == nullptr || !value->is_string() || value->as_string().empty()) {
    throw std::runtime_error("the Firo-Qt connection command has no " +
                             std::string(field));
  }
  const bool has_control_character = std::any_of(
      value->as_string().begin(), value->as_string().end(), [](char character) {
        const auto byte = static_cast<unsigned char>(character);
        return byte < 0x20U || byte == 0x7fU;
      });
  if (value->as_string().size() > kMaximumFiroQtLauncherCommandBytes ||
      has_control_character) {
    throw std::runtime_error("the Firo-Qt connection command has an invalid " +
                             std::string(field));
  }
  return std::string(value->as_string());
}

bool IsSafeLauncherNodeId(std::string_view node_id) {
  if (node_id.empty() || node_id.size() > 32U) {
    return false;
  }
  return std::all_of(node_id.begin(), node_id.end(), [](char character) {
    return (character >= 'a' && character <= 'z') ||
           (character >= 'A' && character <= 'Z') ||
           (character >= '0' && character <= '9') || character == '-' ||
           character == '_';
  });
}

void RequireLauncherTrue(const boost::json::object& object,
                         std::string_view field) {
  const boost::json::value* value = object.if_contains(field);
  if (value == nullptr || !value->is_bool() || !value->as_bool()) {
    throw std::runtime_error("the Firo-Qt connection command has invalid " +
                             std::string(field));
  }
}

std::uint16_t RequiredLauncherPort(const boost::json::object& object) {
  const boost::json::value* value = object.if_contains("peer_port");
  std::uint64_t port = 0U;
  if (value != nullptr && value->is_uint64()) {
    port = value->as_uint64();
  } else if (value != nullptr && value->is_int64() && value->as_int64() > 0) {
    port = static_cast<std::uint64_t>(value->as_int64());
  } else {
    throw std::runtime_error(
        "the Firo-Qt connection command has invalid peer_port");
  }
  if (port == 0U || port > std::numeric_limits<std::uint16_t>::max()) {
    throw std::runtime_error(
        "the Firo-Qt connection command has invalid peer_port");
  }
  return static_cast<std::uint16_t>(port);
}

void RequireLauncherStringArray(const boost::json::object& object,
                                std::string_view field,
                                const std::vector<std::string>& expected) {
  const boost::json::value* value = object.if_contains(field);
  if (value == nullptr || !value->is_array() ||
      value->as_array().size() != expected.size()) {
    throw std::runtime_error("the Firo-Qt connection command has invalid " +
                             std::string(field));
  }
  for (std::size_t index = 0U; index < expected.size(); ++index) {
    const boost::json::value& item = value->as_array()[index];
    if (!item.is_string() || item.as_string() != expected[index]) {
      throw std::runtime_error("the Firo-Qt connection command has invalid " +
                               std::string(field));
    }
  }
}

std::string ExceptionText(const std::exception_ptr& failure) {
  try {
    std::rethrow_exception(failure);
  } catch (const std::exception& error) {
    return error.what();
  } catch (...) {
    return "non-standard exception";
  }
}

void RetainFiroQtLauncherCleanupUncertainty(bool* cleanup_unverified,
                                            std::string* retained,
                                            std::string_view detail) noexcept {
  if (cleanup_unverified == nullptr || retained == nullptr) {
    return;
  }
  *cleanup_unverified = true;
  if (detail.empty()) {
    detail = "unspecified Firo-Qt launcher cleanup uncertainty";
  }
  try {
    if (!retained->empty()) {
      *retained += "; ";
    }
    retained->append(detail);
  } catch (...) {
  }
}

[[noreturn]] void RethrowAfterCandidateCleanup(
    OwnedFiroQtLauncher* candidate, const std::exception_ptr& failure,
    std::optional<OwnedFiroQtLauncher>* pending_cleanup,
    bool* cleanup_unverified, std::string* unverified_cleanup_failure) {
  const auto cleanup_is_unverified = [&] {
    return cleanup_unverified != nullptr && *cleanup_unverified;
  };
  const auto retain_candidate_after_failure = [&] {
    if (candidate->active()) {
      if (pending_cleanup != nullptr && !pending_cleanup->has_value()) {
        pending_cleanup->emplace(std::move(*candidate));
        return;
      }
      RetainFiroQtLauncherCleanupUncertainty(
          cleanup_unverified, unverified_cleanup_failure,
          "candidate cleanup could not retain its retryable ownership");
      return;
    }
    RetainFiroQtLauncherCleanupUncertainty(
        cleanup_unverified, unverified_cleanup_failure,
        "candidate cleanup lost its retryable ownership after an exception");
  };
  if (candidate == nullptr || !candidate->active()) {
    if (cleanup_is_unverified()) {
      throw FiroQtLauncherCleanupUnverified(
          "Firo-Qt launcher replacement failed: " + ExceptionText(failure) +
          "; cleanup could not be verified: " + *unverified_cleanup_failure);
    }
    std::rethrow_exception(failure);
  }
  FiroQtLauncherCleanupResult cleanup;
  try {
    cleanup = candidate->Cleanup();
  } catch (const std::exception& error) {
    retain_candidate_after_failure();
    const std::string_view cleanup_failure =
        *error.what() == '\0' ? "cleanup threw without diagnostics"
                              : error.what();
    const std::string message =
        "Firo-Qt launcher replacement failed: " + ExceptionText(failure) +
        "; verified candidate cleanup also failed: " +
        std::string(cleanup_failure);
    if (cleanup_is_unverified()) {
      throw FiroQtLauncherCleanupUnverified(
          message +
          "; cleanup could not be verified: " + *unverified_cleanup_failure);
    }
    throw std::runtime_error(message);
  } catch (...) {
    retain_candidate_after_failure();
    if (cleanup_is_unverified()) {
      throw FiroQtLauncherCleanupUnverified(
          "Firo-Qt launcher replacement and candidate cleanup failed; cleanup "
          "could not be verified: " +
          *unverified_cleanup_failure);
    }
    throw std::runtime_error(
        "Firo-Qt launcher replacement failed and candidate cleanup threw a "
        "non-standard exception");
  }
  if (cleanup == FiroQtLauncherCleanupResult::kOwnershipChanged) {
    RetainFiroQtLauncherCleanupUncertainty(
        cleanup_unverified, unverified_cleanup_failure,
        "candidate launcher ownership changed");
  }
  if (cleanup_is_unverified()) {
    throw FiroQtLauncherCleanupUnverified(
        "Firo-Qt launcher replacement failed: " + ExceptionText(failure) +
        "; cleanup could not be verified: " + *unverified_cleanup_failure);
  }
  std::rethrow_exception(failure);
}

FiroQtLauncherCleanupResult CleanupPendingCandidate(
    std::optional<OwnedFiroQtLauncher>* pending_cleanup,
    std::optional<std::chrono::steady_clock::time_point> deadline =
        std::nullopt,
    std::stop_token stop_token = {}) {
  if (pending_cleanup == nullptr || !*pending_cleanup) {
    return FiroQtLauncherCleanupResult::kAlreadyAbsent;
  }
  FiroQtLauncherCleanupResult cleanup;
  try {
    cleanup = deadline ? (*pending_cleanup)->Cleanup(*deadline, stop_token)
              : stop_token.stop_possible()
                  ? (*pending_cleanup)->Cleanup(stop_token)
                  : (*pending_cleanup)->Cleanup();
  } catch (...) {
    if (!(*pending_cleanup)->active()) {
      pending_cleanup->reset();
    }
    throw;
  }
  pending_cleanup->reset();
  return cleanup;
}

FiroQtLauncherCleanupResult MergeCleanupResults(
    FiroQtLauncherCleanupResult first, FiroQtLauncherCleanupResult second) {
  if (first == FiroQtLauncherCleanupResult::kOwnershipChanged ||
      second == FiroQtLauncherCleanupResult::kOwnershipChanged) {
    return FiroQtLauncherCleanupResult::kOwnershipChanged;
  }
  if (first == FiroQtLauncherCleanupResult::kRemoved ||
      second == FiroQtLauncherCleanupResult::kRemoved) {
    return FiroQtLauncherCleanupResult::kRemoved;
  }
  return FiroQtLauncherCleanupResult::kAlreadyAbsent;
}

struct ParsedFiroQtLauncherCommand {
  std::string node_id;
  OperatorConnectionCommand command;
  std::string shell_command;
};

[[noreturn]] void ThrowSystemError(std::string_view operation) {
  const int error = errno;
  throw std::system_error(error, std::generic_category(),
                          std::string(operation));
}

void WriteAll(int descriptor, std::string_view content) {
  std::size_t offset = 0U;
  while (offset < content.size()) {
    const ssize_t count =
        write(descriptor, content.data() + offset, content.size() - offset);
    if (count > 0) {
      offset += static_cast<std::size_t>(count);
      continue;
    }
    if (count < 0 && errno == EINTR) {
      continue;
    }
    ThrowSystemError("write Firo-Qt launcher");
  }
}

bool DescriptorHasExactContent(int descriptor, std::string_view expected) {
  std::array<char, 4096U> buffer{};
  std::size_t offset = 0U;
  while (offset < expected.size()) {
    const std::size_t requested =
        std::min(buffer.size(), expected.size() - offset);
    const ssize_t count = read(descriptor, buffer.data(), requested);
    if (count < 0) {
      if (errno == EINTR) {
        continue;
      }
      ThrowSystemError("read sealed Firo-Qt launcher");
    }
    if (count == 0) {
      return false;
    }
    const std::size_t bytes = static_cast<std::size_t>(count);
    if (!std::equal(buffer.data(), buffer.data() + bytes,
                    expected.data() + offset)) {
      return false;
    }
    offset += bytes;
  }

  char extra = '\0';
  ssize_t count = 0;
  do {
    count = read(descriptor, &extra, 1U);
  } while (count < 0 && errno == EINTR);
  if (count < 0) {
    ThrowSystemError("read sealed Firo-Qt launcher end");
  }
  return count == 0;
}

ParsedFiroQtLauncherCommand FiroQtLauncherCommandFromReport(
    const boost::json::object& report,
    std::optional<std::string_view> required_node_id) {
  const boost::json::value* chain = report.if_contains("chain");
  if (chain == nullptr || !chain->is_string() || chain->as_string() != "firo") {
    throw std::runtime_error(
        "the running benchmark is not a compatible Firo run");
  }
  const boost::json::value* connection =
      report.if_contains("operator_connection_command");
  if (connection == nullptr || !connection->is_object()) {
    throw std::runtime_error(
        "the running benchmark has no Firo-Qt connection command");
  }
  const boost::json::object& object = connection->as_object();
  constexpr std::array<std::string_view, 15U> kAllowedFields{
      "arguments",
      "argv",
      "command",
      "data_dir",
      "discovery_disabled",
      "executable",
      "kind",
      "manual_launch",
      "network",
      "node_id",
      "peer_address",
      "peer_endpoint",
      "peer_port",
      "timestamp",
      "wallet_enabled",
  };
  for (const auto& member : object) {
    const std::string_view field(member.key().data(), member.key().size());
    if (std::find(kAllowedFields.begin(), kAllowedFields.end(), field) ==
        kAllowedFields.end()) {
      throw std::runtime_error(
          "the Firo-Qt connection command has unsupported field: " +
          std::string(field));
    }
  }
  const std::string resolved_node_id =
      RequiredLauncherString(object, "node_id");
  if (!IsSafeLauncherNodeId(resolved_node_id)) {
    throw std::runtime_error(
        "the Firo-Qt connection command has an invalid node_id");
  }
  if (required_node_id && resolved_node_id != *required_node_id) {
    throw std::runtime_error(
        "the requested node does not own the Firo-Qt connection command");
  }
  static_cast<void>(RequiredLauncherString(object, "timestamp"));

  if (RequiredLauncherString(object, "kind") != "manual_firo_gui" ||
      RequiredLauncherString(object, "network") != "regtest") {
    throw std::runtime_error(
        "the running benchmark has an incompatible Firo-Qt connection "
        "command");
  }
  RequireLauncherTrue(object, "manual_launch");
  RequireLauncherTrue(object, "discovery_disabled");
  RequireLauncherTrue(object, "wallet_enabled");

  const std::string executable = RequiredLauncherString(object, "executable");
  const std::string data_dir = RequiredLauncherString(object, "data_dir");
  const std::string peer_address =
      RequiredLauncherString(object, "peer_address");
  const std::uint16_t peer_port = RequiredLauncherPort(object);
  const std::filesystem::path executable_path(executable);
  const std::filesystem::path data_dir_path(data_dir);
  if (!executable_path.is_absolute() ||
      executable_path.filename() != "firo-qt" ||
      executable_path.lexically_normal() != executable_path ||
      !data_dir_path.is_absolute() ||
      data_dir_path.lexically_normal() != data_dir_path) {
    throw std::runtime_error(
        "the Firo-Qt connection command has invalid owned paths");
  }
  struct in_addr parsed_address{};
  if (inet_pton(AF_INET, peer_address.c_str(), &parsed_address) != 1) {
    throw std::runtime_error(
        "the Firo-Qt connection command has invalid peer_address");
  }
  const std::uint32_t host_address = ntohl(parsed_address.s_addr);
  if (host_address == 0U || (host_address & 0xf0000000U) == 0xe0000000U) {
    throw std::runtime_error(
        "the Firo-Qt connection command has invalid peer_address");
  }

  const std::string peer_endpoint =
      peer_address + ":" + std::to_string(peer_port);
  if (RequiredLauncherString(object, "peer_endpoint") != peer_endpoint) {
    throw std::runtime_error(
        "the Firo-Qt connection command has inconsistent peer_endpoint");
  }
  const std::vector<std::string> arguments{
      "-regtest",
      "-datadir=" + data_dir,
      "-connect=" + peer_endpoint,
      "-dns=0",
      "-dnsseed=0",
      "-forcednsseed=0",
      "-maxconnections=1",
      "-listen=0",
      "-discover=0",
      "-listenonion=0",
      "-torsetup=0",
      "-upnp=0",
  };
  RequireLauncherStringArray(object, "arguments", arguments);
  std::vector<std::string> argv;
  argv.reserve(arguments.size() + 1U);
  argv.push_back(executable);
  argv.insert(argv.end(), arguments.begin(), arguments.end());
  RequireLauncherStringArray(object, "argv", argv);

  OperatorConnectionCommand typed_command{
      .executable = executable_path,
      .arguments = arguments,
      .data_dir = data_dir_path,
      .peer_address = peer_address,
      .peer_port = peer_port,
  };
  const std::string command = typed_command.ShellCommand();
  if (command.size() > kMaximumFiroQtLauncherCommandBytes ||
      RequiredLauncherString(object, "command") != command) {
    throw std::runtime_error(
        "the Firo-Qt connection command is inconsistent or too large");
  }
  return ParsedFiroQtLauncherCommand{
      .node_id = resolved_node_id,
      .command = std::move(typed_command),
      .shell_command = command,
  };
}

void ThrowIfCancelled(std::stop_token stop_token) {
  if (stop_token.stop_requested()) {
    throw SimulationCancelled();
  }
}

void RequireLauncherCleanupActive(
    std::optional<std::chrono::steady_clock::time_point> deadline,
    std::stop_token stop_token) {
  ThrowIfCancelled(stop_token);
  if (deadline && std::chrono::steady_clock::now() >= *deadline) {
    throw std::runtime_error("Firo-Qt launcher cleanup deadline expired");
  }
}

bool SameOperatorConnectionCommand(const OperatorConnectionCommand& left,
                                   const OperatorConnectionCommand& right) {
  return left.executable == right.executable &&
         left.arguments == right.arguments && left.data_dir == right.data_dir &&
         left.peer_address == right.peer_address &&
         left.peer_port == right.peer_port;
}

bool SameLauncherAuthority(const FiroQtLauncherAuthority& left,
                           const FiroQtLauncherAuthority& right) {
  return left.inventory_generation == right.inventory_generation &&
         left.node_id == right.node_id &&
         SameOperatorConnectionCommand(left.command, right.command);
}

FiroQtLauncherAuthority ResolveLauncherAuthority(
    const FiroQtLauncherAuthorityResolver& resolver,
    const ParsedFiroQtLauncherCommand& parsed, std::stop_token stop_token) {
  ThrowIfCancelled(stop_token);
  if (!resolver) {
    throw std::runtime_error(
        "the Firo-Qt launcher has no live authoritative command resolver");
  }
  FiroQtLauncherAuthority authority = resolver(parsed.node_id, stop_token);
  ThrowIfCancelled(stop_token);
  if (authority.inventory_generation == 0U ||
      authority.node_id != parsed.node_id ||
      !IsSafeLauncherNodeId(authority.node_id) ||
      !SameOperatorConnectionCommand(authority.command, parsed.command) ||
      authority.command.ShellCommand() != parsed.shell_command) {
    throw std::runtime_error(
        "the reported Firo-Qt connection command does not match the live "
        "authoritative node");
  }
  return authority;
}

}  // namespace

OwnedFiroQtLauncher::OwnedFiroQtLauncher(std::filesystem::path path,
                                         std::uintmax_t device,
                                         std::uintmax_t inode, int descriptor,
                                         int seals) noexcept
    : path_(std::move(path)),
      device_(device),
      inode_(inode),
      descriptor_(descriptor),
      seals_(seals),
      active_(true) {}

OwnedFiroQtLauncher::OwnedFiroQtLauncher(OwnedFiroQtLauncher&& other) noexcept
    : path_(std::move(other.path_)),
      device_(other.device_),
      inode_(other.inode_),
      descriptor_(std::exchange(other.descriptor_, -1)),
      seals_(other.seals_),
      active_(std::exchange(other.active_, false)) {
  other.device_ = 0U;
  other.inode_ = 0U;
  other.seals_ = 0;
  other.path_.clear();
}

OwnedFiroQtLauncher& OwnedFiroQtLauncher::operator=(
    OwnedFiroQtLauncher&& other) {
  if (this == &other) {
    return *this;
  }
  static_cast<void>(Cleanup());
  path_ = std::move(other.path_);
  device_ = other.device_;
  inode_ = other.inode_;
  descriptor_ = std::exchange(other.descriptor_, -1);
  seals_ = other.seals_;
  active_ = std::exchange(other.active_, false);
  other.device_ = 0U;
  other.inode_ = 0U;
  other.seals_ = 0;
  other.path_.clear();
  return *this;
}

OwnedFiroQtLauncher::~OwnedFiroQtLauncher() { CleanupNoThrow(); }

OwnedFiroQtLauncher OwnedFiroQtLauncher::Create(
    std::string_view shell_command) {
  if (shell_command.empty()) {
    throw std::invalid_argument("Firo-Qt shell command is empty");
  }
  if (shell_command.find('\0') != std::string_view::npos) {
    throw std::invalid_argument("Firo-Qt shell command contains NUL");
  }

  constexpr unsigned int kMemfdFlags =
      MFD_CLOEXEC | MFD_ALLOW_SEALING | kFiroQtLauncherMemfdExecFlag;
  int writable_descriptor = static_cast<int>(syscall(
      SYS_memfd_create, kFiroQtLauncherDescriptorName.data(), kMemfdFlags));
  if (writable_descriptor < 0 && errno == EINVAL) {
    // MFD_EXEC was added after memfd_create. Kernels predating it create
    // executable memfds by default.
    writable_descriptor = static_cast<int>(
        syscall(SYS_memfd_create, kFiroQtLauncherDescriptorName.data(),
                MFD_CLOEXEC | MFD_ALLOW_SEALING));
  }
  if (writable_descriptor < 0) {
    ThrowSystemError("create anonymous Firo-Qt launcher");
  }
  ScopedDescriptor writable(writable_descriptor);

  int descriptor_flags = 0;
  do {
    descriptor_flags = fcntl(writable.get(), F_GETFD);
  } while (descriptor_flags < 0 && errno == EINTR);
  if (descriptor_flags < 0) {
    ThrowSystemError("protect anonymous Firo-Qt launcher descriptor");
  }
  if ((descriptor_flags & FD_CLOEXEC) == 0) {
    throw std::runtime_error(
        "anonymous Firo-Qt launcher descriptor is not close-on-exec");
  }
  const std::string content =
      "#!/bin/bash\nexec " + std::string(shell_command) + "\n";
  WriteAll(writable.get(), content);
  int sync_result = 0;
  do {
    sync_result = fsync(writable.get());
  } while (sync_result != 0 && errno == EINTR);
  if (sync_result != 0) {
    ThrowSystemError("sync anonymous Firo-Qt launcher");
  }
  if (fchmod(writable.get(), S_IRWXU) != 0) {
    ThrowSystemError("set anonymous Firo-Qt launcher permissions");
  }

  int requested_seals = kRequiredFiroQtLauncherSeals | kFiroQtLauncherExecSeal;
  if (fcntl(writable.get(), F_ADD_SEALS, requested_seals) != 0) {
    if (errno != EINVAL ||
        fcntl(writable.get(), F_ADD_SEALS, kRequiredFiroQtLauncherSeals) != 0) {
      ThrowSystemError("seal anonymous Firo-Qt launcher");
    }
    requested_seals = kRequiredFiroQtLauncherSeals;
  }
  const std::string local_descriptor_path =
      "/proc/self/fd/" + std::to_string(writable.get());
  const int read_only_descriptor =
      open(local_descriptor_path.c_str(), O_RDONLY | O_CLOEXEC);
  if (read_only_descriptor < 0) {
    ThrowSystemError("reopen anonymous Firo-Qt launcher read-only");
  }
  ScopedDescriptor read_only(read_only_descriptor);

  struct stat created_status{};
  if (fstat(read_only.get(), &created_status) != 0) {
    ThrowSystemError("inspect anonymous Firo-Qt launcher");
  }
  int applied_seals = 0;
  do {
    applied_seals = fcntl(read_only.get(), F_GET_SEALS);
  } while (applied_seals < 0 && errno == EINTR);
  if (applied_seals < 0) {
    ThrowSystemError("inspect anonymous Firo-Qt launcher seals");
  }
  if (!S_ISREG(created_status.st_mode) ||
      (created_status.st_mode & 07777) != S_IRWXU ||
      (applied_seals & requested_seals) != requested_seals ||
      !DescriptorHasExactContent(read_only.get(), content)) {
    throw std::runtime_error(
        "anonymous Firo-Qt launcher identity, mode, seals, or content are "
        "invalid");
  }
  const int read_only_flags = fcntl(read_only.get(), F_GETFL);
  const int read_only_descriptor_flags = fcntl(read_only.get(), F_GETFD);
  if (read_only_flags < 0 || (read_only_flags & O_ACCMODE) != O_RDONLY ||
      read_only_descriptor_flags < 0 ||
      (read_only_descriptor_flags & FD_CLOEXEC) == 0) {
    throw std::runtime_error(
        "anonymous Firo-Qt launcher read descriptor is not protected");
  }

  std::filesystem::path path = std::filesystem::path("/proc") /
                               std::to_string(getpid()) / "fd" /
                               std::to_string(read_only.get());
  struct stat published_status{};
  if (stat(path.c_str(), &published_status) != 0 ||
      published_status.st_dev != created_status.st_dev ||
      published_status.st_ino != created_status.st_ino) {
    throw std::runtime_error(
        "anonymous Firo-Qt launcher proc descriptor is unavailable");
  }

  OwnedFiroQtLauncher launcher(
      std::move(path), static_cast<std::uintmax_t>(created_status.st_dev),
      static_cast<std::uintmax_t>(created_status.st_ino), read_only.get(),
      applied_seals);
  static_cast<void>(read_only.release());
  return launcher;
}

FiroQtLauncherCleanupResult OwnedFiroQtLauncher::Cleanup() {
  return CleanupImpl(std::nullopt, {});
}

FiroQtLauncherCleanupResult OwnedFiroQtLauncher::Cleanup(
    std::stop_token stop_token) {
  return CleanupImpl(std::nullopt, stop_token);
}

FiroQtLauncherCleanupResult OwnedFiroQtLauncher::Cleanup(
    std::chrono::steady_clock::time_point deadline,
    std::stop_token stop_token) {
  return CleanupImpl(deadline, stop_token);
}

FiroQtLauncherCleanupResult OwnedFiroQtLauncher::CleanupImpl(
    std::optional<std::chrono::steady_clock::time_point> deadline,
    std::stop_token stop_token) {
  if (!active_) {
    return FiroQtLauncherCleanupResult::kAlreadyAbsent;
  }
  RequireLauncherCleanupActive(deadline, stop_token);

  const auto inspect_descriptor = [&] {
    struct stat descriptor_status{};
    int inspect_result = 0;
    do {
      inspect_result = fstat(descriptor_, &descriptor_status);
    } while (inspect_result != 0 && errno == EINTR);
    if (inspect_result != 0) {
      const int descriptor_error = errno;
      descriptor_ = -1;
      ResetOwnership();
      throw std::system_error(descriptor_error, std::generic_category(),
                              "inspect anonymous Firo-Qt launcher descriptor");
    }
    if (!S_ISREG(descriptor_status.st_mode) ||
        static_cast<std::uintmax_t>(descriptor_status.st_dev) != device_ ||
        static_cast<std::uintmax_t>(descriptor_status.st_ino) != inode_) {
      descriptor_ = -1;
      ResetOwnership();
      return false;
    }
    int current_seals = 0;
    do {
      current_seals = fcntl(descriptor_, F_GET_SEALS);
    } while (current_seals < 0 && errno == EINTR);
    if (current_seals < 0) {
      const int seal_error = errno;
      descriptor_ = -1;
      ResetOwnership();
      throw std::system_error(seal_error, std::generic_category(),
                              "inspect anonymous Firo-Qt launcher seals");
    }
    if (current_seals != seals_ ||
        (current_seals & kRequiredFiroQtLauncherSeals) !=
            kRequiredFiroQtLauncherSeals) {
      descriptor_ = -1;
      ResetOwnership();
      return false;
    }
    return true;
  };
  if (!inspect_descriptor()) {
    return FiroQtLauncherCleanupResult::kOwnershipChanged;
  }

#ifdef BBP_ENABLE_TEST_HOOKS
  if (firo_qt_launcher_cleanup_test_hook) {
    firo_qt_launcher_cleanup_test_hook(
        FiroQtLauncherCleanupTestPhase::kBeforeDescriptorRelease, path_,
        std::nullopt);
  }
#endif

  RequireLauncherCleanupActive(deadline, stop_token);
  if (!inspect_descriptor()) {
    return FiroQtLauncherCleanupResult::kOwnershipChanged;
  }

  // A memfd has no directory entry. Releasing BBP's exact descriptor removes
  // its procfs publication without selecting or deleting a mutable pathname.
  // Other processes may retain descriptors they opened while it was active;
  // cleanup does not claim to revoke those already-open references.
  ResetOwnership();
  return FiroQtLauncherCleanupResult::kRemoved;
}

void OwnedFiroQtLauncher::CleanupNoThrow() noexcept {
  try {
    static_cast<void>(Cleanup());
  } catch (...) {
  }
}

void OwnedFiroQtLauncher::ResetOwnership() noexcept {
  if (descriptor_ >= 0) {
    static_cast<void>(close(descriptor_));
  }
  path_.clear();
  device_ = 0U;
  inode_ = 0U;
  descriptor_ = -1;
  seals_ = 0;
  active_ = false;
}

FiroQtLauncherService::FiroQtLauncherService(
    FiroQtLauncherAuthorityResolver resolver)
    : authority_resolver_(std::move(resolver)) {
  if (!authority_resolver_) {
    throw std::invalid_argument("Firo-Qt launcher authority resolver is empty");
  }
}

FiroQtLauncherService::~FiroQtLauncherService() { CloseAndCleanupNoThrow(); }

std::unique_lock<std::timed_mutex> FiroQtLauncherService::Acquire(
    std::optional<std::chrono::steady_clock::time_point> deadline,
    std::stop_token stop_token) const {
  std::unique_lock<std::timed_mutex> lock(mutation_mutex_, std::defer_lock);
  constexpr auto kLockPollInterval = std::chrono::milliseconds(10);
  while (!lock.try_lock_until(
      deadline ? std::min(*deadline,
                          std::chrono::steady_clock::now() + kLockPollInterval)
               : std::chrono::steady_clock::now() + kLockPollInterval)) {
    ThrowIfCancelled(stop_token);
    if (deadline && std::chrono::steady_clock::now() >= *deadline) {
      throw std::runtime_error(
          "Firo-Qt launcher cleanup deadline expired while waiting for "
          "exclusive access");
    }
  }
  ThrowIfCancelled(stop_token);
  if (deadline && std::chrono::steady_clock::now() >= *deadline) {
    throw std::runtime_error(
        "Firo-Qt launcher cleanup deadline expired after acquiring "
        "exclusive access");
  }
  return lock;
}

FiroQtLauncherSnapshot FiroQtLauncherService::ReplaceFromReport(
    const boost::json::object& report,
    std::optional<std::string_view> required_node_id,
    std::stop_token stop_token) {
  const ParsedFiroQtLauncherCommand parsed =
      FiroQtLauncherCommandFromReport(report, required_node_id);
  ThrowIfCancelled(stop_token);
  std::unique_lock<std::timed_mutex> lock = Acquire(std::nullopt, stop_token);
  if (closed_) {
    throw std::runtime_error("the Firo-Qt launcher service is closed");
  }
  FiroQtLauncherCleanupResult pending_cleanup =
      FiroQtLauncherCleanupResult::kAlreadyAbsent;
  try {
    pending_cleanup = CleanupPendingCandidate(&pending_cleanup_);
  } catch (...) {
    const std::exception_ptr cleanup_failure = std::current_exception();
    if (!pending_cleanup_) {
      RetainFiroQtLauncherCleanupUncertainty(
          &cleanup_unverified_, &unverified_cleanup_failure_,
          "pending candidate cleanup lost its retryable ownership after an "
          "exception");
      throw FiroQtLauncherCleanupUnverified(
          "pending Firo-Qt launcher cleanup could not be verified: " +
          unverified_cleanup_failure_);
    }
    std::rethrow_exception(cleanup_failure);
  }
  if (pending_cleanup == FiroQtLauncherCleanupResult::kOwnershipChanged) {
    RetainFiroQtLauncherCleanupUncertainty(
        &cleanup_unverified_, &unverified_cleanup_failure_,
        "pending candidate ownership changed before replacement");
    throw FiroQtLauncherCleanupUnverified(
        "pending Firo-Qt launcher cleanup could not be verified: " +
        unverified_cleanup_failure_);
  }
  if (cleanup_unverified_) {
    throw FiroQtLauncherCleanupUnverified(
        "a previous Firo-Qt launcher cleanup could not be verified: " +
        unverified_cleanup_failure_);
  }
  const FiroQtLauncherAuthority authority =
      ResolveLauncherAuthority(authority_resolver_, parsed, stop_token);
  const std::string operator_command = authority.command.ShellCommand();
  OwnedFiroQtLauncher candidate = OwnedFiroQtLauncher::Create(operator_command);
  FiroQtLauncherSnapshot publication;
  try {
    publication = FiroQtLauncherSnapshot{
        .node_id = authority.node_id,
        .operator_command = operator_command,
        .launcher_path = candidate.path(),
    };
    ThrowIfCancelled(stop_token);
    if (!SameLauncherAuthority(
            authority, ResolveLauncherAuthority(authority_resolver_, parsed,
                                                stop_token))) {
      throw std::runtime_error(
          "the live Firo-Qt launcher authority changed during candidate "
          "creation");
    }

    if (launcher_) {
      FiroQtLauncherCleanupResult cleanup;
      try {
        cleanup = launcher_->Cleanup();
      } catch (...) {
        const std::exception_ptr cleanup_failure = std::current_exception();
        if (!launcher_->active()) {
          RetainFiroQtLauncherCleanupUncertainty(
              &cleanup_unverified_, &unverified_cleanup_failure_,
              "published launcher cleanup lost its retryable ownership "
              "after an exception");
        }
        std::rethrow_exception(cleanup_failure);
      }
      if (cleanup == FiroQtLauncherCleanupResult::kOwnershipChanged) {
        RetainFiroQtLauncherCleanupUncertainty(
            &cleanup_unverified_, &unverified_cleanup_failure_,
            "previous Firo-Qt launcher ownership changed before replacement");
        launcher_.reset();
        std::lock_guard<std::mutex> snapshot_lock(snapshot_mutex_);
        snapshot_.reset();
        throw FiroQtLauncherCleanupUnverified(
            "previous Firo-Qt launcher cleanup could not be verified: " +
            unverified_cleanup_failure_);
      }
      launcher_.reset();
    }
    ThrowIfCancelled(stop_token);
    if (!SameLauncherAuthority(
            authority, ResolveLauncherAuthority(authority_resolver_, parsed,
                                                stop_token))) {
      throw std::runtime_error(
          "the live Firo-Qt launcher authority changed before publication");
    }

    {
      std::lock_guard<std::mutex> snapshot_lock(snapshot_mutex_);
      snapshot_ = publication;
    }
    launcher_.emplace(std::move(candidate));
  } catch (...) {
    ReconcileSnapshotAfterCleanupFailure();
    RethrowAfterCandidateCleanup(&candidate, std::current_exception(),
                                 &pending_cleanup_, &cleanup_unverified_,
                                 &unverified_cleanup_failure_);
  }
  return publication;
}

std::optional<FiroQtLauncherSnapshot> FiroQtLauncherService::Snapshot() const {
  std::lock_guard<std::mutex> lock(snapshot_mutex_);
  return snapshot_;
}

void FiroQtLauncherService::ReconcileSnapshotAfterCleanupFailure() {
  if (!launcher_) {
    std::lock_guard<std::mutex> snapshot_lock(snapshot_mutex_);
    snapshot_.reset();
    return;
  }
  const bool active = launcher_->active();
  {
    std::lock_guard<std::mutex> snapshot_lock(snapshot_mutex_);
    if (!active || !snapshot_ ||
        launcher_->path() != snapshot_->launcher_path) {
      snapshot_.reset();
    }
  }
  if (!active) {
    launcher_.reset();
  }
}

FiroQtLauncherCleanupResult FiroQtLauncherService::CloseAndCleanup() {
  return CloseAndCleanupImpl(std::nullopt, {});
}

FiroQtLauncherCleanupResult FiroQtLauncherService::CloseAndCleanup(
    std::stop_token stop_token) {
  return CloseAndCleanupImpl(std::nullopt, stop_token);
}

FiroQtLauncherCleanupResult FiroQtLauncherService::CloseAndCleanup(
    std::chrono::steady_clock::time_point deadline,
    std::stop_token stop_token) {
  return CloseAndCleanupImpl(deadline, stop_token);
}

FiroQtLauncherCleanupResult FiroQtLauncherService::CloseAndCleanupImpl(
    std::optional<std::chrono::steady_clock::time_point> deadline,
    std::stop_token stop_token) {
  std::unique_lock<std::timed_mutex> lock = Acquire(deadline, stop_token);
  closed_ = true;
  const auto retain_cleanup_uncertainty = [&](std::string_view detail) {
    RetainFiroQtLauncherCleanupUncertainty(
        &cleanup_unverified_, &unverified_cleanup_failure_, detail);
  };
  FiroQtLauncherCleanupResult pending_cleanup =
      FiroQtLauncherCleanupResult::kAlreadyAbsent;
  std::exception_ptr pending_failure;
  try {
    pending_cleanup =
        CleanupPendingCandidate(&pending_cleanup_, deadline, stop_token);
  } catch (...) {
    pending_failure = std::current_exception();
    if (!pending_cleanup_) {
      retain_cleanup_uncertainty(
          "pending candidate cleanup lost its retryable ownership after an "
          "exception");
    }
    if (stop_token.stop_requested() ||
        (deadline && std::chrono::steady_clock::now() >= *deadline)) {
      std::rethrow_exception(pending_failure);
    }
  }
  if (pending_cleanup == FiroQtLauncherCleanupResult::kOwnershipChanged) {
    retain_cleanup_uncertainty(
        "pending candidate ownership changed before cleanup");
  }

  FiroQtLauncherCleanupResult launcher_cleanup =
      FiroQtLauncherCleanupResult::kAlreadyAbsent;
  std::exception_ptr launcher_failure;
  if (launcher_) {
    try {
      launcher_cleanup = deadline ? launcher_->Cleanup(*deadline, stop_token)
                         : stop_token.stop_possible()
                             ? launcher_->Cleanup(stop_token)
                             : launcher_->Cleanup();
      launcher_.reset();
      std::lock_guard<std::mutex> snapshot_lock(snapshot_mutex_);
      snapshot_.reset();
    } catch (...) {
      launcher_failure = std::current_exception();
      ReconcileSnapshotAfterCleanupFailure();
      if (!launcher_) {
        retain_cleanup_uncertainty(
            "published launcher cleanup lost its retryable ownership after an "
            "exception");
      }
      if (stop_token.stop_requested() ||
          (deadline && std::chrono::steady_clock::now() >= *deadline)) {
        std::rethrow_exception(launcher_failure);
      }
    }
  } else {
    std::lock_guard<std::mutex> snapshot_lock(snapshot_mutex_);
    snapshot_.reset();
  }
  if (launcher_cleanup == FiroQtLauncherCleanupResult::kOwnershipChanged) {
    retain_cleanup_uncertainty(
        "published launcher ownership changed before cleanup");
  }

  if (pending_failure || launcher_failure || cleanup_unverified_) {
    const bool ownership_unverified = cleanup_unverified_;
    std::string message = "Firo-Qt launcher cleanup failed";
    if (pending_failure) {
      message += "; pending candidate: " + ExceptionText(pending_failure);
    }
    if (launcher_failure) {
      message += "; published launcher: " + ExceptionText(launcher_failure);
    }
    if (cleanup_unverified_) {
      message += "; unverified launcher cleanup";
      if (!unverified_cleanup_failure_.empty()) {
        message += ": " + unverified_cleanup_failure_;
      }
    }
    if (ownership_unverified) {
      throw FiroQtLauncherCleanupUnverified(message);
    }
    throw std::runtime_error(message);
  }
  return MergeCleanupResults(pending_cleanup, launcher_cleanup);
}

void FiroQtLauncherService::CloseAndCleanupNoThrow() noexcept {
  try {
    static_cast<void>(CloseAndCleanup());
  } catch (...) {
  }
}

#ifdef BBP_ENABLE_TEST_HOOKS
void SetFiroQtLauncherCleanupTestHook(FiroQtLauncherCleanupTestHook hook) {
  firo_qt_launcher_cleanup_test_hook = std::move(hook);
}
#endif

std::shared_ptr<OperatorConnectionLauncher>
FiroDriver::CreateOperatorConnectionLauncher(
    OperatorConnectionLauncherAuthorityResolver resolver) const {
  return std::make_shared<FiroQtLauncherService>(std::move(resolver));
}

}  // namespace bbp
