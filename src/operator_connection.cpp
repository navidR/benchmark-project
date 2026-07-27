#include "bbp/operator_connection.h"

#include <arpa/inet.h>
#include <fcntl.h>
#include <linux/fs.h>
#include <sys/random.h>
#include <sys/stat.h>
#include <sys/syscall.h>
#include <unistd.h>

#include <algorithm>
#include <array>
#include <boost/json/value.hpp>
#include <cerrno>
#include <chrono>
#include <cstdlib>
#include <exception>
#include <limits>
#include <stdexcept>
#include <system_error>
#include <utility>
#include <vector>

#include "bbp/simulation_cancelled.h"

namespace bbp {
namespace {

constexpr std::string_view kFiroQtLauncherTemplate =
    "/tmp/bbp-firo-qt-XXXXXX.sh";
constexpr std::size_t kMaximumFiroQtLauncherCommandBytes = 1024U * 1024U;
constexpr std::size_t kFiroQtLauncherCleanupRandomBytes = 16U;
constexpr std::size_t kFiroQtLauncherCleanupAttempts = 8U;

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

 private:
  int descriptor_ = -1;
};

class FiroQtLauncherCleanupFailure : public std::runtime_error {
 public:
  using std::runtime_error::runtime_error;
};

[[noreturn]] void ThrowSystemError(std::string_view operation);

std::string RandomFiroQtLauncherCleanupName() {
  std::array<unsigned char, kFiroQtLauncherCleanupRandomBytes> bytes{};
  std::size_t offset = 0U;
  while (offset < bytes.size()) {
    const ssize_t count =
        getrandom(bytes.data() + offset, bytes.size() - offset, 0U);
    if (count < 0) {
      if (errno == EINTR) {
        continue;
      }
      ThrowSystemError("generate Firo-Qt launcher cleanup identity");
    }
    if (count == 0) {
      throw std::runtime_error(
          "generate Firo-Qt launcher cleanup identity made no progress");
    }
    offset += static_cast<std::size_t>(count);
  }

  constexpr char kHex[] = "0123456789abcdef";
  std::string name = ".bbp-firo-qt-cleanup-";
  name.reserve(name.size() + bytes.size() * 2U);
  for (const unsigned char byte : bytes) {
    name.push_back(kHex[byte >> 4U]);
    name.push_back(kHex[byte & 0x0fU]);
  }
  return name;
}

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

[[noreturn]] void RethrowAfterCandidateCleanup(
    OwnedFiroQtLauncher* candidate, const std::exception_ptr& failure,
    std::optional<OwnedFiroQtLauncher>* pending_cleanup) {
  if (candidate == nullptr || !candidate->active()) {
    std::rethrow_exception(failure);
  }
  std::string cleanup_failure;
  try {
    if (candidate->Cleanup() ==
        FiroQtLauncherCleanupResult::kOwnershipChanged) {
      cleanup_failure = "candidate launcher ownership changed";
    }
  } catch (const std::exception& error) {
    cleanup_failure = error.what();
  } catch (...) {
    cleanup_failure = "non-standard candidate cleanup exception";
  }
  if (!cleanup_failure.empty()) {
    if (candidate->active()) {
      if (pending_cleanup == nullptr || pending_cleanup->has_value()) {
        throw std::logic_error(
            "Firo-Qt launcher candidate cleanup tracking is unavailable");
      }
      pending_cleanup->emplace(std::move(*candidate));
    }
    throw std::runtime_error(
        "Firo-Qt launcher replacement failed: " + ExceptionText(failure) +
        "; verified candidate cleanup also failed: " + cleanup_failure);
  }
  std::rethrow_exception(failure);
}

FiroQtLauncherCleanupResult CleanupPendingCandidate(
    std::optional<OwnedFiroQtLauncher>* pending_cleanup) {
  if (pending_cleanup == nullptr || !*pending_cleanup) {
    return FiroQtLauncherCleanupResult::kAlreadyAbsent;
  }
  FiroQtLauncherCleanupResult cleanup;
  try {
    cleanup = (*pending_cleanup)->Cleanup();
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
                                         std::uintmax_t inode, int descriptor)
    : path_(std::move(path)),
      device_(device),
      inode_(inode),
      descriptor_(descriptor),
      active_(true) {}

OwnedFiroQtLauncher::OwnedFiroQtLauncher(OwnedFiroQtLauncher&& other) noexcept
    : path_(std::move(other.path_)),
      device_(other.device_),
      inode_(other.inode_),
      descriptor_(std::exchange(other.descriptor_, -1)),
      active_(std::exchange(other.active_, false)) {
  other.device_ = 0U;
  other.inode_ = 0U;
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
  active_ = std::exchange(other.active_, false);
  other.device_ = 0U;
  other.inode_ = 0U;
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

  std::vector<char> path_template(kFiroQtLauncherTemplate.begin(),
                                  kFiroQtLauncherTemplate.end());
  path_template.push_back('\0');
  int descriptor = mkostemps(path_template.data(), 3, O_CLOEXEC);
  if (descriptor < 0) {
    ThrowSystemError("create Firo-Qt launcher");
  }
  const std::filesystem::path path(path_template.data());
  struct stat created_status{};
  if (fstat(descriptor, &created_status) != 0) {
    const int error = errno;
    static_cast<void>(close(descriptor));
    throw std::system_error(error, std::generic_category(),
                            "inspect created Firo-Qt launcher");
  }

  OwnedFiroQtLauncher launcher(
      path, static_cast<std::uintmax_t>(created_status.st_dev),
      static_cast<std::uintmax_t>(created_status.st_ino), descriptor);
  descriptor = -1;

  try {
    const int descriptor_flags = fcntl(launcher.descriptor_, F_GETFD);
    if (descriptor_flags < 0 || (descriptor_flags & FD_CLOEXEC) == 0) {
      ThrowSystemError("protect Firo-Qt launcher descriptor");
    }
    const std::string content =
        "#!/bin/bash\nexec " + std::string(shell_command) + "\n";
    WriteAll(launcher.descriptor_, content);
    if (fsync(launcher.descriptor_) != 0) {
      ThrowSystemError("sync Firo-Qt launcher");
    }
    if (fchmod(launcher.descriptor_, S_IRWXU) != 0) {
      ThrowSystemError("set Firo-Qt launcher permissions");
    }
    struct stat status{};
    if (fstat(launcher.descriptor_, &status) != 0) {
      ThrowSystemError("inspect Firo-Qt launcher");
    }
    if (!S_ISREG(status.st_mode) || (status.st_mode & 07777) != S_IRWXU) {
      throw std::runtime_error(
          "Firo-Qt launcher is not a regular mode-0700 file");
    }
    return launcher;
  } catch (...) {
    const std::exception_ptr creation_error = std::current_exception();
    try {
      static_cast<void>(launcher.Cleanup());
    } catch (const std::exception& cleanup_error) {
      throw FiroQtLauncherCleanupFailure(
          "Firo-Qt launcher creation failed and verified cleanup also "
          "failed: " +
          std::string(cleanup_error.what()));
    }
    std::rethrow_exception(creation_error);
  }
}

FiroQtLauncherCleanupResult OwnedFiroQtLauncher::Cleanup() {
  if (!active_) {
    return FiroQtLauncherCleanupResult::kAlreadyAbsent;
  }

  const std::filesystem::path parent = path_.parent_path();
  const std::string public_name = path_.filename().string();
  if (parent.empty() || public_name.empty()) {
    throw std::runtime_error(
        "owned Firo-Qt launcher has no cleanup parent or filename");
  }
  const int parent_descriptor =
      open(parent.c_str(), O_RDONLY | O_DIRECTORY | O_CLOEXEC | O_NOFOLLOW);
  if (parent_descriptor < 0) {
    ThrowSystemError("open owned Firo-Qt launcher cleanup directory");
  }
  const ScopedDescriptor parent_guard(parent_descriptor);

  struct stat status{};
  if (fstatat(parent_guard.get(), public_name.c_str(), &status,
              AT_SYMLINK_NOFOLLOW) != 0) {
    if (errno == ENOENT) {
      ResetOwnership();
      return FiroQtLauncherCleanupResult::kAlreadyAbsent;
    }
    ThrowSystemError("inspect owned Firo-Qt launcher during cleanup");
  }
  if (!S_ISREG(status.st_mode) ||
      static_cast<std::uintmax_t>(status.st_dev) != device_ ||
      static_cast<std::uintmax_t>(status.st_ino) != inode_) {
    ResetOwnership();
    return FiroQtLauncherCleanupResult::kOwnershipChanged;
  }

#ifdef BBP_ENABLE_TEST_HOOKS
  if (firo_qt_launcher_cleanup_test_hook) {
    firo_qt_launcher_cleanup_test_hook(
        FiroQtLauncherCleanupTestPhase::kAfterPublicIdentityCheck, path_,
        std::nullopt);
  }
#endif

  std::filesystem::path public_path = path_;
  bool captured = false;
  for (std::size_t attempt = 0U; attempt < kFiroQtLauncherCleanupAttempts;
       ++attempt) {
    const std::string quarantine_name = RandomFiroQtLauncherCleanupName();
    std::filesystem::path quarantine_path = parent / quarantine_name;
    if (syscall(SYS_renameat2, parent_guard.get(), public_name.c_str(),
                parent_guard.get(), quarantine_name.c_str(),
                RENAME_NOREPLACE) == 0) {
      path_.swap(quarantine_path);
      captured = true;
      break;
    }
    const int error = errno;
    if (error == EEXIST) {
      continue;
    }
    if (error == ENOENT) {
      ResetOwnership();
      return FiroQtLauncherCleanupResult::kAlreadyAbsent;
    }
    if (error == ENOSYS || error == EINVAL || error == EOPNOTSUPP) {
      throw std::runtime_error(
          "identity-safe Firo-Qt launcher cleanup is unsupported: " +
          std::error_code(error, std::generic_category()).message());
    }
    throw std::system_error(error, std::generic_category(),
                            "atomically capture owned Firo-Qt launcher");
  }
  if (!captured) {
    throw std::runtime_error(
        "could not reserve a Firo-Qt launcher cleanup quarantine");
  }

#ifdef BBP_ENABLE_TEST_HOOKS
  if (firo_qt_launcher_cleanup_test_hook) {
    firo_qt_launcher_cleanup_test_hook(
        FiroQtLauncherCleanupTestPhase::kAfterAtomicCapture, public_path,
        path_);
  }
#endif

  const std::string quarantine_name = path_.filename().string();
  if (fstatat(parent_guard.get(), quarantine_name.c_str(), &status,
              AT_SYMLINK_NOFOLLOW) != 0) {
    ThrowSystemError("inspect quarantined Firo-Qt launcher during cleanup");
  }
  if (!S_ISREG(status.st_mode) ||
      static_cast<std::uintmax_t>(status.st_dev) != device_ ||
      static_cast<std::uintmax_t>(status.st_ino) != inode_) {
    if (syscall(SYS_renameat2, parent_guard.get(), quarantine_name.c_str(),
                parent_guard.get(), public_name.c_str(),
                RENAME_NOREPLACE) == 0) {
      ResetOwnership();
      return FiroQtLauncherCleanupResult::kOwnershipChanged;
    }
    const int error = errno;
    const std::filesystem::path preserved_foreign = path_;
    ResetOwnership();
    throw std::runtime_error(
        "could not restore a foreign Firo-Qt launcher from " +
        preserved_foreign.string() + ": " +
        std::error_code(error, std::generic_category()).message());
  }

  if (unlinkat(parent_guard.get(), quarantine_name.c_str(), 0) != 0) {
    if (errno == ENOENT) {
      ResetOwnership();
      return FiroQtLauncherCleanupResult::kAlreadyAbsent;
    }
    ThrowSystemError("remove quarantined owned Firo-Qt launcher");
  }
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
    std::stop_token stop_token) const {
  std::unique_lock<std::timed_mutex> lock(mutation_mutex_, std::defer_lock);
  while (!lock.try_lock_for(std::chrono::milliseconds(10))) {
    ThrowIfCancelled(stop_token);
  }
  ThrowIfCancelled(stop_token);
  return lock;
}

FiroQtLauncherSnapshot FiroQtLauncherService::ReplaceFromReport(
    const boost::json::object& report,
    std::optional<std::string_view> required_node_id,
    std::stop_token stop_token) {
  const ParsedFiroQtLauncherCommand parsed =
      FiroQtLauncherCommandFromReport(report, required_node_id);
  ThrowIfCancelled(stop_token);
  std::unique_lock<std::timed_mutex> lock = Acquire(stop_token);
  if (closed_) {
    throw std::runtime_error("the Firo-Qt launcher service is closed");
  }
  if (CleanupPendingCandidate(&pending_cleanup_) ==
      FiroQtLauncherCleanupResult::kOwnershipChanged) {
    throw std::runtime_error(
        "pending Firo-Qt launcher ownership changed before replacement");
  }
  if (!unverified_cleanup_failure_.empty()) {
    throw std::runtime_error(
        "a previous Firo-Qt launcher cleanup could not be verified: " +
        unverified_cleanup_failure_);
  }
  const FiroQtLauncherAuthority authority =
      ResolveLauncherAuthority(authority_resolver_, parsed, stop_token);
  const std::string operator_command = authority.command.ShellCommand();
  OwnedFiroQtLauncher candidate = [&] {
    try {
      return OwnedFiroQtLauncher::Create(operator_command);
    } catch (const FiroQtLauncherCleanupFailure& error) {
      unverified_cleanup_failure_ = error.what();
      throw;
    }
  }();
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
      const FiroQtLauncherCleanupResult cleanup = launcher_->Cleanup();
      if (cleanup == FiroQtLauncherCleanupResult::kOwnershipChanged) {
        launcher_.reset();
        std::lock_guard<std::mutex> snapshot_lock(snapshot_mutex_);
        snapshot_.reset();
        throw std::runtime_error(
            "previous Firo-Qt launcher ownership changed before replacement");
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
                                 &pending_cleanup_);
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
  std::lock_guard<std::timed_mutex> lock(mutation_mutex_);
  closed_ = true;
  FiroQtLauncherCleanupResult pending_cleanup =
      FiroQtLauncherCleanupResult::kAlreadyAbsent;
  std::exception_ptr pending_failure;
  try {
    pending_cleanup = CleanupPendingCandidate(&pending_cleanup_);
  } catch (...) {
    pending_failure = std::current_exception();
  }

  FiroQtLauncherCleanupResult launcher_cleanup =
      FiroQtLauncherCleanupResult::kAlreadyAbsent;
  std::exception_ptr launcher_failure;
  if (launcher_) {
    try {
      launcher_cleanup = launcher_->Cleanup();
      launcher_.reset();
      std::lock_guard<std::mutex> snapshot_lock(snapshot_mutex_);
      snapshot_.reset();
    } catch (...) {
      launcher_failure = std::current_exception();
      ReconcileSnapshotAfterCleanupFailure();
    }
  } else {
    std::lock_guard<std::mutex> snapshot_lock(snapshot_mutex_);
    snapshot_.reset();
  }

  if (pending_failure || launcher_failure ||
      !unverified_cleanup_failure_.empty()) {
    std::string message = "Firo-Qt launcher cleanup failed";
    if (pending_failure) {
      message += "; pending candidate: " + ExceptionText(pending_failure);
    }
    if (launcher_failure) {
      message += "; published launcher: " + ExceptionText(launcher_failure);
    }
    if (!unverified_cleanup_failure_.empty()) {
      message +=
          "; unverified creation cleanup: " + unverified_cleanup_failure_;
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

std::string PosixShellQuote(std::string_view value) {
  if (value.find('\0') != std::string_view::npos) {
    throw std::invalid_argument("shell argument contains NUL");
  }
  std::string quoted;
  quoted.reserve(value.size() + 2U);
  quoted.push_back('\'');
  for (const char character : value) {
    if (character == '\'') {
      quoted += "'\"'\"'";
    } else {
      quoted.push_back(character);
    }
  }
  quoted.push_back('\'');
  return quoted;
}

std::string OperatorConnectionCommand::ShellCommand() const {
  if (executable.empty()) {
    throw std::invalid_argument("operator connection executable is empty");
  }
  std::string command = PosixShellQuote(executable.string());
  for (const std::string& argument : arguments) {
    command.push_back(' ');
    command += PosixShellQuote(argument);
  }
  return command;
}

std::string OperatorConnectionCommandFromReport(
    const boost::json::object& report) {
  const boost::json::value* connection =
      report.if_contains("operator_connection_command");
  if (connection == nullptr || !connection->is_object()) {
    return {};
  }
  const boost::json::value* command =
      connection->as_object().if_contains("command");
  if (command == nullptr || !command->is_string()) {
    return {};
  }
  return std::string(command->as_string());
}

}  // namespace bbp
