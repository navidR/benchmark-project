#include "bbp/operator_connection.h"

#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

#include <boost/json/value.hpp>
#include <cerrno>
#include <cstdlib>
#include <exception>
#include <stdexcept>
#include <system_error>
#include <utility>
#include <vector>

namespace bbp {
namespace {

constexpr std::string_view kFiroQtLauncherTemplate =
    "/tmp/bbp-firo-qt-XXXXXX.sh";

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
  struct stat created_status {};
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
    struct stat status {};
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
      throw std::runtime_error(
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

  struct stat status {};
  if (lstat(path_.c_str(), &status) != 0) {
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
  if (unlink(path_.c_str()) != 0) {
    if (errno == ENOENT) {
      ResetOwnership();
      return FiroQtLauncherCleanupResult::kAlreadyAbsent;
    }
    ThrowSystemError("remove owned Firo-Qt launcher");
  }
  if (lstat(path_.c_str(), &status) == 0) {
    ResetOwnership();
    return FiroQtLauncherCleanupResult::kOwnershipChanged;
  }
  if (errno != ENOENT) {
    ThrowSystemError("verify Firo-Qt launcher cleanup");
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
