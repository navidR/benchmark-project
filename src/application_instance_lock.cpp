#include "bbp/application_instance_lock.h"

#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

#include <cerrno>
#include <cstdlib>
#include <cstring>
#include <stdexcept>
#include <string>
#include <string_view>
#include <utility>

namespace bbp {
namespace {

constexpr std::string_view kStateDirectoryName = ".bbp";
constexpr std::string_view kLockFileName = "bbp.lock";

std::runtime_error SystemError(std::string_view action,
                               const std::filesystem::path& path,
                               int error = errno) {
  return std::runtime_error(std::string(action) + " " + path.string() +
                            " failed: " + std::strerror(error));
}

std::filesystem::path DefaultStateDirectory() {
  const char* home = std::getenv("HOME");
  if (home == nullptr || *home == '\0') {
    throw std::runtime_error(
        "HOME must be set before BBP can create its state directory");
  }
  std::filesystem::path home_path(home);
  if (!home_path.is_absolute()) {
    throw std::runtime_error("HOME must be an absolute path");
  }
  return home_path / kStateDirectoryName;
}

int OpenPrivateStateDirectory(const std::filesystem::path& path) {
  if (path.empty() || !path.is_absolute()) {
    throw std::runtime_error("BBP state directory must be an absolute path");
  }
  if (mkdir(path.c_str(), S_IRWXU) != 0 && errno != EEXIST) {
    throw SystemError("create BBP state directory", path);
  }
  const int descriptor =
      open(path.c_str(), O_RDONLY | O_DIRECTORY | O_CLOEXEC | O_NOFOLLOW);
  if (descriptor < 0) {
    throw SystemError("open BBP state directory", path);
  }
  struct stat status{};
  if (fstat(descriptor, &status) != 0) {
    const int error = errno;
    close(descriptor);
    throw SystemError("inspect BBP state directory", path, error);
  }
  if (!S_ISDIR(status.st_mode) || status.st_uid != geteuid()) {
    close(descriptor);
    throw std::runtime_error(
        "BBP state directory must be owned by the effective user: " +
        path.string());
  }
  if (fchmod(descriptor, S_IRWXU) != 0) {
    const int error = errno;
    close(descriptor);
    throw SystemError("secure BBP state directory", path, error);
  }
  return descriptor;
}

void WriteProcessId(int descriptor, const std::filesystem::path& path) {
  const std::string process_id =
      std::to_string(static_cast<long long>(getpid())) + "\n";
  if (ftruncate(descriptor, 0) != 0 || lseek(descriptor, 0, SEEK_SET) < 0) {
    throw SystemError("reset BBP instance lock", path);
  }
  std::size_t offset = 0U;
  while (offset < process_id.size()) {
    const ssize_t written = write(descriptor, process_id.data() + offset,
                                  process_id.size() - offset);
    if (written < 0) {
      if (errno == EINTR) {
        continue;
      }
      throw SystemError("write BBP instance lock", path);
    }
    if (written == 0) {
      throw std::runtime_error("write BBP instance lock made no progress: " +
                               path.string());
    }
    offset += static_cast<std::size_t>(written);
  }
  if (fsync(descriptor) != 0) {
    throw SystemError("sync BBP instance lock", path);
  }
}

}  // namespace

ApplicationInstanceLock::ApplicationInstanceLock()
    : ApplicationInstanceLock(DefaultStateDirectory()) {}

ApplicationInstanceLock::ApplicationInstanceLock(
    std::filesystem::path state_directory)
    : state_directory_(std::move(state_directory)) {
  const int state_directory_fd = OpenPrivateStateDirectory(state_directory_);
  const std::filesystem::path lock_path = state_directory_ / kLockFileName;
  lock_fd_ =
      openat(state_directory_fd, kLockFileName.data(),
             O_CREAT | O_RDWR | O_CLOEXEC | O_NOFOLLOW, S_IRUSR | S_IWUSR);
  const int open_error = errno;
  close(state_directory_fd);
  if (lock_fd_ < 0) {
    throw SystemError("open BBP instance lock", lock_path, open_error);
  }

  try {
    struct stat status{};
    if (fstat(lock_fd_, &status) != 0) {
      throw SystemError("inspect BBP instance lock", lock_path);
    }
    if (!S_ISREG(status.st_mode) || status.st_uid != geteuid() ||
        status.st_nlink != 1) {
      throw std::runtime_error(
          "BBP instance lock must be a single-link regular file owned by the "
          "effective user: " +
          lock_path.string());
    }
    if (fchmod(lock_fd_, S_IRUSR | S_IWUSR) != 0) {
      throw SystemError("secure BBP instance lock", lock_path);
    }

    struct flock lock{};
    lock.l_type = F_WRLCK;
    lock.l_whence = SEEK_SET;
    int result = 0;
    do {
      result = fcntl(lock_fd_, F_SETLK, &lock);
    } while (result != 0 && errno == EINTR);
    if (result != 0) {
      if (errno == EACCES || errno == EAGAIN) {
        throw std::runtime_error("another BBP instance is already running");
      }
      throw SystemError("acquire BBP instance lock", lock_path);
    }
    WriteProcessId(lock_fd_, lock_path);
  } catch (...) {
    close(lock_fd_);
    lock_fd_ = -1;
    throw;
  }
}

ApplicationInstanceLock::~ApplicationInstanceLock() {
  if (lock_fd_ >= 0) {
    close(lock_fd_);
  }
}

const std::filesystem::path& ApplicationInstanceLock::state_directory() const {
  return state_directory_;
}

}  // namespace bbp
