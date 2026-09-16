#include "simulator_startup_recovery.h"

#include <dirent.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

#include <cerrno>
#include <chrono>
#include <stdexcept>
#include <string>
#include <system_error>
#include <utility>

#include "bbp/logging.h"
#include "bbp/runtime_node_resource_manifest.h"
#include "bbp/simulator/options.h"
#include "bbp/util.h"
#include "simulator_offline_run_cleanup.h"

namespace bbp::simulator_app_internal {
namespace {

constexpr char kActivityRecord[] = ".bbp-active-v1";
constexpr int kDirectoryFlags =
    O_RDONLY | O_DIRECTORY | O_CLOEXEC | O_NOFOLLOW | O_NONBLOCK;

class UniqueFd {
 public:
  explicit UniqueFd(int fd) : fd_(fd) {}
  ~UniqueFd() {
    if (fd_ >= 0) {
      static_cast<void>(close(fd_));
    }
  }
  UniqueFd(UniqueFd&& other) noexcept : fd_(std::exchange(other.fd_, -1)) {}
  UniqueFd(const UniqueFd&) = delete;
  UniqueFd& operator=(const UniqueFd&) = delete;
  int get() const { return fd_; }

 private:
  int fd_;
};

void Require(bool condition, const char* action) {
  if (!condition) {
    throw std::system_error(errno, std::generic_category(), action);
  }
}

struct stat InspectRoot(int fd, const std::filesystem::path& path,
                        bool require_owner = true) {
  struct stat opened{};
  struct stat linked{};
  Require(fstat(fd, &opened) == 0 && fstatat(AT_FDCWD, path.c_str(), &linked,
                                             AT_SYMLINK_NOFOLLOW) == 0,
          "inspect startup recovery root");
  if (!S_ISDIR(opened.st_mode) || !S_ISDIR(linked.st_mode) ||
      (require_owner && opened.st_uid != geteuid()) ||
      opened.st_dev != linked.st_dev || opened.st_ino != linked.st_ino) {
    throw std::runtime_error("startup recovery root identity is unverified");
  }
  return opened;
}

std::string ActivityRecord(const RunOwnership& ownership,
                           const struct stat& root) {
  return ownership.resource_id + "\n" +
         std::to_string(static_cast<std::uintmax_t>(root.st_dev)) + "\n" +
         std::to_string(static_cast<std::uintmax_t>(root.st_ino)) + "\n";
}

}  // namespace

class RunRecoveryLease::State {
 public:
  State(RunOwnership ownership, UniqueFd root, bool create)
      : ownership_(std::move(ownership)),
        root_(std::move(root)),
        root_identity_(InspectRoot(root_.get(), ownership_.run_root)),
        record_(openat(root_.get(), kActivityRecord,
                       O_RDWR | O_CLOEXEC | O_NOFOLLOW | O_NONBLOCK |
                           (create ? O_CREAT | O_EXCL : 0),
                       0600)) {
    Require(record_.get() >= 0, "open startup recovery record");
    Require(fstat(record_.get(), &record_identity_) == 0,
            "inspect startup recovery record");
    if (!S_ISREG(record_identity_.st_mode) ||
        record_identity_.st_uid != geteuid() ||
        record_identity_.st_nlink != 1 ||
        (record_identity_.st_mode & 0077) != 0) {
      throw std::runtime_error(
          "startup recovery record is not private and owned");
    }
    struct flock lock{};
    lock.l_type = F_WRLCK;
    lock.l_whence = SEEK_SET;
    int result;
    do {
      result = fcntl(record_.get(), F_SETLK, &lock);
    } while (result != 0 && errno == EINTR);
    if (!create && result != 0 && (errno == EACCES || errno == EAGAIN)) {
      return;
    }
    Require(result == 0, "lock startup recovery record");
    acquired_ = true;
    VerifyIdentity();
    const std::string expected = ActivityRecord(ownership_, root_identity_);
    if (create) {
      std::size_t offset = 0U;
      while (offset < expected.size()) {
        const ssize_t count = write(record_.get(), expected.data() + offset,
                                    expected.size() - offset);
        if (count < 0 && errno == EINTR) {
          continue;
        }
        Require(count > 0, "write startup recovery record");
        offset += static_cast<std::size_t>(count);
      }
      Require(fsync(record_.get()) == 0 && fsync(root_.get()) == 0,
              "sync startup recovery record");
    } else {
      // Read this descriptor: closing a second descriptor for the same inode
      // would release our POSIX process lock.
      std::string actual(expected.size(), '\0');
      ssize_t count;
      do {
        count = pread(record_.get(), actual.data(), actual.size(), 0);
      } while (count < 0 && errno == EINTR);
      if (record_identity_.st_size != static_cast<off_t>(expected.size()) ||
          count != static_cast<ssize_t>(expected.size()) ||
          actual != expected) {
        throw std::runtime_error(
            "startup recovery record identity does not match");
      }
    }
  }

  bool acquired() const { return acquired_; }
  OwnedRunRootIdentity root_identity() const {
    return {.device = static_cast<std::uintmax_t>(root_identity_.st_dev),
            .inode = static_cast<std::uintmax_t>(root_identity_.st_ino)};
  }

  void Complete() {
    VerifyIdentity();
    Require(unlinkat(root_.get(), kActivityRecord, 0) == 0,
            "remove completed startup recovery record");
    Require(fsync(root_.get()) == 0, "sync completed startup recovery record");
  }

 private:
  void VerifyIdentity() const {
    const struct stat root = InspectRoot(root_.get(), ownership_.run_root);
    struct stat linked_record{};
    Require(fstatat(root_.get(), kActivityRecord, &linked_record,
                    AT_SYMLINK_NOFOLLOW) == 0,
            "verify startup recovery record link");
    if (root.st_dev != root_identity_.st_dev ||
        root.st_ino != root_identity_.st_ino ||
        !S_ISREG(linked_record.st_mode) ||
        linked_record.st_dev != record_identity_.st_dev ||
        linked_record.st_ino != record_identity_.st_ino ||
        LoadRunOwnershipAt(ownership_.run_id, ownership_.run_root,
                           root_.get()) != ownership_) {
      throw std::runtime_error("startup recovery ownership changed");
    }
  }

  RunOwnership ownership_;
  UniqueFd root_;
  struct stat root_identity_{};
  UniqueFd record_;
  struct stat record_identity_{};
  bool acquired_ = false;
};

RunRecoveryLease::RunRecoveryLease(const RunOwnership& ownership)
    : state_(std::make_unique<State>(
          ownership,
          UniqueFd(open(ownership.run_root.c_str(), kDirectoryFlags)), true)) {}

RunRecoveryLease::~RunRecoveryLease() = default;

void RunRecoveryLease::Complete() { state_->Complete(); }

void RecoverStaleRuns(const std::filesystem::path& benchmark_root) {
  UniqueFd root(open(benchmark_root.c_str(), kDirectoryFlags));
  if (root.get() < 0 && errno == ENOENT) {
    return;
  }
  Require(root.get() >= 0, "open startup recovery benchmark root");
  const struct stat initial = InspectRoot(root.get(), benchmark_root, false);
  const auto canonical_root = std::filesystem::canonical(benchmark_root);
  const int copy = fcntl(root.get(), F_DUPFD_CLOEXEC, 0);
  Require(copy >= 0, "duplicate startup recovery directory");
  DIR* raw = fdopendir(copy);
  if (raw == nullptr) {
    const int error = errno;
    static_cast<void>(close(copy));
    throw std::system_error(error, std::generic_category(),
                            "enumerate startup recovery directory");
  }
  std::unique_ptr<DIR, int (*)(DIR*)> directory(raw, closedir);
  for (;;) {
    errno = 0;
    const dirent* entry = readdir(directory.get());
    if (entry == nullptr) {
      Require(errno == 0, "read startup recovery directory");
      break;
    }
    const std::string run_id(entry->d_name);
    try {
      RequireSafeRunId(run_id);
    } catch (const std::exception&) {
      continue;
    }
    struct stat linked{};
    Require(
        fstatat(root.get(), run_id.c_str(), &linked, AT_SYMLINK_NOFOLLOW) == 0,
        "inspect startup recovery candidate");
    if (!S_ISDIR(linked.st_mode) || linked.st_uid != geteuid()) {
      continue;
    }
    UniqueFd child(openat(root.get(), run_id.c_str(), kDirectoryFlags));
    Require(child.get() >= 0, "open startup recovery candidate");
    struct stat activity{};
    if (fstatat(child.get(), kActivityRecord, &activity, AT_SYMLINK_NOFOLLOW) !=
        0) {
      Require(errno == ENOENT, "inspect startup recovery activity");
      if (faccessat(child.get(), ".bbp-run", F_OK, AT_SYMLINK_NOFOLLOW) == 0) {
        BBP_LOG(info)
            << "startup recovery retained artifacts without an active "
               "recovery record: "
            << (canonical_root / run_id);
      }
      continue;
    }
    const RunOwnership ownership =
        LoadRunOwnershipAt(run_id, canonical_root / run_id, child.get());
    RunRecoveryLease::State lease(ownership, std::move(child), false);
    if (!lease.acquired()) {
      BBP_LOG(info) << "startup recovery preserved active run " << run_id;
      continue;
    }
    Options cleanup;
    cleanup.output_dir = canonical_root;
    cleanup.run_id = run_id;
    cleanup.nodes = 0U;
    cleanup.isolate_network = false;
    static_cast<void>(CleanupRun(
        cleanup, std::chrono::steady_clock::now() + std::chrono::seconds(30),
        {}, false, &ownership, lease.root_identity()));
    lease.Complete();
    BBP_LOG(info) << "startup recovery completed for " << run_id
                  << "; retained artifacts: " << ownership.run_root;
  }
  const struct stat final = InspectRoot(root.get(), benchmark_root, false);
  if (initial.st_dev != final.st_dev || initial.st_ino != final.st_ino) {
    throw std::runtime_error("startup recovery benchmark root changed");
  }
}

}  // namespace bbp::simulator_app_internal
