#include "simulator_managed_run_root.h"

#include <fcntl.h>
#include <linux/fs.h>
#include <sys/random.h>
#include <sys/stat.h>
#include <sys/syscall.h>
#include <unistd.h>

#include <array>
#include <atomic>
#include <cerrno>
#include <chrono>
#include <exception>
#include <filesystem>
#include <optional>
#include <stdexcept>
#include <string>
#include <string_view>
#include <system_error>
#include <utility>

#include "bbp/cgroup.h"
#include "bbp/logging.h"
#include "bbp/mcp_operation_service.h"
#include "bbp/network.h"
#include "bbp/run_ownership.h"
#include "bbp/runtime_node_resource_manifest.h"
#include "bbp/simulator/constants.h"
#include "bbp/simulator/options.h"
#include "bbp/util.h"

namespace bbp::simulator_app_internal {
namespace {

class UniqueFileDescriptor {
 public:
  explicit UniqueFileDescriptor(int descriptor = -1)
      : descriptor_(descriptor) {}
  ~UniqueFileDescriptor() {
    if (descriptor_ >= 0) {
      static_cast<void>(close(descriptor_));
    }
  }

  UniqueFileDescriptor(const UniqueFileDescriptor&) = delete;
  UniqueFileDescriptor& operator=(const UniqueFileDescriptor&) = delete;
  UniqueFileDescriptor(UniqueFileDescriptor&& other) noexcept
      : descriptor_(std::exchange(other.descriptor_, -1)) {}
  UniqueFileDescriptor& operator=(UniqueFileDescriptor&& other) noexcept {
    if (this != &other) {
      if (descriptor_ >= 0) {
        static_cast<void>(close(descriptor_));
      }
      descriptor_ = std::exchange(other.descriptor_, -1);
    }
    return *this;
  }

  [[nodiscard]] int get() const { return descriptor_; }
  [[nodiscard]] bool valid() const { return descriptor_ >= 0; }

 private:
  int descriptor_ = -1;
};

std::string ExceptionMessage(const std::exception_ptr& error) {
  try {
    std::rethrow_exception(error);
  } catch (const std::exception& exception) {
    return exception.what();
  } catch (...) {
    return "unknown exception";
  }
}

const RunOwnership& RequireRunOwnership(const Options& options) {
  if (!options.run_ownership) {
    throw std::logic_error("run ownership is not initialized");
  }
  return *options.run_ownership;
}

std::string MakeReplayReservationName() {
  std::array<unsigned char, 16U> random_bytes{};
  std::size_t offset = 0U;
  while (offset < random_bytes.size()) {
    const ssize_t count = getrandom(random_bytes.data() + offset,
                                    random_bytes.size() - offset, 0U);
    if (count < 0) {
      if (errno == EINTR) {
        continue;
      }
      throw std::system_error(errno, std::generic_category(),
                              "generate replay reservation name");
    }
    if (count == 0) {
      throw std::runtime_error(
          "replay reservation randomness made no progress");
    }
    offset += static_cast<std::size_t>(count);
  }
  constexpr char kHex[] = "0123456789abcdef";
  std::string name = ".bbp-replay-reservation-";
  name.reserve(name.size() + random_bytes.size() * 2U);
  for (const unsigned char byte : random_bytes) {
    name.push_back(kHex[byte >> 4U]);
    name.push_back(kHex[byte & 0x0fU]);
  }
  return name;
}

void CreateReservedRunDirectoryAt(int run_root_fd, std::string_view name) {
  const std::string name_text(name);
  if (mkdirat(run_root_fd, name_text.c_str(), 0777) != 0) {
    throw std::runtime_error(
        "create reserved run directory failed for " + name_text + ": " +
        std::error_code(errno, std::generic_category()).message());
  }
  UniqueFileDescriptor child(
      openat(run_root_fd, name_text.c_str(),
             O_RDONLY | O_DIRECTORY | O_CLOEXEC | O_NOFOLLOW | O_NONBLOCK));
  if (!child.valid()) {
    throw std::runtime_error(
        "open reserved run directory failed for " + name_text + ": " +
        std::error_code(errno, std::generic_category()).message());
  }
  struct stat opened{};
  struct stat linked{};
  if (fstat(child.get(), &opened) != 0 ||
      fstatat(run_root_fd, name_text.c_str(), &linked, AT_SYMLINK_NOFOLLOW) !=
          0 ||
      !S_ISDIR(opened.st_mode) || !S_ISDIR(linked.st_mode) ||
      opened.st_uid != geteuid() || opened.st_dev != linked.st_dev ||
      opened.st_ino != linked.st_ino) {
    throw std::runtime_error(
        "reserved run directory identity could not be verified: " + name_text);
  }
}

}  // namespace

class ReservedManagedRunRoot::State {
 public:
  void Initialize(std::filesystem::path& run_root, std::string& run_id,
                  std::string& staging_name, UniqueFileDescriptor parent_fd,
                  OwnedRunRootIdentity root_identity) noexcept {
    run_root_.swap(run_root);
    run_id_.swap(run_id);
    staging_name_.swap(staging_name);
    parent_fd_ = std::move(parent_fd);
    root_identity_ = root_identity;
  }

  ~State() {
    if (adopted_.load(std::memory_order_acquire) || !parent_fd_.valid()) {
      return;
    }
    struct stat linked{};
    const std::string& linked_name = published_ ? run_id_ : staging_name_;
    if (fstatat(parent_fd_.get(), linked_name.c_str(), &linked,
                AT_SYMLINK_NOFOLLOW) != 0 ||
        !S_ISDIR(linked.st_mode) || linked.st_uid != geteuid() ||
        static_cast<std::uintmax_t>(linked.st_dev) != root_identity_.device ||
        static_cast<std::uintmax_t>(linked.st_ino) != root_identity_.inode) {
      return;
    }
    if (run_root_fd_.valid()) {
      struct stat opened{};
      if (fstat(run_root_fd_.get(), &opened) != 0 || !S_ISDIR(opened.st_mode) ||
          opened.st_dev != linked.st_dev || opened.st_ino != linked.st_ino) {
        return;
      }
    }
    if (marker_identity_) {
      const std::string marker_name(kRunMarkerFile);
      struct stat linked_marker{};
      if (fstatat(run_root_fd_.get(), marker_name.c_str(), &linked_marker,
                  AT_SYMLINK_NOFOLLOW) != 0) {
        if (errno != ENOENT) {
          return;
        }
      } else if (!S_ISREG(linked_marker.st_mode) ||
                 static_cast<std::uintmax_t>(linked_marker.st_dev) !=
                     marker_identity_->device ||
                 static_cast<std::uintmax_t>(linked_marker.st_ino) !=
                     marker_identity_->inode ||
                 unlinkat(run_root_fd_.get(), marker_name.c_str(), 0) != 0) {
        return;
      }
    }
    if (fstatat(parent_fd_.get(), linked_name.c_str(), &linked,
                AT_SYMLINK_NOFOLLOW) != 0 ||
        !S_ISDIR(linked.st_mode) ||
        static_cast<std::uintmax_t>(linked.st_dev) != root_identity_.device ||
        static_cast<std::uintmax_t>(linked.st_ino) != root_identity_.inode) {
      return;
    }
    static_cast<void>(
        unlinkat(parent_fd_.get(), linked_name.c_str(), AT_REMOVEDIR));
  }

  void BindOpenedRoot(UniqueFileDescriptor run_root_fd) {
    if (run_root_fd_.valid() || !run_root_fd.valid()) {
      throw std::logic_error(
          "replay reservation root descriptor binding is invalid");
    }
    struct stat opened{};
    if (fstat(run_root_fd.get(), &opened) != 0) {
      throw std::system_error(errno, std::generic_category(),
                              "inspect opened replay reservation");
    }
    if (!S_ISDIR(opened.st_mode) || opened.st_uid != geteuid() ||
        static_cast<std::uintmax_t>(opened.st_dev) != root_identity_.device ||
        static_cast<std::uintmax_t>(opened.st_ino) != root_identity_.inode) {
      throw std::runtime_error(
          "opened replay reservation is not the directory BBP created");
    }
    run_root_fd_ = std::move(run_root_fd);
  }

  void Publish() {
    if (published_ || !run_root_fd_.valid()) {
      throw std::logic_error("replay run root was published more than once");
    }
    if (syscall(SYS_renameat2, parent_fd_.get(), staging_name_.c_str(),
                parent_fd_.get(), run_id_.c_str(), RENAME_NOREPLACE) != 0) {
      const int publish_error = errno;
      if (publish_error == EEXIST) {
        throw McpOperationFailure(
            "run_replay_destination_exists",
            "the replay destination run directory already exists", true);
      }
      throw std::system_error(publish_error, std::generic_category(),
                              "publish reserved replay destination");
    }
    published_ = true;
  }

  void SetOwnership(RunOwnership ownership) {
    ownership_ = std::move(ownership);
  }

  void MarkMarkerCreated(RunOwnershipMarkerIdentity marker) {
    marker_identity_ = OwnedRunRootIdentity{
        .device = marker.device,
        .inode = marker.inode,
    };
  }

  [[nodiscard]] const RunOwnership& ownership() const {
    if (!ownership_) {
      throw std::logic_error("reserved run root has no ownership identity");
    }
    return *ownership_;
  }

  [[nodiscard]] int parent_descriptor() const { return parent_fd_.get(); }
  [[nodiscard]] int descriptor() const {
    if (!run_root_fd_.valid()) {
      throw std::logic_error("replay reservation root is not open");
    }
    return run_root_fd_.get();
  }
  [[nodiscard]] const std::string& staging_name() const {
    return staging_name_;
  }
  [[nodiscard]] OwnedRunRootIdentity root_identity() const {
    return root_identity_;
  }

  void Adopt() { adopted_.store(true, std::memory_order_release); }

 private:
  std::filesystem::path run_root_;
  std::string run_id_;
  std::string staging_name_;
  UniqueFileDescriptor parent_fd_;
  UniqueFileDescriptor run_root_fd_;
  OwnedRunRootIdentity root_identity_;
  std::optional<RunOwnership> ownership_;
  std::optional<OwnedRunRootIdentity> marker_identity_;
  bool published_ = false;
  std::atomic<bool> adopted_{false};
};

ReservedManagedRunRoot::ReservedManagedRunRoot(
    std::unique_ptr<State> state) noexcept
    : state_(std::move(state)) {}

ReservedManagedRunRoot::~ReservedManagedRunRoot() = default;

ReservedManagedRunRoot::ReservedManagedRunRoot(
    ReservedManagedRunRoot&&) noexcept = default;

const RunOwnership& ReservedManagedRunRoot::ownership() const {
  return state_->ownership();
}

int ReservedManagedRunRoot::descriptor() const { return state_->descriptor(); }

OwnedRunRootIdentity ReservedManagedRunRoot::root_identity() const {
  return state_->root_identity();
}

void ReservedManagedRunRoot::Adopt() { state_->Adopt(); }

std::filesystem::path BenchmarkRunRoot(const Options& options) {
  return std::filesystem::absolute(options.output_dir) / options.run_id;
}

std::shared_ptr<ReservedManagedRunRoot> ReserveManagedReplayRunRoot(
    const Options& options) {
  const std::filesystem::path run_root =
      BenchmarkRunRoot(options).lexically_normal();
  const std::filesystem::path benchmark_root = run_root.parent_path();
  try {
    auto reservation = std::make_unique<ReservedManagedRunRoot::State>();
    UniqueFileDescriptor parent_fd(
        open(benchmark_root.c_str(),
             O_RDONLY | O_DIRECTORY | O_CLOEXEC | O_NOFOLLOW | O_NONBLOCK));
    if (!parent_fd.valid()) {
      throw std::system_error(errno, std::generic_category(),
                              "open replay benchmark root");
    }
    struct stat opened_parent{};
    struct stat linked_parent{};
    if (fstat(parent_fd.get(), &opened_parent) != 0 ||
        fstatat(AT_FDCWD, benchmark_root.c_str(), &linked_parent,
                AT_SYMLINK_NOFOLLOW) != 0) {
      throw std::system_error(errno, std::generic_category(),
                              "inspect replay benchmark root");
    }
    if (!S_ISDIR(opened_parent.st_mode) || !S_ISDIR(linked_parent.st_mode) ||
        opened_parent.st_dev != linked_parent.st_dev ||
        opened_parent.st_ino != linked_parent.st_ino) {
      throw std::runtime_error(
          "replay benchmark root changed during destination reservation");
    }
    UniqueFileDescriptor descriptor_reserve(
        fcntl(parent_fd.get(), F_DUPFD_CLOEXEC, 0));
    if (!descriptor_reserve.valid()) {
      throw std::system_error(errno, std::generic_category(),
                              "reserve replay directory descriptor capacity");
    }
    std::filesystem::path reserved_run_root = run_root;
    std::string reserved_run_id = options.run_id;

    std::string staging_name;
    constexpr std::size_t kMaximumStagingNameAttempts = 32U;
    for (std::size_t attempt = 0U; attempt < kMaximumStagingNameAttempts;
         ++attempt) {
      staging_name = MakeReplayReservationName();
      if (mkdirat(parent_fd.get(), staging_name.c_str(), 0700) == 0) {
        break;
      }
      const int create_error = errno;
      if (create_error != EEXIST) {
        throw std::system_error(create_error, std::generic_category(),
                                "create private replay reservation");
      }
      staging_name.clear();
    }
    if (staging_name.empty()) {
      throw std::runtime_error(
          "could not allocate a private replay reservation name");
    }

    struct stat created_root{};
    if (fstatat(parent_fd.get(), staging_name.c_str(), &created_root,
                AT_SYMLINK_NOFOLLOW) != 0) {
      const int inspect_error = errno;
      throw std::system_error(inspect_error, std::generic_category(),
                              "inspect created replay reservation");
    }
    reservation->Initialize(
        reserved_run_root, reserved_run_id, staging_name, std::move(parent_fd),
        OwnedRunRootIdentity{
            .device = static_cast<std::uintmax_t>(created_root.st_dev),
            .inode = static_cast<std::uintmax_t>(created_root.st_ino),
        });

    descriptor_reserve = UniqueFileDescriptor();
    int opened_descriptor = -1;
    do {
      opened_descriptor = openat(
          reservation->parent_descriptor(), reservation->staging_name().c_str(),
          O_RDONLY | O_DIRECTORY | O_CLOEXEC | O_NOFOLLOW | O_NONBLOCK);
    } while (opened_descriptor < 0 && errno == EINTR);
    UniqueFileDescriptor run_root_fd(opened_descriptor);
    if (!run_root_fd.valid()) {
      throw std::system_error(errno, std::generic_category(),
                              "open private replay reservation");
    }
    reservation->BindOpenedRoot(std::move(run_root_fd));

    struct stat linked_root{};
    struct stat final_linked_parent{};
    if (fstatat(reservation->parent_descriptor(),
                reservation->staging_name().c_str(), &linked_root,
                AT_SYMLINK_NOFOLLOW) != 0 ||
        fstatat(AT_FDCWD, benchmark_root.c_str(), &final_linked_parent,
                AT_SYMLINK_NOFOLLOW) != 0) {
      throw std::system_error(errno, std::generic_category(),
                              "verify private replay reservation");
    }
    if (!S_ISDIR(created_root.st_mode) || !S_ISDIR(linked_root.st_mode) ||
        created_root.st_uid != geteuid() ||
        created_root.st_dev != linked_root.st_dev ||
        created_root.st_ino != linked_root.st_ino ||
        created_root.st_dev != opened_parent.st_dev ||
        opened_parent.st_dev != final_linked_parent.st_dev ||
        opened_parent.st_ino != final_linked_parent.st_ino) {
      throw std::runtime_error(
          "private replay reservation failed identity verification");
    }

    reservation->Publish();
    struct stat published_root{};
    struct stat published_path{};
    struct stat published_parent{};
    if (fstatat(reservation->parent_descriptor(), options.run_id.c_str(),
                &published_root, AT_SYMLINK_NOFOLLOW) != 0 ||
        fstatat(AT_FDCWD, run_root.c_str(), &published_path,
                AT_SYMLINK_NOFOLLOW) != 0 ||
        fstatat(AT_FDCWD, benchmark_root.c_str(), &published_parent,
                AT_SYMLINK_NOFOLLOW) != 0) {
      throw std::system_error(errno, std::generic_category(),
                              "verify published replay destination");
    }
    if (!S_ISDIR(published_root.st_mode) || !S_ISDIR(published_path.st_mode) ||
        published_root.st_dev != created_root.st_dev ||
        published_root.st_ino != created_root.st_ino ||
        published_path.st_dev != created_root.st_dev ||
        published_path.st_ino != created_root.st_ino ||
        published_parent.st_dev != opened_parent.st_dev ||
        published_parent.st_ino != opened_parent.st_ino) {
      throw std::runtime_error(
          "published replay destination failed identity verification");
    }

    const RunOwnership ownership = CreateRunOwnershipAt(
        options.run_id, run_root, reservation->descriptor());
    reservation->SetOwnership(ownership);
    reservation->MarkMarkerCreated(
        WriteRunOwnershipMarkerAt(ownership, reservation->descriptor()));
    if (LoadRunOwnershipAt(options.run_id, run_root,
                           reservation->descriptor()) != ownership) {
      throw std::runtime_error(
          "reserved replay destination ownership did not read back");
    }
    return std::shared_ptr<ReservedManagedRunRoot>(
        new ReservedManagedRunRoot(std::move(reservation)));
  } catch (const McpOperationFailure&) {
    throw;
  } catch (...) {
    throw McpOperationFailure("run_replay_destination_unavailable",
                              "the replay destination could not be reserved: " +
                                  ExceptionMessage(std::current_exception()),
                              true);
  }
}

void PrepareManagedRunRoot(
    Options* options, ManagedRunNodeVethConfigFactory make_node_veth_config,
    const std::shared_ptr<ReservedManagedRunRoot>& reservation) {
  const std::filesystem::path run_root = BenchmarkRunRoot(*options);
  if (reservation) {
    const RunOwnership& ownership = reservation->ownership();
    if (ownership.run_id != options->run_id ||
        ownership.run_root != run_root.lexically_normal()) {
      throw std::runtime_error(
          "reserved replay destination does not match the prepared run");
    }
    options->run_ownership = ownership;
    if (LoadRunOwnershipAt(options->run_id, ownership.run_root,
                           reservation->descriptor()) != ownership) {
      throw std::runtime_error(
          "reserved replay destination ownership changed before preparation");
    }
    AttachRunLogFileAt(run_root, reservation->descriptor());
    BBP_LOG(info) << "starting run " << options->run_id;
    CreateReservedRunDirectoryAt(reservation->descriptor(), "nodes");
    if (LoadRunOwnershipAt(options->run_id, ownership.run_root,
                           reservation->descriptor()) != ownership) {
      throw std::runtime_error(
          "reserved replay destination identity changed during preparation");
    }
    return;
  }
  if (std::filesystem::exists(run_root)) {
    if (!options->replace_run) {
      throw std::runtime_error(
          "run directory already exists: " + run_root.string() +
          " (use --replace-run to remove it)");
    }
    const RunOwnership previous_ownership =
        LoadRunOwnership(options->run_id, run_root);
    const std::optional<RuntimeNodeResourceManifest> previous_manifest =
        TryLoadRuntimeNodeResourceManifest(previous_ownership);
    const auto cleanup_deadline =
        std::chrono::steady_clock::now() + std::chrono::seconds(30);
    Cgroup::RemoveStaleRun(previous_ownership);
    if (previous_manifest) {
      Options cleanup_options = *options;
      cleanup_options.run_ownership = previous_ownership;
      if (previous_manifest->isolated_network) {
        if (make_node_veth_config == nullptr) {
          throw std::logic_error(
              "managed run-root preparation has no veth configuration "
              "factory");
        }
        for (const RuntimeNodeResourceEntry& entry : previous_manifest->nodes) {
          DeleteNodeVethNetwork(
              make_node_veth_config(cleanup_options, entry.slot));
        }
      }
      for (const RuntimeNodeResourceEntry& entry : previous_manifest->nodes) {
        CleanupRuntimeNodeRpcCredential(previous_ownership, entry);
      }
      for (const RuntimeNodeResourceEntry& entry : previous_manifest->nodes) {
        RemoveRuntimeNodeRoot(previous_ownership, entry, cleanup_deadline);
      }
    }
    RemoveOwnedRunRoot(previous_ownership, cleanup_deadline);
  }
  EnsureDirectory(run_root);
  options->run_ownership = CreateRunOwnership(options->run_id, run_root);
  WriteRunOwnershipMarker(RequireRunOwnership(*options));
  AttachRunLogFile(run_root);
  BBP_LOG(info) << "starting run " << options->run_id;
  EnsureDirectory(run_root / "nodes");
}

void RemovePreparedRunRoot(
    const Options& options,
    const std::shared_ptr<ReservedManagedRunRoot>& reservation) {
  const RunOwnership& expected = RequireRunOwnership(options);
  const RunOwnership loaded =
      reservation ? LoadRunOwnershipAt(options.run_id, expected.run_root,
                                       reservation->descriptor())
                  : LoadRunOwnership(options.run_id, expected.run_root);
  if (loaded != expected) {
    throw std::runtime_error(
        "refusing to remove a prepared run with changed ownership: " +
        expected.run_root.string());
  }
  const auto cleanup_deadline =
      std::chrono::steady_clock::now() + std::chrono::seconds(30);
  DetachRunLogFile(BenchmarkRunRoot(options), cleanup_deadline);
  RemoveOwnedRunRoot(expected, cleanup_deadline, {},
                     reservation ? std::optional<OwnedRunRootIdentity>(
                                       reservation->root_identity())
                                 : std::nullopt);
  if (std::filesystem::exists(expected.run_root)) {
    throw std::runtime_error("prepared run directory survived cleanup: " +
                             expected.run_root.string());
  }
}

}  // namespace bbp::simulator_app_internal
