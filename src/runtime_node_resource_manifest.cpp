#include "bbp/runtime_node_resource_manifest.h"

#include <dirent.h>
#include <fcntl.h>
#include <linux/fs.h>
#include <linux/stat.h>
#include <sys/ioctl.h>
#include <sys/stat.h>
#include <sys/syscall.h>
#include <unistd.h>

#include <algorithm>
#include <array>
#include <boost/json/array.hpp>
#include <boost/json/object.hpp>
#include <boost/json/parse.hpp>
#include <boost/json/serialize.hpp>
#include <cerrno>
#include <cstring>
#include <filesystem>
#include <limits>
#include <map>
#include <memory>
#include <set>
#include <stdexcept>
#include <string>
#include <string_view>
#include <system_error>
#include <utility>

#include "bbp/drivers/chain_driver_registry.h"
#include "bbp/simulator/constants.h"

namespace bbp {
namespace {

constexpr std::string_view kManifestName = "runtime-node-resources.json";
constexpr std::string_view kTemporaryManifestName =
    ".runtime-node-resources.json.tmp";
constexpr std::string_view kNodeMarkerName = ".bbp-node";
constexpr std::uint64_t kManifestVersion = 2U;
constexpr std::uint64_t kNodeMarkerVersion = 1U;
constexpr std::size_t kMaximumManifestBytes = 1024U * 1024U;
constexpr std::size_t kMaximumRuntimeNodeSlots = 16U;
constexpr std::size_t kMaximumManifestEntries = kMaximumRuntimeNodeSlots + 1U;
constexpr std::size_t kMaximumTraversalDepth = 64U;
constexpr std::size_t kMaximumTraversalEntries = 1000000U;

class UniqueFd {
 public:
  explicit UniqueFd(int fd = -1) : fd_(fd) {}
  ~UniqueFd() {
    if (fd_ >= 0) {
      static_cast<void>(close(fd_));
    }
  }

  UniqueFd(const UniqueFd&) = delete;
  UniqueFd& operator=(const UniqueFd&) = delete;
  UniqueFd(UniqueFd&& other) noexcept : fd_(std::exchange(other.fd_, -1)) {}
  UniqueFd& operator=(UniqueFd&& other) noexcept {
    if (this != &other) {
      if (fd_ >= 0) {
        static_cast<void>(close(fd_));
      }
      fd_ = std::exchange(other.fd_, -1);
    }
    return *this;
  }

  [[nodiscard]] int get() const { return fd_; }
  [[nodiscard]] bool valid() const { return fd_ >= 0; }

 private:
  int fd_ = -1;
};

struct DirectoryCloser {
  void operator()(DIR* directory) const {
    if (directory != nullptr) {
      static_cast<void>(closedir(directory));
    }
  }
};

using DirectoryPointer = std::unique_ptr<DIR, DirectoryCloser>;

[[noreturn]] void ThrowErrno(std::string_view operation, int error) {
  throw std::runtime_error(
      std::string(operation) +
      " failed: " + std::error_code(error, std::generic_category()).message());
}

void RequireSafeNodeId(std::string_view node_id) {
  if (node_id.empty() || node_id.size() > 32U) {
    throw std::runtime_error(
        "runtime resource node id must be 1..32 characters");
  }
  for (const char character : node_id) {
    const bool safe = (character >= 'a' && character <= 'z') ||
                      (character >= 'A' && character <= 'Z') ||
                      (character >= '0' && character <= '9') ||
                      character == '-' || character == '_';
    if (!safe) {
      throw std::runtime_error(
          "runtime resource node id contains an unsafe character");
    }
  }
}

std::string_view EntryRootName(const RuntimeNodeResourceEntry& entry) {
  return entry.root_name ? std::string_view(*entry.root_name)
                         : std::string_view(entry.node_id);
}

bool IsReplacementState(RuntimeNodeResourceState state) {
  return state == RuntimeNodeResourceState::kPendingReplace ||
         state == RuntimeNodeResourceState::kPendingReplaceExchanged ||
         state == RuntimeNodeResourceState::kPendingReplaceCommitted ||
         state == RuntimeNodeResourceState::kPendingReplaceUncertain;
}

std::string_view StateName(RuntimeNodeResourceState state) {
  switch (state) {
    case RuntimeNodeResourceState::kPendingAdd:
      return "pending_add";
    case RuntimeNodeResourceState::kPendingReplace:
      return "pending_replace";
    case RuntimeNodeResourceState::kPendingReplaceExchanged:
      return "pending_replace_exchanged";
    case RuntimeNodeResourceState::kPendingReplaceCommitted:
      return "pending_replace_committed";
    case RuntimeNodeResourceState::kPendingReplaceUncertain:
      return "pending_replace_uncertain";
    case RuntimeNodeResourceState::kPendingRemove:
      return "pending_remove";
    case RuntimeNodeResourceState::kLive:
      return "live";
  }
  throw std::logic_error("unknown runtime node resource state");
}

RuntimeNodeResourceState ParseState(std::string_view state) {
  if (state == "pending_add") {
    return RuntimeNodeResourceState::kPendingAdd;
  }
  if (state == "pending_replace") {
    return RuntimeNodeResourceState::kPendingReplace;
  }
  if (state == "pending_replace_exchanged") {
    return RuntimeNodeResourceState::kPendingReplaceExchanged;
  }
  if (state == "pending_replace_committed") {
    return RuntimeNodeResourceState::kPendingReplaceCommitted;
  }
  if (state == "pending_replace_uncertain") {
    return RuntimeNodeResourceState::kPendingReplaceUncertain;
  }
  if (state == "pending_remove") {
    return RuntimeNodeResourceState::kPendingRemove;
  }
  if (state == "live") {
    return RuntimeNodeResourceState::kLive;
  }
  throw std::runtime_error("runtime resource manifest has unknown node state");
}

void RequireRelativeDataDirectory(const RuntimeNodeResourceEntry& entry) {
  if (entry.data_dir.empty() || entry.data_dir.is_absolute() ||
      entry.data_dir.has_root_path() ||
      entry.data_dir.lexically_normal() != entry.data_dir) {
    throw std::runtime_error(
        "runtime resource data directory must be a normalized relative path");
  }
  const std::filesystem::path expected =
      std::filesystem::path("nodes") / EntryRootName(entry);
  auto actual = entry.data_dir.begin();
  for (auto component = expected.begin(); component != expected.end();
       ++component, ++actual) {
    if (actual == entry.data_dir.end() || *actual != *component) {
      throw std::runtime_error(
          "runtime resource data directory must stay under its node root");
    }
  }
  if (actual == entry.data_dir.end()) {
    throw std::runtime_error(
        "runtime resource data directory must be below its node root");
  }
  for (const std::filesystem::path& component : entry.data_dir) {
    if (component.empty() || component == "." || component == "..") {
      throw std::runtime_error(
          "runtime resource data directory has an unsafe component");
    }
  }
}

void RequireEntry(const RuntimeNodeResourceEntry& entry) {
  RequireSafeNodeId(entry.node_id);
  RequireSafeNodeId(EntryRootName(entry));
  if (entry.root_name && *entry.root_name == entry.node_id) {
    throw std::runtime_error(
        "runtime resource root_name must differ from node_id");
  }
  const bool replacement = IsReplacementState(entry.state);
  if (replacement && !entry.root_name) {
    throw std::runtime_error(
        "pending replacement resource requires a distinct root_name");
  }
  if (!replacement && entry.root_name) {
    throw std::runtime_error(
        "only pending replacement resources may select root_name");
  }
  RequireRelativeDataDirectory(entry);
  if (entry.slot >= kMaximumRuntimeNodeSlots) {
    throw std::runtime_error(
        "runtime resource slot exceeds the manifest bound");
  }
  if (entry.chain == ChainKind::kCount) {
    throw std::runtime_error("runtime resource chain is invalid");
  }
}

void RequireManifest(const RuntimeNodeResourceManifest& manifest) {
  if (LoadRunOwnership(manifest.ownership.run_id,
                       manifest.ownership.run_root) != manifest.ownership) {
    throw std::runtime_error(
        "runtime resource manifest ownership no longer matches its run");
  }
  if (manifest.nodes.size() > kMaximumManifestEntries) {
    throw std::runtime_error(
        "runtime resource manifest node count is too large");
  }
  std::set<std::string> root_names;
  std::map<std::string, const RuntimeNodeResourceEntry*, std::less<>>
      live_by_node_id;
  std::map<std::uint32_t, const RuntimeNodeResourceEntry*> live_by_slot;
  std::vector<const RuntimeNodeResourceEntry*> replacements;
  for (const RuntimeNodeResourceEntry& entry : manifest.nodes) {
    RequireEntry(entry);
    if (!root_names.insert(std::string(EntryRootName(entry))).second) {
      throw std::runtime_error(
          "runtime resource manifest contains a duplicate root name");
    }
    if (IsReplacementState(entry.state)) {
      replacements.push_back(&entry);
      continue;
    }
    if (!live_by_node_id.emplace(entry.node_id, &entry).second) {
      throw std::runtime_error(
          "runtime resource manifest contains a duplicate node id");
    }
    if (!live_by_slot.emplace(entry.slot, &entry).second) {
      throw std::runtime_error(
          "runtime resource manifest contains a duplicate resource slot");
    }
  }
  if (replacements.size() > 1U) {
    throw std::runtime_error(
        "runtime resource manifest contains multiple pending replacements");
  }
  for (const RuntimeNodeResourceEntry* replacement : replacements) {
    const auto node = live_by_node_id.find(replacement->node_id);
    const auto slot = live_by_slot.find(replacement->slot);
    if (node == live_by_node_id.end() || slot == live_by_slot.end() ||
        node->second != slot->second ||
        node->second->state != RuntimeNodeResourceState::kLive ||
        node->second->chain != replacement->chain) {
      throw std::runtime_error(
          "runtime resource manifest replacement has no exact live "
          "counterpart");
    }
  }
}

bool SameIdentity(const struct stat& first, const struct stat& second);
std::uint64_t MountId(int descriptor);
std::uint64_t MountIdAt(int parent, std::string_view name);
std::string ReadBoundedFileAt(int parent, std::string_view name,
                              std::size_t maximum);
std::uint64_t RequiredUnsigned(const boost::json::object& object,
                               std::string_view field);
std::string RequiredString(const boost::json::object& object,
                           std::string_view field);
void RejectUnknownFields(const boost::json::object& object,
                         const std::set<std::string_view>& fields,
                         std::string_view description);

UniqueFd OpenOwnedRunRoot(const RunOwnership& ownership) {
  if (LoadRunOwnership(ownership.run_id, ownership.run_root) != ownership) {
    throw std::runtime_error("run ownership changed before resource access");
  }
  UniqueFd run_root(open(ownership.run_root.c_str(),
                         O_RDONLY | O_DIRECTORY | O_CLOEXEC | O_NOFOLLOW));
  if (!run_root.valid()) {
    ThrowErrno("open owned run root", errno);
  }
  struct stat opened_status{};
  if (fstat(run_root.get(), &opened_status) != 0) {
    ThrowErrno("inspect owned run root", errno);
  }
  if (!S_ISDIR(opened_status.st_mode) || opened_status.st_uid != geteuid()) {
    throw std::runtime_error(
        "owned run root is not an effective-user-owned directory");
  }
  struct stat path_status{};
  if (fstatat(AT_FDCWD, ownership.run_root.c_str(), &path_status,
              AT_SYMLINK_NOFOLLOW) != 0) {
    ThrowErrno("reinspect owned run root path", errno);
  }
  if (!SameIdentity(opened_status, path_status)) {
    throw std::runtime_error(
        "owned run root identity changed while it was opened");
  }
  if (LoadRunOwnership(ownership.run_id, ownership.run_root) != ownership) {
    throw std::runtime_error("run ownership changed during resource access");
  }
  const boost::json::value marker = boost::json::parse(
      ReadBoundedFileAt(run_root.get(), kRunMarkerFile, 4096U));
  if (!marker.is_object()) {
    throw std::runtime_error("opened run ownership marker is not an object");
  }
  const boost::json::object& object = marker.as_object();
  RejectUnknownFields(object, {"version", "run_id", "run_root", "resource_id"},
                      "opened run ownership marker");
  if (RequiredUnsigned(object, "version") != 1U ||
      RequiredString(object, "run_id") != ownership.run_id ||
      std::filesystem::path(RequiredString(object, "run_root")) !=
          ownership.run_root ||
      RequiredString(object, "resource_id") != ownership.resource_id) {
    throw std::runtime_error(
        "opened run ownership marker does not match the requested run");
  }
  return run_root;
}

UniqueFd OpenNodesDirectory(const RunOwnership& ownership) {
  UniqueFd run_root = OpenOwnedRunRoot(ownership);
  UniqueFd nodes(openat(run_root.get(), "nodes",
                        O_RDONLY | O_DIRECTORY | O_CLOEXEC | O_NOFOLLOW));
  if (!nodes.valid()) {
    ThrowErrno("open owned runtime nodes directory", errno);
  }
  struct stat status{};
  if (fstat(nodes.get(), &status) != 0) {
    ThrowErrno("inspect owned runtime nodes directory", errno);
  }
  if (!S_ISDIR(status.st_mode) || status.st_uid != geteuid()) {
    throw std::runtime_error(
        "runtime nodes path is not an effective-user-owned directory");
  }
  if (MountId(nodes.get()) != MountId(run_root.get())) {
    throw std::runtime_error(
        "runtime nodes path crosses an owned run mount boundary");
  }
  return nodes;
}

void WriteAll(int descriptor, std::string_view text,
              std::string_view description) {
  while (!text.empty()) {
    const ssize_t written = write(descriptor, text.data(), text.size());
    if (written < 0) {
      if (errno == EINTR) {
        continue;
      }
      ThrowErrno(description, errno);
    }
    if (written == 0) {
      throw std::runtime_error(std::string(description) + " made no progress");
    }
    text.remove_prefix(static_cast<std::size_t>(written));
  }
}

std::string ReadBoundedFileAt(int parent, std::string_view name,
                              std::size_t maximum) {
  const std::string filename(name);
  UniqueFd file(
      openat(parent, filename.c_str(), O_RDONLY | O_CLOEXEC | O_NOFOLLOW));
  if (!file.valid()) {
    ThrowErrno("open runtime ownership file", errno);
  }
  struct stat status{};
  if (fstat(file.get(), &status) != 0) {
    ThrowErrno("inspect runtime ownership file", errno);
  }
  if (!S_ISREG(status.st_mode) || status.st_uid != geteuid() ||
      status.st_size < 0 ||
      static_cast<std::uint64_t>(status.st_size) > maximum ||
      (status.st_mode & (S_IRWXG | S_IRWXO)) != 0) {
    throw std::runtime_error(
        "runtime ownership file is not a bounded owned regular file");
  }
  std::string contents(static_cast<std::size_t>(status.st_size), '\0');
  std::size_t offset = 0U;
  while (offset < contents.size()) {
    const ssize_t count =
        read(file.get(), contents.data() + offset, contents.size() - offset);
    if (count < 0) {
      if (errno == EINTR) {
        continue;
      }
      ThrowErrno("read runtime ownership file", errno);
    }
    if (count == 0) {
      throw std::runtime_error(
          "runtime ownership file changed while it was read");
    }
    offset += static_cast<std::size_t>(count);
  }
  char extra = '\0';
  while (true) {
    const ssize_t count = read(file.get(), &extra, 1U);
    if (count < 0 && errno == EINTR) {
      continue;
    }
    if (count < 0) {
      ThrowErrno("verify runtime ownership file length", errno);
    }
    if (count != 0) {
      throw std::runtime_error("runtime ownership file grew while it was read");
    }
    break;
  }
  return contents;
}

std::uint64_t RequiredUnsigned(const boost::json::object& object,
                               std::string_view field) {
  const boost::json::value* value = object.if_contains(field);
  if (value == nullptr) {
    throw std::runtime_error("runtime resource manifest is missing field: " +
                             std::string(field));
  }
  if (value->is_uint64()) {
    return value->as_uint64();
  }
  if (value->is_int64() && value->as_int64() >= 0) {
    return static_cast<std::uint64_t>(value->as_int64());
  }
  throw std::runtime_error("runtime resource manifest field is not unsigned: " +
                           std::string(field));
}

std::string RequiredString(const boost::json::object& object,
                           std::string_view field) {
  const boost::json::value* value = object.if_contains(field);
  if (value == nullptr || !value->is_string()) {
    throw std::runtime_error(
        "runtime resource manifest field is not a string: " +
        std::string(field));
  }
  return std::string(value->as_string());
}

void RejectUnknownFields(const boost::json::object& object,
                         const std::set<std::string_view>& fields,
                         std::string_view description) {
  for (const auto& member : object) {
    const std::string_view field(member.key().data(), member.key().size());
    if (!fields.contains(field)) {
      throw std::runtime_error(std::string(description) +
                               " has unsupported field: " + std::string(field));
    }
  }
}

boost::json::object NodeMarker(const RunOwnership& ownership,
                               const RuntimeNodeResourceEntry& entry) {
  return boost::json::object{
      {"version", kNodeMarkerVersion},
      {"run_id", ownership.run_id},
      {"resource_id", ownership.resource_id},
      {"node_id", entry.node_id},
      {"slot", entry.slot},
  };
}

void RequireNodeMarker(int node_directory, const RunOwnership& ownership,
                       const RuntimeNodeResourceEntry& entry) {
  const boost::json::value parsed = boost::json::parse(
      ReadBoundedFileAt(node_directory, kNodeMarkerName, 4096U));
  if (!parsed.is_object()) {
    throw std::runtime_error("runtime node ownership marker is not an object");
  }
  const boost::json::object& marker = parsed.as_object();
  RejectUnknownFields(marker,
                      {"version", "run_id", "resource_id", "node_id", "slot"},
                      "runtime node ownership marker");
  if (RequiredUnsigned(marker, "version") != kNodeMarkerVersion ||
      RequiredString(marker, "run_id") != ownership.run_id ||
      RequiredString(marker, "resource_id") != ownership.resource_id ||
      RequiredString(marker, "node_id") != entry.node_id ||
      RequiredUnsigned(marker, "slot") != entry.slot) {
    throw std::runtime_error(
        "runtime node ownership marker does not match the requested resource");
  }
}

bool SameIdentity(const struct stat& first, const struct stat& second) {
  return first.st_dev == second.st_dev && first.st_ino == second.st_ino;
}

std::uint64_t MountId(int descriptor) {
  struct statx status{};
  if (statx(descriptor, "", AT_EMPTY_PATH | AT_NO_AUTOMOUNT, STATX_MNT_ID,
            &status) != 0) {
    ThrowErrno("inspect runtime mount identity", errno);
  }
  if ((status.stx_mask & STATX_MNT_ID) == 0U) {
    throw std::runtime_error("kernel did not report a runtime mount identity");
  }
  return status.stx_mnt_id;
}

std::uint64_t MountIdAt(int parent, std::string_view name) {
  const std::string filename(name);
  struct statx status{};
  if (statx(parent, filename.c_str(), AT_SYMLINK_NOFOLLOW | AT_NO_AUTOMOUNT,
            STATX_MNT_ID, &status) != 0) {
    ThrowErrno("inspect runtime path mount identity", errno);
  }
  if ((status.stx_mask & STATX_MNT_ID) == 0U) {
    throw std::runtime_error(
        "kernel did not report a runtime path mount identity");
  }
  return status.stx_mnt_id;
}

UniqueFd OpenOwnedNodeRootNamed(int nodes, std::string_view root_name,
                                const RunOwnership& ownership,
                                const RuntimeNodeResourceEntry& entry,
                                struct stat* opened_status = nullptr) {
  const std::string name(root_name);
  struct stat before{};
  if (fstatat(nodes, name.c_str(), &before, AT_SYMLINK_NOFOLLOW) != 0) {
    ThrowErrno("inspect runtime node root", errno);
  }
  if (!S_ISDIR(before.st_mode)) {
    throw std::runtime_error("runtime node root is not a directory");
  }
  UniqueFd node(openat(nodes, name.c_str(),
                       O_RDONLY | O_DIRECTORY | O_CLOEXEC | O_NOFOLLOW));
  if (!node.valid()) {
    ThrowErrno("open runtime node root", errno);
  }
  struct stat opened{};
  if (fstat(node.get(), &opened) != 0) {
    ThrowErrno("inspect opened runtime node root", errno);
  }
  if (!SameIdentity(before, opened) || opened.st_uid != geteuid() ||
      MountId(node.get()) != MountId(nodes) ||
      MountIdAt(nodes, name) != MountId(node.get())) {
    throw std::runtime_error(
        "runtime node root ownership, identity, or mount changed while it "
        "was opened");
  }
  RequireNodeMarker(node.get(), ownership, entry);
  if (opened_status != nullptr) {
    *opened_status = opened;
  }
  return node;
}

UniqueFd OpenOwnedNodeRoot(int nodes, const RunOwnership& ownership,
                           const RuntimeNodeResourceEntry& entry,
                           struct stat* opened_status = nullptr) {
  return OpenOwnedNodeRootNamed(nodes, EntryRootName(entry), ownership, entry,
                                opened_status);
}

void WriteNodeMarker(int node_directory, const RunOwnership& ownership,
                     const RuntimeNodeResourceEntry& entry) {
  const std::string marker_name(kNodeMarkerName);
  UniqueFd marker(openat(node_directory, marker_name.c_str(),
                         O_WRONLY | O_CREAT | O_EXCL | O_CLOEXEC | O_NOFOLLOW,
                         S_IRUSR | S_IWUSR));
  if (!marker.valid()) {
    ThrowErrno("create runtime node ownership marker", errno);
  }
  const std::string contents =
      boost::json::serialize(NodeMarker(ownership, entry)) + "\n";
  WriteAll(marker.get(), contents, "write runtime node ownership marker");
  if (fsync(marker.get()) != 0 || fsync(node_directory) != 0) {
    ThrowErrno("sync runtime node ownership marker", errno);
  }
  RequireNodeMarker(node_directory, ownership, entry);
}

std::vector<std::string> DirectoryNames(
    int descriptor, std::size_t* visited_entries,
    std::optional<std::chrono::steady_clock::time_point> absolute_deadline,
    std::stop_token stop_token) {
  const int duplicate = fcntl(descriptor, F_DUPFD_CLOEXEC, 0);
  if (duplicate < 0) {
    ThrowErrno("duplicate runtime node directory", errno);
  }
  DirectoryPointer directory(fdopendir(duplicate));
  if (!directory) {
    const int error = errno;
    static_cast<void>(close(duplicate));
    ThrowErrno("open runtime node directory stream", error);
  }
  std::vector<std::string> names;
  while (true) {
    if (stop_token.stop_requested()) {
      throw std::runtime_error("runtime node cleanup was cancelled");
    }
    if (absolute_deadline &&
        std::chrono::steady_clock::now() >= *absolute_deadline) {
      throw std::runtime_error("runtime node cleanup deadline expired");
    }
    errno = 0;
    dirent* entry = readdir(directory.get());
    if (entry == nullptr) {
      if (errno != 0) {
        ThrowErrno("read runtime node directory", errno);
      }
      break;
    }
    const std::string_view name(entry->d_name);
    if (name == "." || name == "..") {
      continue;
    }
    if (*visited_entries >= kMaximumTraversalEntries) {
      throw std::runtime_error(
          "runtime node cleanup exceeded its directory-entry bound");
    }
    ++*visited_entries;
    names.emplace_back(name);
  }
  std::sort(names.begin(), names.end());
  return names;
}

void RemoveDirectoryContents(
    int directory, std::size_t depth, std::size_t* visited_entries,
    std::optional<std::string_view> preserved_entry,
    std::optional<std::chrono::steady_clock::time_point> absolute_deadline,
    std::stop_token stop_token) {
  if (stop_token.stop_requested()) {
    throw std::runtime_error("runtime node cleanup was cancelled");
  }
  if (absolute_deadline &&
      std::chrono::steady_clock::now() >= *absolute_deadline) {
    throw std::runtime_error("runtime node cleanup deadline expired");
  }
  if (depth > kMaximumTraversalDepth) {
    throw std::runtime_error(
        "runtime node cleanup exceeded its directory-depth bound");
  }
  for (const std::string& name : DirectoryNames(
           directory, visited_entries, absolute_deadline, stop_token)) {
    if (preserved_entry && name == *preserved_entry) {
      continue;
    }
    struct stat before{};
    if (fstatat(directory, name.c_str(), &before, AT_SYMLINK_NOFOLLOW) != 0) {
      ThrowErrno("inspect runtime node cleanup entry", errno);
    }
    if (!S_ISDIR(before.st_mode)) {
      if (unlinkat(directory, name.c_str(), 0) != 0) {
        ThrowErrno("remove runtime node cleanup entry", errno);
      }
      continue;
    }

    UniqueFd child(openat(directory, name.c_str(),
                          O_RDONLY | O_DIRECTORY | O_CLOEXEC | O_NOFOLLOW));
    if (!child.valid()) {
      ThrowErrno("open runtime node cleanup directory", errno);
    }
    struct stat opened{};
    if (fstat(child.get(), &opened) != 0) {
      ThrowErrno("inspect opened runtime node cleanup directory", errno);
    }
    if (!SameIdentity(before, opened)) {
      throw std::runtime_error(
          "runtime node cleanup directory identity changed before traversal");
    }
    if (MountId(child.get()) != MountId(directory) ||
        MountIdAt(directory, name) != MountId(child.get())) {
      throw std::runtime_error(
          "runtime node cleanup refuses to cross a mount boundary");
    }
    RemoveDirectoryContents(child.get(), depth + 1U, visited_entries,
                            std::nullopt, absolute_deadline, stop_token);
    struct stat current{};
    if (fstatat(directory, name.c_str(), &current, AT_SYMLINK_NOFOLLOW) != 0) {
      ThrowErrno("reinspect runtime node cleanup directory", errno);
    }
    if (!SameIdentity(opened, current)) {
      throw std::runtime_error(
          "runtime node cleanup directory identity changed during traversal");
    }
    if (unlinkat(directory, name.c_str(), AT_REMOVEDIR) != 0) {
      ThrowErrno("remove runtime node cleanup directory", errno);
    }
  }
}

void RequireCloneTime(
    std::optional<std::chrono::steady_clock::time_point> absolute_deadline,
    std::stop_token stop_token) {
  if (stop_token.stop_requested()) {
    throw std::runtime_error("runtime node root clone was cancelled");
  }
  if (absolute_deadline &&
      std::chrono::steady_clock::now() >= *absolute_deadline) {
    throw std::runtime_error("runtime node root clone deadline expired");
  }
}

using CloneInode = std::pair<std::uint64_t, std::uint64_t>;

void CloneDirectoryContents(
    int source, int destination, int destination_root,
    const std::filesystem::path& relative, std::size_t depth,
    std::size_t* visited_entries,
    std::map<CloneInode, std::filesystem::path>* hard_links,
    std::optional<std::chrono::steady_clock::time_point> absolute_deadline,
    std::stop_token stop_token) {
  RequireCloneTime(absolute_deadline, stop_token);
  if (depth > kMaximumTraversalDepth) {
    throw std::runtime_error(
        "runtime node root clone exceeded its directory-depth bound");
  }
  for (const std::string& name :
       DirectoryNames(source, visited_entries, absolute_deadline, stop_token)) {
    if (depth == 0U && name == kNodeMarkerName) {
      continue;
    }
    RequireCloneTime(absolute_deadline, stop_token);
    struct stat before{};
    if (fstatat(source, name.c_str(), &before, AT_SYMLINK_NOFOLLOW) != 0) {
      ThrowErrno("inspect runtime node clone source", errno);
    }
    if (before.st_uid != geteuid() ||
        MountIdAt(source, name) != MountId(source)) {
      throw std::runtime_error(
          "runtime node clone source has unsafe ownership or mount identity");
    }
    const std::filesystem::path child_relative = relative / name;
    if (S_ISDIR(before.st_mode)) {
      const mode_t mode = static_cast<mode_t>(before.st_mode & 0777);
      if (mkdirat(destination, name.c_str(), mode) != 0) {
        ThrowErrno("create runtime node clone directory", errno);
      }
      UniqueFd source_child(
          openat(source, name.c_str(),
                 O_RDONLY | O_DIRECTORY | O_CLOEXEC | O_NOFOLLOW));
      UniqueFd destination_child(
          openat(destination, name.c_str(),
                 O_RDONLY | O_DIRECTORY | O_CLOEXEC | O_NOFOLLOW));
      if (!source_child.valid() || !destination_child.valid()) {
        ThrowErrno("open runtime node clone directory", errno);
      }
      struct stat opened{};
      if (fstat(source_child.get(), &opened) != 0) {
        ThrowErrno("inspect runtime node clone directory", errno);
      }
      if (!SameIdentity(before, opened) ||
          MountId(source_child.get()) != MountId(source)) {
        throw std::runtime_error(
            "runtime node clone directory identity changed");
      }
      CloneDirectoryContents(source_child.get(), destination_child.get(),
                             destination_root, child_relative, depth + 1U,
                             visited_entries, hard_links, absolute_deadline,
                             stop_token);
      if (fchmod(destination_child.get(), mode) != 0) {
        ThrowErrno("set runtime node clone directory mode", errno);
      }
      const std::array<timespec, 2U> times{before.st_atim, before.st_mtim};
      if (futimens(destination_child.get(), times.data()) != 0 ||
          fsync(destination_child.get()) != 0) {
        ThrowErrno("sync runtime node clone directory", errno);
      }
      continue;
    }
    if (S_ISREG(before.st_mode)) {
      const CloneInode inode{static_cast<std::uint64_t>(before.st_dev),
                             static_cast<std::uint64_t>(before.st_ino)};
      const auto existing = hard_links->find(inode);
      if (existing != hard_links->end()) {
        if (linkat(destination_root, existing->second.c_str(), destination,
                   name.c_str(), 0) != 0) {
          ThrowErrno("create runtime node clone hard link", errno);
        }
        continue;
      }
      UniqueFd source_file(
          openat(source, name.c_str(), O_RDONLY | O_CLOEXEC | O_NOFOLLOW));
      if (!source_file.valid()) {
        ThrowErrno("open runtime node clone source file", errno);
      }
      struct stat opened{};
      if (fstat(source_file.get(), &opened) != 0) {
        ThrowErrno("inspect runtime node clone source file", errno);
      }
      if (!SameIdentity(before, opened) || !S_ISREG(opened.st_mode) ||
          opened.st_uid != geteuid() ||
          MountId(source_file.get()) != MountId(source)) {
        throw std::runtime_error(
            "runtime node clone source file identity changed");
      }
      const mode_t mode = static_cast<mode_t>(before.st_mode & 0777);
      UniqueFd destination_file(
          openat(destination, name.c_str(),
                 O_WRONLY | O_CREAT | O_EXCL | O_CLOEXEC | O_NOFOLLOW, mode));
      if (!destination_file.valid()) {
        ThrowErrno("create runtime node clone file", errno);
      }
      if (ioctl(destination_file.get(), FICLONE, source_file.get()) != 0) {
        const int clone_error = errno;
        if (clone_error != EOPNOTSUPP && clone_error != ENOTTY &&
            clone_error != EXDEV && clone_error != EINVAL) {
          ThrowErrno("reflink runtime node clone file", clone_error);
        }
        if (ftruncate(destination_file.get(), 0) != 0 ||
            lseek(source_file.get(), 0, SEEK_SET) < 0) {
          ThrowErrno("prepare runtime node clone file copy", errno);
        }
        std::array<char, 1024U * 1024U> buffer{};
        while (true) {
          RequireCloneTime(absolute_deadline, stop_token);
          const ssize_t count =
              read(source_file.get(), buffer.data(), buffer.size());
          if (count < 0 && errno == EINTR) {
            continue;
          }
          if (count < 0) {
            ThrowErrno("read runtime node clone file", errno);
          }
          if (count == 0) {
            break;
          }
          WriteAll(
              destination_file.get(),
              std::string_view(buffer.data(), static_cast<std::size_t>(count)),
              "write runtime node clone file");
        }
      }
      if (fchmod(destination_file.get(), mode) != 0) {
        ThrowErrno("set runtime node clone file mode", errno);
      }
      const std::array<timespec, 2U> times{before.st_atim, before.st_mtim};
      if (futimens(destination_file.get(), times.data()) != 0 ||
          fsync(destination_file.get()) != 0) {
        ThrowErrno("sync runtime node clone file", errno);
      }
      struct stat copied{};
      if (fstat(destination_file.get(), &copied) != 0 ||
          copied.st_size != before.st_size) {
        throw std::runtime_error(
            "runtime node clone file size verification failed");
      }
      hard_links->emplace(inode, child_relative);
      continue;
    }
    if (S_ISLNK(before.st_mode)) {
      if (before.st_size < 0 || before.st_size > 4096) {
        throw std::runtime_error(
            "runtime node clone symlink target exceeds its bound");
      }
      std::string target(static_cast<std::size_t>(before.st_size) + 1U, '\0');
      const ssize_t count =
          readlinkat(source, name.c_str(), target.data(), target.size());
      if (count < 0) {
        ThrowErrno("read runtime node clone symlink", errno);
      }
      if (static_cast<std::size_t>(count) >= target.size()) {
        throw std::runtime_error(
            "runtime node clone symlink changed while it was read");
      }
      target.resize(static_cast<std::size_t>(count));
      struct stat after{};
      if (fstatat(source, name.c_str(), &after, AT_SYMLINK_NOFOLLOW) != 0) {
        ThrowErrno("reinspect runtime node clone symlink", errno);
      }
      if (!SameIdentity(before, after) || !S_ISLNK(after.st_mode) ||
          after.st_uid != geteuid() ||
          MountIdAt(source, name) != MountId(source)) {
        throw std::runtime_error("runtime node clone symlink identity changed");
      }
      if (symlinkat(target.c_str(), destination, name.c_str()) != 0) {
        ThrowErrno("create runtime node clone symlink", errno);
      }
      continue;
    }
    throw std::runtime_error(
        "runtime node clone refuses unsupported filesystem entry type");
  }
}

}  // namespace

void WriteRuntimeNodeResourceManifest(
    const RuntimeNodeResourceManifest& manifest) {
  RequireManifest(manifest);
  boost::json::array nodes;
  nodes.reserve(manifest.nodes.size());
  for (const RuntimeNodeResourceEntry& entry : manifest.nodes) {
    boost::json::object node{
        {"id", entry.node_id},
        {"slot", entry.slot},
        {"chain", std::string(ChainKindName(entry.chain))},
        {"data_dir", entry.data_dir.generic_string()},
        {"state", std::string(StateName(entry.state))},
    };
    if (entry.root_name) {
      node["root_name"] = *entry.root_name;
    }
    nodes.emplace_back(std::move(node));
  }
  const std::string contents =
      boost::json::serialize(boost::json::object{
          {"version", kManifestVersion},
          {"run_id", manifest.ownership.run_id},
          {"resource_id", manifest.ownership.resource_id},
          {"isolated_network", manifest.isolated_network},
          {"nodes", std::move(nodes)},
      }) +
      "\n";
  if (contents.size() > kMaximumManifestBytes) {
    throw std::runtime_error(
        "runtime resource manifest exceeds its size bound");
  }

  UniqueFd run_root = OpenOwnedRunRoot(manifest.ownership);
  const std::string temporary(kTemporaryManifestName);
  const std::string published(kManifestName);
  if (unlinkat(run_root.get(), temporary.c_str(), 0) != 0 && errno != ENOENT) {
    ThrowErrno("remove stale runtime resource manifest temporary", errno);
  }
  UniqueFd file(openat(run_root.get(), temporary.c_str(),
                       O_WRONLY | O_CREAT | O_EXCL | O_CLOEXEC | O_NOFOLLOW,
                       S_IRUSR | S_IWUSR));
  if (!file.valid()) {
    ThrowErrno("create runtime resource manifest temporary", errno);
  }
  bool published_file = false;
  try {
    WriteAll(file.get(), contents, "write runtime resource manifest");
    if (fsync(file.get()) != 0) {
      ThrowErrno("sync runtime resource manifest", errno);
    }
    struct stat temporary_status{};
    if (fstat(file.get(), &temporary_status) != 0) {
      ThrowErrno("inspect runtime resource manifest temporary", errno);
    }
    if (!S_ISREG(temporary_status.st_mode) ||
        temporary_status.st_uid != geteuid() ||
        (temporary_status.st_mode & (S_IRWXG | S_IRWXO)) != 0) {
      throw std::runtime_error(
          "runtime resource manifest temporary has unsafe ownership or mode");
    }
    if (renameat(run_root.get(), temporary.c_str(), run_root.get(),
                 published.c_str()) != 0) {
      ThrowErrno("publish runtime resource manifest", errno);
    }
    published_file = true;
    if (fsync(run_root.get()) != 0) {
      ThrowErrno("sync runtime resource manifest directory", errno);
    }
    struct stat published_status{};
    if (fstatat(run_root.get(), published.c_str(), &published_status,
                AT_SYMLINK_NOFOLLOW) != 0) {
      ThrowErrno("read back runtime resource manifest", errno);
    }
    if (!SameIdentity(temporary_status, published_status) ||
        !S_ISREG(published_status.st_mode) ||
        published_status.st_uid != geteuid()) {
      throw std::runtime_error(
          "runtime resource manifest publication read-back failed");
    }
    if (ReadBoundedFileAt(run_root.get(), kManifestName,
                          kMaximumManifestBytes) != contents) {
      throw std::runtime_error(
          "runtime resource manifest contents differ after publication");
    }
  } catch (...) {
    if (!published_file) {
      static_cast<void>(unlinkat(run_root.get(), temporary.c_str(), 0));
    }
    throw;
  }
}

std::optional<RuntimeNodeResourceManifest> TryLoadRuntimeNodeResourceManifest(
    const RunOwnership& ownership) {
  UniqueFd run_root = OpenOwnedRunRoot(ownership);
  struct stat status{};
  const std::string name(kManifestName);
  if (fstatat(run_root.get(), name.c_str(), &status, AT_SYMLINK_NOFOLLOW) !=
      0) {
    if (errno == ENOENT) {
      return std::nullopt;
    }
    ThrowErrno("inspect runtime resource manifest", errno);
  }
  if (!S_ISREG(status.st_mode)) {
    throw std::runtime_error("runtime resource manifest is not a regular file");
  }
  const boost::json::value parsed = boost::json::parse(
      ReadBoundedFileAt(run_root.get(), kManifestName, kMaximumManifestBytes));
  if (!parsed.is_object()) {
    throw std::runtime_error("runtime resource manifest is not an object");
  }
  const boost::json::object& object = parsed.as_object();
  RejectUnknownFields(
      object, {"version", "run_id", "resource_id", "isolated_network", "nodes"},
      "runtime resource manifest");
  const std::uint64_t version = RequiredUnsigned(object, "version");
  if ((version != 1U && version != kManifestVersion) ||
      RequiredString(object, "run_id") != ownership.run_id ||
      RequiredString(object, "resource_id") != ownership.resource_id) {
    throw std::runtime_error(
        "runtime resource manifest does not match run ownership");
  }
  const boost::json::value* isolated = object.if_contains("isolated_network");
  const boost::json::value* node_values = object.if_contains("nodes");
  if (isolated == nullptr || !isolated->is_bool() || node_values == nullptr ||
      !node_values->is_array() ||
      node_values->as_array().size() > kMaximumManifestEntries) {
    throw std::runtime_error(
        "runtime resource manifest has invalid isolation or node fields");
  }

  RuntimeNodeResourceManifest manifest{
      .ownership = ownership,
      .isolated_network = isolated->as_bool(),
      .nodes = {},
  };
  manifest.nodes.reserve(node_values->as_array().size());
  for (const boost::json::value& value : node_values->as_array()) {
    if (!value.is_object()) {
      throw std::runtime_error(
          "runtime resource manifest node entry is not an object");
    }
    const boost::json::object& node = value.as_object();
    RejectUnknownFields(
        node, {"id", "slot", "chain", "data_dir", "root_name", "state"},
        "runtime resource manifest node entry");
    const std::uint64_t slot = RequiredUnsigned(node, "slot");
    if (slot > std::numeric_limits<std::uint32_t>::max()) {
      throw std::runtime_error("runtime resource manifest slot exceeds uint32");
    }
    std::optional<std::string> root_name;
    if (const boost::json::value* root = node.if_contains("root_name")) {
      if (!root->is_string()) {
        throw std::runtime_error(
            "runtime resource manifest root_name is not a string");
      }
      root_name = std::string(root->as_string());
    }
    manifest.nodes.push_back(RuntimeNodeResourceEntry{
        .node_id = RequiredString(node, "id"),
        .slot = static_cast<std::uint32_t>(slot),
        .chain = ParseChainKind(RequiredString(node, "chain")),
        .data_dir = RequiredString(node, "data_dir"),
        .root_name = std::move(root_name),
        .state = ParseState(RequiredString(node, "state")),
    });
  }
  RequireManifest(manifest);
  return manifest;
}

bool RuntimeNodeRootEntryExists(const RunOwnership& ownership,
                                std::string_view node_id) {
  RequireSafeNodeId(node_id);
  UniqueFd nodes = OpenNodesDirectory(ownership);
  struct stat status{};
  const std::string name(node_id);
  if (fstatat(nodes.get(), name.c_str(), &status, AT_SYMLINK_NOFOLLOW) != 0) {
    if (errno == ENOENT) {
      return false;
    }
    ThrowErrno("inspect runtime node root", errno);
  }
  return true;
}

bool RuntimeNodeRootEntryExists(const RunOwnership& ownership,
                                const RuntimeNodeResourceEntry& entry) {
  RequireEntry(entry);
  return RuntimeNodeRootEntryExists(ownership, EntryRootName(entry));
}

void PrepareRuntimeNodeRoot(const RunOwnership& ownership,
                            const RuntimeNodeResourceEntry& entry,
                            bool* acquired) {
  RequireEntry(entry);
  if (acquired != nullptr) {
    *acquired = false;
  }
  UniqueFd nodes = OpenNodesDirectory(ownership);
  const std::string root_name(EntryRootName(entry));
  if (mkdirat(nodes.get(), root_name.c_str(), S_IRWXU) != 0) {
    ThrowErrno("create runtime node root", errno);
  }
  if (acquired != nullptr) {
    *acquired = true;
  }
  UniqueFd node(openat(nodes.get(), root_name.c_str(),
                       O_RDONLY | O_DIRECTORY | O_CLOEXEC | O_NOFOLLOW));
  if (!node.valid()) {
    const int error = errno;
    if (unlinkat(nodes.get(), root_name.c_str(), AT_REMOVEDIR) == 0 &&
        acquired != nullptr) {
      *acquired = false;
    }
    ThrowErrno("open created runtime node root", error);
  }
  try {
    if (MountId(node.get()) != MountId(nodes.get()) ||
        MountIdAt(nodes.get(), root_name) != MountId(node.get())) {
      throw std::runtime_error(
          "created runtime node root crossed a mount boundary");
    }
    WriteNodeMarker(node.get(), ownership, entry);
  } catch (...) {
    const std::exception_ptr original = std::current_exception();
    std::string cleanup_error;
    if (unlinkat(node.get(), std::string(kNodeMarkerName).c_str(), 0) != 0 &&
        errno != ENOENT) {
      cleanup_error = std::error_code(errno, std::generic_category()).message();
    }
    if (unlinkat(nodes.get(), root_name.c_str(), AT_REMOVEDIR) == 0) {
      if (acquired != nullptr) {
        *acquired = false;
      }
    } else if (cleanup_error.empty()) {
      cleanup_error = std::error_code(errno, std::generic_category()).message();
    }
    if (!cleanup_error.empty()) {
      try {
        std::rethrow_exception(original);
      } catch (const std::exception& error) {
        throw std::runtime_error(
            std::string(error.what()) +
            "; failed runtime node root acquisition cleanup: " + cleanup_error);
      }
    }
    std::rethrow_exception(original);
  }
}

void VerifyRuntimeNodeRootOwnership(const RunOwnership& ownership,
                                    const RuntimeNodeResourceEntry& entry) {
  RequireEntry(entry);
  UniqueFd nodes = OpenNodesDirectory(ownership);
  static_cast<void>(OpenOwnedNodeRoot(nodes.get(), ownership, entry));
}

void CleanupCookieAt(int node_directory) {
  constexpr std::string_view kCredentialName = ".bbp-rpc-cookie";
  const std::string credential(kCredentialName);
  if (unlinkat(node_directory, credential.c_str(), 0) != 0 && errno != ENOENT) {
    ThrowErrno("remove runtime node RPC credential", errno);
  }
  struct stat status{};
  if (fstatat(node_directory, credential.c_str(), &status,
              AT_SYMLINK_NOFOLLOW) == 0 ||
      errno != ENOENT) {
    throw std::runtime_error("runtime node RPC credential survived cleanup");
  }
}

void CleanupRuntimeNodeRpcCredential(const RunOwnership& ownership,
                                     const RuntimeNodeResourceEntry& entry) {
  RequireEntry(entry);
  UniqueFd nodes = OpenNodesDirectory(ownership);
  struct stat present{};
  const std::string root_name(EntryRootName(entry));
  if (fstatat(nodes.get(), root_name.c_str(), &present, AT_SYMLINK_NOFOLLOW) !=
      0) {
    if (errno == ENOENT) {
      return;
    }
    ThrowErrno("inspect runtime node root before credential cleanup", errno);
  }
  UniqueFd node = OpenOwnedNodeRoot(nodes.get(), ownership, entry);
  if (ChainDriverSpecFor(entry.chain).rpc_authentication !=
      RpcAuthenticationMode::kCookieFile) {
    return;
  }
  CleanupCookieAt(node.get());
}

void CleanupLegacyRuntimeNodeRpcCredential(const RunOwnership& ownership,
                                           std::string_view node_id,
                                           ChainKind chain) {
  RequireSafeNodeId(node_id);
  UniqueFd nodes = OpenNodesDirectory(ownership);
  struct stat before{};
  const std::string name(node_id);
  if (fstatat(nodes.get(), name.c_str(), &before, AT_SYMLINK_NOFOLLOW) != 0) {
    if (errno == ENOENT) {
      return;
    }
    ThrowErrno("inspect legacy runtime node root", errno);
  }
  UniqueFd node(openat(nodes.get(), name.c_str(),
                       O_RDONLY | O_DIRECTORY | O_CLOEXEC | O_NOFOLLOW));
  if (!node.valid()) {
    ThrowErrno("open legacy runtime node root", errno);
  }
  struct stat opened{};
  if (fstat(node.get(), &opened) != 0) {
    ThrowErrno("inspect opened legacy runtime node root", errno);
  }
  if (!SameIdentity(before, opened) || !S_ISDIR(opened.st_mode) ||
      opened.st_uid != geteuid() ||
      MountId(node.get()) != MountId(nodes.get()) ||
      MountIdAt(nodes.get(), name) != MountId(node.get())) {
    throw std::runtime_error(
        "legacy runtime node root failed ownership verification");
  }
  if (ChainDriverSpecFor(chain).rpc_authentication ==
      RpcAuthenticationMode::kCookieFile) {
    CleanupCookieAt(node.get());
  }
}

void RemoveRuntimeNodeRoot(
    const RunOwnership& ownership, const RuntimeNodeResourceEntry& entry,
    std::optional<std::chrono::steady_clock::time_point> absolute_deadline,
    std::stop_token stop_token) {
  RequireEntry(entry);
  if (entry.state == RuntimeNodeResourceState::kPendingReplaceUncertain) {
    throw std::runtime_error(
        "runtime node root removal refuses uncertain replacement "
        "orientation");
  }
  UniqueFd nodes = OpenNodesDirectory(ownership);
  struct stat present{};
  const std::string root_name(EntryRootName(entry));
  if (fstatat(nodes.get(), root_name.c_str(), &present, AT_SYMLINK_NOFOLLOW) !=
      0) {
    if (errno == ENOENT) {
      return;
    }
    ThrowErrno("inspect runtime node root before cleanup", errno);
  }
  struct stat opened{};
  UniqueFd node = OpenOwnedNodeRoot(nodes.get(), ownership, entry, &opened);

  std::size_t visited_entries = 0U;
  RemoveDirectoryContents(node.get(), 0U, &visited_entries, kNodeMarkerName,
                          absolute_deadline, stop_token);
  RequireNodeMarker(node.get(), ownership, entry);
  struct stat current{};
  if (fstatat(nodes.get(), root_name.c_str(), &current, AT_SYMLINK_NOFOLLOW) !=
      0) {
    ThrowErrno("reinspect runtime node root before removal", errno);
  }
  if (!SameIdentity(opened, current)) {
    throw std::runtime_error(
        "runtime node root identity changed during cleanup");
  }
  const std::string marker_name(kNodeMarkerName);
  if (unlinkat(node.get(), marker_name.c_str(), 0) != 0) {
    ThrowErrno("remove runtime node ownership marker", errno);
  }
  if (unlinkat(nodes.get(), root_name.c_str(), AT_REMOVEDIR) != 0) {
    const int remove_error = errno;
    try {
      WriteNodeMarker(node.get(), ownership, entry);
    } catch (const std::exception& marker_error) {
      throw std::runtime_error(
          "remove runtime node root failed: " +
          std::error_code(remove_error, std::generic_category()).message() +
          "; ownership marker restoration failed: " + marker_error.what());
    }
    ThrowErrno("remove runtime node root", remove_error);
  }
  if (fstatat(nodes.get(), root_name.c_str(), &current, AT_SYMLINK_NOFOLLOW) ==
          0 ||
      errno != ENOENT) {
    throw std::runtime_error(
        "runtime node root survived descriptor-anchored cleanup");
  }
}

void CloneRuntimeNodeRootForReplacement(
    const RunOwnership& ownership, const RuntimeNodeResourceEntry& source_entry,
    const RuntimeNodeResourceEntry& staging_entry,
    std::optional<std::chrono::steady_clock::time_point> absolute_deadline,
    std::stop_token stop_token, bool* acquired) {
  if (acquired != nullptr) {
    *acquired = false;
  }
  RequireEntry(source_entry);
  RequireEntry(staging_entry);
  if (source_entry.root_name ||
      source_entry.state != RuntimeNodeResourceState::kLive ||
      staging_entry.state != RuntimeNodeResourceState::kPendingReplace ||
      !staging_entry.root_name ||
      source_entry.node_id != staging_entry.node_id ||
      source_entry.slot != staging_entry.slot ||
      source_entry.chain != staging_entry.chain) {
    throw std::runtime_error(
        "runtime node replacement clone entries are incompatible");
  }
  RequireCloneTime(absolute_deadline, stop_token);
  PrepareRuntimeNodeRoot(ownership, staging_entry, acquired);

  UniqueFd nodes = OpenNodesDirectory(ownership);
  UniqueFd source = OpenOwnedNodeRoot(nodes.get(), ownership, source_entry);
  UniqueFd destination =
      OpenOwnedNodeRoot(nodes.get(), ownership, staging_entry);
  std::size_t visited_entries = 0U;
  std::map<CloneInode, std::filesystem::path> hard_links;
  CloneDirectoryContents(source.get(), destination.get(), destination.get(), {},
                         0U, &visited_entries, &hard_links, absolute_deadline,
                         stop_token);
  if (fsync(destination.get()) != 0 || fsync(nodes.get()) != 0) {
    ThrowErrno("sync runtime node replacement clone", errno);
  }
  VerifyRuntimeNodeRootOwnership(ownership, source_entry);
  VerifyRuntimeNodeRootOwnership(ownership, staging_entry);
}

void ExchangeRuntimeNodeRootsForReplacement(
    const RunOwnership& ownership, const RuntimeNodeResourceEntry& first_entry,
    const RuntimeNodeResourceEntry& second_entry,
    RuntimeNodeRootOrientation* orientation) {
  if (orientation == nullptr) {
    throw std::invalid_argument(
        "runtime node replacement exchange requires orientation state");
  }
  if (*orientation == RuntimeNodeRootOrientation::kUnknown) {
    throw std::runtime_error(
        "runtime node replacement root orientation is unknown");
  }
  const RuntimeNodeRootOrientation prior_orientation = *orientation;
  RequireEntry(first_entry);
  RequireEntry(second_entry);
  if (first_entry.root_name ||
      first_entry.state != RuntimeNodeResourceState::kLive ||
      !second_entry.root_name || !IsReplacementState(second_entry.state) ||
      second_entry.state ==
          RuntimeNodeResourceState::kPendingReplaceUncertain ||
      EntryRootName(first_entry) == EntryRootName(second_entry) ||
      first_entry.node_id != second_entry.node_id ||
      first_entry.slot != second_entry.slot ||
      first_entry.chain != second_entry.chain) {
    throw std::runtime_error(
        "runtime node replacement exchange entries are incompatible");
  }
  UniqueFd nodes = OpenNodesDirectory(ownership);
  struct stat first_before{};
  struct stat second_before{};
  static_cast<void>(
      OpenOwnedNodeRoot(nodes.get(), ownership, first_entry, &first_before));
  static_cast<void>(
      OpenOwnedNodeRoot(nodes.get(), ownership, second_entry, &second_before));
  const std::string first_name(EntryRootName(first_entry));
  const std::string second_name(EntryRootName(second_entry));
  if (syscall(SYS_renameat2, nodes.get(), first_name.c_str(), nodes.get(),
              second_name.c_str(), RENAME_EXCHANGE) != 0) {
    ThrowErrno("exchange runtime node replacement roots", errno);
  }
  *orientation = prior_orientation == RuntimeNodeRootOrientation::kOriginal
                     ? RuntimeNodeRootOrientation::kExchanged
                     : RuntimeNodeRootOrientation::kOriginal;
  const auto verify_exchange = [&](const struct stat& first_expected,
                                   const struct stat& second_expected) {
    struct stat first_after{};
    struct stat second_after{};
    static_cast<void>(OpenOwnedNodeRootNamed(nodes.get(), first_name, ownership,
                                             first_entry, &first_after));
    static_cast<void>(OpenOwnedNodeRootNamed(
        nodes.get(), second_name, ownership, second_entry, &second_after));
    if (!SameIdentity(first_after, first_expected) ||
        !SameIdentity(second_after, second_expected)) {
      throw std::runtime_error(
          "runtime node replacement root exchange read-back failed");
    }
  };
  try {
    if (fsync(nodes.get()) != 0) {
      ThrowErrno("sync runtime node replacement root exchange", errno);
    }
    verify_exchange(second_before, first_before);
  } catch (...) {
    const std::exception_ptr failure = std::current_exception();
    std::string failure_text = "unknown exception";
    try {
      std::rethrow_exception(failure);
    } catch (const std::exception& error) {
      failure_text = error.what();
    } catch (...) {
    }
    if (syscall(SYS_renameat2, nodes.get(), first_name.c_str(), nodes.get(),
                second_name.c_str(), RENAME_EXCHANGE) != 0) {
      *orientation = RuntimeNodeRootOrientation::kUnknown;
      throw std::runtime_error(
          "runtime node replacement root exchange verification failed: " +
          failure_text + "; exchange rollback failed: " +
          std::error_code(errno, std::generic_category()).message());
    }
    *orientation = prior_orientation;
    try {
      if (fsync(nodes.get()) != 0) {
        ThrowErrno("sync runtime node replacement exchange rollback", errno);
      }
      verify_exchange(first_before, second_before);
    } catch (...) {
      *orientation = RuntimeNodeRootOrientation::kUnknown;
      throw std::runtime_error(
          "runtime node replacement root exchange verification failed: " +
          failure_text + "; exchange rollback could not be verified");
    }
    std::rethrow_exception(failure);
  }
}

void RemoveOwnedRunRoot(
    const RunOwnership& ownership,
    std::optional<std::chrono::steady_clock::time_point> absolute_deadline,
    std::stop_token stop_token) {
  UniqueFd run_root = OpenOwnedRunRoot(ownership);
  struct stat opened{};
  if (fstat(run_root.get(), &opened) != 0) {
    ThrowErrno("inspect owned run root before removal", errno);
  }
  const std::filesystem::path parent_path = ownership.run_root.parent_path();
  const std::string name = ownership.run_root.filename().string();
  if (parent_path.empty() || name.empty() || name == "." || name == "..") {
    throw std::runtime_error("owned run root has an unsafe parent or name");
  }
  UniqueFd parent(open(parent_path.c_str(),
                       O_RDONLY | O_DIRECTORY | O_CLOEXEC | O_NOFOLLOW));
  if (!parent.valid()) {
    ThrowErrno("open owned run parent", errno);
  }
  struct stat parent_status{};
  if (fstat(parent.get(), &parent_status) != 0) {
    ThrowErrno("inspect owned run parent", errno);
  }
  if (!S_ISDIR(parent_status.st_mode) ||
      MountId(parent.get()) != MountId(run_root.get())) {
    throw std::runtime_error(
        "owned run root removal refuses unsafe ownership or a mount boundary");
  }
  struct stat linked{};
  if (fstatat(parent.get(), name.c_str(), &linked, AT_SYMLINK_NOFOLLOW) != 0) {
    ThrowErrno("reinspect owned run root before removal", errno);
  }
  if (!SameIdentity(opened, linked)) {
    throw std::runtime_error("owned run root identity changed before removal");
  }

  std::size_t visited_entries = 0U;
  RemoveDirectoryContents(run_root.get(), 0U, &visited_entries, kRunMarkerFile,
                          absolute_deadline, stop_token);
  const std::string marker_contents =
      ReadBoundedFileAt(run_root.get(), kRunMarkerFile, 4096U);
  if (LoadRunOwnership(ownership.run_id, ownership.run_root) != ownership) {
    throw std::runtime_error("run ownership changed before final root removal");
  }
  const std::string marker_name(kRunMarkerFile);
  if (unlinkat(run_root.get(), marker_name.c_str(), 0) != 0) {
    ThrowErrno("remove run ownership marker", errno);
  }
  if (fsync(run_root.get()) != 0) {
    ThrowErrno("sync run root before final removal", errno);
  }
  if (unlinkat(parent.get(), name.c_str(), AT_REMOVEDIR) != 0) {
    const int remove_error = errno;
    UniqueFd marker(openat(run_root.get(), marker_name.c_str(),
                           O_WRONLY | O_CREAT | O_EXCL | O_CLOEXEC | O_NOFOLLOW,
                           S_IRUSR | S_IWUSR));
    if (!marker.valid()) {
      throw std::runtime_error(
          "remove owned run root failed: " +
          std::error_code(remove_error, std::generic_category()).message() +
          "; ownership marker restoration failed: " +
          std::error_code(errno, std::generic_category()).message());
    }
    WriteAll(marker.get(), marker_contents, "restore run ownership marker");
    if (fsync(marker.get()) != 0 || fsync(run_root.get()) != 0) {
      ThrowErrno("sync restored run ownership marker", errno);
    }
    ThrowErrno("remove owned run root", remove_error);
  }
  if (fstatat(parent.get(), name.c_str(), &linked, AT_SYMLINK_NOFOLLOW) == 0 ||
      errno != ENOENT) {
    throw std::runtime_error(
        "owned run root survived descriptor-anchored removal");
  }
}

}  // namespace bbp
