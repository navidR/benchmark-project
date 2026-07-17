#include "bbp/node_artifact_inventory.h"

#include <dirent.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

#include <algorithm>
#include <cerrno>
#include <cstddef>
#include <iomanip>
#include <memory>
#include <sstream>
#include <string>
#include <system_error>
#include <utility>
#include <vector>

namespace bbp {
namespace {

constexpr std::size_t kMaximumDataEntries = 512U;
constexpr std::size_t kMaximumLogFiles = 64U;
constexpr std::size_t kMaximumVisitedEntries = 4096U;
constexpr std::size_t kMaximumDirectoryDepth = 16U;

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

bool IsSafeNodeId(std::string_view node_id) {
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

std::string ErrnoText(std::string_view operation, int error) {
  return std::string(operation) + ": " +
         std::error_code(error, std::generic_category()).message();
}

UniqueFd OpenDirectory(const std::filesystem::path& path) {
  return UniqueFd(
      open(path.c_str(), O_RDONLY | O_DIRECTORY | O_CLOEXEC | O_NOFOLLOW));
}

UniqueFd OpenDirectoryAt(int parent_fd, const char* name) {
  return UniqueFd(
      openat(parent_fd, name, O_RDONLY | O_DIRECTORY | O_CLOEXEC | O_NOFOLLOW));
}

std::string EscapePathComponent(std::string_view component) {
  std::ostringstream output;
  output << std::uppercase << std::hex;
  for (const unsigned char character : component) {
    if (character >= 0x20U && character <= 0x7eU && character != '\\') {
      output << static_cast<char>(character);
    } else if (character == '\\') {
      output << "\\\\";
    } else {
      output << "\\x" << std::setw(2) << std::setfill('0')
             << static_cast<unsigned int>(character);
    }
  }
  return output.str();
}

std::string JoinRelativePath(std::string_view parent, std::string_view child) {
  const std::string escaped = EscapePathComponent(child);
  return parent.empty() ? escaped : std::string(parent) + "/" + escaped;
}

NodeArtifactType ArtifactType(const struct stat& status) {
  if (S_ISDIR(status.st_mode)) {
    return NodeArtifactType::kDirectory;
  }
  if (S_ISREG(status.st_mode)) {
    return NodeArtifactType::kRegularFile;
  }
  if (S_ISLNK(status.st_mode)) {
    return NodeArtifactType::kSymbolicLink;
  }
  return NodeArtifactType::kOther;
}

std::uint64_t ArtifactSize(const struct stat& status) {
  return status.st_size < 0 ? 0U : static_cast<std::uint64_t>(status.st_size);
}

bool HasLogSuffix(std::string_view name) {
  constexpr std::string_view kSuffix = ".log";
  return name.size() >= kSuffix.size() &&
         name.substr(name.size() - kSuffix.size()) == kSuffix;
}

struct ScanState {
  NodeArtifactInventory* inventory = nullptr;
  std::size_t visited_entries = 0U;
};

void RememberWarning(std::string warning, ScanState* state) {
  if (state->inventory->warning.empty()) {
    state->inventory->warning = std::move(warning);
  }
}

std::vector<std::string> ReadDirectoryNames(int directory_fd,
                                            ScanState* state) {
  const int duplicate_fd = fcntl(directory_fd, F_DUPFD_CLOEXEC, 0);
  if (duplicate_fd < 0) {
    RememberWarning(ErrnoText("duplicate artifact directory", errno), state);
    return {};
  }
  DirectoryPointer directory(fdopendir(duplicate_fd));
  if (!directory) {
    const int error = errno;
    static_cast<void>(close(duplicate_fd));
    RememberWarning(ErrnoText("open artifact directory stream", error), state);
    return {};
  }

  std::vector<std::string> names;
  while (true) {
    errno = 0;
    dirent* entry = readdir(directory.get());
    if (entry == nullptr) {
      if (errno != 0) {
        RememberWarning(ErrnoText("read artifact directory", errno), state);
      }
      break;
    }
    const std::string_view name(entry->d_name);
    if (name == "." || name == "..") {
      continue;
    }
    if (state->visited_entries + names.size() >= kMaximumVisitedEntries) {
      state->inventory->data_entries_truncated = true;
      state->inventory->log_files_truncated = true;
      break;
    }
    names.emplace_back(name);
  }
  std::sort(names.begin(), names.end());
  return names;
}

void AppendDataEntry(const NodeArtifactEntry& entry, ScanState* state) {
  if (state->inventory->data_entries.size() < kMaximumDataEntries) {
    state->inventory->data_entries.push_back(entry);
  } else {
    state->inventory->data_entries_truncated = true;
  }
}

void AppendLogFile(NodeArtifactEntry entry, ScanState* state) {
  if (state->inventory->log_files.size() < kMaximumLogFiles) {
    state->inventory->log_files.push_back(std::move(entry));
  } else {
    state->inventory->log_files_truncated = true;
  }
}

void ScanDataDirectory(int directory_fd, std::string_view relative_parent,
                       std::size_t depth, ScanState* state) {
  const std::vector<std::string> names =
      ReadDirectoryNames(directory_fd, state);
  for (const std::string& name : names) {
    if (state->visited_entries >= kMaximumVisitedEntries) {
      state->inventory->data_entries_truncated = true;
      state->inventory->log_files_truncated = true;
      return;
    }
    ++state->visited_entries;
    struct stat status {};
    if (fstatat(directory_fd, name.c_str(), &status, AT_SYMLINK_NOFOLLOW) < 0) {
      RememberWarning(ErrnoText("inspect node artifact", errno), state);
      continue;
    }

    const std::string relative_path = JoinRelativePath(relative_parent, name);
    const NodeArtifactEntry entry{
        .relative_path = relative_path,
        .type = ArtifactType(status),
        .size_bytes = ArtifactSize(status),
    };
    AppendDataEntry(entry, state);
    if (entry.type == NodeArtifactType::kRegularFile && HasLogSuffix(name)) {
      NodeArtifactEntry log = entry;
      log.relative_path = "data/" + relative_path;
      AppendLogFile(std::move(log), state);
    }

    if (entry.type != NodeArtifactType::kDirectory) {
      continue;
    }
    if (depth >= kMaximumDirectoryDepth) {
      state->inventory->data_entries_truncated = true;
      state->inventory->log_files_truncated = true;
      continue;
    }
    UniqueFd child = OpenDirectoryAt(directory_fd, name.c_str());
    if (!child.valid()) {
      RememberWarning(ErrnoText("open node artifact directory", errno), state);
      continue;
    }
    ScanDataDirectory(child.get(), relative_path, depth + 1U, state);
  }
}

void ScanNodeRootLogs(int node_fd, ScanState* state) {
  const std::vector<std::string> names = ReadDirectoryNames(node_fd, state);
  for (const std::string& name : names) {
    if (state->visited_entries >= kMaximumVisitedEntries) {
      state->inventory->data_entries_truncated = true;
      state->inventory->log_files_truncated = true;
      return;
    }
    ++state->visited_entries;
    if (!HasLogSuffix(name)) {
      continue;
    }
    struct stat status {};
    if (fstatat(node_fd, name.c_str(), &status, AT_SYMLINK_NOFOLLOW) < 0 ||
        !S_ISREG(status.st_mode)) {
      continue;
    }
    AppendLogFile(
        NodeArtifactEntry{
            .relative_path = EscapePathComponent(name),
            .type = NodeArtifactType::kRegularFile,
            .size_bytes = ArtifactSize(status),
        },
        state);
  }
}

}  // namespace

std::string_view NodeArtifactTypeName(NodeArtifactType type) {
  switch (type) {
    case NodeArtifactType::kDirectory:
      return "directory";
    case NodeArtifactType::kRegularFile:
      return "file";
    case NodeArtifactType::kSymbolicLink:
      return "symlink";
    case NodeArtifactType::kOther:
      return "other";
  }
  return "other";
}

NodeArtifactInventory InspectNodeArtifacts(
    const std::filesystem::path& run_root, std::string_view node_id) {
  NodeArtifactInventory inventory;
  inventory.node_id = std::string(node_id);
  if (!IsSafeNodeId(node_id)) {
    inventory.error = "node artifact browser rejected an unsafe node id";
    return inventory;
  }
  inventory.data_directory = "nodes/" + std::string(node_id) + "/data";

  UniqueFd run = OpenDirectory(run_root);
  if (!run.valid()) {
    inventory.error = ErrnoText("open run artifact directory", errno);
    return inventory;
  }
  UniqueFd nodes = OpenDirectoryAt(run.get(), "nodes");
  if (!nodes.valid()) {
    inventory.error = ErrnoText("open run nodes directory", errno);
    return inventory;
  }
  const std::string node_id_text(node_id);
  UniqueFd node = OpenDirectoryAt(nodes.get(), node_id_text.c_str());
  if (!node.valid()) {
    inventory.error = ErrnoText("open node artifact directory", errno);
    return inventory;
  }

  ScanState state{.inventory = &inventory};
  ScanNodeRootLogs(node.get(), &state);
  UniqueFd data = OpenDirectoryAt(node.get(), "data");
  if (!data.valid()) {
    RememberWarning(ErrnoText("open node data directory", errno), &state);
    return inventory;
  }
  ScanDataDirectory(data.get(), {}, 0U, &state);
  std::sort(inventory.log_files.begin(), inventory.log_files.end(),
            [](const NodeArtifactEntry& left, const NodeArtifactEntry& right) {
              return left.relative_path < right.relative_path;
            });
  return inventory;
}

}  // namespace bbp
