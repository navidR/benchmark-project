#include "runtime_capacity_persistence.h"

#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

#include <boost/json/parse.hpp>
#include <boost/json/serialize.hpp>
#include <cerrno>
#include <cstddef>
#include <stdexcept>
#include <system_error>
#include <utility>

#include "bbp/simulator/options.h"
#include "bbp/util.h"
#include "simulator_scenario_serialization.h"

namespace bbp::simulator_app_internal {
namespace {

class FileDescriptor {
 public:
  explicit FileDescriptor(int descriptor) : descriptor_(descriptor) {}
  ~FileDescriptor() {
    if (descriptor_ >= 0) {
      static_cast<void>(close(descriptor_));
    }
  }
  FileDescriptor(const FileDescriptor&) = delete;
  FileDescriptor& operator=(const FileDescriptor&) = delete;
  FileDescriptor(FileDescriptor&& other) noexcept
      : descriptor_(std::exchange(other.descriptor_, -1)) {}
  int get() const { return descriptor_; }

 private:
  int descriptor_;
};

[[noreturn]] void ThrowIo(std::string_view operation) {
  throw std::system_error(errno, std::generic_category(),
                          std::string(operation));
}

OwnedRunRootIdentity VerifyRoot(int descriptor, const RunOwnership& ownership) {
  struct stat opened{};
  struct stat linked{};
  if (fstat(descriptor, &opened) != 0 ||
      fstatat(AT_FDCWD, ownership.run_root.c_str(), &linked,
              AT_SYMLINK_NOFOLLOW) != 0) {
    ThrowIo("inspect capacity document root");
  }
  if (!S_ISDIR(opened.st_mode) || !S_ISDIR(linked.st_mode) ||
      opened.st_uid != geteuid() || opened.st_dev != linked.st_dev ||
      opened.st_ino != linked.st_ino ||
      LoadRunOwnershipAt(ownership.run_id, ownership.run_root, descriptor) !=
          ownership) {
    throw std::runtime_error("capacity document root ownership changed");
  }
  return {static_cast<std::uintmax_t>(opened.st_dev),
          static_cast<std::uintmax_t>(opened.st_ino)};
}

FileDescriptor OpenRoot(const RunOwnership& ownership) {
  FileDescriptor root(
      open(ownership.run_root.c_str(),
           O_RDONLY | O_DIRECTORY | O_CLOEXEC | O_NOFOLLOW | O_NONBLOCK));
  if (root.get() < 0) {
    ThrowIo("open capacity document root");
  }
  return root;
}

boost::json::object ReadDocument(int root, std::string_view name,
                                 std::string_view run_id, std::string* text) {
  *text = ReadRuntimeCapacityDocumentAt(root, name);
  boost::json::value parsed = boost::json::parse(*text);
  if (!parsed.is_object()) {
    throw std::runtime_error("capacity document is not an object: " +
                             std::string(name));
  }
  boost::json::object document = std::move(parsed.as_object());
  const boost::json::value* id = document.if_contains("run_id");
  if (id == nullptr || !id->is_string() || id->as_string() != run_id) {
    throw std::runtime_error("capacity document run identity differs: " +
                             std::string(name));
  }
  return document;
}

void PublishText(int root, std::string_view name, std::string_view contents) {
  const std::string temporary = "." + std::string(name) + ".capacity.tmp";
  FileDescriptor output(
      openat(root, temporary.c_str(),
             O_WRONLY | O_CREAT | O_EXCL | O_CLOEXEC | O_NOFOLLOW, 0644));
  if (output.get() < 0) {
    ThrowIo("create capacity document temporary");
  }
  const auto require_output_identity = [&](const std::string& linked_name) {
    struct stat opened{};
    struct stat linked{};
    if (fstat(output.get(), &opened) != 0 ||
        fstatat(root, linked_name.c_str(), &linked, AT_SYMLINK_NOFOLLOW) != 0) {
      ThrowIo("inspect capacity document output identity");
    }
    if (!S_ISREG(opened.st_mode) || !S_ISREG(linked.st_mode) ||
        opened.st_uid != geteuid() || opened.st_dev != linked.st_dev ||
        opened.st_ino != linked.st_ino) {
      throw std::runtime_error("capacity document output identity changed");
    }
  };
  bool renamed = false;
  try {
    std::size_t offset = 0U;
    while (offset < contents.size()) {
      const ssize_t written = write(output.get(), contents.data() + offset,
                                    contents.size() - offset);
      if (written < 0 && errno == EINTR) {
        continue;
      }
      if (written < 0) {
        ThrowIo("write capacity document temporary");
      }
      if (written == 0) {
        throw std::runtime_error("capacity document write made no progress");
      }
      offset += static_cast<std::size_t>(written);
    }
    if (fsync(output.get()) != 0) {
      ThrowIo("sync capacity document temporary");
    }
    require_output_identity(temporary);
    if (ReadRuntimeCapacityDocumentAt(root, temporary) != contents) {
      throw std::runtime_error("capacity document temporary read-back differs");
    }
    const std::string destination(name);
    if (renameat(root, temporary.c_str(), root, destination.c_str()) != 0) {
      ThrowIo("publish capacity document");
    }
    renamed = true;
    if (fsync(root) != 0) {
      ThrowIo("sync capacity document root");
    }
    require_output_identity(destination);
    if (ReadRuntimeCapacityDocumentAt(root, name) != contents) {
      throw std::runtime_error("published capacity document read-back differs");
    }
  } catch (const std::exception& failure) {
    if (!renamed) {
      try {
        struct stat opened{};
        struct stat linked{};
        if (fstat(output.get(), &opened) != 0) {
          ThrowIo("inspect failed capacity document temporary");
        }
        if (fstatat(root, temporary.c_str(), &linked, AT_SYMLINK_NOFOLLOW) !=
            0) {
          if (errno != ENOENT) {
            ThrowIo("inspect linked failed capacity document temporary");
          }
        } else {
          if (!S_ISREG(linked.st_mode) || linked.st_uid != geteuid() ||
              opened.st_dev != linked.st_dev ||
              opened.st_ino != linked.st_ino) {
            throw std::runtime_error(
                "capacity document temporary identity changed");
          }
          if (unlinkat(root, temporary.c_str(), 0) != 0) {
            ThrowIo("remove failed capacity document temporary");
          }
        }
      } catch (const std::exception& cleanup_failure) {
        throw std::runtime_error(
            std::string(failure.what()) +
            "; temporary cleanup failed: " + cleanup_failure.what());
      }
    }
    throw;
  }
}

void RequirePreparedRoot(int root,
                         const PreparedRuntimeCapacityDocuments& prepared) {
  if (VerifyRoot(root, prepared.ownership) != prepared.root_identity) {
    throw std::runtime_error("capacity document root identity changed");
  }
}

}  // namespace

std::string ReadRuntimeCapacityDocumentAt(int root, std::string_view name,
                                          std::stop_token stop_token) {
  const std::string filename(name);
  struct stat status{};
  if (fstatat(root, filename.c_str(), &status, AT_SYMLINK_NOFOLLOW) != 0) {
    ThrowIo("inspect capacity document");
  }
  if (!S_ISREG(status.st_mode) || status.st_uid != geteuid() ||
      status.st_size < 0) {
    throw std::runtime_error(
        "capacity document is not an owned regular file: " + filename);
  }
  const auto size = static_cast<std::uintmax_t>(status.st_size);
  if (size > std::string{}.max_size()) {
    throw std::length_error(
        "capacity document exceeds string memory capacity: " + filename);
  }
  const std::string text = ReadTextAt(
      root, name, size == 0U ? 1U : static_cast<std::size_t>(size), stop_token);
  struct stat final_status{};
  if (fstatat(root, filename.c_str(), &final_status, AT_SYMLINK_NOFOLLOW) !=
      0) {
    ThrowIo("reinspect capacity document");
  }
  if (text.size() != size || status.st_dev != final_status.st_dev ||
      status.st_ino != final_status.st_ino ||
      status.st_uid != final_status.st_uid ||
      status.st_size != final_status.st_size ||
      status.st_mtim.tv_sec != final_status.st_mtim.tv_sec ||
      status.st_mtim.tv_nsec != final_status.st_mtim.tv_nsec ||
      status.st_ctim.tv_sec != final_status.st_ctim.tv_sec ||
      status.st_ctim.tv_nsec != final_status.st_ctim.tv_nsec) {
    throw std::runtime_error("capacity document changed while reading: " +
                             filename);
  }
  return text;
}

PreparedRuntimeCapacityDocuments PrepareRuntimeCapacityDocuments(
    const Options& candidate) {
  if (!candidate.run_ownership) {
    throw std::runtime_error(
        "capacity document publication requires run ownership");
  }
  PreparedRuntimeCapacityDocuments prepared;
  prepared.ownership = *candidate.run_ownership;
  FileDescriptor root = OpenRoot(prepared.ownership);
  prepared.root_identity = VerifyRoot(root.get(), prepared.ownership);

  const auto update = [&](boost::json::object& document) {
    document["node_capacity"] = candidate.node_capacity;
    document["network_address_pool"] = candidate.network_address_pool;
  };
  RuntimeCapacityDocumentChange resolved_change{
      .path = prepared.ownership.run_root / "resolved-scenario.json",
      .before = {},
      .after = {}};
  boost::json::object resolved =
      ReadDocument(root.get(), "resolved-scenario.json", candidate.run_id,
                   &resolved_change.before);
  update(resolved);
  resolved.erase("network_address_range");
  resolved["network_allocation"] =
      candidate.network_address_plan
          ? boost::json::value(candidate.network_address_plan->ToSerialized())
          : boost::json::value(nullptr);
  resolved_change.after = boost::json::serialize(resolved) + "\n";
  prepared.changes.push_back(std::move(resolved_change));
  prepared.changes.push_back({
      .path = prepared.ownership.run_root / "scenario.yaml",
      .before = ReadRuntimeCapacityDocumentAt(root.get(), "scenario.yaml"),
      .after = YamlFromJson(resolved),
  });

  struct stat source_status{};
  if (fstatat(root.get(), "source-scenario.json", &source_status,
              AT_SYMLINK_NOFOLLOW) == 0) {
    RuntimeCapacityDocumentChange source_change{
        .path = prepared.ownership.run_root / "source-scenario.json",
        .before = {},
        .after = {}};
    boost::json::object source =
        ReadDocument(root.get(), "source-scenario.json", candidate.run_id,
                     &source_change.before);
    update(source);
    source_change.after = boost::json::serialize(source) + "\n";
    prepared.changes.push_back(std::move(source_change));
  } else if (errno != ENOENT) {
    ThrowIo("inspect source scenario for capacity update");
  }
  RequirePreparedRoot(root.get(), prepared);
  return prepared;
}

void PublishRuntimeCapacityDocuments(
    const PreparedRuntimeCapacityDocuments& prepared) {
  FileDescriptor root = OpenRoot(prepared.ownership);
  RequirePreparedRoot(root.get(), prepared);
  for (const RuntimeCapacityDocumentChange& change : prepared.changes) {
    const std::string name = change.path.filename().string();
    if (ReadRuntimeCapacityDocumentAt(root.get(), name) != change.before) {
      throw std::runtime_error(
          "capacity document changed before publication: " + name);
    }
    PublishText(root.get(), name, change.after);
  }
  RequirePreparedRoot(root.get(), prepared);
}

void RestoreRuntimeCapacityDocuments(
    const PreparedRuntimeCapacityDocuments& prepared) {
  FileDescriptor root = OpenRoot(prepared.ownership);
  RequirePreparedRoot(root.get(), prepared);
  std::string failures;
  for (const RuntimeCapacityDocumentChange& change : prepared.changes) {
    try {
      const std::string name = change.path.filename().string();
      if (ReadRuntimeCapacityDocumentAt(root.get(), name) != change.before) {
        PublishText(root.get(), name, change.before);
      }
    } catch (const std::exception& error) {
      failures += change.path.filename().string() + ": " + error.what() + "\n";
    }
  }
  RequirePreparedRoot(root.get(), prepared);
  if (!failures.empty()) {
    throw std::runtime_error("capacity document restoration failed:\n" +
                             failures);
  }
}

}  // namespace bbp::simulator_app_internal
