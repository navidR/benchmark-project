#include "bbp/run_ownership.h"

#include <fcntl.h>
#include <sys/random.h>
#include <sys/stat.h>
#include <unistd.h>

#include <array>
#include <boost/json/object.hpp>
#include <boost/json/parse.hpp>
#include <boost/json/serialize.hpp>
#include <boost/json/value.hpp>
#include <cerrno>
#include <cstring>
#include <set>
#include <stdexcept>
#include <string>
#include <string_view>

#include "bbp/simulator/constants.h"
#include "bbp/util.h"

namespace bbp {
namespace {

constexpr std::uint64_t kMarkerVersion = 1U;
constexpr std::size_t kResourceByteCount = 16U;
constexpr std::size_t kResourceHexCount = kResourceByteCount * 2U;
constexpr std::size_t kInterfaceTokenHexCount = 8U;
constexpr std::size_t kMaximumMarkerBytes = 4096U;

std::filesystem::path CanonicalRunRoot(const std::filesystem::path& run_root) {
  std::error_code error;
  const std::filesystem::file_status status =
      std::filesystem::symlink_status(run_root, error);
  if (error || !std::filesystem::is_directory(status)) {
    throw std::runtime_error("run ownership requires a real run directory: " +
                             run_root.string());
  }
  const std::filesystem::path canonical =
      std::filesystem::canonical(run_root, error);
  if (error) {
    throw std::runtime_error("canonicalize run ownership root failed for " +
                             run_root.string() + ": " + error.message());
  }
  return canonical;
}

void RequireResourceId(std::string_view resource_id) {
  if (resource_id.size() != kResourceHexCount) {
    throw std::runtime_error(
        "run ownership resource id must contain 32 lowercase hex digits");
  }
  for (const char character : resource_id) {
    const bool valid = (character >= '0' && character <= '9') ||
                       (character >= 'a' && character <= 'f');
    if (!valid) {
      throw std::runtime_error(
          "run ownership resource id must contain 32 lowercase hex digits");
    }
  }
}

RunOwnership BuildRunOwnershipForRoot(std::string run_id,
                                      std::filesystem::path run_root,
                                      std::string resource_id) {
  RequireSafeRunId(run_id);
  RequireResourceId(resource_id);
  return RunOwnership{
      .run_id = run_id,
      .run_root = std::move(run_root),
      .resource_id = resource_id,
      .cgroup_name = resource_id,
      .interface_token = resource_id.substr(0U, kInterfaceTokenHexCount),
  };
}

RunOwnership BuildRunOwnership(std::string run_id,
                               const std::filesystem::path& run_root,
                               std::string resource_id) {
  return BuildRunOwnershipForRoot(std::move(run_id), CanonicalRunRoot(run_root),
                                  std::move(resource_id));
}

std::string RandomResourceId() {
  std::array<unsigned char, kResourceByteCount> bytes{};
  std::size_t offset = 0U;
  while (offset < bytes.size()) {
    const ssize_t count =
        getrandom(bytes.data() + offset, bytes.size() - offset, 0U);
    if (count < 0) {
      if (errno == EINTR) {
        continue;
      }
      throw std::runtime_error("getrandom for run ownership failed: " +
                               std::string(std::strerror(errno)));
    }
    if (count == 0) {
      throw std::runtime_error("getrandom for run ownership made no progress");
    }
    offset += static_cast<std::size_t>(count);
  }

  constexpr char kHex[] = "0123456789abcdef";
  std::string output;
  output.reserve(kResourceHexCount);
  for (const unsigned char byte : bytes) {
    output.push_back(kHex[byte >> 4U]);
    output.push_back(kHex[byte & 0x0fU]);
  }
  return output;
}

const boost::json::value& RequiredField(const boost::json::object& marker,
                                        std::string_view name) {
  const boost::json::value* value = marker.if_contains(name);
  if (value == nullptr) {
    throw std::runtime_error("run ownership marker is missing field: " +
                             std::string(name));
  }
  return *value;
}

std::string RequiredString(const boost::json::object& marker,
                           std::string_view name) {
  const boost::json::value& value = RequiredField(marker, name);
  if (!value.is_string()) {
    throw std::runtime_error("run ownership marker field is not a string: " +
                             std::string(name));
  }
  return std::string(value.as_string());
}

RunOwnership ParseRunOwnershipMarker(std::string run_id,
                                     const std::filesystem::path& run_root,
                                     std::string_view text,
                                     const std::filesystem::path& marker_path) {
  boost::json::value parsed;
  try {
    parsed = boost::json::parse(text);
  } catch (const std::exception& parse_error) {
    throw std::runtime_error("invalid run ownership marker " +
                             marker_path.string() + ": " + parse_error.what());
  }
  if (!parsed.is_object()) {
    throw std::runtime_error("run ownership marker is not an object: " +
                             marker_path.string());
  }
  const boost::json::object& marker = parsed.as_object();
  const std::set<std::string_view> fields = {"version", "run_id", "run_root",
                                             "resource_id"};
  for (const auto& member : marker) {
    const std::string_view key(member.key().data(), member.key().size());
    if (!fields.contains(key)) {
      throw std::runtime_error("run ownership marker has unsupported field: " +
                               std::string(member.key()));
    }
  }
  const boost::json::value& version = RequiredField(marker, "version");
  const bool supported_version =
      (version.is_uint64() && version.as_uint64() == kMarkerVersion) ||
      (version.is_int64() && version.as_int64() >= 0 &&
       static_cast<std::uint64_t>(version.as_int64()) == kMarkerVersion);
  if (!supported_version) {
    throw std::runtime_error("run ownership marker version is unsupported");
  }
  if (RequiredString(marker, "run_id") != run_id) {
    throw std::runtime_error("run ownership marker run id does not match");
  }
  if (std::filesystem::path(RequiredString(marker, "run_root")) != run_root) {
    throw std::runtime_error("run ownership marker root does not match");
  }
  return BuildRunOwnershipForRoot(std::move(run_id), run_root,
                                  RequiredString(marker, "resource_id"));
}

void WriteAll(int fd, std::string_view text,
              const std::filesystem::path& path) {
  while (!text.empty()) {
    const ssize_t count = write(fd, text.data(), text.size());
    if (count < 0) {
      if (errno == EINTR) {
        continue;
      }
      throw std::runtime_error("write run ownership marker failed for " +
                               path.string() + ": " +
                               std::string(std::strerror(errno)));
    }
    if (count == 0) {
      throw std::runtime_error(
          "write run ownership marker made no progress for " + path.string());
    }
    text.remove_prefix(static_cast<std::size_t>(count));
  }
}

struct stat RequireDescriptorBoundRunRoot(const std::filesystem::path& run_root,
                                          int run_root_fd) {
  if (run_root_fd < 0 || !run_root.is_absolute() ||
      run_root.lexically_normal() != run_root) {
    throw std::runtime_error(
        "descriptor-bound run ownership requires an absolute normalized root");
  }

  struct stat opened{};
  if (fstat(run_root_fd, &opened) != 0) {
    throw std::runtime_error("inspect descriptor-bound run root failed: " +
                             std::string(std::strerror(errno)));
  }
  struct stat linked{};
  if (fstatat(AT_FDCWD, run_root.c_str(), &linked, AT_SYMLINK_NOFOLLOW) != 0) {
    throw std::runtime_error("inspect descriptor-bound run root path failed: " +
                             std::string(std::strerror(errno)));
  }
  if (!S_ISDIR(opened.st_mode) || !S_ISDIR(linked.st_mode) ||
      opened.st_uid != geteuid() || opened.st_dev != linked.st_dev ||
      opened.st_ino != linked.st_ino) {
    throw std::runtime_error(
        "descriptor-bound run root does not match its owned path");
  }
  return opened;
}

void RequireInterfaceInputs(const RunOwnership& ownership,
                            std::uint32_t node_index, char suffix) {
  RequireResourceId(ownership.resource_id);
  if (ownership.interface_token !=
      ownership.resource_id.substr(0U, kInterfaceTokenHexCount)) {
    throw std::runtime_error("run ownership interface token is inconsistent");
  }
  if (node_index >= 16U) {
    throw std::runtime_error("run interface node index must be 0..15");
  }
  if (suffix != 'h' && suffix != 'p') {
    throw std::runtime_error("run interface suffix must be h or p");
  }
}

}  // namespace

RunOwnership CreateRunOwnership(std::string run_id,
                                const std::filesystem::path& run_root) {
  return BuildRunOwnership(std::move(run_id), run_root, RandomResourceId());
}

RunOwnership CreateRunOwnershipAt(std::string run_id,
                                  const std::filesystem::path& run_root,
                                  int run_root_fd) {
  RequireSafeRunId(run_id);
  static_cast<void>(RequireDescriptorBoundRunRoot(run_root, run_root_fd));
  return BuildRunOwnershipForRoot(std::move(run_id), run_root,
                                  RandomResourceId());
}

RunOwnership LoadRunOwnership(std::string run_id,
                              const std::filesystem::path& run_root) {
  return LoadRunOwnership(std::move(run_id), run_root, {});
}

RunOwnership LoadRunOwnership(std::string run_id,
                              const std::filesystem::path& run_root,
                              std::stop_token stop_token) {
  if (stop_token.stop_requested()) {
    throw std::runtime_error("run ownership loading was cancelled");
  }
  RequireSafeRunId(run_id);
  const std::filesystem::path canonical_root = CanonicalRunRoot(run_root);
  const std::filesystem::path marker_path =
      canonical_root / std::string(kRunMarkerFile);
  std::error_code error;
  const std::filesystem::file_status marker_status =
      std::filesystem::symlink_status(marker_path, error);
  if (error || !std::filesystem::is_regular_file(marker_status)) {
    throw std::runtime_error(
        "run ownership requires a regular non-symlink marker: " +
        marker_path.string());
  }

  RunOwnership ownership = ParseRunOwnershipMarker(
      std::move(run_id), canonical_root,
      ReadText(marker_path, kMaximumMarkerBytes, stop_token), marker_path);
  if (stop_token.stop_requested()) {
    throw std::runtime_error("run ownership loading was cancelled");
  }
  return ownership;
}

RunOwnership LoadRunOwnershipAt(std::string run_id,
                                const std::filesystem::path& run_root,
                                int run_root_fd, std::stop_token stop_token) {
  if (stop_token.stop_requested()) {
    throw std::runtime_error("run ownership loading was cancelled");
  }
  RequireSafeRunId(run_id);
  const struct stat opened =
      RequireDescriptorBoundRunRoot(run_root, run_root_fd);

  const std::filesystem::path marker_path =
      run_root / std::string(kRunMarkerFile);
  RunOwnership ownership = ParseRunOwnershipMarker(
      std::move(run_id), run_root,
      ReadTextAt(run_root_fd, kRunMarkerFile, kMaximumMarkerBytes, stop_token),
      marker_path);

  const struct stat final_opened =
      RequireDescriptorBoundRunRoot(run_root, run_root_fd);
  if (opened.st_dev != final_opened.st_dev ||
      opened.st_ino != final_opened.st_ino) {
    throw std::runtime_error(
        "descriptor-bound run root identity changed during ownership load");
  }
  if (stop_token.stop_requested()) {
    throw std::runtime_error("run ownership loading was cancelled");
  }
  return ownership;
}

void WriteRunOwnershipMarker(const RunOwnership& ownership) {
  const RunOwnership validated = BuildRunOwnership(
      ownership.run_id, ownership.run_root, ownership.resource_id);
  if (validated != ownership) {
    throw std::runtime_error("run ownership fields are inconsistent");
  }
  boost::json::object marker;
  marker["version"] = kMarkerVersion;
  marker["run_id"] = ownership.run_id;
  marker["run_root"] = ownership.run_root.string();
  marker["resource_id"] = ownership.resource_id;
  const std::string contents = boost::json::serialize(marker) + "\n";
  const std::filesystem::path marker_path =
      ownership.run_root / std::string(kRunMarkerFile);
  const int fd =
      open(marker_path.c_str(),
           O_WRONLY | O_CREAT | O_EXCL | O_CLOEXEC | O_NOFOLLOW, 0600);
  if (fd < 0) {
    throw std::runtime_error("create run ownership marker failed for " +
                             marker_path.string() + ": " +
                             std::string(std::strerror(errno)));
  }
  try {
    WriteAll(fd, contents, marker_path);
  } catch (...) {
    close(fd);
    throw;
  }
  if (close(fd) != 0) {
    throw std::runtime_error("close run ownership marker failed for " +
                             marker_path.string() + ": " +
                             std::string(std::strerror(errno)));
  }
}

RunOwnershipMarkerIdentity WriteRunOwnershipMarkerAt(
    const RunOwnership& ownership, int run_root_fd) {
  const RunOwnership validated = BuildRunOwnershipForRoot(
      ownership.run_id, ownership.run_root, ownership.resource_id);
  if (validated != ownership) {
    throw std::runtime_error("run ownership fields are inconsistent");
  }
  const struct stat opened =
      RequireDescriptorBoundRunRoot(ownership.run_root, run_root_fd);
  boost::json::object marker;
  marker["version"] = kMarkerVersion;
  marker["run_id"] = ownership.run_id;
  marker["run_root"] = ownership.run_root.string();
  marker["resource_id"] = ownership.resource_id;
  const std::string contents = boost::json::serialize(marker) + "\n";
  const std::filesystem::path marker_path =
      ownership.run_root / std::string(kRunMarkerFile);
  const std::string marker_name(kRunMarkerFile);
  const int fd =
      openat(run_root_fd, marker_name.c_str(),
             O_WRONLY | O_CREAT | O_EXCL | O_CLOEXEC | O_NOFOLLOW, 0600);
  if (fd < 0) {
    throw std::runtime_error("create run ownership marker failed for " +
                             marker_path.string() + ": " +
                             std::string(std::strerror(errno)));
  }
  struct stat created_marker{};
  if (fstat(fd, &created_marker) != 0) {
    const int inspect_error = errno;
    static_cast<void>(close(fd));
    throw std::runtime_error(
        "inspect created run ownership marker failed for " +
        marker_path.string() + ": " +
        std::string(std::strerror(inspect_error)));
  }
  const auto remove_created_marker = [&] {
    struct stat linked_marker{};
    if (fstatat(run_root_fd, marker_name.c_str(), &linked_marker,
                AT_SYMLINK_NOFOLLOW) == 0 &&
        linked_marker.st_dev == created_marker.st_dev &&
        linked_marker.st_ino == created_marker.st_ino) {
      static_cast<void>(unlinkat(run_root_fd, marker_name.c_str(), 0));
    }
  };
  if (!S_ISREG(created_marker.st_mode) || created_marker.st_uid != geteuid()) {
    static_cast<void>(close(fd));
    remove_created_marker();
    throw std::runtime_error(
        "created run ownership marker is not an owned regular file");
  }
  try {
    WriteAll(fd, contents, marker_path);
    struct stat final_marker{};
    if (fstat(fd, &final_marker) != 0) {
      const int inspect_error = errno;
      throw std::runtime_error(
          "reinspect created run ownership marker failed for " +
          marker_path.string() + ": " +
          std::string(std::strerror(inspect_error)));
    }
    struct stat linked_marker{};
    if (fstatat(run_root_fd, marker_name.c_str(), &linked_marker,
                AT_SYMLINK_NOFOLLOW) != 0) {
      const int inspect_error = errno;
      throw std::runtime_error(
          "reinspect linked run ownership marker failed for " +
          marker_path.string() + ": " +
          std::string(std::strerror(inspect_error)));
    }
    if (!S_ISREG(final_marker.st_mode) ||
        final_marker.st_dev != created_marker.st_dev ||
        final_marker.st_ino != created_marker.st_ino ||
        final_marker.st_uid != created_marker.st_uid ||
        linked_marker.st_dev != created_marker.st_dev ||
        linked_marker.st_ino != created_marker.st_ino ||
        final_marker.st_size < 0 ||
        static_cast<std::uintmax_t>(final_marker.st_size) != contents.size()) {
      throw std::runtime_error(
          "run ownership marker changed during descriptor-bound creation");
    }
  } catch (...) {
    static_cast<void>(close(fd));
    remove_created_marker();
    throw;
  }
  if (close(fd) != 0) {
    const int close_error = errno;
    remove_created_marker();
    throw std::runtime_error("close run ownership marker failed for " +
                             marker_path.string() + ": " +
                             std::string(std::strerror(close_error)));
  }
  try {
    const struct stat final_opened =
        RequireDescriptorBoundRunRoot(ownership.run_root, run_root_fd);
    if (opened.st_dev != final_opened.st_dev ||
        opened.st_ino != final_opened.st_ino) {
      throw std::runtime_error(
          "descriptor-bound run root identity changed during marker creation");
    }
  } catch (...) {
    remove_created_marker();
    throw;
  }
  return RunOwnershipMarkerIdentity{
      .device = static_cast<std::uintmax_t>(created_marker.st_dev),
      .inode = static_cast<std::uintmax_t>(created_marker.st_ino),
  };
}

std::string RunInterfaceName(const RunOwnership& ownership,
                             std::uint32_t node_index, char suffix) {
  RequireInterfaceInputs(ownership, node_index, suffix);
  return "bbp" + ownership.interface_token + "n" +
         std::to_string(node_index + 1U) + suffix;
}

std::string RunInterfaceAlias(const RunOwnership& ownership,
                              std::uint32_t node_index, char suffix) {
  RequireInterfaceInputs(ownership, node_index, suffix);
  return "bbp:" + ownership.resource_id + ":n" +
         std::to_string(node_index + 1U) + suffix;
}

}  // namespace bbp
