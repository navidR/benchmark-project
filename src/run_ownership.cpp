#include "bbp/run_ownership.h"

#include <fcntl.h>
#include <sys/random.h>
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

RunOwnership BuildRunOwnership(std::string run_id,
                               const std::filesystem::path& run_root,
                               std::string resource_id) {
  RequireSafeRunId(run_id);
  RequireResourceId(resource_id);
  const std::filesystem::path canonical_root = CanonicalRunRoot(run_root);
  return RunOwnership{
      .run_id = run_id,
      .run_root = canonical_root,
      .resource_id = resource_id,
      .cgroup_name = resource_id,
      .interface_token = resource_id.substr(0U, kInterfaceTokenHexCount),
  };
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

RunOwnership LoadRunOwnership(std::string run_id,
                              const std::filesystem::path& run_root) {
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

  boost::json::value parsed;
  try {
    parsed = boost::json::parse(ReadText(marker_path));
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
  if (std::filesystem::path(RequiredString(marker, "run_root")) !=
      canonical_root) {
    throw std::runtime_error("run ownership marker root does not match");
  }
  return BuildRunOwnership(std::move(run_id), canonical_root,
                           RequiredString(marker, "resource_id"));
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
