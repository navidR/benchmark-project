#include "simulator_retained_run_registry.h"

#include <dirent.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

#include <algorithm>
#include <boost/json/object.hpp>
#include <boost/json/parse.hpp>
#include <boost/json/serialize.hpp>
#include <cerrno>
#include <exception>
#include <limits>
#include <memory>
#include <optional>
#include <set>
#include <stdexcept>
#include <string>
#include <string_view>
#include <system_error>
#include <utility>
#include <vector>

#include "bbp/drivers/chain_driver_registry.h"
#include "bbp/mcp_operation_service.h"
#include "bbp/run_ownership.h"
#include "bbp/simulation_event_kind.h"
#include "bbp/simulator/options.h"
#include "bbp/util.h"
#include "simulator_managed_run_root.h"

namespace bbp {
namespace simulator_app_internal {
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

constexpr std::string_view kRetainedRunRegistrySummaryFileName =
    "run-registry-summary.json";
constexpr std::string_view kRetainedRunRegistrySummaryTemporaryName =
    ".run-registry-summary.json.tmp";
constexpr std::string_view kRetainedRunRegistrySummaryFormat =
    "bbp-retained-run-registry";
constexpr std::uint64_t kRetainedRunRegistrySummaryVersion = 1U;
constexpr std::size_t kMaximumRetainedRunRegistrySummaryBytes = 4096U;
constexpr std::size_t kMaximumRetainedRunResolvedScenarioBytes =
    4U * 1024U * 1024U;

class RetainedRunRegistryLimitExceeded : public std::runtime_error {
 public:
  using std::runtime_error::runtime_error;
};

const boost::json::value& RetainedRunRegistryField(
    const boost::json::object& object, std::string_view field,
    std::string_view context) {
  const boost::json::value* value = object.if_contains(field);
  if (value == nullptr) {
    throw std::runtime_error(std::string(context) +
                             " is missing field: " + std::string(field));
  }
  return *value;
}

std::string RetainedRunRegistryString(const boost::json::object& object,
                                      std::string_view field,
                                      std::string_view context) {
  const boost::json::value& value =
      RetainedRunRegistryField(object, field, context);
  if (!value.is_string()) {
    throw std::runtime_error(std::string(context) + " field " +
                             std::string(field) + " is not a string");
  }
  return std::string(value.as_string());
}

std::uint64_t RetainedRunRegistryUint64(const boost::json::object& object,
                                        std::string_view field,
                                        std::string_view context) {
  const boost::json::value& value =
      RetainedRunRegistryField(object, field, context);
  if (value.is_uint64()) {
    return value.as_uint64();
  }
  if (value.is_int64() && value.as_int64() >= 0) {
    return static_cast<std::uint64_t>(value.as_int64());
  }
  throw std::runtime_error(std::string(context) + " field " +
                           std::string(field) +
                           " is not a nonnegative integer");
}

std::uint32_t RetainedRunRegistryUint32(const boost::json::object& object,
                                        std::string_view field,
                                        std::string_view context) {
  const std::uint64_t value = RetainedRunRegistryUint64(object, field, context);
  if (value > std::numeric_limits<std::uint32_t>::max()) {
    throw std::runtime_error(std::string(context) + " field " +
                             std::string(field) + " exceeds uint32");
  }
  return static_cast<std::uint32_t>(value);
}

void RequireRetainedRunRegistryFields(
    const boost::json::object& object,
    std::initializer_list<std::string_view> expected,
    std::string_view context) {
  const std::set<std::string_view> fields(expected);
  if (object.size() != fields.size()) {
    throw std::runtime_error(std::string(context) +
                             " has an unexpected field count");
  }
  for (const auto& member : object) {
    const std::string_view field(member.key().data(), member.key().size());
    if (!fields.contains(field)) {
      throw std::runtime_error(std::string(context) +
                               " has unsupported field: " + std::string(field));
    }
  }
}

void ValidateRetainedRunRegistrySnapshot(
    const McpRetainedRunSnapshot& snapshot) {
  RequireSafeRunId(snapshot.run_id);
  if (snapshot.state != "finished" && snapshot.state != "failed" &&
      snapshot.state != "cancelled" && snapshot.state != "incomplete") {
    throw std::runtime_error("retained run registry state is invalid");
  }
  const ChainKind chain = ParseChainKind(snapshot.chain);
  if (snapshot.chain_node_maximum != ChainDriverSpecFor(chain).max_nodes ||
      snapshot.node_capacity > snapshot.chain_node_maximum ||
      snapshot.node_count > snapshot.node_capacity) {
    throw std::runtime_error(
        "retained run registry node bounds are inconsistent");
  }
}

std::uint32_t RetainedRunRegistryRuntimeNodeCount(
    const boost::json::object& publication) {
  static_cast<void>(RetainedRunRegistryUint64(publication, "generation",
                                              "runtime generation"));
  const std::uint32_t node_count = RetainedRunRegistryUint32(
      publication, "node_count", "runtime generation");
  const boost::json::value& node_ids =
      RetainedRunRegistryField(publication, "node_ids", "runtime generation");
  const boost::json::value& node_configs = RetainedRunRegistryField(
      publication, "node_configs", "runtime generation");
  const boost::json::value& topology =
      RetainedRunRegistryField(publication, "topology", "runtime generation");
  const boost::json::value& topology_current_edges = RetainedRunRegistryField(
      publication, "topology_current_edges", "runtime generation");
  if (!node_ids.is_array() || !node_configs.is_array() ||
      !topology.is_object() || !topology_current_edges.is_array() ||
      node_ids.as_array().size() != node_count ||
      node_configs.as_array().size() != node_count ||
      RetainedRunRegistryString(publication, "manifest_state",
                                "runtime generation") != "live") {
    throw std::runtime_error(
        "legacy retained runtime generation publication is incomplete");
  }

  std::set<std::string, std::less<>> active_node_ids;
  for (const boost::json::value& value : node_ids.as_array()) {
    if (!value.is_string() ||
        !active_node_ids.insert(std::string(value.as_string())).second) {
      throw std::runtime_error(
          "legacy retained runtime generation node ids are invalid");
    }
  }
  for (const boost::json::value& value : node_configs.as_array()) {
    if (!value.is_object()) {
      throw std::runtime_error(
          "legacy retained runtime generation node config is invalid");
    }
    const std::string node_id = RetainedRunRegistryString(
        value.as_object(), "id", "runtime generation node config");
    if (!active_node_ids.contains(node_id)) {
      throw std::runtime_error(
          "legacy retained runtime generation node config is inconsistent");
    }
  }
  return node_count;
}

McpRetainedRunSnapshot ParseRetainedRunRegistrySummary(
    std::string_view contents, std::string_view expected_run_id) {
  boost::json::value parsed;
  try {
    parsed = boost::json::parse(contents);
  } catch (const std::exception& error) {
    throw std::runtime_error("parse retained run registry summary failed: " +
                             std::string(error.what()));
  }
  if (!parsed.is_object()) {
    throw std::runtime_error(
        "retained run registry summary root is not an object");
  }
  const boost::json::object& document = parsed.as_object();
  RequireRetainedRunRegistryFields(
      document,
      {"format", "version", "run_id", "state", "chain", "node_count",
       "node_capacity", "chain_node_maximum"},
      "retained run registry summary");
  if (RetainedRunRegistryString(document, "format", "registry summary") !=
          kRetainedRunRegistrySummaryFormat ||
      RetainedRunRegistryUint64(document, "version", "registry summary") !=
          kRetainedRunRegistrySummaryVersion) {
    throw std::runtime_error(
        "retained run registry summary format or version is unsupported");
  }
  McpRetainedRunSnapshot snapshot{
      .run_id =
          RetainedRunRegistryString(document, "run_id", "registry summary"),
      .state = RetainedRunRegistryString(document, "state", "registry summary"),
      .chain = RetainedRunRegistryString(document, "chain", "registry summary"),
      .node_count =
          RetainedRunRegistryUint32(document, "node_count", "registry summary"),
      .node_capacity = RetainedRunRegistryUint32(document, "node_capacity",
                                                 "registry summary"),
      .chain_node_maximum = RetainedRunRegistryUint32(
          document, "chain_node_maximum", "registry summary"),
  };
  if (snapshot.run_id != expected_run_id) {
    throw std::runtime_error(
        "retained run registry summary run_id does not match its root");
  }
  ValidateRetainedRunRegistrySnapshot(snapshot);
  return snapshot;
}

std::optional<std::uintmax_t> RetainedRunRegistryFileSizeAt(
    int run_root_fd, std::string_view name) {
  const std::string name_text(name);
  struct stat status{};
  if (fstatat(run_root_fd, name_text.c_str(), &status, AT_SYMLINK_NOFOLLOW) !=
      0) {
    if (errno == ENOENT) {
      return std::nullopt;
    }
    throw std::system_error(errno, std::generic_category(),
                            "inspect retained run registry input " + name_text);
  }
  if (!S_ISREG(status.st_mode) || status.st_uid != geteuid() ||
      status.st_size < 0) {
    throw std::runtime_error(
        "retained run registry input is not an owned regular file: " +
        name_text);
  }
  return static_cast<std::uintmax_t>(status.st_size);
}

std::string ReadLegacyRetainedRunRegistryFile(
    int run_root_fd, std::string_view name, std::size_t maximum_file_bytes,
    std::size_t* remaining_bytes, std::stop_token stop_token, bool required) {
  const std::optional<std::uintmax_t> size =
      RetainedRunRegistryFileSizeAt(run_root_fd, name);
  if (!size) {
    if (required) {
      throw std::runtime_error("retained run registry input is missing: " +
                               std::string(name));
    }
    return {};
  }
  if (*size > maximum_file_bytes || *size > *remaining_bytes) {
    throw RetainedRunRegistryLimitExceeded(
        "legacy retained run metadata exceeds the registry byte bound");
  }
  const std::size_t bounded_size = static_cast<std::size_t>(*size);
  std::string contents = ReadTextAt(
      run_root_fd, name, std::max<std::size_t>(1U, bounded_size), stop_token);
  if (contents.size() != bounded_size) {
    throw std::runtime_error(
        "legacy retained run metadata size changed during its read");
  }
  *remaining_bytes -= bounded_size;
  return contents;
}

McpRetainedRunSnapshot LoadLegacyRetainedRunRegistrySnapshotAt(
    int run_root_fd, std::string_view run_id, std::size_t* remaining_bytes,
    std::stop_token stop_token) {
  const std::string resolved_text = ReadLegacyRetainedRunRegistryFile(
      run_root_fd, "resolved-scenario.json",
      kMaximumRetainedRunResolvedScenarioBytes, remaining_bytes, stop_token,
      true);
  boost::json::value resolved_value;
  try {
    resolved_value = boost::json::parse(resolved_text);
  } catch (const std::exception& error) {
    throw std::runtime_error(
        "parse legacy retained resolved scenario failed: " +
        std::string(error.what()));
  }
  if (!resolved_value.is_object()) {
    throw std::runtime_error(
        "legacy retained resolved scenario is not an object");
  }
  const boost::json::object& resolved = resolved_value.as_object();
  if (RetainedRunRegistryString(resolved, "run_id", "resolved scenario") !=
      run_id) {
    throw std::runtime_error(
        "legacy retained resolved scenario run_id does not match its root");
  }
  const std::string chain =
      RetainedRunRegistryString(resolved, "chain", "resolved scenario");
  const ChainKind chain_kind = ParseChainKind(chain);
  std::uint32_t node_count =
      RetainedRunRegistryUint32(resolved, "nodes", "resolved scenario");
  const boost::json::value* capacity_value =
      resolved.if_contains("node_capacity");
  const std::uint32_t node_capacity =
      capacity_value == nullptr
          ? node_count
          : RetainedRunRegistryUint32(resolved, "node_capacity",
                                      "resolved scenario");

  bool run_started = false;
  bool run_finished = false;
  bool run_failed = false;
  bool run_cancelled = false;
  const std::string events = ReadLegacyRetainedRunRegistryFile(
      run_root_fd, "events.jsonl", *remaining_bytes, remaining_bytes,
      stop_token, false);
  std::size_t offset = 0U;
  while (offset < events.size()) {
    if (stop_token.stop_requested()) {
      throw McpOperationCancelled();
    }
    const std::size_t end = events.find('\n', offset);
    if (end == std::string::npos) {
      break;
    }
    const std::string_view line(events.data() + offset, end - offset);
    offset = end + 1U;
    if (line.empty()) {
      throw std::runtime_error(
          "legacy retained event stream contains an empty record");
    }
    boost::json::value event_value;
    try {
      event_value = boost::json::parse(line);
    } catch (const std::exception& error) {
      throw std::runtime_error("parse legacy retained event record failed: " +
                               std::string(error.what()));
    }
    if (!event_value.is_object()) {
      throw std::runtime_error("legacy retained event record is not an object");
    }
    const boost::json::object& event = event_value.as_object();
    if (RetainedRunRegistryString(event, "run_id", "event record") != run_id) {
      throw std::runtime_error(
          "legacy retained event run_id does not match its root");
    }
    const std::string name =
        RetainedRunRegistryString(event, "event", "event record");
    if (name == SimulationEventKindName(SimulationEventKind::kRunStarted)) {
      run_started = true;
    } else if (name ==
               SimulationEventKindName(SimulationEventKind::kRunFinished)) {
      run_finished = true;
    } else if (name ==
               SimulationEventKindName(SimulationEventKind::kRunFailed)) {
      run_failed = true;
    } else if (name ==
               SimulationEventKindName(SimulationEventKind::kRunCancelled)) {
      run_cancelled = true;
    } else if (name == SimulationEventKindName(
                           SimulationEventKind::kRuntimeGenerationPublished)) {
      const std::string detail =
          RetainedRunRegistryString(event, "detail", "runtime generation");
      boost::json::value detail_value;
      try {
        detail_value = boost::json::parse(detail);
      } catch (const std::exception& error) {
        throw std::runtime_error(
            "parse legacy retained runtime generation failed: " +
            std::string(error.what()));
      }
      if (!detail_value.is_object()) {
        throw std::runtime_error(
            "legacy retained runtime generation detail is not an object");
      }
      node_count =
          RetainedRunRegistryRuntimeNodeCount(detail_value.as_object());
    }
  }

  McpRetainedRunSnapshot snapshot{
      .run_id = std::string(run_id),
      .state = run_failed ? "failed"
                          : (run_cancelled ? "cancelled"
                                           : (run_started && run_finished
                                                  ? "finished"
                                                  : "incomplete")),
      .chain = chain,
      .node_count = node_count,
      .node_capacity = node_capacity,
      .chain_node_maximum = ChainDriverSpecFor(chain_kind).max_nodes,
  };
  ValidateRetainedRunRegistrySnapshot(snapshot);
  return snapshot;
}

McpRetainedRunSnapshot LoadRetainedRunRegistrySnapshotAt(
    int run_root_fd, std::string_view run_id,
    std::size_t* remaining_legacy_bytes, std::stop_token stop_token) {
  const std::optional<std::uintmax_t> summary_size =
      RetainedRunRegistryFileSizeAt(run_root_fd,
                                    kRetainedRunRegistrySummaryFileName);
  if (!summary_size) {
    return LoadLegacyRetainedRunRegistrySnapshotAt(
        run_root_fd, run_id, remaining_legacy_bytes, stop_token);
  }
  if (*summary_size == 0U ||
      *summary_size > kMaximumRetainedRunRegistrySummaryBytes) {
    throw std::runtime_error(
        "retained run registry summary exceeds its byte bound");
  }
  return ParseRetainedRunRegistrySummary(
      ReadTextAt(run_root_fd, kRetainedRunRegistrySummaryFileName,
                 kMaximumRetainedRunRegistrySummaryBytes, stop_token),
      run_id);
}

[[noreturn]] void ThrowRetainedRunRegistryFailure(std::string_view code,
                                                  std::string message,
                                                  bool retryable) {
  throw McpOperationFailure(std::string(code), std::move(message), retryable);
}

}  // namespace

void WriteRetainedRunRegistrySummary(const Options& options,
                                     std::string_view state,
                                     std::uint32_t node_count) {
  if (!options.run_ownership) {
    return;
  }
  const RunOwnership& expected_ownership = *options.run_ownership;
  const std::filesystem::path run_root =
      BenchmarkRunRoot(options).lexically_normal();
  UniqueFileDescriptor run_root_fd(
      open(run_root.c_str(),
           O_RDONLY | O_DIRECTORY | O_CLOEXEC | O_NOFOLLOW | O_NONBLOCK));
  if (!run_root_fd.valid()) {
    throw std::system_error(errno, std::generic_category(),
                            "open retained run registry root");
  }
  const RunOwnership initial_ownership =
      LoadRunOwnershipAt(options.run_id, run_root, run_root_fd.get());
  if (initial_ownership != expected_ownership) {
    throw std::runtime_error(
        "retained run registry ownership does not match the launched run");
  }

  const ChainDriverSpec& chain_spec = ChainDriverSpecFor(options.chain);
  const McpRetainedRunSnapshot snapshot{
      .run_id = options.run_id,
      .state = std::string(state),
      .chain = std::string(ChainKindName(options.chain)),
      .node_count = node_count,
      .node_capacity = options.node_capacity,
      .chain_node_maximum = chain_spec.max_nodes,
  };
  ValidateRetainedRunRegistrySnapshot(snapshot);
  const boost::json::object document{
      {"format", kRetainedRunRegistrySummaryFormat},
      {"version", kRetainedRunRegistrySummaryVersion},
      {"run_id", snapshot.run_id},
      {"state", snapshot.state},
      {"chain", snapshot.chain},
      {"node_count", snapshot.node_count},
      {"node_capacity", snapshot.node_capacity},
      {"chain_node_maximum", snapshot.chain_node_maximum},
  };
  std::string contents = boost::json::serialize(document);
  if (contents.size() >= kMaximumRetainedRunRegistrySummaryBytes) {
    throw std::logic_error(
        "retained run registry summary exceeded its byte bound");
  }
  contents.push_back('\n');

  const std::string temporary_name(kRetainedRunRegistrySummaryTemporaryName);
  if (unlinkat(run_root_fd.get(), temporary_name.c_str(), 0) != 0 &&
      errno != ENOENT) {
    throw std::system_error(errno, std::generic_category(),
                            "remove stale retained registry summary temporary");
  }
  int output_fd =
      openat(run_root_fd.get(), temporary_name.c_str(),
             O_WRONLY | O_CREAT | O_EXCL | O_CLOEXEC | O_NOFOLLOW, 0600);
  if (output_fd < 0) {
    throw std::system_error(errno, std::generic_category(),
                            "create retained registry summary temporary");
  }
  bool published = false;
  try {
    std::size_t offset = 0U;
    while (offset < contents.size()) {
      const ssize_t written =
          write(output_fd, contents.data() + offset, contents.size() - offset);
      if (written < 0) {
        if (errno == EINTR) {
          continue;
        }
        throw std::system_error(errno, std::generic_category(),
                                "write retained registry summary");
      }
      if (written == 0) {
        throw std::runtime_error(
            "write retained registry summary made no progress");
      }
      offset += static_cast<std::size_t>(written);
    }
    if (fsync(output_fd) != 0) {
      throw std::system_error(errno, std::generic_category(),
                              "sync retained registry summary");
    }
    if (close(output_fd) != 0) {
      const int close_error = errno;
      output_fd = -1;
      throw std::system_error(close_error, std::generic_category(),
                              "close retained registry summary");
    }
    output_fd = -1;
    const std::string summary_name(kRetainedRunRegistrySummaryFileName);
    if (renameat(run_root_fd.get(), temporary_name.c_str(), run_root_fd.get(),
                 summary_name.c_str()) != 0) {
      throw std::system_error(errno, std::generic_category(),
                              "publish retained registry summary");
    }
    published = true;
    if (fsync(run_root_fd.get()) != 0) {
      throw std::system_error(errno, std::generic_category(),
                              "sync retained registry root");
    }
  } catch (...) {
    if (output_fd >= 0) {
      static_cast<void>(close(output_fd));
    }
    if (!published) {
      static_cast<void>(unlinkat(run_root_fd.get(), temporary_name.c_str(), 0));
    }
    throw;
  }

  const McpRetainedRunSnapshot read_back = ParseRetainedRunRegistrySummary(
      ReadTextAt(run_root_fd.get(), kRetainedRunRegistrySummaryFileName,
                 kMaximumRetainedRunRegistrySummaryBytes, {}),
      options.run_id);
  const RunOwnership final_ownership =
      LoadRunOwnershipAt(options.run_id, run_root, run_root_fd.get());
  if (read_back.run_id != snapshot.run_id ||
      read_back.state != snapshot.state || read_back.chain != snapshot.chain ||
      read_back.node_count != snapshot.node_count ||
      read_back.node_capacity != snapshot.node_capacity ||
      read_back.chain_node_maximum != snapshot.chain_node_maximum ||
      final_ownership != initial_ownership) {
    throw std::runtime_error(
        "retained run registry summary failed durable read-back");
  }
}

std::vector<McpRetainedRunSnapshot> DiscoverRetainedRuns(
    const std::filesystem::path& benchmark_root, std::string_view active_run_id,
    std::stop_token stop_token) {
  if (stop_token.stop_requested()) {
    throw McpOperationCancelled();
  }
  UniqueFileDescriptor root_fd(
      open(benchmark_root.c_str(),
           O_RDONLY | O_DIRECTORY | O_CLOEXEC | O_NOFOLLOW | O_NONBLOCK));
  if (!root_fd.valid()) {
    ThrowRetainedRunRegistryFailure(
        "retained_run_registry_unavailable",
        "open benchmark root for retained run discovery failed: " +
            std::error_code(errno, std::generic_category()).message(),
        true);
  }
  struct stat initial_root{};
  struct stat linked_root{};
  if (fstat(root_fd.get(), &initial_root) != 0 ||
      fstatat(AT_FDCWD, benchmark_root.c_str(), &linked_root,
              AT_SYMLINK_NOFOLLOW) != 0 ||
      !S_ISDIR(initial_root.st_mode) || !S_ISDIR(linked_root.st_mode) ||
      initial_root.st_dev != linked_root.st_dev ||
      initial_root.st_ino != linked_root.st_ino) {
    ThrowRetainedRunRegistryFailure(
        "retained_run_registry_unavailable",
        "benchmark root identity could not be verified for retained discovery",
        true);
  }

  const int duplicate_fd = fcntl(root_fd.get(), F_DUPFD_CLOEXEC, 0);
  if (duplicate_fd < 0) {
    ThrowRetainedRunRegistryFailure(
        "retained_run_registry_unavailable",
        "duplicate benchmark root descriptor for retained discovery failed: " +
            std::error_code(errno, std::generic_category()).message(),
        true);
  }
  DIR* raw_directory = fdopendir(duplicate_fd);
  if (raw_directory == nullptr) {
    const int directory_error = errno;
    static_cast<void>(close(duplicate_fd));
    ThrowRetainedRunRegistryFailure(
        "retained_run_registry_unavailable",
        "open benchmark root directory stream failed: " +
            std::error_code(directory_error, std::generic_category()).message(),
        true);
  }
  std::unique_ptr<DIR, int (*)(DIR*)> directory(raw_directory, closedir);

  std::vector<McpRetainedRunSnapshot> retained;
  std::size_t examined_entries = 0U;
  std::size_t remaining_legacy_bytes = kMcpHostMaximumLegacyRegistryBytes;
  while (true) {
    if (stop_token.stop_requested()) {
      throw McpOperationCancelled();
    }
    errno = 0;
    dirent* entry = readdir(directory.get());
    if (entry == nullptr) {
      if (errno != 0) {
        ThrowRetainedRunRegistryFailure(
            "retained_run_registry_unavailable",
            "enumerate benchmark root for retained runs failed: " +
                std::error_code(errno, std::generic_category()).message(),
            true);
      }
      break;
    }
    const std::string_view name(entry->d_name);
    if (name == "." || name == "..") {
      continue;
    }
    if (++examined_entries > kMcpHostMaximumRunRegistryEntries) {
      ThrowRetainedRunRegistryFailure(
          "retained_run_registry_limit_exceeded",
          "benchmark root contains more than 256 direct entries; retained run "
          "discovery refuses partial results",
          false);
    }
    try {
      RequireSafeRunId(name);
    } catch (const std::exception&) {
      continue;
    }
    if (!active_run_id.empty() && name == active_run_id) {
      continue;
    }

    int child_descriptor = -1;
    do {
      child_descriptor =
          openat(root_fd.get(), entry->d_name,
                 O_RDONLY | O_DIRECTORY | O_CLOEXEC | O_NOFOLLOW | O_NONBLOCK);
    } while (child_descriptor < 0 && errno == EINTR);
    if (child_descriptor < 0) {
      const int open_error = errno;
      if (open_error == ENOENT || open_error == ENOTDIR ||
          open_error == ELOOP || open_error == EACCES) {
        continue;
      }
      ThrowRetainedRunRegistryFailure(
          "retained_run_registry_unavailable",
          "open retained run candidate failed: " +
              std::error_code(open_error, std::generic_category()).message(),
          true);
    }
    UniqueFileDescriptor child_fd(child_descriptor);
    const std::string run_id(name);
    const std::filesystem::path run_root =
        (benchmark_root / run_id).lexically_normal();
    RunOwnership initial_ownership;
    try {
      initial_ownership =
          LoadRunOwnershipAt(run_id, run_root, child_fd.get(), stop_token);
    } catch (const std::exception&) {
      if (stop_token.stop_requested()) {
        throw McpOperationCancelled();
      }
      continue;
    }

    try {
      if (!RetainedRunRegistryFileSizeAt(child_fd.get(),
                                         kRetainedRunRegistrySummaryFileName) &&
          !RetainedRunRegistryFileSizeAt(child_fd.get(),
                                         "resolved-scenario.json")) {
        const RunOwnership final_ownership =
            LoadRunOwnershipAt(run_id, run_root, child_fd.get(), stop_token);
        if (final_ownership != initial_ownership) {
          ThrowRetainedRunRegistryFailure(
              "retained_run_registry_changed",
              "owned retained run identity changed during discovery: " + run_id,
              true);
        }
        continue;
      }
      McpRetainedRunSnapshot snapshot = LoadRetainedRunRegistrySnapshotAt(
          child_fd.get(), run_id, &remaining_legacy_bytes, stop_token);
      const RunOwnership final_ownership =
          LoadRunOwnershipAt(run_id, run_root, child_fd.get(), stop_token);
      if (final_ownership != initial_ownership) {
        ThrowRetainedRunRegistryFailure(
            "retained_run_registry_changed",
            "owned retained run identity changed during discovery: " + run_id,
            true);
      }
      retained.push_back(std::move(snapshot));
    } catch (const McpOperationCancelled&) {
      throw;
    } catch (const McpOperationFailure&) {
      throw;
    } catch (const RetainedRunRegistryLimitExceeded& error) {
      ThrowRetainedRunRegistryFailure(
          "retained_run_registry_limit_exceeded",
          "bounded legacy metadata discovery failed for " + run_id + ": " +
              error.what(),
          false);
    } catch (...) {
      const std::exception_ptr metadata_failure = std::current_exception();
      if (stop_token.stop_requested()) {
        throw McpOperationCancelled();
      }
      try {
        const RunOwnership final_ownership =
            LoadRunOwnershipAt(run_id, run_root, child_fd.get(), stop_token);
        if (final_ownership != initial_ownership) {
          ThrowRetainedRunRegistryFailure(
              "retained_run_registry_changed",
              "owned retained run identity changed during discovery: " + run_id,
              true);
        }
      } catch (const McpOperationFailure&) {
        throw;
      } catch (...) {
        ThrowRetainedRunRegistryFailure(
            "retained_run_registry_changed",
            "owned retained run disappeared or changed during discovery: " +
                run_id,
            true);
      }
      ThrowRetainedRunRegistryFailure(
          "retained_run_registry_invalid",
          "owned retained run metadata is invalid for " + run_id + ": " +
              ExceptionMessage(metadata_failure),
          false);
    }
  }

  struct stat final_root{};
  struct stat final_linked_root{};
  if (fstat(root_fd.get(), &final_root) != 0 ||
      fstatat(AT_FDCWD, benchmark_root.c_str(), &final_linked_root,
              AT_SYMLINK_NOFOLLOW) != 0 ||
      initial_root.st_dev != final_root.st_dev ||
      initial_root.st_ino != final_root.st_ino ||
      final_root.st_dev != final_linked_root.st_dev ||
      final_root.st_ino != final_linked_root.st_ino) {
    ThrowRetainedRunRegistryFailure(
        "retained_run_registry_changed",
        "benchmark root identity changed during retained run discovery", true);
  }
  if (stop_token.stop_requested()) {
    throw McpOperationCancelled();
  }
  std::sort(retained.begin(), retained.end(),
            [](const McpRetainedRunSnapshot& left,
               const McpRetainedRunSnapshot& right) {
              return left.run_id < right.run_id;
            });
  return retained;
}

}  // namespace simulator_app_internal
}  // namespace bbp
