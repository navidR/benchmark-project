#include "bbp/mcp_run_evidence.h"

#include <dirent.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

#include <algorithm>
#include <array>
#include <boost/beast/core/detail/base64.hpp>
#include <boost/json/parse.hpp>
#include <boost/json/serialize.hpp>
#include <cctype>
#include <cerrno>
#include <charconv>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <limits>
#include <map>
#include <memory>
#include <optional>
#include <set>
#include <stdexcept>
#include <string>
#include <string_view>
#include <system_error>
#include <utility>
#include <vector>

#include "bbp/mcp_operation_service.h"
#include "bbp/run_ownership.h"

namespace bbp {
namespace {

constexpr std::size_t kMaximumArtifactEntries = 4096U;
constexpr std::size_t kMaximumArtifactDepth = 16U;
constexpr std::size_t kMaximumArtifactPathBytes = 4096U;
constexpr std::size_t kMaximumEvidenceScannedRecords = 4096U;
constexpr std::uint64_t kMaximumEvidenceScannedBytes = 8U * 1024U * 1024U;
constexpr std::uint64_t kEvidenceSequenceOffsetBits = 56U;
constexpr std::uint64_t kEvidenceSequenceOffsetMask =
    (std::uint64_t{1U} << kEvidenceSequenceOffsetBits) - 1U;
constexpr std::size_t kEvidenceSourceCount = 4U;
constexpr std::size_t kEvidenceFileSourceCount = 3U;
constexpr std::size_t kEvidenceSnapshotWordCount = 4U;
constexpr std::uint8_t kNoPendingFamily = 0xffU;
constexpr std::string_view kEvidenceCursorPrefix = "mre1:";
constexpr std::size_t kEvidenceCursorSize =
    kEvidenceCursorPrefix.size() + 6U +
    16U * (kEvidenceFileSourceCount + kEvidenceSnapshotWordCount + 1U);
static_assert(kEvidenceCursorSize <= 256U);

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

std::runtime_error IoError(std::string_view operation) {
  return std::runtime_error(std::string(operation) + ": " +
                            std::strerror(errno));
}

void ThrowIfCancelled(std::stop_token stop_token) {
  if (stop_token.stop_requested()) {
    throw McpOperationCancelled();
  }
}

UniqueFd OpenOwnedRun(std::string_view run_id,
                      const std::filesystem::path& run_root) {
  const RunOwnership ownership =
      LoadRunOwnership(std::string(run_id), run_root);
  UniqueFd descriptor(open(ownership.run_root.c_str(),
                           O_RDONLY | O_DIRECTORY | O_CLOEXEC | O_NOFOLLOW));
  if (!descriptor.valid()) {
    throw IoError("open owned MCP run directory");
  }
  return descriptor;
}

UniqueFd OpenRegularAt(int directory_fd, std::string_view name) {
  const std::string text(name);
  UniqueFd descriptor(openat(directory_fd, text.c_str(),
                             O_RDONLY | O_CLOEXEC | O_NOFOLLOW | O_NONBLOCK));
  if (!descriptor.valid()) {
    if (errno == ENOENT) {
      return UniqueFd();
    }
    throw IoError("open owned MCP evidence file");
  }
  struct stat status{};
  if (fstat(descriptor.get(), &status) != 0) {
    throw IoError("inspect owned MCP evidence file");
  }
  if (!S_ISREG(status.st_mode)) {
    throw std::runtime_error("MCP evidence path is not a regular file");
  }
  return descriptor;
}

std::uint64_t FileSize(int fd) {
  struct stat status{};
  if (fstat(fd, &status) != 0) {
    throw IoError("inspect MCP run file");
  }
  if (status.st_size < 0) {
    throw std::runtime_error("MCP run file has a negative size");
  }
  return static_cast<std::uint64_t>(status.st_size);
}

struct JsonLine {
  std::uint64_t start = 0U;
  std::uint64_t next = 0U;
  std::string text;
};

std::optional<JsonLine> ReadJsonLine(int fd, std::uint64_t offset,
                                     std::stop_token stop_token) {
  const std::uint64_t file_size = FileSize(fd);
  if (offset > file_size) {
    throw std::runtime_error(
        "MCP evidence cursor exceeds the current source size");
  }
  if (offset == file_size) {
    return std::nullopt;
  }
  if (offset > static_cast<std::uint64_t>(std::numeric_limits<off_t>::max())) {
    throw std::runtime_error("MCP evidence cursor exceeds platform limits");
  }

  JsonLine line{.start = offset, .next = offset, .text = {}};
  std::array<char, 16384U> buffer{};
  while (line.next < file_size) {
    ThrowIfCancelled(stop_token);
    const std::uint64_t available = file_size - line.next;
    const std::size_t request = static_cast<std::size_t>(
        std::min<std::uint64_t>(available, buffer.size()));
    const ssize_t count =
        pread(fd, buffer.data(), request, static_cast<off_t>(line.next));
    if (count < 0) {
      if (errno == EINTR) {
        continue;
      }
      throw IoError("read MCP evidence file");
    }
    if (count == 0) {
      return std::nullopt;
    }
    const auto begin = buffer.begin();
    const auto end = begin + count;
    const auto newline = std::find(begin, end, '\n');
    const std::size_t consumed =
        static_cast<std::size_t>(std::distance(begin, newline));
    if (line.text.size() + consumed > kMcpMaximumEvidenceTextBytes) {
      throw std::runtime_error("MCP evidence record exceeds the byte bound");
    }
    line.text.append(buffer.data(), consumed);
    line.next += consumed;
    if (newline != end) {
      ++line.next;
      return line;
    }
  }
  // Writers append complete newline-delimited records. A trailing partial
  // record remains invisible until its newline is published.
  return std::nullopt;
}

std::optional<std::uint64_t> JsonUnsigned(const boost::json::object& object,
                                          std::string_view name) {
  const boost::json::value* value = object.if_contains(name);
  if (value == nullptr) {
    return std::nullopt;
  }
  if (value->is_uint64()) {
    return value->as_uint64();
  }
  if (value->is_int64() && value->as_int64() >= 0) {
    return static_cast<std::uint64_t>(value->as_int64());
  }
  return std::nullopt;
}

std::optional<std::string> JsonString(const boost::json::object& object,
                                      std::string_view name) {
  const boost::json::value* value = object.if_contains(name);
  if (value == nullptr || !value->is_string()) {
    return std::nullopt;
  }
  return std::string(value->as_string());
}

// Gregorian civil date to days since 1970-01-01. This avoids platform-local
// timezone state and remains portable to the later non-Linux build.
std::int64_t DaysFromCivil(int year, unsigned month, unsigned day) {
  year -= month <= 2U;
  const int era = (year >= 0 ? year : year - 399) / 400;
  const unsigned year_of_era = static_cast<unsigned>(year - era * 400);
  const int shifted_month = static_cast<int>(month) + (month > 2U ? -3 : 9);
  const unsigned day_of_year =
      (153U * static_cast<unsigned>(shifted_month) + 2U) / 5U + day - 1U;
  const unsigned day_of_era =
      year_of_era * 365U + year_of_era / 4U - year_of_era / 100U + day_of_year;
  return static_cast<std::int64_t>(era) * 146097 +
         static_cast<std::int64_t>(day_of_era) - 719468;
}

std::uint64_t TimestampMilliseconds(const boost::json::object& object) {
  if (const std::optional<std::uint64_t> value =
          JsonUnsigned(object, "timestamp_ms")) {
    return *value;
  }
  const std::optional<std::string> timestamp = JsonString(object, "timestamp");
  if (!timestamp || timestamp->size() != 20U || (*timestamp)[4] != '-' ||
      (*timestamp)[7] != '-' || (*timestamp)[10] != 'T' ||
      (*timestamp)[13] != ':' || (*timestamp)[16] != ':' ||
      (*timestamp)[19] != 'Z') {
    return 0U;
  }
  const auto parse = [&](std::size_t offset,
                         std::size_t count) -> std::optional<unsigned> {
    unsigned value = 0U;
    const auto [end, error] = std::from_chars(
        timestamp->data() + offset, timestamp->data() + offset + count, value);
    if (error != std::errc{} || end != timestamp->data() + offset + count) {
      return std::nullopt;
    }
    return value;
  };
  const auto year = parse(0U, 4U);
  const auto month = parse(5U, 2U);
  const auto day = parse(8U, 2U);
  const auto hour = parse(11U, 2U);
  const auto minute = parse(14U, 2U);
  const auto second = parse(17U, 2U);
  if (!year || !month || !day || !hour || !minute || !second || *month < 1U ||
      *month > 12U || *day < 1U || *day > 31U || *hour > 23U || *minute > 59U ||
      *second > 60U) {
    return 0U;
  }
  const std::int64_t days =
      DaysFromCivil(static_cast<int>(*year), *month, *day);
  if (days < 0) {
    return 0U;
  }
  const std::uint64_t seconds = static_cast<std::uint64_t>(days) * 86400U +
                                *hour * 3600U + *minute * 60U +
                                std::min(*second, 59U);
  if (seconds > std::numeric_limits<std::uint64_t>::max() / 1000U) {
    return 0U;
  }
  return seconds * 1000U;
}

bool Contains(std::string_view text, std::string_view fragment) {
  return text.find(fragment) != std::string_view::npos;
}

enum class EvidenceSource : std::uint8_t {
  kSnapshot,
  kEvents,
  kMetrics,
  kWalletMetrics,
  kCount,
};

static_assert(static_cast<std::size_t>(EvidenceSource::kCount) ==
              kEvidenceSourceCount);
static_assert(kEvidenceFileSourceCount + 1U == kEvidenceSourceCount);
static_assert(kEvidenceSnapshotWordCount * 64U >=
              (std::uint64_t{1U} << 8U) - 1U);

constexpr std::size_t SourceIndex(EvidenceSource source) {
  return static_cast<std::size_t>(source);
}

constexpr EvidenceSource NextSource(EvidenceSource source) {
  return static_cast<EvidenceSource>((SourceIndex(source) + 1U) %
                                     kEvidenceSourceCount);
}

std::size_t FileSourceIndex(EvidenceSource source) {
  if (source == EvidenceSource::kSnapshot || source == EvidenceSource::kCount) {
    throw std::logic_error("non-file MCP evidence source has no offset");
  }
  return SourceIndex(source) - 1U;
}

EvidenceSource SourceFor(McpInformationFamily family) {
  switch (family) {
    case McpInformationFamily::kEvents:
    case McpInformationFamily::kLogs:
    case McpInformationFamily::kLogHistory:
    case McpInformationFamily::kRpcFailures:
    case McpInformationFamily::kErrors:
    case McpInformationFamily::kCommandHistory:
    case McpInformationFamily::kLifecycle:
    case McpInformationFamily::kCleanupState:
    case McpInformationFamily::kTransactionLoad:
    case McpInformationFamily::kWorkloadHistory:
    case McpInformationFamily::kProgress:
      return EvidenceSource::kEvents;
    case McpInformationFamily::kMetrics:
    case McpInformationFamily::kMeasurements:
    case McpInformationFamily::kMeasurementHistory:
    case McpInformationFamily::kComparisons:
      return EvidenceSource::kMetrics;
    case McpInformationFamily::kWalletMetrics:
    case McpInformationFamily::kBalances:
      return EvidenceSource::kWalletMetrics;
    default:
      return EvidenceSource::kSnapshot;
  }
}

std::string_view SourceFile(EvidenceSource source) {
  switch (source) {
    case EvidenceSource::kEvents:
      return "events.jsonl";
    case EvidenceSource::kMetrics:
      return "metrics.jsonl";
    case EvidenceSource::kWalletMetrics:
      return "wallet-metrics.jsonl";
    case EvidenceSource::kSnapshot:
    case EvidenceSource::kCount:
      break;
  }
  throw std::logic_error("snapshot evidence has no JSONL source");
}

bool EventMatches(McpInformationFamily family, std::string_view kind) {
  switch (family) {
    case McpInformationFamily::kEvents:
      return true;
    case McpInformationFamily::kLogs:
    case McpInformationFamily::kLogHistory:
      return kind == "stdout_tail" || kind == "stderr_tail" ||
             kind == "daemon_log_tail";
    case McpInformationFamily::kRpcFailures:
      return Contains(kind, "rpc") || kind == "metrics_node_unavailable" ||
             kind == "wallet_metrics_unavailable";
    case McpInformationFamily::kErrors:
      return Contains(kind, "failed") || Contains(kind, "unavailable") ||
             Contains(kind, "error");
    case McpInformationFamily::kCommandHistory:
      return kind.starts_with("operator_command_");
    case McpInformationFamily::kLifecycle:
      return kind == "state" || kind.starts_with("run_") ||
             kind.starts_with("process_") || kind == "rpc_ready";
    case McpInformationFamily::kCleanupState:
      return kind == "network_removed" || kind == "cgroup_remove_failed" ||
             kind == "run_cgroup_remove_failed" || kind == "state";
    case McpInformationFamily::kTransactionLoad:
      return kind.starts_with("transaction_") ||
             kind == "wallet_transaction_submitted" ||
             kind == "raw_transaction_submitted";
    case McpInformationFamily::kWorkloadHistory:
      return kind.starts_with("scheduled_event_") ||
             kind == "checkpoint_recorded";
    case McpInformationFamily::kProgress:
      return Contains(kind, "progress") || Contains(kind, "started") ||
             Contains(kind, "completed") || Contains(kind, "failed");
    default:
      return true;
  }
}

bool MatchesFamily(McpInformationFamily family, EvidenceSource source,
                   const boost::json::object& object) {
  if (source != EvidenceSource::kEvents) {
    return true;
  }
  const std::optional<std::string> kind = JsonString(object, "event");
  return kind && EventMatches(family, *kind);
}

bool MatchesNode(const std::set<std::string, std::less<>>& node_ids,
                 const boost::json::object& object) {
  if (node_ids.empty()) {
    return true;
  }
  const std::optional<std::string> node_id = JsonString(object, "node_id");
  return node_id && node_ids.contains(*node_id);
}

std::uint64_t EncodeSequence(std::size_t family_index, std::uint64_t offset) {
  if (family_index >= (std::uint64_t{1U} << 8U) ||
      offset > kEvidenceSequenceOffsetMask) {
    throw std::runtime_error("MCP evidence sequence exceeds its encoded bound");
  }
  return (static_cast<std::uint64_t>(family_index)
          << kEvidenceSequenceOffsetBits) |
         offset;
}

std::pair<std::size_t, std::uint64_t> DecodeSequence(std::uint64_t sequence) {
  return {
      static_cast<std::size_t>(sequence >> kEvidenceSequenceOffsetBits),
      sequence & kEvidenceSequenceOffsetMask,
  };
}

std::string Hex64(std::uint64_t value) {
  constexpr std::array<char, 16U> kHex = {'0', '1', '2', '3', '4', '5',
                                          '6', '7', '8', '9', 'a', 'b',
                                          'c', 'd', 'e', 'f'};
  std::string result(16U, '0');
  for (std::size_t index = 0U; index < result.size(); ++index) {
    result[result.size() - index - 1U] = kHex[value & 0x0fU];
    value >>= 4U;
  }
  return result;
}

void AppendHexByte(std::string* output, std::uint8_t value) {
  const std::string encoded = Hex64(value);
  output->append(encoded.end() - 2, encoded.end());
}

std::uint64_t ParseCursorHex(std::string_view cursor, std::size_t* offset,
                             std::size_t digits) {
  if (*offset + digits > cursor.size()) {
    throw std::invalid_argument("MCP evidence cursor is truncated");
  }
  std::uint64_t value = 0U;
  const char* begin = cursor.data() + *offset;
  const char* end = begin + digits;
  const auto [parsed_end, error] = std::from_chars(begin, end, value, 16);
  if (error != std::errc{} || parsed_end != end) {
    throw std::invalid_argument("MCP evidence cursor is malformed");
  }
  *offset += digits;
  return value;
}

std::uint64_t FamilyOrderFingerprint(
    const std::vector<McpInformationFamily>& families) {
  std::uint64_t fingerprint = 14695981039346656037ULL;
  const auto mix = [&](std::uint64_t value) {
    for (std::size_t byte = 0U; byte < sizeof(value); ++byte) {
      fingerprint ^= value & 0xffU;
      fingerprint *= 1099511628211ULL;
      value >>= 8U;
    }
  };
  mix(families.size());
  for (const McpInformationFamily family : families) {
    mix(static_cast<std::uint64_t>(family));
  }
  return fingerprint;
}

struct EvidenceCursor {
  std::array<std::uint64_t, kEvidenceFileSourceCount> offsets{};
  std::array<std::uint64_t, kEvidenceSnapshotWordCount> snapshots{};
  std::uint64_t family_fingerprint = 0U;
  std::uint8_t next_source = 0U;
  std::uint8_t pending_family = kNoPendingFamily;
  std::uint8_t family_count = 0U;
};

bool SnapshotEmitted(const EvidenceCursor& cursor, std::size_t family_index) {
  const std::size_t word = family_index / 64U;
  const std::size_t bit = family_index % 64U;
  return (cursor.snapshots[word] & (std::uint64_t{1U} << bit)) != 0U;
}

void MarkSnapshotEmitted(EvidenceCursor* cursor, std::size_t family_index) {
  const std::size_t word = family_index / 64U;
  const std::size_t bit = family_index % 64U;
  cursor->snapshots[word] |= std::uint64_t{1U} << bit;
}

bool HasFamilyForSource(const std::vector<McpInformationFamily>& families,
                        EvidenceSource source) {
  return std::any_of(families.begin(), families.end(),
                     [source](McpInformationFamily family) {
                       return SourceFor(family) == source;
                     });
}

std::optional<std::size_t> NextFamilyForSource(
    const std::vector<McpInformationFamily>& families, EvidenceSource source,
    std::size_t begin) {
  for (std::size_t index = begin; index < families.size(); ++index) {
    if (SourceFor(families[index]) == source) {
      return index;
    }
  }
  return std::nullopt;
}

std::string EncodeOpaqueCursor(const EvidenceCursor& cursor) {
  std::string encoded(kEvidenceCursorPrefix);
  encoded.reserve(kEvidenceCursorSize);
  AppendHexByte(&encoded, cursor.next_source);
  AppendHexByte(&encoded, cursor.pending_family);
  AppendHexByte(&encoded, cursor.family_count);
  for (const std::uint64_t offset : cursor.offsets) {
    if (offset > kEvidenceSequenceOffsetMask) {
      throw std::runtime_error("MCP evidence cursor offset exceeds its bound");
    }
    encoded += Hex64(offset);
  }
  for (const std::uint64_t snapshots : cursor.snapshots) {
    encoded += Hex64(snapshots);
  }
  encoded += Hex64(cursor.family_fingerprint);
  if (encoded.size() != kEvidenceCursorSize) {
    throw std::logic_error("MCP evidence cursor has an invalid encoded size");
  }
  return encoded;
}

EvidenceCursor DecodeOpaqueCursor(const McpRunEvidenceQuery& query) {
  EvidenceCursor cursor{
      .family_fingerprint = FamilyOrderFingerprint(query.families),
      .family_count = static_cast<std::uint8_t>(query.families.size()),
  };
  if (query.cursor.empty()) {
    if (query.start_sequence == 0U) {
      return cursor;
    }
    if (query.families.size() != 1U) {
      throw std::invalid_argument(
          "MCP evidence start sequence requires one family");
    }
    const auto [family_index, offset] = DecodeSequence(query.start_sequence);
    if (family_index != 0U) {
      throw std::invalid_argument(
          "MCP evidence start sequence family is out of range");
    }
    const EvidenceSource source = SourceFor(query.families.front());
    if (source == EvidenceSource::kSnapshot) {
      throw std::invalid_argument(
          "MCP evidence snapshot family has no start sequence");
    }
    cursor.offsets[FileSourceIndex(source)] = offset;
    cursor.next_source = static_cast<std::uint8_t>(SourceIndex(source));
    return cursor;
  }
  if (query.start_sequence != 0U) {
    throw std::invalid_argument(
        "MCP evidence cursor and start sequence are mutually exclusive");
  }
  if (query.cursor.size() != kEvidenceCursorSize ||
      !query.cursor.starts_with(kEvidenceCursorPrefix)) {
    throw std::invalid_argument("MCP evidence cursor has an invalid format");
  }

  std::size_t offset = kEvidenceCursorPrefix.size();
  cursor.next_source =
      static_cast<std::uint8_t>(ParseCursorHex(query.cursor, &offset, 2U));
  cursor.pending_family =
      static_cast<std::uint8_t>(ParseCursorHex(query.cursor, &offset, 2U));
  cursor.family_count =
      static_cast<std::uint8_t>(ParseCursorHex(query.cursor, &offset, 2U));
  for (std::uint64_t& source_offset : cursor.offsets) {
    source_offset = ParseCursorHex(query.cursor, &offset, 16U);
  }
  for (std::uint64_t& snapshots : cursor.snapshots) {
    snapshots = ParseCursorHex(query.cursor, &offset, 16U);
  }
  cursor.family_fingerprint = ParseCursorHex(query.cursor, &offset, 16U);

  if (offset != query.cursor.size() ||
      cursor.next_source >= kEvidenceSourceCount ||
      cursor.family_count != query.families.size() ||
      cursor.family_fingerprint != FamilyOrderFingerprint(query.families)) {
    throw std::invalid_argument(
        "MCP evidence cursor does not match the family selection");
  }
  for (const std::uint64_t source_offset : cursor.offsets) {
    if (source_offset > kEvidenceSequenceOffsetMask) {
      throw std::invalid_argument("MCP evidence cursor offset is out of range");
    }
  }
  for (std::size_t family_index = 0U;
       family_index < kEvidenceSnapshotWordCount * 64U; ++family_index) {
    if (!SnapshotEmitted(cursor, family_index)) {
      continue;
    }
    if (family_index >= query.families.size() ||
        SourceFor(query.families[family_index]) != EvidenceSource::kSnapshot) {
      throw std::invalid_argument(
          "MCP evidence cursor snapshot state is invalid");
    }
  }
  if (cursor.pending_family != kNoPendingFamily) {
    if (cursor.pending_family >= query.families.size()) {
      throw std::invalid_argument(
          "MCP evidence cursor pending family is out of range");
    }
    const EvidenceSource pending_source =
        SourceFor(query.families[cursor.pending_family]);
    if (pending_source == EvidenceSource::kSnapshot ||
        SourceIndex(pending_source) != cursor.next_source) {
      throw std::invalid_argument(
          "MCP evidence cursor pending family source is invalid");
    }
  }
  return cursor;
}

boost::json::object EvidenceRecord(McpInformationFamily family,
                                   std::uint64_t sequence,
                                   boost::json::object object) {
  boost::json::object record{
      {"family", McpInformationFamilyName(family)},
      {"sequence", sequence},
      {"timestamp_ms", TimestampMilliseconds(object)},
  };
  if (const std::optional<std::string> node_id =
          JsonString(object, "node_id")) {
    record["node_id"] = *node_id;
  }
  if (const std::optional<std::string> kind = JsonString(object, "event")) {
    record["kind"] = *kind;
  }
  if (boost::json::value* detail = object.if_contains("detail");
      detail != nullptr && detail->is_string() &&
      !detail->as_string().empty()) {
    try {
      *detail = boost::json::parse(detail->as_string());
    } catch (const std::exception&) {
      // Non-JSON event details are intentionally retained as exact strings.
    }
  }
  record["data"] = std::move(object);
  return record;
}

std::string Lower(std::string_view value) {
  std::string result(value);
  std::transform(result.begin(), result.end(), result.begin(),
                 [](unsigned char character) {
                   return static_cast<char>(std::tolower(character));
                 });
  return result;
}

bool SensitiveArtifactName(std::string_view name) {
  const std::string lower = Lower(name);
  return Contains(lower, "cookie") || Contains(lower, "wallet.dat") ||
         Contains(lower, "private") || Contains(lower, "secret") ||
         Contains(lower, "token") || Contains(lower, "credential") ||
         Contains(lower, "rpcpassword") || Contains(lower, "masternode.conf") ||
         lower.ends_with(".key") || lower.ends_with(".pem");
}

bool TextArtifactSuffix(std::string_view name) {
  const std::string lower = Lower(name);
  for (const std::string_view suffix :
       {".json", ".jsonl", ".yaml", ".yml", ".log", ".txt", ".toml"}) {
    if (lower.ends_with(suffix)) {
      return true;
    }
  }
  return name == ".bbp-run";
}

std::uint64_t Fnv1a(std::string_view text, std::uint64_t seed) {
  std::uint64_t hash = seed;
  for (const unsigned char character : text) {
    hash ^= character;
    hash *= 1099511628211ULL;
  }
  return hash;
}

std::string ArtifactId(std::string_view relative_path) {
  return "artifact-" + Hex64(Fnv1a(relative_path, 14695981039346656037ULL)) +
         Hex64(Fnv1a(relative_path, 1099511628211ULL));
}

enum class ArtifactType { kFile, kDirectory, kSymlink, kOther };

std::string_view ArtifactTypeName(ArtifactType type) {
  switch (type) {
    case ArtifactType::kFile:
      return "file";
    case ArtifactType::kDirectory:
      return "directory";
    case ArtifactType::kSymlink:
      return "symlink";
    case ArtifactType::kOther:
      return "other";
  }
  return "other";
}

ArtifactType ArtifactTypeFor(const struct stat& status) {
  if (S_ISREG(status.st_mode)) {
    return ArtifactType::kFile;
  }
  if (S_ISDIR(status.st_mode)) {
    return ArtifactType::kDirectory;
  }
  if (S_ISLNK(status.st_mode)) {
    return ArtifactType::kSymlink;
  }
  return ArtifactType::kOther;
}

struct ArtifactEntry {
  std::string id;
  std::string relative_path;
  std::vector<std::string> components;
  ArtifactType type = ArtifactType::kOther;
  std::uint64_t size = 0U;
  std::uint64_t device = 0U;
  std::uint64_t inode = 0U;
  bool readable = false;
  bool collision = false;
};

struct ArtifactScan {
  std::vector<ArtifactEntry> entries;
  bool truncated = false;
};

bool SafeReadableArtifact(const ArtifactEntry& entry) {
  if (entry.type != ArtifactType::kFile || entry.components.empty() ||
      !TextArtifactSuffix(entry.components.back())) {
    return false;
  }
  for (const std::string& component : entry.components) {
    if (SensitiveArtifactName(component)) {
      return false;
    }
  }
  if (entry.components.front() == "mcp") {
    return false;
  }
  if (entry.components.front() == "nodes") {
    return Lower(entry.components.back()).ends_with(".log");
  }
  return true;
}

struct DirectoryListing {
  std::vector<std::string> names;
  bool truncated = false;
};

DirectoryListing DirectoryNames(int directory_fd, std::size_t remaining,
                                bool exclude_mcp, std::stop_token stop_token) {
  ThrowIfCancelled(stop_token);
  const int duplicate = fcntl(directory_fd, F_DUPFD_CLOEXEC, 0);
  if (duplicate < 0) {
    throw IoError("duplicate MCP artifact directory");
  }
  DirectoryPointer directory(fdopendir(duplicate));
  if (!directory) {
    const int error = errno;
    static_cast<void>(close(duplicate));
    errno = error;
    throw IoError("open MCP artifact directory stream");
  }
  DirectoryListing listing;
  listing.names.reserve(remaining);
  while (true) {
    ThrowIfCancelled(stop_token);
    errno = 0;
    dirent* entry = readdir(directory.get());
    if (entry == nullptr) {
      if (errno != 0) {
        throw IoError("read MCP artifact directory");
      }
      break;
    }
    const std::string_view name(entry->d_name);
    if (name == "." || name == ".." || (exclude_mcp && name == "mcp")) {
      continue;
    }
    if (listing.names.size() >= remaining) {
      listing.truncated = true;
      break;
    }
    listing.names.emplace_back(name);
  }
  std::sort(listing.names.begin(), listing.names.end());
  return listing;
}

std::string JoinPath(const std::vector<std::string>& components) {
  std::filesystem::path path;
  for (const std::string& component : components) {
    path /= component;
  }
  return path.generic_string();
}

void ScanArtifactsAt(int directory_fd, std::vector<std::string> components,
                     std::size_t depth, ArtifactScan* scan,
                     std::stop_token stop_token) {
  ThrowIfCancelled(stop_token);
  if (depth > kMaximumArtifactDepth) {
    scan->truncated = true;
    return;
  }
  const DirectoryListing listing = DirectoryNames(
      directory_fd, kMaximumArtifactEntries - scan->entries.size(), depth == 0U,
      stop_token);
  scan->truncated = scan->truncated || listing.truncated;
  for (const std::string& name : listing.names) {
    ThrowIfCancelled(stop_token);
    if (scan->entries.size() >= kMaximumArtifactEntries) {
      scan->truncated = true;
      return;
    }
    struct stat status{};
    if (fstatat(directory_fd, name.c_str(), &status, AT_SYMLINK_NOFOLLOW) !=
        0) {
      throw IoError("inspect MCP run artifact");
    }
    std::vector<std::string> child_components = components;
    child_components.push_back(name);
    const std::string relative_path = JoinPath(child_components);
    if (relative_path.size() > kMaximumArtifactPathBytes) {
      scan->truncated = true;
      continue;
    }
    ArtifactEntry entry{
        .id = ArtifactId(relative_path),
        .relative_path = relative_path,
        .components = child_components,
        .type = ArtifactTypeFor(status),
        .size = status.st_size < 0 ? 0U
                                   : static_cast<std::uint64_t>(status.st_size),
        .device = static_cast<std::uint64_t>(status.st_dev),
        .inode = static_cast<std::uint64_t>(status.st_ino),
    };
    entry.readable = SafeReadableArtifact(entry);
    scan->entries.push_back(std::move(entry));

    if (S_ISDIR(status.st_mode)) {
      UniqueFd child(openat(directory_fd, name.c_str(),
                            O_RDONLY | O_DIRECTORY | O_CLOEXEC | O_NOFOLLOW));
      if (!child.valid()) {
        throw IoError("open MCP run artifact directory");
      }
      ScanArtifactsAt(child.get(), std::move(child_components), depth + 1U,
                      scan, stop_token);
    }
  }
}

ArtifactScan ScanArtifacts(std::string_view run_id,
                           const std::filesystem::path& run_root,
                           std::stop_token stop_token) {
  ThrowIfCancelled(stop_token);
  UniqueFd root = OpenOwnedRun(run_id, run_root);
  ArtifactScan scan;
  ScanArtifactsAt(root.get(), {}, 0U, &scan, stop_token);
  std::map<std::string, std::size_t, std::less<>> ids;
  for (std::size_t index = 0U; index < scan.entries.size(); ++index) {
    ThrowIfCancelled(stop_token);
    const auto [existing, inserted] =
        ids.emplace(scan.entries[index].id, index);
    if (!inserted && scan.entries[existing->second].relative_path !=
                         scan.entries[index].relative_path) {
      scan.entries[existing->second].collision = true;
      scan.entries[existing->second].readable = false;
      scan.entries[index].collision = true;
      scan.entries[index].readable = false;
    }
  }
  return scan;
}

UniqueFd OpenArtifactEntry(int run_fd, const ArtifactEntry& entry) {
  if (entry.components.empty()) {
    throw std::logic_error("MCP artifact entry has no path components");
  }
  UniqueFd parent;
  int parent_fd = run_fd;
  for (std::size_t index = 0U; index + 1U < entry.components.size(); ++index) {
    UniqueFd next(openat(parent_fd, entry.components[index].c_str(),
                         O_RDONLY | O_DIRECTORY | O_CLOEXEC | O_NOFOLLOW));
    if (!next.valid()) {
      throw IoError("open MCP artifact parent directory");
    }
    parent = std::move(next);
    parent_fd = parent.get();
  }
  UniqueFd file(openat(parent_fd, entry.components.back().c_str(),
                       O_RDONLY | O_CLOEXEC | O_NOFOLLOW | O_NONBLOCK));
  if (!file.valid()) {
    throw IoError("open MCP artifact");
  }
  struct stat status{};
  if (fstat(file.get(), &status) != 0) {
    throw IoError("inspect MCP artifact");
  }
  if (!S_ISREG(status.st_mode) ||
      static_cast<std::uint64_t>(status.st_dev) != entry.device ||
      static_cast<std::uint64_t>(status.st_ino) != entry.inode) {
    throw std::runtime_error(
        "MCP artifact identity changed while the read was admitted");
  }
  return file;
}

std::string ReadAt(int fd, std::uint64_t offset, std::size_t limit,
                   std::stop_token stop_token) {
  if (offset > static_cast<std::uint64_t>(std::numeric_limits<off_t>::max())) {
    throw std::runtime_error("MCP artifact offset exceeds platform limits");
  }
  std::string content(limit, '\0');
  std::size_t total = 0U;
  while (total < content.size()) {
    ThrowIfCancelled(stop_token);
    const ssize_t count =
        pread(fd, content.data() + total, content.size() - total,
              static_cast<off_t>(offset + static_cast<std::uint64_t>(total)));
    if (count < 0) {
      if (errno == EINTR) {
        continue;
      }
      throw IoError("read MCP artifact");
    }
    if (count == 0) {
      break;
    }
    total += static_cast<std::size_t>(count);
  }
  content.resize(total);
  return content;
}

std::string Base64Encode(std::string_view content) {
  namespace beast = boost::beast;
  std::string encoded(beast::detail::base64::encoded_size(content.size()),
                      '\0');
  const std::size_t size = beast::detail::base64::encode(
      encoded.data(), content.data(), content.size());
  encoded.resize(size);
  return encoded;
}

}  // namespace

boost::json::object QueryMcpRunEvidence(std::string_view run_id,
                                        const std::filesystem::path& run_root,
                                        const McpRunEvidenceQuery& query,
                                        std::stop_token stop_token) {
  if (query.families.empty()) {
    throw std::invalid_argument("MCP evidence query requires families");
  }
  if (query.families.size() >= (1U << 8U)) {
    throw std::invalid_argument("MCP evidence family selection is excessive");
  }
  for (const McpInformationFamily family : query.families) {
    if (static_cast<std::size_t>(family) >=
        static_cast<std::size_t>(McpInformationFamily::kCount)) {
      throw std::invalid_argument("MCP evidence family is out of range");
    }
  }
  if (query.limit == 0U || query.limit > kMcpListPageSize) {
    throw std::invalid_argument("MCP evidence limit is out of range");
  }
  if (query.end_sequence && query.cursor.empty() &&
      *query.end_sequence < query.start_sequence) {
    throw std::invalid_argument(
        "MCP evidence end sequence precedes start sequence");
  }
  const std::set<std::string, std::less<>> node_ids(query.node_ids.begin(),
                                                    query.node_ids.end());
  if (node_ids.size() != query.node_ids.size()) {
    throw std::invalid_argument("MCP evidence node ids must be unique");
  }

  EvidenceCursor cursor = DecodeOpaqueCursor(query);
  boost::json::array items;
  std::size_t scanned_records = 0U;
  std::uint64_t scanned_bytes = 0U;
  bool scan_bound_reached = false;
  bool page_full = false;
  std::size_t idle_sources = 0U;

  ThrowIfCancelled(stop_token);
  UniqueFd root = OpenOwnedRun(run_id, run_root);
  std::array<UniqueFd, kEvidenceFileSourceCount> files;
  std::array<bool, kEvidenceFileSourceCount> source_open_attempted{};
  while (!page_full && !scan_bound_reached &&
         idle_sources < kEvidenceSourceCount) {
    ThrowIfCancelled(stop_token);
    const EvidenceSource source =
        static_cast<EvidenceSource>(cursor.next_source);
    if (source == EvidenceSource::kSnapshot) {
      std::optional<std::size_t> family_index;
      for (std::size_t index = 0U; index < query.families.size(); ++index) {
        if (SourceFor(query.families[index]) == EvidenceSource::kSnapshot &&
            !SnapshotEmitted(cursor, index)) {
          family_index = index;
          break;
        }
      }
      if (family_index) {
        const McpInformationFamily family = query.families[*family_index];
        items.emplace_back(boost::json::object{
            {"family", McpInformationFamilyName(family)},
            {"sequence", EncodeSequence(*family_index, 0U)},
            {"timestamp_ms", 0U},
            {"data",
             boost::json::object{
                 {"resource_uri",
                  "bbp:///" + std::string(McpInformationFamilyName(family))},
                 {"snapshot", true}}}});
        MarkSnapshotEmitted(&cursor, *family_index);
        idle_sources = 0U;
      } else {
        ++idle_sources;
      }
      cursor.pending_family = kNoPendingFamily;
      cursor.next_source =
          static_cast<std::uint8_t>(SourceIndex(NextSource(source)));
      page_full = items.size() >= query.limit;
      continue;
    }

    if (!HasFamilyForSource(query.families, source)) {
      cursor.pending_family = kNoPendingFamily;
      cursor.next_source =
          static_cast<std::uint8_t>(SourceIndex(NextSource(source)));
      ++idle_sources;
      continue;
    }

    if (scanned_records >= kMaximumEvidenceScannedRecords ||
        scanned_bytes >= kMaximumEvidenceScannedBytes) {
      scan_bound_reached = true;
      break;
    }
    const std::size_t source_index = FileSourceIndex(source);
    if (!source_open_attempted[source_index]) {
      files[source_index] = OpenRegularAt(root.get(), SourceFile(source));
      source_open_attempted[source_index] = true;
    }
    const bool had_pending_family = cursor.pending_family != kNoPendingFamily;
    if (!files[source_index].valid()) {
      if (cursor.offsets[source_index] != 0U || had_pending_family) {
        throw std::runtime_error(
            "MCP evidence source disappeared after the cursor was issued");
      }
      cursor.pending_family = kNoPendingFamily;
      cursor.next_source =
          static_cast<std::uint8_t>(SourceIndex(NextSource(source)));
      ++idle_sources;
      continue;
    }

    const std::optional<JsonLine> line = ReadJsonLine(
        files[source_index].get(), cursor.offsets[source_index], stop_token);
    if (!line) {
      if (had_pending_family) {
        throw std::runtime_error(
            "MCP evidence pending record is no longer available");
      }
      cursor.pending_family = kNoPendingFamily;
      cursor.next_source =
          static_cast<std::uint8_t>(SourceIndex(NextSource(source)));
      ++idle_sources;
      continue;
    }

    ++scanned_records;
    scanned_bytes += line->next - line->start;
    boost::json::value parsed = boost::json::parse(line->text);
    if (!parsed.is_object()) {
      throw std::runtime_error("MCP evidence record is not a JSON object");
    }
    const boost::json::value* record_run_id =
        parsed.as_object().if_contains("run_id");
    if (record_run_id == nullptr || !record_run_id->is_string() ||
        record_run_id->as_string() != run_id) {
      throw std::runtime_error(
          "MCP evidence record run_id does not match the selected run");
    }
    const bool node_matches = MatchesNode(node_ids, parsed.as_object());
    const std::size_t first_family =
        had_pending_family ? cursor.pending_family : 0U;
    cursor.pending_family = kNoPendingFamily;
    for (std::size_t family_index = first_family;
         family_index < query.families.size(); ++family_index) {
      const McpInformationFamily family = query.families[family_index];
      if (SourceFor(family) != source || !node_matches ||
          !MatchesFamily(family, source, parsed.as_object())) {
        continue;
      }
      const std::uint64_t sequence = EncodeSequence(family_index, line->start);
      if (query.end_sequence && sequence > *query.end_sequence) {
        continue;
      }
      items.emplace_back(EvidenceRecord(family, sequence, parsed.as_object()));
      if (items.size() < query.limit) {
        continue;
      }
      if (const std::optional<std::size_t> pending =
              NextFamilyForSource(query.families, source, family_index + 1U)) {
        cursor.pending_family = static_cast<std::uint8_t>(*pending);
        cursor.next_source = static_cast<std::uint8_t>(SourceIndex(source));
      } else {
        cursor.offsets[source_index] = line->next;
        cursor.next_source =
            static_cast<std::uint8_t>(SourceIndex(NextSource(source)));
      }
      page_full = true;
      break;
    }

    if (page_full) {
      continue;
    }
    cursor.offsets[source_index] = line->next;
    cursor.next_source =
        static_cast<std::uint8_t>(SourceIndex(NextSource(source)));
    idle_sources = 0U;
    scan_bound_reached = scanned_records >= kMaximumEvidenceScannedRecords ||
                         scanned_bytes >= kMaximumEvidenceScannedBytes;
  }

  const std::string next_cursor = EncodeOpaqueCursor(cursor);
  ThrowIfCancelled(stop_token);
  return boost::json::object{
      {"result_family", "evidence_page"},
      {"run_id", run_id},
      {"items", std::move(items)},
      {"next_cursor", next_cursor},
      {"truncated", page_full || scan_bound_reached},
  };
}

boost::json::object BuildMcpRunArtifactInventory(
    std::string_view run_id, const std::filesystem::path& run_root,
    std::stop_token stop_token) {
  const ArtifactScan scan = ScanArtifacts(run_id, run_root, stop_token);
  boost::json::array entries;
  entries.reserve(scan.entries.size());
  for (const ArtifactEntry& entry : scan.entries) {
    ThrowIfCancelled(stop_token);
    entries.emplace_back(boost::json::object{
        {"artifact_id", entry.id},
        {"relative_path", entry.relative_path},
        {"type", ArtifactTypeName(entry.type)},
        {"size", entry.size},
        {"readable", entry.readable},
        {"id_collision", entry.collision},
    });
  }
  ThrowIfCancelled(stop_token);
  return boost::json::object{
      {"entries", std::move(entries)},
      {"truncated", scan.truncated},
      {"maximum_entries", kMaximumArtifactEntries},
      {"maximum_depth", kMaximumArtifactDepth},
      {"credential_directory_excluded", true},
  };
}

boost::json::object ReadMcpRunArtifact(std::string_view run_id,
                                       const std::filesystem::path& run_root,
                                       std::string_view artifact_id,
                                       std::uint64_t offset, std::size_t limit,
                                       std::stop_token stop_token) {
  ThrowIfCancelled(stop_token);
  if (artifact_id.empty()) {
    throw std::invalid_argument("MCP artifact id must not be empty");
  }
  if (limit == 0U || limit > (1U << 20U)) {
    throw std::invalid_argument("MCP artifact read limit is out of range");
  }
  const ArtifactScan scan = ScanArtifacts(run_id, run_root, stop_token);
  const ArtifactEntry* selected = nullptr;
  for (const ArtifactEntry& entry : scan.entries) {
    ThrowIfCancelled(stop_token);
    if (entry.id != artifact_id) {
      continue;
    }
    if (selected != nullptr && selected->relative_path != entry.relative_path) {
      throw std::runtime_error("MCP artifact id collision is ambiguous");
    }
    selected = &entry;
  }
  if (selected == nullptr) {
    throw std::runtime_error("MCP artifact id is not present in this run");
  }
  if (selected->collision) {
    throw std::runtime_error("MCP artifact id collision is not readable");
  }
  if (!selected->readable) {
    throw std::runtime_error(
        "MCP artifact contents are not safe for public retrieval");
  }
  if (offset > selected->size) {
    throw std::invalid_argument("MCP artifact offset exceeds file size");
  }

  UniqueFd root = OpenOwnedRun(run_id, run_root);
  UniqueFd file = OpenArtifactEntry(root.get(), *selected);
  const std::size_t requested = static_cast<std::size_t>(
      std::min<std::uint64_t>(limit, selected->size - offset));
  const std::string content = ReadAt(file.get(), offset, requested, stop_token);
  ThrowIfCancelled(stop_token);
  const std::string encoded_content = Base64Encode(content);
  ThrowIfCancelled(stop_token);
  const std::uint64_t next_offset =
      offset + static_cast<std::uint64_t>(content.size());
  return boost::json::object{{"result_family", "artifact_content"},
                             {"run_id", run_id},
                             {"artifact_id", artifact_id},
                             {"offset", offset},
                             {"size", selected->size},
                             {"encoding", "base64"},
                             {"content", encoded_content},
                             {"next_offset", next_offset},
                             {"eof", next_offset == selected->size}};
}

}  // namespace bbp
