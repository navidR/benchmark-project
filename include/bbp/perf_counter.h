#pragma once

#include <sys/types.h>

#include <cstdint>
#include <filesystem>
#include <memory>
#include <optional>
#include <stdexcept>
#include <string>
#include <string_view>
#include <vector>

namespace bbp {

enum class PerfCounterKind {
  kCycles,
  kInstructions,
  kCacheReferences,
  kCacheMisses,
  kBranchInstructions,
  kBranchMisses,
  kContextSwitches,
  kPageFaults,
  kTaskClock,
};

std::string_view PerfCounterKindName(PerfCounterKind kind);
std::optional<PerfCounterKind> PerfCounterKindFromName(std::string_view name);
const std::vector<PerfCounterKind>& DefaultPerfCounterKinds();

enum class PerfCounterTargetKind {
  kNode,
  kWallet,
  kGroup,
  kCgroup,
};

std::string_view PerfCounterTargetKindName(PerfCounterTargetKind kind);
std::optional<PerfCounterTargetKind> PerfCounterTargetKindFromName(
    std::string_view name);

struct PerfCounterTarget {
  PerfCounterTargetKind kind = PerfCounterTargetKind::kNode;
  std::string id;
  std::vector<std::string> node_ids;

  bool operator==(const PerfCounterTarget&) const = default;
};

enum class PerfCounterErrorKind {
  kInvalidArgument,
  kPermissionDenied,
  kProcessUnavailable,
  kUnsupported,
  kResourceExhausted,
  kInternal,
};

std::string_view PerfCounterErrorKindName(PerfCounterErrorKind kind);

class PerfCounterError : public std::runtime_error {
 public:
  PerfCounterError(PerfCounterErrorKind kind, int error_number,
                   const std::string& message);

  PerfCounterErrorKind kind() const { return kind_; }
  int error_number() const { return error_number_; }

 private:
  PerfCounterErrorKind kind_;
  int error_number_;
};

struct ScaledPerfCounterValue {
  std::optional<std::uint64_t> value;
  bool scaled = false;
  bool overflow = false;
};

ScaledPerfCounterValue ScalePerfCounterValue(std::uint64_t raw_value,
                                             std::uint64_t time_enabled,
                                             std::uint64_t time_running);

struct PerfCounterValue {
  PerfCounterKind kind = PerfCounterKind::kCycles;
  std::uint64_t raw_value = 0;
  std::optional<std::uint64_t> scaled_value;
  std::uint64_t time_enabled_ns = 0;
  std::uint64_t time_running_ns = 0;
  bool multiplexed = false;
  bool scaled = false;
  bool scaled_overflow = false;
};

class ProcessPerfCounters {
 public:
  static ProcessPerfCounters Open(pid_t pid,
                                  const std::vector<PerfCounterKind>& kinds);

  ProcessPerfCounters(const ProcessPerfCounters&) = delete;
  ProcessPerfCounters& operator=(const ProcessPerfCounters&) = delete;
  ProcessPerfCounters(ProcessPerfCounters&&) noexcept;
  ProcessPerfCounters& operator=(ProcessPerfCounters&&) noexcept;
  ~ProcessPerfCounters();

  pid_t pid() const;
  const std::vector<PerfCounterKind>& kinds() const;
  std::vector<PerfCounterValue> Read() const;

 private:
  class Impl;
  explicit ProcessPerfCounters(std::unique_ptr<Impl> impl);

  std::unique_ptr<Impl> impl_;
};

class CgroupPerfCounters {
 public:
  static CgroupPerfCounters Open(const std::filesystem::path& cgroup_path,
                                 const std::vector<PerfCounterKind>& kinds);

  CgroupPerfCounters(const CgroupPerfCounters&) = delete;
  CgroupPerfCounters& operator=(const CgroupPerfCounters&) = delete;
  CgroupPerfCounters(CgroupPerfCounters&&) noexcept;
  CgroupPerfCounters& operator=(CgroupPerfCounters&&) noexcept;
  ~CgroupPerfCounters();

  const std::filesystem::path& cgroup_path() const;
  const std::vector<PerfCounterKind>& kinds() const;
  const std::vector<int>& cpus() const;
  std::vector<PerfCounterValue> Read() const;

 private:
  class Impl;
  explicit CgroupPerfCounters(std::unique_ptr<Impl> impl);

  std::unique_ptr<Impl> impl_;
};

}  // namespace bbp
