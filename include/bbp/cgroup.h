#pragma once

#include <sys/types.h>

#include <compare>
#include <cstdint>
#include <filesystem>
#include <map>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

#include "bbp/run_ownership.h"

namespace bbp {

struct BlockDeviceId {
  std::uint32_t major = 0;
  std::uint32_t minor = 0;

  auto operator<=>(const BlockDeviceId&) const = default;
};

struct IoLimit {
  BlockDeviceId device;
  std::optional<std::uint64_t> read_bytes_per_sec;
  std::optional<std::uint64_t> write_bytes_per_sec;
  std::optional<std::uint64_t> read_operations_per_sec;
  std::optional<std::uint64_t> write_operations_per_sec;

  bool operator==(const IoLimit&) const = default;
};

BlockDeviceId ParseBlockDeviceId(std::string_view text);
std::string BlockDeviceIdText(const BlockDeviceId& device);

struct CgroupMetrics {
  uint64_t cpu_usage_usec = 0;
  uint64_t cpu_throttled_usec = 0;
  uint64_t cpu_pressure_some_total_usec = 0;
  uint64_t cpu_pressure_full_total_usec = 0;
  uint64_t memory_current = 0;
  uint64_t memory_peak = 0;
  std::optional<uint64_t> memory_high_limit_bytes;
  std::optional<uint64_t> memory_max_limit_bytes;
  std::optional<uint64_t> cpu_quota_us;
  uint64_t cpu_period_us = 0;
  uint64_t cpu_weight = 0;
  uint64_t io_weight = 0;
  std::vector<IoLimit> io_limits;
  uint64_t io_read_bytes = 0;
  uint64_t io_write_bytes = 0;
  uint64_t io_read_operations = 0;
  uint64_t io_write_operations = 0;
  uint64_t io_discard_bytes = 0;
  uint64_t io_discard_operations = 0;
  uint64_t io_pressure_some_total_usec = 0;
  uint64_t io_pressure_full_total_usec = 0;
  uint64_t pids_current = 0;
  std::optional<uint64_t> pids_max_limit;
  uint64_t pids_max_events = 0;
  uint64_t cgroup_populated = 0;
  uint64_t cgroup_frozen = 0;
  uint64_t memory_low = 0;
  uint64_t memory_high = 0;
  uint64_t memory_max = 0;
  uint64_t oom = 0;
  uint64_t oom_kill = 0;
  uint64_t oom_group_kill = 0;
  std::map<std::string, uint64_t> memory_stat;
};

struct CgroupFreezeProbe {
  std::string run_id;
  std::string node_id;
  pid_t child_pid = -1;
  bool frozen_after_freeze = false;
  bool frozen_after_thaw = false;
};

class Cgroup {
 public:
  static void PrepareRun(const std::string& run_id);
  static Cgroup Create(const std::string& run_id, const std::string& node_id);
  static void RemoveRun(const std::string& run_id);
  static void RemoveStaleRun(const RunOwnership& ownership);
  static CgroupFreezeProbe ProbeFreezeThaw();

  explicit Cgroup(std::filesystem::path path) : path_(std::move(path)) {}

  const std::filesystem::path& path() const { return path_; }

  void AttachPid(pid_t pid) const;
  void SetMemoryMax(uint64_t bytes) const;
  void SetMemoryHigh(uint64_t bytes) const;
  void SetCpuMax(std::optional<uint64_t> quota_us, uint64_t period_us) const;
  void SetCpuWeight(uint64_t weight) const;
  void SetIoMax(const IoLimit& limit) const;
  void SetIoWeight(uint64_t weight) const;
  void SetPidsMax(uint64_t n) const;
  CgroupMetrics ReadMetrics() const;
  void Freeze() const;
  void Thaw() const;
  bool Frozen() const;
  void KillAll() const;
  void Remove() const;

 private:
  std::filesystem::path path_;
};

#ifdef BBP_ENABLE_TEST_HOOKS
struct CgroupScopeTestConfig {
  std::filesystem::path root;
  std::string simulator_name;
  std::filesystem::path state_file;
  bool allow_root_process_move = true;
};

void PrepareCgroupRunInTestScope(const CgroupScopeTestConfig& config,
                                 const std::string& run_id);
void RemoveCgroupRunInTestScope(const CgroupScopeTestConfig& config,
                                const std::string& run_id);
#endif

}  // namespace bbp
