#pragma once

#include <cstdint>
#include <filesystem>
#include <optional>
#include <string>

#include <sys/types.h>

namespace bsim {

struct CgroupMetrics {
  uint64_t cpu_usage_usec = 0;
  uint64_t cpu_throttled_usec = 0;
  uint64_t cpu_pressure_some_total_usec = 0;
  uint64_t cpu_pressure_full_total_usec = 0;
  uint64_t memory_current = 0;
  uint64_t memory_peak = 0;
  uint64_t io_read_bytes = 0;
  uint64_t io_write_bytes = 0;
  uint64_t io_pressure_some_total_usec = 0;
  uint64_t io_pressure_full_total_usec = 0;
  uint64_t pids_current = 0;
  uint64_t pids_max_events = 0;
  uint64_t cgroup_populated = 0;
  uint64_t cgroup_frozen = 0;
  uint64_t memory_low = 0;
  uint64_t memory_high = 0;
  uint64_t memory_max = 0;
  uint64_t oom = 0;
  uint64_t oom_kill = 0;
  uint64_t oom_group_kill = 0;
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
  static Cgroup Create(const std::string& run_id, const std::string& node_id);
  static void RemoveRun(const std::string& run_id);
  static CgroupFreezeProbe ProbeFreezeThaw();

  explicit Cgroup(std::filesystem::path path) : path_(std::move(path)) {}

  const std::filesystem::path& path() const { return path_; }

  void AttachPid(pid_t pid) const;
  void SetMemoryMax(uint64_t bytes) const;
  void SetMemoryHigh(uint64_t bytes) const;
  void SetCpuMax(std::optional<uint64_t> quota_us, uint64_t period_us) const;
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

}  // namespace bsim
