#include "bbp/perf_counter.h"

#include <fcntl.h>
#include <linux/perf_event.h>
#include <sys/ioctl.h>
#include <sys/syscall.h>
#include <unistd.h>
extern "C" {
#include <perf/core.h>
#include <perf/evsel.h>
#include <perf/threadmap.h>
}

#include <algorithm>
#include <boost/multiprecision/cpp_int.hpp>
#include <cerrno>
#include <cstring>
#include <limits>
#include <memory>
#include <mutex>
#include <set>
#include <string>
#include <utility>

namespace bbp {
namespace {

static_assert(sizeof(perf_event_attr) == 144U,
              "perf_event_attr must match the vendored libperf ABI");

struct PerfEventEncoding {
  std::uint32_t type;
  std::uint64_t config;
};

PerfEventEncoding CounterEncoding(PerfCounterKind kind) {
  switch (kind) {
    case PerfCounterKind::kCycles:
      return {PERF_TYPE_HARDWARE, PERF_COUNT_HW_CPU_CYCLES};
    case PerfCounterKind::kInstructions:
      return {PERF_TYPE_HARDWARE, PERF_COUNT_HW_INSTRUCTIONS};
    case PerfCounterKind::kCacheReferences:
      return {PERF_TYPE_HARDWARE, PERF_COUNT_HW_CACHE_REFERENCES};
    case PerfCounterKind::kCacheMisses:
      return {PERF_TYPE_HARDWARE, PERF_COUNT_HW_CACHE_MISSES};
    case PerfCounterKind::kBranchInstructions:
      return {PERF_TYPE_HARDWARE, PERF_COUNT_HW_BRANCH_INSTRUCTIONS};
    case PerfCounterKind::kBranchMisses:
      return {PERF_TYPE_HARDWARE, PERF_COUNT_HW_BRANCH_MISSES};
    case PerfCounterKind::kContextSwitches:
      return {PERF_TYPE_SOFTWARE, PERF_COUNT_SW_CONTEXT_SWITCHES};
    case PerfCounterKind::kPageFaults:
      return {PERF_TYPE_SOFTWARE, PERF_COUNT_SW_PAGE_FAULTS};
    case PerfCounterKind::kTaskClock:
      return {PERF_TYPE_SOFTWARE, PERF_COUNT_SW_TASK_CLOCK};
  }
  throw PerfCounterError(PerfCounterErrorKind::kInvalidArgument, EINVAL,
                         "unknown perf counter kind");
}

int SilentLibperfLog(enum libperf_print_level level, const char* format,
                     va_list arguments) {
  (void)level;
  (void)format;
  (void)arguments;
  return 0;
}

void InitializeLibperf() {
  static std::once_flag initialized;
  std::call_once(initialized, [] { libperf_init(SilentLibperfLog); });
}

PerfCounterErrorKind ClassifyPerfError(int error_number) {
  switch (error_number) {
    case EACCES:
    case EPERM:
      return PerfCounterErrorKind::kPermissionDenied;
    case ESRCH:
      return PerfCounterErrorKind::kProcessUnavailable;
    case EINVAL:
    case ENODEV:
    case ENOENT:
    case ENOSYS:
    case EOPNOTSUPP:
      return PerfCounterErrorKind::kUnsupported;
    case EAGAIN:
    case EMFILE:
    case ENFILE:
    case ENOMEM:
    case ENOSPC:
      return PerfCounterErrorKind::kResourceExhausted;
    default:
      return PerfCounterErrorKind::kInternal;
  }
}

[[noreturn]] void ThrowPerfOperationError(std::string_view operation,
                                          PerfCounterKind kind, int result) {
  int error_number = result < -1 ? -result : errno;
  if (error_number <= 0) {
    error_number = EIO;
  }
  throw PerfCounterError(ClassifyPerfError(error_number), error_number,
                         std::string(operation) + " for " +
                             std::string(PerfCounterKindName(kind)) +
                             " failed: " + std::strerror(error_number));
}

struct ThreadMapDeleter {
  void operator()(perf_thread_map* threads) const {
    perf_thread_map__put(threads);
  }
};

struct EvselDeleter {
  void operator()(perf_evsel* event) const {
    if (event == nullptr) {
      return;
    }
    perf_evsel__close(event);
    perf_evsel__delete(event);
  }
};

using ThreadMapPtr = std::unique_ptr<perf_thread_map, ThreadMapDeleter>;
using EvselPtr = std::unique_ptr<perf_evsel, EvselDeleter>;

class UniqueFd {
 public:
  UniqueFd() = default;
  explicit UniqueFd(int fd) : fd_(fd) {}
  UniqueFd(const UniqueFd&) = delete;
  UniqueFd& operator=(const UniqueFd&) = delete;
  UniqueFd(UniqueFd&& other) noexcept : fd_(std::exchange(other.fd_, -1)) {}
  UniqueFd& operator=(UniqueFd&& other) noexcept {
    if (this != &other) {
      Reset();
      fd_ = std::exchange(other.fd_, -1);
    }
    return *this;
  }
  ~UniqueFd() { Reset(); }

  int get() const { return fd_; }

 private:
  void Reset() {
    if (fd_ >= 0) {
      (void)close(fd_);
      fd_ = -1;
    }
  }

  int fd_ = -1;
};

void ValidateCounterSelection(const std::vector<PerfCounterKind>& kinds) {
  if (kinds.empty()) {
    throw PerfCounterError(PerfCounterErrorKind::kInvalidArgument, EINVAL,
                           "perf counter selection must not be empty");
  }
  const std::set<PerfCounterKind> unique(kinds.begin(), kinds.end());
  if (unique.size() != kinds.size()) {
    throw PerfCounterError(PerfCounterErrorKind::kInvalidArgument, EINVAL,
                           "perf counter selection contains duplicates");
  }
}

[[noreturn]] void ThrowPerfSystemError(std::string_view operation,
                                       PerfCounterKind kind, int error_number) {
  if (error_number <= 0) {
    error_number = EIO;
  }
  throw PerfCounterError(ClassifyPerfError(error_number), error_number,
                         std::string(operation) + " for " +
                             std::string(PerfCounterKindName(kind)) +
                             " failed: " + std::strerror(error_number));
}

int OpenCgroupPerfEvent(const perf_event_attr& attributes, int cgroup_fd,
                        int cpu) {
  return static_cast<int>(syscall(SYS_perf_event_open, &attributes, cgroup_fd,
                                  cpu, -1,
                                  PERF_FLAG_PID_CGROUP | PERF_FLAG_FD_CLOEXEC));
}

std::vector<int> AllowedCpus() {
  constexpr std::size_t kWordBits = sizeof(unsigned long) * 8U;
  constexpr std::size_t kMaximumMaskBytes = 1024U * 1024U;
  std::size_t mask_bytes = 128U;
  while (mask_bytes <= kMaximumMaskBytes) {
    std::vector<unsigned long> mask((mask_bytes + sizeof(unsigned long) - 1U) /
                                    sizeof(unsigned long));
    errno = 0;
    const long result = syscall(SYS_sched_getaffinity, 0, mask_bytes,
                                static_cast<void*>(mask.data()));
    if (result < 0) {
      if (errno == EINVAL && mask_bytes < kMaximumMaskBytes) {
        mask_bytes *= 2U;
        continue;
      }
      const int error_number = errno == 0 ? EIO : errno;
      throw PerfCounterError(
          ClassifyPerfError(error_number), error_number,
          "read simulator CPU affinity for cgroup perf failed: " +
              std::string(std::strerror(error_number)));
    }

    const std::size_t populated_bytes = static_cast<std::size_t>(result);
    const std::size_t populated_words =
        std::min(mask.size(), (populated_bytes + sizeof(unsigned long) - 1U) /
                                  sizeof(unsigned long));
    std::vector<int> cpus;
    for (std::size_t word_index = 0; word_index < populated_words;
         ++word_index) {
      const unsigned long word = mask[word_index];
      for (std::size_t bit = 0; bit < kWordBits; ++bit) {
        if ((word & (1UL << bit)) == 0UL) {
          continue;
        }
        const std::size_t cpu = word_index * kWordBits + bit;
        if (cpu > static_cast<std::size_t>(std::numeric_limits<int>::max())) {
          throw PerfCounterError(PerfCounterErrorKind::kUnsupported, EOVERFLOW,
                                 "CPU index exceeds int for cgroup perf");
        }
        cpus.push_back(static_cast<int>(cpu));
      }
    }
    if (cpus.empty()) {
      throw PerfCounterError(PerfCounterErrorKind::kUnsupported, ENODEV,
                             "simulator has no allowed CPUs for cgroup perf");
    }
    return cpus;
  }
  throw PerfCounterError(PerfCounterErrorKind::kUnsupported, EOVERFLOW,
                         "CPU affinity mask exceeds cgroup perf limit");
}

struct DirectPerfReadValue {
  std::uint64_t value = 0;
  std::uint64_t time_enabled = 0;
  std::uint64_t time_running = 0;
};

DirectPerfReadValue ReadDirectPerfEvent(int fd, PerfCounterKind kind) {
  DirectPerfReadValue value;
  std::size_t offset = 0;
  while (offset < sizeof(value)) {
    const ssize_t count = read(fd, reinterpret_cast<char*>(&value) + offset,
                               sizeof(value) - offset);
    if (count > 0) {
      offset += static_cast<std::size_t>(count);
      continue;
    }
    if (count < 0 && errno == EINTR) {
      continue;
    }
    ThrowPerfSystemError("read cgroup perf event", kind,
                         count == 0 ? EIO : errno);
  }
  return value;
}

void EnableDirectPerfEvent(int fd, PerfCounterKind kind) {
  while (ioctl(fd, PERF_EVENT_IOC_ENABLE, 0) != 0) {
    if (errno == EINTR) {
      continue;
    }
    ThrowPerfSystemError("enable cgroup perf event", kind, errno);
  }
}

std::uint64_t SaturatingUint64(const boost::multiprecision::uint128_t& value,
                               bool* overflow) {
  const boost::multiprecision::uint128_t maximum =
      std::numeric_limits<std::uint64_t>::max();
  if (value > maximum) {
    *overflow = true;
    return std::numeric_limits<std::uint64_t>::max();
  }
  return static_cast<std::uint64_t>(value);
}

}  // namespace

std::string_view PerfCounterKindName(PerfCounterKind kind) {
  switch (kind) {
    case PerfCounterKind::kCycles:
      return "cycles";
    case PerfCounterKind::kInstructions:
      return "instructions";
    case PerfCounterKind::kCacheReferences:
      return "cache-references";
    case PerfCounterKind::kCacheMisses:
      return "cache-misses";
    case PerfCounterKind::kBranchInstructions:
      return "branch-instructions";
    case PerfCounterKind::kBranchMisses:
      return "branch-misses";
    case PerfCounterKind::kContextSwitches:
      return "context-switches";
    case PerfCounterKind::kPageFaults:
      return "page-faults";
    case PerfCounterKind::kTaskClock:
      return "task-clock";
  }
  throw PerfCounterError(PerfCounterErrorKind::kInvalidArgument, EINVAL,
                         "unknown perf counter kind");
}

std::optional<PerfCounterKind> PerfCounterKindFromName(std::string_view name) {
  for (const PerfCounterKind kind : DefaultPerfCounterKinds()) {
    if (PerfCounterKindName(kind) == name) {
      return kind;
    }
  }
  return std::nullopt;
}

const std::vector<PerfCounterKind>& DefaultPerfCounterKinds() {
  static const std::vector<PerfCounterKind> kinds = {
      PerfCounterKind::kCycles,
      PerfCounterKind::kInstructions,
      PerfCounterKind::kCacheReferences,
      PerfCounterKind::kCacheMisses,
      PerfCounterKind::kBranchInstructions,
      PerfCounterKind::kBranchMisses,
      PerfCounterKind::kContextSwitches,
      PerfCounterKind::kPageFaults,
      PerfCounterKind::kTaskClock,
  };
  return kinds;
}

std::string_view PerfCounterTargetKindName(PerfCounterTargetKind kind) {
  switch (kind) {
    case PerfCounterTargetKind::kNode:
      return "node";
    case PerfCounterTargetKind::kWallet:
      return "wallet";
    case PerfCounterTargetKind::kGroup:
      return "group";
    case PerfCounterTargetKind::kCgroup:
      return "cgroup";
  }
  throw PerfCounterError(PerfCounterErrorKind::kInvalidArgument, EINVAL,
                         "unknown perf counter target kind");
}

std::optional<PerfCounterTargetKind> PerfCounterTargetKindFromName(
    std::string_view name) {
  if (name == "node") {
    return PerfCounterTargetKind::kNode;
  }
  if (name == "wallet") {
    return PerfCounterTargetKind::kWallet;
  }
  if (name == "group") {
    return PerfCounterTargetKind::kGroup;
  }
  if (name == "cgroup") {
    return PerfCounterTargetKind::kCgroup;
  }
  return std::nullopt;
}

std::string_view PerfCounterErrorKindName(PerfCounterErrorKind kind) {
  switch (kind) {
    case PerfCounterErrorKind::kInvalidArgument:
      return "invalid_argument";
    case PerfCounterErrorKind::kPermissionDenied:
      return "permission_denied";
    case PerfCounterErrorKind::kProcessUnavailable:
      return "process_unavailable";
    case PerfCounterErrorKind::kUnsupported:
      return "unsupported";
    case PerfCounterErrorKind::kResourceExhausted:
      return "resource_exhausted";
    case PerfCounterErrorKind::kInternal:
      return "internal";
  }
  return "internal";
}

PerfCounterError::PerfCounterError(PerfCounterErrorKind kind, int error_number,
                                   const std::string& message)
    : std::runtime_error(message), kind_(kind), error_number_(error_number) {}

ScaledPerfCounterValue ScalePerfCounterValue(std::uint64_t raw_value,
                                             std::uint64_t time_enabled,
                                             std::uint64_t time_running) {
  if (time_running == 0U) {
    return {};
  }
  if (time_running >= time_enabled) {
    return {.value = raw_value};
  }

  using boost::multiprecision::uint128_t;
  const uint128_t scaled =
      (uint128_t(raw_value) * uint128_t(time_enabled)) / time_running;
  const uint128_t maximum = std::numeric_limits<std::uint64_t>::max();
  if (scaled > maximum) {
    return {.value = std::numeric_limits<std::uint64_t>::max(),
            .scaled = true,
            .overflow = true};
  }
  return {.value = static_cast<std::uint64_t>(scaled), .scaled = true};
}

class ProcessPerfCounters::Impl {
 public:
  Impl(pid_t process_pid, std::vector<PerfCounterKind> selected_kinds,
       ThreadMapPtr selected_threads)
      : pid(process_pid),
        kinds(std::move(selected_kinds)),
        threads(std::move(selected_threads)) {}

  pid_t pid;
  std::vector<PerfCounterKind> kinds;
  ThreadMapPtr threads;
  std::vector<EvselPtr> events;
};

ProcessPerfCounters ProcessPerfCounters::Open(
    pid_t pid, const std::vector<PerfCounterKind>& kinds) {
  if (pid <= 0) {
    throw PerfCounterError(PerfCounterErrorKind::kInvalidArgument, EINVAL,
                           "perf counter PID must be positive");
  }
  ValidateCounterSelection(kinds);

  InitializeLibperf();
  ThreadMapPtr threads(perf_thread_map__new_dummy());
  if (!threads) {
    throw PerfCounterError(PerfCounterErrorKind::kResourceExhausted, ENOMEM,
                           "create libperf thread map failed: out of memory");
  }
  perf_thread_map__set_pid(threads.get(), 0, pid);

  auto impl = std::make_unique<Impl>(pid, kinds, std::move(threads));
  impl->events.reserve(kinds.size());
  for (const PerfCounterKind kind : kinds) {
    const PerfEventEncoding encoding = CounterEncoding(kind);
    perf_event_attr attributes{};
    attributes.size = sizeof(attributes);
    attributes.type = encoding.type;
    attributes.config = encoding.config;
    attributes.read_format =
        PERF_FORMAT_TOTAL_TIME_ENABLED | PERF_FORMAT_TOTAL_TIME_RUNNING;
    attributes.disabled = 1;
    attributes.inherit = 1;
    attributes.inherit_stat = 1;

    EvselPtr event(perf_evsel__new(&attributes));
    if (!event) {
      throw PerfCounterError(PerfCounterErrorKind::kResourceExhausted, ENOMEM,
                             "create libperf event selector for " +
                                 std::string(PerfCounterKindName(kind)) +
                                 " failed: out of memory");
    }
    errno = 0;
    const int opened =
        perf_evsel__open(event.get(), nullptr, impl->threads.get());
    if (opened != 0) {
      ThrowPerfOperationError("open libperf event", kind, opened);
    }
    impl->events.push_back(std::move(event));
  }

  for (std::size_t index = 0; index < impl->events.size(); ++index) {
    errno = 0;
    const int enabled = perf_evsel__enable(impl->events[index].get());
    if (enabled != 0) {
      ThrowPerfOperationError("enable libperf event", kinds[index], enabled);
    }
  }
  return ProcessPerfCounters(std::move(impl));
}

ProcessPerfCounters::ProcessPerfCounters(std::unique_ptr<Impl> impl)
    : impl_(std::move(impl)) {}

ProcessPerfCounters::ProcessPerfCounters(ProcessPerfCounters&&) noexcept =
    default;
ProcessPerfCounters& ProcessPerfCounters::operator=(
    ProcessPerfCounters&&) noexcept = default;
ProcessPerfCounters::~ProcessPerfCounters() = default;

pid_t ProcessPerfCounters::pid() const { return impl_->pid; }

const std::vector<PerfCounterKind>& ProcessPerfCounters::kinds() const {
  return impl_->kinds;
}

std::vector<PerfCounterValue> ProcessPerfCounters::Read() const {
  std::vector<PerfCounterValue> values;
  values.reserve(impl_->events.size());
  for (std::size_t index = 0; index < impl_->events.size(); ++index) {
    perf_counts_values counts{};
    errno = 0;
    const int result =
        perf_evsel__read(impl_->events[index].get(), 0, 0, &counts);
    if (result != 0) {
      ThrowPerfOperationError("read libperf event", impl_->kinds[index],
                              result);
    }
    const ScaledPerfCounterValue scaled =
        ScalePerfCounterValue(counts.val, counts.ena, counts.run);
    values.push_back({
        .kind = impl_->kinds[index],
        .raw_value = counts.val,
        .scaled_value = scaled.value,
        .time_enabled_ns = counts.ena,
        .time_running_ns = counts.run,
        .multiplexed = counts.run < counts.ena,
        .scaled = scaled.scaled,
        .scaled_overflow = scaled.overflow,
    });
  }
  return values;
}

class CgroupPerfCounters::Impl {
 public:
  struct Event {
    PerfCounterKind kind = PerfCounterKind::kCycles;
    std::vector<UniqueFd> fds;
  };

  std::filesystem::path cgroup_path;
  std::vector<PerfCounterKind> kinds;
  std::vector<int> cpus;
  UniqueFd cgroup_fd;
  std::vector<Event> events;
};

CgroupPerfCounters CgroupPerfCounters::Open(
    const std::filesystem::path& cgroup_path,
    const std::vector<PerfCounterKind>& kinds) {
  if (cgroup_path.empty()) {
    throw PerfCounterError(PerfCounterErrorKind::kInvalidArgument, EINVAL,
                           "perf cgroup path must not be empty");
  }
  ValidateCounterSelection(kinds);
  InitializeLibperf();

  const int cgroup_fd =
      open(cgroup_path.c_str(), O_RDONLY | O_DIRECTORY | O_CLOEXEC);
  if (cgroup_fd < 0) {
    const int error_number = errno;
    throw PerfCounterError(ClassifyPerfError(error_number), error_number,
                           "open perf cgroup " + cgroup_path.string() +
                               " failed: " + std::strerror(error_number));
  }

  auto impl = std::make_unique<Impl>();
  impl->cgroup_path = cgroup_path;
  impl->kinds = kinds;
  impl->cgroup_fd = UniqueFd(cgroup_fd);

  impl->cpus = AllowedCpus();

  impl->events.reserve(kinds.size());
  for (const PerfCounterKind kind : kinds) {
    const PerfEventEncoding encoding = CounterEncoding(kind);
    perf_event_attr attributes{};
    attributes.size = sizeof(attributes);
    attributes.type = encoding.type;
    attributes.config = encoding.config;
    attributes.read_format =
        PERF_FORMAT_TOTAL_TIME_ENABLED | PERF_FORMAT_TOTAL_TIME_RUNNING;
    attributes.disabled = 1;

    Impl::Event event;
    event.kind = kind;
    event.fds.reserve(impl->cpus.size());
    for (const int cpu : impl->cpus) {
      errno = 0;
      const int fd =
          OpenCgroupPerfEvent(attributes, impl->cgroup_fd.get(), cpu);
      if (fd < 0) {
        ThrowPerfSystemError("open cgroup perf event", kind, errno);
      }
      event.fds.emplace_back(fd);
    }
    impl->events.push_back(std::move(event));
  }

  for (const Impl::Event& event : impl->events) {
    for (const UniqueFd& fd : event.fds) {
      EnableDirectPerfEvent(fd.get(), event.kind);
    }
  }
  return CgroupPerfCounters(std::move(impl));
}

CgroupPerfCounters::CgroupPerfCounters(std::unique_ptr<Impl> impl)
    : impl_(std::move(impl)) {}

CgroupPerfCounters::CgroupPerfCounters(CgroupPerfCounters&&) noexcept = default;
CgroupPerfCounters& CgroupPerfCounters::operator=(
    CgroupPerfCounters&&) noexcept = default;
CgroupPerfCounters::~CgroupPerfCounters() = default;

const std::filesystem::path& CgroupPerfCounters::cgroup_path() const {
  return impl_->cgroup_path;
}

const std::vector<PerfCounterKind>& CgroupPerfCounters::kinds() const {
  return impl_->kinds;
}

const std::vector<int>& CgroupPerfCounters::cpus() const { return impl_->cpus; }

std::vector<PerfCounterValue> CgroupPerfCounters::Read() const {
  using boost::multiprecision::uint128_t;
  std::vector<PerfCounterValue> values;
  values.reserve(impl_->events.size());
  for (const Impl::Event& event : impl_->events) {
    uint128_t raw_total = 0;
    uint128_t enabled_total = 0;
    uint128_t running_total = 0;
    uint128_t scaled_total = 0;
    bool scaled_available = true;
    bool scaled = false;
    bool multiplexed = false;
    bool overflow = false;
    for (const UniqueFd& fd : event.fds) {
      const DirectPerfReadValue read_value =
          ReadDirectPerfEvent(fd.get(), event.kind);
      raw_total += read_value.value;
      enabled_total += read_value.time_enabled;
      running_total += read_value.time_running;
      multiplexed =
          multiplexed || read_value.time_running < read_value.time_enabled;
      const ScaledPerfCounterValue cpu_scaled =
          read_value.time_enabled == 0U && read_value.time_running == 0U
              ? ScaledPerfCounterValue{.value = read_value.value}
              : ScalePerfCounterValue(read_value.value, read_value.time_enabled,
                                      read_value.time_running);
      if (!cpu_scaled.value) {
        scaled_available = false;
      } else {
        scaled_total += *cpu_scaled.value;
      }
      scaled = scaled || cpu_scaled.scaled;
      overflow = overflow || cpu_scaled.overflow;
    }

    PerfCounterValue value;
    value.kind = event.kind;
    value.raw_value = SaturatingUint64(raw_total, &overflow);
    value.time_enabled_ns = SaturatingUint64(enabled_total, &overflow);
    value.time_running_ns = SaturatingUint64(running_total, &overflow);
    if (scaled_available) {
      value.scaled_value = SaturatingUint64(scaled_total, &overflow);
    }
    value.multiplexed = multiplexed;
    value.scaled = scaled;
    value.scaled_overflow = overflow;
    values.push_back(value);
  }
  return values;
}

}  // namespace bbp
