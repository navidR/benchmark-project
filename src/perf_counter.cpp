#include "bbp/perf_counter.h"

#include <linux/perf_event.h>
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
    (void)perf_evsel__disable(event);
    perf_evsel__close(event);
    perf_evsel__delete(event);
  }
};

using ThreadMapPtr = std::unique_ptr<perf_thread_map, ThreadMapDeleter>;
using EvselPtr = std::unique_ptr<perf_evsel, EvselDeleter>;

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
  if (kinds.empty()) {
    throw PerfCounterError(PerfCounterErrorKind::kInvalidArgument, EINVAL,
                           "perf counter selection must not be empty");
  }
  const std::set<PerfCounterKind> unique(kinds.begin(), kinds.end());
  if (unique.size() != kinds.size()) {
    throw PerfCounterError(PerfCounterErrorKind::kInvalidArgument, EINVAL,
                           "perf counter selection contains duplicates");
  }

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

}  // namespace bbp
