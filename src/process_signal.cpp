#include "bbp/process_signal.h"

#include <signal.h>
#include <sys/syscall.h>
#include <sys/wait.h>
#include <unistd.h>

#include <cerrno>
#include <charconv>
#include <stdexcept>
#include <system_error>
#include <utility>

#include "owned_process_signal.h"

namespace bbp {
namespace {

constexpr std::pair<std::string_view, int> kNamedSignals[] = {
    {"SIGHUP", SIGHUP},       {"SIGINT", SIGINT},   {"SIGQUIT", SIGQUIT},
    {"SIGILL", SIGILL},       {"SIGTRAP", SIGTRAP}, {"SIGABRT", SIGABRT},
    {"SIGBUS", SIGBUS},       {"SIGFPE", SIGFPE},   {"SIGKILL", SIGKILL},
    {"SIGUSR1", SIGUSR1},     {"SIGSEGV", SIGSEGV}, {"SIGUSR2", SIGUSR2},
    {"SIGPIPE", SIGPIPE},     {"SIGALRM", SIGALRM}, {"SIGTERM", SIGTERM},
    {"SIGCHLD", SIGCHLD},     {"SIGCONT", SIGCONT}, {"SIGSTOP", SIGSTOP},
    {"SIGTSTP", SIGTSTP},     {"SIGTTIN", SIGTTIN}, {"SIGTTOU", SIGTTOU},
    {"SIGURG", SIGURG},       {"SIGXCPU", SIGXCPU}, {"SIGXFSZ", SIGXFSZ},
    {"SIGVTALRM", SIGVTALRM}, {"SIGPROF", SIGPROF}, {"SIGWINCH", SIGWINCH},
    {"SIGIO", SIGIO},         {"SIGSYS", SIGSYS},
#ifdef SIGPWR
    {"SIGPWR", SIGPWR},
#endif
#ifdef SIGSTKFLT
    {"SIGSTKFLT", SIGSTKFLT},
#endif
#ifdef SIGEMT
    {"SIGEMT", SIGEMT},
#endif
#ifdef SIGPOLL
    {"SIGPOLL", SIGPOLL},
#endif
#ifdef SIGIOT
    {"SIGIOT", SIGIOT},
#endif
#ifdef SIGCLD
    {"SIGCLD", SIGCLD},
#endif
};

int Decimal(std::string_view value) {
  int parsed = 0;
  if (value.empty() || value.front() < '0' || value.front() > '9') {
    throw std::invalid_argument("signal requires an unsigned decimal number");
  }
  const auto [end, error] =
      std::from_chars(value.data(), value.data() + value.size(), parsed);
  if (error != std::errc{} || end != value.data() + value.size()) {
    throw std::invalid_argument("signal contains an invalid decimal number");
  }
  return parsed;
}

void RequireSignal(int signal) {
  if (signal <= 0 || signal >= NSIG) {
    throw std::invalid_argument("signal number is unavailable on this host");
  }
}

}  // namespace

int ParseProcessSignal(std::string_view value) {
  for (const auto& [name, number] : kNamedSignals) {
    if (value == name) {
      return number;
    }
  }
  if (value == "SIGRTMIN") {
    return SIGRTMIN;
  }
  if (value == "SIGRTMAX") {
    return SIGRTMAX;
  }
  if (value.starts_with("SIGRTMIN+") || value.starts_with("SIGRTMAX-")) {
    const int offset = Decimal(value.substr(9U));
    if (offset > SIGRTMAX - SIGRTMIN) {
      throw std::invalid_argument(
          "real-time signal is unavailable on this host");
    }
    return value.starts_with("SIGRTMIN+") ? SIGRTMIN + offset
                                          : SIGRTMAX - offset;
  }
  const int number = Decimal(value);
  RequireSignal(number);
  return number;
}

std::string ProcessSignalName(int signal) {
  RequireSignal(signal);
  for (const auto& [name, number] : kNamedSignals) {
    if (signal == number) {
      return std::string(name);
    }
  }
  if (signal == SIGRTMIN) {
    return "SIGRTMIN";
  }
  if (signal == SIGRTMAX) {
    return "SIGRTMAX";
  }
  if (signal > SIGRTMIN && signal < SIGRTMAX) {
    return "SIGRTMIN+" + std::to_string(signal - SIGRTMIN);
  }
  return std::to_string(signal);
}

ProcessSignalScope ParseProcessSignalScope(std::string_view value) {
  if (value == "process") return ProcessSignalScope::kProcess;
  if (value == "process_group") return ProcessSignalScope::kProcessGroup;
  throw std::invalid_argument("signal scope must be process or process_group");
}

std::string_view ProcessSignalScopeName(ProcessSignalScope scope) {
  switch (scope) {
    case ProcessSignalScope::kProcess:
      return "process";
    case ProcessSignalScope::kProcessGroup:
      return "process_group";
  }
  throw std::invalid_argument("invalid process signal scope");
}

ProcessSignalDelivery DeliverOwnedProcessSignal(pid_t pid, int pidfd,
                                                int signal,
                                                ProcessSignalScope scope) {
  static_cast<void>(ProcessSignalName(signal));
  if (scope != ProcessSignalScope::kProcess &&
      scope != ProcessSignalScope::kProcessGroup) {
    throw std::invalid_argument("invalid process signal delivery scope");
  }
  if (pid <= 0 || pidfd < 0) {
    throw std::system_error(ECHILD, std::generic_category(),
                            "signal target has no live owned child identity");
  }
  siginfo_t state{};
  int inspected;
  do {
    inspected = waitid(P_PIDFD, static_cast<id_t>(pidfd), &state,
                       WEXITED | WNOHANG | WNOWAIT);
  } while (inspected < 0 && errno == EINTR);
  if (inspected < 0) {
    throw std::system_error(errno, std::generic_category(),
                            "verify signal target child ownership");
  }
  if (state.si_pid != 0) {
    throw std::system_error(ESRCH, std::generic_category(),
                            "signal target has already exited");
  }
  unsigned int flags = 0U;
  if (scope == ProcessSignalScope::kProcessGroup) {
    if (getpgid(pid) != pid || getsid(pid) != pid) {
      throw std::system_error(EPERM, std::generic_category(),
                              "signal target is not an owned private group");
    }
    // Linux UAPI PIDFD_SIGNAL_PROCESS_GROUP (available since Linux 6.9).
    flags = 1U << 2U;
  }
  int result = -1;
#ifdef SYS_pidfd_send_signal
  result = static_cast<int>(
      syscall(SYS_pidfd_send_signal, pidfd, signal, nullptr, flags));
#else
  errno = ENOSYS;
#endif
  const int error = result < 0 ? errno : 0;
  return {.target_pid = pid,
          .process_group_id = pid,
          .signal = signal,
          .scope = scope,
          .kernel_result = result,
          .error_number = error};
}

}  // namespace bbp
