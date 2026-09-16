#pragma once

#include <sys/types.h>

#include <string>
#include <string_view>

namespace bbp {

// Linux kernel signal numbers are 1..NSIG-1. Libc-reserved slots without a
// public name can be selected explicitly by their decimal number.
int ParseProcessSignal(std::string_view value);
std::string ProcessSignalName(int signal);

enum class ProcessSignalScope { kProcess, kProcessGroup };

ProcessSignalScope ParseProcessSignalScope(std::string_view value);
std::string_view ProcessSignalScopeName(ProcessSignalScope scope);

struct ProcessSignalRequest {
  int signal = 0;
  ProcessSignalScope scope = ProcessSignalScope::kProcess;
};

struct ProcessSignalDelivery {
  pid_t target_pid = -1;
  pid_t process_group_id = -1;
  int signal = 0;
  ProcessSignalScope scope = ProcessSignalScope::kProcess;
  int kernel_result = -1;
  int error_number = 0;
};

}  // namespace bbp
