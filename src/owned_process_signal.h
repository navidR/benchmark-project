#pragma once

#include "bbp/process_signal.h"

namespace bbp {

// The owner retains a matching PID/pidfd pair for an unreaped direct child.
// Verify that ownership before delivering to its process or private group.
ProcessSignalDelivery DeliverOwnedProcessSignal(pid_t pid, int pidfd,
                                                int signal,
                                                ProcessSignalScope scope);

}  // namespace bbp
