#pragma once

#include "bbp/run_process_state.h"

namespace bbp {

struct NodeRuntime;

namespace simulator_app_internal {

void ResetNodePerfCounters(NodeRuntime& node,
                           const RunProcessState::Guard& guard);
void AttachNodePerfCounters(NodeRuntime& node,
                            const RunProcessState::Guard& guard);

}  // namespace simulator_app_internal
}  // namespace bbp
