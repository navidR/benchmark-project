#pragma once

#include "bbp/runtime_node_inventory.h"
#include "bbp/simulator/workload_kind.h"

namespace bbp {

// Mutation-admitted workloads take their authoritative node snapshot inside
// their execution path after acquiring node_mutation_mutex.
[[nodiscard]] RuntimeNodeSnapshot SnapshotScenarioDispatchNodes(
    const RuntimeNodeInventory& inventory, WorkloadKind kind);

}  // namespace bbp
