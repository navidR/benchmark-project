#include "bbp/simulator/scenario_workload_admission.h"

namespace bbp {

RuntimeNodeSnapshot SnapshotScenarioDispatchNodes(
    const RuntimeNodeInventory& inventory, WorkloadKind kind) {
  if (kind == WorkloadKind::kWalletTransactions ||
      kind == WorkloadKind::kRestartNode ||
      kind == WorkloadKind::kBlockGeneration ||
      kind == WorkloadKind::kWaitUntilHeight ||
      kind == WorkloadKind::kFreezeNode) {
    return {};
  }
  return inventory.Snapshot();
}

}  // namespace bbp
