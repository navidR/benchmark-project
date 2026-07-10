#pragma once

namespace bsim {

enum class WorkloadKind {
  kBlockGeneration,
  kWaitUntilHeight,
  kWaitForPeers,
  kConnectPeer,
  kDisconnectPeer,
  kRestartNode,
  kFreezeNode,
  kUpdateResourceLimits,
  kResourcePressure,
  kPartitionNodes,
  kHealPartition,
  kSendRawTransaction,
  kWalletTransactions,
};

}  // namespace bsim
