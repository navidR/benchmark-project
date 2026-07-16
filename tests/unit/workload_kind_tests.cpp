#include <array>
#include <boost/test/unit_test.hpp>
#include <optional>

#include "bbp/simulator/workload_kind.h"

BOOST_AUTO_TEST_CASE(workload_kind_names_round_trip) {
  constexpr std::array<bbp::WorkloadKind, 22> kKinds = {
      bbp::WorkloadKind::kBlockGeneration,
      bbp::WorkloadKind::kWaitUntilHeight,
      bbp::WorkloadKind::kWaitForPeers,
      bbp::WorkloadKind::kConnectPeer,
      bbp::WorkloadKind::kDisconnectPeer,
      bbp::WorkloadKind::kRestartNode,
      bbp::WorkloadKind::kFreezeNode,
      bbp::WorkloadKind::kUpdateResourceLimits,
      bbp::WorkloadKind::kSetResourceProfile,
      bbp::WorkloadKind::kSetNetworkProfile,
      bbp::WorkloadKind::kResourcePressure,
      bbp::WorkloadKind::kSetNetworkCondition,
      bbp::WorkloadKind::kBlockNetworkFlow,
      bbp::WorkloadKind::kUnblockNetworkFlow,
      bbp::WorkloadKind::kPartitionNodes,
      bbp::WorkloadKind::kHealPartition,
      bbp::WorkloadKind::kSetEdgeCondition,
      bbp::WorkloadKind::kActivateEdge,
      bbp::WorkloadKind::kDeactivateEdge,
      bbp::WorkloadKind::kRestoreEdge,
      bbp::WorkloadKind::kSendRawTransaction,
      bbp::WorkloadKind::kWalletTransactions,
  };

  for (const bbp::WorkloadKind kind : kKinds) {
    const std::optional<bbp::WorkloadKind> parsed =
        bbp::ParseWorkloadKind(bbp::WorkloadKindName(kind));
    BOOST_REQUIRE(parsed);
    BOOST_CHECK(*parsed == kind);
  }
  BOOST_TEST(!bbp::ParseWorkloadKind("unknown_workload"));
}
