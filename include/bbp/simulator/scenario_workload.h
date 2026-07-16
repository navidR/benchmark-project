#pragma once

#include "bbp/simulator/block_generation_workload.h"
#include "bbp/simulator/connect_peer_workload.h"
#include "bbp/simulator/disconnect_peer_workload.h"
#include "bbp/simulator/freeze_node_workload.h"
#include "bbp/simulator/network_partition_workload.h"
#include "bbp/simulator/profile_switch_workload.h"
#include "bbp/simulator/resource_limit_update_workload.h"
#include "bbp/simulator/resource_pressure_workload.h"
#include "bbp/simulator/restart_node_workload.h"
#include "bbp/simulator/send_raw_transaction_workload.h"
#include "bbp/simulator/topology_edge_workload.h"
#include "bbp/simulator/wait_for_peers_workload.h"
#include "bbp/simulator/wait_until_height_workload.h"
#include "bbp/simulator/wallet_transactions_workload.h"
#include "bbp/simulator/workload_kind.h"

namespace bbp {

struct ScenarioWorkload {
  WorkloadKind kind = WorkloadKind::kBlockGeneration;
  BlockGenerationWorkload block_generation;
  WaitUntilHeightWorkload wait_until_height;
  WaitForPeersWorkload wait_for_peers;
  ConnectPeerWorkload connect_peer;
  DisconnectPeerWorkload disconnect_peer;
  RestartNodeWorkload restart_node;
  FreezeNodeWorkload freeze_node;
  ResourceLimitUpdateWorkload update_resource_limits;
  ResourcePressureWorkload resource_pressure;
  NetworkPartitionWorkload network_partition;
  ProfileSwitchWorkload profile_switch;
  TopologyEdgeWorkload topology_edge;
  SendRawTransactionWorkload send_raw_transaction;
  WalletTransactionsWorkload wallet_transactions;
};

}  // namespace bbp
