#pragma once

#include "benchmark_sim/simulator/block_generation_workload.h"
#include "benchmark_sim/simulator/connect_peer_workload.h"
#include "benchmark_sim/simulator/disconnect_peer_workload.h"
#include "benchmark_sim/simulator/freeze_node_workload.h"
#include "benchmark_sim/simulator/network_partition_workload.h"
#include "benchmark_sim/simulator/resource_limit_update_workload.h"
#include "benchmark_sim/simulator/resource_pressure_workload.h"
#include "benchmark_sim/simulator/restart_node_workload.h"
#include "benchmark_sim/simulator/send_raw_transaction_workload.h"
#include "benchmark_sim/simulator/wait_for_peers_workload.h"
#include "benchmark_sim/simulator/wait_until_height_workload.h"
#include "benchmark_sim/simulator/wallet_transactions_workload.h"
#include "benchmark_sim/simulator/workload_kind.h"

namespace bsim {

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
  SendRawTransactionWorkload send_raw_transaction;
  WalletTransactionsWorkload wallet_transactions;
};

}  // namespace bsim
