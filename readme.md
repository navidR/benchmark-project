# Blockchain Benchmark Project

Blockchain Benchmark Project is a Linux-local blockchain benchmark simulator.

The goal is to run real blockchain daemon processes locally, isolate them like
separate nodes, apply controlled CPU, memory, process, and network conditions,
drive benchmark workloads, and write reproducible metrics.

Firo is the current priority. Bitcoin Core and Monero are future targets after
the Firo path is working end to end.

## What Works Now

- Start up to 16 Firo regtest nodes.
- Run Firo nodes inside isolated network namespaces with one veth pair per node.
- Apply simple per-node network conditions through host-side `netem` or TBF.
- Apply and remove live per-node TCP block rules, optionally scoped by source.
- Apply and heal source-aware group network partitions after startup or as an
  ordered workload.
- Apply live per-node cgroup resource updates after startup or as an ordered
  workload.
- Restart a running Firo node before workload generation or as an ordered
  workload.
- Freeze and thaw a running Firo node cgroup before workload generation or as
  an ordered workload.
- Wait for JSON-RPC readiness.
- Produce regtest blocks through a reproducible global Bernoulli scheduler,
  selecting one configured miner uniformly after each successful draw.
- Wait for blocks from explicit generation workloads to propagate before those
  workloads complete.
- Record optional periodic metric samples concurrently with wallet setup,
  runtime events, and workloads.
- Record nine process performance counters through the official Linux
  `libperf` API, including raw/scaled values and multiplexing times.
- Record cgroup usage, pressure, event counters, and configured limits.
- Record Firo daemon version, protocol version, and subversion in metrics.
- Record event and metric files under a run directory.
- Operate a live run through ncurses and view a completed run in read-only
  mode.
- Exercise Linux network namespace, veth, address, route, and qdisc operations
  through simulator probes.
- Run unit tests with CTest.

## Requirements

- Linux with cgroup v2, network namespaces, veth, and traffic-control support.
- CMake 3.20 or newer.
- A C++20 compiler.
- A compiled `firod` binary for Firo smoke runs.
- Privileges/capabilities for network probes, usually `CAP_SYS_ADMIN` and
  `CAP_NET_ADMIN`.
- Permission to use `perf_event_open(2)` for daemon processes. Prefer
  `CAP_PERFMON` on kernels that provide it; `CAP_SYS_ADMIN` also grants access
  on older kernels. The host's `kernel.perf_event_paranoid` and container
  seccomp policy must permit the call.

The normal development path is a Docker container with the project mounted. The
examples below use `benchmark-project-codex` as the container name.

Set paths used by the commands:

```bash
export PROJECT_ROOT=/path/to/benchmark-project
export FIROD=/path/to/firod
```

## Build

Configure and build inside Docker:

```bash
docker exec -e PROJECT_ROOT="$PROJECT_ROOT" benchmark-project-codex bash -lc \
  'cd "$PROJECT_ROOT" &&
   cmake -S . -B build -DCMAKE_BUILD_TYPE=Debug &&
   cmake --build build -j16'
```

Build directly on the host:

```bash
cd "$PROJECT_ROOT"
cmake -S . -B build -DCMAKE_BUILD_TYPE=Debug
cmake --build build -j16
```

## Test

Run unit tests:

```bash
docker exec -e PROJECT_ROOT="$PROJECT_ROOT" benchmark-project-codex bash -lc \
  'cd "$PROJECT_ROOT" &&
   ctest --test-dir build --output-on-failure'
```

## Run Firo Smoke Benchmarks

One Firo node:

```bash
docker exec -e PROJECT_ROOT="$PROJECT_ROOT" -e FIROD="$FIROD" \
  benchmark-project-codex bash -lc \
  'cd "$PROJECT_ROOT" &&
   ./build/bbp \
     --chain firo \
     --node-binary "$FIROD" \
     --benchmark-root runs \
     --run-id smoke1 \
     --replace-run \
     --nodes 1 \
     --block-production-probability 1 \
     --block-production-period-ms 250 \
     --metrics-sample-count 4 \
     --metrics-interval 250ms \
     --ready-timeout-sec 45'
```

Multiple Firo nodes:

```bash
docker exec -e PROJECT_ROOT="$PROJECT_ROOT" -e FIROD="$FIROD" \
  benchmark-project-codex bash -lc \
  'cd "$PROJECT_ROOT" &&
   ./build/bbp \
     --chain firo \
     --node-binary "$FIROD" \
     --benchmark-root runs \
     --run-id smoke3 \
     --replace-run \
     --nodes 3 \
     --block-production-probability 1 \
     --block-production-period-ms 250 \
     --metrics-sample-count 4 \
     --metrics-interval 250ms \
     --generate-node 2 \
     --ready-timeout-sec 45 \
     --sync-timeout-sec 45'
```

Multiple isolated Firo nodes:

```bash
docker exec -e PROJECT_ROOT="$PROJECT_ROOT" -e FIROD="$FIROD" \
  benchmark-project-codex bash -lc \
  'cd "$PROJECT_ROOT" &&
   ./build/bbp \
     --chain firo \
     --node-binary "$FIROD" \
     --benchmark-root runs \
     --run-id isolated-smoke \
     --replace-run \
     --nodes 3 \
     --block-production-probability 1 \
     --block-production-period-ms 250 \
     --metrics-sample-count 4 \
     --metrics-interval 250ms \
     --ready-timeout-sec 45 \
     --sync-timeout-sec 45 \
     --isolate-network'
```

Isolated Firo node with a default network condition:

```bash
docker exec -e PROJECT_ROOT="$PROJECT_ROOT" -e FIROD="$FIROD" \
  benchmark-project-codex bash -lc \
  'cd "$PROJECT_ROOT" &&
   ./build/bbp \
     --chain firo \
     --node-binary "$FIROD" \
     --benchmark-root runs \
     --run-id isolated-delay \
     --replace-run \
     --nodes 1 \
     --block-production-probability 1 \
     --block-production-period-ms 250 \
     --metrics-sample-count 4 \
     --metrics-interval 250ms \
     --ready-timeout-sec 45 \
     --isolate-network \
     --network-delay-ms 5'
```

Isolated Firo node with a bandwidth limit:

```bash
docker exec -e PROJECT_ROOT="$PROJECT_ROOT" -e FIROD="$FIROD" \
  benchmark-project-codex bash -lc \
  'cd "$PROJECT_ROOT" &&
   ./build/bbp \
     --chain firo \
     --node-binary "$FIROD" \
     --benchmark-root runs \
     --run-id isolated-bandwidth \
     --replace-run \
     --nodes 1 \
     --block-production-probability 1 \
     --block-production-period-ms 250 \
     --metrics-sample-count 4 \
     --metrics-interval 250ms \
     --ready-timeout-sec 45 \
     --isolate-network \
     --network-bandwidth-mbps 20'
```

Per-node isolated network conditions use repeatable JSON objects:

```bash
./build/bbp \
  --chain firo \
  --node-binary "$FIROD" \
  --benchmark-root runs \
  --run-id isolated-per-node \
  --replace-run \
  --nodes 2 \
  --block-production-probability 1 \
  --block-production-period-ms 250 \
  --metrics-sample-count 4 \
  --metrics-interval 250ms \
  --isolate-network \
  --node-network-condition-json '{"node":2,"bandwidth_mbps":20}'
```

Runtime network updates use the same JSON shape and are applied after nodes are
running, while scheduled block production and metrics collection continue:

```bash
./build/bbp \
  --chain firo \
  --node-binary "$FIROD" \
  --benchmark-root runs \
  --run-id live-netem \
  --replace-run \
  --nodes 2 \
  --block-production-probability 1 \
  --block-production-period-ms 250 \
  --metrics-sample-count 4 \
  --metrics-interval 250ms \
  --isolate-network \
  --runtime-node-network-condition-json '{"node":2,"bandwidth_mbps":10}'
```

Runtime block/unblock rules match a destination IPv4 address and TCP port on a
node's host-side veth. Add `src_address` when the rule should only match one
source node. This example applies and then removes the same source-scoped rule
while scheduled block production and metrics collection continue:

```bash
./build/bbp \
  --chain firo \
  --node-binary "$FIROD" \
  --benchmark-root runs \
  --run-id live-block-unblock \
  --replace-run \
  --nodes 2 \
  --block-production-probability 1 \
  --block-production-period-ms 250 \
  --metrics-sample-count 4 \
  --metrics-interval 250ms \
  --isolate-network \
  --runtime-node-block-json '{"node":1,"src_address":"10.210.2.2","dst_address":"10.210.1.2","dst_port":18168}' \
  --runtime-node-unblock-json '{"node":1,"src_address":"10.210.2.2","dst_address":"10.210.1.2","dst_port":18168}'
```

Runtime partition/heal accepts two node groups and installs source-aware
cross-group P2P block rules in both directions:

```bash
./build/bbp \
  --chain firo \
  --node-binary "$FIROD" \
  --benchmark-root runs \
  --run-id live-partition-heal \
  --replace-run \
  --nodes 3 \
  --block-production-probability 1 \
  --block-production-period-ms 250 \
  --metrics-sample-count 4 \
  --metrics-interval 250ms \
  --isolate-network \
  --runtime-partition-json '{"group_a":[1,2],"group_b":[3]}' \
  --runtime-heal-partition-json '{"group_a":[1,2],"group_b":[3]}'
```

The current implementation applies bandwidth with TBF and delay/loss conditions
with `netem`. When both are requested, TBF is the root qdisc and netem is
attached below it. The netem fields are `delay_ms`, `jitter_ms`,
`loss_basis_points`, `duplicate_basis_points`, `corrupt_basis_points`,
`reorder_basis_points`, and `limit_packets`.

Default resource limits apply to each node cgroup:

```bash
./build/bbp \
  --chain firo \
  --node-binary "$FIROD" \
  --benchmark-root runs \
  --run-id resource-smoke \
  --replace-run \
  --nodes 1 \
  --block-production-probability 1 \
  --block-production-period-ms 250 \
  --metrics-sample-count 4 \
  --metrics-interval 250ms \
  --memory-high-bytes 1073741824 \
  --memory-max-bytes 1610612736 \
  --cpu-quota-us 75000 \
  --cpu-period-us 100000 \
  --pids-max 128
```

Runtime resource updates are applied after nodes are running, before block
generation. Omitted fields keep their current values; `cpu_quota_us: null`
restores unlimited CPU quota.

```bash
./build/bbp \
  --chain firo \
  --node-binary "$FIROD" \
  --benchmark-root runs \
  --run-id live-resources \
  --replace-run \
  --nodes 1 \
  --block-production-probability 1 \
  --block-production-period-ms 250 \
  --metrics-sample-count 4 \
  --metrics-interval 250ms \
  --runtime-node-resource-json '{"node":1,"memory_high_bytes":1073741824,"cpu_quota_us":50000,"cpu_period_us":100000,"pids_max":128}'
```

Runtime restarts stop a node through Firo RPC, respawn it in the same
cgroup/network/data directory, wait for RPC readiness, then continue the run:

```bash
./build/bbp \
  --chain firo \
  --node-binary "$FIROD" \
  --benchmark-root runs \
  --run-id restart-smoke \
  --replace-run \
  --nodes 1 \
  --block-production-probability 1 \
  --block-production-period-ms 250 \
  --metrics-sample-count 4 \
  --metrics-interval 250ms \
  --runtime-node-restart-json '{"node":1}'
```

Runtime freezes pause a node cgroup for a bounded duration, verify frozen and
thawed states, then continue the run:

```bash
./build/bbp \
  --chain firo \
  --node-binary "$FIROD" \
  --benchmark-root runs \
  --run-id freeze-smoke \
  --replace-run \
  --nodes 1 \
  --block-production-probability 1 \
  --block-production-period-ms 250 \
  --metrics-sample-count 4 \
  --metrics-interval 250ms \
  --runtime-node-freeze-json '{"node":1,"duration_ms":100}'
```

Extra metric samples can be collected while runtime updates, restarts, and
scheduled block production execute. The nodes remain active until the requested
sample count is complete:

```bash
./build/bbp \
  --chain firo \
  --node-binary "$FIROD" \
  --benchmark-root runs \
  --run-id sampled-smoke \
  --replace-run \
  --nodes 1 \
  --block-production-probability 1 \
  --block-production-period-ms 250 \
  --metrics-sample-count 5 \
  --metrics-interval 1s
```

Temporary chain RPC unavailability during restart or freeze is recorded as a
`metrics_node_unavailable` event for that node. Remaining nodes and later
samples continue; storage and internal collection failures still fail the run.
The default `--metrics-sample-count 0` keeps metrics and scheduled block
production running until the integrated TUI exits. Headless runs that must
finish automatically should pass a positive sample count. `--no-mining`
disables scheduled production without disabling metrics.
`generated_block_count` counts blocks explicitly requested by the simulator;
native-mining output is reflected in chain height and is not attributed to a
specific miner unless that chain driver can report the attribution.

Every node sample attempts to include `cycles`, `instructions`, cache and
branch references/misses, context switches, page faults, and task clock. The
simulator uses its owned daemon PID and reopens the counters after a restart.
`perf_counter_target_pid`, `perf_counter_attached_pid`, and
`perf_counter_process_generation` identify that attachment. Each counter keeps
its raw value, safely scaled value, enabled time, and running time so
multiplexing remains visible.

For a Docker run, grant perf access explicitly when it is not already covered
by the privileged simulator environment, for example with `--cap-add PERFMON`
(or `--cap-add SYS_ADMIN` on older kernels). Some Docker seccomp profiles also
deny `perf_event_open`; use an administrator-approved profile that permits the
syscall. The current host policy can be inspected without changing it at
`/proc/sys/kernel/perf_event_paranoid`. If the kernel denies or does not support
the events, the benchmark continues to collect its other metrics and records
`perf_counters_available: false` with a typed `perf_counter_error_kind` and
error detail; unavailable counters are never reported as zero.

The same run settings can be loaded from a JSON or YAML scenario file. Both
formats use the same field names and validation rules.

```json
{
  "chain_daemon": "/path/to/firod",
  "output_dir": "runs",
  "run_id": "scenario-smoke",
  "nodes": 3,
  "ready_timeout_sec": 45,
  "metrics_sample_count": 5,
  "metrics_interval_ms": 1000,
  "block_production": {
    "enabled": true,
    "native_mining": false,
    "period_ms": 1000,
    "probability": 0.5,
    "seed": 7,
    "difficulty": null
  },
  "workloads": [
    {
      "type": "wait_for_peers",
      "node": 2,
      "peer_count": 1,
      "timeout_sec": 45
    },
    {
      "type": "disconnect_peer",
      "node": 2,
      "peer": 1,
      "timeout_sec": 45
    },
    {
      "type": "connect_peer",
      "node": 2,
      "peer": 1,
      "timeout_sec": 45
    },
    {
      "type": "partition_nodes",
      "group_a": [1],
      "group_b": [2, 3]
    },
    {
      "type": "heal_partition",
      "group_a": [1],
      "group_b": [2, 3]
    },
    {
      "type": "block_generation",
      "node": 2,
      "count": 1,
      "sync_timeout_sec": 45
    },
    {
      "type": "block_generation",
      "node": 1,
      "count": 1,
      "sync_timeout_sec": 45
    },
    {
      "type": "wait_until_height",
      "node": 2,
      "height": 1,
      "timeout_sec": 45
    }
  ],
  "resources": {
    "memory_high_bytes": 1073741824,
    "memory_max_bytes": 1610612736,
    "cpu_quota_us": 75000,
    "cpu_period_us": 100000,
    "pids_max": 128,
    "runtime_node_limits": [
      {
        "node": 1,
        "memory_high_bytes": 805306368,
        "cpu_quota_us": null,
        "pids_max": 128
      }
    ]
  },
  "network": {
    "isolated": true,
    "default_condition": {
      "delay_ms": 1,
      "limit_packets": 1000
    },
    "runtime_node_conditions": [
      {
        "node": 1,
        "delay_ms": 3,
        "jitter_ms": 1
      }
    ],
    "runtime_node_blocks": [
      {
        "node": 1,
        "src_address": "10.210.2.2",
        "dst_address": "10.210.1.2",
        "dst_port": 18168
      }
    ],
    "runtime_node_unblocks": [
      {
        "node": 1,
        "src_address": "10.210.2.2",
        "dst_address": "10.210.1.2",
        "dst_port": 18168
      }
    ],
    "runtime_partitions": [
      {
        "group_a": [1, 2],
        "group_b": [3]
      }
    ],
    "runtime_partition_heals": [
      {
        "group_a": [1, 2],
        "group_b": [3]
      }
    ]
  },
  "process": {
    "runtime_node_restarts": [
      {
        "node": 1
      }
    ],
    "runtime_node_freezes": [
      {
        "node": 1,
        "duration_ms": 100
      }
    ]
  }
}
```

Run it:

```bash
./build/bbp --scenario /path/to/scenario.json --replace-run
```

The equivalent YAML entry point is:

```bash
./build/bbp --scenario /path/to/scenario.yaml --replace-run
```

Wallet and miner roles can be declared in the scenario topology. Counts resolve
deterministically to concrete node lists; explicit `wallet_nodes` and
`miner_nodes` may be used when a scenario needs fixed assignments. If a
scenario has miners, each successful global block-production draw selects one
active miner uniformly. Without a topology, `generate_node` identifies the
single default miner.

```json
{
  "node_count": 3,
  "topology": {
    "node_count": 3,
    "wallet_node_count": 2,
    "miner_node_count": 1,
    "allow_miner_wallet_overlap": false
  }
}
```

Logical P2P topology is selected by the same typed `topology` object. The
default is `full_mesh`; supported graph types are `ring`, `star`,
`random_graph`, `scale_free_graph`, `latency_matrix`, `custom_edge_list`,
`partitioned_groups`, and `internet_like_region_graph`. Seeded graph resolution
is deterministic, node ids are 1-based in scenario files, and the canonical
directed `resolved_edges` array is written to `resolved-scenario.json`.

```json
{
  "node_count": 4,
  "topology": {
    "node_count": 4,
    "type": "ring",
    "peer_connectivity": [
      {"node": 1, "all_peers": true}
    ]
  }
}
```

`star` accepts `center_node`; `random_graph` accepts `seed` and
`average_degree`; `scale_free_graph` accepts `seed` plus either
`attachment_count` or `average_degree`. A custom edge has `from`, `to`,
`bidirectional`, and `active` fields. It may also define the flattened typed
condition fields `bandwidth_mbps`, `delay_ms`, `jitter_ms`,
`loss_basis_points`, `duplicate_basis_points`, `corrupt_basis_points`,
`reorder_basis_points`, and `limit_packets`. The compatibility field
`latency_ms` maps to `delay_ms`; when both are present, their values must match.
Directional conditions require `network.isolated: true` and are validated
before the simulator creates resources.

For each source node, active outgoing edges receive deterministic bands in
canonical destination order. Unconditioned edges retain their bands so adding
or removing a condition does not renumber later destinations. The simulator
applies each condition to an exact destination IPv4 `/32` on the source node's
namespace-side veth through direct rtnetlink calls, verifies the qdisc and
flower-filter state from the kernel, and records a
`directional_network_policies_verified` event. The current 16-node isolated
address plan permits at most 15 outgoing destination bands per source. The
configured and resolved edge conditions are preserved in
`resolved-scenario.json` and in the shared CLI/TUI run report.

Every periodic node sample also reads the current simulator-owned flower and
per-band qdisc objects inside that node's namespace. The metrics preserve exact
per-edge classifier counters and every TBF/netem stage, plus checked aggregate
packet, byte, drop, overlimit, queue, backlog, and requeue fields under
`directional_network_*`. A combined TBF-plus-netem path uses its ingress TBF
bytes and packets once while summing distinct drop/queue counters across both
stages. Kernel state is matched against the synchronized current policy before
publishing a sample. The report retains the complete final
`directional_network_policy_counters` array, and the TUI node detail shows the
current shaped-edge packet/drop summary.

Active outgoing edges are also the authoritative allowed-peer set for each
node. Background peer-count enforcement selects only allowed logical peers, and
scenario or TUI `connect_peer` actions reject a target that is not an active
outgoing edge. `disconnect_peer` remains available for removing a stale session
after topology changes.

The ordered `events` array can mutate an inventory edge with
`set_edge_condition`, `activate_edge`, `deactivate_edge`, or `restore_edge`.
`from` and `to` are required 1-based node ids. Condition updates use the same
flattened typed fields as custom edges; activation, deactivation, and restore
instead accept a positive `timeout_sec` for the verified driver peer action.
Inactive custom edges remain in the inventory and reserve their canonical band,
so later activation never renumbers another destination. `restore_edge` restores
both the configured active state and configured condition.

```json
{
  "events": [
    {
      "at": "5s",
      "action": "set_edge_condition",
      "from": 1,
      "to": 2,
      "bandwidth_mbps": 10,
      "delay_ms": 40
    },
    {
      "at": "10s",
      "action": "deactivate_edge",
      "from": 1,
      "to": 2,
      "timeout_sec": 15
    },
    {
      "at": "15s",
      "action": "restore_edge",
      "from": 1,
      "to": 2,
      "timeout_sec": 15
    }
  ]
}
```

The complete ordered action sequence is validated before resource creation,
including edge existence/state and peer-policy feasibility. Each live change is
transactional across kernel policy read-back, the synchronized logical
allow-list, driver peer state, and restart peer configuration. Successful
changes emit `topology_edge_updated`; incomplete rollback emits
`topology_edge_update_rollback_failed`. The resolved scenario preserves
`topology_initial_edges`, while the shared report exposes bounded
`topology_edge_updates` and the event-reduced `topology_current_edges`; the TUI
uses that current state for its eligible-edge count.

Partition and region node groups must assign every simulated node exactly once.
Region edges connect the first node in each region as its gateway; without
explicit `region_edges`, all region gateways form a backbone mesh. A latency
matrix uses `null` for an absent directed edge and a non-negative millisecond
value for a present directed edge; each off-diagonal value also resolves to the
edge's typed `delay_ms` condition:

```json
{
  "node_count": 3,
  "topology": {
    "node_count": 3,
    "type": "latency_matrix",
    "latency_matrix_ms": [
      [0, 20, null],
      [35, 0, 10],
      [null, null, 0]
    ]
  }
}
```

Peer-count policies are resolved against each node's eligible logical peers.
`all_peers` means all active outgoing edges for that node, and an impossible
minimum is rejected before any process or network resource is created.

Raw Firo transactions can be driven without enabling the wallet. The workload
mines mature funding to the source address, signs with the supplied regtest WIF,
submits the transaction, and waits for it in the mempool:

```json
{
  "type": "send_raw_transaction",
  "funding_node": 1,
  "submit_node": 1,
  "source_address": "TEDbE9M6woLAtvxKoFitLpFgeDHFicgTA2",
  "source_private_key": "cTpB4YiyKiBcPxnefsDpbnDxFDffjqJob8wGCEDXxgQ7zQoMXJdH",
  "destination_address": "TPxjJMGYU3jFz9zioYfGcq7w47ZGFW3Xbh",
  "funding_blocks": 101,
  "amount": "39.99000000",
  "fee": "0.01000000",
  "timeout_sec": 30
}
```

Each run writes:

- `runs/<run-id>/scenario.yaml`
- `runs/<run-id>/resolved-scenario.json`
- `runs/<run-id>/events.jsonl`
- `runs/<run-id>/metrics.jsonl`
- `runs/<run-id>/nodes/<node-id>/`

Block-generation workloads run sequentially. For each one, the
`generated_blocks` event detail is JSON with the workload index, generator node,
generated count, start and target height, reward address, and returned block
hashes. Per-node block sync confirmations are exposed in reports as
`height_reached`. Compact event summaries preserve event timestamps when they
are present in `events.jsonl`.
`wait_until_height` workloads wait for one Firo node to reach a target height
and emit a structured `height_wait_reached` event.
`wait_for_peers` workloads wait for one Firo node to report at least the target
peer count and emit a structured `peer_count_reached` event.
`disconnect_peer` workloads call Firo `disconnectnode` for one target peer, wait
for that peer address to disappear from `getpeerinfo`, and emit a structured
`peer_disconnected` event.
`connect_peer` workloads call Firo `addnode <address> onetry`, wait for the
target address in `getpeerinfo`, and emit a structured `peer_connected` event.
`send_raw_transaction` workloads use Firo raw transaction RPCs with wallet
disabled and emit a structured `raw_transaction_submitted` event.
`restart_node` workloads restart one Firo node and emit a structured
`node_restarted` event.
`freeze_node` workloads freeze one Firo node cgroup for `duration_ms`, thaw it,
and emit a structured `node_freeze_completed` event.
`update_resource_limits` workloads apply live cgroup limit changes to one node
and emit a structured `resource_limits_updated` event.
`partition_nodes` workloads install source-aware group P2P drop filters and
emit a structured `network_partition_applied` event.
`heal_partition` workloads remove matching group P2P drop filters and emit a
structured `network_partition_healed` event.
An explicit empty scenario workload list, `"workloads": []`, disables block
generation for that run.

Summarize an existing run:

```bash
./build/bbp --benchmark-root runs --report-run <run-id>
```

The compact report includes the run status, lifecycle timestamps, failure
detail when present, event counts, workload summaries, final per-node metrics,
and latest log tails.

View an existing run in the read-only TUI:

```bash
./build/bbp --benchmark-root runs --run <run-id>
```

The TUI shows run status, lifecycle timestamps, workload summary, node chain
state, resource metrics, network counters, host qdisc state, directional edge
qdisc packet/drop totals, and simulator logs. Use
the arrow keys to select a node. Press `p` to toggle its connected-peer pane or
`l` to toggle its separate log pane. Both panes support arrow keys, Page Up,
Page Down, Home, and End for scrolling; press the opening key again to close
the pane.

During a live benchmark launched without `--no-tui`, press `d` to disconnect
the selected node from the simulated network or `s` to request that its mining
operation stop. While the node log pane is open, `+` and `-` request higher or
lower daemon log verbosity. Commands pass through the chain driver; when the
selected chain does not support an operation, the TUI shows a dismissible
`Command error` popup instead of attempting a chain-specific fallback.

Without an explicit topology policy, a multi-node run starts with every node
connected to every other node. Scenario peer-count policies can replace that
default with per-node minimum and maximum connection counts.

Render one TUI frame and exit, useful for validation:

```bash
TERM=xterm ./build/bbp --benchmark-root runs --run <run-id> --once
```

## Run Network Probes

The probes validate the Linux isolation and network-control pieces used by the
simulator.

```bash
docker exec -e PROJECT_ROOT="$PROJECT_ROOT" benchmark-project-codex bash -lc \
  'cd "$PROJECT_ROOT" &&
   ./build/bbp --probe-network &&
   ./build/bbp --probe-capabilities &&
   ./build/bbp --probe-cgroup-freeze &&
   ./build/bbp --probe-netns &&
   ./build/bbp --probe-veth &&
   ./build/bbp --probe-address &&
   ./build/bbp --probe-route &&
   ./build/bbp --probe-qdisc &&
   ./build/bbp --probe-qdisc-mutation &&
   ./build/bbp --probe-bandwidth-limit &&
   ./build/bbp --probe-network-condition &&
   ./build/bbp --probe-combined-network-condition &&
   ./build/bbp --probe-directional-network-condition &&
   ./build/bbp --probe-network-condition-update'
```

Useful focused probes:

```bash
./build/bbp --probe-veth
./build/bbp --probe-capabilities
./build/bbp --probe-cgroup-freeze
./build/bbp --probe-route
./build/bbp --probe-qdisc-mutation
./build/bbp --probe-bandwidth-limit
./build/bbp --probe-network-condition
./build/bbp --probe-combined-network-condition
./build/bbp --probe-directional-network-condition
./build/bbp --probe-network-condition-update
```

The directional probe enters a temporary node namespace on a worker thread,
installs an owned 16-band `prio` root on the namespace-side veth, classifies
exact IPv4 destinations with `flower`, and verifies independent netem and
TBF-plus-netem branches through kernel read-back. It then removes the complete
policy and proves that a foreign root qdisc is rejected and preserved.

## Cleanup Checks

Check for leftover Firo daemons:

```bash
docker exec benchmark-project-codex bash -lc 'pgrep -a firod || true'
```

Check for leaked temporary veth names:

```bash
docker exec -e PROJECT_ROOT="$PROJECT_ROOT" benchmark-project-codex bash -lc \
  'cd "$PROJECT_ROOT" &&
   ./build/bbp --probe-network | rg "bbp[0-9]+[hp]" || true'
```

Remove stale simulator-owned kernel objects for a run ID:

```bash
docker exec -e PROJECT_ROOT="$PROJECT_ROOT" benchmark-project-codex bash -lc \
  'cd "$PROJECT_ROOT" &&
   ./build/bbp \
     --benchmark-root runs \
     --cleanup-run isolated-smoke'
```

## License

GPLv3. See `LICENSE`.
