# Benchmark Project

Benchmark Project is a Linux-local blockchain benchmark simulator.

The goal is to run real blockchain daemon processes locally, isolate them like
separate nodes, apply controlled CPU, memory, process, and network conditions,
drive benchmark workloads, and write reproducible metrics.

Firo is the current priority. Bitcoin Core and Monero are future targets after
the Firo path is working end to end.

## What Works Now

- Start up to 16 Firo regtest nodes.
- Run Firo nodes inside isolated network namespaces with one veth pair per node.
- Apply simple per-node network conditions through host-side `netem` or TBF.
- Apply and remove live per-node TCP destination block rules.
- Apply live per-node cgroup resource updates after startup.
- Restart a running Firo node before workload generation.
- Freeze and thaw a running Firo node cgroup for a bounded duration.
- Wait for JSON-RPC readiness.
- Generate regtest blocks.
- Wait for generated blocks to propagate before final metrics are recorded.
- Record optional periodic metric samples between runtime events and workloads.
- Record cgroup usage, pressure, event counters, and configured limits.
- Record Firo daemon version, protocol version, and subversion in metrics.
- Record event and metric files under a run directory.
- View a run directory through a read-only ncurses TUI.
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
   ./build/benchmark-sim \
     --firod "$FIROD" \
     --output-dir runs \
     --run-id smoke1 \
     --replace-run \
     --nodes 1 \
     --generate-blocks 1 \
     --ready-timeout-sec 45'
```

Multiple Firo nodes:

```bash
docker exec -e PROJECT_ROOT="$PROJECT_ROOT" -e FIROD="$FIROD" \
  benchmark-project-codex bash -lc \
  'cd "$PROJECT_ROOT" &&
   ./build/benchmark-sim \
     --firod "$FIROD" \
     --output-dir runs \
     --run-id smoke3 \
     --replace-run \
     --nodes 3 \
     --generate-blocks 1 \
     --ready-timeout-sec 45 \
     --sync-timeout-sec 45'
```

Multiple isolated Firo nodes:

```bash
docker exec -e PROJECT_ROOT="$PROJECT_ROOT" -e FIROD="$FIROD" \
  benchmark-project-codex bash -lc \
  'cd "$PROJECT_ROOT" &&
   ./build/benchmark-sim \
     --firod "$FIROD" \
     --output-dir runs \
     --run-id isolated-smoke \
     --replace-run \
     --nodes 3 \
     --generate-blocks 1 \
     --ready-timeout-sec 45 \
     --sync-timeout-sec 45 \
     --isolate-network'
```

Isolated Firo node with a default network condition:

```bash
docker exec -e PROJECT_ROOT="$PROJECT_ROOT" -e FIROD="$FIROD" \
  benchmark-project-codex bash -lc \
  'cd "$PROJECT_ROOT" &&
   ./build/benchmark-sim \
     --firod "$FIROD" \
     --output-dir runs \
     --run-id isolated-delay \
     --replace-run \
     --nodes 1 \
     --generate-blocks 1 \
     --ready-timeout-sec 45 \
     --isolate-network \
     --network-delay-ms 5'
```

Isolated Firo node with a bandwidth limit:

```bash
docker exec -e PROJECT_ROOT="$PROJECT_ROOT" -e FIROD="$FIROD" \
  benchmark-project-codex bash -lc \
  'cd "$PROJECT_ROOT" &&
   ./build/benchmark-sim \
     --firod "$FIROD" \
     --output-dir runs \
     --run-id isolated-bandwidth \
     --replace-run \
     --nodes 1 \
     --generate-blocks 1 \
     --ready-timeout-sec 45 \
     --isolate-network \
     --network-bandwidth-mbps 20'
```

Per-node isolated network conditions use repeatable JSON objects:

```bash
./build/benchmark-sim \
  --firod "$FIROD" \
  --output-dir runs \
  --run-id isolated-per-node \
  --replace-run \
  --nodes 2 \
  --generate-blocks 1 \
  --isolate-network \
  --node-network-condition-json '{"node":2,"bandwidth_mbps":20}'
```

Runtime network updates use the same JSON shape and are applied after nodes are
running, before block generation:

```bash
./build/benchmark-sim \
  --firod "$FIROD" \
  --output-dir runs \
  --run-id live-netem \
  --replace-run \
  --nodes 2 \
  --generate-blocks 1 \
  --isolate-network \
  --runtime-node-network-condition-json '{"node":2,"bandwidth_mbps":10}'
```

Runtime block/unblock rules match a destination IPv4 address and TCP port on a
node's host-side veth. This example applies and then removes the same rule
before block generation:

```bash
./build/benchmark-sim \
  --firod "$FIROD" \
  --output-dir runs \
  --run-id live-block-unblock \
  --replace-run \
  --nodes 2 \
  --generate-blocks 1 \
  --isolate-network \
  --runtime-node-block-json '{"node":1,"dst_address":"10.210.1.2","dst_port":18168}' \
  --runtime-node-unblock-json '{"node":1,"dst_address":"10.210.1.2","dst_port":18168}'
```

Runtime partition/heal currently supports two singleton node groups:

```bash
./build/benchmark-sim \
  --firod "$FIROD" \
  --output-dir runs \
  --run-id live-partition-heal \
  --replace-run \
  --nodes 2 \
  --generate-blocks 1 \
  --isolate-network \
  --runtime-partition-json '{"group_a":[1],"group_b":[2]}' \
  --runtime-heal-partition-json '{"group_a":[1],"group_b":[2]}'
```

The current implementation applies bandwidth with TBF and delay/loss conditions
with `netem`. When both are requested, TBF is the root qdisc and netem is
attached below it. The netem fields are `delay_ms`, `jitter_ms`,
`loss_basis_points`, `duplicate_basis_points`, `corrupt_basis_points`,
`reorder_basis_points`, and `limit_packets`.

Default resource limits apply to each node cgroup:

```bash
./build/benchmark-sim \
  --firod "$FIROD" \
  --output-dir runs \
  --run-id resource-smoke \
  --replace-run \
  --nodes 1 \
  --generate-blocks 1 \
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
./build/benchmark-sim \
  --firod "$FIROD" \
  --output-dir runs \
  --run-id live-resources \
  --replace-run \
  --nodes 1 \
  --generate-blocks 1 \
  --runtime-node-resource-json '{"node":1,"memory_high_bytes":1073741824,"cpu_quota_us":50000,"cpu_period_us":100000,"pids_max":128}'
```

Runtime restarts stop a node through Firo RPC, respawn it in the same
cgroup/network/data directory, wait for RPC readiness, then continue the run:

```bash
./build/benchmark-sim \
  --firod "$FIROD" \
  --output-dir runs \
  --run-id restart-smoke \
  --replace-run \
  --nodes 1 \
  --generate-blocks 1 \
  --runtime-node-restart-json '{"node":1}'
```

Runtime freezes pause a node cgroup for a bounded duration, verify frozen and
thawed states, then continue the run:

```bash
./build/benchmark-sim \
  --firod "$FIROD" \
  --output-dir runs \
  --run-id freeze-smoke \
  --replace-run \
  --nodes 1 \
  --generate-blocks 1 \
  --runtime-node-freeze-json '{"node":1,"duration_ms":100}'
```

Extra metric samples can be collected between runtime updates/restarts and
block generation:

```bash
./build/benchmark-sim \
  --firod "$FIROD" \
  --output-dir runs \
  --run-id sampled-smoke \
  --replace-run \
  --nodes 1 \
  --generate-blocks 1 \
  --metrics-sample-count 5 \
  --metrics-interval-ms 1000
```

The same run settings can be loaded from a JSON scenario file:

```json
{
  "firod": "/path/to/firod",
  "output_dir": "runs",
  "run_id": "scenario-smoke",
  "nodes": 1,
  "generate_blocks": 1,
  "ready_timeout_sec": 45,
  "sync_timeout_sec": 45,
  "metrics_sample_count": 5,
  "metrics_interval_ms": 1000,
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
        "dst_address": "10.210.1.2",
        "dst_port": 18168
      }
    ],
    "runtime_node_unblocks": [
      {
        "node": 1,
        "dst_address": "10.210.1.2",
        "dst_port": 18168
      }
    ],
    "runtime_partitions": [
      {
        "group_a": [1],
        "group_b": [2]
      }
    ],
    "runtime_partition_heals": [
      {
        "group_a": [1],
        "group_b": [2]
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
./build/benchmark-sim --scenario-json /path/to/scenario.json --replace-run
```

Each run writes:

- `runs/<run-id>/scenario.yaml`
- `runs/<run-id>/resolved-scenario.json`
- `runs/<run-id>/events.jsonl`
- `runs/<run-id>/metrics.jsonl`
- `runs/<run-id>/nodes/<node-id>/`

Summarize an existing run:

```bash
./build/benchmark-sim --report-run runs/<run-id>
```

View an existing or active run in the read-only TUI:

```bash
./build/benchmark-tui --run runs/<run-id>
```

Render one TUI frame and exit, useful for validation:

```bash
TERM=xterm ./build/benchmark-tui --run runs/<run-id> --once
```

## Run Network Probes

The probes validate the Linux isolation and network-control pieces used by the
simulator.

```bash
docker exec -e PROJECT_ROOT="$PROJECT_ROOT" benchmark-project-codex bash -lc \
  'cd "$PROJECT_ROOT" &&
   ./build/benchmark-sim --probe-network &&
   ./build/benchmark-sim --probe-capabilities &&
   ./build/benchmark-sim --probe-cgroup-freeze &&
   ./build/benchmark-sim --probe-netns &&
   ./build/benchmark-sim --probe-veth &&
   ./build/benchmark-sim --probe-address &&
   ./build/benchmark-sim --probe-route &&
   ./build/benchmark-sim --probe-qdisc &&
   ./build/benchmark-sim --probe-qdisc-mutation &&
   ./build/benchmark-sim --probe-bandwidth-limit &&
   ./build/benchmark-sim --probe-network-condition &&
   ./build/benchmark-sim --probe-combined-network-condition &&
   ./build/benchmark-sim --probe-network-condition-update'
```

Useful focused probes:

```bash
./build/benchmark-sim --probe-veth
./build/benchmark-sim --probe-capabilities
./build/benchmark-sim --probe-cgroup-freeze
./build/benchmark-sim --probe-route
./build/benchmark-sim --probe-qdisc-mutation
./build/benchmark-sim --probe-bandwidth-limit
./build/benchmark-sim --probe-network-condition
./build/benchmark-sim --probe-combined-network-condition
./build/benchmark-sim --probe-network-condition-update
```

## Cleanup Checks

Check for leftover Firo daemons:

```bash
docker exec benchmark-project-codex bash -lc 'pgrep -a firod || true'
```

Check for leaked temporary veth names:

```bash
docker exec -e PROJECT_ROOT="$PROJECT_ROOT" benchmark-project-codex bash -lc \
  'cd "$PROJECT_ROOT" &&
   ./build/benchmark-sim --probe-network | rg "bs[0-9]+[hp]" || true'
```

Remove stale simulator-owned kernel objects for a run ID:

```bash
docker exec -e PROJECT_ROOT="$PROJECT_ROOT" benchmark-project-codex bash -lc \
  'cd "$PROJECT_ROOT" &&
   ./build/benchmark-sim \
     --output-dir runs \
     --run-id isolated-smoke \
     --cleanup-run'
```

## License

GPLv3. See `LICENSE`.
