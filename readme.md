# Benchmark Project

Benchmark Project is a Linux-local blockchain benchmark simulator.

The goal is to run real blockchain daemon processes locally, isolate them like
separate nodes, apply controlled CPU, memory, process, and network conditions,
drive benchmark workloads, and write reproducible metrics.

Firo is the MVP priority. Bitcoin Core and Monero are future targets after the
Firo path is working end to end.

## What Works Now

- Start one or two Firo regtest nodes.
- Run Firo nodes inside isolated network namespaces with one veth pair per node.
- Apply a simple per-node network condition through host-side `netem`.
- Wait for JSON-RPC readiness.
- Generate regtest blocks.
- Wait for generated blocks to propagate before final metrics are recorded.
- Record event and metric files under a run directory.
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

Two Firo nodes:

```bash
docker exec -e PROJECT_ROOT="$PROJECT_ROOT" -e FIROD="$FIROD" \
  benchmark-project-codex bash -lc \
  'cd "$PROJECT_ROOT" &&
   ./build/benchmark-sim \
     --firod "$FIROD" \
     --output-dir runs \
     --run-id smoke2 \
     --replace-run \
     --nodes 2 \
     --generate-blocks 1 \
     --ready-timeout-sec 45 \
     --sync-timeout-sec 45'
```

Two isolated Firo nodes:

```bash
docker exec -e PROJECT_ROOT="$PROJECT_ROOT" -e FIROD="$FIROD" \
  benchmark-project-codex bash -lc \
  'cd "$PROJECT_ROOT" &&
   ./build/benchmark-sim \
     --firod "$FIROD" \
     --output-dir runs \
     --run-id isolated-smoke \
     --replace-run \
     --nodes 2 \
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
  --node-network-condition-json '{"node":2,"delay_ms":5}'
```

Each run writes:

- `runs/<run-id>/scenario.yaml`
- `runs/<run-id>/resolved-scenario.json`
- `runs/<run-id>/events.jsonl`
- `runs/<run-id>/metrics.jsonl`
- `runs/<run-id>/nodes/<node-id>/`

## Run Network Probes

The probes validate the Linux isolation and network-control pieces used by the
simulator.

```bash
docker exec -e PROJECT_ROOT="$PROJECT_ROOT" benchmark-project-codex bash -lc \
  'cd "$PROJECT_ROOT" &&
   ./build/benchmark-sim --probe-network &&
   ./build/benchmark-sim --probe-netns &&
   ./build/benchmark-sim --probe-veth &&
   ./build/benchmark-sim --probe-address &&
   ./build/benchmark-sim --probe-route &&
   ./build/benchmark-sim --probe-qdisc &&
   ./build/benchmark-sim --probe-qdisc-mutation &&
   ./build/benchmark-sim --probe-network-condition'
```

Useful focused probes:

```bash
./build/benchmark-sim --probe-veth
./build/benchmark-sim --probe-route
./build/benchmark-sim --probe-qdisc-mutation
./build/benchmark-sim --probe-network-condition
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
