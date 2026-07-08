# Benchmark Project

Linux-only local blockchain benchmark simulator.

The simulator runs real blockchain daemon processes, controls the Linux
environment around them, and records benchmark outputs. Firo is the absolute
priority for the MVP. Bitcoin Core and Monero are later targets only when they
are needed after Firo support is solid.

## Current Scope

Implemented MVP pieces:

- C++20/CMake project with Google clang-format style.
- Vendored Boost from `third_party/boost`.
- Vendored libmnl from `third_party/libmnl`.
- Firo regtest process launch and JSON-RPC smoke flow.
- cgroup v2 resource setup and metrics reads.
- Network link discovery through rtnetlink/libmnl.
- Network namespace creation and inspection.
- veth create, move-to-netns, link up, and delete.
- IPv4 address add/list/delete through rtnetlink/libmnl.
- IPv4 route add/list/delete through rtnetlink/libmnl.
- qdisc dump plus root `pfifo` replace/delete probe through rtnetlink/libmnl.
- Boost.Test unit tests for utility and network dump paths.

Not yet complete:

- Full per-node isolated runtime wiring for Firo daemons.
- Scenario parser beyond generated MVP output files.
- TBF/netem bandwidth, delay, jitter, loss, reorder, duplicate, and corrupt
  policy translation.
- tc filters/actions or nftables netlink policy support.
- ncurses TUI.
- Bitcoin Core and Monero drivers.

## Hard Rule

Simulator code must not shell out to command-line tools for OS control.

Forbidden inside simulator code:

- `ip`
- `tc`
- `nft`
- `iptables`
- cgroup command-line tools
- Docker or Podman
- shell wrappers such as `system`, `popen`, or `bash -c`

Network control is implemented through the official Linux kernel netlink ABI
using vendored libmnl. Build and validation scripts may still use normal shell
commands outside the simulator.

## Dependencies

Project library dependencies must be vendored in `third_party/` and pinned to
official release tags.

Current vendored dependencies:

- Boost: main C++ library stack, consumed through
  `add_subdirectory(third_party/boost)`.
- libmnl: netlink/rtnetlink transport and message helpers.

Approved future dependency:

- ncurses: C++ TUI implementation.

The simulator must not depend on system Boost or system libmnl packages.

## Build

Set these paths for local builds and examples:

```bash
export PROJECT_ROOT=/path/to/benchmark-project
export FIROD=/path/to/firod
```

The current development path uses a Docker container with the project mounted.
The examples below use `benchmark-project-codex` as the container name.

```bash
docker exec -e PROJECT_ROOT="$PROJECT_ROOT" benchmark-project-codex bash -lc \
  'cd "$PROJECT_ROOT" &&
   cmake -S . -B build -DCMAKE_BUILD_TYPE=Debug &&
   cmake --build build -j16'
```

Direct host build has the same CMake shape when the environment has the needed
compiler and Linux headers:

```bash
cmake -S . -B build -DCMAKE_BUILD_TYPE=Debug
cmake --build build -j16
```

## Tests

```bash
docker exec -e PROJECT_ROOT="$PROJECT_ROOT" benchmark-project-codex bash -lc \
  'cd "$PROJECT_ROOT" &&
   ctest --test-dir build --output-on-failure'
```

## Network Probes

These probes exercise kernel network operations through libmnl/rtnetlink.

```bash
./build/benchmark-sim --probe-network
./build/benchmark-sim --probe-netns
./build/benchmark-sim --probe-veth
./build/benchmark-sim --probe-address
./build/benchmark-sim --probe-route
./build/benchmark-sim --probe-qdisc
./build/benchmark-sim --probe-qdisc-mutation
```

Run them inside the Docker development container:

```bash
docker exec -e PROJECT_ROOT="$PROJECT_ROOT" benchmark-project-codex bash -lc \
  'cd "$PROJECT_ROOT" &&
   ./build/benchmark-sim --probe-veth &&
   ./build/benchmark-sim --probe-address &&
   ./build/benchmark-sim --probe-route &&
   ./build/benchmark-sim --probe-qdisc-mutation'
```

Expected capabilities for the network probes include `CAP_SYS_ADMIN` and
`CAP_NET_ADMIN`.

## Firo Regtest Smoke

Set `FIROD` to a compiled Firo daemon binary.

One-node smoke:

```bash
docker exec -e PROJECT_ROOT="$PROJECT_ROOT" -e FIROD="$FIROD" \
  benchmark-project-codex bash -lc \
  'cd "$PROJECT_ROOT" &&
   ./build/benchmark-sim
     --firod "$FIROD"
     --output-dir runs
     --run-id smoke1
     --replace-run
     --nodes 1
     --generate-blocks 1
     --ready-timeout-sec 45'
```

Two-node smoke:

```bash
docker exec -e PROJECT_ROOT="$PROJECT_ROOT" -e FIROD="$FIROD" \
  benchmark-project-codex bash -lc \
  'cd "$PROJECT_ROOT" &&
   ./build/benchmark-sim
     --firod "$FIROD"
     --output-dir runs
     --run-id smoke2
     --replace-run
     --nodes 2
     --generate-blocks 1
     --ready-timeout-sec 45'
```

Outputs are written under `runs/<run-id>/`:

- `scenario.yaml`
- `resolved-scenario.json`
- `events.jsonl`
- `metrics.jsonl`
- per-node data/log directories

## Cleanup Checks

After probe or smoke runs:

```bash
docker exec benchmark-project-codex bash -lc 'pgrep -a firod || true'
```

Check for leaked temporary benchmark veth names:

```bash
docker exec -e PROJECT_ROOT="$PROJECT_ROOT" benchmark-project-codex bash -lc \
  'cd "$PROJECT_ROOT" &&
   ./build/benchmark-sim --probe-network | rg "bs[0-9]+[hp]" || true'
```

## License

GPLv3. See `LICENSE`.
