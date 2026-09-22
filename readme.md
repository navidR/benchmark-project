# Blockchain Benchmark Project

Blockchain Benchmark Project (BBP) runs real blockchain daemons on one Linux
machine, applies controlled network and resource conditions, and records
workload events and metrics. It provides a CLI, an ncurses terminal UI (TUI),
and a local Model Context Protocol (MCP) server.

## Scope and Features

Each run uses one chain and a local test network; mainnet, testnet, and mixed-chain
runs are not supported.

| Driver | Network | Implemented scope and limits |
| --- | --- | --- |
| Firo (`firod`) | Regtest | Block generation, public and private Spark wallet workloads, raw transactions, masternode operations, and Firo-Qt connection commands. |
| Bitcoin Core (`bitcoind`) | Regtest | Daemon lifecycle, block generation, peer control, and chain metrics. Wallet initialization, transaction submission, and native continuous mining are not implemented. |
| Monero (`monerod`) | Regtest fakechain | Daemon and wallet lifecycle, wallet funding, native private transfers, transaction tracking, wallet/chain metrics, block generation, and native mining. Live connections are limited to configured startup peers. |

Shared facilities include:

- Per-node cgroup v2 CPU, memory, I/O, and process limits, with live updates.
- Network namespaces and virtual Ethernet links, enabled by default; delay,
  loss, bandwidth limits, directional conditions, and partition/heal operations.
- Configurable peer topologies, scheduled events, node restart/freeze operations,
  and live node creation and removal.
- JSON/YAML scenarios, seeded block scheduling and transaction strategies.
- Chain, wallet, resource, network, and optional Linux performance-counter
  metrics, plus retained logs and reports.

There is no fixed 16-node ceiling. `--node-capacity` is an initial reservation,
defaulting to the initial node count (at least one), and can grow with explicit
node creation. Address-pool or loopback-port limits and available host resources
still apply. The default full-mesh topology becomes expensive as node count grows.
Seeded scheduling does not make daemon execution or benchmark results deterministic.

## Requirements

- Linux with cgroup v2 and writable access to the required controllers.
- Network namespace, veth, and traffic-control support; isolated runs require
  `CAP_SYS_ADMIN` and `CAP_NET_ADMIN`.
- A C/C++ toolchain supporting C++20, CMake 3.20+, Git, and GNU Make.
- Initialized submodules for Boost, libmnl, libyaml, and ncurses; libperf sources
  are also vendored. Chain daemon binaries are built separately.
- For Firo, an executable `firo-qt` beside the resolved `firod` binary. Current
  startup generates its connection command even for headless runs.
- For Monero wallet nodes, an executable `monero-wallet-rpc` beside `monerod`.
- A terminal for the TUI, or `--no-tui` for headless use.

Use a dedicated Linux environment or a container configured for these kernel
operations. An ordinary unprivileged Docker container is not sufficient.
`--no-isolate-network` selects loopback networking; it does not remove the cgroup
requirements or permit network shaping.

Performance counters additionally need permission to call `perf_event_open`
under the host's perf and container seccomp policies. When unavailable, BBP
records availability/error fields rather than treating missing counters as zero.

At node startup, BBP attempts to raise the shared
`net.netfilter.nf_conntrack_max` limit to 1,048,576 when lower. It warns and
continues if this fails. Successful changes are not restored on exit and may
allow greater kernel memory use.

## Build

From the repository root:

```bash
git submodule update --init --recursive
cmake -S . -B build -DCMAKE_BUILD_TYPE=Release -DBUILD_TESTING=ON
cmake --build build -j$(nproc)
./build/bbp --help
```

The optional native Firo-Qt launcher is disabled by default. Enable it at
configure time with `-DFIRO_GUI_LAUNCHER=ON`; the `show-firo-qt` log command does
not require that option.

## Tests

Select the unit-test executable explicitly:

```bash
ctest --test-dir build -R '^bbp-unit-tests$' --output-on-failure
```

Despite its name, this executable also contains privilege-dependent kernel
checks, which can skip when facilities are unavailable. Setting `BBP_REAL_FIROD`
opts into a real-daemon cgroup test; leave it unset for ordinary unit testing.

The complete CTest suite also includes CLI, TUI, lifecycle, and runtime
integration tests. Inspect the list before running it in a disposable test
environment, not alongside an active benchmark:

```bash
ctest --test-dir build -N
ctest --test-dir build --output-on-failure
```

CLI probes such as `--probe-veth`, `--probe-cgroup-freeze`, and
`--probe-directional-network-condition` exercise kernel operations and are not
read-only checks. The separate [MCP acceptance client](tests/integration/mcp_discovery_client.py)
starts, stops, and cleans runs; its header documents the required Python packages.

## Quick Start

Run a finite, three-node Firo benchmark in a suitably privileged environment:

```bash
export FIROD=/absolute/path/to/firod
./build/bbp --chain firo --node-binary "$FIROD" \
  --benchmark-root runs --run-id smoke-3 --nodes 3 \
  --block-production-probability 1 --block-production-period-ms 1000 \
  --metrics-sample-count 5 --metrics-interval 1s --no-tui
```

Scheduled production makes one probability draw per period and, on success,
selects a configured miner uniformly to request a block. A positive metric
sample count gives this headless run an automatic endpoint; it is not a limit
on total wall time, which also includes setup and cleanup.

Use a new run ID for each run. `--replace-run` deletes an existing validated,
simulator-owned run directory; do not use it for results you need to keep.
Without a sample limit or scenario duration, a run stays active until explicitly
stopped. Omit `--no-tui` for interactive operation; Ctrl-C requests headless
shutdown. `--no-mining` disables scheduled production, not metrics.

BBP uses `$HOME/.bbp/bbp.lock` to allow one application instance per HOME.
Use the existing TUI/MCP connection to operate a live application.

## Workloads and Scenarios

For continuous public-wallet traffic with two wallets and one miner:

```bash
./build/bbp --chain firo --node-binary "$FIROD" \
  --benchmark-root runs --run-id wallet-load --nodes 3 \
  --wallet-node-count 2 --transaction-load-strategy random_bruteforce
```

This initializes and funds managed wallets before generating traffic.
The direct CLI also accepts `equal_fanout`; both use a default rate of 2
transactions/second and concurrency 2. These options require at least two
wallets and cannot be combined with `--scenario`. Rate, amount, funding, and
queue settings belong in a scenario for custom workloads.

A minimal `scenario.json` with a duration and scheduled checkpoint:

```json
{
  "chain": "firo",
  "simulation": {
    "name": "firo-regtest",
    "seed": 42,
    "duration": "30s",
    "metrics_interval": "1s"
  },
  "nodes": 3,
  "events": [
    {"at": "10s", "action": "checkpoint", "name": "mid-run"}
  ]
}
```

```bash
./build/bbp --scenario scenario.json --node-binary "$FIROD" \
  --benchmark-root runs --no-tui
```

JSON and YAML use the same fields. For this example, duration starts after
startup; scenarios with explicit node start/stop times instead measure from the
node-lifecycle epoch. Duration cannot be combined with a positive
`metrics_sample_count`.
`simulation.time_scale` scales scheduled simulation time; metrics and TUI refresh
intervals remain wall-clock intervals.

Scenario `workloads` describe ordered actions; `events` schedule actions with
`at` and `action`. Supported workload families include:

| Family | Actions |
| --- | --- |
| Blocks and readiness | `block_generation`, `wait_until_height`, `wait_for_peers` |
| Transactions | `wallet_transactions` (Firo and Monero), `send_raw_transaction` (Firo) |
| Lifecycle and resources | `restart_node`, `freeze_node`, `resource_pressure`, `update_resource_limits`, `set_resource_profile` |
| Network and peers | `connect_peer`, `disconnect_peer`, `set_network_condition`, `set_network_profile`, `block_network_flow`, `unblock_network_flow`, `partition_nodes`, `heal_partition` |
| Topology and evidence | `set_edge_condition`, `activate_edge`, `deactivate_edge`, `restore_edge`, `checkpoint` |

Actions remain subject to chain capabilities and network-isolation requirements.
Wallet strategies include `round_robin`, `random`, `fanout`, `hotspot`,
`random_bruteforce`, and `equal_fanout`.
Network `bandwidth_kbps` values are decimal **kilobytes/second**, not kilobits;
zero means unlimited.

See [scenario fixtures](tests/scenarios) for field-level examples, including
[wallet loads](tests/scenarios/valid-wallet-transaction-load-strategies.json)
and [network controls](tests/scenarios/valid-network-control-workloads.json).
Fixtures often contain placeholder daemon paths and are not ready-to-run
benchmarks. The MCP schema resources expose the current input contracts.

### Monero Wallet Workloads

Run two managed wallets and one miner with native private transfers:

```bash
./build/bbp --chain monero --node-binary /absolute/path/to/monerod \
  --benchmark-root runs --run-id monero-wallet-load --nodes 3 \
  --wallet-node-count 2 --transaction-load-strategy random_bruteforce \
  --memory-max-bytes 4294967296 --memory-high-bytes 3221225472
```

BBP owns each wallet RPC process in its node's cgroup and network namespace.
Wallet files persist under the node's data directory and reopen after restart.
An unexpected wallet RPC exit fails the run and triggers owned cleanup.
The network is local fakechain, although Monero uses mainnet-format addresses
there. These wallets do not spend mainnet funds.

Both generic wallet modes use Monero's native private transfers. BBP amounts
have eight decimal places; the driver converts them to Monero's twelve-place
atomic units. Monero computes fees at native priority 1; BBP's `fee` does not
override them. Funding reserves at least 0.01 XMR per planned transfer.
Coinbase outputs require 60 blocks to unlock, and ordinary transfers retain
Monero's native unlock rules. Each payment must fit one native transaction;
payments requiring splitting are rejected before relay.
Relay follows native Monero timing, so submission does not imply immediate
visibility or confirmation on every node. An uncertain relay outcome is not
automatically retried.

Allow sufficient memory for RandomX, especially when using native continuous
mining. The example provides 4 GiB per node, shared with its wallet process.
Runtime difficulty changes, relative log-verbosity changes, and Bitcoin-style
raw transaction workloads are not supported by this driver.

Verified on 2026-09-18 with native Monero v0.18.1.0-ea9be68fb: two funded
wallets sent payments in both directions, both payments confirmed on all three
nodes, both wallets retained their history after restart, and normal shutdown
removed owned processes, node cgroups, and wallet credential configurations.
A separate wallet-process kill check verified run failure and owned cleanup.

## TUI and MCP

The TUI opens by default. Tab cycles node, wallet, topology, and metric views;
arrows select entries. `l` opens node logs, `p` peers, `b` the artifact browser,
`c` the command palette, and `i` MCP connection details. Escape or `q`/`Q` opens
the exit confirmation; `y` confirms, while `n` or Escape cancels. Completed runs
can be opened read-only with `--run`.

The application automatically starts an authenticated HTTP MCP endpoint on
`127.0.0.1` with an allocated port. Connection details are logged and published
in `$HOME/.bbp/mcp/client.json`, with the bearer token in
`$HOME/.bbp/mcp/token`. Both files contain sensitive connection material and
are removed on normal shutdown; do not commit or share them.

MCP supports scenario validation/resolution, run lifecycle, runtime commands,
workload control, instrumentation, and evidence queries. Discover supported
operations and schemas through `tools/list`, `resources/list`,
`bbp:///capabilities`, and `bbp:///schemas`; do not assume every driver supports
every advertised operation. Running `./build/bbp --no-tui` without a scenario or
initial nodes opens an idle control application for MCP-managed runs.

### Firo-Qt and External Funding

For a live Firo run, BBP can generate a connection command when an executable
`firo-qt` exists beside the resolved `firod` binary. Open the command palette
(`c`) and enter:

```text
show-firo-qt
```

This reprints `manual Firo GUI command: ...` in Simulator Logs; it does not
launch the GUI. Run the printed command from a graphical session with access to
the referenced paths and peer addresses. It uses a separate wallet directory
under `operator/firo-qt`, not a managed node's data directory. Container paths
and networking must also be reachable from that session.

To fund a Firo-Qt regtest receiving address from an active public-wallet workload,
use the command palette:

```text
add-target <receiving-address>
remove-target <receiving-address>
```

`add-target` validates and registers the address; it is not an immediate payment.
Targets receive a share of ongoing `random_bruteforce`, `random`, or
`round_robin` traffic at the existing total rate. They are not managed wallets
or senders. Private traffic and explicitly targeted fan-out/hotspot workloads
are unchanged. Targets last for the current run; removal stops future selection,
but in-flight payments may finish. Submission counts are not wallet balances.

## Output and Cleanup

Artifacts live under `<benchmark-root>/<run-id>/` (default root: `runs`):

| Artifact | Contents |
| --- | --- |
| `source-scenario.json`, `resolved-scenario.json` | Stored input and resolved configuration |
| `events.jsonl` | Lifecycle, workload, command, and diagnostic events |
| `metrics.jsonl`, `wallet-metrics.jsonl` | Node and wallet samples, when collected |
| `simulator.log` | Simulator messages and generated operator commands |
| `nodes/<node-id>/` | Daemon data, configuration, and logs |
| `runtime-node-resources.json` | Owned runtime-resource inventory |

JSONL files contain one JSON record per line. Performance-counter samples retain
raw/scaled values and timing information when available. Treat run directories
as sensitive: daemon data and configuration can contain wallet material and RPC
credentials.

After the application exits, inspect retained results:

```bash
./build/bbp --benchmark-root runs --report-run smoke-3
./build/bbp --benchmark-root runs --run smoke-3
```

`--report-run` logs a JSON summary of status, events, workloads, final metrics,
and bounded recent metric history. Raw JSONL files retain the collected history.

Default cleanup stops owned processes and removes owned network/cgroup resources
while preserving artifacts. For stale resources after an interrupted run:

```bash
./build/bbp --benchmark-root runs --cleanup-run smoke-3
```

Use cleanup only for an inactive run you own, with the required privileges.
Do not remove ownership metadata or run cleanup against a live benchmark.

## License

[GNU General Public License v3](LICENSE).
