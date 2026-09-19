# Reporting and TUI measurements — September 19, 2026

The reporting change removes three measured sources of redundant work: copying
parsed JSON objects, rereading a retained run after loading its MCP metadata,
and rebuilding derived summaries when neither records nor recovered manifest
fields changed. The TUI keeps one incremental reader. File replacement checks,
manifest validation, record counts, bounded histories, and cancellation remain
in effect. A dirty flag survives an interrupted refresh.

## Retained run

Compared against `0f81b4c2027425aeaec0986ac99c9367626f353c` on an Intel
Core i9-14900HX (32 logical CPUs), Linux x86-64, strict **Debug** builds
(`CMAKE_CXX_FLAGS_DEBUG=-g`). These are development-build measurements, not
Release-build estimates. No build or test suite ran during the measurements.

The input is the completed 20-node, 20-wallet, full-mesh Firo run
`runs/rpc-pressure-20260919/rpc-pressure-after`, with 1,000 transactions observed
on every node. Input sizes and SHA-256 hashes:

| File | Bytes | SHA-256 |
| --- | ---: | --- |
| resolved-scenario.json | 49,047 | e4d63ae63be598fb68e09f56d4f46e11c5767d5fe53606a63ac53395324d94bd |
| events.jsonl | 44,701,976 | 8bd6050516868ba19ae413fa0eafc1093c45b89bbc943421829e0da4a120a120 |
| metrics.jsonl | 11,501,346 | 9bb7830ca6f28c45320389ce672f169374813e2b78765d04505630c50ab2fe14 |
| wallet-metrics.jsonl | 39,050,322 | 10a82e9792d7adf5e6c6784ba9b5ed41771c4e39fa7a4477ac271e3dcd1fef6f |

The manual benchmark uses a 45-row, 160-column PTY, a 250-ms refresh setting,
and 32 alternating wallet/node view switches, with 150 ms between switches.
Three alternating before/after pairs completed successfully. The first pair
also recorded `perf` CPU samples; the other two did not. Startup, CPU, and
memory values below are medians of the three runs. Switch measurements pool
all 96 samples per binary.

| Measurement | Before | After |
| --- | ---: | ---: |
| First node-view heading | 7.015 s | 5.814 s |
| BBP user + system CPU through the last switch | 14.40 s | 5.89 s |
| BBP peak resident memory | 59,380 KiB | 42,112 KiB |
| Median switch response | 96.71 ms | 3.23 ms |
| Maximum measured switch response | 1,510.15 ms | 4.86 ms |

CPU use fell 59% and peak resident memory fell 29% for this interaction.
The fixed input cadence can align with the TUI's 50-ms input polling; these
response measurements are not worst-case latency guarantees. Retained startup
still constructs a complete report for accurate MCP metadata before drawing.

The initial retained-TUI profile attributed 78.67% of user CPU samples to report
refresh, including 18.69% to rebuilding summaries. Inclusive call-stack
percentages overlap and must not be added. Sampling used `cpu-clock:u`, 99 Hz,
and 8-KiB DWARF stacks. Profiling is optional developer tooling, not a BBP
runtime dependency.

Reproduce using saved before/after binaries built with identical settings and
the same unmodified retained run (or substitute another substantial run):

```sh
python3 tests/benchmarks/retained_tui.py /path/to/bbp-before /path/to/run /tmp/bbp-before-1 --perf
python3 tests/benchmarks/retained_tui.py build-ninja/bbp /path/to/run /tmp/bbp-after-1 --perf
```

Repeat with fresh output prefixes, omitting `--perf` for unprofiled pairs.
The script writes timing/CPU/memory JSON and a terminal transcript; with
`--perf`, it also writes a profile. It uses Python's standard library and
reads BBP's own `/proc` CPU and peak-memory counters, excluding the profiler.
Use a quiet host and keep terminal size, refresh interval, dataset, build type,
and interaction sequence identical. Analyze a profile with:

```sh
DEBUGINFOD_URLS= perf report --stdio --children -g none -i /tmp/bbp-before-1.perf
```

## Live workload and correctness

Live acceptance uses the same read-only Firo binary on both sides (SHA-256
`05bfc8b3d049ebd0e371cecdcf07656c3a9553a7b9b9f843100f749a9a724102`),
20 isolated nodes and wallets, full-mesh peers, one miner overlapping wallet
20, scheduled blocks every five seconds, 20 workload workers, queue capacity
4096, and a 50-transactions/s **target**, capped at 400 transactions. The test
uses a 240-second window including daemon startup and normal SIGINT cleanup
if the interactive session remains open. The target rate is not a measured
throughput claim. No node sampling or workload reduction is part of the patch.

Both live runs submitted all 400 transactions and retained exactly 8,000 unique
visibility records and 8,000 unique confirmation records (400 transactions ×
20 nodes). Each had zero warning/error lines, all 20 nodes reached `Cleaned`,
and no recorded daemon process, owned cgroup, or host veth remained. Both BBP
sessions exited successfully.

| Live view switching | Before | After |
| --- | ---: | ---: |
| Median | 90.21 ms | 83.28 ms |
| 95th percentile | 140.13 ms | 132.74 ms |
| Maximum measured | 170.55 ms | 144.40 ms |

These single-run live timings establish comparable responsiveness; the large,
repeatable improvement is in retained reporting. Filtered live CPU samples
were also collected, but most inherited DWARF stacks could not be unwound;
precise stack attribution comes from the retained profile.

Full CLI reports on the unchanged retained dataset were structurally identical.
One CLI comparison took 6.371 s before versus 5.542 s after. Existing report,
partial-record, file-replacement, cancellation, and TUI lifecycle tests were
reused. One new regression covers a manifest-only node removal invalidating
cached summaries when no new JSONL records arrive.

Artifacts for the comparison, including exact scenarios, profiles, terminal
transcripts and timing JSON, are retained locally under
`/tmp/bbp-performance-20260919-41itpusl`. Native run data is under
`runs/performance-20260919`. Run directories and large profiles are not Git
artifacts.

The following scenario reproduces the live workload; choose a fresh run ID and
output directory, and point `default_binary` at the same Firo build:

```json
{
  "run_id": "performance-local",
  "chain": "firo",
  "output_dir": "/tmp/bbp-performance-runs",
  "simulation": {"metrics_interval": "1s", "cleanup_policy": "automatic"},
  "chains": {"firo": {"driver": "firo", "default_binary": "/path/to/firod"}},
  "nodes": 20,
  "node_capacity": 20,
  "network_address_pool": "10.0.0.0/8",
  "isolated_network": true,
  "ready_timeout_sec": 120,
  "sync_timeout_sec": 120,
  "metrics_sample_count": 60,
  "block_production": {
    "enabled": true, "native_mining": false,
    "period_ms": 5000, "probability": 1, "seed": 7
  },
  "topology": {
    "type": "full_mesh", "node_count": 20,
    "wallet_node_count": 20, "miner_node_count": 1,
    "wallet_nodes": [1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20],
    "miner_nodes": [20], "allow_miner_wallet_overlap": true,
    "wallet_initialization": {"strategy": "driver_rpc", "mode": "public"}
  },
  "workloads": [
    {
      "type": "wallet_transactions", "strategy": "random_bruteforce",
      "funding_strategy": "round_robin", "funding_blocks_per_wallet": 101,
      "readiness_confirmations": 101, "funding_threshold": "1.00100000",
      "concurrency": 20, "queue_capacity": 4096, "mode": "public",
      "amount": {"distribution": "uniform", "min": "0.01000000", "max": "0.10000000"},
      "fee_policy": "fixed", "fee": "0.00001000",
      "retained_balance_percentage": 80, "seed": 20260918,
      "timeout_sec": 120, "transaction_count": 400, "transaction_rate": 50
    },
    {"type": "block_generation", "node": 20, "count": 1, "sync_timeout_sec": 120}
  ]
}
```

Launch in a terminal with `build-ninja/bbp --scenario /path/to/scenario.json
--refresh-ms 250`. Live profiling must include BBP threads created after
startup; when recording inherited samples, filter analysis to BBP's process,
excluding daemon processes. The first live frame includes daemon startup and
peer readiness, so it is not comparable to retained-viewer startup.

Validation: strict Debug build with `-j$(nproc)`, 60 focused report cases,
15 focused report/TUI/capacity CTests, and all 392 serial CTests passed
(the complete suite took 279.20 seconds).
