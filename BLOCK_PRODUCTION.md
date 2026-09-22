# Accumulating transactions before scheduled mining

For transaction-load tests, add both fields to a scenario's `block_production`:

```json
{
  "enabled": true,
  "period_ms": 250,
  "probability": 1,
  "min_pool_transactions": 16,
  "max_pool_wait_ms": 5000
}
```

At each successful scheduled draw, BBP waits until the selected miner reports
at least 16 pool transactions or five seconds have elapsed, then requests one
block. The deadline also cancels a pending pool observation. Shutdown cancels
the wait without mining. A low or empty pool at the deadline permits a block,
so exhausted wallet inputs, slow submission, and final partial batches cannot
prevent chain progress indefinitely. The threshold is a target, not a promise:
propagation, validation, transaction conflicts and native block limits can
change the number actually included.

Both fields default to zero, preserving immediate scheduled production. Set
both to positive integers to enable accumulation; `max_pool_wait_ms` must be
at most 3,600,000. These are wall-clock milliseconds. The limit bounds only
accumulation, not scheduler delay, block-generation lock admission or daemon
mining time. The regular period, probability and miner selection still apply.
The policy requires enabled scheduled mining and is rejected for native mining.
It is configured per run; changing the live Bernoulli policy does not change
these accumulation fields.

Pool counts and block generation use the chain-driver interface. No pool
snapshot is taken from a different node, and the mining lock is not held during
accumulation. Explicit block commands and workload/funding blocks keep their
existing semantics. Wallet submission rate, concurrency and all-node observation
are unchanged. Avoid competing explicit/native miners when testing this policy.

`scheduled_block_produced` events record `non_reward_transaction_counts`
(aligned with `hashes`), `pool_transactions_before_mining` (the last count seen
during accumulation), `pool_wait_ms` and `pool_threshold_reached`. Failed count
reads are null. The pool count is null when accumulation is disabled or no read
completed. Reward/coinbase transactions are excluded; a chain browser showing
one total transaction can therefore be displaying an empty workload block.

Run reports and the MCP mining resource expose
`scheduled_block_transaction_distribution`: observed and unobserved block
counts, total non-reward transactions, minimum, maximum, mean and six fixed
buckets (0, 1, 2–9, 10–99, 100–999, 1000+). This summary covers every recorded
scheduled block, including empty startup and idle blocks, even after detailed
history exceeds its 256-event limit. Missing legacy observations are counted
as unobserved, not zero. It excludes manual/funding blocks and is not a census
of a reorg-adjusted canonical chain.

## Functional evidence

The operator's retained Monero FCMP run `m12-fcmp-base-260922-163627` configured
250 ms mining with probability 1. Its records contain 5,570 submissions and
5,231 mined non-reward transactions at the miner's final metric sample.
Across 708 node samples, pool occupancy reached 458; the miner's 59 samples
had median 0 and maximum 97. The selected captured window at heights
2238–2284 contains 32 empty, nine one-transaction and six multi-transaction
blocks after excluding rewards. Its largest block has 16 non-reward
transactions. This selected window and sparse sampling do not establish a
whole-run distribution or a daemon throughput limit. Only initial/final
wallet in-flight snapshots were retained, so continuous baseline concurrency
cannot be reconstructed.

An isolated four-node Monero run with the same FCMP binary, 128 attempted
transactions, target 50/s, 16 workers, threshold 16 and a five-second maximum
wait submitted and confirmed 127 transactions on every node. All eight
nonempty scheduled blocks contained multiple transactions (3–25). One wallet
attempt reported no unlocked/usable money; it was recorded as a failure.
All nodes and owned network resources cleaned up. With 140 funding blocks per
wallet, two subsequent runs each submitted and confirmed all 128 transactions
on all four nodes, with zero failed attempts. Their nonempty block counts were
`10, 11, 10, 26, 29, 42` and `10, 10, 11, 22, 23, 26, 26`. In the final run,
89 live workload samples measured up to four submissions in flight and 105
transactions awaiting confirmation; the miner's sampled pool reached 44.
The final report includes 12 empty startup/idle blocks and seven nonempty
blocks, with 128 non-reward transactions in total. During the interval from
first submission to final all-node confirmation, seven of eight scheduled
blocks held multiple transactions and one was empty. Maximum observed
accumulation wait was 5,000 ms. All three runs cleaned up their processes and
network resources. These are functional checks, not daemon throughput
comparisons, and the configured 50/s rate is a requested target.

The final scenario is `tests/scenarios/monero-dense-blocks.json`. Reproduce it
with a Monero daemon and matching wallet RPC binary built for the desired
regtest hard-fork configuration:

```sh
build-ninja/bbp --scenario tests/scenarios/monero-dense-blocks.json \
  --node-binary /path/to/monerod --no-tui --keep-artifacts
```

The checked scenario retains a 90-second run limit and isolated networking.
Use a fresh run ID/directory for each execution. Test artifacts are under
`runs/dense-blocks-20260922/monero-accumulate-{1,2,3}`; scenarios, live samples,
checks and logs are in `/tmp/bbp-resume-20260922/dense-blocks`. The operator's
original run and the read-only daemon source/build trees were untouched.

The stock MCP client's output validator rejected an existing wallet workload
result schema (nullable duration and resolved accounting/configuration fields).
The final measurement used the client's low-level typed request API and
retained raw responses; this separate schema defect is recorded in the backlog.
