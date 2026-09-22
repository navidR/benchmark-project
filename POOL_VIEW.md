# Transaction pool view

Press `o` or cycle with `Tab` to open the pool. Arrows, Page Up/Down, Home
and End move the selector through transaction IDs in ascending order. `x` or
Enter cycles focus between the list, pool totals and selected transaction
details; the same keys scroll each pane. Selection stays with its transaction
through refreshes. A departure shows the known reason, or explicitly says
unknown, and selects the closest remaining row by position.

The displayed source stays selected while healthy. A source switch is announced;
absence on another node is not proof of mining or eviction. Each request obtains
a current node-generation lease. All node reads and decoding belong to drivers.
A background TUI worker refreshes at a 500 ms cadence with one request at a
time and cancels obsolete reads during navigation. Shutdown cancels and drains reads
before releasing node state. Workload generation and all-node observation are
independent of this single-node browsing view.

MCP `pool.query` accepts `run_id`, optional `selected_id`, optional fallback
`index` (0–65535), and `limit` (1–32). The `pool` resource uses the same service;
successful captures publish `pool` subscription notifications. Unknown fields
are JSON `null` and displayed as `N/A`. Units are carried in each summary.

BBP retains one current snapshot and one selected detail, with a limit of 65,536
transactions and 32 MiB of normalized JSON; exceeding either bound gives an
explicit display error. `transaction-pool.json` is atomically replaced after
successful reads. Retained viewers use only that capture, show its Unix sample
time in milliseconds, and compute ages at that time. Other uncaptured detail
fields are explicitly unavailable. They never contact a live daemon. A run
that was never queried has no pool capture.

| Quantity | Bitcoin / Firo | Monero |
|---|---|---|
| Serialized bytes | Selected verbose raw transaction `size`, or decoded `hex` length. Pool `size`/`vsize` are policy sizes and cannot supply full serialized-byte totals. | Pool `blob_size`: stored serialized blob bytes, possibly pruned, not inferred full bytes. |
| Weight | Explicit daemon `weight`, otherwise unknown. | Explicit native `weight`; absent/zero legacy fields are unknown. |
| Metadata bytes | Explicit `extraPayload` decoded length or `extraPayloadSize`; otherwise unknown. | Decoded `extra` byte count from `tx_json`. |
| Fee | Base fee, converted exactly to units of 10^-8 coin; priority deltas excluded. | Native integer atomic fee, in units of 10^-12 coin. |
| Fee rate | Base fee divided by explicit `vsize_adjusted`, `vsize`, or legacy `size`, in that order. | Atomic fee divided by native weight. |
| Dependencies / ancestry | Explicit daemon fields; ancestor/descendant counts and virtual sizes include self. | Unknown; ring members are not dependencies. |
| Replacement / relay | Explicit BIP125 status and inverse `unbroadcast` (initial broadcast acknowledged) only. | Replacement unknown; explicit `relayed`, `do_not_relay`, double-spend and last-validation-failure facts. |

Size/weight/fee aggregates require every row's corresponding value; fee-rate
averages are arithmetic means of known per-transaction rates. Memory usage is
a separate same-node `getmempoolinfo.usage` sample where exposed, not serialized
bytes. First-seen timestamps use daemon receive/admission time in Unix seconds.
Input/output counts count decoded RPC entries. No private amounts, missing
bytes, fees, replacement relation or eviction reason are estimated.
