# Chain view

Press `v` or cycle with `Tab` to browse blocks. The list runs from genesis
to tip. Arrows, Page Up/Down, Home and End move the selector. Entry selects
the tip; moving away holds the selection as blocks arrive. End resumes tip
following. `x` or Enter cycles focus between blocks, block details and
transactions; the same navigation keys scroll the focused pane.

The source node is shown explicitly. BBP keeps that source while it answers
and announces a switch when it becomes unavailable. All RPC calls and decoding
belong to the active chain driver. A background TUI worker cancels obsolete
reads; drawing and keyboard input do not wait for RPC. MCP `chain.query`
accepts `run_id`, optional `first_height`/`selected_height`, and a `limit` of
1–32. Omitting heights selects the latest block. The `chain` resource reads
the same service. Captured-page changes publish `chain` evidence notifications
through the existing subscription API, regardless of which frontend read it.

Only a visible page, selected detail and two adjacent summaries are fetched.
The cache keeps at most 64 summaries and four details totaling at most 2 MiB
of normalized JSON. Larger individual details report an explicit display-limit
error. Missing/pruned data reports an error on its row and detail pane.
Unknown/inapplicable fields appear as `N/A`, represented by JSON `null`.
Reorgs discard cached entries above a verified common cached ancestor; when
none is available, all uncertain entries are discarded. Details are bound to
both height and hash and checked against a final canonical-chain read.

`chain-blocks.json` retains the bounded cache atomically in the run directory.
It is a browsing snapshot, not a blockchain archive. Retained viewers use only
these captured records and explicitly report uncaptured blocks/details. They
never connect to a live daemon. Confirmations and tip are captured values;
displayed age is elapsed wall time since the recorded block timestamp.

## Byte and weight definitions

All sizes are bytes. Averages and totals include the miner/coinbase transaction
and are available only when every transaction has a known full serialized size.
Block weight and transaction weight retain chain-native units and are separate
from serialized bytes. No weight is inferred from rounded virtual size.

| Field | Bitcoin and Firo | Monero |
|---|---|---|
| Block bytes | `getblock` serialized `size`, including witness where supported | Length of `get_block.blob` decoded from hex; includes header, miner transaction, count and non-miner transaction hashes, **not** other transaction bodies |
| Header bytes | Length of raw `getblockheader` hex | Encoded major/minor versions and timestamp (three varints), previous hash and nonce in the returned blob |
| Transaction bytes | Verbose block transaction `size` | Full `/get_transactions` hex; a pruned prefix alone is not a full size |
| Block metadata bytes | Block bytes minus all transaction bytes | Block blob bytes minus miner transaction bytes, thus including header and transaction-hash list |
| Transaction metadata bytes | Explicit `extraPayload` hex length or `extraPayloadSize`; otherwise unknown | Number of bytes in the decoded `extra` field |
| Weight | Explicit RPC `weight`, otherwise unknown | Explicit `block_weight`; per-transaction weight is unavailable |
| Fee | Explicit RPC `fee` or Firo Spark `nFees`, in native coins; otherwise unknown | Explicit RingCT `txnFee`, in atomic units (10^-12 XMR); otherwise unknown |

Input/output counts count RPC entries; Monero ring members are not counted as
separate inputs. Coinbase status comes from Bitcoin/Firo `vin.coinbase` or
Monero's explicit miner-transaction identity. No fee is guessed from amounts,
and no private transaction information is inferred.
