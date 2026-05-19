# IPLD Data Model Specification

This document describes the IPLD DAG structure produced by `blobscan-ipld`.
All nodes are encoded in **dag-cbor**. Raw blob data uses the **raw** codec.
All map keys are sorted lexicographically to ensure deterministic CIDs.

---

## Node hierarchy

```
NetworkRoot (dag-cbor)
└── epochs: { "269568" → &EpochNode, "269569" → &EpochNode, … }
                              │
                         EpochNode (dag-cbor)
                         └── blobs
                               ├── BlobMap (dag-cbor, ≤ hamt_threshold blobs)
                               │   └── entries: { "<versionedHash>" → [&BlobMetadata, …], … }
                               └── HAMTRoot (dag-cbor, > hamt_threshold blobs)
                                   └── shards: [ &HAMTShard, … ]
                                                      │
                                                 BlobMetadata (dag-cbor)
                                                 └── data → &Blob (raw)
```

The `NetworkRoot` is rebuilt from scratch after **every** epoch is processed.
Its CID therefore changes on each update but always represents the complete
current state of the indexed network.

---

## NetworkRoot

**Codec:** dag-cbor  
**CID hash:** sha2-256

| Field | Type | Description |
|-------|------|-------------|
| `network` | `String` | Network name, e.g. `"mainnet"` |
| `latestEpoch` | `Int` | Highest epoch number indexed so far |
| `totalSizeBytes` | `Int` | Approximate cumulative blob data size |
| `epochs` | `{String : &EpochNode}` | Map from epoch number (decimal string) to EpochNode CID |

**Key design:** Epoch numbers are stored as decimal strings (e.g. `"269568"`)
rather than integers so the map can be traversed with standard IPLD path
selectors (`/epochs/269568`).

**Example (JSON-equivalent):**
```json
{
  "network": "mainnet",
  "latestEpoch": 269570,
  "totalSizeBytes": 11010048,
  "epochs": {
    "269568": { "/": "bafyreib..." },
    "269569": { "/": "bafyreic..." },
    "269570": { "/": "bafyreid..." }
  }
}
```

---

## EpochNode

**Codec:** dag-cbor  
**CID hash:** sha2-256

| Field | Type | Description |
|-------|------|-------------|
| `epoch` | `Int` | Beacon epoch number |
| `slot` | `Int` | First slot of the epoch (may be 0 in push mode) |
| `network` | `String` | Network name |
| `approximateSizeBytes` | `Int` | Total blob data size for this epoch |
| `blobCount` | `Int` | Number of blobs in this epoch |
| `blobs` | `BlobIndex` | versionedHash-keyed index, inline map or HAMT link (see below) |

---

## BlobIndex

A union type selected by the `"type"` key:

### BlobMap (inline, for epochs with < `hamt_threshold` blobs)

```json
{
  "type": "map",
  "entries": {
    "0x01aabb…": [ { "/": "bafyreie..." } ],
    "0x01ccdd…": [ { "/": "bafyreif..." }, { "/": "bafyreig0..." } ]
  }
}
```

Keys are `versionedHash` (0x-prefixed hex) strings, sorted lexicographically.
The value is the list of `&BlobMetadata` links for that versionedHash, ordered
by `(slot, index)` — a list (not a single link) because the same blob data
(e.g. the zero blob) can appear more than once in an epoch, and every
occurrence must remain addressable. The list is length 1 in the common case.

### HAMTRoot (for epochs with ≥ `hamt_threshold` blobs)

```json
{
  "type": "hamt",
  "shardSize": 256,
  "shards": [
    { "/": "bafyreig..." },
    { "/": "bafyreih..." }
  ]
}
```

Each shard is a dag-cbor map of up to 256 entries: `{ "<versionedHash>" → [&BlobMetadata, …] }`.

---

## BlobMetadata

**Codec:** dag-cbor  
**CID hash:** sha2-256

| Field | Type | Description |
|-------|------|-------------|
| `commitment` | `String` | KZG commitment, `0x`-prefixed hex (48 bytes) |
| `versionedHash` | `String` | EIP-4844 versioned hash, `0x`-prefixed hex |
| `txHash` | `String` | Execution-layer transaction hash (empty if EL not available) |
| `blockNumber` | `Int` | EL block number (0 if EL not available) |
| `blockHash` | `String` | EL block hash (empty if EL not available) |
| `slot` | `Int` | Beacon slot |
| `epoch` | `Int` | Beacon epoch |
| `index` | `Int` | Blob index within the transaction (0-based) |
| `size` | `Int` | Raw blob size in bytes (always 131072 for EIP-4844) |
| `data` | `&Blob` | CID link to the raw blob node |

---

## Blob

**Codec:** raw  
**CID hash:** sha2-256

The raw 131 072-byte EIP-4844 blob field element data. Stored with codec `raw`
so the CID is the sha2-256 of the raw bytes. The same blob data always produces
the same CID, enabling deduplication across epochs.

```
CID = sha2-256( raw blob bytes )   codec = 0x55 (raw)
```

---

## Metadata size calculus

This section estimates how much storage the IPLD metadata layer requires,
separate from the raw blob data itself (131 072 bytes per blob).

### Assumptions

- CID links in dag-cbor are encoded as 36 bytes each (CIDv1, sha2-256, raw or dag-cbor prefix).
- CBOR map key overhead: ~2–4 bytes per key (string length + value).
- Hex commitment string: 98 bytes (`"0x"` + 96 hex chars).
- Hex versioned hash / tx hash / block hash: 66 bytes each.
- All numbers fit in CBOR uint (1–9 bytes; assume 4 bytes average for slots/epochs).
- HAMT is not used below the threshold (default 5000 blobs/epoch); estimates use the
  inline map path.

### Per-blob metadata

**`Blob` node (raw codec):**

| Item | Size |
|------|------|
| Raw blob data | 131 072 B |
| CID (content hash) | 36 B |

**`BlobMetadata` node (dag-cbor):**

| Field | Size |
|-------|------|
| `commitment` (string) | ~100 B |
| `versioned_hash` (string) | ~68 B |
| `tx_hash` (string) | ~68 B |
| `block_hash` (string) | ~68 B |
| `block_number` (uint) | ~10 B |
| `slot` (uint) | ~10 B |
| `epoch` (uint) | ~8 B |
| `index` (uint) | ~5 B |
| `size` (uint) | ~8 B |
| `data` (CID link to Blob) | ~38 B |
| CBOR overhead (map header + key names) | ~120 B |
| **Total BlobMetadata node** | **~503 B ≈ 0.5 KB** |
| BlobMetadata CID | 36 B |

**Total metadata overhead per blob (excluding raw data):**

```
BlobMetadata node body  ≈   503 B
BlobMetadata CID        ≈    36 B
Entry in EpochNode map  ≈   134 B  (98 B commitment key + 36 B CID link)
─────────────────────────────────
Per-blob metadata total ≈   673 B  ≈ 0.66 KB
```

Fraction of raw blob size: `673 / 131 072 ≈ 0.51 %`

### Per-epoch metadata

A typical mainnet epoch has ~100–500 blobs (post-Dencun peak).
Using 200 blobs as a representative value:

| Component | Size |
|-----------|------|
| 200 × BlobMetadata nodes | ~100 KB |
| 200 × entries in EpochNode map | ~26 KB |
| EpochNode header fields | ~200 B |
| EpochNode CID | 36 B |
| **EpochNode total overhead** | **~126 KB** |

### At 17 million blobs (full historical mainnet estimate)

As of early 2026, mainnet has accumulated roughly 17 million EIP-4844 blobs
since the Cancun hard fork.

| Component | Calculation | Size |
|-----------|-------------|------|
| Raw blob data | 17 000 000 × 128 KB | **~2.08 TB** |
| BlobMetadata nodes | 17 000 000 × 503 B | **~8.1 GB** |
| EpochNode map entries | 17 000 000 × 134 B | **~2.2 GB** |
| EpochNode headers (~50k epochs) | 50 000 × 200 B | **~9.7 MB** |
| NetworkRoot node | ~50 000 entries × 46 B + header | **~2.3 MB** |
| **Total metadata** | | **~10.3 GB** |
| **Total (data + metadata)** | | **~2.09 TB** |

**Metadata overhead ratio: `10.3 GB / 2 090 GB ≈ 0.49 %`**

The IPLD metadata layer adds less than 0.5 % on top of the raw blob data.
At any realistic scale the metadata is negligible compared to the blob storage.

> **Note:** These are upper-bound estimates assuming all fields are non-empty
> (tx_hash, block_hash populated). If EL enrichment is disabled, the per-blob
> metadata is ~267 B, reducing the overhead further.

---

## Growth projections

### Network constants (Ethereum mainnet)

| Parameter | Value | Notes |
|-----------|-------|-------|
| Slot duration | 12 s | Fixed post-Merge |
| Slots per epoch | 32 | Fixed |
| Epoch duration | 384 s (~6.4 min) | 32 × 12 s |
| Epochs per day | 225 | 86 400 s / 384 s |
| Epochs per year | 82 125 | 225 × 365 |
| Max blobs per block | 6 | EIP-4844 (Cancun) |
| Target blobs per block | 3 | EIP-4844 base target |
| Max blobs per epoch | 192 | 32 slots × 6 blobs |
| Target blobs per epoch | 96 | 32 slots × 3 blobs |

### Current observed throughput (post-Cancun, as of Q1 2025)

**FIXME: Update with real numbers from latest network upgrade!!!**

Mainnet blob usage has been running between the target and the maximum since
Cancun (March 2024). A conservative mid-point of **~4 blobs/block** is used
for projections, giving:

| Metric | Value |
|--------|-------|
| Blobs per block (avg) | ~4 |
| Blobs per epoch (avg) | ~128 (32 × 4) |
| Blobs per day | ~28 800 (128 × 225) |
| Blobs per month | ~864 000 |
| Blobs per year | ~10 512 000 (~10.5 M) |

### Raw data growth rate

```
128 blobs/epoch × 128 KB/blob = 16 384 KB = 16 MB per epoch
16 MB/epoch × 225 epochs/day  = 3 600 MB/day  ≈ 3.5 GB/day
3.5 GB/day × 30              ≈ 105 GB/month
3.5 GB/day × 365             ≈ 1.28 TB/year
```

### Metadata growth rate

```
128 blobs/epoch × 673 B/blob  ≈  86 KB/epoch    (metadata only)
86 KB/epoch × 225 epochs/day  ≈  18.9 MB/day
18.9 MB/day × 365             ≈  6.7 GB/year
```

### Storage projection table

| Timeframe | Epochs | Blobs | Raw data | Metadata | Total |
|-----------|--------|-------|----------|----------|-------|
| 1 day | 225 | 28 800 | ~3.5 GB | ~18.9 MB | ~3.52 GB |
| 1 week | 1 575 | ~200 K | ~24.5 GB | ~132 MB | ~24.6 GB |
| 1 month | 6 750 | ~864 K | ~105 GB | ~567 MB | ~105.6 GB |
| 6 months | 40 500 | ~5.2 M | ~630 GB | ~3.4 GB | ~633 GB |
| 1 year | 81 000 | ~10.5 M | ~1.28 TB | ~6.7 GB | ~1.29 TB |
| 2 years | 162 000 | ~21 M | ~2.56 TB | ~13.4 GB | ~2.57 TB |
| 5 years | 405 000 | ~52.5 M | ~6.4 TB | ~33.5 GB | ~6.43 TB |

**Metadata remains consistently < 0.52 % of total storage across all timeframes.**

### NetworkRoot growth

The `NetworkRoot` node holds one CID link per epoch (~46 bytes each).

```
After 1 year  : 81 000 epochs × 46 B ≈  3.6 MB  (single node, in-memory trivial)
After 5 years : 405 000 epochs × 46 B ≈ 18 MB   (still a single dag-cbor node)
```

A single dag-cbor map with 405 000 entries is large but manageable. At
>500 000 entries it may be worth sharding the `NetworkRoot` similarly to how
the blob index uses HAMT — however that is not expected to be necessary within
a 5-year horizon at current rates.

### EIP-7594 (PeerDAS) impact

**FIXME: Update with actual numbers**

PeerDAS is expected to increase the blob count per slot significantly
(proposals range from 32 to 128 blobs/slot). At **32 blobs/slot** (10× current
target), projections scale linearly:

| Metric | Current (~4/block) | PeerDAS (~32/block) |
|--------|--------------------|---------------------|
| Blobs/day | ~28 800 | ~230 400 |
| Raw data/day | ~3.5 GB | ~28 GB |
| Metadata/day | ~18.9 MB | ~151 MB |
| Raw data/year | ~1.28 TB | ~10.2 TB |
| Metadata/year | ~6.7 GB | ~53 GB |

Even under PeerDAS, metadata overhead stays below 0.52 %. The `NetworkRoot`
node would reach ~18 MB after one year and would benefit from sharding after
~2 years.

### Efficiency summary

| Metric | Value |
|--------|-------|
| Metadata per blob | ~673 B |
| Metadata as % of blob size (128 KB) | **0.51 %** |
| Metadata as % of total storage | **< 0.52 %** (all scales) |
| Current raw data ingest rate | **~3.5 GB/day** |
| Current metadata ingest rate | **~18.9 MB/day** |
| Storage needed for full archive (1 year) | **~1.29 TB** |
| Storage needed for full archive (5 years) | **~6.43 TB** |
| Storage needed under PeerDAS (1 year) | **~10.2 TB** |

---

## CID generation summary

| Node type | Codec | Hash |
|-----------|-------|------|
| `NetworkRoot` | dag-cbor (`0x71`) | sha2-256 |
| `EpochNode` | dag-cbor (`0x71`) | sha2-256 |
| `BlobMetadata` | dag-cbor (`0x71`) | sha2-256 |
| `HAMTShard` | dag-cbor (`0x71`) | sha2-256 |
| `Blob` (raw data) | raw (`0x55`) | sha2-256 |

All CIDs are CIDv1.

---

## Determinism guarantee

Given identical inputs, all CIDs are reproducible:

- Map keys are always sorted lexicographically before encoding.
- Epoch numbers in `NetworkRoot.epochs` are sorted numerically.
- Blob index keys (versionedHash) are sorted lexicographically; the
  occurrence list for each key is ordered by `(slot, index)`.
- The `raw` codec for blob data means the blob CID equals the content hash.

---

## IPLD path examples

```bash
# Get the NetworkRoot for a running IPFS node
ipfs dag get <NetworkRootCID>

# Navigate to a specific epoch
ipfs dag get <NetworkRootCID>/epochs/269568

# Get a blob's metadata by versionedHash ([0] = first occurrence)
ipfs dag get <NetworkRootCID>/epochs/269568/blobs/entries/0x01aabb…/0

# Get the raw blob data CID
ipfs dag get <NetworkRootCID>/epochs/269568/blobs/entries/0x01aabb…/0/data

# Fetch raw blob bytes
ipfs block get <BlobCID>
```

---

## PostgreSQL schema

The `ipld_epochs` and `ipld_blobs` tables mirror the IPLD DAG in a relational
form for fast lookups.

```sql
-- One row per processed epoch
CREATE TABLE IF NOT EXISTS ipld_epochs (
    network    TEXT    NOT NULL,
    epoch      BIGINT  NOT NULL,
    cid        TEXT    NOT NULL,
    blob_count INT     NOT NULL DEFAULT 0,
    size_bytes BIGINT  NOT NULL DEFAULT 0,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (network, epoch)
);

-- One row per blob
CREATE TABLE IF NOT EXISTS ipld_blobs (
    network     TEXT    NOT NULL,
    epoch       BIGINT  NOT NULL,
    blob_index  INT     NOT NULL,
    commitment  TEXT    NOT NULL,
    data_cid    TEXT    NOT NULL,   -- CID of the raw Blob node
    meta_cid    TEXT    NOT NULL,   -- CID of the BlobMetadata node
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (network, epoch, blob_index)
);
```
