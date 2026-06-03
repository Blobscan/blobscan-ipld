# Operating Modes & HTTP Push API

`blobscan-ipld` supports two independent data-ingestion modes that can run
separately or together. Both produce identical IPLD DAGs and store the same
CIDs in PostgreSQL — the difference is only in **how blob data arrives**.

---

## Operating modes

### Mode 1 — Beacon-pull (autonomous)

The generator connects directly to an Ethereum **Beacon Node** (consensus
layer client, e.g. Lighthouse, Prysm, Teku) and continuously fetches blob data
from it without any external intervention.

```
Beacon Node REST API
        │
        │  GET /eth/v1/beacon/states/head/finality_checkpoints
        │  GET /eth/v1/beacon/blob_sidecars/{slot}
        ▼
  blobscan-ipld run
        │
        ├─ build IPLD DAG (blob + metadata + epoch nodes)
        ├─ upload blocks to IPFS
        ├─ rebuild NetworkRoot
        └─ save CIDs to PostgreSQL
```

**How it works:**

1. On startup, reads `last_processed_epoch` from the state file and resumes
   from there (or from `generator.start_epoch` if no state exists).
2. Polls `GET /eth/v1/beacon/states/head/finality_checkpoints` every
   `generator.poll_interval` (default 12 s) to detect newly finalized epochs.
3. For each new epoch, iterates all 32 slots and calls
   `GET /eth/v1/beacon/blob_sidecars/{slot}` for each. Missed slots (HTTP 404
   — proposer did not include blobs) are silently skipped.
4. Runs `generator.workers` (default 8) parallel workers to build the IPLD
   blob nodes.
5. Builds the `EpochNode`, uploads all blocks to IPFS, pins the root CID, saves
   to DB, and rebuilds the `NetworkRoot`.
6. Advances `last_processed_epoch` and repeats.

**Requirements:**

- `network.beacon_rpc` must be set to a reachable Beacon Node REST API base URL.
- The beacon node must retain blob sidecars for the epochs being processed
  (standard retention is ~18 days / ~4096 epochs on mainnet).
- `storage.postgres_dsn` is **optional** but recommended. Without it, blobs and
  epoch CIDs are not persisted; `export-car`, `finalize-epoch`, `NetworkRoot`
  rebuild will not work.

**Start:**

```bash
blobscan-ipld run
```

**Process a single epoch one-shot (useful for backfill or testing):**

```bash
blobscan-ipld -n 269568 epoch
```

---

### Mode 2 — HTTP push API (external source)

An HTTP server accepts blobs pushed from any external system — another indexer,
a custom scraper, a bridge from a different RPC — without requiring a local
beacon node. This is the appropriate mode when:

- You already have blob data from another source.
- The beacon node's blob retention window has passed (blobs older than ~18 days
  are pruned from standard nodes).
- You want to selectively index specific epochs without running a full pull loop.

```
External system (indexer / RPC / scraper)
        │
        │  POST /blob  (JSON: commitment, data, slot, epoch, ...)
        ▼
  blobscan-ipld serve
        │
        ├─ validate + decode blob
        ├─ build IPLD blob node + metadata node
        ├─ upload blocks to IPFS
        └─ save CIDs to PostgreSQL
        │
        │  (after all blobs for an epoch are pushed)
        ▼
  EpochNode built + NetworkRoot rebuilt
```

**Requirements:**

- `network.beacon_rpc` is **not required** for this mode.
- `storage.postgres_dsn` is **optional**. Without it blobs are uploaded to
  IPFS but not persisted to the DB — see the feature table below.
- IPFS node must be running.

**Start:**

```bash
blobscan-ipld serve
```

Default listen address: `:8080`. Override with `generator.api_listen`.

**Epoch finalization** is not automatic — the server stores individual blobs
as they arrive but only builds the `EpochNode` when told to (see `finalize`
in `POST /blob`, or run the CLI explicitly):

```bash
blobscan-ipld -n 269568 finalize-epoch
```

> **Note:** `finalize-epoch` requires DB persistence because it reloads blob
> CIDs from the DB to reconstruct the `EpochNode`. With DB disabled, use
> `finalize: true` on the last blob push instead — the epoch is finalized
> in the same request.

---

### Mode 3 — Combined (pull + push simultaneously)

Both modes can run in the same process. The beacon-pull loop keeps up with the
live chain while the push API accepts historical or supplementary data
concurrently.

```bash
blobscan-ipld -pull serve
```

The pull loop runs in a background goroutine; the push API handles incoming
requests on the main HTTP server.

---

## PostgreSQL — what works without it

PostgreSQL is **optional** in all modes. When `storage.postgres_dsn` is not
set, the following features are unavailable:

| Feature | Requires DB | Notes |
|---------|-------------|-------|
| Upload blobs to IPFS | no | always works |
| Return `data_cid` / `meta_cid` in response | no | always works |
| `finalize: true` in `POST /blob` | **yes** | needs stored blob CIDs to rebuild EpochNode |
| `finalize-epoch` CLI | **yes** | reads blobs from DB |
| `export-car` / `export-car-range` CLI | **yes** | reads epoch + blob CIDs from DB |
| `NetworkRoot` rebuild after each epoch | **yes** | reads all epoch CIDs from DB |

When running without DB and you need epoch finalization, use `finalize: true`
on the last `POST /blob` — the epoch CIDs returned in the response can be
recorded externally.

---

## Choosing a mode

| Situation | Recommended mode |
|-----------|------------------|
| Running a full archive node with local beacon node | **run** (beacon-pull) |
| Backfilling from another indexer or data dump | **serve** (push API) |
| Blobs older than beacon retention (~18 days) | **serve** (push API) |
| Keep live chain in sync + accept pushed backfill | **serve -pull** (combined) |
| One-shot processing of a specific epoch | **epoch -n N** |
| Building an epoch from already-pushed blobs | **finalize-epoch -n N** |
| IPFS-only, no DB | any mode — omit `postgres_dsn` |

---

## Endpoints

### `POST /blob`

Store a single blob in IPFS and persist its CIDs to PostgreSQL.

The raw data and its metadata are stored as IPLD nodes and uploaded to the IPFS
node immediately. The `EpochNode` and `NetworkRoot` are optionally also built
in the same request — see `finalize` below.

#### Request

**Content-Type:** `application/json`

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `commitment` | `string` | ✓ | KZG commitment, `0x`-prefixed hex (48 bytes) |
| `versioned_hash` | `string` | ✓ | EIP-4844 versioned hash, `0x`-prefixed hex |
| `data` | `string` | ✓ | Raw blob data, `0x`-prefixed hex (131072 bytes = 262144 hex chars) |
| `slot` | `uint64` | ✓ | Beacon slot number |
| `epoch` | `uint64` | ✓ | Beacon epoch number |
| `index` | `int` | ✓ | Blob index within the transaction (0-based) |
| `tx_hash` | `string` | — | Execution-layer transaction hash (optional) |
| `block_number` | `uint64` | — | EL block number (optional) |
| `block_hash` | `string` | — | EL block hash (optional) |
| `finalize` | `bool` | — | If `true`, build the `EpochNode` and `NetworkRoot` immediately after storing this blob. Use on the **last** blob of an epoch when the caller knows the epoch is complete. |

**Example — plain push (no auto-finalize):**

```bash
curl -X POST http://localhost:8080/blob \
  -H "Content-Type: application/json" \
  -d '{
    "commitment":     "0x8d7a3579...",
    "versioned_hash": "0x01b3a1c2...",
    "data":           "0x0000....",
    "slot":           8626176,
    "epoch":          269568,
    "index":          0,
    "tx_hash":        "0xabc123...",
    "block_number":   19500000,
    "block_hash":     "0xdef456..."
  }'
```

**Example — explicit finalize on last blob:**

```bash
curl -X POST http://localhost:8080/blob \
  -H "Content-Type: application/json" \
  -d '{
    "commitment":     "0xb1c2...",
    "versioned_hash": "0x01d3...",
    "data":           "0x0000....",
    "slot":           8626208,
    "epoch":          269568,
    "index":          5,
    "finalize":       true
  }'
```

#### Response `201 Created`

```json
{
  "data_cid":   "bafkreib...",
  "meta_cid":   "bafyreic...",
  "commitment": "0x8d7a3579...",
  "epoch":      269568,
  "finalized":  true,
  "epoch_cid":  "bafyreig..."
}
```

| Field | Description |
|-------|-------------|
| `data_cid` | CID of the raw `Blob` node (codec=raw) |
| `meta_cid` | CID of the `BlobMetadata` node (codec=dag-cbor) |
| `commitment` | Echo of the input commitment |
| `epoch` | Echo of the input epoch |
| `finalized` | `true` if the `EpochNode` was built during this request |
| `epoch_cid` | CID of the `EpochNode`; only present when `finalized` is `true` |

#### Error responses

| Status | Meaning |
|--------|---------|
| `400 Bad Request` | Missing or invalid fields (e.g. wrong data length) |
| `500 Internal Server Error` | IPFS upload, DB write, or finalization failed |

```json
{ "error": "data: expected 131072 bytes, got 64" }
```

---

### `GET /healthz`

Returns `200 OK` with `{"status":"ok"}` when the server is running.

```bash
curl http://localhost:8080/healthz
# {"status":"ok"}
```

---

## Push workflows

### 1. Manual finalize (explicit CLI call)

Use when your pusher does not know the epoch size in advance.

```bash
EPOCH=269568
CONFIG=mainnet.yaml

for blob in blobs_${EPOCH}/*.json; do
    curl -sf -X POST http://localhost:8080/blob \
         -H "Content-Type: application/json" \
         -d @"$blob"
done

blobscan-ipld -n $EPOCH finalize-epoch
```

### 2. Explicit finalize on last blob

Use when the pusher knows which blob is last (e.g. it has fetched the full
slot list from a beacon node or another RPC).

```bash
LAST_INDEX=5
for i in $(seq 0 $LAST_INDEX); do
    FINALIZE="false"
    [ "$i" -eq "$LAST_INDEX" ] && FINALIZE="true"
    curl -sf -X POST http://localhost:8080/blob \
         -H "Content-Type: application/json" \
         -d "$(jq -n --argjson f $FINALIZE --argjson i $i \
               '{commitment:"0x...",versioned_hash:"0x...",data:"0x...",
                 slot:8626176,epoch:269568,index:$i,finalize:$f}')"
done
```

---

## CAR export

CAR export is a separate manual operation and is **not** triggered automatically
by the push API or the beacon-pull loop. Run it on demand:

```bash
# Export epoch 269568 to the default path (<car_dir>/<network>/269568.car)
blobscan-ipld -n 269568 export-car

# Export to a custom path
blobscan-ipld -n 269568 -out /tmp/269568.car export-car
```

The command:
1. Looks up the epoch's root CID from PostgreSQL (`ipld_epochs`).
2. Looks up all blob CIDs for the epoch from PostgreSQL (`ipld_blobs`).
3. Fetches all blocks from the local IPFS node.
4. Writes a self-contained CAR v2 file with the `EpochNode` CID as the root.

The resulting file can be verified and imported:

```bash
ipfs-car inspect 269568.car
ipfs dag import 269568.car
```

---

## Configuration

Relevant environment variable:

```bash
GENERATOR_API_LISTEN=:8080   # address for the push API; unset = disabled
```

See `docs/configuration.md` for the full reference.
