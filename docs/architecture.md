# Architecture

## Overview

`blobscan-ipld` is structured as a pipeline of independent, testable modules.
Each module has a single responsibility and communicates through plain Go types
defined in `types/types.go`. No module imports another module except through
the `types` package, keeping the dependency graph acyclic and easy to test.

```
┌─────────────────────────────────────────────────────────────────┐
│                        generator package                        │
│  (orchestrator: poll loop, worker pool, epoch/root builder)     │
└───┬──────────┬──────────┬──────────┬──────────┬────────────────┘
    │          │          │          │          │
    ▼          ▼          ▼          ▼          ▼
 beacon     builder      car        ipfs     state/db
 client     package    exporter    client    backend
```

## Packages

### `config`
Loads and validates a YAML configuration file. Applies defaults for any
optional fields that are not set. Returns a `*config.Config` struct used by
every other package. Validation fails fast at startup with a clear error
message rather than panicking at runtime.

### `types`
Plain Go structs with no external dependencies beyond `go-cid`. Defines:
- **Input types** (`BlobInput`, `EpochInput`) — raw data from the beacon node or the push API.
- **Result types** (`BlobResult`, `EpochResult`, `NetworkRootResult`) — CIDs produced by the builder.
- **State types** (`State`) — used by the file-backed state manager.

Keeping these in a separate package means the `builder` package can be tested
without any I/O dependencies.

### `store`
Two files:

- **`blockstore.go`** — `MemBlockstore`: a thread-safe `map[cid.Cid]blocks.Block`
  that implements the `blockstore.Blockstore` interface. Used as a staging area
  for blocks before they are exported to a CAR file or uploaded to IPFS.
  The `All()` method returns a snapshot of all blocks for bulk operations.

- **`linksystem.go`** — `NewLinkSystem(bs *MemBlockstore) ipld.LinkSystem`:
  wires a `MemBlockstore` into an `ipld.LinkSystem`. The `EncoderChooser`
  selects `dag-cbor` for structured nodes and a raw byte encoder for blob data.
  The `DecoderChooser` mirrors this. All reads and writes go through the
  `MemBlockstore`.

### `builder`
Pure functions that construct IPLD nodes and store them via a `LinkSystem`.
No network I/O. All functions are deterministic: given the same inputs they
always produce the same CIDs.

| File | Functions | Description |
|------|-----------|-------------|
| `blob.go` | `StoreRawBlob`, `StoreBlobMetadata`, `ProcessBlob` | Stores the 128 KiB raw blob (codec=raw) and a dag-cbor metadata node linking to it |
| `epoch.go` | `BuildEpochNode` | Builds an EpochNode with either a flat BlobMap or a sharded HAMT index |
| `network.go` | `BuildNetworkRoot` | Builds the mutable NetworkRoot node from all known epoch CIDs (loaded from DB) |

**CID link prototypes** (defined in `blob.go`, shared across the package):

```go
linkProtoRaw     // CIDv1, codec=0x55 (raw),      mh=sha2-256
linkProtoDagCBOR // CIDv1, codec=0x71 (dag-cbor), mh=sha2-256
```

### `car`
Wraps `go-car/v2` to export a `MemBlockstore` as a self-contained CAR v2 file.

- **`ExportRangeCAR`** — writes atomically via a `.tmp` file + `os.Rename`.
  On failure the temporary file is removed and the original is untouched.
- **`RangeCARPath`** — returns the canonical path
  `<car_dir>/<network>/<firstEpoch>-<lastEpoch>.car`.
- **`VerifyCARRoot`** — opens an existing CAR file and checks that a given CID
  is listed as one of its roots. Used for post-export sanity checks.

### `ipfs`
A minimal HTTP RPC client for a Kubo-compatible IPFS node. Accepts both
multiaddr strings (`/ip4/127.0.0.1/tcp/5001`) and plain HTTP URLs.

| Method | Endpoint | Description |
|--------|----------|-------------|
| `PutBlock` | `POST /api/v0/block/put` | Upload a single block with correct codec/mhtype params |
| `PutBlockstore` | — | Iterates `MemBlockstore.All()` and calls `PutBlock` for each |
| `Pin` | `POST /api/v0/pin/add?recursive=true` | Recursively pin a CID |
| `DagStat` | `POST /api/v0/dag/stat` | Return cumulative DAG size |
| `PublishIPNS` | `POST /api/v0/name/publish` | Publish a CID under a named key |
| `ResolveIPNS` | `POST /api/v0/name/resolve` | Resolve an IPNS name to a path |
| `KeyList` | `POST /api/v0/key/list` | List all keys in the IPFS keystore |

### `beacon`
A minimal Ethereum Beacon Node REST API (v1) client.

| Method | Endpoint | Description |
|--------|----------|-------------|
| `GetFinalityCheckpoints` | `GET /eth/v1/beacon/states/{id}/finality_checkpoints` | Returns finalized and justified epoch numbers |
| `GetBlockHeader` | `GET /eth/v1/beacon/headers/{id}` | Returns slot, proposer, roots for a block |
| `GetBlobSidecars` | `GET /eth/v1/beacon/blob_sidecars/{id}` | Returns all blob sidecars for a slot |
| `FetchEpochInput` | — | Iterates all 32 slots of an epoch, collects sidecars, returns `EpochInput` |

Missed slots (HTTP 404) are silently skipped. An optional `ELClient` interface
can be passed to `FetchEpochInput` to enrich blobs with `txHash` and
`blockNumber` from the execution layer.

### `state`
Defines the `Backend` interface for progress tracking:

```go
type Backend interface {
    GetLastProcessedEpoch(ctx context.Context) (uint64, error)
    SetLastProcessedEpoch(ctx context.Context, epoch uint64) error
}
```

Two implementations:
- **`state.Manager`** — file-backed; reads/writes `<data_dir>/<network>-state.json` atomically via `.tmp` + `os.Rename`. Used when `postgres_dsn` is not set.
- **`db.DBStateBackend`** — DB-backed; `GetLastProcessedEpoch` runs `SELECT MAX(epoch) FROM ipld_epochs`; `SetLastProcessedEpoch` is a no-op (the epoch row written by `SaveEpoch` is already the progress marker).

### `generator`
The main orchestrator. Wires all packages together and runs the processing loop.

**`Generator` struct fields:**
- `cfg` — loaded config
- `beacon` — beacon client (nil when `beacon_rpc` is not set)
- `ipfs` — IPFS client
- `db` — PostgreSQL client (nil when `postgres_dsn` is not set)
- `state` — `state.Backend` (DB-backed or file-backed, selected at startup)
- `log` — structured logger (`log/slog`)

**Processing flow per epoch** (inside `processEpoch`):
1. `beacon.FetchEpochInput` — fetch all blob sidecars for the epoch
2. `processEpochBlobs` — parallel worker pool writes raw blobs + metadata into a `MemBlockstore`
3. `builder.BuildEpochNode` — builds the epoch DAG node
4. `ipfs.PutBlockstore` — uploads all blocks for the epoch
5. `db.SaveBlobs` / `db.SaveEpoch` — persist to PostgreSQL (if DB configured)
6. `rebuildNetworkRoot` — queries all epoch CIDs from DB, rebuilds and uploads `NetworkRoot`
7. `state.SetLastProcessedEpoch` — advance the progress marker

## Concurrency model

- The main poll loop runs in a single goroutine.
- Within each epoch, blobs are processed by a pool of `generator.workers`
  goroutines. All workers share a single `MemBlockstore` + `LinkSystem`; the
  blockstore's `sync.RWMutex` serialises concurrent writes.
- The push API (`serve` mode) runs on a separate HTTP server goroutine;
  each request is handled independently with no shared mutable state.
- State writes are serialised by the `sync.RWMutex` inside `state.Manager`
  (file backend) or are inherently atomic as DB row writes (DB backend).

## Error handling

- Transient errors during the poll loop (e.g. beacon node temporarily
  unreachable) are logged and the loop continues on the next tick.
- Errors during epoch or range processing are returned up the call stack and
  logged; the generator does **not** exit on transient errors.
- All file writes (state, CAR) are atomic: a `.tmp` file is written first,
  then renamed, so a crash mid-write leaves the previous file intact.
