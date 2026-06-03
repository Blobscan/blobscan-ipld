**WORK IN PROGRESS**

# blobscan-ipld

Automatic IPLD DAG generator for Ethereum EIP-4844 blob data, designed for
[Blobscan](https://blobscan.com)-style indexers. Fetches finalized epochs from
a Beacon Node, builds a deterministic content-addressed DAG, exports
self-contained CAR v2 archives, and publishes a mutable network root via IPNS.

## Quick start

```bash
# 1. Build the binary
go build ./cmd/blobscan-ipld

# 2. Set required environment variables
cp .env.example .env   # edit NETWORK_NAME, {MAINNET,SEPOLIA,HOODI}_BEACON_RPC, DATA_DIR, IPFS_API_ADDR

# 3. Run (beacon-pull mode)
export $(grep -v '^#' .env | xargs)
./blobscan-ipld run

# Or start the HTTP push API instead
./blobscan-ipld serve
```

## How it works

### Beacon-pull mode (`run`)

```
Beacon Node (REST API)
        │
        ▼  GetFinalityCheckpoints + GetBlobSidecars (every slot in epoch)
beacon.Client.FetchEpochInput()
        │
        ▼  parallel worker pool (generator.workers goroutines)
builder.ProcessBlob()  ×N
        ├─ StoreRawBlob()       → CIDv1 codec=raw    sha2-256  (128 KiB blob)
        └─ StoreBlobMetadata()  → CIDv1 codec=dag-cbor sha2-256
        │
        ▼
builder.BuildEpochNode()
        ├─ blob count < hamt_threshold  → inline BlobMap   (dag-cbor map)
        └─ blob count ≥ hamt_threshold  → sharded HAMTIndex (dag-cbor shards)
        │
        ├─► ipfs.PutBlockstore()    → /api/v0/block/put  (all blocks)
        ├─► ipfs.Pin()              → /api/v0/pin/add    (if pin_on_add=true)
        ├─► db.SaveBlobs/SaveEpoch  → PostgreSQL          (if postgres_dsn set)
        └─► builder.BuildNetworkRoot() → NetworkRoot dag-cbor (rebuilt per epoch)
```

When PostgreSQL is configured, the generator runs a **live** goroutine (polls
every `poll_interval` for newly finalized epochs) and a **backfill** goroutine
(crawls history from `start_epoch` concurrently) so a fresh deployment stays
current while catching up. On restart both cursors resume from `ipld_state`.
Without PostgreSQL the original sequential loop is used instead.

### Push API mode (`serve`)

An external source `POST /blob` blobs one at a time. Each blob is processed and
uploaded to IPFS immediately. Call with `finalize: true` on the last blob of an
epoch to build the `EpochNode` and `NetworkRoot` in the same request.

## DAG node types

| Node | Codec | Mutable | Description |
|------|-------|---------|-------------|
| `NetworkRoot` | dag-cbor | Yes — rebuilt per epoch | Top-level index keyed by epoch number |
| `EpochNode` | dag-cbor | No | All blobs for one finalized epoch |
| `BlobMetadata` | dag-cbor | No | KZG commitment, tx hash, link to raw data |
| Raw blob | raw | No | 128 KiB EIP-4844 blob field element |

Full field-level documentation: [`docs/data-model.md`](docs/data-model.md)

## Project layout

```
blobscan-ipld/
├── cmd/blobscan-ipld/main.go   CLI entry point; subcommands: run, serve, epoch, finalize-epoch, export-car, export-car-range
├── config/config.go            Env-var loader, validation, defaults
├── types/types.go              Pure domain types — no IPLD imports
├── api/server.go               HTTP push API server (POST /blob, GET /healthz)
├── db/db.go                    PostgreSQL client: schema migration, SaveBlobs, SaveEpoch, state backend
├── store/
│   ├── blockstore.go           Thread-safe in-memory blockstore (MemBlockstore)
│   └── linksystem.go           ipld.LinkSystem backed by MemBlockstore
├── builder/
│   ├── blob.go                 StoreRawBlob, StoreBlobMetadata, ProcessBlob
│   ├── epoch.go                BuildEpochNode (BlobMap or HAMT shards)
│   └── network.go              BuildNetworkRoot
├── car/exporter.go             ExportRangeCAR (atomic write)
├── ipfs/client.go              Kubo HTTP RPC: PutBlockstore, GetBlocks, Pin
├── beacon/client.go            Beacon REST API: finality checkpoints, blob sidecars
├── state/manager.go            state.Backend interface; file-backed Manager
├── generator/generator.go      Orchestrator: beacon-pull loop, push API callbacks, epoch finalizer
├── schema/schema.ipldsch       Canonical IPLD schema (reference only)
├── .env.example                Annotated example environment variables
└── docs/                       Extended documentation
```

## CLI subcommands

Global flags (before subcommand): `-log-level <level>` (default: `info`)

| Subcommand | Description |
|------------|-------------|
| `run` | Beacon-pull loop: poll for finalized epochs continuously |
| `serve [-pull]` | Start the HTTP push API; `-pull` also runs the beacon-pull loop |
| `epoch -n N` | Process a single epoch one-shot (beacon-pull, backfill / debug) |
| `finalize-epoch -n N` | Build the EpochNode for epoch N using blobs already pushed via API (requires DB) |
| `export-car -n N [-out path]` | Export a CAR v2 file for epoch N from IPFS (requires DB) |
| `export-car-range -from N -to M [-out path]` | Export a single CAR file covering epochs N–M (requires DB) |

## Configuration reference

See [`docs/configuration.md`](docs/configuration.md) for every variable and its
default. Minimum required environment variables:

```bash
NETWORK_NAME=mainnet
MAINNET_BEACON_RPC=http://localhost:5052   # omit when using push API only
IPFS_API_ADDR=/ip4/127.0.0.1/tcp/5001
DATA_DIR=/var/lib/blobscan-ipld/mainnet
POSTGRES_DSN=postgres://user:pass@localhost:5432/blobscan  # optional
```

## Pinning

| Goal | Command |
|------|---------|
| Follow the entire network (live, via IPNS) | `ipfs pin add -r /ipns/<key>` |
| Pin a specific epoch | `ipfs pin add -r /ipfs/<EpochNodeCID>` |
| Pin the current network root | `ipfs pin add -r /ipfs/<NetworkRootCID>` |
| Import an epoch from a CAR file | `ipfs dag import <epoch>.car` |

## Determinism

The same epoch number + same blob data always produces the **same CIDs** at
every level of the DAG:

- All dag-cbor map keys are sorted lexicographically before encoding.
- Only `sha2-256` is used as the multihash function.
- `approximateSizeBytes` is a deterministic sum, not a timestamp or random value.
- Blob commitments are used as map keys (unique per blob per epoch).

## Running tests

```bash
go test ./...
```

## Documentation

| Page | Contents |
|------|----------|
| [`docs/api.md`](docs/api.md) | HTTP push API endpoints, operating modes, push workflows |
| [`docs/architecture.md`](docs/architecture.md) | Module responsibilities, data-flow, concurrency model |
| [`docs/data-model.md`](docs/data-model.md) | IPLD schema, all node fields, JSON examples |
| [`docs/configuration.md`](docs/configuration.md) | Every config field, type, default, and validation rule |
| [`docs/api-reference.md`](docs/api-reference.md) | All exported Go functions and types |
| [`docs/operations.md`](docs/operations.md) | Setup, backfill, CAR import, pinning strategies |
| [`docs/state-and-resumption.md`](docs/state-and-resumption.md) | State backends (DB and file), restart behaviour, rollback |

## Dependencies

| Package | Purpose |
|---------|--------|
| `github.com/ipld/go-ipld-prime` | IPLD node model, `LinkSystem`, dag-cbor codec, `qp` builder |
| `github.com/ipld/go-car/v2` | CAR v2 read/write with built-in index |
| `github.com/ipfs/go-cid` | CID construction and parsing |
| `github.com/ipfs/go-block-format` | `Block` interface |
| `github.com/jackc/pgx/v5` | PostgreSQL driver and connection pool |
| `github.com/multiformats/go-multicodec` | Multicodec constants (raw, dag-cbor) |
| `github.com/multiformats/go-multihash` | Multihash construction for CID building |
