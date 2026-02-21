# API Reference

All exported symbols across every package. Internal helpers are omitted.

---

## Package `config`

### `func Load(path string) (*Config, error)`
Reads a YAML file at `path`, validates required fields, applies defaults, and
returns a `*Config`. Returns an error if the file cannot be read, the YAML is
malformed, or any required field is missing.

### Types

```go
type Config struct {
    Network   NetworkConfig
    IPFS      IPFSConfig
    Storage   StorageConfig
    Generator GeneratorConfig
}

type NetworkConfig struct {
    Name      string // yaml:"name"
    BeaconRPC string // yaml:"beacon_rpc" — optional for push API mode
}

type IPFSConfig struct {
    APIAddr  string
    PinOnAdd bool
    Timeout  time.Duration
}

type StorageConfig struct {
    DataDir     string
    CARDir      string
    PostgresDSN string // optional; empty disables DB persistence
}

type GeneratorConfig struct {
    HAMTThreshold      int
    PollInterval       time.Duration
    StartEpoch         uint64
    Workers            int
    SkipExistingEpochs bool
    APIListen          string
}
```

---

## Package `types`

Pure domain types with no external dependencies beyond `go-cid`.

### Input types

```go
// BlobInput is raw data for a single blob fetched from the beacon node.
type BlobInput struct {
    Commitment    string  // KZG commitment, 0x-prefixed hex
    VersionedHash string  // EIP-4844 versioned hash, 0x-prefixed hex
    TxHash        string  // execution-layer tx hash
    BlockNumber   uint64
    BlockHash     string
    Slot          uint64
    Epoch         uint64
    Index         int     // blob index within the transaction
    Data          []byte  // raw 128 KiB blob field element
}

// EpochInput groups all blobs for one finalized epoch.
type EpochInput struct {
    Epoch uint64
    Slot  uint64  // first slot of the epoch
    Blobs []BlobInput
}
```

### Result types

```go
// BlobResult holds the CIDs produced after storing a single blob.
type BlobResult struct {
    Commitment string
    DataCID    cid.Cid // codec=raw
    MetaCID    cid.Cid // codec=dag-cbor
    SizeBytes  int64
}

// EpochResult holds the CID of a built EpochNode.
type EpochResult struct {
    Epoch                uint64
    CID                  cid.Cid
    ApproximateSizeBytes int64
}
```

### State types

```go
// State is used by the file-backed state manager.
type State struct {
    Network            string
    LastProcessedEpoch uint64
}
```

---

## Package `store`

### `func NewMemBlockstore() *MemBlockstore`
Creates an empty thread-safe in-memory blockstore.

### `func NewLinkSystem(bs *MemBlockstore) ipld.LinkSystem`
Returns an `ipld.LinkSystem` backed by `bs`. Encodes structured nodes with
dag-cbor and raw blob bytes with codec=raw. Both encoder and decoder choosers
are registered.

### `type MemBlockstore`

Implements the `blockstore.Blockstore` interface. All methods are safe for
concurrent use.

| Method | Description |
|--------|-------------|
| `Put(ctx, block) error` | Store a block |
| `PutMany(ctx, []block) error` | Store multiple blocks |
| `Get(ctx, cid) (block, error)` | Retrieve a block; returns `ErrNotFound` if absent |
| `Has(ctx, cid) (bool, error)` | Check existence |
| `GetSize(ctx, cid) (int, error)` | Return raw data size |
| `DeleteBlock(ctx, cid) error` | Remove a block |
| `AllKeysChan(ctx) (<-chan cid.Cid, error)` | Stream all CIDs |
| `HashOnRead(bool)` | No-op |
| `Len() int` | Number of stored blocks |
| `All() []blocks.Block` | Snapshot of all blocks (used for CAR export and IPFS upload) |

---

## Package `builder`

All functions are **deterministic**: the same inputs always produce the same
CIDs. All functions are **pure** with respect to I/O: they only read/write
through the provided `ipld.LinkSystem`.

### `func StoreRawBlob(ctx, lsys, data []byte) (cid.Cid, error)`
Stores raw blob bytes as a `basicnode.Bytes` node with `linkProtoRaw`
(codec=raw, sha2-256). Returns the CID.

### `func StoreBlobMetadata(ctx, lsys, inp BlobInput, dataCID cid.Cid) (cid.Cid, error)`
Builds a dag-cbor map node with 10 fields (commitment, versionedHash, txHash,
blockNumber, blockHash, slot, epoch, index, size, data) and stores it.
Returns the metadata CID.

### `func ProcessBlob(ctx, lsys, inp BlobInput) (BlobResult, error)`
Convenience wrapper: calls `StoreRawBlob` then `StoreBlobMetadata`. Returns a
`BlobResult` with both CIDs and the raw size.

### `func BuildEpochNode(ctx, lsys, inp EpochInput, results []BlobResult, network string, hamtThreshold int) (EpochResult, error)`
Builds and stores an `EpochNode` dag-cbor node. The `blobIndex` field is:
- A flat `BlobMap` if `len(results) < hamtThreshold`
- A sharded `HAMTIndex` if `len(results) >= hamtThreshold`

In both cases blob commitments are sorted lexicographically before encoding.

### `func BuildNetworkRoot(ctx, lsys, network string, epochs []EpochResult) (cid.Cid, error)`
Builds and stores a `NetworkRoot` dag-cbor node keyed by epoch number string.
Epochs are sorted numerically. Returns the CID of the new root.

---

## Package `car`

### `func ExportRangeCAR(ctx, bs *MemBlockstore, rootCID cid.Cid, outPath string) error`
Exports all blocks from `bs` into a CAR v2 file at `outPath` with `rootCID` as
the single root. Writes atomically: a `.tmp` file is written first, then
renamed. Parent directories are created automatically. On any error the
temporary file is removed.

### `func RangeCARPath(carDir, network string, firstEpoch, lastEpoch uint64) string`
Returns the canonical path for a range CAR file:
`<carDir>/<network>/<firstEpoch>-<lastEpoch>.car`

### `func VerifyCARRoot(carPath string, rootCID cid.Cid) error`
Opens an existing CAR v2 file and checks that `rootCID` is listed as one of
its roots. Returns an error if the file cannot be opened, is not a valid CAR,
or does not contain the expected root.

---

## Package `ipfs`

### `func NewClient(apiAddr string, timeout time.Duration) (*Client, error)`
Creates a Kubo HTTP RPC client. `apiAddr` may be a multiaddr
(`/ip4/127.0.0.1/tcp/5001`) or a plain HTTP URL (`http://127.0.0.1:5001`).

### `func (c *Client) PutBlock(ctx, blk blocks.Block) error`
Uploads a single block via `POST /api/v0/block/put`. Sets `cid-codec`, `mhtype`,
and `mhlen` query parameters from the block's CID prefix.

### `func (c *Client) PutBlockstore(ctx, bs *MemBlockstore) error`
Calls `PutBlock` for every block returned by `bs.All()`. Returns on the first
error.

### `func (c *Client) Pin(ctx, c cid.Cid) error`
Recursively pins a CID via `POST /api/v0/pin/add?recursive=true`.

### `func (c *Client) DagStat(ctx, root cid.Cid) (uint64, error)`
Returns the cumulative DAG size in bytes via `POST /api/v0/dag/stat`.

### `func (c *Client) PublishIPNS(ctx, keyName string, target cid.Cid, ttl, lifetime time.Duration) (*IPNSPublishResult, error)`
Publishes `/ipfs/<target>` under the named key via `POST /api/v0/name/publish`.
Returns the IPNS name and the published value path.

### `func (c *Client) ResolveIPNS(ctx, ipnsName string) (string, error)`
Resolves an IPNS name to its current `/ipfs/<CID>` path via
`POST /api/v0/name/resolve`.

### `func (c *Client) KeyList(ctx) ([]string, error)`
Returns the names of all keys in the IPFS keystore via `POST /api/v0/key/list`.

### `type IPNSPublishResult`
```go
type IPNSPublishResult struct {
    Name  string  // IPNS key identifier, e.g. "k51q..."
    Value string  // published path, e.g. "/ipfs/bafyrei..."
}
```

---

## Package `beacon`

### `func NewClient(baseURL string, timeout time.Duration) *Client`
Creates a Beacon Node REST API client. `baseURL` should be the API root,
e.g. `"http://localhost:5052"`.

### `func (c *Client) GetFinalityCheckpoints(ctx, stateID string) (*FinalityCheckpoints, error)`
Calls `GET /eth/v1/beacon/states/{stateID}/finality_checkpoints`.
Use `stateID = "head"` for the current head state.

### `func (c *Client) GetBlockHeader(ctx, blockID string) (*BlockHeader, error)`
Calls `GET /eth/v1/beacon/headers/{blockID}`. `blockID` may be `"head"`, a
slot number, or a block root.

### `func (c *Client) GetBlobSidecars(ctx, blockID string) ([]BlobSidecar, error)`
Calls `GET /eth/v1/beacon/blob_sidecars/{blockID}`. Returns all blob sidecars
for the given slot or block root.

### `func (c *Client) FetchEpochInput(ctx, epoch uint64, el ELClient) (EpochInput, error)`
Iterates all 32 slots of `epoch`, calls `GetBlobSidecars` for each, and
assembles an `EpochInput`. Missed slots (HTTP 404) are silently skipped. If
`el` is non-nil, enriches each blob with `TxHash`, `BlockNumber`, and
`BlockHash` from the execution layer.

### `func EpochToFirstSlot(epoch uint64) uint64`
Returns `epoch * 32`.

### `func SlotToEpoch(slot uint64) uint64`
Returns `slot / 32`.

### `type ELClient` (interface)
```go
type ELClient interface {
    GetBlobTxData(ctx context.Context, blockRoot, commitment string) (*ELBlobData, error)
}
```
Optional interface for enriching blobs with execution-layer data. Pass `nil`
to `FetchEpochInput` to skip EL enrichment.

---

## Package `state`

### `type Backend` (interface)
```go
type Backend interface {
    GetLastProcessedEpoch(ctx context.Context) (uint64, error)
    SetLastProcessedEpoch(ctx context.Context, epoch uint64) error
}
```
Implemented by `*Manager` (file-backed) and `*db.DBStateBackend` (DB-backed).
Selected automatically by `generator.New` based on whether `postgres_dsn` is set.

### `func NewManager(dataDir, network string) (*Manager, error)`
Creates a file-backed `Manager` at `<dataDir>/<network>-state.json`. If the
file does not exist, an empty state is initialised.

### `func (m *Manager) Get() types.State`
Returns a copy of the current state (read-locked).

### `func (m *Manager) GetLastProcessedEpoch(ctx) (uint64, error)`
Implements `Backend`. Returns `state.LastProcessedEpoch`.

### `func (m *Manager) SetLastProcessedEpoch(ctx, epoch uint64) error`
Implements `Backend`. Updates `last_processed_epoch` and persists atomically.

---

## Package `api`

### `func New(addr string, processor BlobProcessor, finalizer EpochFinalizer, log *slog.Logger) *Server`
Creates and configures the HTTP server. Routes: `POST /blob`, `GET /healthz`.
`finalizer` may be `nil`; when nil, `finalize: true` in push requests returns
an error.

### `func (s *Server) ListenAndServe() error`
Starts listening. Blocks until the server stops.

### `func (s *Server) Shutdown(ctx context.Context) error`
Gracefully stops the server.

### Types

```go
type BlobPushRequest struct {
    Commitment    string `json:"commitment"`
    VersionedHash string `json:"versioned_hash"`
    Data          string `json:"data"`
    TxHash        string `json:"tx_hash,omitempty"`
    BlockNumber   uint64 `json:"block_number,omitempty"`
    BlockHash     string `json:"block_hash,omitempty"`
    Slot          uint64 `json:"slot"`
    Epoch         uint64 `json:"epoch"`
    Index         int    `json:"index"`
    Finalize      bool   `json:"finalize,omitempty"`
}

type BlobPushResponse struct {
    DataCID    string `json:"data_cid"`
    MetaCID    string `json:"meta_cid"`
    Commitment string `json:"commitment"`
    Epoch      uint64 `json:"epoch"`
    Finalized  bool   `json:"finalized,omitempty"`
    EpochCID   string `json:"epoch_cid,omitempty"`
}

type BlobProcessor func(ctx context.Context, req BlobPushRequest) (BlobPushResponse, error)
type EpochFinalizer func(ctx context.Context, epoch uint64) (epochCID string, err error)
```

---

## Package `db`

### `func New(ctx context.Context, dsn string) (*Client, error)`
Connects to PostgreSQL and runs schema migrations. Returns an error if the
connection cannot be established.

### `func (c *Client) Close()`
Closes the connection pool.

### `func (c *Client) SaveBlobs(ctx, network string, epoch uint64, inputs []BlobInput, results []BlobResult) error`
Inserts blob rows. Uses `ON CONFLICT DO NOTHING`.

### `func (c *Client) SaveEpoch(ctx, network string, result EpochResult, blobCount int) error`
Inserts or updates the epoch row.

### `func (c *Client) GetBlobsByEpoch(ctx context.Context, epoch uint64) ([]types.BlobInput, error)`
Returns all blobs stored for the given epoch.

### `func (c *Client) GetAllEpochs(ctx context.Context, network string) ([]types.EpochResult, error)`
Returns all epoch CIDs for the network, sorted by epoch ascending.

### `func NewDBStateBackend(c *Client, network string) *DBStateBackend`
Returns a `state.Backend` backed by `MAX(epoch)` on `ipld_epochs`.

---

## Package `generator`

### `func New(ctx context.Context, cfg *config.Config, log *slog.Logger) (*Generator, error)`
Creates a `Generator`. Connects to IPFS; optionally connects to PostgreSQL and
the beacon node depending on config. Selects DB or file state backend
automatically. Returns an error if required clients cannot be initialised.

### `func (g *Generator) Close()`
Releases resources (closes DB pool if open).

### `func (g *Generator) Run(ctx context.Context) error`
Starts the continuous beacon-pull loop. Requires `beacon_rpc` to be configured.
Blocks until `ctx` is cancelled; transient errors are logged and the loop
continues.

### `func (g *Generator) ProcessEpoch(ctx context.Context, epoch uint64) error`
One-shot: processes a single epoch via the beacon node. Used by the `epoch`
subcommand.

### `func (g *Generator) ProcessBlobInput(ctx context.Context, req api.BlobPushRequest) (api.BlobPushResponse, error)`
Push API callback: stores a single blob in IPFS (and DB if configured).

### `func (g *Generator) FinalizeEpochWithCID(ctx context.Context, epoch uint64) (string, error)`
Push API callback: builds the `EpochNode` for `epoch` from blobs stored in the
DB and returns its CID string. Requires DB persistence.

### `func (g *Generator) FinalizeEpoch(ctx context.Context, epoch uint64) error`
CLI entry point for the `finalize-epoch` subcommand. Requires DB persistence.
