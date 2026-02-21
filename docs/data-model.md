# Data Model

All nodes are encoded with **dag-cbor** (codec `0x71`) except raw blob data
which uses **raw** (codec `0x55`). All CIDs use **sha2-256** as the multihash
(`0x12`, 32-byte digest), CID version 1.

Map keys inside every dag-cbor node are sorted **lexicographically** before
encoding. This is the only requirement for full determinism: given the same
inputs the generator always produces the same CIDs.

---

## NetworkRoot

The top-level index for a network. Rebuilt from scratch after every epoch is
finalized and uploaded to IPFS. When an IPNS key is configured, the current
`NetworkRoot` CID is also published under that key, giving consumers a stable
`/ipns/<key>` pointer that always resolves to the latest state.

**Source:** `builder.BuildNetworkRoot` in `builder/network.go`

### Fields

| Field | Type | Description |
|-------|------|-------------|
| `network` | string | Network name, e.g. `"mainnet"`, `"sepolia"` |
| `latestEpoch` | int | Highest epoch number in the index |
| `epochCount` | int | Total number of epochs indexed |
| `approximateSizeBytes` | int | Sum of `approximateSizeBytes` across all epochs |
| `epochs` | map\<string, &EpochNode\> | Key: decimal epoch number string |

### Example

```json
{
  "network": "mainnet",
  "latestEpoch": 270000,
  "epochCount": 432,
  "approximateSizeBytes": 46789012345,
  "epochs": {
    "269568": { "/": "bafyreib2..." },
    "269569": { "/": "bafyreic3..." },
    "270000": { "/": "bafyreid4..." }
  }
}
```

### IPNS resolution

```bash
# Resolve the current NetworkRoot CID
ipfs name resolve /ipns/k51q...

# Fetch the NetworkRoot node
ipfs dag get /ipns/k51q...
```

---

## EpochNode

An immutable node representing one finalized Ethereum epoch (32 slots). Contains
a `blobIndex` that maps KZG commitments to `BlobMetadata` nodes.

**Source:** `builder.BuildEpochNode` in `builder/epoch.go`

### Fields

| Field | Type | Description |
|-------|------|-------------|
| `epoch` | int | Ethereum epoch number |
| `slot` | int | First slot of the epoch (`epoch × 32`) |
| `network` | string | Network name |
| `blobCount` | int | Total number of blobs in this epoch |
| `approximateSizeBytes` | int | Sum of raw blob sizes in bytes |
| `blobIndex` | BlobIndex | Inline map or HAMT shards (see below) |

### Example

```json
{
  "epoch": 1000,
  "slot": 32000,
  "network": "mainnet",
  "blobCount": 28,
  "approximateSizeBytes": 3670016,
  "blobIndex": {
    "type": "map",
    "blobs": {
      "0xaabb...": { "/": "bafyreig7..." },
      "0xccdd...": { "/": "bafyreih8..." }
    }
  }
}
```

### CAR file

Each epoch can be exported to a self-contained CAR v2 file using the
`export-car` CLI subcommand:

```bash
blobscan-ipld export-car -config mainnet.yaml -n 269568
# writes to <storage.car_dir>/mainnet/269568.car
```

The CAR file includes every block reachable from the `EpochNode` root —
all `BlobMetadata` nodes and raw blob blocks — plus a built-in index for
random-access block lookup without reading the full file.

```bash
# Import into your local IPFS node
ipfs dag import mainnet/269568.car

# Pin after import
ipfs pin add -r <EpochNodeCID>
```

---

## BlobIndex

The `blobIndex` field inside an `EpochNode` is one of two representations
depending on the number of blobs in the epoch, controlled by
`generator.hamt_threshold` (default: 5000).

### BlobMap (< hamt_threshold blobs)

An inline dag-cbor map with two fields:

| Field | Type | Description |
|-------|------|-------------|
| `type` | string | Always `"map"` |
| `blobs` | map\<string, &BlobMetadata\> | Key: KZG commitment hex string |

```json
{
  "type": "map",
  "blobs": {
    "0xaabb...": { "/": "bafyreig7..." }
  }
}
```

### HAMTIndex (≥ hamt_threshold blobs)

A sharded structure for epochs with very large numbers of blobs. Blobs are
sorted by commitment and split into shards of 256 entries each. Each shard is
stored as a separate dag-cbor block.

| Field | Type | Description |
|-------|------|-------------|
| `type` | string | Always `"hamt"` |
| `shardSize` | int | Entries per shard (always 256) |
| `shards` | list\<&Shard\> | Ordered list of links to shard blocks |

Each shard block is a dag-cbor map: `commitment → &BlobMetadata`.

> **Note:** This is a simplified sharding scheme. For full HAMT ADL
> compatibility, replace with `go-ipld-adl-hamt`.

---

## BlobMetadata

An immutable dag-cbor node holding all indexable fields for a single blob,
plus a link to the raw blob data.

**Source:** `builder.StoreBlobMetadata` in `builder/blob.go`

### Fields

| Field | Type | Description |
|-------|------|-------------|
| `commitment` | string | KZG commitment, 0x-prefixed hex (48 bytes) |
| `versionedHash` | string | EIP-4844 versioned hash: `0x01 \|\| SHA256(commitment)[1:]` |
| `txHash` | string | Execution-layer transaction hash — **empty in current implementation** (ELClient not wired) |
| `blockNumber` | int | Execution-layer block number — **0 in current implementation** |
| `blockHash` | string | Beacon block root (`sc.BlockRoot`) — overwritten with EL block hash if ELClient is wired |
| `slot` | int | Beacon slot number |
| `epoch` | int | Beacon epoch number |
| `index` | int | Blob index within the transaction (0-based) |
| `size` | int | Raw blob size in bytes (always 131072 = 128 KiB) |
| `data` | &Blob | CID link to the raw blob block (codec=raw) |

### Example

```json
{
  "commitment":    "0xaabbccddeeff...",
  "versionedHash": "0x01bbccddeeff...",
  "txHash":        "0xdeadbeef...",
  "blockNumber":   19000000,
  "blockHash":     "0xcafebabe...",
  "slot":          32001,
  "epoch":         1000,
  "index":         0,
  "size":          131072,
  "data":          { "/": "bafkreibm..." }
}
```

---

## Raw Blob

The raw 128 KiB (131072 bytes) EIP-4844 blob field element, stored with
codec=raw. The CID is the sha2-256 hash of the raw bytes.

**Source:** `builder.StoreRawBlob` in `builder/blob.go`

```
CIDv1  codec=0x55(raw)  mh=sha2-256
```

This is the leaf node of the entire DAG. Two transactions that include the
same blob data will produce the same raw blob CID, enabling natural
deduplication at the IPFS block layer.

---

## IPLD Schema

The canonical schema is in `schema/schema.ipldsch`:

```
type NetworkRoot struct {
  network              String
  epochs               {String : &EpochNode}
  latestEpoch          Int
  epochCount           Int
  approximateSizeBytes Int
}

type EpochNode struct {
  epoch               Int
  slot                Int
  network             String
  blobIndex           BlobIndex
  approximateSizeBytes Int
}

type BlobIndex union {
  | BlobMap    "map"
  | &HAMTShard "hamt"
} representation keyed

type BlobMap struct {
  blobs {String : &BlobMetadata}
}

type BlobMetadata struct {
  commitment          String
  versionedHash       String
  txHash              String
  blockNumber         Int
  blockHash           String
  slot                Int
  epoch               Int
  index               Int
  size                Int
  data                &Blob
}

type Blob bytes
```

---

## Traversal examples

```bash
# Via IPNS (always resolves to the latest NetworkRoot)
ipfs dag get /ipns/k51q.../

# Get a specific epoch via IPNS
ipfs dag get /ipns/k51q.../epochs/269568

# Get a specific blob's metadata via IPNS
ipfs dag get /ipns/k51q.../epochs/269568/blobIndex/blobs/0xaabb...

# Via direct CID (stable, immutable)
# NetworkRoot CID is returned by the generator or: SELECT cid FROM ipld_network_roots
ipfs dag get <NetworkRootCID>
ipfs dag get <NetworkRootCID>/epochs/269568
ipfs dag get <NetworkRootCID>/epochs/269568/blobIndex/blobs/0xaabb...

# Get the raw blob bytes
ipfs block get bafkreibm...
```
