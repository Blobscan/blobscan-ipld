# Operations Guide

## Prerequisites

| Requirement | Notes |
|-------------|-------|
| Go 1.26+ | `go version` to check |
| IPFS node (Kubo) | Running locally or remotely; API must be reachable |
| Ethereum Beacon Node | REST API enabled (Lighthouse, Prysm, Teku, Nimbus, Lodestar). **Requires `--custody-group-count=64`+** post-PeerDAS — see [Beacon node requirements (PeerDAS)](#beacon-node-requirements-peerdas) below |
| Ethereum Execution Node | JSON-RPC API — optional; needed to populate `txHash` and `blockNumber` in `BlobMetadata` (not yet wired) |
| Disk space | ~3.5 GiB per 1000 epochs of mainnet blob data (128 KiB × avg blobs/epoch) |

> **Execution Layer node — current status:** `beacon.FetchEpochInput` accepts an optional `ELClient`
> interface to enrich each blob with `txHash`, `blockNumber`, and the EL `blockHash`. The generator
> currently passes `nil`, so **those fields are always empty** in the generated DAG. To enable EL
> enrichment, implement the `beacon.ELClient` interface and wire it into `generator.processEpoch`.

---

## Beacon node requirements (PeerDAS)

Since the Ethereum **PeerDAS upgrade (EIP-7594)**, regular beacon nodes no
longer custody all blob data — they only hold a subset of *data columns*.
The `/eth/v1/beacon/blob_sidecars/{slot}` endpoint requires the node to
custody **at least 64** data columns in order to reconstruct and serve full blob sidecars.

blobscan-ipld uses this endpoint, so **the beacon node must custody at least
64 data columns** (i.e. `--custody-group-count=64` or higher). Without this,
the beacon node returns HTTP 503 and blobscan-ipld will abort with:

```
beacon node does not custody enough data columns to serve blob sidecars (PeerDAS);
reconfigure it with --custody-group-count=64 or higher (or equivalent for your client)
```

Setting 128 makes the node a full supernode (all groups); 64 is the minimum
required to serve the blob sidecars endpoint.

### Client-specific flags

| Client | Flag (minimum) | Flag (full supernode) |
|-----------|----------------|----------------------|
| Lighthouse | `--custody-group-count 64` | `--custody-group-count 128` |
| Prysm | `--custody-group-count=64` | `--custody-group-count=128` |
| Teku | `--p2p-custody-group-count=64` | `--p2p-custody-group-count=128` |
| Lodestar | `--chain.custodyGroupCount 64` | `--chain.custodyGroupCount 128` |
| Nimbus | `--custody-group-count=64` | `--custody-group-count=128` |

> **Resource impact:** higher custody group counts increase bandwidth and
> disk usage. 64 is the minimum for blobscan-ipld; 128 (full supernode)
> makes the node custody all data columns on the network.

---

## Docker Compose (recommended)

The repository ships with two Compose files that start PostgreSQL, an IPFS
(Kubo) node, and `blobscan-ipld` in a single command. No local Go toolchain or
manual IPFS setup is required.

### Quick start

```bash
cp .env.example .env
# Edit .env — set POSTGRES_PASSWORD and the beacon RPC URL for your network
```

**.env variables:**

| Variable | Description |
|----------|-------------|
| `POSTGRES_PASSWORD` | Password for the `blobscan` PostgreSQL user |
| `MAINNET_BEACON_RPC` | Beacon node REST API URL for mainnet |
| `SEPOLIA_BEACON_RPC` | Beacon node REST API URL for Sepolia |

**Mainnet:**

```bash
docker compose up -d
```

**Sepolia:**

```bash
docker compose -f docker-compose.sepolia.yml up -d
```

The correct `start_epoch` for each network is applied automatically — no
config file changes needed.

### Useful commands

```bash
# Open a PostgreSQL shell (Sepolia)
docker compose -f docker-compose.sepolia.yml exec postgres psql -U blobscan -d ipld_sepolia

# Open a PostgreSQL shell (mainnet)
docker compose exec postgres psql -U blobscan -d ipld_mainnet

# Follow generator logs
docker compose -f docker-compose.sepolia.yml logs -f blobscan-ipld

# Check container status
docker compose -f docker-compose.sepolia.yml ps
```

### Ports exposed on localhost

| Service | Port | Protocol |
|---------|------|----------|
| blobscan-ipld HTTP push API | `8080` | HTTP |
| IPFS API | `5001` | HTTP |
| IPFS gateway | `8081` | HTTP |
| IPFS swarm | `4001` | TCP/UDP |

### GitHub Container Registry

Pre-built images are published to `ghcr.io/blobscan/blobscan-ipld` on every
push to `master` and for every version tag. To use them instead of building
locally:

```yaml
# in docker-compose.yml, replace `build: .` with:
image: ghcr.io/blobscan/blobscan-ipld:latest
```

Tags follow the pattern `latest`, `master`, `sha-<short>`, and `<semver>` for
tagged releases.

---

## IPFS node setup (Kubo + Pebble)

Kubo (the reference IPFS implementation) supports pluggable datastores. For
blob-heavy workloads, **Pebble** is strongly recommended over both the
default `flatfs` and the legacy `badgerds` plugin. Pebble is CockroachDB's
production-grade LSM-tree key/value store (a RocksDB-style engine written in
Go), exposed to Kubo as the `pebbleds` plugin.

### Why Pebble (vs. the default `flatfs`)

`flatfs` stores every IPFS block as an individual file in a sharded
directory tree. That model breaks down quickly at the scale this indexer
produces (millions of ~128 KiB blob blocks per network). Pebble packs the
same data into a small number of large SST files and gives you, in addition:

- **Far higher write throughput.** Pebble's LSM-tree absorbs the generator's
  steady stream of block writes in batched, sequential I/O. `flatfs` issues
  one `open`/`write`/`fsync`/`close` syscall per block — orders of magnitude
  more filesystem work for the same payload.
- **No million-files problem.** `flatfs` pressures the filesystem with
  millions of small files: inode exhaustion, slow directory listings, slow
  backups/rsync, and a metadata cache that no longer fits in RAM. Pebble
  stores blocks inside a handful of large SST files, so the filesystem only
  sees a few dozen entries no matter how many blocks you've indexed.
- **Much smaller on-disk footprint per block.** `flatfs` pays a full
  filesystem-block (typically 4 KiB) of overhead for every IPFS block plus
  per-file inode/metadata cost. Pebble amortises both across SSTs.
- **Continuous background compaction with no manual tuning.** Pebble
  auto-tunes its level structure and compacts in the background. Read and
  write amplification stay bounded as the repo grows into the tens of GiB —
  there is no equivalent of "the directory got too big" with `flatfs`.
- **Configurable block cache for read-heavy queries.** `flatfs` relies
  entirely on the OS page cache. Pebble exposes a dedicated block cache
  (`cacheSize`) you can size for the indexer's hot set, so repeated reads
  of recent epochs stay in memory.
- **Crash-safe with fast recovery.** The Pebble WAL guarantees durability;
  recovery is a short log replay rather than a full directory rescan.
- **Faster shutdown and startup.** No per-file `fsync` storms on shutdown;
  no directory-walk cost on startup.

### Why Pebble (vs. `badgerds`)

`badgerds` is also an LSM-tree, so it shares Pebble's broad advantages over
`flatfs`. The reasons to prefer Pebble specifically:

- **Actively maintained and the supported choice in Kubo.** `badgerds` has
  been deprecated in Kubo and is no longer recommended for new deployments;
  `pebbleds` is the maintained replacement. Picking it now avoids a forced
  datastore change later.
- **More predictable write latency under sustained load.** Badger's
  value-log architecture introduces periodic GC pauses that can stall writes
  on a continuously-ingesting indexer. Pebble's level-based compaction is
  smoother and avoids the value-log step entirely.
- **No value-log GC to tune.** Badger requires periodic `RunValueLogGC` to
  reclaim space in its value log; if it's skipped, on-disk size drifts
  upward. Pebble reclaims space automatically as part of normal compaction.
- **Lower steady-state memory footprint** for the same hot-set size, with
  Pebble's block cache giving finer control over the memory/read-hit-rate
  trade-off than Badger's table/value caches.
- **Faster, more reliable crash recovery.** Long-running `badgerds` repos
  are prone to slow index rebuilds (and occasional corruption) after an
  unclean shutdown. Pebble's WAL replay is short and well-tested.
- **Smaller, simpler on-disk format.** Pebble does not split data between an
  SST tree and a separate value log, which keeps backups and disk-usage
  accounting straightforward.

### Docker Compose users

The Compose files in this repo apply the `server,pebbleds` Kubo profiles
automatically on first init via `IPFS_PROFILE`. No manual steps are required
for a fresh deployment — skip to [Initial setup](#initial-setup).

> **Note:** Kubo profiles are only applied when the repo is *first*
> initialised. Use a fresh data volume so the `pebbleds` profile takes effect.

#### Enabling Kubo logging

To debug IPFS uploads or inspect incoming requests, enable Kubo's structured
logging by adding environment variables to the `ipfs` service in `docker-compose.yml`:

```yaml
ipfs:
  image: ipfs/kubo:latest
  environment:
    IPFS_PROFILE: "server,pebbleds"
    GOLOG_LOG_LEVEL: "info"  # or "debug" for verbose output
```

Log levels:
- `info` — General activity, including HTTP API requests to port 5001
- `debug` — Very detailed, includes request/response bodies (verbose)
- `warn` — Only warnings and errors

View logs after startup:
```bash
docker-compose logs ipfs -f
```

#### Checking what Kubo has indexed

To see what blocks Kubo has stored so far:

```bash
# Total repository size and block count
docker-compose exec ipfs ipfs repo stat

# List all content hashes currently stored
docker-compose exec ipfs ipfs refs local

# Count total blocks
docker-compose exec ipfs ipfs refs local | wc -l

# See pinned content
docker-compose exec ipfs ipfs pin ls
```

Run these commands before and after uploads to verify that new data is being
stored in Kubo.

### 1. Install Kubo with Pebble support

The `pebbleds` plugin ships in the default Kubo build from v0.31.0 onward —
no custom build is required. Download a release from
<https://github.com/ipfs/kubo/releases>:

```bash
wget https://github.com/ipfs/kubo/releases/download/v0.32.1/kubo_v0.32.1_linux-amd64.tar.gz
tar -xzf kubo_v0.32.1_linux-amd64.tar.gz
sudo bash kubo/install.sh
ipfs version
```

Verify the plugin is available:

```bash
ipfs plugin ls
# Should include: pebbleds (datastore)
```

### 2. Initialise the IPFS repo with Pebble

Apply the `server` and `pebbleds` profiles at init time:

```bash
ipfs init --profile=server,pebbleds
```

This produces a `Datastore.Spec` section like:

```json
"Datastore": {
  "StorageMax": "10GB",
  "StorageGCWatermark": 90,
  "GCPeriod": "1h",
  "Spec": {
    "mounts": [
      {
        "child": {
          "path": "pebbleds",
          "type": "pebbleds"
        },
        "mountpoint": "/blocks",
        "prefix": "pebble.datastore",
        "type": "measure"
      },
      {
        "child": {
          "compression": "none",
          "path": "datastore",
          "type": "levelds"
        },
        "mountpoint": "/",
        "prefix": "leveldb.datastore",
        "type": "measure"
      }
    ],
    "type": "mount"
  },
  "HashOnRead": false,
  "BloomFilterSize": 0
}
```

Raise `StorageMax` to match the disk you've allocated (e.g. `8TB` for archival mainnet).

### 3. Configure the API and Gateway (optional)

Edit `~/.ipfs/config` to bind the API and gateway to the addresses expected
by the generator:

```json
"Addresses": {
  "API": "/ip4/127.0.0.1/tcp/5001",
  "Gateway": "/ip4/127.0.0.1/tcp/8080",
  "Swarm": [
    "/ip4/0.0.0.0/tcp/4001",
    "/ip6/::/tcp/4001"
  ]
}
```

### 4. Tune Pebble for large block workloads (optional)

The `pebbleds` plugin works well out of the box. Pebble auto-tunes most
parameters from the workload, so manual tuning is rarely needed. The few knobs
exposed by Kubo can be set in `Datastore.Spec.mounts[0].child`:

```json
{
  "path": "pebbleds",
  "type": "pebbleds",
  "cacheSize": 1073741824,
  "formatMajorVersion": 0,
  "disableWAL": false
}
```

| Parameter | Value | Rationale |
|-----------|-------|-----------|
| `cacheSize` | `1073741824` (1 GiB) | Larger block cache improves read hit rate for hot epochs |
| `disableWAL` | `false` | Keep crash safety on; the generator's pipeline already batches writes |
| `formatMajorVersion` | `0` | Use the latest format Pebble supports; bump only on advice from Pebble release notes |

For most deployments leaving the defaults is fine.

### 5. Configure storage limits (StorageMax)

The `StorageMax` setting in Kubo's config controls the maximum size of stored
blocks before garbage collection is triggered. It's configured when the repo is
first initialised and stored in `~/.ipfs/config`:

```json
"Datastore": {
  "StorageMax": "8TB",
  "StorageGCWatermark": 90,
  "GCPeriod": "1h"
}
```

**You can change `StorageMax` at any time** — edit the config file and restart
Kubo, no repo reinitialization needed.

#### Checking and editing StorageMax

Check the current value:

```bash
docker-compose exec ipfs ipfs config Datastore.StorageMax
```

Change it using `ipfs config`:

```bash
docker-compose exec ipfs ipfs config Datastore.StorageMax 8TB

# Restart Kubo to apply the change
docker-compose restart ipfs
```

#### Recommended values

| Use case | StorageMax |
|----------|-----------|
| Testing / Sepolia | `20GB` |
| Mainnet (small history) | `100GB` |
| **Blob archival (recommended)** | **`8TB` or higher** |

`StorageMax` is not a hard limit — Kubo will write beyond it if needed, but
triggers automatic garbage collection when exceeded. Setting it too low causes
frequent GC; setting it high gives Kubo more breathing room before cleanup.

> **For blob archival:** Since blobs are immutable and indexed forever, set
> `StorageMax` to match your available disk space (e.g., `8TB`). Garbage
> collection is wasteful — Kubo would trigger GC once full, delete pinned blocks,
> and re-download them from peers on next access. Instead, allocate enough disk
> to store the entire blob history without GC, then pin everything permanently
> with `ipfs pin add`.

#### Decreasing StorageMax

If you decrease `StorageMax` below the current repo size, Kubo will **delete
blocks** via garbage collection on next startup to bring the repo under the new
limit. **This is destructive** — blocks that are not pinned will be lost.

To safely decrease `StorageMax`:
1. Ensure all important blocks are pinned: `ipfs pin add -r <CID>`
2. Edit the config and decrease `StorageMax`
3. Restart Kubo — it will run GC and delete unpinned blocks until under the limit
4. Verify with `ipfs repo stat` that the new size is respected

For blob archival, **never decrease `StorageMax` below your indexed data size**.
Keep it set to your full disk capacity to avoid accidental data loss.

### 6. Start the IPFS daemon

```bash
ipfs daemon --migrate=true
```

`--migrate=true` automatically runs any pending repo migrations on startup.

For production, run as a systemd service:

```ini
[Unit]
Description=IPFS Daemon (Kubo)
After=network.target

[Service]
ExecStart=/usr/local/bin/ipfs daemon --migrate=true
Restart=on-failure
RestartSec=5s
User=ipfs
Environment=IPFS_PATH=/var/lib/ipfs
LimitNOFILE=65536

[Install]
WantedBy=multi-user.target
```

```bash
sudo systemctl enable --now ipfs
```

### 7. Verify the API is reachable

```bash
curl -s http://127.0.0.1:5001/api/v0/id | jq .ID
# → "12D3KooW..."
```

This is the address you set in `ipfs.api_addr` in the generator config.

### 8. Create an IPNS key (optional)

Only needed if you want to publish the `NetworkRoot` under a stable IPNS name:

```bash
ipfs key gen --type=ed25519 blobscan-mainnet
# → k51qzi5uqu5d...
```

### Pebble maintenance

```bash
# Run garbage collection manually (removes unreferenced blocks)
ipfs repo gc

# Check repo size
ipfs repo stat
```

Pebble runs background compactions continuously; there is no manual
compaction command to invoke. If repo size grows unexpectedly, check for
unreferenced blocks with `ipfs repo gc` and review pinned roots.

---

## Initial setup

> Before running the generator, complete the [IPFS node setup](#ipfs-node-setup-kubo--pebble)
> section above. The IPNS key is created in step 8 of that section.

### 1. Create storage directories

The generator creates directories automatically, but you can pre-create them
with the right permissions:

```bash
mkdir -p /var/lib/blobscan-ipld/mainnet/car
```

### 2. Set environment variables

Copy the example and edit the required values:

```bash
cp .env.example .env
```

Minimum required variables:

```bash
NETWORK_NAME=mainnet
MAINNET_BEACON_RPC=http://localhost:5052
IPFS_API_ADDR=/ip4/127.0.0.1/tcp/5001
DATA_DIR=/var/lib/blobscan-ipld/mainnet
POSTGRES_DSN=postgres://user:pass@localhost:5432/blobscan  # optional
```

### 4. Build

```bash
go build ./cmd/blobscan-ipld
```

---

## Running the generator

### Continuous mode (normal operation)

```bash
./blobscan-ipld run
```

The generator:
1. Resumes from `MAX(epoch)` in PostgreSQL (or state file if no DB).
2. Polls the beacon node every `poll_interval` (default 12 s).
3. For each new finalized epoch: fetches blobs, builds the DAG, uploads to
   IPFS, saves to DB, rebuilds `NetworkRoot`.

Logs are written to stderr in structured text format. Use `-log-level debug`
for verbose output including per-block operations.

### Graceful shutdown

Send `SIGINT` (Ctrl-C) or `SIGTERM`. The generator finishes the current epoch
and exits cleanly. The state file is always consistent on exit.

### Running as a systemd service

```ini
[Unit]
Description=Blobscan IPLD DAG Generator
After=network.target ipfs.service

[Service]
EnvironmentFile=/etc/blobscan-ipld/mainnet.env
ExecStart=/usr/local/bin/blobscan-ipld run
Restart=on-failure
RestartSec=10s
User=blobscan
StandardOutput=journal
StandardError=journal

[Install]
WantedBy=multi-user.target
```

---

## Summary

The `summary` subcommand prints a human-readable overview of what has been
indexed without requiring any manual SQL queries.

```bash
# Default: epoch count, blob count, data size, time range, cursors
./blobscan-ipld summary

# Full detail: all flags enabled
./blobscan-ipld summary -gaps -top 10 -monthly -check-ipfs
```

**Example output:**

```
── blobscan-ipld summary ── sepolia ─────────────────────────────────────
  Epochs           1,234  [132608 → 133841]  (no gaps)
  Blobs            28,456  (avg 23.1/epoch · peak 347 in epoch 133500)
  Data size        3.62 GiB  (avg 3.0 MiB/epoch)
  Time             2024-01-15T12:00:00Z → 2024-03-20T14:24:00Z  (65 days)
  Cursors          live=133841  backfill=133750
  IPFS             use -check-ipfs to verify upload status
```

### Flags

| Flag | Description |
|------|-------------|
| `-check-ipfs` | Check each epoch node CID against the IPFS node (16 parallel workers). Reports how many epoch nodes are present and lists any missing ones (up to 10). |
| `-gaps` | List all contiguous ranges of missing epochs within the indexed range. |
| `-top N` | Show the N epochs with the highest blob count as a table (epoch, blobs, size, time). |
| `-monthly` | Show a month-by-month breakdown of indexed epochs, blobs, and data size. |

**`-check-ipfs` detail:**

```
  IPFS             1,231/1,234 epoch nodes present  (99.8%)
                   missing: 132900 · 133100 · 133500
```

The check uses `block/stat` on each epoch node CID. If an epoch node is present
its blob blocks were also uploaded in the same batch, so this is a reliable proxy
for overall IPFS upload completeness. Use `backfill-ipfs` to recover missing epochs.

**`-gaps` detail:**

```
── Epoch gaps (3 ranges · 15 epochs missing) ────────────────────────────
  132900 → 132905  (6 epochs)
  133100           (1 epoch)
  133500 → 133508  (9 epochs)
```

**`-monthly` detail:**

```
── Monthly breakdown ─────────────────────────────────────────────────────
  MONTH       EPOCHS       BLOBS       SIZE
  2024-01        234       5,432    696 MiB
  2024-02        310       7,210    924 MiB
  2024-03        690      15,814   2.00 GiB
```

---

## Uploading historical epochs to IPFS (backfill-ipfs)

If the indexer was previously run with `IPFS_SKIP_UPLOAD=true` (or without an
IPFS node configured), the DB contains correct CIDs but the actual IPLD blocks
were never uploaded to IPFS. Use the `backfill-ipfs` subcommand to recover:

```bash
# Upload all epochs stored in the DB
./blobscan-ipld backfill-ipfs

# Upload a specific range only
./blobscan-ipld backfill-ipfs -from 269568 -to 270000
```

**Requirements:**
- `IPFS_SKIP_UPLOAD` must be unset or `false` (an IPFS node must be reachable).
- `storage.postgres_dsn` must be set.
- `network.beacon_rpc` must be set — blob data is re-fetched from the beacon
  node because the raw 128 KiB bytes were never persisted locally.

**How it works:**

For each epoch in the range the command:
1. Loads blob metadata (commitment, CIDs, slot, etc.) from the DB.
2. Re-fetches the raw blob sidecars from the beacon node.
3. Re-runs the full `ProcessBlob` pipeline to produce `DataCID` and `MetaCID`.
4. **Compares freshly-computed CIDs against the DB values** — any mismatch is
   logged as an error (`backfill-ipfs: DataCID mismatch` / `MetaCID mismatch`).
   Upload continues with the newly-computed blocks and the DB is updated.
5. Uploads all blocks (blob data, blob metadata, epoch node, HAMT if any) to IPFS.
6. Updates the epoch row in the DB (idempotent).
7. Rebuilds `NetworkRoot` once at the end.

A single beacon RPC error on one epoch is logged and skipped; the rest of the
run continues. Use `-log-level debug` to see per-block IPFS upload progress.

**IPFS upload errors are retried indefinitely** (with `poll_interval` between
attempts) rather than skipping the epoch. This handles transient failures such
as the IPFS container being temporarily unreachable (e.g. Docker DNS not yet
resolving `ipfs` during startup). Because `block/put` is idempotent on Kubo,
resuming a partially-uploaded epoch is safe. The retry loop exits as soon as the
upload succeeds or the process receives a shutdown signal (SIGINT/SIGTERM).

**Beacon node custody window:**

Standard beacon nodes only retain blob sidecars for ~18 days (4096 epochs). For
epochs older than that, you need a beacon node configured as an archival node or
one that fetches blob history from a provider that retains full history.
Set the `network.beacon_rpc` to such an endpoint before running `backfill-ipfs`.

---

## Exporting blob CID references (export-blob-refs)

The `export-blob-refs` subcommand exports all blob CID references from the
local DB as a CSV file that can be directly imported into blobscan's
`blob_data_storage_reference` table.

```bash
# Export all blobs to a file
./blobscan-ipld export-blob-refs -out /tmp/refs.csv

# Export a specific epoch range to stdout
./blobscan-ipld export-blob-refs -from 269568 -to 270000
```

**Flags:**

| Flag | Description |
|------|-------------|
| `-from N` | First epoch to export (default: 0) |
| `-to N` | Last epoch to export (default: max epoch in DB) |
| `-out PATH` | Output CSV file path (default: stdout) |

**Requirements:**
- `POSTGRES_DSN` must be set.

**CSV format:**

The output CSV has a header row and four columns matching the blobscan
`blob_data_storage_reference` table:

```
blob_hash,storage,data_reference,meta_reference
0x01ab…,ipfs,bafyrei…,bafyrei…
```

**Importing into blobscan:**

```bash
psql "$BLOBSCAN_DATABASE_URL" -c "\copy blob_data_storage_reference(blob_hash, storage, data_reference, meta_reference) FROM '/tmp/refs.csv' WITH (FORMAT csv, HEADER true)"
```

The import uses `\copy` which handles conflicts based on the table's composite
primary key `(blob_hash, storage)`. If rows already exist, use an intermediate
staging approach or add `ON CONFLICT DO NOTHING` via a temp table:

```sql
CREATE TEMP TABLE staging (LIKE blob_data_storage_reference INCLUDING ALL);
\copy staging FROM '/tmp/refs.csv' WITH (FORMAT csv, HEADER true);
INSERT INTO blob_data_storage_reference
SELECT * FROM staging
ON CONFLICT (blob_hash, storage) DO UPDATE SET
  data_reference = EXCLUDED.data_reference,
  meta_reference = EXCLUDED.meta_reference;
```

---

## Backfilling historical epochs

Use the `epoch` subcommand to process a single epoch and exit. This is useful for:
- Filling gaps in the DAG after a downtime.
- Testing the pipeline with a specific epoch.
- Debugging a problematic epoch.

```bash
# Process epoch 300000 only
./blobscan-ipld -n 300000 epoch
```

To backfill a range of epochs, set `GENERATOR_START_EPOCH` and run in
continuous mode with `GENERATOR_SKIP_EXISTING_EPOCHS=false`. The generator will process
all epochs from `start_epoch` to the current finalized epoch before entering
the normal poll loop.

```yaml
generator:
  # start_epoch defaults to 269568 on mainnet; override only if needed
  skip_existing_epochs: false
```

---

## Working with CAR files

### File naming

CAR files are written to:
```
<storage.car_dir>/<network>/<firstEpoch>-<lastEpoch>.car
```

Example: `/var/lib/blobscan-ipld/mainnet/car/mainnet/0-999.car`

### Importing a CAR file into IPFS

```bash
ipfs dag import mainnet/0-999.car
```

This adds all blocks from the CAR file to your local IPFS node without
pinning them. To also pin:

```bash
ipfs dag import mainnet/269568.car
ipfs pin add -r <EpochNodeCID>
```

### Verifying a CAR file

```bash
# Check the CAR header and list roots
ipfs-car inspect mainnet/269568.car
```

The `car.VerifyCARRoot` function in `car/exporter.go` can be called
programmatically to check that a CAR file's root CID matches the expected
`EpochNode` CID stored in PostgreSQL (`ipld_epochs.cid`).

### Sharing CAR files

CAR files are self-contained and can be distributed via any file transfer
mechanism (HTTP, BitTorrent, S3, etc.). Recipients can import them directly
into their IPFS node without needing to contact the original generator.

---

## Pinning strategies

Each epoch produces one independently-pinnable CID. There is no mutable root
or IPNS pointer — the source of truth for epoch CIDs is the PostgreSQL
`ipld_epochs` table.

> **There is no automatic sync.** IPFS pins are static — pinning a CID today
> does not cause new epochs to be pinned as the generator runs. To keep a
> replica up to date, you must run the pin command periodically (e.g. via
> cron) or run your own generator instance.

### Pin a single epoch

```bash
# Look up the CID from the database:
# SELECT cid FROM ipld_epochs WHERE epoch = 269568;

ipfs pin add -r <EpochNodeCID>
```

### Pin all epochs for a network (batch)

```bash
# Generate pin commands from the database and execute them:
psql "$DSN" -t -c "SELECT cid FROM ipld_epochs WHERE network = 'mainnet' ORDER BY epoch" \
  | xargs -I{} ipfs pin add -r {}
```

### Pin new epochs periodically (cron)

To keep a replica pinned as new epochs are processed, run a cron job that pins
any epochs not yet pinned locally:

```bash
#!/usr/bin/env bash
# pin-new-epochs.sh — run every few minutes via cron
DSN="postgres://user:password@localhost:5432/blobscan"

psql "$DSN" -t -A -c \
  "SELECT cid FROM ipld_epochs WHERE network = 'mainnet' ORDER BY epoch" \
| while read cid; do
    ipfs pin add -r "$cid" 2>/dev/null && echo "pinned $cid"
  done
```

`ipfs pin add` is idempotent — already-pinned CIDs are skipped instantly.

### Unpin an epoch

```bash
ipfs pin rm <EpochNodeCID>
ipfs repo gc
```

---

## Monitoring

The generator logs structured key-value pairs to stderr with visual symbols for clarity. Key log lines:

| Event | Symbol | Level | Key fields |
|-------|--------|-------|-----------|
| Startup banner | — | `INFO` | Engine initialization message |
| Beacon network verified | ✓ | `INFO` | Network name |
| State backend loaded | ✓ | `INFO` | Backend type and path |
| Genesis time loaded | ✓ | `INFO` | Network genesis timestamp |
| Parallel processing enabled | ┌─ | `INFO` | Live and backfill cursors |
| Live processing started | ▶ | `INFO` | Network, poll interval |
| New finalized epochs | ▲ | `INFO` | Count and epoch range |
| Backfill started | ⟲ | `INFO` | Epoch range, total count |
| Epoch built (live) | ● | `INFO` | `cid`, `rpc_requests`, blob count |
| Epoch built (backfill) | ■ | `INFO` | `cid`, `rpc_requests`, blob count |
| IPFS upload disabled | ⊘ | `INFO` | — |
| No new finalized epochs | — | `DEBUG` | Finalized epoch, cursor |
| Any processing error | ✗ | `ERROR` | Error details |

**RPC Request Counting:** The `rpc_requests` field in epoch-built logs shows the cumulative count of all HTTP requests made to the beacon node since startup. This includes:
- Finality checkpoints (1 per live tick + 1 startup)
- Blob sidecars (32 per epoch processed)
- Network verification (1 startup)
- Genesis time fetch (1 startup)

This counter is useful for tracking and comparing with your billing dashboard if you use a remote RPC provider.

### Example log output

```
time=2024-03-15T10:00:00Z level=INFO msg="
╔═══════════════════════════════════════════════════════════╗
║                  blobscan-ipld engine                    ║
║         Building IPLD DAGs from Ethereum blobs           ║
╚═══════════════════════════════════════════════════════════╝"
time=2024-03-15T10:00:01Z level=INFO msg="✓ Beacon network verified" network=mainnet
time=2024-03-15T10:00:01Z level=INFO msg="✓ Genesis time loaded" genesis_time=2020-12-01T12:00:23Z
time=2024-03-15T10:00:01Z level=INFO msg="▶ Live processing started [mainnet] — polling every 12s"
time=2024-03-15T10:00:02Z level=INFO msg="▲ 33 epochs finalized [269568 .. 269600]"
time=2024-03-15T10:00:02Z level=INFO msg="● Epoch 269568 built [28 blobs]" cid=bafyreid... rpc_requests=35
time=2024-03-15T10:00:03Z level=INFO msg="● Epoch 269569 built [31 blobs]" cid=bafyreia... rpc_requests=68
time=2024-03-15T10:00:04Z level=INFO msg="⟲ Backfill: 100 epochs [134500 → 134599]"
time=2024-03-15T10:00:05Z level=INFO msg="■ Epoch 134500 built [45 blobs]" cid=bafyreif... rpc_requests=3235
...
```

Note: `rpc_requests` shows cumulative request count since startup.

---

## Upgrading

The state format is stable (both DB and file). To upgrade the binary:

1. Stop the running generator (`SIGTERM`).
2. Replace the binary.
3. Restart — it will resume from the last processed epoch.

If the IPLD node format changes (e.g. new fields added to `EpochNode`), the
CIDs of previously-built nodes will differ. In that case you must either:
- Accept that old and new ranges use different schemas (they remain valid IPLD).
- Re-process historical epochs with `skip_existing_epochs: false` and
  `start_epoch: 0` to rebuild the entire DAG with the new format.
