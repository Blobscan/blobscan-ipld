# Operations Guide

## Prerequisites

| Requirement | Notes |
|-------------|-------|
| Go 1.21+ | `go version` to check |
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

## IPFS node setup (Kubo + Badger)

Kubo (the reference IPFS implementation) supports pluggable datastores. For
blob-heavy workloads, **Badger** is strongly recommended over the default
`flatfs` because it handles large numbers of small-to-medium blocks much more
efficiently (better write throughput, less filesystem inode pressure).

### 1. Install Kubo with Badger support

The standard `ipfs` binary does **not** include the Badger plugin — it must be
compiled in. The easiest path is the official `kubo` build with plugins:

```bash
# Option A: download a pre-built release that includes the badger plugin
# Check https://github.com/ipfs/kubo/releases for the latest version
wget https://github.com/ipfs/kubo/releases/download/v0.27.0/kubo_v0.27.0_linux-amd64.tar.gz
tar -xzf kubo_v0.27.0_linux-amd64.tar.gz
sudo bash kubo/install.sh
ipfs version
```

```bash
# Option B: build from source with the badger plugin enabled
git clone https://github.com/ipfs/kubo.git
cd kubo
# Enable the badger plugin in plugin/loader/preload_list:
echo "badgerds github.com/ipfs/kubo/plugin/plugins/badgerds *" >> plugin/loader/preload_list
make build
sudo cp cmd/ipfs/ipfs /usr/local/bin/ipfs
```

Verify the plugin is available:

```bash
ipfs plugin ls
# Should include: badgerds (datastore)
```

### 2. Initialise the IPFS repo with Badger

```bash
# Initialise a fresh repo (skip if you already have one)
ipfs init --profile=server
```

After initialisation, replace the datastore config. Edit
`~/.ipfs/config` (or `$IPFS_PATH/config`) and replace the entire
`"Datastore"` section:

```json
"Datastore": {
  "StorageMax": "100GB",
  "StorageGCWatermark": 90,
  "GCPeriod": "1h",
  "Spec": {
    "type": "measure",
    "prefix": "badger.datastore",
    "child": {
      "type": "badgerds",
      "path": "badgerds",
      "syncWrites": false,
      "truncate": true
    }
  },
  "HashOnRead": false,
  "BloomFilterSize": 0
}
```

> **`syncWrites: false`** gives significantly better write throughput at the
> cost of a small risk of data loss on a hard crash. For an indexer that can
> always re-fetch data from the beacon node this is an acceptable trade-off.
> Set to `true` if you need strict durability.

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

### 4. Tune Badger for large block workloads

Add a `"Datastore"` → `"Params"` section (Kubo passes these through to the
Badger options):

```json
"Spec": {
  "type": "measure",
  "prefix": "badger.datastore",
  "child": {
    "type": "badgerds",
    "path": "badgerds",
    "syncWrites": false,
    "truncate": true,
    "vlogFileSize": 1073741824,
    "valueThreshold": 1024,
    "numVersionsToKeep": 1,
    "maxTableSize": 67108864
  }
}
```

| Parameter | Value | Rationale |
|-----------|-------|-----------|
| `vlogFileSize` | `1073741824` (1 GiB) | Larger value log files reduce GC pressure for large blobs |
| `valueThreshold` | `1024` | Values ≥ 1 KiB go to the value log; raw 128 KiB blobs always go there |
| `numVersionsToKeep` | `1` | No MVCC history needed; saves space |
| `maxTableSize` | `67108864` (64 MiB) | Larger SST tables improve compaction efficiency |

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

### Badger maintenance

```bash
# Run garbage collection manually (removes unreferenced blocks)
ipfs repo gc

# Check repo size
ipfs repo stat

# Badger compaction runs automatically; to force it:
# Stop the daemon, then:
ipfs repo gc --stream-errors
```

---

## Initial setup

> Before running the generator, complete the [IPFS node setup](#ipfs-node-setup-kubo--badger)
> section above. The IPNS key is created in step 8 of that section.

### 1. Create storage directories

The generator creates directories automatically, but you can pre-create them
with the right permissions:

```bash
mkdir -p /var/lib/blobscan-ipld/mainnet/car
```

### 2. Write a config file

Copy the example and edit the required fields:

```bash
cp config.yaml mainnet.yaml
```

Minimum required fields:

```yaml
network:
  name: mainnet
  beacon_rpc: "http://localhost:5052"
ipfs:
  api_addr: "/ip4/127.0.0.1/tcp/5001"
storage:
  data_dir: "/var/lib/blobscan-ipld/mainnet"
  postgres_dsn: "postgres://user:pass@localhost:5432/blobscan"  # optional
```

### 4. Build

```bash
go build ./cmd/blobscan-ipld
```

---

## Running the generator

### Continuous mode (normal operation)

```bash
./blobscan-ipld -config mainnet.yaml run
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
ExecStart=/usr/local/bin/blobscan-ipld -config /etc/blobscan-ipld/mainnet.yaml run
Restart=on-failure
RestartSec=10s
User=blobscan
StandardOutput=journal
StandardError=journal

[Install]
WantedBy=multi-user.target
```

---

## Backfilling historical epochs

Use the `epoch` subcommand to process a single epoch and exit. This is useful for:
- Filling gaps in the DAG after a downtime.
- Testing the pipeline with a specific epoch.
- Debugging a problematic epoch.

```bash
# Process epoch 300000 only
./blobscan-ipld -config mainnet.yaml -n 300000 epoch
```

To backfill a range of epochs, set `start_epoch` in the config and run in
continuous mode with `skip_existing_epochs: false`. The generator will process
all epochs from `start_epoch` to the current finalized epoch before entering
the normal poll loop.

```yaml
generator:
  start_epoch: 269568   # first EIP-4844 epoch on mainnet
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

The generator logs structured key-value pairs to stderr. Key log lines:

| Event | Level | Key fields |
|-------|-------|-----------|
| Generator started | `INFO` | `network`, `poll_interval` |
| New finalized epochs detected | `INFO` | `from`, `to` |
| Processing epoch | `INFO` | `epoch` |
| Epoch node built | `INFO` | `epoch`, `cid`, `blobs`, `size_bytes` |
| Using DB state backend | `INFO` | — |
| Using file state backend | `INFO` | `path` |
| DB disabled (postgres_dsn not set) | `WARN` | — |
| No new finalized epochs | `DEBUG` | `finalized`, `last_processed` |
| Any processing error | `ERROR` | `err` |

### Example log output

```
time=2024-03-15T10:00:00Z level=INFO msg="generator starting" network=mainnet poll_interval=12s
time=2024-03-15T10:00:01Z level=INFO msg="new finalized epochs detected" from=269568 to=269600
time=2024-03-15T10:00:01Z level=INFO msg="processing epoch" epoch=269568
time=2024-03-15T10:00:03Z level=INFO msg="epoch node built" epoch=269568 cid=bafyrei... blobs=28 size_bytes=3670016
time=2024-03-15T10:00:03Z level=INFO msg="CAR v2 exported" path=/var/lib/blobscan-ipld/mainnet/car/mainnet/269568.car cid=bafyrei...
time=2024-03-15T10:00:04Z level=INFO msg="processing epoch" epoch=269569
...
```

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
