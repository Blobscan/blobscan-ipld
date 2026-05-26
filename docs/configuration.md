# Configuration Reference

Configuration is loaded from a YAML file passed via the `-config` flag
(default: `config.yaml`). The file is read once at startup; changes require a
restart.

Required fields are marked **required**. All other fields have defaults applied
automatically after parsing.

---

## `network`

Identifies the Ethereum network being indexed.

| Field | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| `name` | string | **yes** | — | Network identifier used in node fields, file paths, and the state file name. E.g. `"mainnet"`, `"sepolia"`, `"holesky"` |
| `beacon_rpc` | string | no | `""` | Base URL of the Beacon Node REST API. E.g. `"http://localhost:5052"`. Required for beacon-pull mode (`run` / `epoch` subcommands); not needed for the push API (`serve`) |
| `beacon_timeout` | duration | no | `60s` | HTTP request timeout for all Beacon Node API calls |
| `beacon_rate_limit` | float | no | `100` | Maximum requests per second to the beacon RPC. Set this to ~80-90% of your provider's limit to avoid HTTP 429 errors. E.g., use `100` for Quicknode's 125 req/s limit |
| `beacon_rate_burst` | int | no | `10` | Token bucket burst size for rate limiting. Controls how many requests can be sent at once before backoff applies |
| `beacon_429_backoff` | duration | no | `1s` | Initial backoff duration when a 429 (rate limit) error is received. Doubles on each consecutive 429, capped at 60s |

---

## `ipfs`

Connection settings for the IPFS node (Kubo-compatible).

| Field | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| `api_addr` | string | conditional | — | IPFS HTTP RPC address. Accepts multiaddr (`/ip4/127.0.0.1/tcp/5001`) or plain URL (`http://127.0.0.1:5001`). Required unless `skip_upload: true` |
| `pin_on_add` | bool | no | `false` | If `true`, recursively pins each epoch CID after uploading its blocks |
| `timeout` | duration | no | `30s` | HTTP request timeout for all IPFS API calls |
| `skip_upload` | bool | no | `false` | If `true`, skip all IPFS interaction. CIDs are still computed deterministically and saved to the database. Useful for DB-only indexing without running an IPFS node. When set, `api_addr` is not required |
| `upload_workers` | int | no | `16` | Number of parallel `block/put` requests per epoch. The HTTP transport sizes its keepalive pool to match, so increasing this fans uploads out across many connections. Lower if the local IPFS daemon saturates; raise for high-latency remote IPFS endpoints. Override via env `IPFS_UPLOAD_WORKERS=N` |

---

## `ipns`

Controls the IPNS record published after each range is finalized.

| Field | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| `key_name` | string | **yes** | — | Name of the key in the IPFS keystore to publish under. Create with `ipfs key gen --type=ed25519 <name>` |
| `ttl` | duration | no | `1h` | IPNS record TTL hint for resolvers (how long they may cache the record) |
| `lifetime` | duration | no | `24h` | IPNS record validity lifetime. The record must be re-published before this expires |

---

## `storage`

Local filesystem paths and database connection.

| Field | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| `data_dir` | string | **yes** | — | Root directory for all generated files. The state JSON file is written here as `<network>-state.json` |
| `postgres_dsn` | string | no | `""` | PostgreSQL connection string, e.g. `"postgres://user:pass@localhost:5432/blobscan"`. When empty, DB persistence is disabled and some features are unavailable (see `docs/api.md` for the feature matrix) |
| `car_dir` | string | no | `<data_dir>/car` | Directory for exported CAR v2 files. Files are written to `<car_dir>/<network>/<firstEpoch>-<lastEpoch>.car` |

---

## `generator`

Controls DAG generation behaviour.

| Field | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| `epochs_per_range` | int | no | `1000` | Number of epochs to accumulate before building a `RangeNode` and exporting a CAR file |
| `hamt_threshold` | int | no | `5000` | If a single epoch contains this many blobs or more, the blob index uses HAMT shards instead of a flat map |
| `poll_interval` | duration | no | `12s` | How often to query the beacon node for new finalized epochs. One Ethereum slot is 12 s; one epoch is 6.4 min |
| `start_epoch` | uint64 | no | network default | First epoch to process when starting from scratch. When `0` (not set), the Deneb fork epoch is applied automatically for known networks (see table below). Override explicitly to resume from a specific epoch. |
| `workers` | int | no | `16` | Number of goroutines in the parallel blob-processing worker pool per epoch. CID hashing is CPU-bound on 131 KB blobs, so a value near vCPU count is a good default. Override via env `GENERATOR_WORKERS` |
| `beacon_workers` | int | no | `16` | Number of slots fetched in parallel per epoch. Each slot is one HTTP request to the beacon node; the global cap is `network.beacon_rate_limit`. Override via env `GENERATOR_BEACON_WORKERS` |
| `skip_existing_epochs` | bool | no | `false` | If `true` and a state file exists, start from `last_processed_epoch + 1` instead of `start_epoch` |

---

## Full annotated example

```yaml
network:
  name: mainnet
  beacon_rpc: "http://localhost:5052"
  beacon_timeout: 60s
  beacon_rate_limit: 100     # Adjust to ~80-90% of your RPC provider's limit
  beacon_rate_burst: 10
  beacon_429_backoff: 1s

ipfs:
  api_addr: "/ip4/127.0.0.1/tcp/5001"
  pin_on_add: true
  timeout: 30s

ipns:
  key_name: "blobscan-mainnet"
  ttl: 1h
  lifetime: 24h

storage:
  data_dir: "/var/lib/blobscan-ipld/mainnet"
  postgres_dsn: "postgres://blobscan:secret@localhost:5432/blobscan"
  # car_dir and blockstore default to subdirectories of data_dir

generator:
  epochs_per_range: 1000
  hamt_threshold: 5000
  poll_interval: 12s
  # start_epoch defaults to 269568 for mainnet; set explicitly to override
  workers: 8
  skip_existing_epochs: true
```

---

## Push-API-only example (no beacon node, no DB)

Minimal configuration for accepting blobs via the HTTP push API and storing
them in IPFS only. No beacon node or PostgreSQL is required. Epoch
finalization is done with `finalize: true` on the last `POST /blob`.

```yaml
network:
  name: mainnet

ipfs:
  api_addr: "/ip4/127.0.0.1/tcp/5001"
  timeout: 30s

storage:
  data_dir: "/var/lib/blobscan-ipld/mainnet"
  # postgres_dsn omitted — DB persistence disabled

generator:
  api_listen: ":8080"
  hamt_threshold: 5000
```

> **Features unavailable without `postgres_dsn`:** `export-car`,
> `export-car-range`, `finalize-epoch`, `NetworkRoot` rebuild.
> Use `finalize: true` on the last `POST /blob` to build the `EpochNode`
> in-request.

---

## Sepolia example

```yaml
network:
  name: sepolia
  beacon_rpc: "http://localhost:5052"
  beacon_timeout: 60s
  beacon_rate_limit: 100     # Adjust to ~80-90% of your RPC provider's limit
  beacon_rate_burst: 10
  beacon_429_backoff: 1s

ipfs:
  api_addr: "/ip4/127.0.0.1/tcp/5001"
  pin_on_add: false
  timeout: 30s

ipns:
  key_name: "blobscan-sepolia"
  ttl: 1h
  lifetime: 24h

storage:
  data_dir: "/var/lib/blobscan-ipld/sepolia"

generator:
  epochs_per_range: 1000
  hamt_threshold: 5000
  poll_interval: 12s
  # start_epoch defaults to 132608 for sepolia
  workers: 4
  skip_existing_epochs: true
```

### Default `start_epoch` by network

When `start_epoch` is `0` (the default), the generator automatically uses the
first epoch that contains blob sidecars for the configured network:

| `network.name` | Default `start_epoch` | Fork / date |
|----------------|-----------------------|-------------|
| `mainnet`      | `269568`              | Dencun, Mar 13 2024 |
| `sepolia`      | `132608`              | Dencun, Jan 30 2024 |
| `gnosis`       | `889856`              | Dencun, Mar 11 2024 |
| `hoodi`        | `0`                   | Launched post-Deneb |

For any other network name the value stays `0` and you must set `start_epoch`
explicitly if genesis-era slots predate EIP-4844.

---

## Environment variable overrides

Several fields can be overridden at runtime via environment variables without
editing the YAML file. Environment variables take precedence over the file.

| Variable | Config field overridden |
|----------|------------------------|
| `NETWORK_NAME` | `network.name` |
| `BEACON_RPC` | `network.beacon_rpc` |
| `BEACON_TIMEOUT` | `network.beacon_timeout` |
| `BEACON_RATE_LIMIT` | `network.beacon_rate_limit` |
| `BEACON_RATE_BURST` | `network.beacon_rate_burst` |
| `BEACON_429_BACKOFF` | `network.beacon_429_backoff` |
| `POSTGRES_DSN` | `storage.postgres_dsn` |
| `IPFS_API_ADDR` | `ipfs.api_addr` |
| `IPFS_SKIP_UPLOAD` | `ipfs.skip_upload` (set to `true` or `1` to enable) |
| `IPFS_UPLOAD_WORKERS` | `ipfs.upload_workers` |
| `GENERATOR_WORKERS` | `generator.workers` |
| `GENERATOR_BEACON_WORKERS` | `generator.beacon_workers` |

This is the mechanism used by the Docker Compose files — a single `config.yaml`
is shared across networks and the per-deployment values are injected via the
container environment.

---

## Validation rules

The following fields are validated at startup; the process exits with a clear
error if any are missing:

- `network.name` — must be non-empty
- `ipfs.api_addr` — must be non-empty **unless** `ipfs.skip_upload: true`
- `storage.data_dir` — must be non-empty

Conditionally required:

- `network.beacon_rpc` — required for `run` and `epoch` subcommands; optional for `serve`
- `storage.postgres_dsn` — optional in all modes; omitting it disables DB persistence
  and the following features: `export-car`, `export-car-range`, `finalize-epoch`,
  `NetworkRoot` rebuild, and `total_blob_count` auto-finalization

All other fields are optional and receive defaults silently.

---

## Duration format

Go duration strings are accepted: `12s`, `1m`, `1h`, `24h`, `500ms`.
