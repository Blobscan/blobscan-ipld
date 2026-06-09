# Configuration Reference

All configuration is read from environment variables at startup. No config file
is needed. Variables that are unset or empty fall back to the listed default.

---

## Network

| Variable | Required | Default | Description |
|----------|----------|---------|-------------|
| `NETWORK_NAME` | **yes** | — | Network identifier: `mainnet`, `sepolia`, `gnosis`, `hoodi` |
| `BEACON_RPC` | conditional | `""` | Beacon Node REST API base URL (set by compose files from network-specific `{MAINNET,SEPOLIA,HOODI}_BEACON_RPC`). Required for `run` / `epoch` subcommands; not needed for `serve` |
| `BEACON_TIMEOUT` | no | `60s` | HTTP request timeout for all Beacon Node API calls |
| `BEACON_RATE_LIMIT` | no | `100` | Max requests/second to beacon RPC. Set to ~80-90% of provider limit (Quicknode: 125 req/s → use `100`) |
| `BEACON_RATE_BURST` | no | `32` | Token bucket burst size. Controls how many requests can fire at once |
| `BEACON_429_BACKOFF` | no | `1s` | Initial backoff when a 429 error is received. Doubles on each consecutive 429, capped at 60s |

---

## Storage

| Variable | Required | Default | Description |
|----------|----------|---------|-------------|
| `DATA_DIR` | **yes** | — | Root directory for all generated files. The state JSON file is written here as `<network>-state.json` |
| `CAR_DIR` | no | `$DATA_DIR/car` | Directory for exported CAR v2 files |
| `POSTGRES_DSN` | no | `""` | PostgreSQL connection string, e.g. `postgres://user:pass@localhost:5432/ipld_mainnet?sslmode=disable`. When empty, DB persistence is disabled |

---

## IPFS

| Variable | Required | Default | Description |
|----------|----------|---------|-------------|
| `IPFS_API_ADDR` | conditional | — | IPFS HTTP RPC address. Accepts multiaddr (`/ip4/127.0.0.1/tcp/5001`) or plain URL. Required unless `IPFS_SKIP_UPLOAD=true` |
| `IPFS_PIN_ON_ADD` | no | `true` | Recursively pins each epoch CID after uploading its blocks. On by default so an archival node's blocks are protected from `ipfs repo gc`. Set to `false` to opt out |
| `IPFS_TIMEOUT` | no | `30s` | HTTP request timeout for all IPFS API calls |
| `IPFS_SKIP_UPLOAD` | no | `false` | If `true`, skip all IPFS interaction. CIDs are still computed and saved to DB. `IPFS_API_ADDR` not required |
| `IPFS_UPLOAD_WORKERS` | no | `16` | Parallel `block/put` requests per epoch. Lower if local IPFS saturates; raise for remote high-latency endpoints |

---

## Generator

| Variable | Required | Default | Description |
|----------|----------|---------|-------------|
| `GENERATOR_WORKERS` | no | `16` | Parallel blob-processing goroutines. CID hashing is CPU-bound; set near vCPU count |
| `GENERATOR_BEACON_WORKERS` | no | `16` | Parallel slot fetches per epoch. Each slot is one HTTP request; global cap is `BEACON_RATE_LIMIT` |
| `BACKFILL_EPOCH_WORKERS` | no | `4` | Parallel epoch builders in the backfill pipeline. Each worker fetches+builds one epoch concurrently; the shared `BEACON_RATE_LIMIT` prevents exceeding RPC quotas. Increase for faster backfill on local nodes; decrease if hitting rate limits |
| `GENERATOR_POLL_INTERVAL` | no | per-network | How often to query the beacon node for new finalized epochs. Defaults to one slot duration: `12s` for Ethereum networks, `5s` for Gnosis |
| `GENERATOR_START_EPOCH` | no | network default | First epoch to process when starting from scratch. Defaults to the Dencun fork epoch for known networks (see table below). Set explicitly to resume from a specific epoch |
| `GENERATOR_HAMT_THRESHOLD` | no | `5000` | If a single epoch has this many blobs or more, the blob index uses HAMT shards instead of a flat map |
| `GENERATOR_SKIP_EXISTING_EPOCHS` | no | `false` | If `true` and a state file exists, start from `last_processed_epoch + 1` instead of `GENERATOR_START_EPOCH` |
| `GENERATOR_API_LISTEN` | no | `""` | Address for the HTTP push API (e.g. `:8080`). Empty = disabled |
| `NETWORK_ROOT_PAGE_SIZE` | no | `10000` | Max epochs per NetworkRoot page. Must be ≥ 1000 |

### Default `GENERATOR_START_EPOCH` by network

When `GENERATOR_START_EPOCH` is unset, the generator automatically uses the
first epoch containing blob sidecars for the configured network:

| `NETWORK_NAME` | Default start epoch | Fork / date |
|----------------|---------------------|-------------|
| `mainnet`      | `269568`            | Dencun, Mar 13 2024 |
| `sepolia`      | `132608`            | Dencun, Jan 30 2024 |
| `gnosis`       | `889856`            | Dencun, Mar 11 2024 |
| `hoodi`        | `0`                 | Launched post-Deneb |

For any other network name the value stays `0`; set `GENERATOR_START_EPOCH`
explicitly if genesis-era slots predate EIP-4844.

### Consensus layer chain parameters by network

These are applied automatically from `NETWORK_NAME`. They affect slot-to-time
conversion, epoch-to-slot arithmetic, and the `GENERATOR_POLL_INTERVAL` default:

| `NETWORK_NAME` | Slots per epoch | Seconds per slot |
|----------------|-----------------|------------------|
| `mainnet`      | 32              | 12               |
| `sepolia`      | 32              | 12               |
| `hoodi`        | 32              | 12               |
| `gnosis`       | **16**          | **5**            |

---

## Blobscan API (optional)

| Variable | Required | Default | Description |
|----------|----------|---------|-------------|
| `BLOBSCAN_API_URL` | no | `""` | Blobscan REST API base URL for reporting CID references, e.g. `http://blobscan:3001` |
| `BLOBSCAN_API_KEY` | no | `""` | API key — value of `IPFS_STORAGE_API_KEY` in blobscan |

---

## Sentry (optional)

| Variable | Required | Default | Description |
|----------|----------|---------|-------------|
| `SENTRY_DSN` | no | `""` | Sentry DSN. Leave empty to disable error tracking |
| `SENTRY_ENVIRONMENT` | no | `$NETWORK_NAME` | Sentry environment tag (e.g. `mainnet`, `sepolia`, `production`) |
| `SENTRY_RELEASE` | no | `""` | Release string sent with events, e.g. `v1.2.3` or a git SHA |
| `SENTRY_SAMPLE_RATE` | no | `1.0` | Traces sample rate (0–1). `1.0` captures every event |

When `SENTRY_DSN` is set, all `ERROR`-level log entries are automatically forwarded to Sentry. Sentry is flushed with a 2-second timeout on shutdown.

---

## Validation rules

The process exits at startup if any of these are violated:

- `NETWORK_NAME` — must be non-empty
- `IPFS_API_ADDR` — must be non-empty **unless** `IPFS_SKIP_UPLOAD=true`
- `DATA_DIR` — must be non-empty
- `NETWORK_ROOT_PAGE_SIZE` — must be ≥ 1000 if set

Conditionally required at the subcommand level (validated at runtime, not startup):

- `BEACON_RPC` — required for `run` and `epoch` subcommands
- `POSTGRES_DSN` — required for `export-car`, `export-car-range`, `finalize-epoch`, `backfill-ipfs`

---

## Duration format

Go duration strings are accepted: `12s`, `1m`, `1h`, `24h`, `500ms`.

---

## Example

See `.env.example` in the project root for a fully annotated template.
