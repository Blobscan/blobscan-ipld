# State and Resumption

## Overview

The generator tracks its progress through a `state.Backend` interface. Two
backends are supported, selected automatically at startup:

| Condition | Backend used |
|-----------|-------------|
| `storage.postgres_dsn` is set | **DB backend** — progress derived from `MAX(epoch)` in `ipld_epochs` |
| `storage.postgres_dsn` is empty | **File backend** — progress stored in a JSON file in `storage.data_dir` |

Both backends expose the same two operations:
- `GetLastProcessedEpoch` — return the highest fully-processed epoch
- `SetLastProcessedEpoch` — advance the progress marker

---

## DB backend (recommended)

When PostgreSQL is configured, no separate state file is needed.

**How it works:**

- `GetLastProcessedEpoch` runs `SELECT MAX(epoch) FROM ipld_epochs WHERE network = $1`.
- `SetLastProcessedEpoch` is a **no-op** — the epoch row written by `SaveEpoch`
  inside the same processing step already serves as the progress marker.
  There is no separate write, so progress update and epoch persistence are
  inherently atomic.

**Resume on restart:**

The generator queries `MAX(epoch)` and resumes from `max + 1`. There is no
state file to manage, back up, or corrupt.

**Rollback / reset:**

To roll back to epoch N, delete all rows with `epoch > N`:

```sql
DELETE FROM ipld_epochs WHERE network = 'mainnet' AND epoch > 1500;
DELETE FROM ipld_blobs  WHERE epoch > 1500;
```

Restart the generator; it will resume from epoch 1501.

To reset completely:

```sql
TRUNCATE ipld_epochs, ipld_blobs;
```

---

## File backend (fallback, no DB)

When `postgres_dsn` is not set, progress is stored in a JSON file.

### State file location

```
<storage.data_dir>/<network>-state.json
```

Examples:
- `/var/lib/blobscan-ipld/mainnet/mainnet-state.json`
- `/var/lib/blobscan-ipld/sepolia/sepolia-state.json`

### State file format

```json
{
  "network": "mainnet",
  "last_processed_epoch": 269999
}
```

| Field | Description |
|-------|-------------|
| `network` | Network name from config |
| `last_processed_epoch` | Highest epoch that has been fully processed |

### How state is written

All writes are **atomic**: the manager writes to `<path>.tmp` then calls
`os.Rename`. A crash mid-write leaves the previous state file intact.

State is written **after each epoch** — once the blob DAG is built and uploaded
to IPFS, `last_processed_epoch` is updated.

### Resume on restart

The generator reads `last_processed_epoch` from the file. With
`skip_existing_epochs: true` (recommended) it resumes from `last + 1`.
With `skip_existing_epochs: false` it always starts from `start_epoch` in
the config, regardless of the state file.

### Manual state editing

The state file is plain JSON and can be edited while the generator is stopped.

**Roll back to a previous epoch:**

```json
{ "network": "mainnet", "last_processed_epoch": 1500 }
```

The generator will re-process epochs from 1501.

**Reset completely:** delete the state file. The generator starts from
`start_epoch` in the config.

---

## Consistency guarantee

In both backends, if the generator crashes after uploading blocks to IPFS but
before recording progress, the epoch will be **re-processed on the next run**.
Because blob CIDs are content-addressed (sha2-256 of the raw data), re-processing
the same blobs produces the **identical CIDs** — no duplicate or inconsistent data
is written to IPFS or the DB (`ON CONFLICT DO UPDATE` ensures idempotency).

---

## State and multiple networks

Each network has its own isolated state:

- **DB backend**: filtered by the `network` column in `ipld_epochs`.
- **File backend**: separate file per network (`<network>-state.json`).

Running two instances for different networks requires two separate config files
with different `storage.data_dir` paths (file backend) or the same DB with
different `network.name` values (DB backend).
