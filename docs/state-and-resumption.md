# State and Resumption

## Overview

The generator tracks its progress through a `state.Backend` interface. Two
backends are supported, selected automatically at startup:

| Condition | Backend used |
|-----------|-------------|
| `storage.postgres_dsn` is set | **DB backend** — cursors stored in `ipld_state` table |
| `storage.postgres_dsn` is empty | **File backend** — cursors stored in a JSON file in `storage.data_dir` |

Both backends expose four operations:
- `GetLastProcessedEpoch` / `SetLastProcessedEpoch` — live goroutine cursor
- `GetBackfillCursor` / `SetBackfillCursor` — backfill goroutine cursor

---

## Parallel backfill and live processing

When `postgres_dsn` is configured and there is a historical gap between
`start_epoch` and the current finalized tip, the `run` subcommand launches two
goroutines:

| Goroutine | What it processes | Cursor used |
|-----------|------------------|-------------|
| **live** | newly finalized epochs (current tip onward) | `live_<network>` in `ipld_state` |
| **backfill** | historical epochs from `start_epoch` to the live anchor | `backfill_<network>` in `ipld_state` |

Both goroutines write to the same `ipld_epochs` / `ipld_blobs` tables and IPFS
node. All writes are idempotent — if a live epoch is written before the backfill
reaches it, the backfill skips it via `EpochExists` and just advances its cursor.

The NetworkRoot is rebuilt after every live epoch, and once more when the backfill
finishes.

**First run behavior:**

On a fresh database, the live cursor is initialized to `currentFinalized - 1` so
that the live goroutine starts at the current tip. The backfill goroutine covers
`[start_epoch, currentFinalized - 2]` concurrently.

**Resume behavior:**

On restart both cursors are read from `ipld_state` and each goroutine resumes
exactly where it left off.

**No-DB fallback:**

When PostgreSQL is not configured, parallel mode is not available. The generator
falls back to the original single-goroutine sequential loop.

---

## DB backend (recommended)

### `ipld_state` table

```sql
CREATE TABLE IF NOT EXISTS ipld_state (
    key   TEXT   PRIMARY KEY,
    value BIGINT NOT NULL DEFAULT 0
);
```

Keys used:

| Key | Description |
|-----|-------------|
| `live_<network>` | live goroutine cursor |
| `backfill_<network>` | backfill goroutine cursor |

**Migration from older deployments:**

`GetLastProcessedEpoch` falls back to `MAX(epoch) FROM ipld_epochs` if no
`live_<network>` row exists yet, so upgrades from pre-parallel releases require
no manual migration.

**Rollback / reset:**

To roll back to epoch N, delete all rows with `epoch > N` and reset the cursors:

```sql
DELETE FROM ipld_epochs WHERE network = 'mainnet' AND epoch > 1500;
DELETE FROM ipld_blobs  WHERE epoch > 1500;
DELETE FROM ipld_state  WHERE key IN ('live_mainnet', 'backfill_mainnet');
```

To reset completely:

```sql
TRUNCATE ipld_epochs, ipld_blobs, ipld_state;
```

---

## File backend (fallback, no DB)

When `postgres_dsn` is not set, progress is stored in a JSON file.

### State file location

```
<storage.data_dir>/<network>-state.json
```

### State file format

```json
{
  "network": "mainnet",
  "last_processed_epoch": 269999,
  "backfill_cursor": 269750
}
```

| Field | Description |
|-------|-------------|
| `network` | Network name from config |
| `last_processed_epoch` | Live goroutine cursor |
| `backfill_cursor` | Backfill goroutine cursor (0 if backfill never ran) |

### How state is written

All writes are **atomic**: the manager writes to `<path>.tmp` then calls
`os.Rename`. A crash mid-write leaves the previous state file intact.

State is written **after each epoch** in each goroutine independently.

### Manual state editing

The state file is plain JSON and can be edited while the generator is stopped.

**Roll back to a previous epoch:**

```json
{ "network": "mainnet", "last_processed_epoch": 1500, "backfill_cursor": 0 }
```

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

- **DB backend**: filtered by the `network` column in `ipld_epochs` and the
  `_<network>` suffix in `ipld_state` keys.
- **File backend**: separate file per network (`<network>-state.json`).
