// Package db provides a PostgreSQL client for persisting IPLD CIDs produced
// by the generator. It stores one row per epoch and one row per blob.
package db

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/blobscan/blobscan-ipld/types"
)

// Client wraps a pgxpool connection pool.
type Client struct {
	pool *pgxpool.Pool
}

// New creates a Client connected to the given PostgreSQL DSN and runs
// schema migrations to ensure the required tables exist. If the target
// database does not exist it is created automatically.
func New(ctx context.Context, dsn string) (*Client, error) {
	if err := ensureDatabase(ctx, dsn); err != nil {
		return nil, err
	}

	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return nil, fmt.Errorf("db: connect %q: %w", dsn, err)
	}

	c := &Client{pool: pool}
	if err := c.migrate(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("db: migrate: %w", err)
	}
	return c, nil
}

// Close releases all connections in the pool.
func (c *Client) Close() {
	c.pool.Close()
}

// ensureDatabase connects to the "postgres" maintenance database and creates
// the target database if it does not already exist.
func ensureDatabase(ctx context.Context, dsn string) error {
	cfg, err := pgx.ParseConfig(dsn)
	if err != nil {
		return fmt.Errorf("db: parse dsn: %w", err)
	}
	dbName := cfg.Database
	cfg.Database = "postgres"

	conn, err := pgx.ConnectConfig(ctx, cfg)
	if err != nil {
		return fmt.Errorf("db: connect to postgres maintenance db: %w", err)
	}
	defer conn.Close(ctx) //nolint:errcheck

	var exists bool
	if err := conn.QueryRow(ctx,
		"SELECT EXISTS(SELECT 1 FROM pg_database WHERE datname = $1)", dbName,
	).Scan(&exists); err != nil {
		return fmt.Errorf("db: check database %q: %w", dbName, err)
	}
	if !exists {
		if _, err := conn.Exec(ctx,
			fmt.Sprintf("CREATE DATABASE %s", pgx.Identifier{dbName}.Sanitize()),
		); err != nil {
			return fmt.Errorf("db: create database %q: %w", dbName, err)
		}
	}
	return nil
}

// ─── Schema ───────────────────────────────────────────────────────────────────

const schema = `
CREATE TABLE IF NOT EXISTS ipld_epochs (
    epoch          BIGINT      PRIMARY KEY,
    network        TEXT        NOT NULL,
    slot           BIGINT      NOT NULL,
    epoch_time     TIMESTAMPTZ,
    cid            TEXT        NOT NULL,
    car_path       TEXT        NOT NULL,
    blob_count     INT         NOT NULL,
    size_bytes     BIGINT      NOT NULL,
    created_at     TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS ipld_blobs (
    commitment     TEXT        PRIMARY KEY,
    epoch          BIGINT      NOT NULL REFERENCES ipld_epochs(epoch),
    slot           BIGINT      NOT NULL,
    slot_time      TIMESTAMPTZ,
    network        TEXT        NOT NULL,
    blob_index     INT         NOT NULL,
    data_cid       TEXT        NOT NULL,
    meta_cid       TEXT        NOT NULL,
    versioned_hash TEXT        NOT NULL,
    tx_hash        TEXT        NOT NULL DEFAULT '',
    block_number   BIGINT      NOT NULL DEFAULT 0,
    block_hash     TEXT        NOT NULL DEFAULT '',
    size_bytes     BIGINT      NOT NULL,
    created_at     TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS ipld_blobs_epoch_idx ON ipld_blobs(epoch);

CREATE TABLE IF NOT EXISTS ipld_state (
    key   TEXT   PRIMARY KEY,
    value BIGINT NOT NULL DEFAULT 0
);
`

// migrations runs ALTER TABLE statements that add columns to existing tables.
// Each statement uses IF NOT EXISTS so it is safe to run on every startup.
const migrations = `
ALTER TABLE ipld_epochs ADD COLUMN IF NOT EXISTS epoch_time TIMESTAMPTZ;
ALTER TABLE ipld_blobs  ADD COLUMN IF NOT EXISTS slot_time  TIMESTAMPTZ;
`

func (c *Client) migrate(ctx context.Context) error {
	if _, err := c.pool.Exec(ctx, schema); err != nil {
		return fmt.Errorf("db: apply schema: %w", err)
	}
	if _, err := c.pool.Exec(ctx, migrations); err != nil {
		return fmt.Errorf("db: apply migrations: %w", err)
	}
	return nil
}

// nullTime returns nil when t is zero (maps to SQL NULL) and a pointer
// to t otherwise (maps to TIMESTAMPTZ).
func nullTime(t time.Time) *time.Time {
	if t.IsZero() {
		return nil
	}
	return &t
}

// ─── Write operations ─────────────────────────────────────────────────────────

// SaveEpoch inserts or updates a row in ipld_epochs for the given result.
// epochTime is the timestamp of the epoch's first slot; pass a zero time when
// genesis time is unavailable (stored as NULL).
// It is idempotent: re-processing the same epoch overwrites the previous row.
func (c *Client) SaveEpoch(ctx context.Context, network string, result types.EpochResult, blobCount int, epochTime time.Time) error {
	_, err := c.pool.Exec(ctx, `
		INSERT INTO ipld_epochs (epoch, network, slot, epoch_time, cid, car_path, blob_count, size_bytes)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
		ON CONFLICT (epoch) DO UPDATE SET
			epoch_time = EXCLUDED.epoch_time,
			cid        = EXCLUDED.cid,
			car_path   = EXCLUDED.car_path,
			blob_count = EXCLUDED.blob_count,
			size_bytes = EXCLUDED.size_bytes,
			created_at = NOW()
	`,
		result.Epoch,
		network,
		result.Epoch*32,
		nullTime(epochTime),
		result.CID.String(),
		result.CARPath,
		blobCount,
		result.ApproximateSizeBytes,
	)
	if err != nil {
		return fmt.Errorf("db: save epoch %d: %w", result.Epoch, err)
	}
	return nil
}

// blobsCopyColumns is the column order used by SaveBlobs' CopyFrom.
var blobsCopyColumns = []string{
	"commitment", "epoch", "slot", "slot_time", "network", "blob_index",
	"data_cid", "meta_cid", "versioned_hash", "tx_hash", "block_number",
	"block_hash", "size_bytes",
}

// SaveBlobs inserts or updates one row per blob in ipld_blobs.
// genesisTime is used to compute each blob's slot_time; pass a zero time when
// genesis time is unavailable (slot_time stored as NULL).
// It is idempotent: re-processing the same commitment overwrites the previous row.
//
// Rows are bulk-loaded into a per-tx temp staging table via pgx's binary
// CopyFrom path and then upserted into ipld_blobs with a single INSERT/SELECT,
// turning ~N round-trips into 2 for an epoch with N blobs.
func (c *Client) SaveBlobs(ctx context.Context, network string, epoch uint64, blobs []types.BlobInput, results []types.BlobResult, genesisTime time.Time) error {
	if len(blobs) != len(results) {
		return fmt.Errorf("db: SaveBlobs: blobs/results length mismatch (%d vs %d)", len(blobs), len(results))
	}
	if len(blobs) == 0 {
		return nil
	}

	tx, err := c.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("db: begin tx: %w", err)
	}
	defer tx.Rollback(ctx) //nolint:errcheck

	// Temp table mirrors ipld_blobs' column set (excluding created_at, which
	// defaults to NOW() on the upsert into the real table).
	if _, err := tx.Exec(ctx, `
		CREATE TEMP TABLE ipld_blobs_stage (
			commitment     TEXT        NOT NULL,
			epoch          BIGINT      NOT NULL,
			slot           BIGINT      NOT NULL,
			slot_time      TIMESTAMPTZ,
			network        TEXT        NOT NULL,
			blob_index     INT         NOT NULL,
			data_cid       TEXT        NOT NULL,
			meta_cid       TEXT        NOT NULL,
			versioned_hash TEXT        NOT NULL,
			tx_hash        TEXT        NOT NULL,
			block_number   BIGINT      NOT NULL,
			block_hash     TEXT        NOT NULL,
			size_bytes     BIGINT      NOT NULL
		) ON COMMIT DROP
	`); err != nil {
		return fmt.Errorf("db: create stage table epoch %d: %w", epoch, err)
	}

	rows := make([][]any, len(blobs))
	for i, inp := range blobs {
		res := results[i]
		var slotTime *time.Time
		if !genesisTime.IsZero() {
			t := genesisTime.Add(time.Duration(inp.Slot) * 12 * time.Second)
			slotTime = &t
		}
		rows[i] = []any{
			inp.Commitment,
			int64(epoch),
			int64(inp.Slot),
			slotTime,
			network,
			inp.Index,
			res.DataCID.String(),
			res.MetaCID.String(),
			inp.VersionedHash,
			inp.TxHash,
			int64(inp.BlockNumber),
			inp.BlockHash,
			res.SizeBytes,
		}
	}

	if _, err := tx.CopyFrom(ctx,
		pgx.Identifier{"ipld_blobs_stage"},
		blobsCopyColumns,
		pgx.CopyFromRows(rows),
	); err != nil {
		return fmt.Errorf("db: copy blobs epoch %d: %w", epoch, err)
	}

	// DISTINCT ON deduplicates rows that share the same commitment (e.g. the
	// zero blob appearing multiple times in one epoch). Without it Postgres
	// raises SQLSTATE 21000 when a single INSERT/SELECT would update the same
	// target row twice. ORDER BY commitment is required by DISTINCT ON syntax.
	if _, err := tx.Exec(ctx, `
		INSERT INTO ipld_blobs
			(commitment, epoch, slot, slot_time, network, blob_index, data_cid, meta_cid,
			 versioned_hash, tx_hash, block_number, block_hash, size_bytes)
		SELECT DISTINCT ON (commitment)
		       commitment, epoch, slot, slot_time, network, blob_index, data_cid, meta_cid,
		       versioned_hash, tx_hash, block_number, block_hash, size_bytes
		FROM ipld_blobs_stage
		ORDER BY commitment
		ON CONFLICT (commitment) DO UPDATE SET
			slot_time      = EXCLUDED.slot_time,
			data_cid       = EXCLUDED.data_cid,
			meta_cid       = EXCLUDED.meta_cid,
			versioned_hash = EXCLUDED.versioned_hash,
			tx_hash        = EXCLUDED.tx_hash,
			block_number   = EXCLUDED.block_number,
			block_hash     = EXCLUDED.block_hash,
			created_at     = NOW()
	`); err != nil {
		return fmt.Errorf("db: upsert blobs epoch %d: %w", epoch, err)
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("db: commit blobs tx epoch %d: %w", epoch, err)
	}
	return nil
}

// ─── State backend (implements state.Backend) ─────────────────────────────────

// DBStateBackend wraps a Client and implements state.Backend backed by
// the ipld_epochs table: the last processed epoch is simply MAX(epoch).
// No separate state table is needed.
type DBStateBackend struct {
	client  *Client
	network string
}

// NewDBStateBackend returns a state.Backend backed by the DB for the given network.
func NewDBStateBackend(c *Client, network string) *DBStateBackend {
	return &DBStateBackend{client: c, network: network}
}

// GetLastProcessedEpoch returns the live cursor from ipld_state, falling back
// to MAX(epoch) for deployments that pre-date the ipld_state table.
func (b *DBStateBackend) GetLastProcessedEpoch(ctx context.Context) (uint64, error) {
	var epoch uint64
	err := b.client.pool.QueryRow(ctx, `
		SELECT COALESCE(
			(SELECT value FROM ipld_state WHERE key = $1),
			(SELECT MAX(epoch) FROM ipld_epochs WHERE network = $2),
			0
		)`, "live_"+b.network, b.network,
	).Scan(&epoch)
	if err != nil {
		return 0, fmt.Errorf("db: get last processed epoch: %w", err)
	}
	return epoch, nil
}

// SetLastProcessedEpoch persists the live cursor to ipld_state.
func (b *DBStateBackend) SetLastProcessedEpoch(ctx context.Context, epoch uint64) error {
	_, err := b.client.pool.Exec(ctx, `
		INSERT INTO ipld_state (key, value) VALUES ($1, $2)
		ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value`,
		"live_"+b.network, epoch,
	)
	if err != nil {
		return fmt.Errorf("db: set live cursor %d: %w", epoch, err)
	}
	return nil
}

// GetBackfillCursor returns the backfill goroutine's cursor from ipld_state.
func (b *DBStateBackend) GetBackfillCursor(ctx context.Context) (uint64, error) {
	var epoch uint64
	err := b.client.pool.QueryRow(ctx,
		`SELECT COALESCE((SELECT value FROM ipld_state WHERE key = $1), 0)`,
		"backfill_"+b.network,
	).Scan(&epoch)
	if err != nil {
		return 0, fmt.Errorf("db: get backfill cursor: %w", err)
	}
	return epoch, nil
}

// SetBackfillCursor persists the backfill cursor to ipld_state.
func (b *DBStateBackend) SetBackfillCursor(ctx context.Context, epoch uint64) error {
	_, err := b.client.pool.Exec(ctx, `
		INSERT INTO ipld_state (key, value) VALUES ($1, $2)
		ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value`,
		"backfill_"+b.network, epoch,
	)
	if err != nil {
		return fmt.Errorf("db: set backfill cursor %d: %w", epoch, err)
	}
	return nil
}

// ─── Read operations ──────────────────────────────────────────────────────────

// EpochExists reports whether an epoch has already been saved for the given network.
func (c *Client) EpochExists(ctx context.Context, network string, epoch uint64) (bool, error) {
	var exists bool
	err := c.pool.QueryRow(ctx,
		`SELECT EXISTS(SELECT 1 FROM ipld_epochs WHERE epoch = $1 AND network = $2)`, epoch, network,
	).Scan(&exists)
	if err != nil {
		return false, fmt.Errorf("db: epoch exists %d: %w", epoch, err)
	}
	return exists, nil
}

// EpochRecord is a lightweight view of a saved epoch used for rebuilding the
// NetworkRoot and for CAR export.
type EpochRecord struct {
	Epoch     uint64
	CID       string
	SizeBytes int64
}

// GetAllEpochs returns all saved epochs for a network ordered by epoch number.
func (c *Client) GetAllEpochs(ctx context.Context, network string) ([]EpochRecord, error) {
	rows, err := c.pool.Query(ctx,
		`SELECT epoch, cid, size_bytes FROM ipld_epochs WHERE network = $1 ORDER BY epoch`,
		network,
	)
	if err != nil {
		return nil, fmt.Errorf("db: get all epochs: %w", err)
	}
	defer rows.Close()

	var out []EpochRecord
	for rows.Next() {
		var r EpochRecord
		if err := rows.Scan(&r.Epoch, &r.CID, &r.SizeBytes); err != nil {
			return nil, fmt.Errorf("db: scan epoch row: %w", err)
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

// ─── Summary / analytics queries ─────────────────────────────────────────────

// SummaryStats holds aggregate statistics for a network.
type SummaryStats struct {
	EpochCount         int64
	EmptyEpochCount    int64 // epochs with blob_count = 0
	FirstEpoch         uint64
	LastEpoch          uint64
	ExpectedEpochCount int64 // LastEpoch - FirstEpoch + 1; 0 when no epochs
	GapCount           int64 // ExpectedEpochCount - EpochCount
	TotalBlobs         int64
	TotalSizeBytes     int64
	AvgBlobsPerEpoch   float64
	MaxBlobsPerEpoch   int64
	MaxBlobsEpoch      uint64 // epoch with the most blobs
	FirstEpochTime     *time.Time
	LastEpochTime      *time.Time
	LiveCursor         uint64
	BackfillCursor     uint64
}

// GetSummaryStats returns aggregate statistics for the given network.
func (c *Client) GetSummaryStats(ctx context.Context, network string) (SummaryStats, error) {
	var s SummaryStats

	// Aggregate epoch stats in a single pass.
	err := c.pool.QueryRow(ctx, `
		SELECT
			COUNT(*)                                             AS epoch_count,
			COUNT(*) FILTER (WHERE blob_count = 0)              AS empty_epoch_count,
			COALESCE(MIN(epoch), 0)                              AS first_epoch,
			COALESCE(MAX(epoch), 0)                              AS last_epoch,
			COALESCE(MAX(epoch) - MIN(epoch) + 1, 0)            AS expected_count,
			COALESCE(SUM(blob_count), 0)                         AS total_blobs,
			COALESCE(SUM(size_bytes), 0)                         AS total_size,
			COALESCE(ROUND(AVG(blob_count)::numeric, 1), 0)     AS avg_blobs,
			COALESCE(MAX(blob_count), 0)                         AS max_blobs,
			COALESCE((SELECT epoch FROM ipld_epochs
			           WHERE network = $1
			           ORDER BY blob_count DESC LIMIT 1), 0)    AS max_blobs_epoch,
			MIN(epoch_time)                                      AS first_time,
			MAX(epoch_time)                                      AS last_time
		FROM ipld_epochs WHERE network = $1
	`, network).Scan(
		&s.EpochCount,
		&s.EmptyEpochCount,
		&s.FirstEpoch,
		&s.LastEpoch,
		&s.ExpectedEpochCount,
		&s.TotalBlobs,
		&s.TotalSizeBytes,
		&s.AvgBlobsPerEpoch,
		&s.MaxBlobsPerEpoch,
		&s.MaxBlobsEpoch,
		&s.FirstEpochTime,
		&s.LastEpochTime,
	)
	if err != nil {
		return s, fmt.Errorf("db: summary stats: %w", err)
	}
	s.GapCount = s.ExpectedEpochCount - s.EpochCount

	// State cursors.
	rows, err := c.pool.Query(ctx,
		`SELECT key, value FROM ipld_state WHERE key IN ($1, $2)`,
		"live_"+network, "backfill_"+network,
	)
	if err != nil {
		return s, fmt.Errorf("db: summary cursors: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var key string
		var val uint64
		if err := rows.Scan(&key, &val); err != nil {
			return s, fmt.Errorf("db: summary cursor scan: %w", err)
		}
		switch key {
		case "live_" + network:
			s.LiveCursor = val
		case "backfill_" + network:
			s.BackfillCursor = val
		}
	}
	return s, rows.Err()
}

// GapRange is a contiguous range of missing epochs [Start, End].
type GapRange struct {
	Start uint64
	End   uint64
}

// Count returns the number of missing epochs in the range.
func (g GapRange) Count() uint64 { return g.End - g.Start + 1 }

// GetEpochGapRanges returns all contiguous ranges of epochs absent from the DB
// within [MIN(epoch), MAX(epoch)] for the network, using a LEAD window function.
func (c *Client) GetEpochGapRanges(ctx context.Context, network string) ([]GapRange, error) {
	rows, err := c.pool.Query(ctx, `
		SELECT epoch + 1      AS gap_start,
		       next_epoch - 1 AS gap_end
		FROM (
			SELECT epoch,
			       LEAD(epoch) OVER (ORDER BY epoch) AS next_epoch
			FROM ipld_epochs WHERE network = $1
		) t
		WHERE next_epoch > epoch + 1
		ORDER BY gap_start
	`, network)
	if err != nil {
		return nil, fmt.Errorf("db: epoch gap ranges: %w", err)
	}
	defer rows.Close()

	var out []GapRange
	for rows.Next() {
		var g GapRange
		if err := rows.Scan(&g.Start, &g.End); err != nil {
			return nil, fmt.Errorf("db: epoch gap scan: %w", err)
		}
		out = append(out, g)
	}
	return out, rows.Err()
}

// EpochTopRow is a lightweight view of one epoch used for top-N tables.
type EpochTopRow struct {
	Epoch     uint64
	BlobCount int64
	SizeBytes int64
	EpochTime *time.Time
}

// GetTopEpochsByBlobCount returns the top N epochs with the highest blob counts.
func (c *Client) GetTopEpochsByBlobCount(ctx context.Context, network string, limit int) ([]EpochTopRow, error) {
	rows, err := c.pool.Query(ctx, `
		SELECT epoch, blob_count, size_bytes, epoch_time
		FROM ipld_epochs WHERE network = $1
		ORDER BY blob_count DESC LIMIT $2
	`, network, limit)
	if err != nil {
		return nil, fmt.Errorf("db: top epochs: %w", err)
	}
	defer rows.Close()

	var out []EpochTopRow
	for rows.Next() {
		var r EpochTopRow
		if err := rows.Scan(&r.Epoch, &r.BlobCount, &r.SizeBytes, &r.EpochTime); err != nil {
			return nil, fmt.Errorf("db: top epoch scan: %w", err)
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

// MonthlyBlobStat holds per-month aggregate statistics.
type MonthlyBlobStat struct {
	Month      time.Time
	EpochCount int64
	BlobCount  int64
	SizeBytes  int64
}

// GetMonthlyStats returns per-month blob statistics ordered by month ascending.
// Epochs whose epoch_time is NULL are excluded.
func (c *Client) GetMonthlyStats(ctx context.Context, network string) ([]MonthlyBlobStat, error) {
	rows, err := c.pool.Query(ctx, `
		SELECT DATE_TRUNC('month', epoch_time) AS month,
		       COUNT(*)                         AS epoch_count,
		       SUM(blob_count)                  AS blob_count,
		       SUM(size_bytes)                  AS size_bytes
		FROM ipld_epochs
		WHERE network = $1 AND epoch_time IS NOT NULL
		GROUP BY 1 ORDER BY 1
	`, network)
	if err != nil {
		return nil, fmt.Errorf("db: monthly stats: %w", err)
	}
	defer rows.Close()

	var out []MonthlyBlobStat
	for rows.Next() {
		var m MonthlyBlobStat
		if err := rows.Scan(&m.Month, &m.EpochCount, &m.BlobCount, &m.SizeBytes); err != nil {
			return nil, fmt.Errorf("db: monthly stat scan: %w", err)
		}
		out = append(out, m)
	}
	return out, rows.Err()
}

// GetMaxEpoch returns the highest epoch number stored for a network, or 0 if none.
func (c *Client) GetMaxEpoch(ctx context.Context, network string) (uint64, error) {
	var epoch uint64
	err := c.pool.QueryRow(ctx,
		`SELECT COALESCE(MAX(epoch), 0) FROM ipld_epochs WHERE network = $1`, network,
	).Scan(&epoch)
	if err != nil {
		return 0, fmt.Errorf("db: get max epoch: %w", err)
	}
	return epoch, nil
}

// GetBlobsByEpoch returns all blob records for a given epoch, ordered by blob_index.
type BlobRecord struct {
	Commitment    string
	DataCID       string
	MetaCID       string
	BlobIndex     int
	Slot          uint64
	SlotTime      *time.Time
	VersionedHash string
	TxHash        string
	BlockNumber   uint64
	BlockHash     string
	SizeBytes     int64
}

// BlobRef is a lightweight projection of ipld_blobs used for exporting CID
// references into blobscan's blob_data_storage_reference table.
type BlobRef struct {
	VersionedHash string
	DataCID       string
	MetaCID       string
}

// GetBlobRefs streams all (versioned_hash, data_cid, meta_cid) tuples for
// blobs in the epoch range [fromEpoch, toEpoch]. Results are ordered by epoch
// and blob_index so the output is deterministic.
func (c *Client) GetBlobRefs(ctx context.Context, network string, fromEpoch, toEpoch uint64) ([]BlobRef, error) {
	rows, err := c.pool.Query(ctx,
		`SELECT versioned_hash, data_cid, meta_cid
		 FROM ipld_blobs
		 WHERE network = $1 AND epoch >= $2 AND epoch <= $3
		 ORDER BY epoch, blob_index`,
		network, fromEpoch, toEpoch,
	)
	if err != nil {
		return nil, fmt.Errorf("db: get blob refs [%d,%d]: %w", fromEpoch, toEpoch, err)
	}
	defer rows.Close()

	var out []BlobRef
	for rows.Next() {
		var r BlobRef
		if err := rows.Scan(&r.VersionedHash, &r.DataCID, &r.MetaCID); err != nil {
			return nil, fmt.Errorf("db: scan blob ref: %w", err)
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

// GetEmptyEpochs returns all epoch numbers for the network that have
// blob_count=0 in ipld_epochs and no rows in ipld_blobs — i.e. genuinely
// empty epochs (no blobs on-chain), not corrupted ones.
func (c *Client) GetEmptyEpochs(ctx context.Context, network string) ([]uint64, error) {
	rows, err := c.pool.Query(ctx,
		`SELECT e.epoch
		 FROM ipld_epochs e
		 WHERE e.network = $1 AND e.blob_count = 0
		   AND NOT EXISTS (
		       SELECT 1 FROM ipld_blobs b
		       WHERE b.epoch = e.epoch AND b.network = e.network
		   )
		 ORDER BY e.epoch`,
		network,
	)
	if err != nil {
		return nil, fmt.Errorf("db: get empty epochs: %w", err)
	}
	defer rows.Close()
	var out []uint64
	for rows.Next() {
		var epoch uint64
		if err := rows.Scan(&epoch); err != nil {
			return nil, fmt.Errorf("db: scan empty epoch: %w", err)
		}
		out = append(out, epoch)
	}
	return out, rows.Err()
}

// ─── Health-check queries ───────────────────────────────────────────────────
//
// These return offending rows for the health-check command. Each is a single
// aggregate query filtered by network; callers report counts + a few samples.

// EpochCountMismatch is an epoch whose stored blob_count disagrees with the
// actual number of ipld_blobs rows for that epoch.
type EpochCountMismatch struct {
	Epoch  uint64
	Stored int64 // ipld_epochs.blob_count
	Actual int64 // COUNT(ipld_blobs)
}

// EpochSizeMismatch is an epoch whose stored size_bytes disagrees with the sum
// of its blobs' sizes. Treated as a warning (size is "approximate").
type EpochSizeMismatch struct {
	Epoch  uint64
	Stored int64 // ipld_epochs.size_bytes
	Actual int64 // SUM(ipld_blobs.size_bytes)
}

// EpochAnomaly is a generic per-epoch count of offending blob rows.
type EpochAnomaly struct {
	Epoch uint64
	Count int64
}

// BlobIndexAnomaly describes blob_index irregularities within an epoch:
// duplicate indices (Rows != DistinctIdx) or non-contiguous indices
// (MaxIdx+1 != DistinctIdx for the expected 0-based dense layout).
type BlobIndexAnomaly struct {
	Epoch       uint64
	Rows        int64
	DistinctIdx int64
	MaxIdx      int64
}

// GetBlobCountMismatches returns epochs whose stored blob_count differs from the
// actual ipld_blobs row count in either direction (e.g. the blob_count=0
// corruption, but also any over- or under-count).
func (c *Client) GetBlobCountMismatches(ctx context.Context, network string) ([]EpochCountMismatch, error) {
	rows, err := c.pool.Query(ctx,
		`SELECT e.epoch, e.blob_count, COUNT(b.commitment)
		 FROM ipld_epochs e
		 LEFT JOIN ipld_blobs b ON b.epoch = e.epoch AND b.network = e.network
		 WHERE e.network = $1
		 GROUP BY e.epoch, e.blob_count
		 HAVING e.blob_count <> COUNT(b.commitment)
		 ORDER BY e.epoch`,
		network,
	)
	if err != nil {
		return nil, fmt.Errorf("db: get blob_count mismatches: %w", err)
	}
	defer rows.Close()
	var out []EpochCountMismatch
	for rows.Next() {
		var m EpochCountMismatch
		if err := rows.Scan(&m.Epoch, &m.Stored, &m.Actual); err != nil {
			return nil, fmt.Errorf("db: scan blob_count mismatch: %w", err)
		}
		out = append(out, m)
	}
	return out, rows.Err()
}

// GetSizeMismatches returns epochs whose stored size_bytes differs from the sum
// of their blobs' size_bytes.
func (c *Client) GetSizeMismatches(ctx context.Context, network string) ([]EpochSizeMismatch, error) {
	rows, err := c.pool.Query(ctx,
		`SELECT e.epoch, e.size_bytes, COALESCE(SUM(b.size_bytes), 0)
		 FROM ipld_epochs e
		 LEFT JOIN ipld_blobs b ON b.epoch = e.epoch AND b.network = e.network
		 WHERE e.network = $1
		 GROUP BY e.epoch, e.size_bytes
		 HAVING e.size_bytes <> COALESCE(SUM(b.size_bytes), 0)
		 ORDER BY e.epoch`,
		network,
	)
	if err != nil {
		return nil, fmt.Errorf("db: get size mismatches: %w", err)
	}
	defer rows.Close()
	var out []EpochSizeMismatch
	for rows.Next() {
		var m EpochSizeMismatch
		if err := rows.Scan(&m.Epoch, &m.Stored, &m.Actual); err != nil {
			return nil, fmt.Errorf("db: scan size mismatch: %w", err)
		}
		out = append(out, m)
	}
	return out, rows.Err()
}

// GetNetworkMismatches returns epochs that have blob rows whose network column
// disagrees with the epoch's network. Because ipld_epochs.epoch is the PK alone
// (globally unique), the FK can be satisfied by an epoch row of another network;
// this catches that.
func (c *Client) GetNetworkMismatches(ctx context.Context, network string) ([]EpochAnomaly, error) {
	rows, err := c.pool.Query(ctx,
		`SELECT b.epoch, COUNT(*)
		 FROM ipld_blobs b
		 JOIN ipld_epochs e ON e.epoch = b.epoch
		 WHERE e.network = $1 AND b.network <> e.network
		 GROUP BY b.epoch
		 ORDER BY b.epoch`,
		network,
	)
	if err != nil {
		return nil, fmt.Errorf("db: get network mismatches: %w", err)
	}
	defer rows.Close()
	return scanEpochAnomalies(rows, "network mismatch")
}

// GetOrphanBlobs returns epochs whose blobs reference no matching
// (epoch, network) row in ipld_epochs.
func (c *Client) GetOrphanBlobs(ctx context.Context, network string) ([]EpochAnomaly, error) {
	rows, err := c.pool.Query(ctx,
		`SELECT b.epoch, COUNT(*)
		 FROM ipld_blobs b
		 WHERE b.network = $1
		   AND NOT EXISTS (
		       SELECT 1 FROM ipld_epochs e
		       WHERE e.epoch = b.epoch AND e.network = b.network
		   )
		 GROUP BY b.epoch
		 ORDER BY b.epoch`,
		network,
	)
	if err != nil {
		return nil, fmt.Errorf("db: get orphan blobs: %w", err)
	}
	defer rows.Close()
	return scanEpochAnomalies(rows, "orphan blobs")
}

func scanEpochAnomalies(rows pgx.Rows, what string) ([]EpochAnomaly, error) {
	var out []EpochAnomaly
	for rows.Next() {
		var a EpochAnomaly
		if err := rows.Scan(&a.Epoch, &a.Count); err != nil {
			return nil, fmt.Errorf("db: scan %s: %w", what, err)
		}
		out = append(out, a)
	}
	return out, rows.Err()
}

// GetBlobIndexAnomalies returns epochs whose blob_index values are not a clean
// 0-based dense set: either duplicated (Rows != DistinctIdx) or with gaps
// (MaxIdx+1 != DistinctIdx). Callers classify duplicates as FAIL, gaps as WARN.
func (c *Client) GetBlobIndexAnomalies(ctx context.Context, network string) ([]BlobIndexAnomaly, error) {
	rows, err := c.pool.Query(ctx,
		`SELECT epoch,
		        COUNT(*)                   AS rows,
		        COUNT(DISTINCT blob_index) AS distinct_idx,
		        MAX(blob_index)            AS max_idx
		 FROM ipld_blobs
		 WHERE network = $1
		 GROUP BY epoch
		 HAVING COUNT(*) <> COUNT(DISTINCT blob_index)
		     OR MAX(blob_index) + 1 <> COUNT(DISTINCT blob_index)
		 ORDER BY epoch`,
		network,
	)
	if err != nil {
		return nil, fmt.Errorf("db: get blob_index anomalies: %w", err)
	}
	defer rows.Close()
	var out []BlobIndexAnomaly
	for rows.Next() {
		var a BlobIndexAnomaly
		if err := rows.Scan(&a.Epoch, &a.Rows, &a.DistinctIdx, &a.MaxIdx); err != nil {
			return nil, fmt.Errorf("db: scan blob_index anomaly: %w", err)
		}
		out = append(out, a)
	}
	return out, rows.Err()
}

func (c *Client) GetBlobsByEpoch(ctx context.Context, network string, epoch uint64) ([]BlobRecord, error) {
	rows, err := c.pool.Query(ctx,
		`SELECT commitment, data_cid, meta_cid, blob_index,
		        slot, slot_time, versioned_hash, tx_hash, block_number, block_hash, size_bytes
		 FROM ipld_blobs WHERE network = $1 AND epoch = $2 ORDER BY blob_index`,
		network, epoch,
	)
	if err != nil {
		return nil, fmt.Errorf("db: get blobs epoch %d: %w", epoch, err)
	}
	defer rows.Close()

	var out []BlobRecord
	for rows.Next() {
		var r BlobRecord
		if err := rows.Scan(&r.Commitment, &r.DataCID, &r.MetaCID, &r.BlobIndex,
			&r.Slot, &r.SlotTime, &r.VersionedHash, &r.TxHash, &r.BlockNumber, &r.BlockHash, &r.SizeBytes); err != nil {
			return nil, fmt.Errorf("db: scan blob row: %w", err)
		}
		out = append(out, r)
	}
	return out, rows.Err()
}
