// Package db provides a PostgreSQL client for persisting IPLD CIDs produced
// by the generator. It stores one row per epoch and one row per blob.
package db

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/blobscan/blobscan-ipld/types"
)

// Client wraps a pgxpool connection pool.
type Client struct {
	pool *pgxpool.Pool
}

// New creates a Client connected to the given PostgreSQL DSN and runs
// schema migrations to ensure the required tables exist.
func New(ctx context.Context, dsn string) (*Client, error) {
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

// ─── Schema ───────────────────────────────────────────────────────────────────

const schema = `
CREATE TABLE IF NOT EXISTS ipld_epochs (
    epoch          BIGINT      PRIMARY KEY,
    network        TEXT        NOT NULL,
    slot           BIGINT      NOT NULL,
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
`

func (c *Client) migrate(ctx context.Context) error {
	_, err := c.pool.Exec(ctx, schema)
	if err != nil {
		return fmt.Errorf("db: apply schema: %w", err)
	}
	return nil
}

// ─── Write operations ─────────────────────────────────────────────────────────

// SaveEpoch inserts or updates a row in ipld_epochs for the given result.
// It is idempotent: re-processing the same epoch overwrites the previous row.
func (c *Client) SaveEpoch(ctx context.Context, network string, result types.EpochResult, blobCount int) error {
	_, err := c.pool.Exec(ctx, `
		INSERT INTO ipld_epochs (epoch, network, slot, cid, car_path, blob_count, size_bytes)
		VALUES ($1, $2, $3, $4, $5, $6, $7)
		ON CONFLICT (epoch) DO UPDATE SET
			cid        = EXCLUDED.cid,
			car_path   = EXCLUDED.car_path,
			blob_count = EXCLUDED.blob_count,
			size_bytes = EXCLUDED.size_bytes,
			created_at = NOW()
	`,
		result.Epoch,
		network,
		result.Epoch*32, // first slot of epoch
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

// SaveBlobs inserts or updates one row per blob in ipld_blobs.
// It is idempotent: re-processing the same commitment overwrites the previous row.
func (c *Client) SaveBlobs(ctx context.Context, network string, epoch uint64, blobs []types.BlobInput, results []types.BlobResult) error {
	if len(blobs) != len(results) {
		return fmt.Errorf("db: SaveBlobs: blobs/results length mismatch (%d vs %d)", len(blobs), len(results))
	}

	tx, err := c.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("db: begin tx: %w", err)
	}
	defer tx.Rollback(ctx) //nolint:errcheck

	for i, inp := range blobs {
		res := results[i]
		_, err := tx.Exec(ctx, `
			INSERT INTO ipld_blobs
				(commitment, epoch, slot, network, blob_index, data_cid, meta_cid,
				 versioned_hash, tx_hash, block_number, block_hash, size_bytes)
			VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12)
			ON CONFLICT (commitment) DO UPDATE SET
				data_cid       = EXCLUDED.data_cid,
				meta_cid       = EXCLUDED.meta_cid,
				versioned_hash = EXCLUDED.versioned_hash,
				tx_hash        = EXCLUDED.tx_hash,
				block_number   = EXCLUDED.block_number,
				block_hash     = EXCLUDED.block_hash,
				created_at     = NOW()
		`,
			inp.Commitment,
			epoch,
			inp.Slot,
			network,
			inp.Index,
			res.DataCID.String(),
			res.MetaCID.String(),
			inp.VersionedHash,
			inp.TxHash,
			inp.BlockNumber,
			inp.BlockHash,
			res.SizeBytes,
		)
		if err != nil {
			return fmt.Errorf("db: save blob %s: %w", inp.Commitment, err)
		}
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

// GetLastProcessedEpoch returns MAX(epoch) from ipld_epochs for the network,
// or 0 if no epochs have been saved yet.
func (b *DBStateBackend) GetLastProcessedEpoch(ctx context.Context) (uint64, error) {
	var epoch uint64
	err := b.client.pool.QueryRow(ctx,
		`SELECT COALESCE(MAX(epoch), 0) FROM ipld_epochs WHERE network = $1`,
		b.network,
	).Scan(&epoch)
	if err != nil {
		return 0, fmt.Errorf("db: get last processed epoch: %w", err)
	}
	return epoch, nil
}

// SetLastProcessedEpoch is a no-op for the DB backend: the epoch row written
// by SaveEpoch already acts as the persistent progress marker.
func (b *DBStateBackend) SetLastProcessedEpoch(_ context.Context, _ uint64) error {
	return nil
}

// ─── Read operations ──────────────────────────────────────────────────────────

// EpochExists reports whether an epoch has already been saved.
func (c *Client) EpochExists(ctx context.Context, epoch uint64) (bool, error) {
	var exists bool
	err := c.pool.QueryRow(ctx,
		`SELECT EXISTS(SELECT 1 FROM ipld_epochs WHERE epoch = $1)`, epoch,
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

// GetBlobsByEpoch returns all blob records for a given epoch, ordered by blob_index.
type BlobRecord struct {
	Commitment    string
	DataCID       string
	MetaCID       string
	BlobIndex     int
	Slot          uint64
	VersionedHash string
	TxHash        string
	BlockNumber   uint64
	BlockHash     string
	SizeBytes     int64
}

func (c *Client) GetBlobsByEpoch(ctx context.Context, epoch uint64) ([]BlobRecord, error) {
	rows, err := c.pool.Query(ctx,
		`SELECT commitment, data_cid, meta_cid, blob_index,
		        slot, versioned_hash, tx_hash, block_number, block_hash, size_bytes
		 FROM ipld_blobs WHERE epoch = $1 ORDER BY blob_index`,
		epoch,
	)
	if err != nil {
		return nil, fmt.Errorf("db: get blobs epoch %d: %w", epoch, err)
	}
	defer rows.Close()

	var out []BlobRecord
	for rows.Next() {
		var r BlobRecord
		if err := rows.Scan(&r.Commitment, &r.DataCID, &r.MetaCID, &r.BlobIndex,
			&r.Slot, &r.VersionedHash, &r.TxHash, &r.BlockNumber, &r.BlockHash, &r.SizeBytes); err != nil {
			return nil, fmt.Errorf("db: scan blob row: %w", err)
		}
		out = append(out, r)
	}
	return out, rows.Err()
}
