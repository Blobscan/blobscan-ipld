// Package types defines the core domain types used across the blobscan-ipld
// generator. These are plain Go structs; IPLD node construction happens in the
// builder package so that this package has zero IPLD dependencies and is easy
// to unit-test.
package types

import "github.com/ipfs/go-cid"

// ─── Raw input types (from beacon / EL node) ─────────────────────────────────

// BlobInput is the raw data fetched from the beacon node for a single blob.
type BlobInput struct {
	Commitment    string // KZG commitment, 0x-prefixed hex
	VersionedHash string // EIP-4844 versioned hash, 0x-prefixed hex
	TxHash        string // transaction hash, 0x-prefixed hex
	BlockNumber   uint64
	BlockHash     string // 0x-prefixed hex
	Slot          uint64
	Epoch         uint64
	Index         int    // blob index within the transaction
	Data          []byte // raw 128 KiB blob field element data
	// Size is the authoritative byte length of Data. It must be set explicitly
	// when Data is not loaded (e.g. when reconstructing from the DB) so that
	// StoreBlobMetadata produces the same CID as the original. When Data is
	// present, Size may be left as zero and len(Data) is used as the fallback.
	Size int64
}

// EpochInput groups all blobs that belong to a single finalized epoch.
type EpochInput struct {
	Epoch uint64
	Slot  uint64 // first slot of the epoch
	Blobs []BlobInput
}

// ─── IPLD node result types ───────────────────────────────────────────────────

// BlobResult holds the CIDs produced after storing a single blob.
type BlobResult struct {
	Commitment string
	DataCID    cid.Cid // CID of the raw blob bytes (codec=raw)
	MetaCID    cid.Cid // CID of the BlobMetadata IPLD node (codec=dag-cbor)
	SizeBytes  int64   // raw blob size
}

// EpochResult holds the CID of a fully-built EpochNode and its CAR file path.
type EpochResult struct {
	Epoch                uint64
	CID                  cid.Cid
	CARPath              string
	ApproximateSizeBytes int64
}

// NetworkRootResult holds the CID of the rebuilt NetworkRoot node.
type NetworkRootResult struct {
	CID       cid.Cid
	PageCount int // number of EpochPage blocks in the paged structure
}

// ─── Persistent state ────────────────────────────────────────────────────────

// State is persisted to disk between runs so the generator can resume.
type State struct {
	Network            string `json:"network"`
	LastProcessedEpoch uint64 `json:"last_processed_epoch"`
	BackfillCursor     uint64 `json:"backfill_cursor,omitempty"`
}
