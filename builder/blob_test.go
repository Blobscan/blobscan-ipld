package builder_test

import (
	"context"
	"testing"

	"github.com/blobscan/blobscan-ipld/builder"
	"github.com/blobscan/blobscan-ipld/store"
	"github.com/blobscan/blobscan-ipld/types"
)

func makeTestBlob(commitment string, epoch, slot uint64, idx int) types.BlobInput {
	// Seed each blob's data with its idx so different blobs have different bytes
	// and therefore different DataCIDs (matching real-world behaviour).
	data := make([]byte, 131072) // 128 KiB
	for i := range data {
		data[i] = byte((i + idx) % 256)
	}
	return types.BlobInput{
		Commitment:    commitment,
		VersionedHash: "0x01" + commitment[4:],
		TxHash:        "0xdeadbeef",
		BlockNumber:   1000000 + epoch,
		BlockHash:     "0xblockhash",
		Slot:          slot,
		Epoch:         epoch,
		Index:         idx,
		Data:          data,
	}
}

// TestProcessBlob verifies that the same input always produces the same CIDs
// (determinism) and that both a raw blob CID and a metadata CID are produced.
func TestProcessBlob(t *testing.T) {
	ctx := context.Background()
	bs := store.NewMemBlockstore()
	lsys := store.NewLinkSystem(bs)

	inp := makeTestBlob("0xaabbccdd", 100, 3200, 0)

	res1, err := builder.ProcessBlob(ctx, lsys, inp)
	if err != nil {
		t.Fatalf("ProcessBlob: %v", err)
	}

	// Second call with identical input must produce identical CIDs.
	bs2 := store.NewMemBlockstore()
	lsys2 := store.NewLinkSystem(bs2)
	res2, err := builder.ProcessBlob(ctx, lsys2, inp)
	if err != nil {
		t.Fatalf("ProcessBlob (2nd): %v", err)
	}

	if res1.DataCID != res2.DataCID {
		t.Errorf("DataCID not deterministic: %s != %s", res1.DataCID, res2.DataCID)
	}
	if res1.MetaCID != res2.MetaCID {
		t.Errorf("MetaCID not deterministic: %s != %s", res1.MetaCID, res2.MetaCID)
	}
	if res1.DataCID == res1.MetaCID {
		t.Error("DataCID and MetaCID must be different")
	}
	if res1.SizeBytes != int64(len(inp.Data)) {
		t.Errorf("SizeBytes: got %d, want %d", res1.SizeBytes, len(inp.Data))
	}

	// Blockstore must contain exactly 2 blocks: raw blob + metadata.
	if bs.Len() != 2 {
		t.Errorf("blockstore len: got %d, want 2", bs.Len())
	}
}

// TestStoreBlobMetadataSizeField verifies that StoreBlobMetadata produces the same
// MetaCID whether the size comes from len(inp.Data) or from inp.Size (DB-reconstruction
// path where Data is nil but Size is explicitly set).
func TestStoreBlobMetadataSizeField(t *testing.T) {
	ctx := context.Background()

	// Process a real blob to get the reference CIDs.
	inp := makeTestBlob("0xaabbccdd", 100, 3200, 0)
	bs1 := store.NewMemBlockstore()
	lsys1 := store.NewLinkSystem(bs1)
	ref, err := builder.ProcessBlob(ctx, lsys1, inp)
	if err != nil {
		t.Fatalf("ProcessBlob: %v", err)
	}

	// Simulate DB-reconstruction: Data is nil, Size is set from stored SizeBytes.
	inpNoData := types.BlobInput{
		Commitment:    inp.Commitment,
		VersionedHash: inp.VersionedHash,
		TxHash:        inp.TxHash,
		BlockNumber:   inp.BlockNumber,
		BlockHash:     inp.BlockHash,
		Slot:          inp.Slot,
		Epoch:         inp.Epoch,
		Index:         inp.Index,
		Data:          nil,
		Size:          ref.SizeBytes, // populated from BlobResult.SizeBytes / DB
	}

	bs2 := store.NewMemBlockstore()
	lsys2 := store.NewLinkSystem(bs2)
	// StoreBlobMetadata requires a known dataCID; use the one from the real run.
	metaCID, err := builder.StoreBlobMetadata(ctx, lsys2, inpNoData, ref.DataCID)
	if err != nil {
		t.Fatalf("StoreBlobMetadata with nil Data: %v", err)
	}

	if metaCID != ref.MetaCID {
		t.Errorf("MetaCID mismatch: with nil Data got %s, with real Data got %s", metaCID, ref.MetaCID)
	}
}

// TestProcessBlobDifferentInputs verifies that different blobs produce different CIDs.
func TestProcessBlobDifferentInputs(t *testing.T) {
	ctx := context.Background()
	bs := store.NewMemBlockstore()
	lsys := store.NewLinkSystem(bs)

	inp1 := makeTestBlob("0xaabbccdd", 100, 3200, 0)
	inp2 := makeTestBlob("0x11223344", 100, 3200, 1)

	res1, err := builder.ProcessBlob(ctx, lsys, inp1)
	if err != nil {
		t.Fatalf("ProcessBlob 1: %v", err)
	}
	res2, err := builder.ProcessBlob(ctx, lsys, inp2)
	if err != nil {
		t.Fatalf("ProcessBlob 2: %v", err)
	}

	if res1.DataCID == res2.DataCID {
		t.Error("different blobs should produce different DataCIDs")
	}
	if res1.MetaCID == res2.MetaCID {
		t.Error("different blobs should produce different MetaCIDs")
	}
}
