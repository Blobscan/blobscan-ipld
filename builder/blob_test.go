package builder_test

import (
	"context"
	"testing"

	"github.com/blobscan/blobscan-ipld/builder"
	"github.com/blobscan/blobscan-ipld/store"
	"github.com/blobscan/blobscan-ipld/types"
)

func makeTestBlob(commitment string, epoch, slot uint64, idx int) types.BlobInput {
	data := make([]byte, 131072) // 128 KiB
	for i := range data {
		data[i] = byte(i % 256)
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
