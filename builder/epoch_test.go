package builder_test

import (
	"context"
	"testing"

	"github.com/blobscan/blobscan-ipld/builder"
	"github.com/blobscan/blobscan-ipld/store"
	"github.com/blobscan/blobscan-ipld/types"
)

func buildTestEpoch(t *testing.T, epoch uint64, blobCount int) (types.EpochResult, *store.MemBlockstore) {
	t.Helper()
	ctx := context.Background()
	bs := store.NewMemBlockstore()
	lsys := store.NewLinkSystem(bs)

	blobs := make([]types.BlobInput, blobCount)
	for i := range blobs {
		blobs[i] = makeTestBlob(
			"0x"+padHex(uint64(i+1), 8),
			epoch,
			epoch*32,
			i,
		)
	}

	results := make([]types.BlobResult, blobCount)
	for i, b := range blobs {
		res, err := builder.ProcessBlob(ctx, lsys, b)
		if err != nil {
			t.Fatalf("ProcessBlob %d: %v", i, err)
		}
		results[i] = res
	}

	inp := types.EpochInput{Epoch: epoch, Slot: epoch * 32, Blobs: blobs}
	epochResult, err := builder.BuildEpochNode(ctx, lsys, inp, results, "mainnet", 5000)
	if err != nil {
		t.Fatalf("BuildEpochNode: %v", err)
	}

	return epochResult, bs
}

// padHex returns a hex string of n padded to width digits.
func padHex(n uint64, width int) string {
	s := ""
	for n > 0 {
		s = string(rune("0123456789abcdef"[n%16])) + s
		n /= 16
	}
	for len(s) < width {
		s = "0" + s
	}
	return s
}

// TestBuildEpochNode verifies determinism and basic structure.
func TestBuildEpochNode(t *testing.T) {
	res1, _ := buildTestEpoch(t, 100, 3)
	res2, _ := buildTestEpoch(t, 100, 3)

	if res1.CID != res2.CID {
		t.Errorf("EpochNode CID not deterministic: %s != %s", res1.CID, res2.CID)
	}
	if res1.Epoch != 100 {
		t.Errorf("Epoch: got %d, want 100", res1.Epoch)
	}
	if res1.ApproximateSizeBytes == 0 {
		t.Error("ApproximateSizeBytes should be > 0")
	}
}

// TestBuildEpochNodeDifferentEpochs verifies that different epochs produce
// different CIDs even with the same blobs.
func TestBuildEpochNodeDifferentEpochs(t *testing.T) {
	res1, _ := buildTestEpoch(t, 100, 2)
	res2, _ := buildTestEpoch(t, 101, 2)

	if res1.CID == res2.CID {
		t.Error("different epochs must produce different CIDs")
	}
}

// TestBuildRangeNode verifies that a range node is built correctly from
// multiple epoch results.
func TestBuildRangeNode(t *testing.T) {
	ctx := context.Background()

	epochResults := make([]types.EpochResult, 3)

	for i := range epochResults {
		epoch := uint64(100 + i)
		res, _ := buildTestEpoch(t, epoch, 2)
		epochResults[i] = res
	}

	bs := store.NewMemBlockstore()
	lsys := store.NewLinkSystem(bs)

	rangeResult, err := builder.BuildRangeNode(ctx, lsys, "mainnet", epochResults)
	if err != nil {
		t.Fatalf("BuildRangeNode: %v", err)
	}

	if rangeResult.Epoch != 100 {
		t.Errorf("Epoch (first): got %d, want 100", rangeResult.Epoch)
	}
	if rangeResult.CID.String() == "" {
		t.Error("RangeNode CID should not be empty")
	}
	if bs.Len() == 0 {
		t.Error("range blockstore should not be empty")
	}

	// Determinism check.
	bs2 := store.NewMemBlockstore()
	lsys2 := store.NewLinkSystem(bs2)
	rangeResult2, err := builder.BuildRangeNode(ctx, lsys2, "mainnet", epochResults)
	if err != nil {
		t.Fatalf("BuildRangeNode (2nd): %v", err)
	}
	if rangeResult.CID != rangeResult2.CID {
		t.Errorf("RangeNode CID not deterministic: %s != %s", rangeResult.CID, rangeResult2.CID)
	}
}
