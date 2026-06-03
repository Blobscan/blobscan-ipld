package builder_test

import (
	"context"
	"testing"

	"github.com/ipfs/go-cid"

	"github.com/blobscan/blobscan-ipld/builder"
	"github.com/blobscan/blobscan-ipld/store"
	"github.com/blobscan/blobscan-ipld/types"
)

// makeEpochResults creates n synthetic EpochResult values starting at epoch
// startEpoch with sequential CIDs (derived from epoch_test helpers).
func makeEpochResults(t *testing.T, startEpoch uint64, n int) []types.EpochResult {
	t.Helper()
	results := make([]types.EpochResult, n)
	for i := 0; i < n; i++ {
		epoch := startEpoch + uint64(i)
		// Use buildTestEpoch to get a real CID from the block builder so that the
		// block data is valid dag-cbor (required for deterministic CID generation).
		er, _ := buildTestEpoch(t, epoch, 1)
		results[i] = er
	}
	return results
}

// TestBuildNetworkRoot_BlockSizeUnder2MiB is the core regression test: with
// 50 000 synthetic epochs no single block in the MemBlockstore may exceed the
// IPFS 2 MiB limit.
func TestBuildNetworkRoot_BlockSizeUnder2MiB(t *testing.T) {
	const maxBlock = 2 * 1024 * 1024 // Kubo hard limit
	ctx := context.Background()

	epochs := makeEpochResults(t, 0, 50000)

	bs := store.NewMemBlockstore()
	lsys := store.NewLinkSystem(bs)

	_, err := builder.BuildNetworkRoot(ctx, lsys, "testnet", epochs, 10000)
	if err != nil {
		t.Fatalf("BuildNetworkRoot error: %v", err)
	}

	for _, blk := range bs.All() {
		if sz := len(blk.RawData()); sz > maxBlock {
			t.Errorf("block %s is %d bytes, exceeds 2 MiB limit", blk.Cid(), sz)
		}
	}
}

// TestBuildNetworkRoot_PageSplitting verifies page boundary alignment.
// 25 001 epochs (0..25000) with pageSize=10000 must produce exactly 3 pages
// (starting at 0, 10000, 20000).
func TestBuildNetworkRoot_PageSplitting(t *testing.T) {
	ctx := context.Background()
	epochs := makeEpochResults(t, 0, 25001)

	bs := store.NewMemBlockstore()
	lsys := store.NewLinkSystem(bs)

	result, err := builder.BuildNetworkRoot(ctx, lsys, "testnet", epochs, 10000)
	if err != nil {
		t.Fatalf("BuildNetworkRoot error: %v", err)
	}
	if result.PageCount != 3 {
		t.Errorf("expected 3 pages, got %d", result.PageCount)
	}
}

// TestBuildNetworkRoot_Determinism verifies that two calls with the same input
// produce the same root CID.
func TestBuildNetworkRoot_Determinism(t *testing.T) {
	ctx := context.Background()
	epochs := makeEpochResults(t, 0, 100)

	build := func() cid.Cid {
		bs := store.NewMemBlockstore()
		lsys := store.NewLinkSystem(bs)
		r, err := builder.BuildNetworkRoot(ctx, lsys, "testnet", epochs, 10000)
		if err != nil {
			t.Fatalf("BuildNetworkRoot error: %v", err)
		}
		return r.CID
	}

	cid1 := build()
	cid2 := build()
	if !cid1.Equals(cid2) {
		t.Errorf("non-deterministic: %s != %s", cid1, cid2)
	}
}

// TestBuildNetworkRoot_Empty verifies that an empty epoch list doesn't panic.
func TestBuildNetworkRoot_Empty(t *testing.T) {
	ctx := context.Background()
	bs := store.NewMemBlockstore()
	lsys := store.NewLinkSystem(bs)

	result, err := builder.BuildNetworkRoot(ctx, lsys, "testnet", nil, 10000)
	if err != nil {
		t.Fatalf("BuildNetworkRoot error: %v", err)
	}
	if result.PageCount != 0 {
		t.Errorf("expected 0 pages for empty input, got %d", result.PageCount)
	}
	if result.CID == (cid.Cid{}) {
		t.Error("expected non-zero CID even for empty input")
	}
}

// TestBuildNetworkRoot_SinglePage verifies that a small epoch set stays in one page.
func TestBuildNetworkRoot_SinglePage(t *testing.T) {
	ctx := context.Background()
	epochs := makeEpochResults(t, 0, 500)

	bs := store.NewMemBlockstore()
	lsys := store.NewLinkSystem(bs)

	result, err := builder.BuildNetworkRoot(ctx, lsys, "testnet", epochs, 10000)
	if err != nil {
		t.Fatalf("BuildNetworkRoot error: %v", err)
	}
	if result.PageCount != 1 {
		t.Errorf("expected 1 page, got %d", result.PageCount)
	}
}
