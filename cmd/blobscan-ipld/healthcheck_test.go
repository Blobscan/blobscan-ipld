package main

import (
	"context"
	"reflect"
	"testing"

	"github.com/blobscan/blobscan-ipld/builder"
	"github.com/blobscan/blobscan-ipld/db"
	"github.com/blobscan/blobscan-ipld/generator"
	"github.com/blobscan/blobscan-ipld/store"
	"github.com/blobscan/blobscan-ipld/types"
)

func TestCollapseRanges(t *testing.T) {
	cases := []struct {
		name string
		in   []uint64
		want [][2]uint64
	}{
		{"empty", nil, nil},
		{"single", []uint64{5}, [][2]uint64{{5, 5}}},
		{"contiguous", []uint64{1, 2, 3}, [][2]uint64{{1, 3}}},
		{"gapped", []uint64{1, 2, 4, 5, 9}, [][2]uint64{{1, 2}, {4, 5}, {9, 9}}},
		{"unsorted+dup", []uint64{9, 1, 2, 2, 5, 4}, [][2]uint64{{1, 2}, {4, 5}, {9, 9}}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got := collapseRanges(c.in)
			if !reflect.DeepEqual(got, c.want) {
				t.Errorf("collapseRanges(%v) = %v, want %v", c.in, got, c.want)
			}
		})
	}
}

func TestDedupUint64(t *testing.T) {
	got := dedupUint64([]uint64{3, 1, 2, 1, 3})
	want := []uint64{1, 2, 3}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("dedupUint64 = %v, want %v", got, want)
	}
	if dedupUint64(nil) != nil {
		t.Error("dedupUint64(nil) should be nil")
	}
}

func TestCapSamples(t *testing.T) {
	in := []uint64{1, 2, 3, 4, 5}
	if got := capSamples(in, 3); len(got) != 3 {
		t.Errorf("capSamples len = %d, want 3", len(got))
	}
	if got := capSamples(in, 10); len(got) != 5 {
		t.Errorf("capSamples len = %d, want 5", len(got))
	}
}

func TestFormatEpochSamples(t *testing.T) {
	if got := formatEpochSamples([]uint64{1, 2, 3}, 10); got != "1 · 2 · 3" {
		t.Errorf("got %q", got)
	}
	if got := formatEpochSamples([]uint64{1, 2, 3, 4, 5}, 2); got != "1 · 2 … +3" {
		t.Errorf("got %q", got)
	}
}

func TestWorstStatus(t *testing.T) {
	r := &healthReport{}
	r.add("a", 0, statusPass, "", nil)
	r.add("b", 0, statusWarn, "", nil)
	if r.worst() != statusWarn {
		t.Errorf("worst = %v, want WARN", r.worst())
	}
	r.add("c", 0, statusFail, "", nil)
	if r.worst() != statusFail {
		t.Errorf("worst = %v, want FAIL", r.worst())
	}
	// SKIP never raises severity.
	skipOnly := &healthReport{}
	skipOnly.add("x", 0, statusSkip, "", nil)
	if skipOnly.worst() != statusPass {
		t.Errorf("worst of skip-only = %v, want PASS", skipOnly.worst())
	}
}

// TestTier1MetaRecomputeDetectsCorruption exercises the core Tier-1 detection
// primitive: recomputing meta_cid from a DB row via ReconstructFromDB +
// StoreBlobMetadata, and confirming a tampered meta_cid no longer matches while
// the correct one does. This is the offline check that catches the wrong-
// meta_cid corruption that repair-epochs could not.
func TestTier1MetaRecomputeDetectsCorruption(t *testing.T) {
	ctx := context.Background()

	// Build a real blob to obtain a correct (DataCID, MetaCID) pair.
	data := make([]byte, 131072)
	for i := range data {
		data[i] = byte(i)
	}
	inp := types.BlobInput{
		Commitment:    "0xabc",
		VersionedHash: "0x01def",
		TxHash:        "0xtx",
		BlockNumber:   100,
		BlockHash:     "0xblk",
		Slot:          320,
		Epoch:         10,
		Index:         0,
		Data:          data,
	}
	bs := store.NewMemBlockstore()
	lsys := store.NewLinkSystem(bs)
	ref, err := builder.ProcessBlob(ctx, lsys, inp)
	if err != nil {
		t.Fatalf("ProcessBlob: %v", err)
	}

	// A correct DB row reconstructs to the same MetaCID.
	good := []db.BlobRecord{{
		Commitment:    inp.Commitment,
		DataCID:       ref.DataCID.String(),
		MetaCID:       ref.MetaCID.String(),
		BlobIndex:     inp.Index,
		Slot:          inp.Slot,
		VersionedHash: inp.VersionedHash,
		TxHash:        inp.TxHash,
		BlockNumber:   inp.BlockNumber,
		BlockHash:     inp.BlockHash,
		SizeBytes:     ref.SizeBytes,
	}}

	epochInp, blobResults, err := generator.ReconstructFromDB(10, good)
	if err != nil {
		t.Fatalf("ReconstructFromDB: %v", err)
	}
	rbs := store.NewMemBlockstore()
	rlsys := store.NewLinkSystem(rbs)
	recomputed, err := builder.StoreBlobMetadata(ctx, rlsys, epochInp.Blobs[0], blobResults[0].DataCID)
	if err != nil {
		t.Fatalf("StoreBlobMetadata: %v", err)
	}
	if recomputed.String() != good[0].MetaCID {
		t.Fatalf("recompute of good row mismatched: got %s want %s", recomputed, good[0].MetaCID)
	}

	// A row whose stored meta_cid was corrupted must NOT match the recompute —
	// this is exactly what the health check flags.
	if recomputed.String() == ref.DataCID.String() {
		t.Fatal("sanity: meta and data CID should differ")
	}
	corruptedMetaCID := ref.DataCID.String() // plausible-looking but wrong
	if recomputed.String() == corruptedMetaCID {
		t.Error("recomputed MetaCID unexpectedly equals the corrupted value")
	}
}
