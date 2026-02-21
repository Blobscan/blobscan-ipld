// Package car handles exporting a MemBlockstore as a self-contained CAR v2
// archive. CAR v2 files include an index section that allows random access to
// blocks without reading the full archive.
package car

import (
	"context"
	"fmt"
	"os"
	"path/filepath"

	"github.com/ipfs/go-cid"
	carv2 "github.com/ipld/go-car/v2"
	"github.com/ipld/go-car/v2/blockstore"

	"github.com/blobscan/blobscan-ipld/store"
)

// ExportRangeCAR writes all blocks from bs into a CAR v2 file at outPath,
// with rootCID as the single root. The file is written atomically (temp file
// then rename) to avoid partial writes.
func ExportRangeCAR(ctx context.Context, bs *store.MemBlockstore, rootCID cid.Cid, outPath string) error {
	if err := os.MkdirAll(filepath.Dir(outPath), 0o755); err != nil {
		return fmt.Errorf("car: mkdir %q: %w", filepath.Dir(outPath), err)
	}

	tmpPath := outPath + ".tmp"

	// Open a writable CAR v2 blockstore backed by the output file.
	rw, err := blockstore.OpenReadWrite(
		tmpPath,
		[]cid.Cid{rootCID},
		carv2.WriteAsCarV1(false), // produce CAR v2 with index
	)
	if err != nil {
		return fmt.Errorf("car: open write blockstore %q: %w", tmpPath, err)
	}

	// Copy all blocks from the in-memory store into the CAR blockstore.
	for _, blk := range bs.All() {
		if err := rw.Put(ctx, blk); err != nil {
			rw.Discard()
			_ = os.Remove(tmpPath)
			return fmt.Errorf("car: put block %s: %w", blk.Cid(), err)
		}
	}

	// Finalize the CAR v2 (writes the index and header).
	if err := rw.Finalize(); err != nil {
		_ = os.Remove(tmpPath)
		return fmt.Errorf("car: finalize %q: %w", tmpPath, err)
	}

	// Atomic rename.
	if err := os.Rename(tmpPath, outPath); err != nil {
		_ = os.Remove(tmpPath)
		return fmt.Errorf("car: rename %q → %q: %w", tmpPath, outPath, err)
	}

	return nil
}

// EpochCARPath returns the canonical file path for a per-epoch CAR file.
// Format: <carDir>/<network>/<epoch>.car
func EpochCARPath(carDir, network string, epoch uint64) string {
	return filepath.Join(
		carDir,
		network,
		fmt.Sprintf("%d.car", epoch),
	)
}

// RangeCARPath returns the canonical file path for a range CAR file.
// Kept for backwards compatibility.
// Format: <carDir>/<network>/<firstEpoch>-<lastEpoch>.car
func RangeCARPath(carDir, network string, firstEpoch, lastEpoch uint64) string {
	return filepath.Join(
		carDir,
		network,
		fmt.Sprintf("%d-%d.car", firstEpoch, lastEpoch),
	)
}

// VerifyCARRoot opens an existing CAR v2 file and checks that rootCID is
// listed as one of its roots. Useful for post-export sanity checks.
func VerifyCARRoot(carPath string, rootCID cid.Cid) error {
	f, err := os.Open(carPath)
	if err != nil {
		return fmt.Errorf("car: open %q: %w", carPath, err)
	}
	defer f.Close()

	cr, err := carv2.NewReader(f)
	if err != nil {
		return fmt.Errorf("car: read header %q: %w", carPath, err)
	}
	defer cr.Close()

	roots, err := cr.Roots()
	if err != nil {
		return fmt.Errorf("car: get roots %q: %w", carPath, err)
	}

	for _, r := range roots {
		if r == rootCID {
			return nil
		}
	}
	return fmt.Errorf("car: root CID %s not found in %q (roots: %v)", rootCID, carPath, roots)
}
