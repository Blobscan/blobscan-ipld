package builder

import (
	"context"
	"fmt"
	"sort"
	"strconv"

	"github.com/ipfs/go-cid"
	"github.com/ipld/go-ipld-prime"
	"github.com/ipld/go-ipld-prime/fluent/qp"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	"github.com/ipld/go-ipld-prime/node/basicnode"

	"github.com/blobscan/blobscan-ipld/types"
)

// BuildEpochNode constructs and stores an EpochNode dag-cbor node.
//
// The blob index is either a simple map (BlobMap) or a HAMT link depending on
// the hamtThreshold. Map keys (blob commitments) are sorted lexicographically
// so the resulting CID is fully deterministic.
func BuildEpochNode(
	ctx context.Context,
	lsys ipld.LinkSystem,
	inp types.EpochInput,
	results []types.BlobResult,
	network string,
	hamtThreshold int,
) (types.EpochResult, error) {
	var totalSize int64
	for _, r := range results {
		totalSize += r.SizeBytes
	}

	var blobIndexNode ipld.Node
	var err error

	if len(results) >= hamtThreshold {
		blobIndexNode, err = buildHAMTBlobIndex(ctx, lsys, inp.Blobs, results)
	} else {
		blobIndexNode, err = buildMapBlobIndex(inp.Blobs, results)
	}
	if err != nil {
		return types.EpochResult{}, fmt.Errorf("builder: epoch %d blob index: %w", inp.Epoch, err)
	}

	epochNode, err := qp.BuildMap(basicnode.Prototype.Map, 6, func(ma ipld.MapAssembler) {
		qp.MapEntry(ma, "epoch",                qp.Int(int64(inp.Epoch)))
		qp.MapEntry(ma, "slot",                 qp.Int(int64(inp.Slot)))
		qp.MapEntry(ma, "network",              qp.String(network))
		qp.MapEntry(ma, "approximateSizeBytes", qp.Int(totalSize))
		qp.MapEntry(ma, "blobCount",            qp.Int(int64(len(results))))
		qp.MapEntry(ma, "blobIndex",            qp.Node(blobIndexNode))
	})
	if err != nil {
		return types.EpochResult{}, fmt.Errorf("builder: build epoch node %d: %w", inp.Epoch, err)
	}

	lnk, err := lsys.Store(
		ipld.LinkContext{Ctx: ctx},
		linkProtoDagCBOR,
		epochNode,
	)
	if err != nil {
		return types.EpochResult{}, fmt.Errorf("builder: store epoch node %d: %w", inp.Epoch, err)
	}

	return types.EpochResult{
		Epoch:                inp.Epoch,
		CID:                  lnk.(cidlink.Link).Cid,
		ApproximateSizeBytes: totalSize,
	}, nil
}

// blobEntry pairs a BlobInput with its computed BlobResult for sorting.
type blobEntry struct {
	blob   types.BlobInput
	result types.BlobResult
}

// blobKey returns the unique map key for a blob: "<slot>/<index>".
// Using slot+index instead of commitment avoids duplicate-key errors when
// the same blob data (e.g. the zero blob) appears multiple times in an epoch.
func blobKey(b types.BlobInput) string {
	return strconv.FormatUint(b.Slot, 10) + "/" + strconv.Itoa(b.Index)
}

// buildMapBlobIndex builds a simple dag-cbor map: "<slot>/<index>" → &BlobMetadata.
// Keys are sorted lexicographically for determinism.
func buildMapBlobIndex(blobs []types.BlobInput, results []types.BlobResult) (ipld.Node, error) {
	entries := make([]blobEntry, len(blobs))
	for i := range blobs {
		entries[i] = blobEntry{blob: blobs[i], result: results[i]}
	}
	sort.Slice(entries, func(i, j int) bool {
		return blobKey(entries[i].blob) < blobKey(entries[j].blob)
	})

	n, err := qp.BuildMap(basicnode.Prototype.Map, int64(len(entries)+1), func(ma ipld.MapAssembler) {
		qp.MapEntry(ma, "type", qp.String("map"))
		qp.MapEntry(ma, "blobs", qp.Map(int64(len(entries)), func(bma ipld.MapAssembler) {
			for _, e := range entries {
				qp.MapEntry(bma, blobKey(e.blob), qp.Link(cidlink.Link{Cid: e.result.MetaCID}))
			}
		}))
	})
	if err != nil {
		return nil, fmt.Errorf("builder: build map blob index: %w", err)
	}
	return n, nil
}

// buildHAMTBlobIndex builds a HAMT-based blob index for large epochs.
// This implementation uses a simple sharded approach compatible with
// go-ipld-adl-hamt conventions. For production, replace with the full ADL.
func buildHAMTBlobIndex(ctx context.Context, lsys ipld.LinkSystem, blobs []types.BlobInput, results []types.BlobResult) (ipld.Node, error) {
	entries := make([]blobEntry, len(blobs))
	for i := range blobs {
		entries[i] = blobEntry{blob: blobs[i], result: results[i]}
	}
	sort.Slice(entries, func(i, j int) bool {
		return blobKey(entries[i].blob) < blobKey(entries[j].blob)
	})

	// Build shards of up to 256 entries each.
	const shardSize = 256

	var shardCIDs []cid.Cid
	for start := 0; start < len(entries); start += shardSize {
		end := start + shardSize
		if end > len(entries) {
			end = len(entries)
		}
		shard := entries[start:end]

		shardNode, err := qp.BuildMap(basicnode.Prototype.Map, int64(len(shard)), func(ma ipld.MapAssembler) {
			for _, e := range shard {
				qp.MapEntry(ma, blobKey(e.blob), qp.Link(cidlink.Link{Cid: e.result.MetaCID}))
			}
		})
		if err != nil {
			return nil, fmt.Errorf("builder: build hamt shard: %w", err)
		}

		lnk, err := lsys.Store(ipld.LinkContext{Ctx: ctx}, linkProtoDagCBOR, shardNode)
		if err != nil {
			return nil, fmt.Errorf("builder: store hamt shard: %w", err)
		}
		shardCIDs = append(shardCIDs, lnk.(cidlink.Link).Cid)
	}

	// Build the HAMT root node pointing to all shards.
	hamtRoot, err := qp.BuildMap(basicnode.Prototype.Map, 3, func(ma ipld.MapAssembler) {
		qp.MapEntry(ma, "type",      qp.String("hamt"))
		qp.MapEntry(ma, "shardSize", qp.Int(shardSize))
		qp.MapEntry(ma, "shards",    qp.List(int64(len(shardCIDs)), func(la ipld.ListAssembler) {
			for _, sc := range shardCIDs {
				qp.ListEntry(la, qp.Link(cidlink.Link{Cid: sc}))
			}
		}))
	})
	if err != nil {
		return nil, fmt.Errorf("builder: build hamt root: %w", err)
	}

	return hamtRoot, nil
}
