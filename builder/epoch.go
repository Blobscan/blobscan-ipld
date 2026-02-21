package builder

import (
	"context"
	"fmt"
	"sort"

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
		blobIndexNode, err = buildHAMTBlobIndex(ctx, lsys, results)
	} else {
		blobIndexNode, err = buildMapBlobIndex(results)
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

// buildMapBlobIndex builds a simple dag-cbor map: commitment → &BlobMetadata.
// Keys are sorted for determinism.
func buildMapBlobIndex(results []types.BlobResult) (ipld.Node, error) {
	// Sort by commitment for determinism.
	sorted := make([]types.BlobResult, len(results))
	copy(sorted, results)
	sort.Slice(sorted, func(i, j int) bool {
		return sorted[i].Commitment < sorted[j].Commitment
	})

	n, err := qp.BuildMap(basicnode.Prototype.Map, int64(len(sorted)+1), func(ma ipld.MapAssembler) {
		qp.MapEntry(ma, "type", qp.String("map"))
		qp.MapEntry(ma, "blobs", qp.Map(int64(len(sorted)), func(bma ipld.MapAssembler) {
			for _, r := range sorted {
				metaLink := cidlink.Link{Cid: r.MetaCID}
				qp.MapEntry(bma, r.Commitment, qp.Link(metaLink))
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
func buildHAMTBlobIndex(ctx context.Context, lsys ipld.LinkSystem, results []types.BlobResult) (ipld.Node, error) {
	// Sort for determinism.
	sorted := make([]types.BlobResult, len(results))
	copy(sorted, results)
	sort.Slice(sorted, func(i, j int) bool {
		return sorted[i].Commitment < sorted[j].Commitment
	})

	// Build shards of up to 256 entries each.
	const shardSize = 256
	type shardEntry struct {
		key  string
		link cid.Cid
	}

	var shardCIDs []cid.Cid
	for start := 0; start < len(sorted); start += shardSize {
		end := start + shardSize
		if end > len(sorted) {
			end = len(sorted)
		}
		shard := sorted[start:end]

		shardNode, err := qp.BuildMap(basicnode.Prototype.Map, int64(len(shard)), func(ma ipld.MapAssembler) {
			for _, r := range shard {
				qp.MapEntry(ma, r.Commitment, qp.Link(cidlink.Link{Cid: r.MetaCID}))
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
