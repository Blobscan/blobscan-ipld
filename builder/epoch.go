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
// The `blobs` index is either a simple map (BlobMap) or a HAMT link depending
// on the hamtThreshold. It maps each blob's versionedHash to the (slot/index
// ordered) list of its BlobMetadata occurrences within the epoch. Keys are
// sorted lexicographically so the resulting CID is fully deterministic.
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

	var blobsNode ipld.Node
	var err error

	if len(results) >= hamtThreshold {
		blobsNode, err = buildHAMTBlobIndex(ctx, lsys, inp.Blobs, results)
	} else {
		blobsNode, err = buildMapBlobIndex(inp.Blobs, results)
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
		qp.MapEntry(ma, "blobs",                qp.Node(blobsNode))
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

// blobGroup holds, for one versionedHash, the BlobMetadata CIDs of every
// occurrence of that blob within the epoch, ordered by (slot, index).
//
// A blob is keyed by its versionedHash so the index path is self-describing
// (`<EpochCID>/blobs/entries/<versionedHash>`). The value is a list because
// the same blob data (e.g. the zero blob) can appear more than once in an
// epoch — collapsing those into a single entry would drop occurrences.
type blobGroup struct {
	versionedHash string
	metaCIDs      []cid.Cid
}

// groupByVersionedHash buckets blobs by versionedHash, orders the occurrences
// in each bucket by (slot, index), and returns the buckets sorted
// lexicographically by versionedHash — fully deterministic.
func groupByVersionedHash(blobs []types.BlobInput, results []types.BlobResult) []blobGroup {
	type occurrence struct {
		slot  uint64
		index int
		meta  cid.Cid
	}

	byHash := make(map[string][]occurrence)
	for i := range blobs {
		b := blobs[i]
		byHash[b.VersionedHash] = append(byHash[b.VersionedHash], occurrence{
			slot:  b.Slot,
			index: b.Index,
			meta:  results[i].MetaCID,
		})
	}

	groups := make([]blobGroup, 0, len(byHash))
	for versionedHash, occs := range byHash {
		sort.Slice(occs, func(i, j int) bool {
			if occs[i].slot != occs[j].slot {
				return occs[i].slot < occs[j].slot
			}
			return occs[i].index < occs[j].index
		})

		metaCIDs := make([]cid.Cid, len(occs))
		for i, o := range occs {
			metaCIDs[i] = o.meta
		}
		groups = append(groups, blobGroup{versionedHash: versionedHash, metaCIDs: metaCIDs})
	}
	sort.Slice(groups, func(i, j int) bool {
		return groups[i].versionedHash < groups[j].versionedHash
	})
	return groups
}

// appendGroupEntry adds a single `versionedHash → [&BlobMetadata, …]` entry.
func appendGroupEntry(ma ipld.MapAssembler, g blobGroup) {
	metaCIDs := g.metaCIDs
	qp.MapEntry(ma, g.versionedHash, qp.List(int64(len(metaCIDs)), func(la ipld.ListAssembler) {
		for _, c := range metaCIDs {
			qp.ListEntry(la, qp.Link(cidlink.Link{Cid: c}))
		}
	}))
}

// buildMapBlobIndex builds a simple dag-cbor map:
// { type: "map", entries: { "<versionedHash>": [ &BlobMetadata, … ] } }.
// Keys are sorted lexicographically for determinism.
func buildMapBlobIndex(blobs []types.BlobInput, results []types.BlobResult) (ipld.Node, error) {
	groups := groupByVersionedHash(blobs, results)

	n, err := qp.BuildMap(basicnode.Prototype.Map, 2, func(ma ipld.MapAssembler) {
		qp.MapEntry(ma, "type", qp.String("map"))
		qp.MapEntry(ma, "entries", qp.Map(int64(len(groups)), func(bma ipld.MapAssembler) {
			for _, g := range groups {
				appendGroupEntry(bma, g)
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
	groups := groupByVersionedHash(blobs, results)

	// Build shards of up to 256 versionedHash entries each.
	const shardSize = 256

	var shardCIDs []cid.Cid
	for start := 0; start < len(groups); start += shardSize {
		end := start + shardSize
		if end > len(groups) {
			end = len(groups)
		}
		shard := groups[start:end]

		shardNode, err := qp.BuildMap(basicnode.Prototype.Map, int64(len(shard)), func(ma ipld.MapAssembler) {
			for _, g := range shard {
				appendGroupEntry(ma, g)
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
