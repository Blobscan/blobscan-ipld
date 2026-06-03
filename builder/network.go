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

// BuildNetworkRoot constructs and stores a paged NetworkRoot dag-cbor node that
// indexes all known epochs via a two-level structure:
//
//	NetworkRoot  →  EpochPage₀, EpochPage₁, …
//	EpochPage    →  epoch₀, epoch₁, … (up to pageSize entries each)
//
// Paging prevents the root block from exceeding the IPFS 2 MiB block limit as
// the number of epochs grows. Each EpochPage covers a fixed-width epoch range
// aligned to multiples of pageSize (e.g. epochs 0–9999, 10000–19999, …).
//
// epochs must be the full list of all processed epochs (loaded from DB).
// pageSize is the maximum number of epoch entries per page (minimum 1000).
// The map keys within each block are sorted numerically for determinism.
func BuildNetworkRoot(
	ctx context.Context,
	lsys ipld.LinkSystem,
	network string,
	epochs []types.EpochResult,
	pageSize int,
) (types.NetworkRootResult, error) {
	if pageSize < 1000 {
		pageSize = 1000
	}

	// Sort by epoch number for determinism.
	sorted := make([]types.EpochResult, len(epochs))
	copy(sorted, epochs)
	sort.Slice(sorted, func(i, j int) bool {
		return sorted[i].Epoch < sorted[j].Epoch
	})

	// Aggregate totals.
	var latestEpoch uint64
	var totalSizeBytes int64
	if len(sorted) > 0 {
		latestEpoch = sorted[len(sorted)-1].Epoch
		for _, e := range sorted {
			totalSizeBytes += e.ApproximateSizeBytes
		}
	}

	// Group epochs into pages aligned to pageSize multiples.
	ps := uint64(pageSize)
	pageMap := make(map[uint64][]types.EpochResult)
	for _, e := range sorted {
		start := (e.Epoch / ps) * ps
		pageMap[start] = append(pageMap[start], e)
	}

	// Sort page starts numerically for deterministic map ordering.
	pageStarts := make([]uint64, 0, len(pageMap))
	for start := range pageMap {
		pageStarts = append(pageStarts, start)
	}
	sort.Slice(pageStarts, func(i, j int) bool {
		return pageStarts[i] < pageStarts[j]
	})

	// Build each EpochPage block and collect its CID.
	type pageEntry struct {
		key string
		cid cid.Cid
	}
	pages := make([]pageEntry, 0, len(pageStarts))
	for _, start := range pageStarts {
		pageCID, err := buildEpochPage(ctx, lsys, pageMap[start])
		if err != nil {
			return types.NetworkRootResult{}, fmt.Errorf("builder: build epoch page %d: %w", start, err)
		}
		pages = append(pages, pageEntry{
			key: strconv.FormatUint(start, 10),
			cid: pageCID,
		})
	}

	// Build the NetworkRoot node.
	// Field keys are in dag-cbor canonical order (by byte-length, then lex).
	rootNode, err := qp.BuildMap(basicnode.Prototype.Map, 6, func(ma ipld.MapAssembler) {
		qp.MapEntry(ma, "type",           qp.String("paged"))
		qp.MapEntry(ma, "network",        qp.String(network))
		qp.MapEntry(ma, "pageSize",       qp.Int(int64(pageSize)))
		qp.MapEntry(ma, "latestEpoch",    qp.Int(int64(latestEpoch)))
		qp.MapEntry(ma, "totalSizeBytes", qp.Int(totalSizeBytes))
		qp.MapEntry(ma, "pages", qp.Map(int64(len(pages)), func(pma ipld.MapAssembler) {
			for _, p := range pages {
				qp.MapEntry(pma, p.key, qp.Link(cidlink.Link{Cid: p.cid}))
			}
		}))
	})
	if err != nil {
		return types.NetworkRootResult{}, fmt.Errorf("builder: build network root: %w", err)
	}

	lnk, err := lsys.Store(
		ipld.LinkContext{Ctx: ctx},
		linkProtoDagCBOR,
		rootNode,
	)
	if err != nil {
		return types.NetworkRootResult{}, fmt.Errorf("builder: store network root: %w", err)
	}

	return types.NetworkRootResult{
		CID:       lnk.(cidlink.Link).Cid,
		PageCount: len(pages),
	}, nil
}

// buildEpochPage stores a single EpochPage dag-cbor node and returns its CID.
// The epochs slice must already be sorted by epoch number.
// An EpochPage node has the shape: { epochs: { "<n>": &EpochNode, … } }
func buildEpochPage(
	ctx context.Context,
	lsys ipld.LinkSystem,
	epochs []types.EpochResult,
) (cid.Cid, error) {
	node, err := qp.BuildMap(basicnode.Prototype.Map, 1, func(ma ipld.MapAssembler) {
		qp.MapEntry(ma, "epochs", qp.Map(int64(len(epochs)), func(ema ipld.MapAssembler) {
			for _, e := range epochs {
				key := strconv.FormatUint(e.Epoch, 10)
				qp.MapEntry(ema, key, qp.Link(cidlink.Link{Cid: e.CID}))
			}
		}))
	})
	if err != nil {
		return cid.Undef, fmt.Errorf("builder: build epoch page node: %w", err)
	}

	lnk, err := lsys.Store(
		ipld.LinkContext{Ctx: ctx},
		linkProtoDagCBOR,
		node,
	)
	if err != nil {
		return cid.Undef, fmt.Errorf("builder: store epoch page: %w", err)
	}

	return lnk.(cidlink.Link).Cid, nil
}
