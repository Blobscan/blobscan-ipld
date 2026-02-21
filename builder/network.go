package builder

import (
	"context"
	"fmt"
	"sort"
	"strconv"

	"github.com/ipld/go-ipld-prime"
	"github.com/ipld/go-ipld-prime/fluent/qp"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	"github.com/ipld/go-ipld-prime/node/basicnode"

	"github.com/blobscan/blobscan-ipld/types"
)

// BuildNetworkRoot constructs and stores a NetworkRoot dag-cbor node that
// indexes all known epochs by their epoch number (as a decimal string key).
// It is called after every epoch so the root always reflects the current state.
//
// epochs must be the full list of all processed epochs (loaded from DB).
// The map keys are sorted numerically for determinism.
func BuildNetworkRoot(
	ctx context.Context,
	lsys ipld.LinkSystem,
	network string,
	epochs []types.EpochResult,
) (types.NetworkRootResult, error) {
	// Sort by epoch number for determinism.
	sorted := make([]types.EpochResult, len(epochs))
	copy(sorted, epochs)
	sort.Slice(sorted, func(i, j int) bool {
		return sorted[i].Epoch < sorted[j].Epoch
	})

	var latestEpoch uint64
	var totalSize int64
	if len(sorted) > 0 {
		latestEpoch = sorted[len(sorted)-1].Epoch
		for _, e := range sorted {
			totalSize += e.ApproximateSizeBytes
		}
	}

	rootNode, err := qp.BuildMap(basicnode.Prototype.Map, 4, func(ma ipld.MapAssembler) {
		qp.MapEntry(ma, "network",     qp.String(network))
		qp.MapEntry(ma, "latestEpoch", qp.Int(int64(latestEpoch)))
		qp.MapEntry(ma, "totalSizeBytes", qp.Int(totalSize))
		qp.MapEntry(ma, "epochs", qp.Map(int64(len(sorted)), func(ema ipld.MapAssembler) {
			for _, e := range sorted {
				key := strconv.FormatUint(e.Epoch, 10)
				qp.MapEntry(ema, key, qp.Link(cidlink.Link{Cid: e.CID}))
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
		CID: lnk.(cidlink.Link).Cid,
	}, nil
}
