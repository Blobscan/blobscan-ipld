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

// BuildRangeNode constructs a dag-cbor node that acts as the root of a
// multi-epoch CAR export. It contains a map from epoch number (decimal string)
// to EpochNode CID, mirroring the relevant slice of NetworkRoot.epochs.
//
// epochs must be the subset of epochs being exported, sorted by epoch number.
func BuildRangeNode(
	ctx context.Context,
	lsys ipld.LinkSystem,
	network string,
	epochs []types.EpochResult,
) (types.EpochResult, error) {
	sorted := make([]types.EpochResult, len(epochs))
	copy(sorted, epochs)
	sort.Slice(sorted, func(i, j int) bool {
		return sorted[i].Epoch < sorted[j].Epoch
	})

	var totalSize int64
	for _, e := range sorted {
		totalSize += e.ApproximateSizeBytes
	}

	firstEpoch := sorted[0].Epoch
	lastEpoch := sorted[len(sorted)-1].Epoch

	node, err := qp.BuildMap(basicnode.Prototype.Map, 5, func(ma ipld.MapAssembler) {
		qp.MapEntry(ma, "network",        qp.String(network))
		qp.MapEntry(ma, "firstEpoch",     qp.Int(int64(firstEpoch)))
		qp.MapEntry(ma, "lastEpoch",      qp.Int(int64(lastEpoch)))
		qp.MapEntry(ma, "totalSizeBytes", qp.Int(totalSize))
		qp.MapEntry(ma, "epochs", qp.Map(int64(len(sorted)), func(ema ipld.MapAssembler) {
			for _, e := range sorted {
				key := strconv.FormatUint(e.Epoch, 10)
				qp.MapEntry(ema, key, qp.Link(cidlink.Link{Cid: e.CID}))
			}
		}))
	})
	if err != nil {
		return types.EpochResult{}, fmt.Errorf("builder: build range node: %w", err)
	}

	lnk, err := lsys.Store(ipld.LinkContext{Ctx: ctx}, linkProtoDagCBOR, node)
	if err != nil {
		return types.EpochResult{}, fmt.Errorf("builder: store range node: %w", err)
	}

	return types.EpochResult{
		Epoch:                firstEpoch,
		CID:                  lnk.(cidlink.Link).Cid,
		ApproximateSizeBytes: totalSize,
	}, nil
}
