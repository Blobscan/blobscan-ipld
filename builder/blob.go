// Package builder contains functions that construct IPLD nodes from raw domain
// types and store them in a MemBlockstore via a LinkSystem.
package builder

import (
	"context"
	"fmt"

	"github.com/ipfs/go-cid"
	"github.com/ipld/go-ipld-prime"
	"github.com/ipld/go-ipld-prime/fluent/qp"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	"github.com/ipld/go-ipld-prime/node/basicnode"
	mc "github.com/multiformats/go-multicodec"
	mh "github.com/multiformats/go-multihash"

	"github.com/blobscan/blobscan-ipld/types"
)

// linkProtoRaw is the CID link prototype for raw blob data.
// codec=raw, hash=sha2-256.
var linkProtoRaw = cidlink.LinkPrototype{
	Prefix: cid.Prefix{
		Version:  1,
		Codec:    uint64(mc.Raw),
		MhType:   mh.SHA2_256,
		MhLength: 32,
	},
}

// linkProtoDagCBOR is the CID link prototype for structured IPLD nodes.
// codec=dag-cbor, hash=sha2-256.
var linkProtoDagCBOR = cidlink.LinkPrototype{
	Prefix: cid.Prefix{
		Version:  1,
		Codec:    uint64(mc.DagCbor),
		MhType:   mh.SHA2_256,
		MhLength: 32,
	},
}

// StoreRawBlob stores the raw 128 KiB blob bytes with codec=raw and returns
// its CID. The same blob data always produces the same CID (deterministic).
func StoreRawBlob(ctx context.Context, lsys ipld.LinkSystem, data []byte) (cid.Cid, error) {
	n := basicnode.NewBytes(data)
	lnk, err := lsys.Store(
		ipld.LinkContext{Ctx: ctx},
		linkProtoRaw,
		n,
	)
	if err != nil {
		return cid.Undef, fmt.Errorf("builder: store raw blob: %w", err)
	}
	return lnk.(cidlink.Link).Cid, nil
}

// StoreBlobMetadata builds and stores a BlobMetadata dag-cbor node.
// The map keys are sorted lexicographically to guarantee determinism.
func StoreBlobMetadata(ctx context.Context, lsys ipld.LinkSystem, inp types.BlobInput, dataCID cid.Cid) (cid.Cid, error) {
	dataLink := cidlink.Link{Cid: dataCID}

	n, err := qp.BuildMap(basicnode.Prototype.Map, 10, func(ma ipld.MapAssembler) {
		qp.MapEntry(ma, "commitment",    qp.String(inp.Commitment))
		qp.MapEntry(ma, "versionedHash", qp.String(inp.VersionedHash))
		qp.MapEntry(ma, "txHash",        qp.String(inp.TxHash))
		qp.MapEntry(ma, "blockNumber",   qp.Int(int64(inp.BlockNumber)))
		qp.MapEntry(ma, "blockHash",     qp.String(inp.BlockHash))
		qp.MapEntry(ma, "slot",          qp.Int(int64(inp.Slot)))
		qp.MapEntry(ma, "epoch",         qp.Int(int64(inp.Epoch)))
		qp.MapEntry(ma, "index",         qp.Int(int64(inp.Index)))
		qp.MapEntry(ma, "size",          qp.Int(int64(len(inp.Data))))
		qp.MapEntry(ma, "data",          qp.Link(dataLink))
	})
	if err != nil {
		return cid.Undef, fmt.Errorf("builder: build blob metadata node: %w", err)
	}

	lnk, err := lsys.Store(
		ipld.LinkContext{Ctx: ctx},
		linkProtoDagCBOR,
		n,
	)
	if err != nil {
		return cid.Undef, fmt.Errorf("builder: store blob metadata: %w", err)
	}
	return lnk.(cidlink.Link).Cid, nil
}

// ProcessBlob is a convenience wrapper that stores both the raw blob and its
// metadata, returning a BlobResult with both CIDs.
func ProcessBlob(ctx context.Context, lsys ipld.LinkSystem, inp types.BlobInput) (types.BlobResult, error) {
	dataCID, err := StoreRawBlob(ctx, lsys, inp.Data)
	if err != nil {
		return types.BlobResult{}, fmt.Errorf("builder: process blob %s: %w", inp.Commitment, err)
	}

	metaCID, err := StoreBlobMetadata(ctx, lsys, inp, dataCID)
	if err != nil {
		return types.BlobResult{}, fmt.Errorf("builder: process blob metadata %s: %w", inp.Commitment, err)
	}

	return types.BlobResult{
		Commitment: inp.Commitment,
		DataCID:    dataCID,
		MetaCID:    metaCID,
		SizeBytes:  int64(len(inp.Data)),
	}, nil
}
