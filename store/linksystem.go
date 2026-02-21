package store

import (
	"bytes"
	"fmt"
	"io"

	blocks "github.com/ipfs/go-block-format"
	"github.com/ipld/go-ipld-prime"
	"github.com/ipld/go-ipld-prime/codec"
	"github.com/ipld/go-ipld-prime/codec/dagcbor"
	"github.com/ipld/go-ipld-prime/datamodel"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	mc "github.com/multiformats/go-multicodec"
)

// NewLinkSystem returns an ipld.LinkSystem backed by the given MemBlockstore.
// Structured nodes use dag-cbor; raw blob bytes use codec=raw.
func NewLinkSystem(bs *MemBlockstore) ipld.LinkSystem {
	lsys := cidlink.DefaultLinkSystem()

	lsys.StorageWriteOpener = func(lctx ipld.LinkContext) (io.Writer, ipld.BlockWriteCommitter, error) {
		buf := new(bytes.Buffer)
		return buf, func(lnk ipld.Link) error {
			c := lnk.(cidlink.Link).Cid
			raw := make([]byte, buf.Len())
			copy(raw, buf.Bytes())
			blk, err := blocks.NewBlockWithCid(raw, c)
			if err != nil {
				return fmt.Errorf("linksystem: create block %s: %w", c, err)
			}
			return bs.Put(lctx.Ctx, blk)
		}, nil
	}

	lsys.StorageReadOpener = func(lctx ipld.LinkContext, lnk ipld.Link) (io.Reader, error) {
		c := lnk.(cidlink.Link).Cid
		blk, err := bs.Get(lctx.Ctx, c)
		if err != nil {
			return nil, fmt.Errorf("linksystem: get block %s: %w", c, err)
		}
		return bytes.NewReader(blk.RawData()), nil
	}

	lsys.EncoderChooser = func(lp datamodel.LinkPrototype) (codec.Encoder, error) {
		lpp, ok := lp.(cidlink.LinkPrototype)
		if !ok {
			return nil, fmt.Errorf("linksystem: unsupported link prototype %T", lp)
		}
		switch mc.Code(lpp.Prefix.Codec) {
		case mc.DagCbor:
			return dagcbor.Encode, nil
		case mc.Raw:
			return rawEncode, nil
		default:
			return nil, fmt.Errorf("linksystem: unsupported encoder codec 0x%x", lpp.Prefix.Codec)
		}
	}

	lsys.DecoderChooser = func(lnk ipld.Link) (ipld.Decoder, error) {
		lnkCID := lnk.(cidlink.Link).Cid
		switch mc.Code(lnkCID.Prefix().Codec) {
		case mc.DagCbor:
			return dagcbor.Decode, nil
		case mc.Raw:
			return rawDecode, nil
		default:
			return nil, fmt.Errorf("linksystem: unsupported decoder codec 0x%x", lnkCID.Prefix().Codec)
		}
	}

	return lsys
}

// rawEncode writes the node's bytes representation directly (codec=raw).
func rawEncode(n ipld.Node, w io.Writer) error {
	b, err := n.AsBytes()
	if err != nil {
		return err
	}
	_, err = w.Write(b)
	return err
}

// rawDecode reads all bytes and returns a bytes node (codec=raw).
func rawDecode(na ipld.NodeAssembler, r io.Reader) error {
	data, err := io.ReadAll(r)
	if err != nil {
		return err
	}
	return na.AssignBytes(data)
}
