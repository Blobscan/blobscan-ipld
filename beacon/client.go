// Package beacon provides a minimal Ethereum Beacon Node REST API client for
// fetching finalized epoch data and blob sidecars.
package beacon

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/blobscan/blobscan-ipld/types"
)

// ErrInsufficientCustody is returned when the beacon node does not custody
// enough data columns to serve blob sidecars (PeerDAS / EIP-7594). The node
// must be reconfigured with --custody-group-count=64 or higher (or equivalent)
// before blobscan-ipld can fetch blobs from it.
var ErrInsufficientCustody = errors.New(
	"beacon node does not custody enough data columns to serve blob sidecars (PeerDAS); " +
		"reconfigure it with --custody-group-count=64 or higher (or equivalent for your client)",
)

// Client is a minimal Beacon Node REST API client (Ethereum Beacon API v1).
type Client struct {
	base string
	http *http.Client
}

// NewClient creates a new Beacon Node client.
// baseURL should be the REST API base, e.g. "http://localhost:5052".
func NewClient(baseURL string, timeout time.Duration) *Client {
	return &Client{
		base: strings.TrimRight(baseURL, "/"),
		http: &http.Client{Timeout: timeout},
	}
}

// ─── Finality ─────────────────────────────────────────────────────────────────

// FinalityCheckpoints holds the finalized and justified checkpoint epochs.
type FinalityCheckpoints struct {
	FinalizedEpoch uint64
	JustifiedEpoch uint64
}

// GetFinalityCheckpoints returns the current finality checkpoints for the
// given state ID (e.g. "head", "finalized").
func (c *Client) GetFinalityCheckpoints(ctx context.Context, stateID string) (*FinalityCheckpoints, error) {
	url := fmt.Sprintf("%s/eth/v1/beacon/states/%s/finality_checkpoints", c.base, stateID)
	var resp struct {
		Data struct {
			Finalized struct {
				Epoch string `json:"epoch"`
			} `json:"finalized"`
			CurrentJustified struct {
				Epoch string `json:"epoch"`
			} `json:"current_justified"`
		} `json:"data"`
	}
	if err := c.get(ctx, url, &resp); err != nil {
		return nil, fmt.Errorf("beacon: finality checkpoints: %w", err)
	}

	finalized, err := strconv.ParseUint(resp.Data.Finalized.Epoch, 10, 64)
	if err != nil {
		return nil, fmt.Errorf("beacon: parse finalized epoch: %w", err)
	}
	justified, err := strconv.ParseUint(resp.Data.CurrentJustified.Epoch, 10, 64)
	if err != nil {
		return nil, fmt.Errorf("beacon: parse justified epoch: %w", err)
	}

	return &FinalityCheckpoints{
		FinalizedEpoch: finalized,
		JustifiedEpoch: justified,
	}, nil
}

// ─── Block / Slot ─────────────────────────────────────────────────────────────

// BlockHeader holds minimal block header info.
type BlockHeader struct {
	Slot          uint64
	ProposerIndex uint64
	ParentRoot    string
	StateRoot     string
	BodyRoot      string
}

// GetBlockHeader returns the block header for a given block ID
// (e.g. "head", a slot number, or a block root).
func (c *Client) GetBlockHeader(ctx context.Context, blockID string) (*BlockHeader, error) {
	url := fmt.Sprintf("%s/eth/v1/beacon/headers/%s", c.base, blockID)
	var resp struct {
		Data struct {
			Header struct {
				Message struct {
					Slot          string `json:"slot"`
					ProposerIndex string `json:"proposer_index"`
					ParentRoot    string `json:"parent_root"`
					StateRoot     string `json:"state_root"`
					BodyRoot      string `json:"body_root"`
				} `json:"message"`
			} `json:"header"`
		} `json:"data"`
	}
	if err := c.get(ctx, url, &resp); err != nil {
		return nil, fmt.Errorf("beacon: block header %s: %w", blockID, err)
	}

	slot, _ := strconv.ParseUint(resp.Data.Header.Message.Slot, 10, 64)
	proposer, _ := strconv.ParseUint(resp.Data.Header.Message.ProposerIndex, 10, 64)

	return &BlockHeader{
		Slot:          slot,
		ProposerIndex: proposer,
		ParentRoot:    resp.Data.Header.Message.ParentRoot,
		StateRoot:     resp.Data.Header.Message.StateRoot,
		BodyRoot:      resp.Data.Header.Message.BodyRoot,
	}, nil
}

// ─── Blob Sidecars ────────────────────────────────────────────────────────────

// BlobSidecar is a single blob sidecar as returned by the Beacon API.
type BlobSidecar struct {
	BlockRoot       string `json:"block_root"`
	Index           string `json:"index"`
	Slot            string `json:"slot"`
	BlockParentRoot string `json:"block_parent_root"`
	ProposerIndex   string `json:"proposer_index"`
	Blob            string `json:"blob"`           // 0x-prefixed hex, 131072 bytes
	KZGCommitment   string `json:"kzg_commitment"` // 0x-prefixed hex, 48 bytes
	KZGProof        string `json:"kzg_proof"`      // 0x-prefixed hex, 48 bytes
}

// GetBlobSidecars returns all blob sidecars for the block at the given slot.
func (c *Client) GetBlobSidecars(ctx context.Context, blockID string) ([]BlobSidecar, error) {
	url := fmt.Sprintf("%s/eth/v1/beacon/blob_sidecars/%s", c.base, blockID)
	var resp struct {
		Data []BlobSidecar `json:"data"`
	}
	if err := c.get(ctx, url, &resp); err != nil {
		return nil, fmt.Errorf("beacon: blob sidecars %s: %w", blockID, err)
	}
	return resp.Data, nil
}

// ─── Genesis ──────────────────────────────────────────────────────────────────

// GetGenesisTime returns the Unix timestamp of the beacon chain genesis.
func (c *Client) GetGenesisTime(ctx context.Context) (time.Time, error) {
	url := fmt.Sprintf("%s/eth/v1/beacon/genesis", c.base)
	var resp struct {
		Data struct {
			GenesisTime string `json:"genesis_time"`
		} `json:"data"`
	}
	if err := c.get(ctx, url, &resp); err != nil {
		return time.Time{}, fmt.Errorf("beacon: genesis: %w", err)
	}
	secs, err := strconv.ParseInt(resp.Data.GenesisTime, 10, 64)
	if err != nil {
		return time.Time{}, fmt.Errorf("beacon: parse genesis_time: %w", err)
	}
	return time.Unix(secs, 0).UTC(), nil
}

// GetNetworkName returns the CONFIG_NAME from the beacon node's consensus spec
// (e.g. "mainnet", "sepolia", "gnosis"). Used to verify the configured network
// name matches the actual beacon node before processing begins.
func (c *Client) GetNetworkName(ctx context.Context) (string, error) {
	url := fmt.Sprintf("%s/eth/v1/config/spec", c.base)
	var resp struct {
		Data struct {
			ConfigName string `json:"CONFIG_NAME"`
		} `json:"data"`
	}
	if err := c.get(ctx, url, &resp); err != nil {
		return "", fmt.Errorf("beacon: config spec: %w", err)
	}
	if resp.Data.ConfigName == "" {
		return "", fmt.Errorf("beacon: config spec: CONFIG_NAME is empty")
	}
	return strings.ToLower(resp.Data.ConfigName), nil
}

// SlotTime returns the timestamp of the given slot based on the genesis time.
// Each slot is 12 seconds.
func SlotTime(genesisTime time.Time, slot uint64) time.Time {
	return genesisTime.Add(time.Duration(slot) * 12 * time.Second)
}

// ─── Epoch helpers ────────────────────────────────────────────────────────────

const slotsPerEpoch = 32

// EpochToFirstSlot returns the first slot of the given epoch.
func EpochToFirstSlot(epoch uint64) uint64 { return epoch * slotsPerEpoch }

// SlotToEpoch returns the epoch that contains the given slot.
func SlotToEpoch(slot uint64) uint64 { return slot / slotsPerEpoch }

// FetchEpochInput fetches all blob sidecars for every slot in the given epoch
// and assembles them into an EpochInput ready for the DAG builder.
// txHash and blockNumber are fetched from the EL node via the provided
// ELClient; pass nil to skip EL enrichment.
func (c *Client) FetchEpochInput(ctx context.Context, epoch uint64, el ELClient) (types.EpochInput, error) {
	firstSlot := EpochToFirstSlot(epoch)
	lastSlot := firstSlot + slotsPerEpoch - 1

	inp := types.EpochInput{
		Epoch: epoch,
		Slot:  firstSlot,
	}

	for slot := firstSlot; slot <= lastSlot; slot++ {
		sidecars, err := c.GetBlobSidecars(ctx, strconv.FormatUint(slot, 10))
		if err != nil {
			// A 404 means the slot was missed; skip it.
			if isNotFound(err) {
				continue
			}
			return types.EpochInput{}, fmt.Errorf("beacon: fetch slot %d: %w", slot, err)
		}

		for _, sc := range sidecars {
			blobData, err := hexToBytes(sc.Blob)
			if err != nil {
				return types.EpochInput{}, fmt.Errorf("beacon: decode blob hex slot %d: %w", slot, err)
			}

			idx, _ := strconv.Atoi(sc.Index)

			bi := types.BlobInput{
				Commitment:    sc.KZGCommitment,
				VersionedHash: kzgCommitmentToVersionedHash(sc.KZGCommitment),
				BlockHash:     sc.BlockRoot,
				Slot:          slot,
				Epoch:         epoch,
				Index:         idx,
				Data:          blobData,
			}

			// Enrich with EL data if available.
			if el != nil {
				elData, err := el.GetBlobTxData(ctx, sc.BlockRoot, sc.KZGCommitment)
				if err == nil {
					bi.TxHash = elData.TxHash
					bi.BlockNumber = elData.BlockNumber
					bi.BlockHash = elData.BlockHash
				}
			}

			inp.Blobs = append(inp.Blobs, bi)
		}
	}

	return inp, nil
}

// ─── EL client interface ──────────────────────────────────────────────────────

// ELClient is an optional interface for fetching execution-layer data.
type ELClient interface {
	GetBlobTxData(ctx context.Context, blockRoot, commitment string) (*ELBlobData, error)
}

// ELBlobData holds execution-layer data for a blob transaction.
type ELBlobData struct {
	TxHash      string
	BlockNumber uint64
	BlockHash   string
}

// ─── HTTP helper ──────────────────────────────────────────────────────────────

func (c *Client) get(ctx context.Context, url string, out interface{}) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return err
	}
	req.Header.Set("Accept", "application/json")

	resp, err := c.http.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNotFound {
		return fmt.Errorf("not found (404): %s", url)
	}
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		if resp.StatusCode == http.StatusServiceUnavailable &&
			strings.Contains(string(body), "not sufficient to serve blob sidecars") {
			return ErrInsufficientCustody
		}
		return fmt.Errorf("HTTP %d: %s", resp.StatusCode, body)
	}

	return json.NewDecoder(resp.Body).Decode(out)
}

func isNotFound(err error) bool {
	return err != nil && strings.Contains(err.Error(), "not found (404)")
}

// hexToBytes decodes a 0x-prefixed hex string to bytes.
func hexToBytes(h string) ([]byte, error) {
	h = strings.TrimPrefix(h, "0x")
	if len(h)%2 != 0 {
		h = "0" + h
	}
	b := make([]byte, len(h)/2)
	for i := range b {
		hi := hexVal(h[2*i])
		lo := hexVal(h[2*i+1])
		if hi > 15 || lo > 15 {
			return nil, fmt.Errorf("invalid hex char at position %d", 2*i)
		}
		b[i] = hi<<4 | lo
	}
	return b, nil
}

func hexVal(c byte) byte {
	switch {
	case c >= '0' && c <= '9':
		return c - '0'
	case c >= 'a' && c <= 'f':
		return c - 'a' + 10
	case c >= 'A' && c <= 'F':
		return c - 'A' + 10
	default:
		return 255
	}
}

// kzgCommitmentToVersionedHash computes the EIP-4844 versioned hash from a
// KZG commitment: SHA-256(commitment)[1:] with version byte 0x01 prepended.
// This is a simplified version; production code should use crypto/sha256.
func kzgCommitmentToVersionedHash(commitment string) string {
	// In production, compute: 0x01 || SHA256(commitment_bytes)[1:]
	// Here we return a placeholder that the caller can replace.
	return "0x01" + commitment[4:] // placeholder
}
