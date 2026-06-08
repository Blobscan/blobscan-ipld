// Package ipfs provides a thin client for interacting with an IPFS node via
// its HTTP RPC API (Kubo-compatible). It handles block-level uploads, pinning,
// and IPNS publishing.
package ipfs

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"mime/multipart"
	"net"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
	"golang.org/x/sync/errgroup"

	"github.com/blobscan/blobscan-ipld/store"
)

// Client is a minimal Kubo HTTP RPC client.
type Client struct {
	base    string // e.g. "http://127.0.0.1:5001"
	http    *http.Client
	pinHTTP *http.Client // no hard Timeout; pin duration is controlled via context
	timeout time.Duration
	pinOnAdd      bool
	uploadWorkers int
}

// NewClient creates a new IPFS HTTP RPC client.
// apiAddr should be a multiaddr string like "/ip4/127.0.0.1/tcp/5001" or a
// plain HTTP URL like "http://127.0.0.1:5001".
//
// uploadWorkers controls the fan-out of PutBlockstore (parallel block uploads).
// If uploadWorkers <= 0, it defaults to 1 (serial uploads, legacy behavior).
// The underlying http.Transport is sized to keep that many keepalive
// connections open per host so that uploads actually run in parallel rather
// than being serialized by Go's default MaxIdleConnsPerHost=2.
func NewClient(apiAddr string, timeout time.Duration, pinOnAdd bool, uploadWorkers int) (*Client, error) {
	base := normalizeAddr(apiAddr)
	if uploadWorkers <= 0 {
		uploadWorkers = 1
	}

	maxConns := uploadWorkers * 2
	transport := &http.Transport{
		Proxy: http.ProxyFromEnvironment,
		DialContext: (&net.Dialer{
			Timeout:   30 * time.Second,
			KeepAlive: 30 * time.Second,
		}).DialContext,
		ForceAttemptHTTP2:     true,
		MaxIdleConns:          maxConns,
		MaxIdleConnsPerHost:   maxConns,
		MaxConnsPerHost:       maxConns,
		IdleConnTimeout:       90 * time.Second,
		TLSHandshakeTimeout:   10 * time.Second,
		ExpectContinueTimeout: 1 * time.Second,
	}

	return &Client{
		base: base,
		http: &http.Client{Timeout: timeout, Transport: transport},
		// pinHTTP shares the transport but has no hard Timeout: recursive
		// pin/add can take far longer than the per-request IPFS_TIMEOUT (Kubo
		// walks and may fetch the whole DAG). Callers bound it via context.
		pinHTTP:       &http.Client{Transport: transport},
		timeout:       timeout,
		pinOnAdd:      pinOnAdd,
		uploadWorkers: uploadWorkers,
	}, nil
}

// normalizeAddr converts a multiaddr or plain URL to an HTTP base URL.
func normalizeAddr(addr string) string {
	if strings.HasPrefix(addr, "http://") || strings.HasPrefix(addr, "https://") {
		return strings.TrimRight(addr, "/")
	}
	// Minimal multiaddr parsing: /ip4/127.0.0.1/tcp/5001 → http://127.0.0.1:5001
	parts := strings.Split(strings.TrimPrefix(addr, "/"), "/")
	if len(parts) >= 4 {
		return fmt.Sprintf("http://%s:%s", parts[1], parts[3])
	}
	return "http://" + addr
}

// ─── Block API ────────────────────────────────────────────────────────────────

// HasBlock checks whether a block with the given CID exists locally on the IPFS
// node. It uses block/stat with offline=true, so it never triggers a network
// fetch — a false result means the block is genuinely absent from the local
// datastore, not merely unreachable.
func (c *Client) HasBlock(ctx context.Context, id cid.Cid) (bool, error) {
	endpoint := fmt.Sprintf("%s/api/v0/block/stat?arg=%s&offline=true", c.base, id.String())
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, nil)
	if err != nil {
		return false, fmt.Errorf("ipfs: build block/stat request: %w", err)
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return false, fmt.Errorf("ipfs: block/stat request: %w", err)
	}
	defer resp.Body.Close()
	io.Copy(io.Discard, resp.Body) //nolint:errcheck
	return resp.StatusCode == http.StatusOK, nil
}

// ListRecursivePins returns the set of recursively-pinned CIDs on the node,
// keyed by their canonical cid.Cid string. It issues a single
// /api/v0/pin/ls?type=recursive request, so checking many CIDs against the
// returned set is far cheaper than one block/stat per CID.
//
// CIDs are re-encoded through cid.Decode so the keys match regardless of the
// base/version the node reports them in; callers should look up using the same
// canonical form (see cid.Cid.String after Decode).
func (c *Client) ListRecursivePins(ctx context.Context) (map[string]struct{}, error) {
	endpoint := fmt.Sprintf("%s/api/v0/pin/ls?type=recursive", c.base)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, nil)
	if err != nil {
		return nil, fmt.Errorf("ipfs: build pin/ls request: %w", err)
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return nil, fmt.Errorf("ipfs: pin/ls request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("ipfs: pin/ls HTTP %d: %s", resp.StatusCode, body)
	}

	var result struct {
		Keys map[string]struct {
			Type string `json:"Type"`
		} `json:"Keys"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, fmt.Errorf("ipfs: pin/ls decode: %w", err)
	}

	set := make(map[string]struct{}, len(result.Keys))
	for raw := range result.Keys {
		id, err := cid.Decode(raw)
		if err != nil {
			// Keep the raw form too so an unparseable key can still match a
			// caller that happens to compare raw strings.
			set[raw] = struct{}{}
			continue
		}
		set[id.String()] = struct{}{}
	}
	return set, nil
}

// PutBlock uploads a single raw block to the IPFS node using /api/v0/block/put.
// The CID codec and multihash are inferred from the block's CID prefix.
func (c *Client) PutBlock(ctx context.Context, blk blocks.Block) error {
	prefix := blk.Cid().Prefix()

	var body bytes.Buffer
	mw := multipart.NewWriter(&body)
	fw, err := mw.CreateFormFile("data", "blob")
	if err != nil {
		return fmt.Errorf("ipfs: create form file: %w", err)
	}
	if _, err := fw.Write(blk.RawData()); err != nil {
		return fmt.Errorf("ipfs: write block data: %w", err)
	}
	mw.Close()

	params := url.Values{}
	params.Set("cid-codec", codecName(prefix.Codec))
	params.Set("mhtype", mhName(prefix.MhType))
	params.Set("mhlen", fmt.Sprintf("%d", prefix.MhLength))
	params.Set("pin", fmt.Sprintf("%t", c.pinOnAdd))

	endpoint := fmt.Sprintf("%s/api/v0/block/put?%s", c.base, params.Encode())
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, &body)
	if err != nil {
		return fmt.Errorf("ipfs: build block/put request: %w", err)
	}
	req.Header.Set("Content-Type", mw.FormDataContentType())

	resp, err := c.http.Do(req)
	if err != nil {
		return fmt.Errorf("ipfs: block/put request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("ipfs: block/put HTTP %d: %s", resp.StatusCode, body)
	}
	return nil
}

// ProgressFunc is called during batch uploads with the current index and total count.
type ProgressFunc func(current, total int, blockCID string)

// PutBlockstore uploads all blocks from a MemBlockstore to the IPFS node.
// block/put is idempotent on Kubo — uploading an already-present block is a no-op.
// Uploads are fanned out across c.uploadWorkers goroutines; on the first error
// remaining uploads are cancelled and the error is returned.
// An optional ProgressFunc is called after each block is processed; for parallel
// uploads the (current, total) counter is monotonic but block order is not the
// blockstore order.
func (c *Client) PutBlockstore(ctx context.Context, bs *store.MemBlockstore, progress ...ProgressFunc) error {
	blks := bs.All()
	total := len(blks)
	if total == 0 {
		return nil
	}

	var fn ProgressFunc
	if len(progress) > 0 {
		fn = progress[0]
	}

	workers := c.uploadWorkers
	if workers <= 0 {
		workers = 1
	}
	if workers > total {
		workers = total
	}

	g, gctx := errgroup.WithContext(ctx)
	g.SetLimit(workers)

	var (
		mu   sync.Mutex
		done int
	)

	for i, blk := range blks {
		i, blk := i, blk
		g.Go(func() error {
			if err := c.PutBlock(gctx, blk); err != nil {
				return fmt.Errorf("ipfs: put block %d/%d (%s): %w", i+1, total, blk.Cid(), err)
			}
			if fn != nil {
				mu.Lock()
				done++
				cur := done
				mu.Unlock()
				fn(cur, total, blk.Cid().String())
			}
			return nil
		})
	}

	return g.Wait()
}

// GetBlock fetches a single raw block from the IPFS node using /api/v0/block/get.
func (c *Client) GetBlock(ctx context.Context, id cid.Cid) (blocks.Block, error) {
	endpoint := fmt.Sprintf("%s/api/v0/block/get?arg=%s", c.base, id.String())
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, nil)
	if err != nil {
		return nil, fmt.Errorf("ipfs: build block/get request: %w", err)
	}

	resp, err := c.http.Do(req)
	if err != nil {
		return nil, fmt.Errorf("ipfs: block/get request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("ipfs: block/get HTTP %d: %s", resp.StatusCode, body)
	}

	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("ipfs: block/get read body: %w", err)
	}

	return blocks.NewBlockWithCid(data, id)
}

// GetBlocks fetches multiple blocks from IPFS and puts them into bs.
func (c *Client) GetBlocks(ctx context.Context, bs *store.MemBlockstore, cids []cid.Cid) error {
	for _, id := range cids {
		blk, err := c.GetBlock(ctx, id)
		if err != nil {
			return fmt.Errorf("ipfs: get block %s: %w", id, err)
		}
		if err := bs.Put(ctx, blk); err != nil {
			return fmt.Errorf("ipfs: put block %s into store: %w", id, err)
		}
	}
	return nil
}

// ─── Pin API ──────────────────────────────────────────────────────────────────

// Pin recursively pins a CID on the IPFS node. It uses pinHTTP, which has no
// hard client-side timeout — recursive pinning can be slow — so bound the call
// with a context deadline if you need an upper limit.
func (c *Client) Pin(ctx context.Context, c2 cid.Cid) error {
	endpoint := fmt.Sprintf("%s/api/v0/pin/add?arg=%s&recursive=true", c.base, c2.String())
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, nil)
	if err != nil {
		return fmt.Errorf("ipfs: build pin/add request: %w", err)
	}

	resp, err := c.pinHTTP.Do(req)
	if err != nil {
		return fmt.Errorf("ipfs: pin/add request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("ipfs: pin/add HTTP %d: %s", resp.StatusCode, body)
	}
	return nil
}

// ─── DAG API ──────────────────────────────────────────────────────────────────

// DagStat returns the cumulative size of a DAG rooted at the given CID.
func (c *Client) DagStat(ctx context.Context, root cid.Cid) (uint64, error) {
	endpoint := fmt.Sprintf("%s/api/v0/dag/stat?arg=%s", c.base, root.String())
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, nil)
	if err != nil {
		return 0, fmt.Errorf("ipfs: build dag/stat request: %w", err)
	}

	resp, err := c.http.Do(req)
	if err != nil {
		return 0, fmt.Errorf("ipfs: dag/stat request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return 0, fmt.Errorf("ipfs: dag/stat HTTP %d: %s", resp.StatusCode, body)
	}

	var result struct {
		Size uint64 `json:"Size"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return 0, fmt.Errorf("ipfs: dag/stat decode: %w", err)
	}
	return result.Size, nil
}

// ─── IPNS API ─────────────────────────────────────────────────────────────────

// IPNSPublishResult is the response from /api/v0/name/publish.
type IPNSPublishResult struct {
	Name  string `json:"Name"`  // IPNS key (e.g. "k51q...")
	Value string `json:"Value"` // published path (e.g. "/ipfs/<CID>")
}

// PublishIPNS publishes a CID under the given IPNS key name.
func (c *Client) PublishIPNS(ctx context.Context, keyName string, target cid.Cid, ttl, lifetime time.Duration) (*IPNSPublishResult, error) {
	params := url.Values{}
	params.Set("arg", "/ipfs/"+target.String())
	params.Set("key", keyName)
	params.Set("ttl", ttl.String())
	params.Set("lifetime", lifetime.String())
	params.Set("resolve", "false")

	endpoint := fmt.Sprintf("%s/api/v0/name/publish?%s", c.base, params.Encode())
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, nil)
	if err != nil {
		return nil, fmt.Errorf("ipfs: build name/publish request: %w", err)
	}

	resp, err := c.http.Do(req)
	if err != nil {
		return nil, fmt.Errorf("ipfs: name/publish request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("ipfs: name/publish HTTP %d: %s", resp.StatusCode, body)
	}

	var result IPNSPublishResult
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, fmt.Errorf("ipfs: name/publish decode: %w", err)
	}
	return &result, nil
}

// ResolveIPNS resolves an IPNS name to its current CID path.
func (c *Client) ResolveIPNS(ctx context.Context, ipnsName string) (string, error) {
	endpoint := fmt.Sprintf("%s/api/v0/name/resolve?arg=%s", c.base, url.QueryEscape(ipnsName))
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, nil)
	if err != nil {
		return "", fmt.Errorf("ipfs: build name/resolve request: %w", err)
	}

	resp, err := c.http.Do(req)
	if err != nil {
		return "", fmt.Errorf("ipfs: name/resolve request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return "", fmt.Errorf("ipfs: name/resolve HTTP %d: %s", resp.StatusCode, body)
	}

	var result struct {
		Path string `json:"Path"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return "", fmt.Errorf("ipfs: name/resolve decode: %w", err)
	}
	return result.Path, nil
}

// KeyList returns all key names known to the IPFS node.
func (c *Client) KeyList(ctx context.Context) ([]string, error) {
	endpoint := fmt.Sprintf("%s/api/v0/key/list", c.base)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, nil)
	if err != nil {
		return nil, fmt.Errorf("ipfs: build key/list request: %w", err)
	}

	resp, err := c.http.Do(req)
	if err != nil {
		return nil, fmt.Errorf("ipfs: key/list request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("ipfs: key/list HTTP %d: %s", resp.StatusCode, body)
	}

	var result struct {
		Keys []struct {
			Name string `json:"Name"`
			ID   string `json:"Id"`
		} `json:"Keys"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, fmt.Errorf("ipfs: key/list decode: %w", err)
	}

	names := make([]string, len(result.Keys))
	for i, k := range result.Keys {
		names[i] = k.Name
	}
	return names, nil
}

// ─── Codec / multihash name helpers ──────────────────────────────────────────

func codecName(codec uint64) string {
	switch codec {
	case 0x55:
		return "raw"
	case 0x71:
		return "dag-cbor"
	case 0x70:
		return "dag-pb"
	default:
		return fmt.Sprintf("0x%x", codec)
	}
}

func mhName(mhType uint64) string {
	switch mhType {
	case 0x12:
		return "sha2-256"
	case 0x14:
		return "sha3-512"
	case 0x1b:
		return "keccak-256"
	default:
		return fmt.Sprintf("0x%x", mhType)
	}
}
