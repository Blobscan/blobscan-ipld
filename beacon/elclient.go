package beacon

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"time"
)

// ExecutionClient implements ELClient against an execution-layer JSON-RPC
// endpoint (e.g. Geth/Nethermind).
//
// Blob sidecars only expose the beacon block root, not the execution block, so
// resolving a blob to its execution-layer transaction takes two steps:
//
//  1. Ask the beacon node for the block root's execution payload (EL block hash
//     + number).
//  2. Call eth_getBlockByHash on the EL node to fetch full transactions, then
//     match each type-3 (blob) transaction's blobVersionedHashes against the
//     blob's versioned hash.
//
// Results are cached per beacon block root so a block carrying many blobs is
// fetched at most once per process lifetime.
type ExecutionClient struct {
	beacon *Client
	rpcURL string
	http   *http.Client

	mu    sync.Mutex
	cache map[string]*blockBlobIndex // beacon block root → resolved EL block + blob tx map
}

// blockBlobIndex holds the resolved execution-layer data for one block, indexed
// by lowercased versioned hash.
type blockBlobIndex struct {
	blockHash   string
	blockNumber uint64
	txByVHash   map[string]string // versioned hash → tx hash
}

// NewExecutionClient creates an ExecutionClient. beacon resolves beacon block
// roots to EL block hashes; rpcURL is the execution-layer JSON-RPC endpoint.
func NewExecutionClient(beacon *Client, rpcURL string, timeout time.Duration) *ExecutionClient {
	if timeout <= 0 {
		timeout = 30 * time.Second
	}
	return &ExecutionClient{
		beacon: beacon,
		rpcURL: rpcURL,
		http:   &http.Client{Timeout: timeout},
		cache:  make(map[string]*blockBlobIndex),
	}
}

// GetBlobTxData returns the execution-layer transaction data for the blob with
// the given versioned hash included in the beacon block at blockRoot.
func (e *ExecutionClient) GetBlobTxData(ctx context.Context, blockRoot, versionedHash string) (*ELBlobData, error) {
	idx, err := e.blockIndex(ctx, blockRoot)
	if err != nil {
		return nil, err
	}

	txHash, ok := idx.txByVHash[strings.ToLower(versionedHash)]
	if !ok {
		return nil, fmt.Errorf("el: versioned hash %s not found in block %s", versionedHash, idx.blockHash)
	}

	return &ELBlobData{
		TxHash:      txHash,
		BlockNumber: idx.blockNumber,
		BlockHash:   idx.blockHash,
	}, nil
}

// blockIndex returns the cached (or freshly fetched) blob index for a beacon
// block root.
func (e *ExecutionClient) blockIndex(ctx context.Context, blockRoot string) (*blockBlobIndex, error) {
	e.mu.Lock()
	if idx, ok := e.cache[blockRoot]; ok {
		e.mu.Unlock()
		return idx, nil
	}
	e.mu.Unlock()

	// Resolve the beacon block root to an execution-layer block hash + number.
	payload, err := e.beacon.GetExecutionPayloadInfo(ctx, blockRoot)
	if err != nil {
		return nil, fmt.Errorf("el: resolve execution payload for %s: %w", blockRoot, err)
	}

	idx, err := e.buildBlockIndex(ctx, payload)
	if err != nil {
		return nil, err
	}

	e.mu.Lock()
	e.cache[blockRoot] = idx
	e.mu.Unlock()
	return idx, nil
}

// rpcTransaction is the subset of an eth_getBlockByHash transaction we need.
type rpcTransaction struct {
	Hash                string   `json:"hash"`
	Type                string   `json:"type"`
	BlobVersionedHashes []string `json:"blobVersionedHashes"`
}

// buildBlockIndex fetches the EL block and indexes blob-carrying transactions by
// versioned hash.
func (e *ExecutionClient) buildBlockIndex(ctx context.Context, payload *ExecutionPayloadInfo) (*blockBlobIndex, error) {
	var result struct {
		Transactions []rpcTransaction `json:"transactions"`
	}
	if err := e.call(ctx, "eth_getBlockByHash", []interface{}{payload.BlockHash, true}, &result); err != nil {
		return nil, fmt.Errorf("el: eth_getBlockByHash %s: %w", payload.BlockHash, err)
	}

	idx := &blockBlobIndex{
		blockHash:   payload.BlockHash,
		blockNumber: payload.BlockNumber,
		txByVHash:   make(map[string]string),
	}
	for _, tx := range result.Transactions {
		// Type 0x3 is EIP-4844 (blob) transactions; only these carry blobs.
		if len(tx.BlobVersionedHashes) == 0 {
			continue
		}
		for _, vh := range tx.BlobVersionedHashes {
			idx.txByVHash[strings.ToLower(vh)] = tx.Hash
		}
	}
	return idx, nil
}

// jsonRPCRequest is a single JSON-RPC 2.0 request.
type jsonRPCRequest struct {
	JSONRPC string        `json:"jsonrpc"`
	ID      int           `json:"id"`
	Method  string        `json:"method"`
	Params  []interface{} `json:"params"`
}

// jsonRPCResponse is a single JSON-RPC 2.0 response.
type jsonRPCResponse struct {
	Result json.RawMessage `json:"result"`
	Error  *struct {
		Code    int    `json:"code"`
		Message string `json:"message"`
	} `json:"error"`
}

// call performs a single JSON-RPC call and unmarshals the result into out.
func (e *ExecutionClient) call(ctx context.Context, method string, params []interface{}, out interface{}) error {
	reqBody, err := json.Marshal(jsonRPCRequest{
		JSONRPC: "2.0",
		ID:      1,
		Method:  method,
		Params:  params,
	})
	if err != nil {
		return err
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, e.rpcURL, bytes.NewReader(reqBody))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")

	resp, err := e.http.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("HTTP %d: %s", resp.StatusCode, body)
	}

	var rpcResp jsonRPCResponse
	if err := json.NewDecoder(resp.Body).Decode(&rpcResp); err != nil {
		return err
	}
	if rpcResp.Error != nil {
		return fmt.Errorf("rpc error %d: %s", rpcResp.Error.Code, rpcResp.Error.Message)
	}
	if len(rpcResp.Result) == 0 || string(rpcResp.Result) == "null" {
		return fmt.Errorf("rpc result is null")
	}
	return json.Unmarshal(rpcResp.Result, out)
}
