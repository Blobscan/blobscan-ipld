package beacon

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// elTestServers spins up a fake beacon REST endpoint (for the execution payload
// lookup) and a fake EL JSON-RPC endpoint, returning a wired ExecutionClient.
func elTestServers(t *testing.T, elBlockHash string, blockNumber string, txs []map[string]interface{}, elCalls *int32) *ExecutionClient {
	t.Helper()

	beaconSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !strings.Contains(r.URL.Path, "/eth/v2/beacon/blocks/") {
			http.Error(w, "unexpected path", http.StatusNotFound)
			return
		}
		resp := map[string]interface{}{
			"data": map[string]interface{}{
				"message": map[string]interface{}{
					"body": map[string]interface{}{
						"execution_payload": map[string]interface{}{
							"block_hash":   elBlockHash,
							"block_number": blockNumber,
						},
					},
				},
			},
		}
		_ = json.NewEncoder(w).Encode(resp)
	}))
	t.Cleanup(beaconSrv.Close)

	elSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if elCalls != nil {
			atomic.AddInt32(elCalls, 1)
		}
		resp := map[string]interface{}{
			"jsonrpc": "2.0",
			"id":      1,
			"result": map[string]interface{}{
				"transactions": txs,
			},
		}
		_ = json.NewEncoder(w).Encode(resp)
	}))
	t.Cleanup(elSrv.Close)

	bc := NewClient(beaconSrv.URL, 5*time.Second, 1, 0, 1, 0, 32, 12)
	return NewExecutionClient(bc, elSrv.URL, 5*time.Second)
}

func TestExecutionClient_GetBlobTxData(t *testing.T) {
	const vhashA = "0x01aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	const vhashB = "0x01bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"

	txs := []map[string]interface{}{
		{"hash": "0xdeadbeef", "type": "0x2"}, // non-blob tx, ignored
		{
			"hash":                "0xtxA",
			"type":                "0x3",
			"blobVersionedHashes": []string{vhashA, vhashB},
		},
	}

	var elCalls int32
	ec := elTestServers(t, "0xblockhash", "12345", txs, &elCalls)

	// Matching the second versioned hash should resolve to the same tx.
	got, err := ec.GetBlobTxData(context.Background(), "0xblockroot", strings.ToUpper(vhashB))
	if err != nil {
		t.Fatalf("GetBlobTxData: %v", err)
	}
	if got.TxHash != "0xtxA" {
		t.Errorf("TxHash = %q, want 0xtxA", got.TxHash)
	}
	if got.BlockHash != "0xblockhash" {
		t.Errorf("BlockHash = %q, want 0xblockhash", got.BlockHash)
	}
	if got.BlockNumber != 12345 {
		t.Errorf("BlockNumber = %d, want 12345", got.BlockNumber)
	}

	// A second lookup against the same block root must be served from cache.
	if _, err := ec.GetBlobTxData(context.Background(), "0xblockroot", vhashA); err != nil {
		t.Fatalf("second GetBlobTxData: %v", err)
	}
	if n := atomic.LoadInt32(&elCalls); n != 1 {
		t.Errorf("EL endpoint called %d times, want 1 (cache miss only once)", n)
	}
}

func TestExecutionClient_VersionedHashNotFound(t *testing.T) {
	txs := []map[string]interface{}{
		{
			"hash":                "0xtxA",
			"type":                "0x3",
			"blobVersionedHashes": []string{"0x01cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"},
		},
	}
	ec := elTestServers(t, "0xblockhash", "1", txs, nil)

	_, err := ec.GetBlobTxData(context.Background(), "0xblockroot", "0x01dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd")
	if err == nil {
		t.Fatal("expected error for unknown versioned hash, got nil")
	}
}
