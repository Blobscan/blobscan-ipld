// Package api exposes an HTTP server that accepts blob data via POST requests
// and runs the same IPLD processing pipeline as the beacon-pull generator.
// This allows external systems to push blobs directly without requiring a
// beacon node connection.
package api

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"strings"
	"time"
)

// BlobPushRequest is the JSON body accepted by POST /blob.
type BlobPushRequest struct {
	// Commitment is the KZG commitment, 0x-prefixed hex (48 bytes = 96 hex chars).
	Commitment string `json:"commitment"`
	// VersionedHash is the EIP-4844 versioned hash, 0x-prefixed hex.
	VersionedHash string `json:"versioned_hash"`
	// Data is the raw blob field element data, 0x-prefixed hex (131072 bytes = 262144 hex chars).
	Data string `json:"data"`
	// TxHash is the execution-layer transaction hash (optional).
	TxHash string `json:"tx_hash,omitempty"`
	// BlockNumber is the EL block number (optional).
	BlockNumber uint64 `json:"block_number,omitempty"`
	// BlockHash is the EL block hash (optional).
	BlockHash string `json:"block_hash,omitempty"`
	// Slot is the beacon slot number.
	Slot uint64 `json:"slot"`
	// Epoch is the beacon epoch number.
	Epoch uint64 `json:"epoch"`
	// Index is the blob index within the transaction (0-based).
	Index int `json:"index"`
	// Finalize instructs the server to build the EpochNode and rebuild the
	// NetworkRoot immediately after storing this blob. Set to true on the last
	// blob of an epoch when the caller knows the epoch is complete.
	Finalize bool `json:"finalize,omitempty"`
}

// BlobPushResponse is returned on success.
type BlobPushResponse struct {
	DataCID    string `json:"data_cid"`
	MetaCID    string `json:"meta_cid"`
	Commitment string `json:"commitment"`
	Epoch      uint64 `json:"epoch"`
	// Finalized is true when FinalizeEpoch was run as part of this request.
	Finalized bool `json:"finalized,omitempty"`
	// EpochCID is the CID of the EpochNode, populated only when Finalized is true.
	EpochCID string `json:"epoch_cid,omitempty"`
}

// ErrorResponse is returned on failure.
type ErrorResponse struct {
	Error string `json:"error"`
}

// BlobProcessor is the function the API calls to handle a validated blob.
type BlobProcessor func(ctx context.Context, req BlobPushRequest) (BlobPushResponse, error)

// EpochFinalizer is the function the API calls to finalize an epoch and return
// its EpochNode CID string.
type EpochFinalizer func(ctx context.Context, epoch uint64) (epochCID string, err error)

// ─── Server ───────────────────────────────────────────────────────────────────

// Server is the HTTP API server.
type Server struct {
	processor BlobProcessor
	finalizer EpochFinalizer
	log       *slog.Logger
	srv       *http.Server
}

// New creates an API server.
// finalizer may be nil; if nil, finalization via POST /blob is disabled.
func New(
	addr string,
	processor BlobProcessor,
	finalizer EpochFinalizer,
	log *slog.Logger,
) *Server {
	s := &Server{
		processor: processor,
		finalizer: finalizer,
		log:       log,
	}

	mux := http.NewServeMux()
	mux.HandleFunc("POST /blob", s.handlePushBlob)
	mux.HandleFunc("GET /healthz", s.handleHealth)

	s.srv = &http.Server{
		Addr:         addr,
		Handler:      mux,
		ReadTimeout:  30 * time.Second,
		WriteTimeout: 0,
		IdleTimeout:  120 * time.Second,
	}

	return s
}

// ListenAndServe starts the HTTP server. It blocks until the server stops.
func (s *Server) ListenAndServe() error {
	s.log.Info("api server listening", "addr", s.srv.Addr)
	if err := s.srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		return fmt.Errorf("api: listen: %w", err)
	}
	return nil
}

// Shutdown gracefully stops the server.
func (s *Server) Shutdown(ctx context.Context) error {
	return s.srv.Shutdown(ctx)
}

// ─── Handlers ─────────────────────────────────────────────────────────────────

func (s *Server) handleHealth(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
}

func (s *Server) handlePushBlob(w http.ResponseWriter, r *http.Request) {
	var req BlobPushRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		s.writeError(w, http.StatusBadRequest, fmt.Sprintf("invalid JSON: %s", err))
		return
	}

	if err := validateBlobRequest(req); err != nil {
		s.writeError(w, http.StatusBadRequest, err.Error())
		return
	}

	resp, err := s.processor(r.Context(), req)
	if err != nil {
		s.log.Error("blob processing failed", "commitment", req.Commitment, "epoch", req.Epoch, "err", err)
		s.writeError(w, http.StatusInternalServerError, fmt.Sprintf("processing failed: %s", err))
		return
	}

	if req.Finalize && s.finalizer != nil {
		epochCID, err := s.finalizer(r.Context(), req.Epoch)
		if err != nil {
			s.log.Error("epoch finalization failed", "epoch", req.Epoch, "err", err)
			s.writeError(w, http.StatusInternalServerError, fmt.Sprintf("finalize epoch failed: %s", err))
			return
		}
		resp.Finalized = true
		resp.EpochCID = epochCID
		s.log.Info("epoch finalized via push", "epoch", req.Epoch, "cid", epochCID)
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	_ = json.NewEncoder(w).Encode(resp)
}

func (s *Server) writeError(w http.ResponseWriter, code int, msg string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(ErrorResponse{Error: msg})
}

// ─── Validation ───────────────────────────────────────────────────────────────

func validateBlobRequest(req BlobPushRequest) error {
	if req.Commitment == "" {
		return fmt.Errorf("commitment is required")
	}
	if req.VersionedHash == "" {
		return fmt.Errorf("versioned_hash is required")
	}
	if req.Data == "" {
		return fmt.Errorf("data is required")
	}
	raw, err := hexDecode(req.Data)
	if err != nil {
		return fmt.Errorf("data: invalid hex: %w", err)
	}
	if len(raw) != 131072 {
		return fmt.Errorf("data: expected 131072 bytes, got %d", len(raw))
	}
	if req.Epoch == 0 && req.Slot == 0 {
		return fmt.Errorf("epoch or slot is required")
	}
	return nil
}

// hexDecode accepts 0x-prefixed or plain hex strings.
func hexDecode(s string) ([]byte, error) {
	s = strings.TrimPrefix(s, "0x")
	return hex.DecodeString(s)
}
