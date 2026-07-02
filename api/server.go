// Package api exposes an HTTP server that accepts blob data via POST requests
// and runs the same IPLD processing pipeline as the beacon-pull generator.
// This allows external systems to push blobs directly without requiring a
// beacon node connection.
package api

import (
	"context"
	_ "embed"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"time"
)

//go:embed openapi.json
var openapiSpec []byte

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

// ReadinessChecker reports whether the downstream dependencies the API relies on
// (notably the kubo node) are reachable. A nil error means ready.
type ReadinessChecker func(ctx context.Context) error

// HTTPObserver records one completed request's latency. It is called by the
// logging middleware once the response status and duration are known. path is
// the matched route pattern (not the raw URL) to keep label cardinality bounded.
type HTTPObserver func(method, path, status string, dur time.Duration)

// ─── Server ───────────────────────────────────────────────────────────────────

// Server is the HTTP API server.
type Server struct {
	processor BlobProcessor
	finalizer EpochFinalizer
	ready     ReadinessChecker
	observe   HTTPObserver
	log       *slog.Logger
	srv       *http.Server
}

// New creates an API server.
// finalizer may be nil; if nil, finalization via POST /blob is disabled.
// ready may be nil; if nil, GET /readyz always reports ready.
// metricsHandler may be nil; if nil, GET /metrics is not exposed.
// observe may be nil; if nil, request latency is not recorded.
func New(
	addr string,
	processor BlobProcessor,
	finalizer EpochFinalizer,
	ready ReadinessChecker,
	metricsHandler http.Handler,
	observe HTTPObserver,
	log *slog.Logger,
) *Server {
	s := &Server{
		processor: processor,
		finalizer: finalizer,
		ready:     ready,
		observe:   observe,
		log:       log,
	}

	mux := http.NewServeMux()
	mux.HandleFunc("POST /blob", s.handlePushBlob)
	mux.HandleFunc("GET /healthz", s.handleHealth)
	mux.HandleFunc("GET /readyz", s.handleReady)
	mux.HandleFunc("GET /openapi.json", s.handleOpenAPISpec)
	mux.HandleFunc("GET /docs", s.handleSwaggerUI)
	if metricsHandler != nil {
		mux.Handle("GET /metrics", metricsHandler)
	}

	s.srv = &http.Server{
		Addr:         addr,
		Handler:      s.logRequests(mux),
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

// ─── Middleware ───────────────────────────────────────────────────────────────

// statusRecorder wraps http.ResponseWriter to capture the status code written
// by the handler, so the logging middleware can report it.
type statusRecorder struct {
	http.ResponseWriter
	status int
}

func (r *statusRecorder) WriteHeader(code int) {
	r.status = code
	r.ResponseWriter.WriteHeader(code)
}

// logRequests logs every HTTP request once it completes, recording method,
// path, response status, duration and remote address.
func (s *Server) logRequests(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		// Default to 200; WriteHeader updates it if the handler sets another code.
		rec := &statusRecorder{ResponseWriter: w, status: http.StatusOK}

		next.ServeHTTP(rec, r)

		dur := time.Since(start)
		s.log.Info("http request",
			"method", r.Method,
			"path", r.URL.Path,
			"status", rec.status,
			"duration", dur,
			"remote", r.RemoteAddr,
			"length", r.ContentLength,
		)

		if s.observe != nil {
			s.observe(methodLabel(r.Method), routeLabel(r.URL.Path), strconv.Itoa(rec.status), dur)
		}
	})
}

// knownRoutes is the set of fixed paths the server exposes. routeLabel maps any
// path outside this set to "other" so the http_request_duration_seconds metric
// cannot blow up its label cardinality on unmatched/probed URLs.
var knownRoutes = map[string]struct{}{
	"/blob":         {},
	"/healthz":      {},
	"/readyz":       {},
	"/openapi.json": {},
	"/docs":         {},
	"/metrics":      {},
}

func routeLabel(path string) string {
	if _, ok := knownRoutes[path]; ok {
		return path
	}
	return "other"
}

// knownMethods is the set of HTTP methods the server handles. methodLabel maps
// any other verb to "other": the logging middleware runs before mux routing, so
// a client can send an arbitrary method string (which the mux then rejects with
// 405). Without this guard each distinct verb would create a new
// http_request_duration_seconds time series and grow the vector without bound.
var knownMethods = map[string]struct{}{
	http.MethodGet:  {},
	http.MethodPost: {},
}

func methodLabel(method string) string {
	if _, ok := knownMethods[method]; ok {
		return method
	}
	return "other"
}

// ─── Handlers ─────────────────────────────────────────────────────────────────

// handleHealth is a process-liveness probe: it returns 200 as long as the HTTP
// server is accepting requests, without touching any downstream dependency.
func (s *Server) handleHealth(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
}

// handleReady is a readiness probe: it verifies the kubo node is reachable via
// the injected ReadinessChecker. A nil checker means there is nothing to verify
// (e.g. uploads disabled), so it reports ready. On failure it returns 503 so
// callers can drain traffic instead of restarting the process.
func (s *Server) handleReady(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	if s.ready == nil {
		_ = json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()

	if err := s.ready(ctx); err != nil {
		w.WriteHeader(http.StatusServiceUnavailable)
		_ = json.NewEncoder(w).Encode(map[string]string{
			"status": "unhealthy",
			"error":  err.Error(),
		})
		return
	}

	_ = json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
}

func (s *Server) handleOpenAPISpec(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "private, max-age=3600")
	_, _ = w.Write(openapiSpec)
}

const swaggerUIHTML = `<!DOCTYPE html>
<html>
<head>
  <title>blobscan-ipld API</title>
  <meta charset="utf-8"/>
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <link rel="stylesheet" type="text/css" href="https://unpkg.com/swagger-ui-dist@5.32.8/swagger-ui.css">
</head>
<body>
<div id="swagger-ui"></div>
<script src="https://unpkg.com/swagger-ui-dist@5.32.8/swagger-ui-bundle.js"></script>
<script>
  SwaggerUIBundle({ url: "/openapi.json", dom_id: "#swagger-ui" })
</script>
</body>
</html>`

func (s *Server) handleSwaggerUI(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	_, _ = w.Write([]byte(swaggerUIHTML))
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
		s.log.Error("blob processing failed", "err", err, "commitment", req.Commitment, "epoch", req.Epoch)
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
	if req.Index < 0 {
		return fmt.Errorf("index must be non-negative")
	}
	return nil
}

// hexDecode accepts 0x-prefixed or plain hex strings.
func hexDecode(s string) ([]byte, error) {
	s = strings.TrimPrefix(s, "0x")
	return hex.DecodeString(s)
}
