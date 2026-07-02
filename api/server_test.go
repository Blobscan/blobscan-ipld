package api

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// newTestServer builds a Server with the given readiness checker, exercising the
// same mux wiring as New without binding a TCP listener.
func newTestServer(ready ReadinessChecker) *Server {
	return New(":0", nil, nil, ready, nil, nil, slog.New(slog.NewTextHandler(io.Discard, nil)))
}

func TestHandleHealthAlwaysOK(t *testing.T) {
	// /healthz is process liveness only: it must report 200 even when the
	// readiness checker would fail.
	s := newTestServer(func(context.Context) error { return errors.New("kubo down") })

	rec := httptest.NewRecorder()
	s.handleHealth(rec, httptest.NewRequest(http.MethodGet, "/healthz", nil))

	if rec.Code != http.StatusOK {
		t.Fatalf("healthz status = %d, want %d", rec.Code, http.StatusOK)
	}
}

func TestHandleReadyNilCheckerOK(t *testing.T) {
	// A nil checker means there is nothing to verify (e.g. uploads disabled).
	s := newTestServer(nil)

	rec := httptest.NewRecorder()
	s.handleReady(rec, httptest.NewRequest(http.MethodGet, "/readyz", nil))

	if rec.Code != http.StatusOK {
		t.Fatalf("readyz status = %d, want %d", rec.Code, http.StatusOK)
	}
}

func TestHandleReadyOK(t *testing.T) {
	s := newTestServer(func(context.Context) error { return nil })

	rec := httptest.NewRecorder()
	s.handleReady(rec, httptest.NewRequest(http.MethodGet, "/readyz", nil))

	if rec.Code != http.StatusOK {
		t.Fatalf("readyz status = %d, want %d", rec.Code, http.StatusOK)
	}
}

func TestHandleReadyUnhealthy(t *testing.T) {
	s := newTestServer(func(context.Context) error { return errors.New("kubo down") })

	rec := httptest.NewRecorder()
	s.handleReady(rec, httptest.NewRequest(http.MethodGet, "/readyz", nil))

	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("readyz status = %d, want %d", rec.Code, http.StatusServiceUnavailable)
	}
}

// stubMetricsHandler is a minimal handler standing in for the Prometheus
// exposition handler, so this test doesn't import the metrics package.
func stubMetricsHandler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/plain; version=0.0.4")
		_, _ = io.WriteString(w, "# HELP blobscanipld_up stub\nblobscanipld_up 1\n")
	})
}

func TestMetricsEndpointServed(t *testing.T) {
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	s := New(":0", nil, nil, nil, stubMetricsHandler(), nil, log)

	ts := httptest.NewServer(s.srv.Handler)
	defer ts.Close()

	resp, err := http.Get(ts.URL + "/metrics")
	if err != nil {
		t.Fatalf("GET /metrics: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("metrics status = %d, want %d", resp.StatusCode, http.StatusOK)
	}
	body, _ := io.ReadAll(resp.Body)
	if !strings.Contains(string(body), "blobscanipld_") {
		t.Fatalf("metrics body missing blobscanipld_ prefix: %q", body)
	}
}

func TestMetricsEndpointDisabledWhenNil(t *testing.T) {
	// With a nil metrics handler, /metrics must not be registered (404).
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	s := New(":0", nil, nil, nil, nil, nil, log)

	ts := httptest.NewServer(s.srv.Handler)
	defer ts.Close()

	resp, err := http.Get(ts.URL + "/metrics")
	if err != nil {
		t.Fatalf("GET /metrics: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("metrics status = %d, want %d", resp.StatusCode, http.StatusNotFound)
	}
}
