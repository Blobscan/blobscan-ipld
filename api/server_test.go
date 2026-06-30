package api

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"
)

// newTestServer builds a Server with the given readiness checker, exercising the
// same mux wiring as New without binding a TCP listener.
func newTestServer(ready ReadinessChecker) *Server {
	return New(":0", nil, nil, ready, slog.New(slog.NewTextHandler(io.Discard, nil)))
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
