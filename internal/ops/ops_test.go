package ops

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func get(t *testing.T, h http.Handler, path string) *httptest.ResponseRecorder {
	t.Helper()
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, path, nil))
	return rec
}

func TestEndpoints(t *testing.T) {
	var dbErr error
	metrics := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write([]byte("m 1\n")) })
	mux := NewMux(func(context.Context) error { return dbErr }, metrics)

	if rec := get(t, mux, "/healthz"); rec.Code != http.StatusOK {
		t.Errorf("/healthz = %d", rec.Code)
	}
	if rec := get(t, mux, "/readyz"); rec.Code != http.StatusOK {
		t.Errorf("/readyz = %d", rec.Code)
	}
	if rec := get(t, mux, "/metrics"); rec.Body.String() != "m 1\n" {
		t.Errorf("/metrics body = %q", rec.Body)
	}

	dbErr = errors.New("dial tcp 10.0.0.5:5432: connection refused")
	rec := get(t, mux, "/readyz")
	if rec.Code != http.StatusServiceUnavailable {
		t.Errorf("/readyz with db down = %d, want 503", rec.Code)
	}
	if strings.Contains(rec.Body.String(), "10.0.0.5") {
		t.Errorf("/readyz leaked the error detail: %s", rec.Body)
	}
	if rec := get(t, mux, "/healthz"); rec.Code != http.StatusOK {
		t.Errorf("/healthz must not depend on the database, got %d", rec.Code)
	}
}

func TestNoMetricsHandler(t *testing.T) {
	if rec := get(t, NewMux(nil, nil), "/metrics"); rec.Code != http.StatusNotFound {
		t.Errorf("/metrics without handler = %d, want 404", rec.Code)
	}
}
