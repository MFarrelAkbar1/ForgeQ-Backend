// Package ops provides the operational endpoints shared by the api and worker
// processes: liveness, readiness and (optionally) Prometheus metrics.
package ops

import (
	"context"
	"encoding/json"
	"net/http"
	"time"
)

const readinessTimeout = 2 * time.Second

// ReadyFunc reports whether the process can do useful work, e.g. by pinging
// the database. A nil ReadyFunc means always ready.
type ReadyFunc func(ctx context.Context) error

// Mux is satisfied by *http.ServeMux and chi.Router.
type Mux interface {
	Handle(pattern string, h http.Handler)
}

// Register mounts the operational endpoints on mux:
//
//	/healthz  liveness: the process is up (never checks dependencies, so a
//	          database outage does not get every pod restarted at once)
//	/readyz   readiness: dependencies are reachable (503 otherwise)
//	/metrics  Prometheus exposition, when metrics is non-nil
func Register(mux Mux, ready ReadyFunc, metrics http.Handler) {
	mux.Handle("/healthz", http.HandlerFunc(liveness))
	mux.Handle("/readyz", readiness(ready))
	if metrics != nil {
		mux.Handle("/metrics", metrics)
	}
}

// NewMux returns a ServeMux with only the operational endpoints, for
// processes (like the worker) that have no other HTTP surface.
func NewMux(ready ReadyFunc, metrics http.Handler) *http.ServeMux {
	mux := http.NewServeMux()
	Register(mux, ready, metrics)
	return mux
}

func liveness(w http.ResponseWriter, _ *http.Request) {
	writeStatus(w, http.StatusOK, "ok")
}

func readiness(ready ReadyFunc) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if ready != nil {
			ctx, cancel := context.WithTimeout(r.Context(), readinessTimeout)
			defer cancel()
			// The cause is not echoed: it may contain hostnames or credentials.
			if err := ready(ctx); err != nil {
				writeStatus(w, http.StatusServiceUnavailable, "unavailable")
				return
			}
		}
		writeStatus(w, http.StatusOK, "ok")
	})
}

func writeStatus(w http.ResponseWriter, code int, status string) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(struct {
		Status string `json:"status"`
	}{status})
}
