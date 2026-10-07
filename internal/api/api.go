// Package api exposes the HTTP API for enqueueing and inspecting jobs.
package api

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"strconv"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"

	"github.com/MFarrelAkbar1/ForgeQ/internal/auth"
	"github.com/MFarrelAkbar1/ForgeQ/internal/dashboard"
	"github.com/MFarrelAkbar1/ForgeQ/internal/job"
	"github.com/MFarrelAkbar1/ForgeQ/internal/ops"
	"github.com/MFarrelAkbar1/ForgeQ/internal/store"
)

// Store is the persistence the API depends on; *store.Store implements it.
type Store interface {
	Enqueue(ctx context.Context, n job.NewJob) (*job.Job, bool, error)
	Get(ctx context.Context, id int64) (*job.Job, error)
	List(ctx context.Context, f store.ListFilter) ([]*job.Job, error)
	Errors(ctx context.Context, jobID int64) ([]job.AttemptError, error)
	RetryDead(ctx context.Context, id int64) (*job.Job, error)
	Delete(ctx context.Context, id int64) error
}

// Observer receives API events, e.g. to export metrics. Methods must be safe
// for concurrent use.
type Observer interface {
	JobEnqueued(created bool)
	HTTPRequest(method, route string, code int, took time.Duration)
}

type nopObserver struct{}

func (nopObserver) JobEnqueued(bool)                               {}
func (nopObserver) HTTPRequest(string, string, int, time.Duration) {}

const (
	maxBodyBytes     = 1 << 20 // 1 MiB
	defaultListLimit = 50
	maxListLimit     = 200
)

type server struct {
	store Store
	log   *slog.Logger
	obs   Observer
}

// Options configures the HTTP handler.
type Options struct {
	Store  Store
	Logger *slog.Logger
	// Ready is the readiness check behind /readyz; nil means always ready.
	Ready ops.ReadyFunc
	// Metrics is served at /metrics when non-nil.
	Metrics http.Handler
	// Observer receives request and enqueue events; nil records nothing.
	Observer Observer
	// APIKey, when set, must be sent as "Authorization: Bearer <key>" on every
	// /api/v1 request: job payloads may hold sensitive data, so reads are
	// protected too. Empty disables the check. Probes and /metrics stay open.
	APIKey string
	// Dashboard, when non-nil, is mounted at dashboard.BasePath and "/"
	// redirects to it. It handles its own authentication.
	Dashboard http.Handler
}

// NewRouter returns the HTTP handler for the API and operational endpoints.
func NewRouter(o Options) http.Handler {
	if o.Observer == nil {
		o.Observer = nopObserver{}
	}
	srv := &server{store: o.Store, log: o.Logger, obs: o.Observer}

	r := chi.NewRouter()
	r.Use(middleware.RequestID)
	// Instrument at the top level so unmatched routes (scanners, typos) are
	// logged and counted too. Probes are hit every few seconds and skipped.
	r.Use(instrument(o.Logger, o.Observer, "/healthz", "/readyz", "/metrics"))
	r.Use(middleware.Recoverer)

	ops.Register(r, o.Ready, o.Metrics)
	r.Route("/api/v1", func(r chi.Router) {
		r.Use(requireAPIKey(o.APIKey))
		r.Route("/jobs", func(r chi.Router) {
			r.Post("/", srv.enqueue)
			r.Get("/", srv.list)
			r.Get("/{id}", srv.get)
			r.Delete("/{id}", srv.delete)
			r.Post("/{id}/retry", srv.retry)
		})
	})
	if o.Dashboard != nil {
		r.Mount(dashboard.BasePath, o.Dashboard)
		r.Get("/", http.RedirectHandler(dashboard.BasePath+"/", http.StatusFound).ServeHTTP)
	}
	return r
}

type enqueueRequest struct {
	Type           string          `json:"type"`
	Queue          string          `json:"queue"`
	Payload        json.RawMessage `json:"payload"`
	Priority       int             `json:"priority"`
	MaxAttempts    int             `json:"max_attempts"`
	ScheduledAt    *time.Time      `json:"scheduled_at"`
	IdempotencyKey string          `json:"idempotency_key"`
}

func (s *server) enqueue(w http.ResponseWriter, r *http.Request) {
	var req enqueueRequest
	if err := decodeJSON(w, r, &req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid_json", err.Error(), nil)
		return
	}

	j, created, err := s.store.Enqueue(r.Context(), job.NewJob{
		Queue:          req.Queue,
		Type:           req.Type,
		Payload:        req.Payload,
		Priority:       req.Priority,
		MaxAttempts:    req.MaxAttempts,
		RunAt:          req.ScheduledAt,
		IdempotencyKey: req.IdempotencyKey,
	})
	verr, invalid := errors.AsType[*job.ValidationError](err)
	switch {
	case invalid:
		writeError(w, http.StatusUnprocessableEntity, "validation_failed", "request has invalid fields", verr.Fields)
		return
	case err != nil:
		s.internalError(w, r, err)
		return
	}

	s.obs.JobEnqueued(created)
	w.Header().Set("Location", fmt.Sprintf("/api/v1/jobs/%d", j.ID))
	if created {
		writeJSON(w, http.StatusCreated, j)
		return
	}
	// Same idempotency key as an earlier request: return the original job.
	writeJSON(w, http.StatusOK, j)
}

type jobDetail struct {
	*job.Job
	Errors []job.AttemptError `json:"errors"`
}

func (s *server) get(w http.ResponseWriter, r *http.Request) {
	id, ok := parseID(w, r)
	if !ok {
		return
	}
	j, err := s.store.Get(r.Context(), id)
	if err != nil {
		s.storeError(w, r, err)
		return
	}
	errs, err := s.store.Errors(r.Context(), id)
	if err != nil {
		s.internalError(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, jobDetail{Job: j, Errors: errs})
}

// retry re-enqueues a dead job (manual dead-letter retry).
func (s *server) retry(w http.ResponseWriter, r *http.Request) {
	id, ok := parseID(w, r)
	if !ok {
		return
	}
	j, err := s.store.RetryDead(r.Context(), id)
	if err != nil {
		s.storeError(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, j)
}

// delete removes a dead or succeeded job.
func (s *server) delete(w http.ResponseWriter, r *http.Request) {
	id, ok := parseID(w, r)
	if !ok {
		return
	}
	if err := s.store.Delete(r.Context(), id); err != nil {
		s.storeError(w, r, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func parseID(w http.ResponseWriter, r *http.Request) (int64, bool) {
	id, err := strconv.ParseInt(chi.URLParam(r, "id"), 10, 64)
	if err != nil || id <= 0 {
		writeError(w, http.StatusBadRequest, "invalid_id", "job id must be a positive integer", nil)
		return 0, false
	}
	return id, true
}

type listResponse struct {
	Jobs       []*job.Job `json:"jobs"`
	NextCursor *int64     `json:"next_cursor"`
}

func (s *server) list(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	f := store.ListFilter{
		Queue:  q.Get("queue"),
		Status: job.Status(q.Get("status")),
		Limit:  defaultListLimit,
	}

	fields := map[string]string{}
	if f.Status != "" && !f.Status.Valid() {
		fields["status"] = "unknown status"
	}
	if v := q.Get("limit"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n < 1 || n > maxListLimit {
			fields["limit"] = fmt.Sprintf("must be between 1 and %d", maxListLimit)
		}
		f.Limit = n
	}
	if v := q.Get("cursor"); v != "" {
		n, err := strconv.ParseInt(v, 10, 64)
		if err != nil || n < 1 {
			fields["cursor"] = "must be a positive integer"
		}
		f.Cursor = n
	}
	if len(fields) > 0 {
		writeError(w, http.StatusBadRequest, "invalid_query", "query has invalid parameters", fields)
		return
	}

	jobs, err := s.store.List(r.Context(), f)
	if err != nil {
		s.internalError(w, r, err)
		return
	}
	resp := listResponse{Jobs: jobs}
	if resp.Jobs == nil {
		resp.Jobs = []*job.Job{}
	}
	if len(jobs) == f.Limit {
		next := jobs[len(jobs)-1].ID
		resp.NextCursor = &next
	}
	writeJSON(w, http.StatusOK, resp)
}

// requireAPIKey rejects requests without the bearer token. Comparison is
// constant-time (see auth.Equal).
func requireAPIKey(key string) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if !auth.ValidBearer(r, key) {
				w.Header().Set("WWW-Authenticate", `Bearer realm="forgeq"`)
				writeError(w, http.StatusUnauthorized, "unauthorized", "missing or invalid API key", nil)
				return
			}
			next.ServeHTTP(w, r)
		})
	}
}

// storeError maps store sentinel errors to HTTP responses.
func (s *server) storeError(w http.ResponseWriter, r *http.Request, err error) {
	switch {
	case errors.Is(err, store.ErrNotFound):
		writeError(w, http.StatusNotFound, "not_found", "job not found", nil)
	case errors.Is(err, store.ErrInvalidState):
		writeError(w, http.StatusConflict, "invalid_state", err.Error(), nil)
	default:
		s.internalError(w, r, err)
	}
}

func (s *server) internalError(w http.ResponseWriter, r *http.Request, err error) {
	s.log.ErrorContext(r.Context(), "request failed",
		"method", r.Method, "path", r.URL.Path,
		"request_id", middleware.GetReqID(r.Context()), "err", err)
	writeError(w, http.StatusInternalServerError, "internal", "internal server error", nil)
}
