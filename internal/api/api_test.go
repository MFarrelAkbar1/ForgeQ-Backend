package api

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/MFarrelAkbar1/ForgeQ-Backend/internal/job"
	"github.com/MFarrelAkbar1/ForgeQ-Backend/internal/store"
)

// fakeStore is an in-memory Store with idempotency-key deduplication.
type fakeStore struct {
	jobs   map[int64]*job.Job
	byKey  map[string]int64
	nextID int64
	err    error // returned by every call when set
}

func newFakeStore() *fakeStore {
	return &fakeStore{jobs: map[int64]*job.Job{}, byKey: map[string]int64{}}
}

func (f *fakeStore) Enqueue(_ context.Context, n job.NewJob) (*job.Job, bool, error) {
	if f.err != nil {
		return nil, false, f.err
	}
	if err := n.Normalize(); err != nil {
		return nil, false, err
	}
	if id, ok := f.byKey[n.Queue+"/"+n.IdempotencyKey]; ok && n.IdempotencyKey != "" {
		return f.jobs[id], false, nil
	}
	f.nextID++
	j := &job.Job{ID: f.nextID, Queue: n.Queue, Type: n.Type, Payload: n.Payload, Status: job.StatusPending}
	f.jobs[j.ID] = j
	if n.IdempotencyKey != "" {
		f.byKey[n.Queue+"/"+n.IdempotencyKey] = j.ID
	}
	return j, true, nil
}

func (f *fakeStore) Get(_ context.Context, id int64) (*job.Job, error) {
	if f.err != nil {
		return nil, f.err
	}
	j, ok := f.jobs[id]
	if !ok {
		return nil, store.ErrNotFound
	}
	return j, nil
}

func (f *fakeStore) List(_ context.Context, fl store.ListFilter) ([]*job.Job, error) {
	if f.err != nil {
		return nil, f.err
	}
	var out []*job.Job
	for id := f.nextID; id >= 1 && len(out) < fl.Limit; id-- {
		if fl.Cursor != 0 && id >= fl.Cursor {
			continue
		}
		out = append(out, f.jobs[id])
	}
	return out, nil
}

func (f *fakeStore) Errors(context.Context, int64) ([]job.AttemptError, error) {
	return []job.AttemptError{{Attempt: 1, Error: "boom"}}, f.err
}

func (f *fakeStore) RetryDead(_ context.Context, id int64) (*job.Job, error) {
	j, ok := f.jobs[id]
	switch {
	case !ok:
		return nil, store.ErrNotFound
	case j.Status != job.StatusDead:
		return nil, store.ErrInvalidState
	}
	j.Status, j.Attempts = job.StatusPending, 0
	return j, nil
}

func (f *fakeStore) Delete(_ context.Context, id int64) error {
	j, ok := f.jobs[id]
	switch {
	case !ok:
		return store.ErrNotFound
	case j.Status != job.StatusDead && j.Status != job.StatusSucceeded:
		return store.ErrInvalidState
	}
	delete(f.jobs, id)
	return nil
}

func do(t *testing.T, h http.Handler, method, path, body string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(method, path, strings.NewReader(body))
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec
}

func newTestRouter(s Store) http.Handler {
	return NewRouter(Options{Store: s, Logger: slog.New(slog.NewTextHandler(io.Discard, nil))})
}

func TestEnqueue(t *testing.T) {
	h := newTestRouter(newFakeStore())

	rec := do(t, h, http.MethodPost, "/api/v1/jobs", `{"type":"send_email","payload":{"to":"a@b.c"}}`)
	if rec.Code != http.StatusCreated {
		t.Fatalf("status = %d, body %s", rec.Code, rec.Body)
	}
	if loc := rec.Header().Get("Location"); loc != "/api/v1/jobs/1" {
		t.Errorf("Location = %q", loc)
	}
	var j job.Job
	if err := json.Unmarshal(rec.Body.Bytes(), &j); err != nil {
		t.Fatal(err)
	}
	if j.Queue != "default" || j.Type != "send_email" {
		t.Errorf("job = %+v", j)
	}
}

func TestEnqueueIdempotent(t *testing.T) {
	h := newTestRouter(newFakeStore())
	body := `{"type":"send_email","idempotency_key":"order-1"}`

	first := do(t, h, http.MethodPost, "/api/v1/jobs", body)
	second := do(t, h, http.MethodPost, "/api/v1/jobs", body)
	if first.Code != http.StatusCreated || second.Code != http.StatusOK {
		t.Fatalf("statuses = %d, %d; want 201, 200", first.Code, second.Code)
	}
	if first.Header().Get("Location") != second.Header().Get("Location") {
		t.Error("idempotent replay should point at the same job")
	}
}

func TestEnqueueRejectsBadInput(t *testing.T) {
	h := newTestRouter(newFakeStore())
	tests := []struct {
		name, body, code string
		status           int
	}{
		{"malformed", `{`, "invalid_json", http.StatusBadRequest},
		{"unknown field", `{"type":"x","colour":"red"}`, "invalid_json", http.StatusBadRequest},
		{"trailing data", `{"type":"x"}{}`, "invalid_json", http.StatusBadRequest},
		{"missing type", `{"queue":"q"}`, "validation_failed", http.StatusUnprocessableEntity},
		{"bad priority", `{"type":"x","priority":1000}`, "validation_failed", http.StatusUnprocessableEntity},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rec := do(t, h, http.MethodPost, "/api/v1/jobs", tt.body)
			if rec.Code != tt.status {
				t.Fatalf("status = %d, want %d; body %s", rec.Code, tt.status, rec.Body)
			}
			var body errorBody
			if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
				t.Fatal(err)
			}
			if body.Error.Code != tt.code {
				t.Errorf("code = %q, want %q", body.Error.Code, tt.code)
			}
		})
	}
}

func TestGet(t *testing.T) {
	fs := newFakeStore()
	h := newTestRouter(fs)
	do(t, h, http.MethodPost, "/api/v1/jobs", `{"type":"x"}`)

	rec := do(t, h, http.MethodGet, "/api/v1/jobs/1", "")
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d", rec.Code)
	}
	var d struct {
		ID     int64              `json:"id"`
		Errors []job.AttemptError `json:"errors"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &d); err != nil {
		t.Fatal(err)
	}
	if d.ID != 1 || len(d.Errors) != 1 {
		t.Errorf("detail = %+v", d)
	}

	if rec := do(t, h, http.MethodGet, "/api/v1/jobs/99", ""); rec.Code != http.StatusNotFound {
		t.Errorf("missing job: status = %d, want 404", rec.Code)
	}
	if rec := do(t, h, http.MethodGet, "/api/v1/jobs/abc", ""); rec.Code != http.StatusBadRequest {
		t.Errorf("bad id: status = %d, want 400", rec.Code)
	}
}

func TestListPagination(t *testing.T) {
	h := newTestRouter(newFakeStore())
	for range 3 {
		do(t, h, http.MethodPost, "/api/v1/jobs", `{"type":"x"}`)
	}

	var page listResponse
	rec := do(t, h, http.MethodGet, "/api/v1/jobs?limit=2", "")
	if err := json.Unmarshal(rec.Body.Bytes(), &page); err != nil {
		t.Fatal(err)
	}
	if len(page.Jobs) != 2 || page.NextCursor == nil || *page.NextCursor != 2 {
		t.Fatalf("first page = %d jobs, cursor %v", len(page.Jobs), page.NextCursor)
	}

	rec = do(t, h, http.MethodGet, "/api/v1/jobs?limit=2&cursor=2", "")
	page = listResponse{}
	if err := json.Unmarshal(rec.Body.Bytes(), &page); err != nil {
		t.Fatal(err)
	}
	if len(page.Jobs) != 1 || page.Jobs[0].ID != 1 || page.NextCursor != nil {
		t.Fatalf("second page = %+v", page)
	}

	if rec := do(t, h, http.MethodGet, "/api/v1/jobs?status=bogus", ""); rec.Code != http.StatusBadRequest {
		t.Errorf("bad status filter: code = %d, want 400", rec.Code)
	}
}

func TestDeadLetterActions(t *testing.T) {
	fs := newFakeStore()
	h := newTestRouter(fs)
	for range 2 {
		do(t, h, http.MethodPost, "/api/v1/jobs", `{"type":"x"}`)
	}
	fs.jobs[1].Status, fs.jobs[1].Attempts = job.StatusDead, 5

	// Steps run in order and depend on each other.
	steps := []struct {
		name, method, path string
		want               int
	}{
		{"retry pending job conflicts", http.MethodPost, "/api/v1/jobs/2/retry", http.StatusConflict},
		{"delete pending job conflicts", http.MethodDelete, "/api/v1/jobs/2", http.StatusConflict},
		{"retry missing job", http.MethodPost, "/api/v1/jobs/99/retry", http.StatusNotFound},
		{"retry bad id", http.MethodPost, "/api/v1/jobs/x/retry", http.StatusBadRequest},
		{"retry dead job", http.MethodPost, "/api/v1/jobs/1/retry", http.StatusOK},
		{"retried job is no longer dead", http.MethodPost, "/api/v1/jobs/1/retry", http.StatusConflict},
	}
	for _, st := range steps {
		if rec := do(t, h, st.method, st.path, ""); rec.Code != st.want {
			t.Errorf("%s: status = %d, want %d; body %s", st.name, rec.Code, st.want, rec.Body)
		}
	}
	if fs.jobs[1].Status != job.StatusPending || fs.jobs[1].Attempts != 0 {
		t.Errorf("retried job = %+v", fs.jobs[1])
	}

	fs.jobs[1].Status = job.StatusDead
	if rec := do(t, h, http.MethodDelete, "/api/v1/jobs/1", ""); rec.Code != http.StatusNoContent {
		t.Errorf("delete dead job: status = %d, want 204", rec.Code)
	}
	if _, ok := fs.jobs[1]; ok {
		t.Error("job still present after delete")
	}
}

func TestInternalErrorHidesDetails(t *testing.T) {
	fs := newFakeStore()
	fs.err = errors.New("connection refused to 10.0.0.5")
	h := newTestRouter(fs)

	rec := do(t, h, http.MethodGet, "/api/v1/jobs/1", "")
	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("status = %d", rec.Code)
	}
	if strings.Contains(rec.Body.String(), "10.0.0.5") {
		t.Error("internal error details leaked to client")
	}
}

func TestOpsEndpointsMounted(t *testing.T) {
	ready := errors.New("db down")
	h := NewRouter(Options{
		Store:  newFakeStore(),
		Logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
		Ready:  func(context.Context) error { return ready },
	})
	if rec := do(t, h, http.MethodGet, "/healthz", ""); rec.Code != http.StatusOK {
		t.Errorf("/healthz = %d", rec.Code)
	}
	if rec := do(t, h, http.MethodGet, "/readyz", ""); rec.Code != http.StatusServiceUnavailable {
		t.Errorf("/readyz with failing check = %d, want 503", rec.Code)
	}
}

type recordingObserver struct {
	routes   []string
	enqueued []bool
}

func (o *recordingObserver) JobEnqueued(created bool) { o.enqueued = append(o.enqueued, created) }
func (o *recordingObserver) HTTPRequest(method, route string, code int, _ time.Duration) {
	o.routes = append(o.routes, fmt.Sprintf("%s %s %d", method, route, code))
}

func TestInstrumentationUsesRoutePatterns(t *testing.T) {
	obs := &recordingObserver{}
	h := NewRouter(Options{
		Store:    newFakeStore(),
		Logger:   slog.New(slog.NewTextHandler(io.Discard, nil)),
		Observer: obs,
	})
	do(t, h, http.MethodPost, "/api/v1/jobs", `{"type":"x"}`)
	do(t, h, http.MethodGet, "/api/v1/jobs/1", "")
	do(t, h, http.MethodGet, "/api/v1/jobs/2", "")
	do(t, h, http.MethodGet, "/nope", "")
	do(t, h, http.MethodGet, "/healthz", "") // probes are not instrumented

	want := []string{
		"POST /api/v1/jobs 201",
		"GET /api/v1/jobs/{id} 200",
		"GET /api/v1/jobs/{id} 404",
		"GET unmatched 404",
	}
	if fmt.Sprint(obs.routes) != fmt.Sprint(want) {
		t.Errorf("routes = %q, want %q", obs.routes, want)
	}
	if len(obs.enqueued) != 1 || !obs.enqueued[0] {
		t.Errorf("enqueued = %v, want [true]", obs.enqueued)
	}
}

func TestAPIKeyProtectsAllAPIRoutes(t *testing.T) {
	fs := newFakeStore()
	h := NewRouter(Options{
		Store:  fs,
		Logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
		APIKey: "s3cret",
	})
	if _, _, err := fs.Enqueue(context.Background(), job.NewJob{Type: "x"}); err != nil {
		t.Fatal(err)
	}
	fs.jobs[1].Status = job.StatusDead

	call := func(method, path, body, token string) *httptest.ResponseRecorder {
		req := httptest.NewRequest(method, path, strings.NewReader(body))
		if token != "" {
			req.Header.Set("Authorization", "Bearer "+token)
		}
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req)
		return rec
	}

	routes := []struct{ method, path, body string }{
		{http.MethodPost, "/api/v1/jobs", `{"type":"x"}`},
		{http.MethodGet, "/api/v1/jobs", ""},
		{http.MethodGet, "/api/v1/jobs/1", ""},
		{http.MethodPost, "/api/v1/jobs/1/retry", ""},
		{http.MethodDelete, "/api/v1/jobs/1", ""},
	}
	for _, rt := range routes {
		for _, token := range []string{"", "nope"} {
			rec := call(rt.method, rt.path, rt.body, token)
			if rec.Code != http.StatusUnauthorized {
				t.Errorf("%s %s with token %q = %d, want 401", rt.method, rt.path, token, rec.Code)
			}
			if rec.Header().Get("WWW-Authenticate") == "" {
				t.Errorf("%s %s: 401 without WWW-Authenticate", rt.method, rt.path)
			}
		}
	}

	// With the key every route gets past auth. Run in order: the retry makes
	// the job pending, so the delete is refused on state (409), not on auth.
	wantOK := []int{http.StatusCreated, http.StatusOK, http.StatusOK, http.StatusOK, http.StatusConflict}
	for i, rt := range routes {
		if rec := call(rt.method, rt.path, rt.body, "s3cret"); rec.Code != wantOK[i] {
			t.Errorf("%s %s with key = %d, want %d", rt.method, rt.path, rec.Code, wantOK[i])
		}
	}

	// Operational endpoints stay open for probes and Prometheus.
	if rec := call(http.MethodGet, "/healthz", "", ""); rec.Code != http.StatusOK {
		t.Errorf("/healthz = %d, want 200 without a key", rec.Code)
	}
}
