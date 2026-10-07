package dashboard

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/go-chi/chi/v5"

	"github.com/MFarrelAkbar1/ForgeQ/internal/job"
	"github.com/MFarrelAkbar1/ForgeQ/internal/store"
)

type fakeStore struct {
	jobs map[int64]*job.Job
}

func ptr[T any](v T) *T { return &v }

func newFakeStore() *fakeStore {
	now := time.Now()
	return &fakeStore{jobs: map[int64]*job.Job{
		1: {ID: 1, Queue: "emails", Type: "send_email", Status: job.StatusDead, Run: 1, Attempts: 3, MaxAttempts: 3,
			Payload: json.RawMessage(`{"to":"a@b.c"}`), LastError: ptr("<script>alert(1)</script>"),
			CreatedAt: now, UpdatedAt: now, RunAt: now},
		2: {ID: 2, Queue: "emails", Type: "send_email", Status: job.StatusPending, Run: 1, MaxAttempts: 3,
			Payload: json.RawMessage(`{}`), CreatedAt: now, UpdatedAt: now, RunAt: now},
	}}
}

func (f *fakeStore) Stats(context.Context) ([]store.QueueStat, error) {
	return []store.QueueStat{
		{Queue: "emails", Status: job.StatusDead, Count: 1},
		{Queue: "emails", Status: job.StatusPending, Count: 1},
	}, nil
}

func (f *fakeStore) List(_ context.Context, fl store.ListFilter) ([]*job.Job, error) {
	var out []*job.Job
	for _, j := range f.jobs {
		if fl.Status == "" || j.Status == fl.Status {
			out = append(out, j)
		}
	}
	return out, nil
}

func (f *fakeStore) Get(_ context.Context, id int64) (*job.Job, error) {
	if j, ok := f.jobs[id]; ok {
		return j, nil
	}
	return nil, store.ErrNotFound
}

func (f *fakeStore) Errors(context.Context, int64) ([]job.AttemptError, error) {
	return []job.AttemptError{{Run: 1, Attempt: 1, Error: "smtp down", OccurredAt: time.Now()}}, nil
}

func (f *fakeStore) RetryDead(_ context.Context, id int64) (*job.Job, error) {
	j, ok := f.jobs[id]
	if !ok {
		return nil, store.ErrNotFound
	}
	if j.Status != job.StatusDead {
		return nil, store.ErrInvalidState
	}
	j.Status, j.Run, j.Attempts = job.StatusPending, j.Run+1, 0
	return j, nil
}

func (f *fakeStore) Delete(_ context.Context, id int64) error {
	if _, ok := f.jobs[id]; !ok {
		return store.ErrNotFound
	}
	delete(f.jobs, id)
	return nil
}

// newTestServer mounts the dashboard at BasePath like the api does.
func newTestServer(t *testing.T, fs *fakeStore, key string) http.Handler {
	t.Helper()
	h, err := New(Options{Store: fs, Logger: slog.New(slog.NewTextHandler(io.Discard, nil)), APIKey: key})
	if err != nil {
		t.Fatal(err)
	}
	r := chi.NewRouter()
	r.Mount(BasePath, h)
	return r
}

type reqOpt func(*http.Request)

func basic(password string) reqOpt {
	return func(r *http.Request) { r.SetBasicAuth("admin", password) }
}
func header(k, v string) reqOpt { return func(r *http.Request) { r.Header.Set(k, v) } }

func do(h http.Handler, method, path string, opts ...reqOpt) *httptest.ResponseRecorder {
	req := httptest.NewRequest(method, path, nil)
	for _, o := range opts {
		o(req)
	}
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec
}

func TestIndex(t *testing.T) {
	h := newTestServer(t, newFakeStore(), "")
	rec := do(h, http.MethodGet, "/ui/")
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d: %s", rec.Code, rec.Body)
	}
	body := rec.Body.String()
	for _, want := range []string{"emails", `id="job-1"`, "hx-post=\"/ui/jobs/1/retry\"", `hx-trigger="every 3s"`} {
		if !strings.Contains(body, want) {
			t.Errorf("index missing %q", want)
		}
	}
	if strings.Contains(body, `id="job-2"`) {
		t.Error("default tab should only list dead jobs")
	}
	if strings.Contains(body, "<script>alert(1)</script>") {
		t.Error("job error was not HTML-escaped")
	}
}

func TestDetail(t *testing.T) {
	h := newTestServer(t, newFakeStore(), "")
	rec := do(h, http.MethodGet, "/ui/jobs/1")
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d: %s", rec.Code, rec.Body)
	}
	body := rec.Body.String()
	for _, want := range []string{"Job #1", "&#34;to&#34;: &#34;a@b.c&#34;", "smtp down"} {
		if !strings.Contains(body, want) {
			t.Errorf("detail missing %q", want)
		}
	}
	if rec := do(h, http.MethodGet, "/ui/jobs/99"); rec.Code != http.StatusNotFound {
		t.Errorf("missing job = %d, want 404", rec.Code)
	}
}

func TestRetryAndDelete(t *testing.T) {
	fs := newFakeStore()
	h := newTestServer(t, fs, "")

	rec := do(h, http.MethodPost, "/ui/jobs/1/retry")
	if rec.Code != http.StatusOK || !strings.Contains(rec.Body.String(), "pill-pending") {
		t.Fatalf("retry = %d %s, want the updated row", rec.Code, rec.Body)
	}
	if rec := do(h, http.MethodPost, "/ui/jobs/1/retry"); rec.Code != http.StatusConflict {
		t.Errorf("second retry = %d, want 409", rec.Code)
	}
	rec = do(h, http.MethodPost, "/ui/jobs/2/retry?from=detail")
	if rec.Code != http.StatusConflict {
		t.Errorf("retry of pending job = %d, want 409", rec.Code)
	}

	fs.jobs[1].Status = job.StatusDead
	rec = do(h, http.MethodPost, "/ui/jobs/1/retry?from=detail")
	if rec.Header().Get("HX-Refresh") != "true" {
		t.Errorf("retry from detail should ask htmx to refresh, headers %v", rec.Header())
	}

	rec = do(h, http.MethodDelete, "/ui/jobs/2?from=detail")
	if rec.Code != http.StatusOK || rec.Header().Get("HX-Redirect") != "/ui/" {
		t.Errorf("delete from detail = %d, HX-Redirect %q", rec.Code, rec.Header().Get("HX-Redirect"))
	}
	if _, ok := fs.jobs[2]; ok {
		t.Error("job not deleted")
	}
}

func TestBasicAuth(t *testing.T) {
	h := newTestServer(t, newFakeStore(), "s3cret")
	rec := do(h, http.MethodGet, "/ui/")
	if rec.Code != http.StatusUnauthorized || !strings.HasPrefix(rec.Header().Get("WWW-Authenticate"), "Basic") {
		t.Errorf("no credentials = %d (WWW-Authenticate %q), want a basic-auth challenge",
			rec.Code, rec.Header().Get("WWW-Authenticate"))
	}
	if rec := do(h, http.MethodGet, "/ui/", basic("wrong")); rec.Code != http.StatusUnauthorized {
		t.Errorf("wrong password = %d, want 401", rec.Code)
	}
	if rec := do(h, http.MethodGet, "/ui/", basic("s3cret")); rec.Code != http.StatusOK {
		t.Errorf("right password = %d, want 200", rec.Code)
	}
	if rec := do(h, http.MethodPost, "/ui/jobs/1/retry"); rec.Code != http.StatusUnauthorized {
		t.Errorf("unauthenticated retry = %d, want 401", rec.Code)
	}
}

func TestCrossSiteRequestsAreRejected(t *testing.T) {
	fs := newFakeStore()
	h := newTestServer(t, fs, "s3cret")

	// A malicious page submitting to the dashboard: the browser attaches the
	// cached basic-auth credentials and marks the request cross-site.
	rec := do(h, http.MethodPost, "/ui/jobs/1/retry", basic("s3cret"), header("Sec-Fetch-Site", "cross-site"))
	if rec.Code != http.StatusForbidden {
		t.Errorf("cross-site retry = %d, want 403", rec.Code)
	}
	if fs.jobs[1].Status != job.StatusDead {
		t.Error("cross-site request changed the job")
	}
	rec = do(h, http.MethodPost, "/ui/jobs/1/retry", basic("s3cret"), header("Sec-Fetch-Site", "same-origin"))
	if rec.Code != http.StatusOK {
		t.Errorf("same-origin retry = %d, want 200", rec.Code)
	}
}

func TestStaticAssets(t *testing.T) {
	h := newTestServer(t, newFakeStore(), "")
	for _, path := range []string{"/ui/static/htmx.min.js", "/ui/static/app.css", "/ui/static/app.js"} {
		rec := do(h, http.MethodGet, path)
		if rec.Code != http.StatusOK || rec.Body.Len() == 0 {
			t.Errorf("%s = %d (%d bytes)", path, rec.Code, rec.Body.Len())
		}
		if rec.Header().Get("Cache-Control") == "" {
			t.Errorf("%s has no Cache-Control", path)
		}
	}
}

func TestPartials(t *testing.T) {
	h := newTestServer(t, newFakeStore(), "")
	rec := do(h, http.MethodGet, "/ui/partials/stats")
	if rec.Code != http.StatusOK || !strings.Contains(rec.Body.String(), `id="stats"`) ||
		strings.Contains(rec.Body.String(), "<html") {
		t.Errorf("stats partial = %d %s", rec.Code, rec.Body)
	}
	if rec := do(h, http.MethodGet, "/ui/partials/jobs?status=bogus"); rec.Code != http.StatusBadRequest {
		t.Errorf("bad status = %d, want 400", rec.Code)
	}
	rec = do(h, http.MethodGet, "/ui/partials/jobs?status=pending")
	if !strings.Contains(rec.Body.String(), `id="job-2"`) {
		t.Errorf("pending rows = %s", rec.Body)
	}
}
