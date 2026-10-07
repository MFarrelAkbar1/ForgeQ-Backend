// Package dashboard serves a small server-rendered UI (html/template + htmx)
// for inspecting queues and handling dead jobs. Templates and static assets,
// including htmx itself, are embedded so the binary has no runtime file or CDN
// dependencies.
package dashboard

import (
	"bytes"
	"context"
	"embed"
	"encoding/json"
	"errors"
	"fmt"
	"html/template"
	"io/fs"
	"log/slog"
	"net/http"
	"slices"
	"strconv"
	"time"

	"github.com/go-chi/chi/v5"

	"github.com/MFarrelAkbar1/ForgeQ-Backend/internal/auth"
	"github.com/MFarrelAkbar1/ForgeQ-Backend/internal/job"
	"github.com/MFarrelAkbar1/ForgeQ-Backend/internal/store"
)

// BasePath is where the dashboard must be mounted.
const BasePath = "/ui"

const pageSize = 25

//go:embed templates/*.html
var templateFS embed.FS

//go:embed static
var staticFS embed.FS

// Store is the persistence the dashboard depends on; *store.Store implements it.
type Store interface {
	Stats(ctx context.Context) ([]store.QueueStat, error)
	List(ctx context.Context, f store.ListFilter) ([]*job.Job, error)
	Get(ctx context.Context, id int64) (*job.Job, error)
	Errors(ctx context.Context, jobID int64) ([]job.AttemptError, error)
	RetryDead(ctx context.Context, id int64) (*job.Job, error)
	Delete(ctx context.Context, id int64) error
}

// Options configures the dashboard.
type Options struct {
	Store  Store
	Logger *slog.Logger
	// APIKey, when set, is required as the HTTP basic-auth password (any
	// username), so a browser can prompt for it. Empty disables auth.
	APIKey string
}

// tabOrder lists statuses in the order an operator cares about them.
var tabOrder = []job.Status{job.StatusDead, job.StatusRetrying, job.StatusRunning, job.StatusPending, job.StatusSucceeded}

type dashboard struct {
	store Store
	log   *slog.Logger
	pages map[string]*template.Template // page name -> layout + partials + page
}

// New returns the dashboard handler, to be mounted at BasePath.
func New(o Options) (http.Handler, error) {
	pages, err := parsePages()
	if err != nil {
		return nil, err
	}
	d := &dashboard{store: o.Store, log: o.Logger, pages: pages}

	static, err := fs.Sub(staticFS, "static")
	if err != nil {
		return nil, err
	}

	r := chi.NewRouter()
	r.Use(requireBasicAuth(o.APIKey))
	r.Get("/", d.index)
	r.Get("/jobs/{id}", d.detail)
	r.Get("/partials/stats", d.statsPartial)
	r.Get("/partials/jobs", d.jobsPartial)
	r.Post("/jobs/{id}/retry", d.retry)
	r.Delete("/jobs/{id}", d.delete)
	r.Handle("/static/*", http.StripPrefix(BasePath+"/static/", cacheFor(time.Hour, http.FileServerFS(static))))

	// Browsers send basic-auth credentials automatically, even on requests
	// triggered by another site, so state-changing requests need CSRF
	// protection. CrossOriginProtection rejects cross-origin POST/DELETE using
	// the Sec-Fetch-Site and Origin headers; no tokens are needed.
	return http.NewCrossOriginProtection().Handler(r), nil
}

var funcs = template.FuncMap{
	"ts": func(t time.Time) string { return t.UTC().Format("2006-01-02 15:04:05Z") },
	"ago": func(t time.Time) string {
		d := time.Since(t).Round(time.Second)
		switch {
		case d < time.Minute:
			return fmt.Sprintf("%ds ago", int(d.Seconds()))
		case d < time.Hour:
			return fmt.Sprintf("%dm ago", int(d.Minutes()))
		case d < 48*time.Hour:
			return fmt.Sprintf("%dh ago", int(d.Hours()))
		default:
			return fmt.Sprintf("%dd ago", int(d.Hours()/24))
		}
	},
	"deref": func(s *string) string {
		if s == nil {
			return ""
		}
		return *s
	},
	"short": func(s string, n int) string {
		r := []rune(s)
		if len(r) <= n {
			return s
		}
		return string(r[:n]) + "…"
	},
	"sub": func(a, b int) int { return a - b },
}

func parsePages() (map[string]*template.Template, error) {
	pages := map[string]*template.Template{}
	for _, page := range []string{"index", "job"} {
		t, err := template.New(page).Funcs(funcs).ParseFS(templateFS,
			"templates/layout.html", "templates/partials.html", "templates/"+page+".html")
		if err != nil {
			return nil, fmt.Errorf("parse %s template: %w", page, err)
		}
		pages[page] = t
	}
	return pages, nil
}

type queueRow struct {
	Queue  string
	Counts map[job.Status]int64
}

type statsView struct {
	Statuses []job.Status
	Queues   []queueRow
}

type rowsView struct {
	Jobs       []*job.Job
	Status     job.Status
	Queue      string
	NextCursor int64 // 0 when there are no more rows
}

type indexView struct {
	Stats    statsView
	Rows     rowsView
	Statuses []job.Status
	Status   job.Status
	Queue    string
}

type detailView struct {
	Job     *job.Job
	Errors  []job.AttemptError
	Payload string
}

func (d *dashboard) index(w http.ResponseWriter, r *http.Request) {
	status, queue, cursor, ok := listParams(w, r)
	if !ok {
		return
	}
	stats, err := d.stats(r.Context())
	if err != nil {
		d.fail(w, r, err)
		return
	}
	rows, err := d.rows(r.Context(), status, queue, cursor)
	if err != nil {
		d.fail(w, r, err)
		return
	}
	d.render(w, r, "index", "layout", indexView{
		Stats: stats, Rows: rows, Statuses: tabOrder, Status: status, Queue: queue,
	})
}

func (d *dashboard) statsPartial(w http.ResponseWriter, r *http.Request) {
	stats, err := d.stats(r.Context())
	if err != nil {
		d.fail(w, r, err)
		return
	}
	d.render(w, r, "index", "stats", stats)
}

func (d *dashboard) jobsPartial(w http.ResponseWriter, r *http.Request) {
	status, queue, cursor, ok := listParams(w, r)
	if !ok {
		return
	}
	rows, err := d.rows(r.Context(), status, queue, cursor)
	if err != nil {
		d.fail(w, r, err)
		return
	}
	d.render(w, r, "index", "rows", rows)
}

func (d *dashboard) detail(w http.ResponseWriter, r *http.Request) {
	id, ok := parseID(w, r)
	if !ok {
		return
	}
	j, err := d.store.Get(r.Context(), id)
	if err != nil {
		d.fail(w, r, err)
		return
	}
	errs, err := d.store.Errors(r.Context(), id)
	if err != nil {
		d.fail(w, r, err)
		return
	}
	d.render(w, r, "job", "layout", detailView{Job: j, Errors: errs, Payload: prettyJSON(j.Payload)})
}

// retry re-enqueues a dead job. From a table row it returns the updated row;
// from the detail page it asks htmx to reload the page.
func (d *dashboard) retry(w http.ResponseWriter, r *http.Request) {
	id, ok := parseID(w, r)
	if !ok {
		return
	}
	j, err := d.store.RetryDead(r.Context(), id)
	if err != nil {
		d.fail(w, r, err)
		return
	}
	d.log.Info("dead job retried from dashboard", "job_id", id)
	if r.URL.Query().Get("from") == "detail" {
		w.Header().Set("HX-Refresh", "true")
		w.WriteHeader(http.StatusNoContent)
		return
	}
	d.render(w, r, "index", "row", j)
}

// delete removes a dead or succeeded job. An empty 200 response makes htmx
// replace the row with nothing; from the detail page it redirects home.
func (d *dashboard) delete(w http.ResponseWriter, r *http.Request) {
	id, ok := parseID(w, r)
	if !ok {
		return
	}
	if err := d.store.Delete(r.Context(), id); err != nil {
		d.fail(w, r, err)
		return
	}
	d.log.Info("job deleted from dashboard", "job_id", id)
	if r.URL.Query().Get("from") == "detail" {
		w.Header().Set("HX-Redirect", BasePath+"/")
	}
	w.WriteHeader(http.StatusOK)
}

func (d *dashboard) stats(ctx context.Context) (statsView, error) {
	stats, err := d.store.Stats(ctx)
	if err != nil {
		return statsView{}, err
	}
	view := statsView{Statuses: tabOrder}
	for _, s := range stats { // ordered by queue
		if n := len(view.Queues); n == 0 || view.Queues[n-1].Queue != s.Queue {
			view.Queues = append(view.Queues, queueRow{Queue: s.Queue, Counts: map[job.Status]int64{}})
		}
		view.Queues[len(view.Queues)-1].Counts[s.Status] = s.Count
	}
	return view, nil
}

func (d *dashboard) rows(ctx context.Context, status job.Status, queue string, cursor int64) (rowsView, error) {
	jobs, err := d.store.List(ctx, store.ListFilter{Status: status, Queue: queue, Cursor: cursor, Limit: pageSize})
	if err != nil {
		return rowsView{}, err
	}
	view := rowsView{Jobs: jobs, Status: status, Queue: queue}
	if len(jobs) == pageSize {
		view.NextCursor = jobs[len(jobs)-1].ID
	}
	return view, nil
}

// render executes the named template into a buffer first, so a template
// error never leaves a half-written page.
func (d *dashboard) render(w http.ResponseWriter, r *http.Request, page, name string, data any) {
	var buf bytes.Buffer
	if err := d.pages[page].ExecuteTemplate(&buf, name, data); err != nil {
		d.fail(w, r, fmt.Errorf("render %s/%s: %w", page, name, err))
		return
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	_, _ = buf.WriteTo(w)
}

// fail maps store errors to plain-text responses (shown by app.js for htmx
// requests). Internal details are logged, not sent.
func (d *dashboard) fail(w http.ResponseWriter, r *http.Request, err error) {
	switch {
	case errors.Is(err, store.ErrNotFound):
		http.Error(w, "Job not found.", http.StatusNotFound)
	case errors.Is(err, store.ErrInvalidState):
		http.Error(w, "The job's status changed; reload to see its current state.", http.StatusConflict)
	default:
		d.log.ErrorContext(r.Context(), "dashboard request failed", "path", r.URL.Path, "err", err)
		http.Error(w, "Internal error.", http.StatusInternalServerError)
	}
}

// listParams reads ?status=&queue=&cursor=; status defaults to dead.
func listParams(w http.ResponseWriter, r *http.Request) (job.Status, string, int64, bool) {
	q := r.URL.Query()
	status := job.Status(q.Get("status"))
	if status == "" {
		status = job.StatusDead
	}
	if !slices.Contains(tabOrder, status) {
		http.Error(w, "Unknown status.", http.StatusBadRequest)
		return "", "", 0, false
	}
	var cursor int64
	if v := q.Get("cursor"); v != "" {
		n, err := strconv.ParseInt(v, 10, 64)
		if err != nil || n < 1 {
			http.Error(w, "Invalid cursor.", http.StatusBadRequest)
			return "", "", 0, false
		}
		cursor = n
	}
	return status, q.Get("queue"), cursor, true
}

func parseID(w http.ResponseWriter, r *http.Request) (int64, bool) {
	id, err := strconv.ParseInt(chi.URLParam(r, "id"), 10, 64)
	if err != nil || id <= 0 {
		http.Error(w, "Invalid job id.", http.StatusBadRequest)
		return 0, false
	}
	return id, true
}

func prettyJSON(raw json.RawMessage) string {
	var buf bytes.Buffer
	if err := json.Indent(&buf, raw, "", "  "); err != nil {
		return string(raw)
	}
	return buf.String()
}

func requireBasicAuth(key string) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if !auth.ValidBasic(r, key) {
				w.Header().Set("WWW-Authenticate", `Basic realm="ForgeQ dashboard", charset="UTF-8"`)
				http.Error(w, "Unauthorized.", http.StatusUnauthorized)
				return
			}
			next.ServeHTTP(w, r)
		})
	}
}

func cacheFor(d time.Duration, next http.Handler) http.Handler {
	value := "public, max-age=" + strconv.Itoa(int(d.Seconds()))
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Cache-Control", value)
		next.ServeHTTP(w, r)
	})
}
