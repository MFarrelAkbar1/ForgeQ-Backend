package worker

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/MFarrelAkbar1/ForgeQ/internal/job"
)

// HandlerFunc processes one job. Returning nil marks the job succeeded; any
// other error schedules a retry unless it is wrapped with Permanent.
//
// Delivery is at-least-once: a job may run again after a crash or lease loss,
// so handlers must be idempotent. ctx is cancelled when the job's timeout
// expires or the worker loses the job's lease (context.Cause(ctx) is then
// store.ErrLeaseLost); handlers should stop promptly when it is done.
type HandlerFunc func(ctx context.Context, j *job.Job) error

// HandlerOption configures a registered handler.
type HandlerOption func(*handler)

// WithTimeout overrides the worker's default per-job timeout for this type.
func WithTimeout(d time.Duration) HandlerOption {
	return func(h *handler) { h.timeout = d }
}

type handler struct {
	fn      HandlerFunc
	timeout time.Duration // zero means the worker default
}

// Registry maps job types to handlers. It is safe for concurrent use.
type Registry struct {
	mu       sync.RWMutex
	handlers map[string]handler
}

// NewRegistry returns an empty registry.
func NewRegistry() *Registry {
	return &Registry{handlers: make(map[string]handler)}
}

// RegisterHandler registers fn for jobType. Like http.HandleFunc it panics on
// programmer errors: an empty type, a nil handler or a duplicate registration.
func (r *Registry) RegisterHandler(jobType string, fn HandlerFunc, opts ...HandlerOption) {
	if jobType == "" || fn == nil {
		panic("worker: RegisterHandler requires a job type and a handler")
	}
	h := handler{fn: fn}
	for _, opt := range opts {
		opt(&h)
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	if _, dup := r.handlers[jobType]; dup {
		panic(fmt.Sprintf("worker: handler for %q already registered", jobType))
	}
	r.handlers[jobType] = h
}

func (r *Registry) lookup(jobType string) (handler, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	h, ok := r.handlers[jobType]
	return h, ok
}

// Types returns the registered job types, sorted.
func (r *Registry) Types() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	types := make([]string, 0, len(r.handlers))
	for t := range r.handlers {
		types = append(types, t)
	}
	sort.Strings(types)
	return types
}

// permanentError marks an error that retrying cannot fix.
type permanentError struct{ err error }

func (e *permanentError) Error() string { return e.err.Error() }
func (e *permanentError) Unwrap() error { return e.err }

// Permanent wraps err so the job goes straight to dead instead of retrying,
// e.g. for an invalid payload.
func Permanent(err error) error {
	if err == nil {
		return nil
	}
	return &permanentError{err: err}
}

// IsPermanent reports whether err was wrapped with Permanent.
func IsPermanent(err error) bool {
	_, ok := errors.AsType[*permanentError](err)
	return ok
}
