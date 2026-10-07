# ForgeQ: notes for Claude

ForgeQ is a distributed job queue in Go (similar in spirit to Sidekiq/Celery). It is a **portfolio project**, so code should be clean, idiomatic and easy to explain in an interview. When in doubt, choose the simpler design.

## Architecture

- **PostgreSQL is the only source of truth.** There is no Redis. All state transitions are guarded SQL statements in `internal/store`.
- `cmd/api` serves the HTTP API (chi), the htmx dashboard at `/ui` (`/` redirects there), and `/healthz`, `/readyz`, `/metrics`.
- `cmd/worker` runs a fetch loop per queue. Each loop claims up to its free slots with `FOR UPDATE SKIP LOCKED` and runs jobs on a semaphore-bounded pool. Each running job has a heartbeat goroutine. Every worker process also runs a `Reaper` (expired leases and retention).
- `cmd/migrate` runs the goose migrations embedded from `migrations/`.
- `cmd/loadgen` enqueues `bench` jobs over HTTP and measures results in SQL (DB clock only). `-chaos` kills or stops compose worker containers at random and counts executions from the `bench job executed` log lines.
- The worker serves `/healthz`, `/readyz` and `/metrics` on `OPS_ADDR` (:9090).
- Docker: one image holds all three binaries, and compose picks one per service. The stack is postgres → migrate (one-shot) → api + 2 worker replicas. Prometheus is optional (`--profile monitoring`, `deploy/prometheus.yml`).
- **Wake-up:** `store.Notifier` sends `pg_notify('forgeq_jobs', queue)` from its *own* transaction, coalesced to at most one per queue per 50 ms. The store calls it after Enqueue (non-delayed), RetryDead, Release and Reap. Never NOTIFY inside job-writing transactions or triggers: NOTIFY takes a global lock at commit, which serialized enqueues (~150/s instead of ~3000/s; trigger removed in migration 00004). `store.Listen` uses a *dedicated* `pgx.Conn` (not from the pool) and reconnects with backoff. Polling (`POLL_INTERVAL`) is always on as the fallback, and delayed jobs are found only by polling.

### Package layout and dependency direction
```
cmd/*            -> wiring only (config, logger, pool, signal handling)
internal/api     -> HTTP API + composition of ops/dashboard routes; consumer-side Store + Observer
internal/dashboard -> html/template + htmx (embedded, incl. htmx.min.js 2.0.11); basic auth + CrossOriginProtection
internal/ops     -> /healthz (liveness, no deps), /readyz (DB ping, no error detail), /metrics mount
internal/metrics -> Prometheus recorders implementing api.Observer / worker.Observer; per-process registry
internal/auth    -> constant-time key checks (bearer for API, basic for dashboard)
internal/worker  -> Registry, pool, heartbeat, shutdown, Reaper, Permanent errors; consumer-side Store/ReaperStore
internal/store   -> pgx SQL, ErrNotFound / ErrLeaseLost / ErrInvalidState, Listen
internal/backoff -> pure backoff.Policy.Delay (injectable Rand)
internal/job     -> pure domain: Job, Status, CanTransition, NewJob.Normalize (no I/O)
internal/config  -> env parsing (all errors joined)
internal/logging -> slog setup
internal/handlers-> demo handlers (send_email, image_resize, generate_report)
```
`job` imports nothing internal. Interfaces are defined where they are consumed (`api.Store`, `worker.Store`, `dashboard.Store`, the `Observer`s), and `*store.Store` satisfies all the Store interfaces. `metrics` imports `api` and `worker` only for their Observer types, never the reverse.

## Job state machine
`pending -> running -> succeeded | retrying | dead`, `retrying -> running`, `running -> pending` (lease released on shutdown), `dead -> pending` (manual retry). The source of truth is `transitions` in `internal/job/job.go`. Update `TestCanTransition` whenever it changes.

## Invariants (do not break these)
- `attempts` is incremented **at claim time**. A crash still uses up an attempt.
- **Fencing:** every write by a worker (`Heartbeat`, `Complete`, `Fail`, `Release`) includes `WHERE status='running' AND locked_by=$worker`. Zero rows affected means `store.ErrLeaseLost`. When a heartbeat gets 0 rows, the job's ctx is cancelled with cause `ErrLeaseLost` and the worker writes nothing afterwards. The heartbeat is stopped before Complete/Fail so the two never race.
- **Shutdown:** jobs run under a jobs context that is separate from the signal ctx. A signal stops claiming. After `SHUTDOWN_TIMEOUT` jobs are cancelled with cause `worker.ErrShutdown` and then `Release`d (pending, `attempts-1`). A handler that succeeds despite the cancellation is still Completed.
- **Reaper:** a single statement moves expired leases to retrying (`run_at=now()`) or dead and logs `store.LeaseExpiredError` in job_errors. It is safe to run in every process (SKIP LOCKED). Retention deletes only `succeeded` jobs, in batches of 1000.
- Status-guarded statements that match 0 rows use `notFoundOrInvalid` to return 404 vs 409.
- Idempotent enqueue uses `INSERT ... ON CONFLICT (queue, idempotency_key) WHERE idempotency_key IS NOT NULL DO NOTHING`, then a `SELECT` (retried if the row vanished). Never check first and insert after.
- Backoff exponent is `attempts-1` (attempts is already incremented): `min(base*2^(attempts-1) + jitter, max)`.
- Delivery is at-least-once. Handlers must be idempotent.
- Result reporting (`Complete`/`Fail`) uses its own context, not the shutdown ctx.
- Error strings are truncated on a UTF-8 rune boundary (Postgres rejects invalid UTF-8).
- `jobs.run` starts at 1 and `RetryDead` increments it. Every `job_errors` row stores `(run, attempt)`.
- **Metrics cardinality:** never use client-chosen values as labels. The API has no queue/type labels, worker job types not in the registry become `unknown`, and HTTP routes use the chi route pattern (unmatched requests become `unmatched`).
- **Auth:** `API_KEY` protects all of `/api/v1` (Bearer) and the whole dashboard (basic auth, any username). `/healthz`, `/readyz` and `/metrics` stay open for probes and Prometheus. Dashboard POST/DELETE also go through `http.CrossOriginProtection`, because browsers attach basic-auth credentials to cross-site requests.
- Request instrumentation (log + metrics) runs at the top-level router and skips only the probe paths.
- **Connection budget:** `DB_MAX_CONNS` (default 10; api 25 in compose) is applied in `store.Connect`. Each worker also uses one LISTEN connection. Never pass `pool_max_conns` in `DATABASE_URL`: migrate and LISTEN use plain pgx or database/sql and forward it to the server as an unknown parameter.
- Delivery is at-least-once, proven by `TestStalledWorkerCausesDuplicateExecution`. Never claim exactly-once in docs.
- `status` is `TEXT` + `CHECK`, not a PG enum, so adding a state is a plain migration and pgx needs no type registration.

## Conventions
- Go 1.27. Prefer the stdlib (`log/slog`, `errors.AsType`, `sync.WaitGroup.Go`, `strings.SplitSeq`). The only third-party libraries are chi, pgx, goose, prometheus/client_golang and testcontainers-go.
- Config comes only from env vars (`internal/config`). Add new ones to `.env.example` and the README.
- Log keys: `job_id`, `queue`, `type`, `attempt`, `worker_id`, `err`, `took`.
- API errors: `{"error":{"code","message","fields"}}`. Validation failures return 422, malformed JSON returns 400. Internal errors are logged but never leaked to the client.
- Wrap errors with context (`fmt.Errorf("claim jobs: %w", err)`), and compare with `errors.Is`/`errors.AsType`.
- Tests are table-driven and use the stdlib `testing` package only (no testify). Unit tests use fakes. Integration tests live in `test/integration` behind `//go:build integration`, share one container and must not use `t.Parallel()`.
- The user is on Windows. Use the Taskfile (no make), keep files LF (`.gitattributes`), and when editing files with scripts write with `newline=''`. In Python `re.sub` replacements, do not use `` backrefs inside non-raw strings (they became U+0001 once); use a function replacement instead. Do not pass multi-line commit messages through PowerShell 5.1 (quoting breaks); use Bash heredocs.

## Commands (Taskfile)
`task check` (build + vet + unit + integration), `task test`, `task test:integration`, `task test:race` (gcc from Strawberry Perl is available here), `task up` / `task up:monitoring` / `task down` / `task logs` (full compose stack), `task db:up`, `task migrate [-- status|down]`, `task run:api`, `task run:worker`. Locally `.env` maps Postgres to 5433, the API to 8081 and Prometheus to 9091, because 5432 and 8080 are already taken on this machine.

## Process
Work in phases. Inside a phase, commit per logical unit, and every commit must pass `go build`, `go vet` (including `-tags integration`) and all tests. Before finishing a phase, also run the timing-sensitive tests repeatedly (`-count=5`) and with `-race`. At the end of each phase, write a per-feature summary for the user (the problem solved and its trade-offs, for interview prep). Ask the user before any design decision with significant trade-offs.

## Phase status
- [x] Phase 1: schema/migrations, enqueue/get/list API, basic worker with SKIP LOCKED, registry and demo handlers, LISTEN/NOTIFY, idempotency
- [x] Phase 2: exponential backoff + jitter, fenced heartbeat (cancel ctx on lease loss), reaper, retention (`JOB_RETENTION`, default 7d), dead-letter retry/delete endpoints, graceful shutdown with timeout and lease release
- [x] Phase 3: htmx dashboard (embed), `/metrics`, `/healthz` + `/readyz`, `API_KEY` auth for mutations and the dashboard, Dockerfile, full docker-compose, `run` column in job_errors
- [x] Phase 4: API_KEY on all of /api/v1, large concurrency tests, coalescing notifier (trigger removed), DB_MAX_CONNS, `cmd/loadgen` (bench + chaos), at-least-once test, GitHub Actions CI (+race), full README with benchmarks, secret scan (gitleaks clean)

## Benchmarks
Run them with `task up && task bench` (loadgen runs inside the compose network) and `task chaos` (from the host, needs the docker CLI). When numbers change, update the README table and machine specs. On this laptop a Postgres commit takes about 3.8 ms (Docker Desktop/WSL2), which bounds per-worker throughput for short jobs.
