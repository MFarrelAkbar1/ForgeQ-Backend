# Urutan membaca kode

Urutan ini bergerak dari kode **tanpa I/O** ke kode yang **mengorkestrasi semuanya**. Setiap langkah hanya bergantung pada langkah sebelumnya, jadi kamu tidak perlu melompat-lompat. Total kode Go non-test sekitar 3.800 baris. Jangan baca semuanya sekaligus: satu tahap per sesi sudah cukup.

Tips: untuk setiap file, baca **test-nya juga**. Test di proyek ini ditulis sebagai spesifikasi (table-driven, nama test menjelaskan perilaku).

---

## Tahap 1: domain murni (tanpa database, tanpa jaringan)

| # | File | Yang dicari | Pertanyaan untuk dirimu |
|---|---|---|---|
| 1 | [`internal/job/job.go`](../../internal/job/job.go) | `Status`, `transitions`, `CanTransition`, struct `Job` | Transisi mana yang **tidak** ada, dan kenapa? (misalnya `succeeded → apa pun`) |
| 2 | [`internal/job/job_test.go`](../../internal/job/job_test.go) | `TestCanTransition` memeriksa **semua** pasangan status | Kenapa memeriksa 5×5 pasangan, bukan hanya yang diizinkan? |
| 3 | [`internal/job/new.go`](../../internal/job/new.go) | `NewJob.Normalize`, `ValidationError` | Kenapa validasi ada di `job`, bukan di handler HTTP? |
| 4 | [`internal/backoff/backoff.go`](../../internal/backoff/backoff.go) + test | `Policy.Delay` | Bagaimana overflow dihindari? (→ [04](04-backoff-jitter.md)) |

## Tahap 2: konfigurasi dan utilitas kecil

| # | File | Yang dicari |
|---|---|---|
| 5 | [`internal/config/config.go`](../../internal/config/config.go) | `Load` mengumpulkan **semua** error dengan `errors.Join`; `parseDuration` (akhiran `d`); `ParseQueues` |
| 6 | [`internal/logging/logging.go`](../../internal/logging/logging.go) | slog JSON/text |
| 7 | [`internal/auth/auth.go`](../../internal/auth/auth.go) | `Equal` (constant-time), `BearerToken` (→ [11](11-keamanan.md)) |
| 8 | [`internal/ops/ops.go`](../../internal/ops/ops.go) | `/healthz` vs `/readyz`, interface `Mux` (→ [10](10-observability.md)) |

## Tahap 3: skema database

| # | File | Yang dicari |
|---|---|---|
| 9 | [`migrations/00001_create_jobs.sql`](../../migrations/00001_create_jobs.sql) | Kolom, `CHECK` status, empat indeks di tabel `jobs` (tiga di antaranya parsial) beserta alasannya di komentar atas setiap indeks, dan trigger lama |
| 10 | `migrations/00002` … `00004` | Index `CONCURRENTLY` + `NO TRANSACTION`; kolom `run`; penghapusan trigger beserta alasannya |
| 11 | [`migrations/migrations.go`](../../migrations/migrations.go), [`cmd/migrate/main.go`](../../cmd/migrate/main.go) | `//go:embed *.sql`, goose provider |

Setelah tahap ini, coba tulis sendiri di kertas: query apa yang akan kamu pakai untuk claim? Lalu bandingkan dengan `claimQuery` di tahap 4.

## Tahap 4: lapisan data (jantung sistem)

| # | File | Urutan baca di dalam file |
|---|---|---|
| 12 | [`internal/store/store.go`](../../internal/store/store.go) | `Connect` → `Enqueue` → `Get`/`List` → **`claimQuery`/`Claim`** → `Complete`/`Heartbeat`/`Release` (perhatikan klausa fence) → `Fail` (transaksi) → `RetryDead`/`Delete`/`notFoundOrInvalid` → **`reapQuery`/`Reap`** → `DeleteSucceededBefore` → `Stats` |
| 13 | [`internal/store/notifier.go`](../../internal/store/notifier.go) | `Notify` (tidak blocking) → `Run` (leading edge + window) → `flush` |
| 14 | [`internal/store/listener.go`](../../internal/store/listener.go) | `Listen` (reconnect + backoff) → `listenOnce` |

Pasangkan dengan [`test/integration/store_test.go`](../../test/integration/store_test.go) dan [`test/integration/main_test.go`](../../test/integration/main_test.go) (testcontainers, `newStore` melakukan TRUNCATE, helper `waitFor`).

## Tahap 5: worker

| # | File | Yang dicari |
|---|---|---|
| 15 | [`internal/worker/registry.go`](../../internal/worker/registry.go) | `HandlerFunc` (baca komentarnya: kontrak at-least-once), `RegisterHandler` (panic seperti `http.HandleFunc`), `Permanent`/`IsPermanent` |
| 16 | [`internal/worker/observer.go`](../../internal/worker/observer.go) | `Outcome`, interface `Observer`, `nopObserver` |
| 17 | [`internal/worker/worker.go`](../../internal/worker/worker.go) | `Store` interface (sisi konsumen) → `New`/`Notify` → **`Run`** (dua context) → **`runQueue`** (semaphore `slots`, `freed`, timer poll) → **`process`** (lease ctx, heartbeat, switch cause) → `heartbeat` → `execute` (timeout, recover) → `finish`/`release` → `truncate` |
| 18 | [`internal/worker/worker_test.go`](../../internal/worker/worker_test.go) | `fakeStore` di memori: contoh kenapa interface di sisi konsumen memudahkan test |
| 19 | [`internal/worker/reaper.go`](../../internal/worker/reaper.go) | `RunOnce`, loop batch selama penuh |
| 20 | [`internal/handlers/handlers.go`](../../internal/handlers/handlers.go) | Handler demo: `decode` → `Permanent`, `simulate` menghormati ctx; `bench` untuk loadgen |

Lalu baca test integrasinya dalam urutan ini: [`worker_test.go`](../../test/integration/worker_test.go) → [`reaper_test.go`](../../test/integration/reaper_test.go) → [`shutdown_test.go`](../../test/integration/shutdown_test.go) → [`notify_test.go`](../../test/integration/notify_test.go) → [`concurrency_test.go`](../../test/integration/concurrency_test.go) → [`atleastonce_test.go`](../../test/integration/atleastonce_test.go).

## Tahap 6: HTTP

| # | File | Yang dicari |
|---|---|---|
| 21 | [`internal/api/respond.go`](../../internal/api/respond.go) | Format error, `decodeJSON` (1 MiB, satu objek, tanpa field asing), `instrument` (pola rute) |
| 22 | [`internal/api/api.go`](../../internal/api/api.go) | `Store`/`Observer` interface, `NewRouter` (urutan middleware), `enqueue` (422 vs 400, 201 vs 200), `storeError` (404/409), `requireAPIKey` |
| 23 | [`internal/metrics/metrics.go`](../../internal/metrics/metrics.go) | Recorder untuk `worker.Observer` dan `api.Observer`; `queueCollector` |
| 24 | [`internal/dashboard/dashboard.go`](../../internal/dashboard/dashboard.go) + `templates/` | `//go:embed`, htmx partials, `requireBasicAuth`, `CrossOriginProtection` |

## Tahap 7: perakitan (wiring)

| # | File | Yang dicari |
|---|---|---|
| 25 | [`cmd/api/main.go`](../../cmd/api/main.go) | Notifier dengan ctx sinyal, timeout server HTTP, shutdown |
| 26 | [`cmd/worker/main.go`](../../cmd/worker/main.go) | **Notifier dengan ctx sendiri** (hidup lebih lama dari `w.Run`), `backoff.Policy{...}.Delay` sebagai fungsi, urutan tutup |

Pada titik ini kamu sudah bisa menggambar ulang diagram di [00-gambaran-besar.md](00-gambaran-besar.md) tanpa melihat.

## Tahap 8: operasi dan pengukuran

| # | File | Yang dicari |
|---|---|---|
| 27 | [`docker-compose.yml`](../../docker-compose.yml) | Anggaran koneksi, `stop_grace_period`, healthcheck `/readyz`, profil `monitoring` |
| 28 | [`Dockerfile`](../../Dockerfile), [`Taskfile.yml`](../../Taskfile.yml), [`.github/workflows/ci.yml`](../../.github/workflows/ci.yml) | Satu image untuk tiga binary; perintah `task`; CI dengan `-race` |
| 29 | [`cmd/loadgen/`](../../cmd/loadgen/) | `main.go` → `enqueue.go` → `measure.go` (pengukuran di SQL) → `chaos.go` (SIGKILL acak, menghitung log eksekusi) |

---

## Cara membaca yang efektif

1. **Baca dengan pertanyaan.** Sebelum membuka `store.go`, tanyakan: "bagaimana dua worker tidak mengambil job yang sama?" Lalu cari jawabannya.
2. **Ikuti satu job**, seperti di [01-perjalanan-satu-job.md](01-perjalanan-satu-job.md), dengan "Go to Definition" di editor.
3. **Rusak sesuatu.** Setiap file konsep punya bagian Eksperimen. Melihat test gagal dengan pesan yang spesifik adalah cara tercepat untuk memahami gunanya sebuah baris.
4. **Jelaskan dengan suara keras.** Kalau kamu bisa menjelaskan `claimQuery` baris per baris tanpa melihat, kamu siap ditanya di interview.
