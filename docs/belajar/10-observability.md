# 10 · Observability: health probe, metrics, kardinalitas

## Masalahnya apa?

Tiga situasi di produksi:

1. **Database restart 20 detik** untuk upgrade minor. Kubernetes memeriksa `/healthz` semua pod API dan worker, dan semuanya gagal karena DB tidak bisa dihubungi. Kubernetes me-**restart semua pod sekaligus**. Saat DB kembali, 20 pod start bersamaan dan menyerbu DB dengan koneksi baru. Gangguan 20 detik berubah menjadi gangguan 3 menit.
2. **"Kenapa email telat?"** Tanpa angka, tidak ada yang tahu apakah antrean menumpuk, worker sering gagal, atau handler lambat.
3. **Prometheus kehabisan memori.** Seseorang menambahkan label `path` ke metrik HTTP. Bot pemindai mengirim request ke `/api/v1/jobs/1`, `/api/v1/jobs/2`, ... sampai `/wp-admin/xyz123`. Setiap path unik menjadi **time series** baru: jutaan series dalam sehari.

## Konsep umumnya

**Liveness vs readiness** (istilah Kubernetes, berlaku juga untuk Docker healthcheck dan load balancer):

| Probe | Pertanyaannya | Kalau gagal |
|---|---|---|
| Liveness | "Apakah proses ini macet/rusak dan perlu di-restart?" | Proses **di-restart** |
| Readiness | "Apakah proses ini bisa melayani kerja **sekarang**?" | Proses **dikeluarkan sementara** dari traffic, tanpa restart |

Liveness **tidak boleh** bergantung pada dependency eksternal. Restart tidak akan memperbaiki database yang mati.

**Metrik Prometheus**: counter (hanya naik), gauge (naik-turun), histogram (distribusi durasi). Setiap kombinasi nilai label adalah satu *time series*. **Kardinalitas** adalah jumlah kombinasi itu. Aturannya: **nilai label harus berasal dari himpunan kecil yang dikendalikan server, bukan dari input klien.**

## Implementasi di ForgeQ

### Probe: [`internal/ops/ops.go`](../../internal/ops/ops.go)

```go
//	/healthz  liveness: the process is up (never checks dependencies, so a
//	          database outage does not get every pod restarted at once)
//	/readyz   readiness: dependencies are reachable (503 otherwise)
//	/metrics  Prometheus exposition, when metrics is non-nil
func Register(mux Mux, ready ReadyFunc, metrics http.Handler) {
    mux.Handle("/healthz", http.HandlerFunc(liveness))
    mux.Handle("/readyz", readiness(ready))
    ...
}
```

- `liveness` selalu menjawab `200 {"status":"ok"}`.
- `readiness` memanggil `ready(ctx)` (yaitu `pool.Ping`) dengan timeout 2 s. Kalau gagal, jawabannya `503 {"status":"unavailable"}`. Perhatikan komentarnya: *"The cause is not echoed: it may contain hostnames or credentials."*
- `Mux` adalah interface kecil (`Handle(pattern, h)`), sehingga `Register` bisa dipasang di chi router (API) maupun `http.ServeMux` (worker, lewat `NewMux` di `:9090`).
- `docker-compose.yml` memakai `/readyz` sebagai healthcheck.

### Metrik worker: [`internal/metrics/metrics.go`](../../internal/metrics/metrics.go)

| Metrik | Tipe | Label |
|---|---|---|
| `forgeq_jobs_inflight` | gauge | `queue` |
| `forgeq_jobs_processed_total` | counter | `queue`, `type`, `outcome` |
| `forgeq_job_duration_seconds` | histogram | `queue`, `type` |
| `forgeq_jobs_reaped_total` | counter | `status` |
| `forgeq_jobs_retention_deleted_total` | counter | (tidak ada) |

Package `worker` **tidak mengimpor Prometheus**. Ia hanya mengenal interface `worker.Observer` ([`internal/worker/observer.go`](../../internal/worker/observer.go)): `JobStarted`, `JobFinished`, `JobReaped`, `JobsDeleted`. `metrics.Worker` mengimplementasikannya. Arah dependensinya `metrics → worker`, bukan sebaliknya, sehingga unit test worker memakai `recordingObserver` sederhana.

### Menjaga kardinalitas

**Label `type` di worker**: tipe job berasal dari **klien** (`POST {"type": "apa-saja"}`). `(*Worker).typeLabel` membatasinya:

```go
// typeLabel bounds metric cardinality: job types come from clients, so only
// registered types are reported by name.
func (w *Worker) typeLabel(jobType string) string {
    if _, ok := w.registry.lookup(jobType); ok {
        return jobType
    }
    return "unknown"
}
```

Label `queue` di worker aman karena nilainya berasal dari konfigurasi worker (`WORKER_QUEUES`), bukan dari klien. Fetch loop hanya meng-claim queue yang terdaftar.

**Label `route` di API**: middleware `instrument` di [`internal/api/respond.go`](../../internal/api/respond.go) memakai **pola rute chi**, bukan path mentah:

```go
route := "unmatched"
if rc := chi.RouteContext(r.Context()); rc != nil && rc.RoutePattern() != "" {
    route = rc.RoutePattern()            // "/api/v1/jobs/{id}", bukan "/api/v1/jobs/96517"
}
obs.HTTPRequest(r.Method, route, status, took)
```

Request ke path yang tidak ada (pemindai, salah ketik) semuanya jatuh ke satu label `unmatched`. Middleware ini dipasang di **router paling atas**, sehingga 404 juga tercatat. Path probe dilewati karena di-hit setiap beberapa detik.

**Metrik API tidak memakai label queue/type sama sekali** (komentar `NewAPI`: *"Labels deliberately avoid client-chosen values"*). Angka per queue diambil dari kolektor kedalaman antrean.

### Kedalaman antrean: `queueCollector`

`forgeq_jobs{queue,status}` dibaca **dari PostgreSQL setiap kali di-scrape** (`Store.Stats`, `GROUP BY queue, status`):

- **Tepat dan konsisten antar-replika.** Counter di memori satu proses API tidak akan melihat job yang di-enqueue lewat replika lain.
- **Semua status dilaporkan**, termasuk yang 0, sehingga grafik dan alert tidak "bolong" saat sebuah status kosong (`TestQueueDepthFillsMissingStatuses`).
- **Kalau query gagal**, kolektor mengirim `prometheus.NewInvalidMetric` sehingga scrape menunjukkan error, alih-alih angka 0 yang menyesatkan.

Label `queue` di sini memang berasal dari data. Jumlahnya dibatasi oleh jumlah queue yang benar-benar ada di tabel, dan retensi menjaga tabel tetap kecil. Ini dicatat sebagai kompromi yang disadari.

### Registry per proses

`metrics.NewRegistry()` membuat registry baru (bukan `prometheus.DefaultRegisterer` global), sehingga test bisa membuat registry sendiri tanpa bentrok "duplicate registration".

### Log terstruktur

`log/slog` dengan kunci konsisten: `job_id`, `queue`, `type`, `attempt`, `worker_id`, `err`, `took`. Setiap request HTTP mendapat `request_id` (`middleware.RequestID`) yang juga tercantum di log error internal.

## Apa yang terjadi kalau fitur ini dihapus?

- **`/healthz` memeriksa DB**: gangguan DB singkat memicu restart massal (skenario 1).
- **Label `type` mentah**: klien bisa membuat jutaan series dengan mengirim tipe acak, yang semuanya gagal sebagai `no handler registered` tetapi tetap tercatat.
- **Label path mentah**: setiap id job menjadi series sendiri.
- **Tanpa metrik**: semua pertanyaan operasional dijawab dengan menebak.

## Trade-off dan alternatif

| Pilihan | Untung | Rugi |
|---|---|---|
| Queue depth dari SQL setiap scrape | Tepat, satu sumber kebenaran | Satu `GROUP BY` per scrape. Murah karena retensi, tetapi untuk tabel raksasa lebih baik memakai estimasi atau counter yang di-cache. |
| `/metrics` tanpa auth | Prometheus mudah men-scrape | Nama queue terlihat publik; di produksi, batasi lewat jaringan |
| Observer interface | Worker tidak terikat Prometheus | Satu lapisan indireksi |
| Label `unknown` | Kardinalitas terbatas | Kehilangan detail tipe mana yang tidak terdaftar (masih ada di log) |

## Eksperimen

Semua unit test, tanpa Docker. Branch: `git switch -c coba/observability`.

### Eksperimen A: label `type` mentah

Di `(*Worker).typeLabel`, ganti seluruh isinya dengan `return jobType`.

```sh
go test -count=1 -v -run '^TestWorkerReportsOutcomes$' ./internal/worker/
```

**Hasil yang diharapkan:**

```
worker_test.go:414: outcome[unknown] = "", want "dead"
--- FAIL: TestWorkerReportsOutcomes
```

Tipe `client-invented-type` sekarang muncul sebagai label sendiri, bukan `unknown`.

### Eksperimen B: label path mentah

Di `instrument` (`internal/api/respond.go`):

```diff
-				route = rc.RoutePattern()
+				route = r.URL.Path
```

```sh
go test -count=1 -v -run '^TestInstrumentationUsesRoutePatterns$' ./internal/api/
```

**Hasil yang diharapkan:**

```
api_test.go:332: routes = ["POST /api/v1/jobs 201" "GET /api/v1/jobs/1 200" "GET /api/v1/jobs/2 404" "GET unmatched 404"],
                 want ["POST /api/v1/jobs 201" "GET /api/v1/jobs/{id} 200" "GET /api/v1/jobs/{id} 404" "GET unmatched 404"]
--- FAIL: TestInstrumentationUsesRoutePatterns
```

Dua job berarti dua series. Satu juta job berarti satu juta series.

(Catatan: menghapus seluruh blok `if` akan membuat variabel `rc`/import `chi` tidak terpakai dan kode gagal di-compile. Karena itu yang diganti hanya isinya.)

### Eksperimen C: liveness memeriksa database

Di `ops.Register`:

```diff
-	mux.Handle("/healthz", http.HandlerFunc(liveness))
+	mux.Handle("/healthz", readiness(ready))
```

```sh
go test -count=1 -v -run '^TestEndpoints$' ./internal/ops/
```

**Hasil yang diharapkan:**

```
ops_test.go:43: /healthz must not depend on the database, got 503
--- FAIL: TestEndpoints
```

Bersihkan: `git restore . && git switch main && git branch -D coba/observability`.

### Bonus: lihat metriknya langsung

```sh
task up
curl -s localhost:8081/metrics | grep '^forgeq_jobs{'
docker compose exec worker wget -qO- localhost:9090/metrics | grep forgeq_jobs_processed_total
```

## Latihan interview

1. Apa beda liveness dan readiness probe? Kenapa `/healthz` ForgeQ tidak memeriksa database?
2. Apa itu kardinalitas metrik, dan kenapa label dari input klien berbahaya?
3. Bagaimana ForgeQ mencegah label `type` dan `route` meledak?
4. Kenapa kedalaman antrean dibaca dari database setiap scrape, bukan dihitung di memori API?
5. Kenapa `/readyz` tidak menampilkan pesan error-nya?

### Jawaban

1. Liveness menjawab "perlu di-restart?", readiness menjawab "bisa melayani sekarang?". Kalau `/healthz` bergantung pada DB, gangguan DB singkat membuat orkestrator me-restart semua replika sekaligus. Restart tidak memperbaiki DB, malah menambah badai koneksi saat DB pulih. Ketergantungan DB cukup dicek di `/readyz`, yang hanya mengeluarkan replika dari traffic sementara.
2. Kardinalitas adalah jumlah kombinasi unik nilai label. Setiap kombinasi adalah satu time series yang memakan memori Prometheus selamanya (sampai retensi TSDB). Kalau nilai label berasal dari input klien (path, id, tipe job), klien atau bot bisa membuat series tanpa batas, sehingga terjadi kehabisan memori dan query yang lambat. Efeknya sama dengan DoS.
3. `type` hanya dilaporkan dengan namanya kalau terdaftar di `Registry`, dan sisanya menjadi `unknown` (`typeLabel`). Rute memakai pola chi (`/api/v1/jobs/{id}`), dan request yang tidak cocok rute mana pun menjadi `unmatched`. Metrik API juga sengaja tidak memakai label queue/type.
4. Karena ada banyak replika API dan worker. Counter di memori satu proses hanya melihat sebagian kejadian dan hilang saat restart. Database adalah satu-satunya sumber kebenaran, sehingga `GROUP BY` saat scrape memberi angka yang tepat dan sama dari replika mana pun.
5. Error koneksi bisa berisi hostname, port, nama user, bahkan potongan connection string. `/readyz` tidak diautentikasi, jadi detail itu cukup dicatat di log internal dan respons hanya berisi `unavailable`. `TestEndpoints` memastikan `10.0.0.5` tidak bocor.
