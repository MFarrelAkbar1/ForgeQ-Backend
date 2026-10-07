# 08 · Graceful shutdown

## Masalahnya apa?

Setiap deploy menghentikan worker lama. Docker (atau Kubernetes) mengirim `SIGTERM`, menunggu *grace period*, lalu `SIGKILL`. Saat itu worker sedang menjalankan 10 job, misalnya `generate_report` yang butuh 90 detik.

Kalau worker langsung keluar begitu menerima `SIGTERM`:
- Job yang 2 detik lagi selesai ikut terbuang.
- Job-job itu tetap `running` di database sampai lease kedaluwarsa (30 s) dan reaper mengambilnya (+15 s). Selama itu tidak ada yang mengerjakannya.
- Setiap job yang terpotong **memakan satu attempt** (dihitung saat claim). Lima kali deploy dalam sehari bisa membuat job yang sehat jatuh ke `dead`.

Sebaliknya, kalau worker menunggu semua job selesai tanpa batas, `SIGKILL` dari orkestrator akan datang duluan, dan hasilnya sama buruknya.

## Konsep umumnya

Graceful shutdown terdiri dari beberapa fase:

1. **Berhenti menerima kerja baru** (stop claiming).
2. **Beri waktu** pekerjaan yang sedang berjalan untuk selesai secara normal.
3. Setelah tenggat lewat, **batalkan** sisanya dan **kembalikan** ke antrean dengan rapi.
4. Keluar **sebelum** orkestrator mengirim `SIGKILL`.

Di Go, sinyal ini disalurkan lewat `context`. Kuncinya: context "berhenti claim" dan context "batalkan job" harus **berbeda**. Kalau sama, job langsung dibatalkan begitu sinyal datang.

## Implementasi di ForgeQ

### Sinyal → ctx (`cmd/worker/main.go`)

```go
ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
...
err = w.Run(ctx)
```

### Dua context: `(*Worker).Run` di [`internal/worker/worker.go`](../../internal/worker/worker.go)

```go
// Jobs run under their own context, not ctx, so a shutdown signal only
// stops claiming; jobs are cancelled explicitly once the timeout passes.
jobsCtx, stopJobs := context.WithCancelCause(context.Background())
defer stopJobs(nil)

var fetchers, inflight sync.WaitGroup
for queue, concurrency := range w.cfg.Queues {
    fetchers.Go(func() { w.runQueue(ctx, jobsCtx, &inflight, queue, concurrency) })
}
fetchers.Wait()                                    // (1) semua fetch loop berhenti claim

if !waitTimeout(&inflight, w.cfg.ShutdownTimeout) { // (2) tunggu job, maks 25 s
    stopJobs(ErrShutdown)                           // (3) batalkan dengan cause
    if !waitTimeout(&inflight, releaseGrace) {      // (4) beri 5 s untuk Release
        w.log.Error("some handlers ignored cancellation; the reaper will retry ...")
    }
}
```

1. `runQueue` memakai `ctx` (sinyal) untuk `Claim` dan untuk `select`-nya, sehingga ia keluar begitu sinyal datang. Job dijalankan dengan `w.process(jobsCtx, j)`.
2. `waitTimeout` menunggu `inflight` (WaitGroup semua job yang berjalan) maksimal `SHUTDOWN_TIMEOUT`.
3. `stopJobs(ErrShutdown)` membatalkan semua job dengan **cause** `worker.ErrShutdown`, supaya `process` bisa membedakannya dari timeout atau lease loss.
4. Handler yang mengabaikan ctx akan ditinggal setelah 5 s (`releaseGrace`). Lease-nya kedaluwarsa dan reaper yang mengurusnya.

### Release: mengembalikan tanpa memakan attempt

Di `(*Worker).process`:

```go
switch cause := context.Cause(leaseCtx); {
case errors.Is(cause, store.ErrLeaseLost):
    ...
case err != nil && errors.Is(cause, ErrShutdown):
    // Interrupted by shutdown, not by a fault of the job: hand it back.
    outcome = w.release(j, log)
default:
    // Includes a handler that finished successfully despite the shutdown.
    outcome = w.finish(j, err, took, log)
}
```

Perhatikan `err != nil &&`: handler yang **berhasil** walaupun shutdown sedang berlangsung tetap di-`Complete`. Kerja yang sudah selesai tidak dibuang.

`(*Store).Release` di [`internal/store/store.go`](../../internal/store/store.go):

```sql
UPDATE jobs
SET status = 'pending', attempts = attempts - 1, run_at = now(),
    locked_by = NULL, locked_until = NULL, started_at = NULL, updated_at = now()
WHERE id = $1 AND status = 'running' AND locked_by = $2   -- tetap di-fence
RETURNING queue
```

- **`attempts - 1`**: membatalkan attempt yang dihitung saat claim. Job tidak gagal, worker-nya saja yang berhenti.
- **`status = 'pending'`, `run_at = now()`**: langsung bisa diambil worker lain. Setelah itu `s.notify(queue)` dipanggil, dan Notifier (yang sengaja hidup lebih lama dari `w.Run`, lihat [07](07-listen-notify.md)) mengirim wake-up ke worker lain.
- **Tidak ada baris `job_errors`**: shutdown bukan kegagalan.

`release` dan `finish` memakai `context.Background()` + 10 s, bukan `jobsCtx` yang sudah dibatalkan (invariant: *result reporting uses its own context*).

### Urutan tutup di `main`

```go
err = w.Run(ctx)         // tunggu job selesai atau dilepas
stopNotifier()           // baru matikan notifier (flush terakhir)
<-notifierDone
opsSrv.Shutdown(...)     // /readyz dan /metrics tetap hidup sampai job selesai
```

### Batas waktu orkestrator

[`docker-compose.yml`](../../docker-compose.yml):

```yaml
SHUTDOWN_TIMEOUT: 25s
# Must exceed SHUTDOWN_TIMEOUT plus the 5s release grace, or Docker sends
# SIGKILL before interrupted jobs are released (default is only 10s).
stop_grace_period: 35s
```

25 s (tunggu) + 5 s (release) < 35 s (grace Docker). Default Docker hanya 10 s, sehingga tanpa setelan ini worker akan di-`SIGKILL` di tengah fase tunggu.

## Apa yang terjadi kalau fitur ini dihapus?

- **Job memakai ctx sinyal** (satu context saja): begitu `SIGTERM` datang, semua handler yang menghormati ctx langsung batal, termasuk yang 100 ms lagi selesai. Mereka dicatat sebagai **gagal** (`retrying`, `job_errors` bertambah, attempt terpakai).
- **Tanpa Release**: job yang terpotong ditulis `Fail`, sehingga attempt hilang dan ada error palsu di riwayat. Deploy yang sering bisa membuat job sehat menjadi `dead`.
- **Tanpa batas waktu**: worker menunggu job 10 menit, lalu orkestrator memakai `SIGKILL`, dan kita kembali ke kasus crash.

## Trade-off dan alternatif

| Pilihan | Untung | Rugi |
|---|---|---|
| Tunggu lalu release (ForgeQ) | Kerja yang hampir selesai tidak terbuang; yang terpotong tidak memakan attempt | Deploy lebih lambat (sampai 30 s per worker) |
| Langsung release semua | Deploy cepat | Job lama selalu diulang dari awal |
| Tunggu tanpa batas | Tidak pernah memotong | Bertabrakan dengan `SIGKILL` |
| `attempts - 1` saat release | Deploy tidak menghukum job | Job yang **selalu** terpotong (misalnya lebih lama dari `SHUTDOWN_TIMEOUT` dan deploy terjadi setiap 20 menit) tidak pernah kehabisan attempt. README menyebut ini disengaja. |

Satu catatan lagi: handler **harus** menghormati `ctx`. Handler yang mengabaikannya tidak bisa di-release dan akhirnya diurus reaper (attempt terpakai).

## Eksperimen

Branch: `git switch -c coba/shutdown`.

### Eksperimen A: job memakai context sinyal

Di `(*Worker).runQueue`:

```diff
-				w.process(jobsCtx, j)
+				w.process(ctx, j)
```

```sh
go test -count=1 -v -run '^TestShutdownWaitsForRunningJobs$' ./internal/worker/
go test -tags integration -count=1 -v -run '^TestShutdownLetsRunningJobsFinishAndStopsClaiming$' ./test/integration/
```

**Hasil yang diharapkan:**

```
worker_test.go:298: completed=[] released=[], want the job completed
--- FAIL: TestShutdownWaitsForRunningJobs
shutdown_test.go:110: running job status = retrying, want succeeded
--- FAIL: TestShutdownLetsRunningJobsFinishAndStopsClaiming
```

Job yang seharusnya sempat selesai dalam 300 ms malah dicatat **gagal** (`retrying`).

### Eksperimen B: tanpa Release

Hapus cabang `ErrShutdown` di `(*Worker).process`:

```diff
-	case err != nil && errors.Is(cause, ErrShutdown):
-		// Interrupted by shutdown, not by a fault of the job: hand it back.
-		outcome = w.release(j, log)
```

```sh
go test -count=1 -v -run '^TestShutdownTimeoutReleasesJobs$' ./internal/worker/
go test -tags integration -count=1 -v -run '^TestShutdownReleasesJobWithoutUsingAnAttempt$' ./test/integration/
```

**Hasil yang diharapkan:**

```
worker_test.go:325: released=[] failures=[{1 test-worker context canceled ...}] completed=[], want only a release
--- FAIL: TestShutdownTimeoutReleasesJobs
shutdown_test.go:70: released job = &{... Status:retrying Run:1 Attempts:1 ... LastError:0x...}, want pending with attempts 0 and no lock
--- FAIL: TestShutdownReleasesJobWithoutUsingAnAttempt
```

Job menjadi `retrying` dengan `attempts = 1` dan error `context canceled` di riwayat, padahal job-nya sendiri tidak salah apa-apa.

Bersihkan: `git restore . && git switch main && git branch -D coba/shutdown`.

## Latihan interview

1. Kenapa job berjalan di `jobsCtx` yang terpisah dari context sinyal?
2. Jelaskan empat fase shutdown di `Worker.Run`.
3. Kenapa `Release` mengurangi `attempts`? Apa risikonya?
4. Bagaimana `process` membedakan "dibatalkan karena shutdown" dari "dibatalkan karena timeout" atau "lease hilang"?
5. Kenapa `stop_grace_period` di compose diset 35 s?

### Jawaban

1. Supaya `SIGTERM` hanya menghentikan **claim**, bukan job. Job yang sedang berjalan diberi kesempatan selesai secara normal selama `SHUTDOWN_TIMEOUT`, dan baru dibatalkan secara eksplisit (`stopJobs(ErrShutdown)`) setelah tenggat lewat. Satu context untuk keduanya akan membatalkan semua job seketika.
2. (1) ctx dibatalkan, fetch loop keluar, dan `fetchers.Wait()` memastikan tidak ada claim baru. (2) Tunggu `inflight` sampai `SHUTDOWN_TIMEOUT`. (3) Kalau masih ada job, `stopJobs(ErrShutdown)` dan job yang handler-nya kembali dengan error di-`Release`. (4) Tunggu lagi sampai `releaseGrace` (5 s). Handler yang tetap bandel ditinggal, dan lease-nya akan diurus reaper.
3. `attempts` dinaikkan saat claim. Shutdown bukan kegagalan job, jadi attempt itu dikembalikan supaya deploy tidak "menghukum" job. Risikonya: job yang selalu terpotong oleh shutdown tidak pernah kehabisan attempt dan bisa berputar selamanya. Ini trade-off yang disengaja.
4. Lewat `context.Cause`. Shutdown membatalkan dengan cause `worker.ErrShutdown`, heartbeat membatalkan dengan `store.ErrLeaseLost`, dan timeout handler menghasilkan `context.DeadlineExceeded` dari `context.WithTimeout` di `execute`. `process` melakukan `switch` pada cause itu.
5. Docker mengirim `SIGKILL` setelah `stop_grace_period` (default 10 s). Worker butuh maksimal `SHUTDOWN_TIMEOUT` (25 s) + `releaseGrace` (5 s) = 30 s. Dengan 35 s ada sedikit ruang, sehingga Release selesai sebelum proses dibunuh.
