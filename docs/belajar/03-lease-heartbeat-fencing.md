# 03 · Lease, heartbeat dan fencing

## Masalahnya apa?

Setelah job di-claim (file [02](02-skip-locked-dan-claim.md)), statusnya `running`. Lalu tiga skenario ini terjadi:

1. **Worker mati mendadak.** Container di-`SIGKILL` oleh OOM killer saat menjalankan job `generate_report`. Tidak ada yang menulis `succeeded` atau `failed`, sehingga job `running` **selamanya**. Laporan pelanggan tidak pernah dibuat.
2. **Job memang lama.** `generate_report` butuh 2 menit. Bagaimana sistem membedakan "worker mati" dari "worker masih bekerja keras"?
3. **Worker "zombie".** Worker A terkena *GC pause* atau jaringannya putus selama 45 detik. Sistem mengira A mati dan memberikan job ke worker B. Lalu A bangun dan menulis `UPDATE jobs SET status='failed' ...`, sehingga **menimpa** hasil B yang sudah sukses.

Kebutuhan: (1) mendeteksi worker mati dalam waktu wajar, (2) tidak salah menuduh job lama sebagai mati, (3) memastikan pemilik lama **tidak bisa menulis** setelah haknya dicabut.

## Konsep umumnya

- **Lease (sewa)**: claim tidak berlaku selamanya, hanya "milik worker X **sampai jam T**". Kalau T lewat tanpa diperpanjang, job dianggap yatim.
- **Heartbeat**: selama job berjalan, worker memperpanjang T secara berkala ("saya masih hidup"). Lease bisa pendek (deteksi crash cepat), dan job lama tetap aman selama heartbeat berjalan.
- **Fencing**: setiap tulisan menyertakan identitas pemilik lease, dan database menolaknya kalau pemiliknya sudah berganti. Istilah ini dipopulerkan Martin Kleppmann (*fencing token*): token yang **berubah di setiap claim**. Token ForgeQ adalah `store.Lease` = `(job id, locked_by, run, attempts)` dari hasil claim, dan pengecekannya terjadi **di database, di dalam statement yang sama**.

Lease menjawab masalah 1 dan 2. Fencing menjawab masalah 3. Tanpa fencing, lease justru **berbahaya**: lease memungkinkan dua pemilik (A yang lama dan B yang baru), dan hanya fencing yang memastikan hanya satu yang boleh menulis.

## Implementasi di ForgeQ

### Kolom lease

Di [`migrations/00001_create_jobs.sql`](../../migrations/00001_create_jobs.sql): `locked_by TEXT` dan `locked_until TIMESTAMPTZ`. Keduanya diisi oleh `claimQuery` (`locked_until = now() + LEASE_DURATION`, default 30 s).

### Heartbeat: `(*Worker).heartbeat` di [`internal/worker/worker.go`](../../internal/worker/worker.go)

```go
t := time.NewTicker(w.cfg.HeartbeatInterval)          // default 10s
for {
    select {
    case <-ctx.Done():
        return
    case <-t.C:
    }
    hbCtx, cancel := context.WithTimeout(ctx, w.cfg.HeartbeatInterval)
    err := w.store.Heartbeat(hbCtx, store.LeaseOf(j, w.cfg.ID), w.cfg.LeaseDuration)
    cancel()
    switch {
    case errors.Is(err, store.ErrLeaseLost):
        lose(store.ErrLeaseLost)   // batalkan ctx handler, dengan cause
        return
    case err != nil && ctx.Err() == nil:
        log.Warn("heartbeat failed", "err", err)   // error sementara: coba lagi tick berikutnya
    }
}
```

- `HEARTBEAT_INTERVAL` (10 s) harus jauh lebih kecil dari `LEASE_DURATION` (30 s), supaya satu-dua heartbeat yang lambat tidak langsung menghilangkan lease. `config.Load` menolak konfigurasi dengan `HEARTBEAT_INTERVAL >= LEASE_DURATION`.
- Error jaringan sementara **tidak** membatalkan job. Kalau gangguannya berlangsung melewati lease, reaper akan mengambil job itu, dan heartbeat berikutnya mendapat `ErrLeaseLost`.

### Store: `(*Store).Heartbeat` di [`internal/store/store.go`](../../internal/store/store.go)

```go
type Lease struct {
    JobID    int64
    WorkerID string
    Run      int
    Attempt  int
}

func LeaseOf(j *job.Job, workerID string) Lease { ... } // diambil dari hasil Claim

const fenced = `id = $1 AND status = 'running' AND locked_by = $2 AND run = $3 AND attempts = $4`
```

```sql
UPDATE jobs SET locked_until = now() + $5 * interval '1 millisecond'
WHERE id = $1 AND status = 'running' AND locked_by = $2 AND run = $3 AND attempts = $4
```

Klausa `fenced` ditulis sekali dan dipakai oleh keempat tulisan worker, jadi tidak mungkin salah satunya lupa memeriksa sebagian token.

```go
if tag.RowsAffected() == 0 {
    return ErrLeaseLost
}
```

`0 rows affected` berarti salah satu dari: job sudah bukan `running` (reaper memindahkannya ke `retrying`), `locked_by` sudah orang lain (worker lain meng-claim ulang), atau `attempts`/`run` sudah naik (job di-claim ulang, **bisa jadi oleh worker yang sama**). Semuanya artinya sama: **claim kamu sudah bukan claim yang berlaku**.

### Fencing di semua tulisan worker

| Fungsi | Klausa fence | Kalau 0 baris |
|---|---|---|
| `Heartbeat` | `fenced` (`id`, `status='running'`, `locked_by`, `run`, `attempts`) | `ErrLeaseLost` |
| `Complete` | sama | `ErrLeaseLost` |
| `Fail` | sama (di dalam transaksi) | `ErrLeaseLost`, dan `job_errors` tidak ditulis |
| `Release` | sama | `ErrLeaseLost` |

### Kenapa `locked_by` saja tidak cukup?

Versi awal ForgeQ hanya memakai `locked_by` sebagai token. Itu cukup untuk membedakan worker yang **berbeda**, tetapi tidak untuk membedakan dua eksekusi di worker yang **sama**:

1. W1 (concurrency 10) claim job #7: eksekusi A, `attempts=1`.
2. W1 macet lebih dari 30 s. Reaper memindahkan #7 ke `retrying`.
3. W1 pulih. Fetch loop-nya masih punya slot kosong, jadi W1 claim #7 lagi: eksekusi B, `attempts=2`, `locked_by` **tetap W1**.
4. Heartbeat A berjalan dengan `WHERE locked_by='W1'`: cocok, sehingga A **tidak dibatalkan**. Kalau A lalu gagal, `Fail`-nya juga lolos: job kembali ke `retrying` padahal B masih berjalan, dan `job_errors` mencatat error A sebagai attempt 2.

Dengan `run` dan `attempts` di dalam fence, tulisan A memakai `attempts=1` sementara baris job sudah `attempts=2`, jadi cocok 0 baris dan menghasilkan `ErrLeaseLost`. Skenario ini diuji oleh `TestSameWorkerReclaimCancelsStaleRun`. Setiap bagian token diuji terpisah oleh `TestWritesNeedTheWholeLease`.

**Apakah `Release` (`attempts - 1`) bisa membuat token lama berlaku lagi?** Tidak. Misalkan eksekusi zombie Z memegang `attempts = z`, lalu reaper mengambil job-nya (attempts tetap `z`, status `retrying`). Sejak saat itu:
- `Claim` menaikkan attempts menjadi `z+1` atau lebih.
- `Release` hanya bisa dilakukan oleh claim yang **sedang berlaku** (karena di-fence juga), dan ia hanya membatalkan `+1` miliknya sendiri. Nilainya kembali ke angka sebelum claim itu (`≥ z`), dan statusnya `pending`, bukan `running`.
- `RetryDead` me-reset attempts ke 0, tetapi menaikkan `run`, jadi pasangan `(run, attempts)` tetap berbeda.

Jadi setiap kali job berstatus `running`, attempts-nya selalu `> z`, dan token Z tidak akan pernah cocok lagi. Pasangan `(run, attempts)` boleh saja berulang (misalnya setelah `Release`), tetapi tidak pernah untuk claim yang masih hidup. Untuk fencing, hanya itu yang penting.

### Lease hilang → handler dibatalkan: `(*Worker).process`

```go
leaseCtx, loseLease := context.WithCancelCause(base)
hbCtx, stopHeartbeat := context.WithCancel(leaseCtx)
hbDone := make(chan struct{})
go func() {
    defer close(hbDone)
    w.heartbeat(hbCtx, j, loseLease, log)
}()

err := w.execute(leaseCtx, j)   // handler melihat leaseCtx
stopHeartbeat()
<-hbDone                        // (1) heartbeat PASTI berhenti sebelum menulis hasil

switch cause := context.Cause(leaseCtx); {
case errors.Is(cause, store.ErrLeaseLost):
    log.Warn("lease lost while running; result discarded", ...)
    outcome = OutcomeLeaseLost  // (2) TIDAK menulis apa pun ke DB
...
```

1. **Heartbeat dihentikan dan ditunggu sebelum `Complete`/`Fail`.** Kalau tidak, heartbeat bisa berjalan bersamaan dengan `Complete`. Hasilnya aman berkat fence, tetapi sulit dinalar. Urutan ini dicatat sebagai invariant di `CLAUDE.md`.
2. **Setelah lease hilang, worker tidak menulis apa pun.** Tulisan itu toh akan ditolak fence, jadi tidak perlu dicoba. Handler juga menerima sinyal lewat `context.Cause(ctx) == store.ErrLeaseLost`, sehingga handler yang baik bisa berhenti lebih awal.

### Siapa yang mencabut lease?

Reaper (file [09](09-reaper-retensi.md)) memindahkan job `running` dengan `locked_until < now()` ke `retrying`. Alurnya diuji end-to-end di `TestReaperTakeoverCancelsStaleWorker`:

```mermaid
sequenceDiagram
    participant W1 as worker w1
    participant DB as PostgreSQL
    participant R as reaper
    participant W2 as worker w2
    W1->>DB: Claim (locked_by=w1)
    Note over W1: macet (GC pause / jaringan)
    R->>DB: Reap: locked_until < now() → retrying
    W2->>DB: Claim (locked_by=w2, attempts=2)
    W1->>DB: Heartbeat WHERE locked_by=w1 AND attempts=1
    DB-->>W1: 0 rows → ErrLeaseLost
    Note over W1: ctx dibatalkan, tidak menulis apa pun
    W2->>DB: Complete WHERE locked_by=w2 AND attempts=2 → OK
```

## Apa yang terjadi kalau fitur ini dihapus?

- **Tanpa lease** (tanpa `locked_until`): job milik worker yang crash tidak pernah kembali. Butuh operator untuk mereset manual.
- **Tanpa heartbeat**: setiap job yang lebih lama dari `LEASE_DURATION` dianggap mati. Reaper menyerahkannya ke worker lain padahal yang pertama masih bekerja, sehingga eksekusi ganda terjadi **terus-menerus**, bukan hanya saat crash. Kalau lease dibuat sangat panjang sebagai gantinya, crash baru ketahuan setelah lama sekali.
- **Tanpa fencing**: worker zombie menimpa hasil pemilik baru. Status job bisa mundur dari `succeeded` ke `retrying`, `job_errors` mendapat entri palsu, dan `attempts` jadi kacau.

## Trade-off dan alternatif

| Aspek | Pilihan ForgeQ | Biaya / alternatif |
|---|---|---|
| Panjang lease | 30 s + heartbeat 10 s | Crash terdeteksi 30 s + `REAPER_INTERVAL` (15 s) kemudian. Lease lebih pendek berarti deteksi lebih cepat tetapi lebih rentan terhadap gangguan jaringan singkat. |
| Biaya heartbeat | 1 `UPDATE` per job berjalan per 10 s | 1.000 job berjalan berarti ~100 UPDATE/s. Alternatif: satu heartbeat per *worker* (`UPDATE ... WHERE locked_by = $me`) untuk semua job sekaligus. |
| Token fence | `(locked_by, run, attempts)` dari hasil claim | Kleppmann menyarankan token yang **berubah setiap claim**. ForgeQ memakai ulang kolom yang sudah ada, karena `attempts` naik di setiap claim dan `run` di setiap retry manual, sehingga tidak perlu migrasi. Alternatifnya adalah kolom `lease_token BIGINT` yang hanya pernah naik. Itu lebih "murni" (tidak perlu argumen soal `Release` di atas), tetapi menambah kolom yang fungsinya sama. |
| Jam | `now()` database untuk `locked_until` dan pengecekan reaper | Semua perbandingan lease memakai satu jam, sehingga clock skew antar-worker tidak berpengaruh ([13](13-clock-skew.md)). |
| Batas perlindungan | Fence melindungi **state di database** | Fence **tidak** melindungi dunia luar. Email yang sudah terkirim oleh zombie tetap terkirim. Itulah sebabnya ForgeQ *at-least-once* ([06](06-at-least-once-idempotency.md)). |

Alternatif desain lain: memegang transaksi terbuka selama job berjalan (lock baris otomatis lepas saat koneksi putus). Pendekatan ini sederhana, tetapi memegang satu koneksi per job, dan transaksi panjang mengganggu vacuum.

## Eksperimen

Semua eksperimen di bawah dijalankan di branch sementara: `git switch -c coba/fencing`.

### Eksperimen A: heartbeat tanpa fence

Di `(*Store).Heartbeat`, ganti `fenced` dengan klausa yang hanya memeriksa `id` dan `status`. `$2`–`$4` harus tetap dipakai, karena PostgreSQL menolak parameter yang tidak direferensikan:

```diff
-		WHERE `+fenced, l.args(d.Milliseconds())...)
+		WHERE id = $1 AND status = 'running' AND $2::text IS NOT NULL AND $3::int IS NOT NULL AND $4::int IS NOT NULL`, l.args(d.Milliseconds())...)
```

```sh
go test -tags integration -count=1 -v -run '^(TestHeartbeatIsFenced|TestWorkerKeepsLeaseAliveAndStopsWhenTakenOver|TestReaperTakeoverCancelsStaleWorker)$' ./test/integration/
```

**Hasil yang diharapkan:**

```
store_test.go:379: non-owner heartbeat: err = <nil>, want ErrLeaseLost
--- FAIL: TestHeartbeatIsFenced
worker_test.go:141: stale worker's job was not cancelled
--- FAIL: TestWorkerKeepsLeaseAliveAndStopsWhenTakenOver
reaper_test.go:141: w1 handler was not cancelled after the takeover
--- FAIL: TestReaperTakeoverCancelsStaleWorker
```

Worker zombie terus memperpanjang lease milik worker lain, dan ia tidak pernah tahu bahwa job-nya sudah diambil alih.

### Eksperimen B: tanpa heartbeat sama sekali

Di `(*Worker).process`, ganti pemanggilan heartbeat supaya goroutine-nya hanya menunggu:

```diff
-		w.heartbeat(hbCtx, j, loseLease, log)
+		<-hbCtx.Done()
```

```sh
go test -count=1 -v -run '^(TestWorkerHeartbeatsLongJobs|TestWorkerCancelsJobOnLeaseLoss)$' ./internal/worker/
go test -tags integration -count=1 -v -run '^TestWorkerKeepsLeaseAliveAndStopsWhenTakenOver$' ./test/integration/
```

**Hasil yang diharapkan:**

```
worker_test.go:234: handler ctx cause = context deadline exceeded, want ErrLeaseLost
worker_test.go:243: worker reported a result after losing the lease: completed=[] failures=[{1 test-worker job timed out after 1s ...}]
--- FAIL: TestWorkerCancelsJobOnLeaseLoss
worker_test.go:258: heartbeats = 0, want several during a long job
--- FAIL: TestWorkerHeartbeatsLongJobs
worker_test.go:127: heartbeat did not keep the lease: &{... LockedUntil:<1 detik setelah claim> ...}
--- FAIL: TestWorkerKeepsLeaseAliveAndStopsWhenTakenOver
```

Test integrasi itu menjalankan job dengan lease 1 detik selama 3 detik. Tanpa heartbeat, `locked_until` sudah lewat, sehingga reaper mana pun berhak mengambil job ini padahal worker-nya sehat.

### Eksperimen C: `Complete` tanpa fence

Di `(*Store).Complete`, lakukan penggantian yang sama:

```diff
-		WHERE `+fenced, l.args()...)
+		WHERE id = $1 AND status = 'running' AND $2::text IS NOT NULL AND $3::int IS NOT NULL AND $4::int IS NOT NULL`, l.args()...)
```

```sh
go test -tags integration -count=1 -v -run '^TestCompleteIsFenced$' ./test/integration/
```

**Hasil yang diharapkan:**

```
store_test.go:175: Complete by non-owner: err = <nil>, want ErrLeaseLost
store_test.go:181: Complete by owner: job lease lost
--- FAIL: TestCompleteIsFenced
```

Perhatikan baris kedua: "intruder" berhasil menyelesaikan job, sehingga **pemilik sah** malah ditolak.

### Eksperimen D: fence hanya dengan `locked_by` (versi lama)

Kembalikan fence ke versi awal ForgeQ, yang tidak memeriksa `run` dan `attempts`:

```diff
-const fenced = `id = $1 AND status = 'running' AND locked_by = $2 AND run = $3 AND attempts = $4`
+const fenced = `id = $1 AND status = 'running' AND locked_by = $2 AND $3::int IS NOT NULL AND $4::int IS NOT NULL`
```

```sh
go test -tags integration -count=1 -v -run '^(TestWritesNeedTheWholeLease|TestSameWorkerReclaimCancelsStaleRun)$' ./test/integration/
```

**Hasil yang diharapkan** (dipersingkat; tiga baris `store_test.go` muncul sekali untuk setiap subtest yang gagal):

```
reaper_test.go:224: stale run was not cancelled: its heartbeat was accepted for the new claim
--- FAIL: TestSameWorkerReclaimCancelsStaleRun
store_test.go:226: Heartbeat: err = <nil>, want ErrLeaseLost
store_test.go:229: Release: err = <nil>, want ErrLeaseLost
store_test.go:238: Complete by the real owner afterwards: job lease lost
--- FAIL: TestWritesNeedTheWholeLease
    --- FAIL: TestWritesNeedTheWholeLease/older_attempt
    --- FAIL: TestWritesNeedTheWholeLease/newer_attempt
    --- FAIL: TestWritesNeedTheWholeLease/other_run
```

Subtest `other_worker` tetap lolos, karena `locked_by` masih diperiksa. Tiga subtest lainnya menunjukkan bahwa claim lain dari worker yang sama bisa memperpanjang lease dan me-`Release` job milik claim yang berlaku, sehingga pemilik sah ditolak. `Fail` dan `Complete` palsu tidak muncul sebagai error hanya karena `Release` dipanggil lebih dulu dan sudah mengubah status ke `pending`.

Bersihkan: `git restore . && git switch main && git branch -D coba/fencing`.

## Latihan interview

1. Apa beda *lease* dengan *lock* biasa? Kenapa job queue terdistribusi lebih suka lease?
2. Kenapa `HEARTBEAT_INTERVAL` harus jauh lebih kecil dari `LEASE_DURATION`? Berapa rasio yang masuk akal?
3. Jelaskan skenario "worker zombie" dan bagaimana fencing mencegah kerusakan data.
4. Fencing sudah ada. Kenapa ForgeQ tetap tidak bisa menjamin *exactly-once*?
5. Kenapa heartbeat dihentikan dan **ditunggu** (`<-hbDone`) sebelum memanggil `Complete`?
6. Kenapa ID worker saja bukan fencing token yang cukup? Apa yang ForgeQ pakai sebagai gantinya, dan kenapa `Release` yang mengurangi `attempts` tidak merusaknya?

### Jawaban

1. Lock biasa dipegang sampai dilepas secara eksplisit, atau sampai koneksi putus. Kalau pemegangnya mati tanpa memutus koneksi (hang), lock tertahan selamanya. Lease **kedaluwarsa sendiri** setelah waktu tertentu, sehingga sistem pulih tanpa intervensi. Di sistem terdistribusi kita tidak bisa membedakan "mati" dari "lambat", dan lease mengubah pertanyaan itu menjadi batas waktu yang eksplisit.
2. Supaya beberapa heartbeat boleh gagal atau lambat tanpa kehilangan lease. Dengan 30 s / 10 s, dua heartbeat berturut-turut boleh gagal. Rasio 1:3 adalah aturan praktis yang umum. Rasio yang terlalu rapat (misalnya 1:1,2) membuat jitter jaringan biasa memicu pengambilalihan palsu.
3. Worker A macet melewati lease-nya. Reaper memindahkan job ke `retrying`, dan worker B meng-claim-nya dengan `locked_by = B`. A bangun dan mencoba `Complete`/`Fail`/`Heartbeat`. Semua tulisan A menyertakan lease A (`locked_by = 'A' AND attempts = 1`), sehingga cocok 0 baris dan menghasilkan `ErrLeaseLost`. Status yang ditulis B tidak tersentuh. Di sisi worker, ctx handler A dibatalkan dengan cause `ErrLeaseLost` dan A tidak menulis apa pun lagi.
4. Fencing hanya menjaga tulisan ke **database ForgeQ**. Efek samping ke sistem luar (mengirim email, menagih kartu) terjadi **di dalam handler**, sebelum `Complete`. Kalau A sudah mengirim email lalu macet, B akan mengirimnya lagi. Fence tidak bisa menarik kembali email yang sudah terkirim. Solusinya adalah handler yang idempoten, misalnya dengan mengirim idempotency key ke penyedia email.
5. Supaya tidak ada dua goroutine yang menulis ke baris job yang sama secara bersamaan. Kalau heartbeat masih jalan, `Heartbeat` bisa saja dieksekusi **setelah** `Complete`. Hasilnya aman karena fence (status sudah bukan `running`, jadi 0 baris), tetapi heartbeat akan melaporkan "lease lost" palsu dan log menjadi menyesatkan. Menunggu `hbDone` membuat urutannya deterministik: heartbeat selesai, baru hasil ditulis.
6. Token harus membedakan setiap **claim**, bukan setiap worker. Worker yang macet bisa meng-claim ulang job yang baru saja hilang darinya, karena ia masih punya slot kosong. Kedua eksekusi lalu sama-sama `locked_by = dia`, sehingga heartbeat eksekusi lama memperpanjang lease eksekusi baru dan tidak pernah dibatalkan. ForgeQ memakai `(locked_by, run, attempts)` dari hasil claim, karena `attempts` naik di setiap claim dan `run` di setiap retry manual. `Release` hanya bisa dilakukan oleh claim yang sedang berlaku dan hanya membatalkan `+1` miliknya sendiri. Jadi setelah sebuah claim diambil reaper, `attempts` di baris `running` selalu lebih besar dari milik claim lama, dan token lama tidak pernah cocok lagi.
