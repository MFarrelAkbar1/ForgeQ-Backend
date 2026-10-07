# 09 · Reaper dan retensi

## Masalahnya apa?

**Job yatim.** Worker A meng-claim 10 job, lalu node-nya mati listrik. Lease (file [03](03-lease-heartbeat-fencing.md)) menandai kapan klaim A habis, tetapi **siapa** yang mengecek "sudah lewat" lalu mengembalikan job-job itu ke antrean? Tanpa itu, 10 job `running` selamanya.

**Tabel yang terus membengkak.** Sistem memproses 1 juta job per hari. Setelah setahun ada 365 juta baris `succeeded`. Query dashboard melambat, backup membesar, dan vacuum bekerja keras.

Ditambah satu batasan desain: ForgeQ tidak punya proses "master". Semua worker setara, dan jumlahnya bisa 1 atau 20.

## Konsep umumnya

- **Reaper (sweeper):** pekerjaan latar berkala yang mencari lease yang sudah kedaluwarsa dan memulihkan job-nya: kembali ke antrean, atau ke DLQ kalau attempt sudah habis.
- **Retensi:** kebijakan berapa lama data disimpan. Data lama dihapus **dalam batch kecil** supaya transaksi dan lock-nya pendek.
- **Siapa yang menjalankan?** Pilihannya: (a) satu *leader* (lewat leader election atau advisory lock), atau (b) **semua** proses, dengan query yang aman dijalankan bersamaan. ForgeQ memilih (b).

## Implementasi di ForgeQ

### Reaper: `worker.Reaper` di [`internal/worker/reaper.go`](../../internal/worker/reaper.go)

Dijalankan oleh setiap proses worker (`go reaper.Run(ctx)` di `cmd/worker/main.go`), setiap `REAPER_INTERVAL` (15 s):

```go
func (r *Reaper) RunOnce(ctx context.Context) {
    r.reapExpired(ctx)
    r.deleteOldSucceeded(ctx)
}
```

`reapExpired` memanggil `store.Reap(ctx, 1000)` berulang kali selama hasilnya penuh (1000 baris). Setiap job yang diambil kembali di-log dengan `stale_worker_id` dan dihitung di metrik `forgeq_jobs_reaped_total{status}`.

### Satu statement atomik: `reapQuery` di [`internal/store/store.go`](../../internal/store/store.go)

```sql
WITH expired AS (
    SELECT id, locked_by FROM jobs
    WHERE status = 'running' AND locked_until < now()
    ORDER BY locked_until
    LIMIT $1
    FOR UPDATE SKIP LOCKED                       -- (1)
), reaped AS (
    UPDATE jobs j
    SET status       = CASE WHEN j.attempts >= j.max_attempts THEN 'dead' ELSE 'retrying' END,  -- (2)
        finished_at  = CASE WHEN j.attempts >= j.max_attempts THEN now() END,
        run_at       = now(),                    -- (3)
        last_error   = $2,                       -- LeaseExpiredError
        locked_by    = NULL,
        locked_until = NULL,
        updated_at   = now()
    FROM expired
    WHERE j.id = expired.id
    RETURNING j.id, j.queue, j.status, j.run, j.attempts, expired.locked_by
), logged AS (
    INSERT INTO job_errors (job_id, run, attempt, error, worker_id)  -- (4)
    SELECT id, run, attempts, $2, locked_by FROM reaped
)
SELECT id, queue, status, attempts, locked_by FROM reaped
```

1. **`FOR UPDATE SKIP LOCKED`**: kalau dua reaper berjalan bersamaan, masing-masing mengambil baris yang berbeda. Ini juga melewati baris yang sedang di-update worker (misalnya heartbeat yang sedang berlangsung). Komentar di kode: *"Row locking re-checks locked_until, so a heartbeat that lands first wins."* Setelah lock didapat, `locked_until < now()` dievaluasi ulang, sehingga heartbeat yang baru saja memperpanjang lease membuat baris itu tidak lagi cocok.
2. **`attempts >= max_attempts`**: attempt dihitung saat claim, jadi crash pada attempt terakhir langsung mengirim job ke `dead`. Poison job yang selalu membuat worker crash tidak bisa berputar selamanya.
3. **`run_at = now()`**: job yang di-reap langsung bisa diambil, tanpa backoff (keterbatasan yang tercatat di README).
4. **Riwayat ditulis di statement yang sama**, dengan pesan `store.LeaseExpiredError` (`"lease expired: worker crashed or stalled"`) dan `worker_id` worker yang mati, sehingga operator tahu siapa yang bermasalah.

Setelah `Reap`, untuk setiap job yang menjadi `retrying` dipanggil `s.notify(queue)` supaya worker lain segera bangun.

Indeks yang mendukung: `jobs_lease_idx ON jobs (locked_until) WHERE status = 'running'`.

### Retensi: `deleteOldSucceeded` → `(*Store).DeleteSucceededBefore`

```go
cutoff := r.now().Add(-r.cfg.Retention)   // JOB_RETENTION, default 7d
```

```sql
DELETE FROM jobs WHERE id IN (
    SELECT id FROM jobs
    WHERE status = 'succeeded' AND finished_at < $1
    ORDER BY finished_at
    LIMIT $2                 -- 1000 per batch
    FOR UPDATE SKIP LOCKED
)
```

- **Hanya `succeeded`.** Job `dead` tidak pernah dihapus otomatis karena butuh perhatian manusia ([05](05-dead-letter-queue.md)). Job aktif jelas tidak boleh dihapus.
- **Batch 1000, diulang selama penuh.** Satu `DELETE` raksasa memegang lock dan membuat WAL besar dalam satu transaksi. Batch kecil membuat setiap transaksi singkat.
- `job_errors` ikut terhapus lewat `ON DELETE CASCADE`.
- Indeks khusus `jobs_succeeded_finished_idx ON jobs (finished_at) WHERE status = 'succeeded'` dibuat di [`migrations/00002_index_succeeded_finished_at.sql`](../../migrations/00002_index_succeeded_finished_at.sql) dengan `CREATE INDEX CONCURRENTLY`, supaya pembuatannya tidak mengunci tabel dari penulisan. Karena itu migrasi ini memakai `-- +goose NO TRANSACTION`: `CONCURRENTLY` tidak bisa berjalan di dalam transaksi.
- `JOB_RETENTION` menerima akhiran `d` (`7d`) lewat `parseDuration` di `internal/config/config.go`.

### Kenapa tidak ada leader?

Semua query di atas memakai `SKIP LOCKED`, jadi 10 reaper yang berjalan bersamaan saling membagi pekerjaan alih-alih bertabrakan. Biayanya hanya beberapa query yang tidak menemukan apa-apa. Hasilnya: tidak ada leader election, tidak ada titik kegagalan tunggal, dan tidak ada konfigurasi "proses mana yang jadi reaper".

## Apa yang terjadi kalau fitur ini dihapus?

- **Tanpa reaper**: setiap crash meninggalkan job `running` selamanya. Inilah persis yang diuji `TestReaperRecoversExpiredLeases`.
- **Reaper yang mengabaikan `max_attempts`**: job yang selalu membuat worker crash akan diulang tanpa batas (lihat Eksperimen B).
- **Retensi tanpa filter status**: job `dead` yang sedang menunggu investigasi ikut terhapus diam-diam (lihat Eksperimen A).
- **Tanpa retensi**: tabel tumbuh tanpa batas. Query `GROUP BY` untuk dashboard dan metrik `forgeq_jobs` makin lambat, karena keduanya menghitung semua baris.

## Trade-off dan alternatif

| Pilihan | Untung | Rugi / alternatif |
|---|---|---|
| Reaper di setiap worker | Tanpa leader; tahan kegagalan | Query redundan; alternatifnya `pg_try_advisory_lock` supaya hanya satu yang bekerja |
| Deteksi = lease + interval reaper | Sederhana | Waktu deteksi terburuk ≈ `LEASE_DURATION` + `REAPER_INTERVAL` (30 + 15 s) |
| Retry tanpa backoff | Pemulihan cepat | Poison job yang membuat crash dicoba lagi seketika |
| `DELETE` per batch | Transaksi pendek | Churn tinggi menghasilkan *dead tuple* dan beban autovacuum. Alternatif untuk volume besar: **partisi tabel per waktu** dan `DROP` partisi lama (instan, tanpa dead tuple). |
| Dead tidak pernah dihapus | Aman | Perlu pembersihan manual atau kebijakan terpisah |

## Eksperimen

Branch: `git switch -c coba/reaper`.

### Eksperimen A: retensi tanpa filter status

Di `(*Store).DeleteSucceededBefore`:

```diff
-			WHERE status = 'succeeded' AND finished_at < $1
+			WHERE finished_at < $1
```

```sh
go test -tags integration -count=1 -v -run '^TestRetentionDeletesOnlyOldSucceeded$' ./test/integration/
```

**Hasil yang diharapkan:**

```
reaper_test.go:206: old dead: kept = false, want true
--- FAIL: TestRetentionDeletesOnlyOldSucceeded
```

Job `dead` berumur 30 hari ikut terhapus. Job `pending` lama selamat hanya karena `finished_at`-nya NULL. Jangan mengandalkan kebetulan seperti itu.

### Eksperimen B: reaper mengabaikan `max_attempts`

Di `reapQuery`:

```diff
-		SET status       = CASE WHEN j.attempts >= j.max_attempts THEN 'dead' ELSE 'retrying' END,
+		SET status       = 'retrying',
```

```sh
go test -tags integration -count=1 -v -run '^TestReaperRecoversExpiredLeases$' ./test/integration/
```

**Hasil yang diharapkan:**

```
reaper_test.go:60: job 1: status = retrying, want dead
reaper_test.go:77: reclaim = [1 2] (err <nil>), want job 2 on attempt 2
--- FAIL: TestReaperRecoversExpiredLeases
```

Job 1 (`max_attempts = 1`) seharusnya mati, tetapi malah di-claim lagi. Bayangkan job ini memicu OOM: ia akan membunuh worker satu per satu, selamanya.

Bersihkan: `git restore . && git switch main && git branch -D coba/reaper`.

## Latihan interview

1. Apa yang terjadi pada job `running` milik worker yang mati, langkah demi langkah?
2. Kenapa reaper bisa berjalan di semua worker tanpa leader election?
3. Ada race antara reaper dan heartbeat: lease hampir habis, dan keduanya berjalan bersamaan. Siapa yang menang, dan kenapa hasilnya tetap benar?
4. Kenapa retensi menghapus dalam batch 1000, dan kenapa indeksnya dibuat `CONCURRENTLY`?
5. Untuk 100 juta job per hari, apa yang akan kamu ubah dari strategi retensi ini?

### Jawaban

1. Heartbeat berhenti, sehingga `locked_until` tidak lagi diperpanjang. Setelah lewat, reaper berikutnya (di proses mana pun, maksimal `REAPER_INTERVAL` kemudian) menjalankan `reapQuery`: job dikunci dengan SKIP LOCKED, lalu menjadi `retrying` (atau `dead` kalau attempt habis), `run_at = now()`, dan kegagalan dicatat di `job_errors` atas nama worker yang mati. Notifier membangunkan worker lain, yang meng-claim job itu dengan `attempts + 1`.
2. Semua operasinya memakai `FOR UPDATE SKIP LOCKED` dalam satu statement atomik. Dua reaper tidak pernah memproses baris yang sama: yang satu mengunci, yang lain melewatinya. Kalau tidak ada yang kedaluwarsa, query hanya mengembalikan 0 baris. Harga redundansinya kecil, dan sebagai gantinya kita tidak butuh leader yang bisa gagal.
3. Keduanya meng-update baris yang sama, sehingga row lock menyerialkannya. Kalau heartbeat duluan, `locked_until` sudah diperpanjang, dan reaper yang mendapat lock setelahnya mengevaluasi ulang `locked_until < now()` lalu melewati baris itu. Kalau reaper duluan, status menjadi `retrying`, dan heartbeat (`WHERE status='running' AND locked_by=...`) cocok 0 baris, sehingga worker mendapat `ErrLeaseLost` dan berhenti. Dalam kedua kasus hanya ada satu pemilik.
4. Batch kecil membuat setiap transaksi `DELETE` singkat: lock sedikit, WAL per transaksi kecil, dan replikasi tidak tersendat. `CREATE INDEX CONCURRENTLY` membangun indeks tanpa memblokir INSERT/UPDATE pada tabel `jobs` yang sedang dipakai produksi. Konsekuensinya, statement itu tidak boleh berada di dalam transaksi, sehingga migrasinya ditandai `NO TRANSACTION`.
5. Partisi tabel `jobs` (atau tabel histori untuk job selesai) berdasarkan waktu, misalnya per hari, lalu hapus data lama dengan `DROP PARTITION`. Operasinya instan, tanpa dead tuple dan tanpa beban vacuum. Mungkin juga perlu memisahkan job yang sudah selesai dari tabel antrean panas, dan mengganti `count(*)` di dashboard dengan estimasi atau penghitung.
