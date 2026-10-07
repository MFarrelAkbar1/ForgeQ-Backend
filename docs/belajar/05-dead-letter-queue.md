# 05 · Dead-letter queue

## Masalahnya apa?

Dua job gagal:

- **Job A** `send_email` gagal 5 kali karena SMTP down sepanjang malam. Besok paginya SMTP sudah pulih.
- **Job B** `image_resize` dengan payload `{"url": ""}`. Ini tidak akan pernah berhasil, berapa kali pun dicoba.

Pertanyaannya:
1. Setelah attempt habis, job **tidak boleh hilang**. Seseorang harus bisa melihatnya.
2. Operator perlu tahu **kenapa** job gagal, di setiap percobaan, bukan hanya error terakhir.
3. Operator perlu bisa **menjalankan ulang** job A setelah penyebabnya diperbaiki, atau **membuang** job yang tidak relevan lagi.
4. Job B sebaiknya **tidak membuang 5 attempt** (dan waktu backoff) untuk sesuatu yang pasti gagal.

## Konsep umumnya

**Dead-letter queue (DLQ)**, istilah dari dunia messaging (RabbitMQ, SQS), adalah tempat pesan yang tidak bisa diproses. DLQ adalah "tempat parkir" yang menunggu tindakan manusia. Prinsipnya:

- Job yang menyerah dipindahkan dari antrean aktif, tetapi **tidak dihapus**.
- Riwayat kegagalan disimpan.
- Ada operasi **redrive** (jalankan ulang) dan **purge** (hapus).
- **Error permanen** (validasi, payload rusak) langsung masuk DLQ tanpa retry.

## Implementasi di ForgeQ

### DLQ = status `dead` di tabel yang sama

Tidak ada tabel terpisah. Job dead adalah baris `jobs` dengan `status = 'dead'`. Keputusannya diambil di SQL `(*Store).Fail` ([`internal/store/store.go`](../../internal/store/store.go)):

```sql
SET status = CASE WHEN $4 OR attempts >= max_attempts THEN 'dead' ELSE 'retrying' END,
    finished_at = CASE WHEN $4 OR attempts >= max_attempts THEN now() END,
```

`$4` adalah `Failure.Permanent`. Reaper ([09](09-reaper-retensi.md)) juga bisa menghasilkan `dead` kalau lease kedaluwarsa pada attempt terakhir.

### Error permanen: `worker.Permanent`

Di [`internal/worker/registry.go`](../../internal/worker/registry.go):

```go
type permanentError struct{ err error }
func (e *permanentError) Unwrap() error { return e.err }   // errors.Is tetap tembus

func Permanent(err error) error { ... return &permanentError{err: err} }
func IsPermanent(err error) bool {
    _, ok := errors.AsType[*permanentError](err)
    return ok
}
```

Handler memakainya untuk kesalahan yang tidak akan sembuh. Contoh di [`internal/handlers/handlers.go`](../../internal/handlers/handlers.go):

```go
if p.URL == "" || p.Width <= 0 || p.Height <= 0 {
    return worker.Permanent(errors.New("payload needs url and positive width/height"))
}
```

`decode` membungkus error JSON dengan `Permanent`, dan `(*Worker).execute` juga mengembalikan `Permanent` untuk **tipe job yang tidak terdaftar**: menunggu tidak akan membuat handler-nya muncul.

### Riwayat: tabel `job_errors` dengan `(run, attempt)`

Setiap kegagalan menulis satu baris di transaksi yang sama dengan `UPDATE` status (di `Fail`) atau di statement yang sama (di `reapQuery`). Kolom `run` ditambahkan di [`migrations/00003_add_run.sql`](../../migrations/00003_add_run.sql). Komentar migrasi itu menjelaskan alasannya: supaya riwayat terbaca "run 2, attempt 1", bukan "attempt 1" dua kali.

`(*Store).Errors` mengembalikan riwayat ini, dan `GET /api/v1/jobs/{id}` menyertakannya dalam field `errors`.

### Redrive: `(*Store).RetryDead`

```sql
UPDATE jobs
SET status = 'pending', run = run + 1, attempts = 0, run_at = now(), last_error = NULL,
    started_at = NULL, finished_at = NULL, updated_at = now()
WHERE id = $1 AND status = 'dead'
RETURNING ...
```

- **`AND status = 'dead'`**: guard status. Me-retry job yang sedang `running` akan mengacaukan lease worker lain, jadi ditolak.
- **`run = run + 1, attempts = 0`**: jatah baru di putaran baru. Riwayat lama tetap ada.
- Setelah commit, `s.notify(j.Queue)` membangunkan worker.

### 404 atau 409? `notFoundOrInvalid`

Kalau `UPDATE ... WHERE id = $1 AND status = 'dead'` cocok 0 baris, ada dua kemungkinan: job tidak ada, atau job ada tetapi statusnya bukan `dead`. Fungsi ini membedakannya dengan satu query `SELECT EXISTS`:

```go
func (s *Store) notFoundOrInvalid(ctx context.Context, id int64) error {
    ... SELECT EXISTS (SELECT 1 FROM jobs WHERE id = $1) ...
    if !exists { return ErrNotFound }    // → 404
    return ErrInvalidState               // → 409
}
```

Perhatikan urutannya: **tulis dulu dengan guard, baru jelaskan kalau gagal**. Kebalikannya ("cek status dulu, baru update") punya race: status bisa berubah di antara cek dan update.

### Purge: `(*Store).Delete`

`DELETE ... WHERE id = $1 AND status IN ('dead', 'succeeded')`. Job yang mungkin masih berjalan (`pending`, `running`, `retrying`) tidak bisa dihapus, jadi worker tidak pernah kehilangan job di tengah eksekusi. `job_errors` ikut terhapus lewat `ON DELETE CASCADE`.

### Antarmuka

- API: `POST /api/v1/jobs/{id}/retry`, `DELETE /api/v1/jobs/{id}` (`internal/api/api.go`, `(*server).retry`, `(*server).delete`).
- Dashboard: tab **dead** adalah tab default (`listParams` di `internal/dashboard/dashboard.go`), dengan tombol Retry/Delete (`(*dashboard).retry`, `(*dashboard).delete`).

## Apa yang terjadi kalau fitur ini dihapus?

- **Tanpa status dead** (job gagal langsung dihapus): kegagalan tidak terlihat. Pelanggan tidak menerima email dan tidak ada yang tahu.
- **Tanpa `max_attempts`** (retry selamanya): job B akan mencoba setiap jam selamanya, membuang kapasitas worker dan menutupi masalah sebenarnya.
- **Tanpa `Permanent`**: job dengan payload rusak menghabiskan semua attempt plus total waktu backoff sebelum sampai di DLQ.
- **Tanpa guard `status = 'dead'` di retry**: operator yang menekan Retry dua kali, atau pada job yang sudah berjalan, akan menaikkan `run` lagi dan me-reset `attempts` job yang sedang dipegang worker.
- **Tanpa `run`**: riwayat menjadi ambigu.

## Trade-off dan alternatif

| Pilihan ForgeQ | Untung | Rugi / alternatif |
|---|---|---|
| Satu tabel, `status = 'dead'` | Retry cukup satu `UPDATE`; satu skema; query dashboard sederhana | Job dead berbagi tabel dan indeks dengan job aktif. Alternatif: tabel `dead_jobs` terpisah (retry = pindah baris antar tabel dalam transaksi). |
| Dead tidak pernah dihapus otomatis | Tidak ada kegagalan yang hilang diam-diam | Tabel bisa tumbuh kalau tidak ada yang membersihkan. Retensi hanya menghapus `succeeded`. |
| Error permanen ditentukan handler | Handler paling tahu mana yang bisa sembuh | Kesalahan klasifikasi bisa terjadi: error sementara yang dibungkus Permanent langsung dead. |
| Retry manual me-reset attempts | Operator mendapat jatah penuh lagi | Retry massal ("retry semua dead") belum ada; harus satu per satu. |

## Eksperimen

Branch: `git switch -c coba/dlq`.

### Eksperimen A: retry tanpa guard status

Di `(*Store).RetryDead`:

```diff
-		WHERE id = $1 AND status = 'dead'
+		WHERE id = $1
```

```sh
go test -tags integration -count=1 -v -run '^TestRetryDeadKeepsHistory$' ./test/integration/
```

**Hasil yang diharapkan:**

```
store_test.go:354: retry of pending job: err = <nil>, want ErrInvalidState
store_test.go:380: history[1] = run 3 attempt 1 "boom again", want run 2 attempt 1 "boom again"
--- FAIL: TestRetryDeadKeepsHistory
```

Retry kedua (pada job yang sudah `pending`) diterima dan menaikkan `run` menjadi 3. Riwayat jadi melompat dari run 1 ke run 3.

### Eksperimen B: `run` tidak dinaikkan

```diff
-		SET status = 'pending', run = run + 1, attempts = 0, ...
+		SET status = 'pending', run = run, attempts = 0, ...
```

**Hasil yang diharapkan:**

```
store_test.go:351: retried job = &{... Status:pending Run:1 Attempts:0 ...}, want pending on run 2 with attempts reset
store_test.go:380: history[1] = run 1 attempt 1 "boom again", want run 2 attempt 1 "boom again"
--- FAIL: TestRetryDeadKeepsHistory
```

Dua entri riwayat sama-sama "run 1, attempt 1", sehingga tidak bisa dibedakan.

### Eksperimen C: abaikan `Permanent`

Di `(*Worker).finish`:

```diff
-		Permanent: IsPermanent(jobErr),
+		Permanent: false,
```

```sh
go test -count=1 -v -run '^TestWorkerOutcomes$' ./internal/worker/
go test -tags integration -count=1 -v -run '^TestWorkerEndToEnd$' ./test/integration/
```

**Hasil yang diharapkan:**

```
worker_test.go:166: job 3: Permanent = false, want true
worker_test.go:166: job 6: Permanent = false, want true
--- FAIL: TestWorkerOutcomes
worker_test.go:58: timed out waiting for all jobs to finish
--- FAIL: TestWorkerEndToEnd (15.08s)
```

Job 6 adalah tipe yang tidak terdaftar. Di test integrasi, job `broken` masuk `retrying` dengan jeda 1 menit, bukan `dead`, sehingga test menunggu sampai timeout 15 detik.

Bersihkan: `git restore . && git switch main && git branch -D coba/dlq`.

## Latihan interview

1. Apa itu dead-letter queue, dan kenapa job yang gagal tidak langsung dihapus saja?
2. Bagaimana ForgeQ membedakan error yang layak di-retry dari yang tidak?
3. Kenapa `RetryDead` memakai `UPDATE ... WHERE status = 'dead'` lalu `notFoundOrInvalid`, bukan `SELECT status` dulu lalu `UPDATE`?
4. Apa fungsi kolom `run`?
5. Apa keuntungan dan kerugian menyimpan job dead di tabel yang sama dengan job aktif?

### Jawaban

1. DLQ adalah tempat job yang sudah menyerah (attempt habis atau error permanen), menunggu tindakan manusia. Kalau langsung dihapus, kegagalan jadi tak terlihat, tidak bisa diinvestigasi (riwayat error hilang), dan tidak bisa dijalankan ulang setelah penyebabnya diperbaiki.
2. Secara default, setiap error di-retry dengan backoff. Handler yang tahu errornya tidak akan sembuh (payload invalid, data tidak ada) membungkusnya dengan `worker.Permanent(err)`. `IsPermanent` (via `errors.AsType`) mendeteksinya, dan `Fail` langsung menulis `dead`. Tipe job yang tidak terdaftar dan panic juga ditangani: tipe tak terdaftar dianggap permanen, sedangkan panic dianggap kegagalan biasa yang di-retry.
3. Pola cek-lalu-tulis punya race: di antara `SELECT` dan `UPDATE`, status bisa berubah (misalnya dua operator menekan Retry bersamaan, sehingga keduanya melihat `dead` dan keduanya menaikkan `run`). Dengan guard di `WHERE`, operasinya atomik: tepat satu yang menang, dan yang kalah mendapat 0 baris. Query `EXISTS` sesudahnya hanya untuk menjelaskan kegagalan (404 vs 409), bukan untuk mengambil keputusan.
4. `run` menghitung putaran retry manual (mulai dari 1, naik setiap `RetryDead`). Setiap baris `job_errors` menyimpan `(run, attempt)`, sehingga riwayat tetap unik dan terbaca setelah `attempts` di-reset.
5. Untungnya: retry cukup satu `UPDATE` atomik, hanya ada satu skema, dan listing/statistik cukup dengan `GROUP BY status`. Ruginya: job dead ikut membebani tabel dan indeks yang dipakai jalur panas. Indeks claim bersifat parsial (`pending`/`retrying`), sehingga dampaknya kecil. Juga tidak ada retensi otomatis untuk dead.
