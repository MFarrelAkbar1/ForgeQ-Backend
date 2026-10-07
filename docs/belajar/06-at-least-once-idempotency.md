# 06 · At-least-once dan idempotency

File ini membahas dua hal yang sering tertukar:

1. **At-least-once delivery**: jaminan dari *worker ke handler*. Sebuah job bisa **dijalankan** lebih dari sekali.
2. **Idempotent enqueue**: jaminan dari *klien ke API*. Request enqueue yang diulang tidak membuat job **ganda**.

Keduanya berasal dari fakta yang sama: di sistem terdistribusi, "berhasil, tapi jawabannya hilang" tidak bisa dibedakan dari "gagal".

## Masalahnya apa?

**Skenario 1: duplikat eksekusi.** Handler `charge_card` memanggil API Stripe dan berhasil, sehingga kartu ditagih. Sesaat sebelum `Complete` ditulis, container worker di-`SIGKILL`. Di database job masih `running`. Lease kedaluwarsa, reaper mengembalikannya ke antrean, worker lain menjalankannya lagi, dan **kartu ditagih dua kali**.

**Skenario 2: duplikat enqueue.** Aplikasi toko mengirim `POST /api/v1/jobs` untuk "kirim invoice pesanan #42". Jaringan timeout setelah 30 detik. Apakah job-nya sudah dibuat? Klien tidak tahu, jadi ia mengirim ulang. Kalau ternyata request pertama sudah sampai, sekarang ada **dua** job dan **dua** invoice.

## Konsep umumnya

Ada tiga jaminan pengiriman:

| Jaminan | Artinya | Konsekuensi |
|---|---|---|
| At-most-once | Maksimal sekali, bisa hilang | Ack sebelum mengerjakan. Crash berarti job hilang. |
| **At-least-once** | Minimal sekali, bisa dobel | Ack setelah mengerjakan. Crash berarti diulang. |
| Exactly-once | Tepat sekali | Untuk efek ke sistem **luar**, butuh kerja sama sistem itu (idempotency key atau transaksi bersama). Queue sendirian tidak bisa menjaminnya. |

Celahnya selalu ada di antara "efek samping terjadi" dan "kita mencatat bahwa ia terjadi". Kalau keduanya tidak berada dalam satu transaksi atomik (dan email/Stripe tidak ikut dalam transaksi PostgreSQL kita), salah satu dari dua hal ini pasti terjadi: kadang hilang, atau kadang dobel. ForgeQ memilih **dobel**, lalu menyerahkan deduplikasi ke handler.

**Idempotent** artinya menjalankan operasi N kali hasilnya sama dengan sekali. Cara membuatnya:
- Kunci unik di sistem tujuan: Stripe menerima header `Idempotency-Key`, dan penagihan kedua dengan kunci yang sama dikembalikan tanpa menagih ulang.
- Tabel "sudah diproses" yang ditulis dalam transaksi yang sama dengan efek lokalnya.
- Operasi yang secara alami idempoten: `SET status = 'paid'` (bukan `balance = balance - 100`), `PUT` file dengan nama tetap.

## Implementasi di ForgeQ

### At-least-once: dideklarasikan, dites, didokumentasikan

ForgeQ tidak berpura-pura. Komentar `HandlerFunc` di [`internal/worker/registry.go`](../../internal/worker/registry.go) menyatakannya:

```go
// Delivery is at-least-once: a job may run again after a crash or lease loss,
// so handlers must be idempotent.
```

Kontraknya dibuktikan oleh [`test/integration/atleastonce_test.go`](../../test/integration/atleastonce_test.go), `TestStalledWorkerCausesDuplicateExecution`:

1. Worker `w1` menjalankan job `charge`. Handler-nya **macet dan mengabaikan ctx** (handler yang berperilaku buruk).
2. Lease dibuat kedaluwarsa (`expireLease`), lalu reaper mengembalikan job ke `retrying`.
3. `w2` meng-claim, menjalankan efek samping (`sideEffects.Add(1)`), lalu `Complete`.
4. `w1` bangun dan juga menjalankan efek samping. `sideEffects == 2`.
5. Status akhir job **tetap benar**: `succeeded`, `attempts = 2`. `Complete` milik `w1` ditolak fence ([03](03-lease-heartbeat-fencing.md)).

Jadi: **fencing menjaga state, idempotency menjaga dunia luar.**

Di level sistem, `cmd/loadgen -chaos` (lihat `task chaos` dan README bagian *Delivery guarantee*) membunuh container worker secara acak saat menjalankan 10.000 job, lalu menghitung baris log `bench job executed` per job. Hasil yang tercatat di README: 0 job hilang, 26 job butuh lebih dari satu attempt.

### Idempotent enqueue: `INSERT ... ON CONFLICT DO NOTHING`

Indeks unik parsial di [`migrations/00001_create_jobs.sql`](../../migrations/00001_create_jobs.sql):

```sql
CREATE UNIQUE INDEX jobs_idempotency_idx ON jobs (queue, idempotency_key)
    WHERE idempotency_key IS NOT NULL;
```

`(*Store).Enqueue` di [`internal/store/store.go`](../../internal/store/store.go):

```go
const insert = `
    INSERT INTO jobs (...) VALUES (...)
    ON CONFLICT (queue, idempotency_key) WHERE idempotency_key IS NOT NULL DO NOTHING
    RETURNING ` + jobColumns
const existing = `SELECT ` + jobColumns + ` FROM jobs WHERE queue = $1 AND idempotency_key = $2`

for range 3 {
    j, err = scanJob(s.pool.QueryRow(ctx, insert, ...))
    if err == nil {
        ...
        return j, true, nil              // kita yang membuat → 201
    }
    if !errors.Is(err, pgx.ErrNoRows) { return ..., err }
    // DO NOTHING → RETURNING kosong → sudah ada; baca yang lama
    j, err = scanJob(s.pool.QueryRow(ctx, existing, n.Queue, n.IdempotencyKey))
    if err == nil {
        return j, false, nil             // replay → 200 dengan job lama
    }
    ...                                  // baris lama lenyap di antara keduanya → ulangi
}
```

Poin penting:

- **Jangan cek dulu baru insert.** `SELECT ... WHERE key = ?` lalu `INSERT` punya race: dua request bersamaan sama-sama melihat "belum ada" dan sama-sama insert. Dengan `ON CONFLICT`, **indeks unik** yang menjadi wasit. Request yang kalah *menunggu* sampai pemenang commit, lalu tidak meng-insert apa pun. Aturan ini tercatat di `CLAUDE.md`.
- **Loop 3 kali.** Baris yang konflik bisa **hilang** di antara `INSERT` dan `SELECT` (dihapus lewat API atau oleh retensi). Kalau itu terjadi, kita coba insert lagi.
- **Kunci per queue.** `(queue, idempotency_key)`: kunci `order-42` di queue `emails` dan `reports` adalah dua job yang berbeda.
- **Parsial (`WHERE idempotency_key IS NOT NULL`)**: job tanpa kunci tidak masuk indeks dan tidak pernah konflik.
- **Respons berbeda.** API mengembalikan `201` untuk yang baru dan `200` untuk replay. Metrik `forgeq_jobs_enqueued_total{result="created"|"duplicate"}` membedakan keduanya.

Test: `TestEnqueueIdempotencyUnderConcurrency` (20 goroutine dengan kunci sama menghasilkan tepat 1 baris dan semuanya mendapat id yang sama).

## Apa yang terjadi kalau fitur ini dihapus?

- **Kalau ForgeQ diubah menjadi at-most-once** (misalnya job ditandai `succeeded` sebelum handler jalan): crash membuat job hilang tanpa jejak, dan email invoice tidak pernah terkirim.
- **Tanpa `ON CONFLICT`**: indeks unik masih ada, jadi duplikat tetap tidak masuk, tetapi request kedua mendapat **error 500** alih-alih job yang lama. Klien yang patuh (retry saat timeout) malah dihukum.
- **Tanpa indeks unik** (cek-lalu-insert di Go): di bawah konkurensi, duplikat lolos.

## Trade-off dan alternatif

| Topik | Pilihan ForgeQ | Alternatif / biaya |
|---|---|---|
| Jaminan | At-least-once + fencing | Exactly-once butuh transactional outbox atau idempotency di sistem tujuan |
| Umur idempotency key | Selama baris job masih ada | Setelah retensi menghapus job `succeeded` (7 hari), kunci yang sama bisa membuat job baru. Alternatif: tabel kunci terpisah dengan TTL sendiri. |
| Konflik berbeda isi | Replay dengan payload berbeda tetap mengembalikan job lama | Stripe menolak (`409`) jika parameter berbeda. ForgeQ tidak membandingkan payload. |
| Deduplikasi eksekusi | Tanggung jawab handler | Queue bisa menyediakan helper (misalnya tabel `processed(job_id, run)`), tetapi efek ke sistem luar tetap butuh kerja sama sistem itu |

## Eksperimen

Branch: `git switch -c coba/idempotency`.

### Eksperimen A: hapus `ON CONFLICT`

Di `(*Store).Enqueue`, hapus baris:

```diff
-		ON CONFLICT (queue, idempotency_key) WHERE idempotency_key IS NOT NULL DO NOTHING
```

```sh
go test -tags integration -count=1 -v -run '^TestEnqueueIdempotencyUnderConcurrency$' ./test/integration/
```

**Hasil yang diharapkan:**

```
store_test.go:135: enqueue: insert job: ERROR: duplicate key value violates unique constraint "jobs_idempotency_idx" (SQLSTATE 23505)
--- FAIL: TestEnqueueIdempotencyUnderConcurrency
```

Indeks unik tetap mencegah duplikat, tetapi 19 dari 20 klien sekarang mendapat error, bukan job yang sudah ada.

### Eksperimen B: buat handler di test at-least-once menjadi idempoten

Eksperimen ini kebalikan dari biasanya: kita **memperbaiki** handler, dan test yang mendokumentasikan duplikat jadi gagal karena duplikatnya **hilang**.

Di `test/integration/atleastonce_test.go`, simulasikan sistem hilir yang mengingat job id yang sudah ditagih (seperti `Idempotency-Key` di Stripe):

```diff
 import (
 	"context"
+	"sync"
 	"sync/atomic"
 ...
 	var sideEffects atomic.Int32
+	var charged sync.Map // sistem hilir mengingat job id yang sudah ditagih
+	charge := func(id int64) {
+		if _, dup := charged.LoadOrStore(id, true); !dup {
+			sideEffects.Add(1)
+		}
+	}
 	w1Started := make(chan struct{})
 	w1Resume := make(chan struct{})
+	w1Finished := make(chan struct{})
 	reg1 := worker.NewRegistry()
-	reg1.RegisterHandler("charge", func(context.Context, *job.Job) error {
+	reg1.RegisterHandler("charge", func(_ context.Context, j *job.Job) error {
 		close(w1Started)
 		<-w1Resume
-		sideEffects.Add(1)
+		charge(j.ID)
+		close(w1Finished)
 		return nil
 	})
 ...
-	sideEffects.Add(1)
+	charge(j.ID)
 	if err := s.Complete(ctx, j.ID, "w2"); err != nil {
 ...
 	close(w1Resume)
-	waitFor(t, 5*time.Second, "w1 to finish its stale run", func() bool { return sideEffects.Load() == 2 })
+	<-w1Finished
```

```sh
go test -tags integration -count=1 -v -run '^TestStalledWorkerCausesDuplicateExecution$' ./test/integration/
```

**Hasil yang diharapkan:**

```
atleastonce_test.go:95: side effects = 1, want 2 (the duplicate this test documents)
--- FAIL: TestStalledWorkerCausesDuplicateExecution
```

Job tetap **dijalankan** dua kali (at-least-once tidak berubah), tetapi **efeknya** hanya sekali. Inilah arti "handler harus idempoten".

Bersihkan: `git restore . && git switch main && git branch -D coba/idempotency`.

## Latihan interview

1. Apakah ForgeQ exactly-once? Jelaskan skenario konkret yang membuat sebuah job berjalan dua kali.
2. Fencing sudah ada, lalu kenapa duplikat masih mungkin?
3. Kenapa idempotent enqueue memakai `INSERT ... ON CONFLICT DO NOTHING` lalu `SELECT`, bukan `SELECT` lalu `INSERT`?
4. Kenapa ada loop `for range 3` di `Enqueue`?
5. Bagaimana kamu membuat handler `charge_card` idempoten?

### Jawaban

1. Tidak, ForgeQ at-least-once. Contohnya: worker menjalankan handler, efek samping terjadi (email terkirim), lalu worker di-`SIGKILL` sebelum `Complete` commit. Lease kedaluwarsa, reaper memindahkan job ke `retrying`, dan worker lain menjalankannya lagi. Skenario lain adalah worker yang macet (GC pause) melewati lease lalu melanjutkan, yang direproduksi oleh `TestStalledWorkerCausesDuplicateExecution`.
2. Fencing hanya mencegah tulisan basi ke **database ForgeQ**. Efek samping terjadi di **sistem luar** sebelum hasil ditulis, dan fence tidak bisa membatalkannya. Fencing menjaga state, idempotency menjaga efek.
3. Cek-lalu-insert adalah race read-then-write: dua request bersamaan sama-sama melihat "belum ada", lalu sama-sama insert (atau salah satunya error kalau ada indeks unik). `ON CONFLICT` menyerahkan keputusan ke indeks unik secara atomik: tepat satu yang meng-insert, dan sisanya menunggu commit pemenang lalu mendapat `RETURNING` kosong.
4. Di antara `INSERT` (yang konflik) dan `SELECT` (membaca baris lama), baris itu bisa saja dihapus, misalnya lewat `DELETE /api/v1/jobs/{id}` atau oleh retensi. `SELECT` lalu mengembalikan 0 baris. Daripada error, `Enqueue` mencoba insert lagi, dan kali ini berhasil membuat job baru. Tiga kali cukup karena kejadiannya sangat jarang. Kalau masih gagal juga, ia mengembalikan error yang jelas.
5. Kirim idempotency key yang stabil ke penyedia pembayaran. Kunci bisnis seperti nomor pesanan paling aman, atau `"job-" + id`. Jangan sertakan `attempts` atau `run`, karena kunci itu harus sama di **setiap** eksekusi ulang, termasuk retry manual dari DLQ. Penyedia (misalnya Stripe) akan mengembalikan hasil penagihan pertama untuk kunci yang sama alih-alih menagih lagi. Alternatif lokalnya: catat penagihan di tabel dengan constraint unik pada nomor pesanan, di transaksi yang sama dengan perubahan status pesanan.
