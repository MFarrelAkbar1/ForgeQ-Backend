# 07 · LISTEN/NOTIFY dan temuan bottleneck-nya

File ini sekaligus kisah *debugging* nyata: desain pertama terlihat benar, lolos semua test fungsional, dan baru ketahuan memperlambat enqueue ~17× saat dilakukan load test.

## Masalahnya apa?

Worker harus tahu kapan ada job baru. Kalau hanya mengandalkan **polling**:

- Polling setiap 5 s: job rata-rata menunggu 2,5 s sebelum dimulai. Email "reset password" terasa lambat.
- Polling setiap 50 ms: 2 worker × 4 queue × 20 kali/detik = 160 query/detik ke database, **meskipun antrean kosong**.

Kita ingin latensi rendah **dan** database yang tenang saat sepi.

## Konsep umumnya

PostgreSQL punya pub/sub bawaan:

```sql
LISTEN forgeq_jobs;                          -- sesi worker berlangganan kanal
SELECT pg_notify('forgeq_jobs', 'emails');   -- sesi lain mengirim, payload = nama queue
```

Sifat penting NOTIFY:
- Notifikasi dikirim **saat transaksi pengirim commit**. Kalau transaksi rollback, notifikasinya batal. Listener tidak pernah melihat job yang belum commit.
- Notifikasi **tidak durable**. Kalau listener sedang terputus, notifikasinya hilang. Karena itu NOTIFY hanya **optimasi latensi**, sedangkan polling tetap menjadi jaring pengaman.
- `LISTEN` terikat ke **sesi** (koneksi). Kalau koneksinya dikembalikan ke pool, langganannya ikut berpindah ke pemakai lain atau hilang.
- Untuk menjaga urutan notifikasi sesuai urutan commit, PostgreSQL mengambil **satu lock global** (satu per database, di `PreCommit_Notify` di `src/backend/commands/async.c`) di akhir transaksi yang memanggil NOTIFY, dan **memegangnya sampai commit selesai**, termasuk flush WAL ke disk. Akibatnya, dua transaksi yang sama-sama memanggil NOTIFY tidak bisa commit bersamaan.

Poin terakhir itulah inti cerita ini.

## Implementasi di ForgeQ

### Desain pertama (sudah dihapus): trigger per baris

[`migrations/00001_create_jobs.sql`](../../migrations/00001_create_jobs.sql) membuat trigger:

```sql
CREATE FUNCTION forgeq_notify_job() RETURNS trigger ... AS $$
BEGIN
    IF NEW.status IN ('pending', 'retrying') AND NEW.run_at <= now() THEN
        PERFORM pg_notify('forgeq_jobs', NEW.queue);
    END IF;
    RETURN NULL;
END; $$;
CREATE TRIGGER jobs_notify AFTER INSERT OR UPDATE OF status ON jobs
    FOR EACH ROW EXECUTE FUNCTION forgeq_notify_job();
```

Desain ini elegan: tidak ada kode Go yang bisa lupa mengirim notifikasi. Semua test fungsional lolos.

### Temuan: load test

Saat `cmd/loadgen` dijalankan (fase 4), enqueue mentok di **~150 request/detik**. Penyebabnya: setiap transaksi INSERT memanggil NOTIFY lewat trigger, sehingga setiap commit mengambil lock global notifikasi dan memegangnya selama flush WAL (~3,8 ms di laptop ini). Commit yang biasanya bisa **digabung** (*group commit*: banyak transaksi di-fsync bersama) menjadi **antre satu per satu**. Batas teoretisnya 1 / 3,8 ms ≈ 260 commit/detik, dan dengan overhead lain hasilnya ~150.

Penjelasan ini tertulis di [`migrations/00004_drop_notify_trigger.sql`](../../migrations/00004_drop_notify_trigger.sql) yang menghapus trigger, dan di komentar `Notifier`.

### Desain sekarang: `store.Notifier` (pengirim)

File: [`internal/store/notifier.go`](../../internal/store/notifier.go).

Aturannya (invariant di `CLAUDE.md`): **jangan pernah NOTIFY di dalam transaksi yang menulis job.** Writer cukup *mencatat* bahwa sebuah queue perlu dibangunkan, **setelah** transaksinya commit:

```go
// di Store.Enqueue / RetryDead / Release / Reap, setelah commit:
s.notify(j.Queue)

// Notifier.Notify: hanya memori, tidak blocking
n.mu.Lock()
n.pending[queue] = struct{}{}
n.mu.Unlock()
select { case n.kick <- struct{}{}: default: }
```

Satu goroutine `Run` mengirimnya dengan *coalescing*:

```go
for {
    select {
    case <-ctx.Done(): return
    case <-n.kick:                          // ada yang tertunda
    }
    n.flush(ctx)                            // kirim SEKARANG (leading edge)
    select {
    case <-ctx.Done(): return
    case <-time.After(n.window):            // lalu diam 50 ms
    }                                       // Notify selama 50 ms menumpuk di pending
}
```

`flush` mengirim semua queue yang tertunda dalam **satu** statement, di transaksinya sendiri:

```sql
SELECT pg_notify($1, q) FROM unnest($2::text[]) AS q
```

Hasilnya: maksimal **satu notifikasi per queue per 50 ms**, berapa pun jumlah enqueue-nya. Hanya transaksi milik Notifier yang mengambil lock global, dan transaksi itu jarang.

Detail lain:
- `defer` di `Run` melakukan `flush` terakhir saat shutdown, supaya wake-up untuk job yang di-`Release` saat worker berhenti tetap terkirim. Di `cmd/worker/main.go`, notifier sengaja diberi context sendiri yang hidup lebih lama dari `w.Run`.
- Job **tertunda** (`scheduled_at` di masa depan) dan retry dengan backoff **tidak** di-notify. Mereka ditemukan lewat polling (lihat [13](13-clock-skew.md) tentang cara memutuskan "tertunda").

### Penerima: `store.Listen`

File: [`internal/store/listener.go`](../../internal/store/listener.go).

```go
conn, err := pgx.Connect(ctx, databaseURL)   // koneksi KHUSUS, bukan dari pool
conn.Exec(ctx, "LISTEN "+NotifyChannel)
for {
    n, err := conn.WaitForNotification(ctx) // blocking sampai ada notifikasi
    ...
    onNotify(n.Payload)                      // = (*Worker).Notify
}
```

- **Koneksi khusus**: `LISTEN` terikat ke sesi, dan `WaitForNotification` memegang koneksi tanpa batas waktu. Kalau memakai koneksi pool, satu slot pool hilang permanen dan langganan bisa "bocor" ke pemakai lain. Koneksi ini ikut dihitung dalam anggaran koneksi ([12](12-anggaran-koneksi.md)).
- **Reconnect dengan backoff** (1 s sampai 30 s) dan `Listen` tidak pernah mengembalikan error. Saat terputus, worker hanya log `relying on polling`.

### Penerima di worker: `(*Worker).Notify`

```go
select {
case ch <- struct{}{}:  // w.wake[queue], buffer 1
default:                // wake-up sudah menunggu; gabungkan
}
```

Channel ber-buffer **1** juga merupakan bentuk coalescing: 100 notifikasi saat fetch loop sibuk cukup menghasilkan satu claim tambahan. Queue yang tidak dikonsumsi worker ini diabaikan.

### Polling tetap ada

`runQueue` selalu menunggu dengan `timer.Reset(w.cfg.PollInterval)`. Notifikasi hanya memotong waktu tunggu.

## Apa yang terjadi kalau fitur ini dihapus?

- **Tanpa NOTIFY sama sekali**: semuanya tetap benar, tetapi job baru menunggu rata-rata `POLL_INTERVAL / 2`. Verifikasi live menunjukkan selisih `created_at` dan `started_at` hanya **19 ms** dengan NOTIFY, sedangkan tanpa NOTIFY bisa sampai 1 s.
- **Dengan NOTIFY di dalam transaksi (trigger)**: throughput enqueue anjlok sekitar 17× di disk dengan fsync yang nyata (lihat Eksperimen A).
- **Tanpa coalescing**: setiap enqueue memicu satu transaksi NOTIFY, sehingga contention lock global kembali muncul dan ribuan notifikasi membangunkan worker untuk claim yang sebagian besar kosong.
- **Tanpa polling**: notifikasi yang hilang (listener reconnect, worker baru start) membuat job terlantar sampai ada enqueue lain di queue yang sama.

## Trade-off dan alternatif

| Pilihan | Untung | Rugi |
|---|---|---|
| **Notifier ter-coalesce + polling** (ForgeQ) | Enqueue cepat; wake-up murah; tahan notifikasi hilang | Wake-up bisa terlambat sampai 50 ms; hanya tulisan **lewat store** yang mengirim wake-up (INSERT manual lewat psql hanya ditemukan oleh polling) |
| Trigger per baris | Tidak bisa lupa | Menyerialkan commit (terbukti di bawah) |
| Hanya polling | Paling sederhana | Trade-off latensi vs beban |
| Redis pub/sub atau broker | Cepat, tanpa lock global | Sistem kedua yang harus dijalankan dan dimonitor |

Pelajaran *debugging* yang lebih umum:
1. **Test fungsional tidak menangkap masalah kinerja.** Trigger lolos semua test. Yang menangkapnya adalah load test (`cmd/loadgen`).
2. **Lingkungan test bisa menyembunyikan bottleneck.** Lihat Eksperimen A: di testcontainers efeknya kecil karena fsync dimatikan.
3. **Pasang regression guard setelah memperbaiki.** `TestInsertDoesNotNotifyByItself` memastikan INSERT biasa tidak menghasilkan notifikasi, sehingga trigger tidak bisa diam-diam kembali.

## Eksperimen

### Eksperimen A: kembalikan trigger, lalu ukur di stack nyata (tanpa mengubah kode)

Migrasi 00004 punya bagian `Down` yang membuat ulang trigger. Kita cukup me-rollback migrasi itu.

```sh
task up                                   # stack lengkap
task loadgen -- -jobs 2000 -sleep 5ms     # baseline
task migrate -- down                      # rollback 00004 → trigger kembali
docker compose exec -T postgres psql -U forgeq -d forgeq -tc \
  "SELECT tgname FROM pg_trigger WHERE tgname = 'jobs_notify'"   # → jobs_notify
task loadgen -- -jobs 2000 -sleep 5ms     # dengan trigger
task migrate                              # up lagi → trigger hilang
task loadgen -- -jobs 2000 -sleep 5ms     # pulih
```

**Hasil yang diharapkan** (output asli dari laptop ini, baris `enqueued`):

```
tanpa trigger : enqueued 2000 jobs in 730ms   (2740 req/s, 0 failed requests)
dengan trigger: enqueued 2000 jobs in 12.086s (165 req/s, 0 failed requests)
tanpa trigger : enqueued 2000 jobs in 641ms   (3121 req/s, 0 failed requests)
```

### Eksperimen B: hal yang sama di testcontainers, dan kenapa hasilnya menipu

Di branch sementara (`git switch -c coba/trigger`), ubah bagian `Up` migrasi 00004 supaya **tidak** menghapus trigger:

```diff
 -- +goose Up
-DROP TRIGGER IF EXISTS jobs_notify ON jobs;
-DROP FUNCTION IF EXISTS forgeq_notify_job();
+SELECT 1;
```

Lalu tambahkan test throughput sementara `test/integration/throughput_test.go`:

```go
//go:build integration

package integration

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/MFarrelAkbar1/ForgeQ/internal/job"
)

func TestEnqueueThroughput(t *testing.T) {
	s := newStore(t)
	ctx := context.Background()
	const total, callers = 3000, 50
	var next atomic.Int64
	start := time.Now()
	var wg sync.WaitGroup
	for range callers {
		wg.Go(func() {
			for next.Add(1) <= total {
				if _, _, err := s.Enqueue(ctx, job.NewJob{Type: "t"}); err != nil {
					t.Error(err)
					return
				}
			}
		})
	}
	wg.Wait()
	took := time.Since(start)
	t.Logf("%d enqueue dari %d goroutine dalam %s: %.0f enqueue/detik",
		total, callers, took.Round(time.Millisecond), total/took.Seconds())
}
```

```sh
go test -tags integration -count=1 -v -run '^(TestInsertDoesNotNotifyByItself|TestNotificationsAreCoalesced|TestEnqueueThroughput)$' ./test/integration/
```

**Hasil yang diharapkan:**

```
notify_test.go:111: queue a: 104 notifications for 100 enqueues in 214ms, want 1..4
notify_test.go:111: queue b: 103 notifications for 100 enqueues in 214ms, want 1..4
--- FAIL: TestNotificationsAreCoalesced
notify_test.go:157: insert without notifier produced notifications [emails]
--- FAIL: TestInsertDoesNotNotifyByItself
throughput_test.go:34: 3000 enqueue dari 50 goroutine dalam 284ms: 10568 enqueue/detik
--- PASS: TestEnqueueThroughput
```

Regression guard bekerja: kedua test notifikasi gagal. Tapi lihat throughput-nya: **10.568/s** dengan trigger, dibandingkan **16.207/s** tanpa trigger (test yang sama dijalankan di `main`). Penurunannya hanya ~35%, bukan ~17×. Kenapa?

```sh
grep -n fsync "$(go list -m -f '{{.Dir}}' github.com/testcontainers/testcontainers-go/modules/postgres)/postgres.go"
# postgres.go:163:		testcontainers.WithCmd("postgres", "-c", "fsync=off"),
```

Modul postgres testcontainers menjalankan PostgreSQL dengan **`fsync=off`**. Lock global NOTIFY dipegang selama flush WAL, dan kalau flush hampir gratis, serialisasinya juga hampir gratis. Penalti trigger **sebanding dengan latensi commit**, sehingga hanya terlihat di disk sungguhan. Itulah sebabnya load test dijalankan di stack compose yang memakai volume persisten.

Bersihkan: `git restore . && rm test/integration/throughput_test.go && git switch main && git branch -D coba/trigger`.

## Latihan interview

1. Kenapa ForgeQ tetap polling padahal sudah ada LISTEN/NOTIFY?
2. Jelaskan kenapa trigger NOTIFY per baris menurunkan throughput enqueue dari ~2.750 menjadi ~150 request/detik.
3. Kenapa listener memakai koneksi `pgx.Connect` tersendiri, bukan koneksi dari pool?
4. Apa itu coalescing di Notifier, dan apa harga yang dibayar?
5. Benchmark di test integrasi hanya menunjukkan penurunan 35%. Apa pelajarannya?

### Jawaban

1. NOTIFY tidak durable. Notifikasi hilang kalau listener sedang reconnect, worker baru start, atau Notifier gagal mengirim. Job tertunda dan retry dengan backoff juga sengaja tidak di-notify. Polling menjamin semua job pada akhirnya ditemukan, sedangkan NOTIFY hanya memotong latensi.
2. Transaksi yang memanggil NOTIFY mengambil lock global antrean notifikasi dan memegangnya sampai commit selesai, termasuk flush WAL ke disk. Dengan trigger, **setiap** INSERT adalah transaksi seperti itu, sehingga commit-commit yang biasanya di-fsync bersama (group commit) harus antre satu per satu. Throughput dibatasi `1 / latensi_commit` (~3,8 ms, jadi ≤ ~260/s). Solusinya: kirim NOTIFY dari transaksi terpisah, setelah commit, dan digabung per 50 ms.
3. `LISTEN` adalah state milik sesi, dan `WaitForNotification` memblokir koneksi tanpa batas waktu. Koneksi pool bisa dikembalikan dan dipakai goroutine lain, sehingga langganan hilang atau bocor, dan satu slot pool terkunci permanen. Koneksi khusus bisa dikelola siklus hidupnya sendiri (reconnect dengan backoff).
4. Banyak permintaan `Notify` untuk queue yang sama dalam satu jendela 50 ms digabung menjadi satu `pg_notify`. Notifikasi pertama setelah periode sepi dikirim langsung, dan sisanya dikirim di akhir jendela. Harganya: wake-up bisa terlambat sampai 50 ms, dan ada satu goroutine plus state bersama (`mu`, `pending`).
5. Lingkungan test berbeda dari produksi dalam hal yang menentukan kinerja. Di sini `fsync=off` membuat commit hampir gratis dan menyembunyikan bottleneck. Ukuran kinerja harus diambil di lingkungan yang mirip produksi (volume persisten, fsync aktif). Test integrasi dipakai untuk korektnes, bukan untuk angka throughput.
