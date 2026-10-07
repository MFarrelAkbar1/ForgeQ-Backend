# 13 · Clock skew: jam siapa yang dipercaya?

## Masalahnya apa?

ForgeQ melibatkan setidaknya empat jam: jam **klien** (yang mengirim `scheduled_at`), jam proses **API**, jam proses **worker**, dan jam **server PostgreSQL**. Keempatnya tidak pernah sama persis. NTP menjaga selisihnya di kisaran milidetik, tetapi VM, container, dan laptop yang baru bangun dari sleep bisa meleset lebih jauh.

**Bug nyata di ForgeQ (commit `54c26e3`).** `Store.Enqueue` dulu memutuskan "apakah job ini tertunda?" seperti ini:

```go
// VERSI LAMA (sebelum commit 54c26e3)
if !j.RunAt.After(time.Now()) { // delayed jobs are found by polling
    s.notify(j.Queue)
}
// j.RunAt diisi now() oleh DATABASE; time.Now() adalah jam PROSES API.
```

Untuk job tanpa `scheduled_at`, `run_at` diisi `now()` oleh database. Kalau jam database **sedikit saja lebih cepat** dari jam proses, `run_at` tampak "di masa depan". Job yang seharusnya langsung jalan diperlakukan sebagai job tertunda, sehingga **tidak ada wake-up**, dan worker baru menemukannya di poll berikutnya (sampai 1 s kemudian).

Bug ini tidak membuat job hilang, sehingga test fungsional lolos. Gejalanya hanya **kadang-kadang** lambat. Bug ini ditemukan karena test notifier dijalankan berulang kali dan sesekali gagal: proses test berjalan di host Windows, sedangkan PostgreSQL berjalan di container.

## Konsep umumnya

1. **Jangan membandingkan timestamp dari dua jam yang berbeda** kalau keputusannya peka terhadap selisih kecil.
2. **Pilih satu otoritas waktu** untuk keputusan bersama. Di sistem yang berpusat pada database, otoritasnya adalah `now()` database: semua proses melihat jam yang sama.
3. **Gunakan jam monotonik untuk durasi.** Jam dinding (*wall clock*) bisa melompat mundur saat NTP mengoreksi. Di Go, `time.Now()` menyimpan pembacaan monotonik, sehingga `time.Since(start)` aman dari lompatan. Nilai `time.Time` yang dibaca dari database **tidak** punya komponen monotonik.
4. **Kalau terpaksa mencampur jam**, pastikan akibat terburuknya dapat diterima (misalnya hanya optimasi yang meleset, bukan korektnes).

## Implementasi di ForgeQ

### Keputusan yang memakai jam database saja (aman dari skew)

| Keputusan | Di mana | SQL |
|---|---|---|
| Kapan lease habis | `claimQuery`, `Heartbeat` | `locked_until = now() + lease` |
| Lease sudah lewat? | `reapQuery` | `locked_until < now()` |
| Job sudah boleh jalan? | `claimQuery` | `run_at <= now()` |
| Jalankan segera | `RetryDead`, `Release`, `reapQuery`, `Enqueue` tanpa jadwal | `run_at = now()`, `COALESCE($6, now())` |
| Pengukuran benchmark | `cmd/loadgen/measure.go`, `measure` | `finished_at - created_at`, `started_at - created_at` |

Karena lease ditulis **dan** diperiksa dengan `now()` database, worker yang jamnya meleset 10 menit pun tidak membuat lease-nya lebih panjang atau lebih pendek. Komentar di `measure`: *"All timestamps come from the database clock, so client/server clock skew does not matter."*

### Durasi lokal memakai jam monotonik

`took := time.Since(start)` di `(*Worker).process`, ticker heartbeat, `waitTimeout` saat shutdown, dan jendela 50 ms di Notifier semuanya adalah durasi di dalam satu proses, sehingga tidak terpengaruh skew.

### Perbaikannya: putuskan dari request, bukan dari jam database

[`internal/store/store.go`](../../internal/store/store.go), `(*Store).Enqueue`:

```go
// Delayed jobs are found by polling. Decide from the request, not
// from j.RunAt: that is the database clock, and comparing it with
// this process's clock skips wake-ups whenever the DB runs ahead.
if n.RunAt == nil || !n.RunAt.After(time.Now()) {
    s.notify(j.Queue)
}
```

- Tanpa `scheduled_at` (`n.RunAt == nil`): **selalu** notify. Tidak ada perbandingan jam sama sekali, sehingga kasus yang paling umum kebal terhadap skew.
- Dengan `scheduled_at`: jadwal dari klien dibandingkan dengan jam proses. Ini masih mencampur jam, tetapi akibat terburuknya kecil. Job yang dijadwalkan "1 ms dari sekarang" mungkin tidak di-notify dan ditemukan lewat polling, atau sebaliknya ada notifikasi yang tidak perlu (worker bangun lalu claim kosong). Korektnes tetap dijaga `run_at <= now()` di database.

### Tempat yang masih mencampur jam (dan kenapa itu dapat diterima)

| Tempat | Campuran | Akibat skew | Perbaikan bila perlu |
|---|---|---|---|
| `(*Worker).finish`: `retryAt := time.Now().Add(RetryDelay(...))` disimpan ke `run_at`, lalu dibandingkan dengan `now()` DB | jam worker vs jam DB | Jeda backoff efektif bergeser sebesar skew (worker 5 s tertinggal berarti retry 5 s lebih cepat) | Kirim **durasi** ke SQL: `run_at = now() + $5 * interval '1 millisecond'` |
| `(*Reaper).deleteOldSucceeded`: `cutoff := r.now().Add(-Retention)` vs `finished_at` | jam worker vs jam DB | Retensi 7 hari meleset beberapa detik | Tidak perlu |
| Dashboard `ago` (`time.Since(t)`) | jam API vs timestamp DB | "3s ago" bisa tampil sebagai "-1s ago" | Kosmetik |

## Apa yang terjadi kalau fitur ini dihapus?

Kalau lease memakai jam worker (misalnya `locked_until = $workerNow + 30s` lalu reaper membandingkannya dengan jam reaper): worker yang jamnya tertinggal 40 s menulis lease yang **sudah kedaluwarsa** menurut reaper, sehingga job-nya langsung diambil alih saat masih berjalan dan terjadi eksekusi ganda terus-menerus. Sebaliknya, worker yang jamnya 1 jam di depan menulis lease yang baru habis 1 jam kemudian, sehingga crash-nya tidak terdeteksi selama 1 jam.

Kalau perbaikan `54c26e3` dihapus: wake-up hilang sesekali, dan latensi job naik secara acak sampai `POLL_INTERVAL`.

## Trade-off dan alternatif

| Pilihan | Untung | Rugi |
|---|---|---|
| Jam database sebagai otoritas (ForgeQ) | Satu jam untuk semua keputusan bersama; skew antar-worker tidak relevan | Setiap keputusan waktu harus ditulis di SQL |
| Jam lokal + NTP ketat | Logika di Go | Skew tetap ada; korektnes bergantung pada infrastruktur |
| Logical clock / fencing token | Tidak bergantung jam sama sekali | Lebih rumit. Lease tetap butuh *batas waktu*, sehingga tetap butuh jam. |

## Eksperimen

### Eksperimen A: simulasikan jam database 2 detik lebih cepat

Kita tidak bisa memutar jam container dengan mudah, tetapi bisa menyimulasikan efeknya di satu tempat yang relevan: `run_at` yang ditulis database. Branch: `git switch -c coba/skew`.

Di `(*Store).Enqueue`:

```diff
-		VALUES ($1, $2, $3, $4, $5, COALESCE($6, now()), $7)
+		VALUES ($1, $2, $3, $4, $5, COALESCE($6, now() + interval '2 seconds'), $7)
```

```sh
go test -tags integration -count=1 -v -run '^TestEnqueueWakesWorkers$' ./test/integration/
```

**Hasil yang diharapkan:** `--- PASS`. Perbaikannya bekerja: keputusan notify tidak lagi melihat `j.RunAt`.

### Eksperimen B: kembalikan bug-nya

Masih dengan perubahan di atas, kembalikan juga logika lama:

```diff
-			if n.RunAt == nil || !n.RunAt.After(time.Now()) {
+			if !j.RunAt.After(time.Now()) {
```

```sh
go test -tags integration -count=1 -v -run '^TestEnqueueWakesWorkers$' ./test/integration/
```

**Hasil yang diharapkan:**

```
notify_test.go:72: notifications = [], want [emails]
--- FAIL: TestEnqueueWakesWorkers
```

Job biasa (tanpa jadwal) tidak membangunkan worker sama sekali.

Bersihkan: `git restore . && git switch main && git branch -D coba/skew`.

### Eksperimen C: ukur skew di mesinmu

Program sementara `cmd/skew/main.go` (di branch sementara):

```go
// Command skew mengukur selisih jam antara proses ini dan server PostgreSQL.
package main

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/jackc/pgx/v5"
)

func main() {
	ctx := context.Background()
	conn, err := pgx.Connect(ctx, os.Getenv("DATABASE_URL"))
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	defer conn.Close(ctx)
	for range 5 {
		t0 := time.Now()
		var db time.Time
		if err := conn.QueryRow(ctx, "SELECT clock_timestamp()").Scan(&db); err != nil {
			fmt.Println(err)
			os.Exit(1)
		}
		t1 := time.Now()
		mid := t0.Add(t1.Sub(t0) / 2) // perkiraan saat query dieksekusi di server
		fmt.Printf("jam DB - jam proses = %+8.3f ms (round trip %.3f ms)\n",
			float64(db.Sub(mid).Microseconds())/1000, float64(t1.Sub(t0).Microseconds())/1000)
		time.Sleep(200 * time.Millisecond)
	}
}
```

```sh
task db:up
DATABASE_URL="postgres://forgeq:forgeq@localhost:5433/forgeq?sslmode=disable" go run ./cmd/skew
```

**Hasil saat verifikasi** (host Windows vs PostgreSQL di Docker Desktop):

```
jam DB - jam proses =   +0.202 ms (round trip 2.013 ms)
jam DB - jam proses =   -0.380 ms (round trip 3.119 ms)
jam DB - jam proses =   -0.350 ms (round trip 1.802 ms)
jam DB - jam proses =   -0.119 ms (round trip 2.483 ms)
jam DB - jam proses =   -0.190 ms (round trip 0.512 ms)
```

Selisihnya di bawah 1 ms, sama orde dengan round-trip, dan tandanya berganti-ganti. Inilah sebabnya bug lama hanya muncul **kadang-kadang**: `j.RunAt > time.Now()` bisa benar atau salah tergantung kombinasi skew dan latensi pada saat itu. Jam VM Docker Desktop juga dikenal bisa tertinggal lebih jauh setelah laptop bangun dari sleep, sampai disinkronkan lagi. Jalankan program ini dalam kondisi itu untuk melihat skew yang lebih besar.

Bersihkan: `rm -r cmd/skew`.

## Latihan interview

1. Kenapa `locked_until` dihitung dengan `now()` database, bukan `time.Now()` worker?
2. Jelaskan bug di commit `54c26e3` dan kenapa ia hanya muncul kadang-kadang.
3. Bagaimana perbaikannya menghilangkan masalah untuk kasus yang paling umum?
4. Di mana ForgeQ masih mencampur jam, dan bagaimana kamu memperbaikinya?
5. Apa beda jam dinding dan jam monotonik, dan kapan memakai yang mana?

### Jawaban

1. Lease ditulis oleh worker dan diperiksa oleh reaper di proses lain. Kalau keduanya memakai jam lokal masing-masing, skew antar-mesin langsung menjadi kesalahan lease: lease bisa dianggap kedaluwarsa padahal belum, atau sebaliknya. Dengan `now()` database, penulisan dan pemeriksaan memakai jam yang sama, sehingga skew antar-worker tidak berpengaruh.
2. `Enqueue` memutuskan perlu notify atau tidak dengan membandingkan `run_at` (diisi `now()` database) dengan `time.Now()` proses API. Kalau jam DB sedikit lebih cepat, job biasa tampak tertunda dan tidak di-notify, sehingga baru diambil saat poll (≤ 1 s). Skew di mesin pengembang kurang dari 1 ms, seorde dengan round-trip, jadi hasil perbandingannya acak. Bug ini hanya terlihat sebagai test yang sesekali gagal.
3. Keputusan diambil dari **request**: kalau klien tidak memberi `scheduled_at`, job selalu di-notify tanpa membandingkan jam apa pun. Perbandingan hanya terjadi untuk job yang memang dijadwalkan, dan di situ salah tebak hanya berarti satu wake-up berlebih atau sedikit menunggu poll. Korektnes tetap dijaga `run_at <= now()` di SQL.
4. `retryAt` backoff dihitung dengan `time.Now()` worker lalu dibandingkan dengan `now()` database, sehingga jeda efektif bergeser sebesar skew. Perbaikannya: kirim durasinya dan biarkan SQL menghitung `run_at = now() + delay`. Cutoff retensi juga dihitung di Go, tetapi toleransi 7 hari membuat skew detik tidak relevan.
5. Jam dinding menunjukkan waktu kalender dan bisa melompat (koreksi NTP, perubahan manual). Jam monotonik hanya bergerak maju dan cocok untuk **mengukur durasi**. Di Go, `time.Now()` membawa keduanya, `time.Since`/`Sub` memakai monotonik, sedangkan nilai yang diserialisasi atau dibaca dari DB hanya membawa jam dinding. Pakai monotonik untuk timeout dan durasi lokal, dan pakai satu jam dinding yang otoritatif (DB) untuk keputusan antar-proses.
