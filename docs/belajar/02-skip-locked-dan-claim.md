# 02 · SKIP LOCKED dan claim

## Masalahnya apa?

Ada 2 worker, masing-masing dengan 10 slot, dan 1 job `charge_card` (tagih kartu kredit Rp500.000) di antrean. Kedua worker bertanya ke database pada milidetik yang sama: "job pending mana yang paling depan?"

Pendekatan naif:

```sql
-- Worker A dan B menjalankan ini bersamaan
SELECT id FROM jobs WHERE status = 'pending' ORDER BY id LIMIT 1;   -- keduanya dapat id=7
UPDATE jobs SET status = 'running', locked_by = 'A' WHERE id = 7;  -- A
UPDATE jobs SET status = 'running', locked_by = 'B' WHERE id = 7;  -- B (menimpa A!)
```

Keduanya mendapat `id = 7`, keduanya menjalankan handler, dan **kartu pelanggan ditagih dua kali**. Ini *race condition* klasik **read-then-write**: ada jeda antara "membaca siapa yang kosong" dan "menandainya milik saya".

Kebutuhan kita:
1. **Eksklusif**: satu job hanya diberikan ke satu worker pada satu waktu.
2. **Tidak saling tunggu**: 50 worker yang meng-claim bersamaan tidak boleh mengantre di belakang satu baris yang sama.
3. **Urutan masuk akal**: prioritas tinggi lebih dulu, lalu yang paling lama menunggu.

## Konsep umumnya

**Row lock.** `SELECT ... FOR UPDATE` mengunci baris yang dipilih sampai transaksi selesai. Transaksi lain yang ingin mengunci baris yang sama harus **menunggu**. Setelah menunggu, PostgreSQL **mengevaluasi ulang** kondisi `WHERE` pada versi baris terbaru. Kalau barisnya sudah tidak cocok (misalnya status sudah `running`), baris itu dilewati.

**SKIP LOCKED.** `FOR UPDATE SKIP LOCKED` mengubah perilaku "menunggu" menjadi "**lewati** baris yang sedang dikunci orang lain, ambil yang berikutnya". Worker A mengunci job 7, sehingga worker B langsung mendapat job 8 tanpa menunggu.

**Claim atomik.** Gabungkan "pilih" dan "tandai" dalam **satu statement**, sehingga tidak ada jeda yang bisa disusupi.

Pola ini dipakai banyak job queue berbasis PostgreSQL, misalnya River (Go), Oban (Elixir) dan Solid Queue (Rails). Beberapa yang lebih tua, seperti Que dan GoodJob (Ruby), memakai *advisory lock* sebagai gantinya.

## Implementasi di ForgeQ

File: [`internal/store/store.go`](../../internal/store/store.go), variabel `claimQuery` dan fungsi `(*Store).Claim`.

```sql
WITH next AS (
    SELECT id FROM jobs
    WHERE queue = $1
      AND status IN ('pending', 'retrying')
      AND run_at <= now()                 -- job tertunda belum boleh jalan
    ORDER BY priority DESC, run_at, id    -- prioritas, lalu yang paling lama menunggu
    LIMIT $2                              -- sebanyak slot kosong
    FOR UPDATE SKIP LOCKED                -- kunci; lewati yang sedang dikunci orang lain
)
UPDATE jobs j
SET status       = 'running',
    attempts     = j.attempts + 1,        -- attempt dihitung SAAT CLAIM
    locked_by    = $3,                    -- lease: siapa pemiliknya
    locked_until = now() + $4 * interval '1 millisecond',
    started_at   = now(),
    updated_at   = now()
FROM next
WHERE j.id = next.id
RETURNING j.*
```

Baris-baris penting:

- **`WITH next AS (... FOR UPDATE SKIP LOCKED)`**: CTE memilih **dan mengunci** id. Karena `UPDATE` berada di statement yang sama (satu transaksi implisit), kunci dipegang sampai update selesai dan commit.
- **`LIMIT $2`**: worker meminta sebanyak **slot kosong** (`free := concurrency - len(slots)` di `runQueue`). Worker yang sibuk tidak menimbun job, sehingga job tersisa untuk replika lain.
- **`attempts = j.attempts + 1`**: dihitung saat claim, bukan saat gagal. Kalau worker crash di tengah job, attempt tetap terpakai, sehingga *poison job* tidak bisa berputar selamanya. Invariant ini tercatat di `CLAUDE.md`.
- **`locked_until = now() + lease`**: memakai **jam database**, bukan jam worker (lihat [13](13-clock-skew.md)).
- **`ORDER BY priority DESC, run_at, id`**: urutan claim. Indeks parsial di [`migrations/00001_create_jobs.sql`](../../migrations/00001_create_jobs.sql) dibuat persis untuk urutan ini:

```sql
CREATE INDEX jobs_claimable_idx ON jobs (queue, priority DESC, run_at, id)
    WHERE status IN ('pending', 'retrying');
```

Karena indeks ini **parsial** (hanya baris yang bisa di-claim), ukurannya tetap kecil meski tabel berisi jutaan job `succeeded`.

Di sisi Go, `(*Store).Claim` hanya menjalankan query dan memindai hasilnya:

```go
rows, err := s.pool.Query(ctx, claimQuery, queue, limit, workerID, lease.Milliseconds())
```

Pemanggilnya adalah `(*Worker).runQueue` di [`internal/worker/worker.go`](../../internal/worker/worker.go). Kalau batch yang didapat **penuh** (`claimed == free`), ia langsung claim lagi tanpa menunggu, karena kemungkinan masih ada job lain.

Test yang menjaga fitur ini:

| Test | File | Yang dibuktikan |
|---|---|---|
| `TestClaimOrderAndSchedule` | `test/integration/store_test.go` | urutan prioritas, job tertunda dan queue lain tidak ikut |
| `TestConcurrentClaimsAreExclusive` | `test/integration/store_test.go` | 8 claimer berebut 300 job, tidak ada yang ganda |
| `TestClaimContentionAcrossQueues` | `test/integration/concurrency_test.go` | 32 claimer, 2 queue, 5.000 job |
| `TestManyWorkersRunEachJobExactlyOnce` | `test/integration/concurrency_test.go` | 4 worker penuh, 3.000 job, setiap handler jalan tepat sekali |

## Apa yang terjadi kalau fitur ini dihapus?

Ada dua cara "menghapus", dan hasilnya sangat berbeda. Keduanya dicoba di bagian Eksperimen.

1. **Hapus seluruh `FOR UPDATE SKIP LOCKED`.** Tanpa row lock, dua statement bisa memilih id yang sama. `UPDATE` yang kedua menunggu yang pertama, lalu tetap meng-update karena kondisinya hanya `j.id = next.id` dan tidak memeriksa status lagi. Hasilnya **job diberikan ke dua worker**. Ini kegagalan korektnes.
2. **Ganti dengan `FOR UPDATE` biasa (tanpa SKIP LOCKED).** Korektnes **tetap terjaga**, karena setelah menunggu PostgreSQL mengevaluasi ulang `status IN ('pending','retrying')` dan melewati baris yang sudah `running`. Yang hilang adalah sifat **tidak saling tunggu**: claimer yang bertabrakan harus menunggu kunci orang lain dilepas.

Pelajarannya: **SKIP LOCKED adalah fitur kinerja dan liveness, bukan fitur korektnes.** Korektnes datang dari row lock dan evaluasi ulang `WHERE`.

## Trade-off dan alternatif

| Pilihan | Untung | Rugi |
|---|---|---|
| **SKIP LOCKED + lease (ForgeQ)** | Transaksi claim sangat pendek; worker tidak memegang koneksi atau transaksi selama job berjalan | Butuh heartbeat dan reaper untuk mendeteksi worker mati |
| SKIP LOCKED + transaksi terbuka selama job berjalan | Kalau worker mati, koneksi putus dan lock otomatis lepas, jadi tidak perlu reaper | Satu koneksi DB dipegang per job yang berjalan; job 10 menit berarti transaksi 10 menit (buruk untuk vacuum) |
| `FOR UPDATE` biasa | Sama benarnya | Claimer saling tunggu di baris "terdepan" yang sama |
| Optimistic: `UPDATE ... WHERE id = $1 AND status = 'pending'` lalu cek rows affected | Tanpa lock eksplisit | Banyak tabrakan saat ramai: semua mencoba id yang sama, yang kalah harus mengulang |
| Redis `BRPOPLPUSH` / stream | Sangat cepat | Sistem kedua; job dan data aplikasi tidak bisa dalam satu transaksi |

Konsekuensi lain yang perlu diakui:
- **Bukan FIFO ketat.** Di bawah konkurensi, urutannya "prioritas, lalu kira-kira yang tertua". Job yang sedang dikunci claimer lain dilewati.
- **Prioritas ketat bisa membuat job prioritas rendah kelaparan** (*starvation*) kalau job prioritas tinggi terus berdatangan. Tidak ada mekanisme *aging*.
- **Satu fetch loop per queue claim secara serial.** Ini salah satu batas throughput per worker (lihat tabel benchmark di `README.md`).

## Eksperimen

### Eksperimen A: hapus seluruh lock → claim ganda

```sh
git switch -c coba/tanpa-lock
```

Di `internal/store/store.go`, dalam `claimQuery`, hapus baris `FOR UPDATE SKIP LOCKED` (hanya di `claimQuery`, jangan di `reapQuery` atau retensi):

```diff
 		LIMIT $2
-		FOR UPDATE SKIP LOCKED
 	)
```

```sh
go test -tags integration -count=1 -v -run '^(TestConcurrentClaimsAreExclusive|TestClaimContentionAcrossQueues)$' ./test/integration/
```

**Hasil yang diharapkan** (output asli saat panduan ini diverifikasi):

```
store_test.go:96: jobs claimed more than once: [job 1: w7 and w3 job 2: w7 and w3 ...
--- FAIL: TestConcurrentClaimsAreExclusive
concurrency_test.go:165: 10341 conflicts, first: [job 2523 claimed by c25 and c11 ...
--- FAIL: TestClaimContentionAcrossQueues
```

Lebih dari sepuluh ribu konflik dari 5.000 job: sebagian job diberikan ke tiga atau empat claimer.

### Eksperimen B: `FOR UPDATE` tanpa `SKIP LOCKED` → tetap benar, tetapi menunggu

Ganti baris yang sama menjadi `FOR UPDATE` saja, lalu jalankan test yang sama ditambah `TestManyWorkersRunEachJobExactlyOnce`.

**Hasil yang diharapkan:** ketiga test **PASS**. Throughput di test itu juga hampir sama (verifikasi: 5.744 job/s dengan `FOR UPDATE` vs 5.473 job/s dengan `SKIP LOCKED`, masih dalam batas variasi). Statement claim di ForgeQ sangat pendek (milidetik), jadi waktu tunggunya kecil.

Untuk **melihat** perbedaannya, tahan sebuah lock secara manual dengan dua sesi psql. Pakai queue `demo` yang tidak dikonsumsi worker mana pun, supaya worker compose tidak ikut mengambil job-nya:

```sh
task db:up && task migrate
docker compose exec postgres psql -U forgeq -d forgeq \
  -c "INSERT INTO jobs (queue, type) VALUES ('demo', 'a'), ('demo', 'b') RETURNING id;"
```

Sesi 1 (biarkan terbuka, jangan commit):

```sh
docker compose exec postgres psql -U forgeq -d forgeq
```
```sql
BEGIN;
SELECT id FROM jobs WHERE queue = 'demo' AND status = 'pending' ORDER BY id LIMIT 1 FOR UPDATE;
-- dapat id pertama (misalnya 102521), sekarang terkunci
```

Sesi 2 (terminal lain, dengan perintah `psql` yang sama):

```sql
SET lock_timeout = '3s';
SELECT id FROM jobs WHERE queue = 'demo' AND status = 'pending' ORDER BY id LIMIT 1 FOR UPDATE SKIP LOCKED;
-- langsung: id kedua (102522)
SELECT id FROM jobs WHERE queue = 'demo' AND status = 'pending' ORDER BY id LIMIT 1 FOR UPDATE;
-- menggantung 3 detik, lalu:
-- ERROR:  canceling statement due to lock timeout
-- CONTEXT:  while locking tuple (0,2) in relation "jobs"
```

Tanpa `lock_timeout`, sesi 2 akan menunggu selamanya sampai sesi 1 `COMMIT` atau `ROLLBACK`. Bayangkan 50 worker yang semuanya menunggu di baris yang sama. Akhiri sesi 1 dengan `ROLLBACK;`, lalu bersihkan **hanya** baris demo: `DELETE FROM jobs WHERE queue = 'demo';`.

### Eksperimen C: hitung attempt saat gagal, bukan saat claim

Hapus baris `attempts = j.attempts + 1,` dari `claimQuery`, lalu:

```sh
go test -tags integration -count=1 -v -run '^(TestClaimOrderAndSchedule|TestFailRetriesThenDies)$' ./test/integration/
```

**Hasil yang diharapkan:**

```
store_test.go:39: claimed job 2 in unexpected state: &{... Attempts:0 ...}
--- FAIL: TestClaimOrderAndSchedule
store_test.go:217: after attempt 2: status = retrying, want dead
--- FAIL: TestFailRetriesThenDies
```

`attempts` tidak pernah bertambah, jadi `attempts >= max_attempts` tidak pernah terpenuhi dan job **tidak pernah mati**. Itulah poison job yang berputar selamanya.

Setelah selesai: `git restore . && git switch main && git branch -D coba/tanpa-lock`.

## Latihan interview

1. Apa beda `FOR UPDATE` dan `FOR UPDATE SKIP LOCKED`? Mana yang menjamin satu job tidak diambil dua worker?
2. Kenapa `SELECT` dan `UPDATE` digabung dalam satu statement CTE, bukan dua query terpisah dalam satu transaksi?
3. Kenapa worker meng-claim `LIMIT free` (jumlah slot kosong), bukan selalu 100?
4. Kenapa `attempts` dinaikkan saat claim? Apa kerugiannya?
5. Indeks `jobs_claimable_idx` bersifat parsial. Apa keuntungannya untuk tabel yang berisi 10 juta job `succeeded`?

### Jawaban

1. `FOR UPDATE` mengunci baris dan membuat transaksi lain **menunggu**. `SKIP LOCKED` membuat transaksi lain **melewati** baris itu. Yang menjamin eksklusivitas adalah **row lock** itu sendiri, ditambah evaluasi ulang `WHERE` setelah menunggu. Keduanya benar. SKIP LOCKED hanya menghindari antrean tunggu, dan Eksperimen B membuktikan `FOR UPDATE` biasa tetap lolos test eksklusivitas.
2. Dengan satu statement tidak ada round-trip di antara "pilih" dan "tandai", dan kunci dilepas paling cepat (autocommit). Dua query dalam satu transaksi eksplisit juga benar (kunci dipegang sampai commit), tetapi transaksi lebih panjang, koneksi lebih lama dipakai, dan lebih mudah salah, misalnya lupa `BEGIN`.
3. Supaya worker yang sibuk tidak menimbun job yang belum bisa ia kerjakan. Job yang di-claim langsung `running` dan memakai lease. Kalau ditimbun, worker lain yang menganggur tidak bisa mengambilnya, dan lease bisa kedaluwarsa sebelum job sempat dimulai.
4. Supaya crash juga memakan jatah. Kalau dihitung saat gagal, worker yang mati (SIGKILL, OOM) tidak pernah menulis kegagalan, dan job yang selalu membuat crash akan diulang tanpa batas. Kerugiannya: crash yang bukan salah job (misalnya deploy yang kasar) juga memakan attempt. Graceful shutdown menebusnya dengan `Release` yang mengurangi attempt kembali.
5. Indeks hanya berisi baris `pending`/`retrying`, yang biasanya sedikit. Ukurannya kecil sehingga muat di memori dan cepat dipindai. Sepuluh juta baris `succeeded` sama sekali tidak punya entri di indeks ini, jadi query claim tidak pernah melewati baris yang sudah selesai. Planner bisa memakai indeks parsial ini karena `WHERE` di `claimQuery` (`status IN ('pending', 'retrying')`) cocok dengan predikat indeksnya.
