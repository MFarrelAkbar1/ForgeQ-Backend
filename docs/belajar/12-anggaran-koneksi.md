# 12 · Anggaran koneksi database

## Masalahnya apa?

PostgreSQL membatasi jumlah koneksi (`max_connections`, default **100**). Setiap koneksi adalah satu **proses** OS di server, dengan memori sendiri (beberapa MB), jadi batas ini tidak bisa dinaikkan sembarangan.

Saat load test fase 4 dengan **4 replika worker**, sebagian worker gagal dengan:

```
FATAL: sorry, too many clients already (SQLSTATE 53300)
```

Penyebabnya: pgxpool secara default boleh membuka `max(4, NumCPU)` koneksi **per proses**. Di laptop 24-core ini, itu 24 koneksi per worker. Ditambah 1 koneksi LISTEN per worker, ditambah pool API, totalnya melewati 100.

Bagian yang licik: pool membuka koneksi **secara malas** (sesuai kebutuhan). Saat sepi semuanya tampak baik-baik saja, dan masalahnya baru muncul di bawah beban, yaitu persis saat kamu paling tidak ingin ada kejutan.

## Konsep umumnya

**Connection pool**: sekumpulan koneksi yang dipakai bergantian oleh banyak goroutine. Sebuah query meminjam koneksi, menjalankannya, lalu mengembalikannya. Ukuran pool menentukan berapa query yang bisa berjalan **bersamaan** dari satu proses.

**Anggaran (budget)**: jumlahkan kebutuhan **terburuk** semua proses yang terhubung ke satu server:

```
Σ (pool_maks per proses + koneksi non-pool per proses)  ≤  max_connections − superuser_reserved_connections (3)
```

Kalau melebihi, pilihannya: kecilkan pool per proses, pasang **pooler eksternal** (PgBouncer), atau naikkan `max_connections` (mahal di memori).

**Pool besar tidak otomatis lebih cepat.** Database hanya punya sejumlah core. Ratusan query bersamaan saling berebut CPU, lock, dan I/O. Pool kecil yang selalu sibuk sering lebih cepat daripada pool besar yang saling menunggu.

## Implementasi di ForgeQ

### `DB_MAX_CONNS` diterapkan di satu tempat: `store.Connect`

[`internal/store/store.go`](../../internal/store/store.go):

```go
// maxConns caps the pool (0 keeps pgx's default of max(4, NumCPU)). Every
// process holds up to maxConns connections, so replicas x maxConns must fit
// within the server's max_connections.
func Connect(ctx context.Context, databaseURL string, maxConns int32) (*pgxpool.Pool, error) {
    cfg, err := pgxpool.ParseConfig(databaseURL)
    ...
    if maxConns > 0 {
        cfg.MaxConns = maxConns
    }
    ...
}
```

Nilainya berasal dari `DB_MAX_CONNS` (`internal/config/config.go`, default 10) dan dipakai oleh `cmd/api` dan `cmd/worker`.

### Anggaran di compose

[`docker-compose.yml`](../../docker-compose.yml):

```yaml
x-app-env: &app-env
  # Connection budget: api pool + replicas x (pool + 1 LISTEN connection) must
  # stay under Postgres max_connections (100).
  DB_MAX_CONNS: ${DB_MAX_CONNS:-10}
...
api:
  environment:
    # The api serves many short concurrent requests: give it a larger pool.
    DB_MAX_CONNS: ${API_DB_MAX_CONNS:-25}
```

Hitungannya:

| Proses | Pool | Non-pool | Total |
|---|---|---|---|
| api | 25 | 0 | 25 |
| worker × 2 | 10 | 1 (LISTEN) | 22 |
| **Jumlah** | | | **47** dari 97 yang tersedia |
| `task bench` dengan 4 worker | | | 25 + 4 × 11 = **69** |

Ditambah koneksi sesaat dari `migrate`, `psql`, dan `loadgen` (yang memakai pool pgx default, tetapi malas dan hanya butuh sedikit).

Angka ini bukan sekadar teori. Setelah menjalankan loadgen, `pg_stat_activity` di stack compose menunjukkan:

```
 client_addr | count          -- container
-------------+-------
 172.21.0.4  |    25          -- api
 172.21.0.3  |    11          -- worker-2 (10 pool + 1 LISTEN)
 172.21.0.5  |    11          -- worker-1
             |     1          -- sesi psql yang menjalankan query ini
```

### Kenapa 10 cukup untuk worker dengan 50 slot?

Sebuah job **tidak memegang koneksi selama berjalan**. Koneksi hanya dipinjam sebentar untuk `Claim`, setiap `Heartbeat`, dan `Complete`/`Fail`, masing-masing beberapa milidetik. Handler `generate_report` yang berjalan 90 detik hanya memakai koneksi sekitar 3 × 10 ms selama itu. Ini konsekuensi baik dari desain **lease** dibanding desain "transaksi terbuka selama job berjalan" ([02](02-skip-locked-dan-claim.md)), yang butuh satu koneksi per slot.

### Koneksi di luar pool

- `store.Listen` memakai `pgx.Connect` sendiri (1 per worker, lihat [07](07-listen-notify.md)).
- `cmd/migrate` memakai `database/sql` dengan driver pgx (`migrations.NewProvider`).

Karena itu ada aturan di `CLAUDE.md`: **jangan pernah menaruh `pool_max_conns` di `DATABASE_URL`.** `pgxpool.ParseConfig` memahaminya, tetapi `pgx.Connect` dan `database/sql` meneruskannya ke server sebagai parameter runtime, dan server menolaknya (lihat Eksperimen B).

## Apa yang terjadi kalau fitur ini dihapus?

- **Tanpa `DB_MAX_CONNS`**: ukuran pool bergantung pada jumlah core **mesin yang menjalankan proses**. Laptop 8-core aman, server 64-core tiba-tiba meminta 64 koneksi per worker. Begitu replika ditambah, `too many clients` muncul di bawah beban. Yang gagal adalah proses yang paling akhir meminta koneksi, sehingga gejalanya acak.
- **Tanpa menghitung koneksi LISTEN**: anggaran meleset 1 per worker. Kedengarannya sepele, sampai kamu menjalankan 20 replika.

## Trade-off dan alternatif

| Pilihan | Untung | Rugi |
|---|---|---|
| Pool kecil per proses (ForgeQ) | Sederhana, eksplisit, tanpa komponen tambahan | Anggaran harus dihitung ulang setiap kali replika ditambah |
| PgBouncer (transaction pooling) | Ribuan klien berbagi puluhan koneksi server | Komponen tambahan. Mode transaction pooling **tidak mendukung LISTEN** dan prepared statement lintas transaksi tanpa konfigurasi khusus, sehingga listener tetap butuh koneksi langsung. |
| Naikkan `max_connections` | Cepat | Setiap koneksi adalah proses plus memori; kinerja menurun saat ratusan koneksi aktif |
| Pool berbeda per peran | API 25, worker 10 | Lebih banyak knob untuk diatur |

## Eksperimen

### Eksperimen A: simulasikan replika dengan pool default

Butuh program kecil sementara. Di branch `coba/koneksi`, buat `cmd/conntest/main.go`:

```go
// Command conntest mensimulasikan N replika worker yang memakai pool penuh
// ditambah satu koneksi LISTEN masing-masing (eksperimen anggaran koneksi).
package main

import (
	"context"
	"flag"
	"fmt"
	"os"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/MFarrelAkbar1/ForgeQ/internal/store"
)

func main() {
	url := flag.String("db", "postgres://postgres:x@localhost:5499/postgres?sslmode=disable", "database URL")
	replicas := flag.Int("replicas", 4, "jumlah replika worker")
	maxConns := flag.Int("max-conns", 0, "DB_MAX_CONNS (0 = default pgx)")
	flag.Parse()

	ctx := context.Background()
	var held []*pgxpool.Conn
	total := 0
	for r := 1; r <= *replicas; r++ {
		pool, err := store.Connect(ctx, *url, int32(*maxConns))
		if err != nil {
			fmt.Printf("replika %d: %v\n", r, err)
			os.Exit(1)
		}
		for range pool.Config().MaxConns { // paksa pool penuh, seperti saat beban puncak
			c, err := pool.Acquire(ctx)
			if err != nil {
				fmt.Printf("replika %d (pool %d): gagal setelah %d koneksi: %v\n", r, pool.Config().MaxConns, total, err)
				os.Exit(1)
			}
			held = append(held, c)
			total++
		}
		if _, err := pgx.Connect(ctx, *url); err != nil { // koneksi LISTEN
			fmt.Printf("replika %d: koneksi LISTEN gagal setelah %d koneksi: %v\n", r, total, err)
			os.Exit(1)
		}
		total++
		fmt.Printf("replika %d: pool %d + 1 LISTEN, total %d koneksi\n", r, pool.Config().MaxConns, total)
	}
	fmt.Printf("OK: %d koneksi terbuka\n", total)
}
```

Jalankan PostgreSQL sementara dengan batas kecil (port 5499 supaya tidak bentrok):

```sh
docker run -d --name forgeq-conntest -e POSTGRES_PASSWORD=x -p 5499:5432 postgres:18-alpine -c max_connections=30
sleep 5
go run ./cmd/conntest -replicas 4                 # pool default pgx
go run ./cmd/conntest -replicas 4 -max-conns 5    # DB_MAX_CONNS=5
```

**Hasil yang diharapkan** (laptop 24-core, sehingga default pgx = 24):

```
replika 1: pool 24 + 1 LISTEN, total 25 koneksi
replika 2 (pool 24): gagal setelah 30 koneksi: failed to connect to `user=postgres database=postgres`:
	127.0.0.1:5499 (localhost): server error: FATAL: sorry, too many clients already (SQLSTATE 53300)
```
```
replika 1: pool 5 + 1 LISTEN, total 6 koneksi
replika 2: pool 5 + 1 LISTEN, total 12 koneksi
replika 3: pool 5 + 1 LISTEN, total 18 koneksi
replika 4: pool 5 + 1 LISTEN, total 24 koneksi
OK: 24 koneksi terbuka
```

Di mesin dengan core lebih sedikit, angka pool default-nya berbeda (`max(4, NumCPU)`), tetapi polanya sama. Gagal tepat di 30 dan bukan 27 karena user `postgres` adalah superuser, yang boleh memakai 3 slot cadangan (`superuser_reserved_connections`).

Bersihkan: `docker rm -f forgeq-conntest && rm -r cmd/conntest && git switch main && git branch -D coba/koneksi`.

### Eksperimen B: `pool_max_conns` di URL (tanpa mengubah kode)

Dengan stack compose menyala (`task db:up`):

```sh
DATABASE_URL="postgres://forgeq:forgeq@localhost:5433/forgeq?sslmode=disable&pool_max_conns=5" go run ./cmd/migrate status
```

**Hasil yang diharapkan:**

```
127.0.0.1:5433 (localhost): server error: FATAL: unrecognized configuration parameter "pool_max_conns" (SQLSTATE 42704)
```

Jalankan worker dengan URL yang sama (queue `demo` supaya tidak menyentuh job lain):

```sh
DATABASE_URL="postgres://forgeq:forgeq@localhost:5433/forgeq?sslmode=disable&pool_max_conns=5" \
  WORKER_QUEUES=demo:1 OPS_ADDR=:9096 LOG_FORMAT=text go run ./cmd/worker
```

**Hasil yang diharapkan:**

```
10:38:17.955 INFO msg="worker started" ...
10:38:17.971 WARN msg="notification listener disconnected, relying on polling" err="connect: ... FATAL: unrecognized configuration parameter ..."
10:38:18.987 WARN msg="notification listener disconnected, relying on polling" ...
10:38:21.009 WARN msg="notification listener disconnected, relying on polling" ...
```

Worker **tetap jalan**, karena `pgxpool.ParseConfig` mengerti dan membuang parameter `pool_*`. Namun koneksi LISTEN-nya (`pgx.Connect` biasa) gagal terus dengan backoff 1 s, 2 s, ..., dan worker diam-diam hanya mengandalkan polling. Ini jenis bug yang mudah lolos. Karena itu batas pool diberikan lewat `DB_MAX_CONNS`, bukan lewat URL.

## Latihan interview

1. Kenapa tidak cukup menaikkan `max_connections` menjadi 1000?
2. Hitung anggaran koneksi untuk 1 API (pool 25) dan 6 worker (pool 10).
3. Worker punya 50 slot tetapi pool-nya hanya 10. Kenapa itu tidak menjadi bottleneck?
4. Kenapa `too many clients` baru muncul di bawah beban, tidak saat start?
5. Kapan kamu akan memasang PgBouncer, dan apa yang harus diwaspadai untuk ForgeQ?

### Jawaban

1. Setiap koneksi PostgreSQL adalah satu proses backend dengan memorinya sendiri (`work_mem` per operasi, cache katalog, dan lain-lain). Ratusan koneksi aktif juga saling berebut CPU dan lock, sehingga throughput turun, bukan naik. Batas koneksi adalah perlindungan, bukan sekadar angka konfigurasi.
2. API 25 + 6 × (10 pool + 1 LISTEN) = 25 + 66 = **91**. Dengan `max_connections = 100` dan 3 cadangan superuser, tersisa 6 untuk migrate, psql, monitoring, dan lain-lain. Itu terlalu mepet, jadi turunkan pool worker ke 8 (25 + 54 = 79) atau pasang pooler.
3. Job tidak memegang koneksi selama handler berjalan. Koneksi hanya dipinjam beberapa milidetik untuk claim, heartbeat, dan complete/fail. Selama total waktu query jauh lebih kecil dari waktu handler, 10 koneksi bisa melayani puluhan slot. Untuk job yang sangat pendek (milidetik), barulah koneksi dan latensi commit menjadi faktor (lihat bagian benchmark di README).
4. pgxpool membuka koneksi secara malas, hanya ketika semua koneksi yang ada sedang dipakai dan pool masih di bawah `MaxConns`. Saat sepi setiap proses hanya memakai 1–2 koneksi. Di bawah beban semua pool tumbuh ke maksimumnya bersamaan, dan totalnya melewati batas server.
5. Saat jumlah proses klien (replika × pool) jauh melebihi yang sanggup ditangani server, misalnya puluhan replika atau autoscaling. Hal yang harus diwaspadai: mode *transaction pooling* tidak mempertahankan state sesi, sehingga `LISTEN` tidak bekerja lewat PgBouncer. Koneksi listener harus langsung ke PostgreSQL (atau lewat pool mode *session*), dan prepared statement pgx perlu dikonfigurasi (misalnya `default_query_exec_mode`).
