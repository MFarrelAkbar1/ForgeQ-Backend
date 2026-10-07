# 04 · Backoff dan jitter

## Masalahnya apa?

Pukul 14.00 server SMTP down selama 5 menit. Dalam rentang itu 2.000 job `send_email` gagal. Kapan mereka sebaiknya dicoba lagi?

- **Langsung (tanpa jeda):** 2.000 job langsung gagal lagi, lagi, dan lagi. Dalam hitungan detik semua `max_attempts` habis dan semuanya `dead`, padahal SMTP akan pulih 5 menit lagi. Selama itu kita juga menghujani server yang sedang sakit.
- **Jeda tetap, misalnya 60 detik:** lebih baik, tetapi 2.000 job yang gagal pada detik yang sama juga akan retry pada **detik yang sama**. Begitu SMTP pulih, ia langsung dihantam 2.000 request serentak dan bisa tumbang lagi. Ini disebut *thundering herd*.

Kebutuhan: (1) jeda yang **makin panjang** supaya gangguan panjang tidak menghabiskan attempt, (2) **disebar acak** supaya job yang gagal bersamaan tidak retry bersamaan, (3) ada **batas atas** supaya job tidak menunggu berhari-hari.

## Konsep umumnya

**Exponential backoff:** jeda = `base × 2^(n-1)`. Dengan base 2 s hasilnya 2, 4, 8, 16, 32, ... detik. Total waktu tunggu tumbuh cepat, sehingga beberapa attempt saja sudah mencakup gangguan yang panjang.

**Jitter:** tambahkan komponen acak. Ada beberapa varian (dari artikel AWS *Exponential Backoff and Jitter*):

| Varian | Rumus | Sifat |
|---|---|---|
| Tanpa jitter | `base·2^n` | Serempak, rawan thundering herd |
| **Additive** (ForgeQ) | `base·2^n + rand[0, J)` | Mudah ditebak, sebaran selebar J |
| Full jitter | `rand[0, base·2^n)` | Sebaran paling lebar, rata-rata jeda separuhnya |
| Equal jitter | `base·2^n/2 + rand[0, base·2^n/2)` | Kompromi |

**Cap (batas atas):** `min(..., max)`.

## Implementasi di ForgeQ

File: [`internal/backoff/backoff.go`](../../internal/backoff/backoff.go). Seluruh paket hanya berisi 46 baris dan **fungsi murni** (tanpa I/O, tanpa jam), sehingga mudah ditest.

```go
// delay = min(Base * 2^(attempts-1) + rand[0, Jitter), Max)
func (p Policy) Delay(attempts int) time.Duration {
    exp := max(attempts-1, 0)

    // Double step by step so a large attempt count cannot overflow int64.
    d := p.Base
    for range exp {
        if d >= p.Max {
            break
        }
        d *= 2
    }

    if p.Jitter > 0 {
        r := p.Rand
        if r == nil {
            r = rand.Float64
        }
        d += time.Duration(r() * float64(p.Jitter))
    }
    return min(d, p.Max)
}
```

Baris-baris penting:

- **`attempts-1`**: `attempts` **sudah** dinaikkan saat claim ([02](02-skip-locked-dan-claim.md)). Setelah percobaan pertama gagal, `attempts = 1`, dan retry pertama harus menunggu `base × 2^0 = base`. Kalau rumusnya `2^attempts`, retry pertama langsung menunggu `2 × base`. Detail kecil ini adalah salah satu invariant di `CLAUDE.md`.
- **Menggandakan langkah demi langkah dan berhenti di `Max`**: `base * 2^attempts` dengan `math.Pow` atau shift bisa **overflow** `int64` untuk attempts besar dan menghasilkan durasi negatif. Loop ini berhenti begitu mencapai `Max`, sehingga `Delay(10_000)` tetap aman (ada test-nya).
- **`Rand func() float64` bisa diinjeksi**: test memasang `fixed(0.5)` supaya hasilnya deterministik. Di produksi nilainya `nil`, sehingga dipakai `math/rand/v2`.
- **`min(d, p.Max)` setelah jitter**: cap juga berlaku untuk jitter.

Policy ini dirakit di [`cmd/worker/main.go`](../../cmd/worker/main.go) dan diberikan ke worker sebagai **fungsi**:

```go
RetryDelay: backoff.Policy{
    Base:   cfg.Worker.BackoffBase,    // BACKOFF_BASE, default 2s
    Max:    cfg.Worker.BackoffMax,     // BACKOFF_MAX, default 1h
    Jitter: cfg.Worker.BackoffJitter,  // BACKOFF_JITTER, default 1s
}.Delay,
```

Package `worker` tidak mengimpor `backoff`. Ia hanya tahu `RetryDelay func(attempts int) time.Duration`, dan unit test worker cukup memasang `func(int) time.Duration { return time.Minute }`.

Pemakaiannya ada di `(*Worker).finish`:

```go
retryAt := time.Now().Add(w.cfg.RetryDelay(j.Attempts))
```

`retryAt` disimpan ke kolom `run_at` oleh `Store.Fail`, dan `claimQuery` baru mengambil job ini setelah `run_at <= now()`.

Dengan nilai default, urutan jedanya (tanpa jitter) adalah:

| Gagal ke- (`attempts`) | Jeda sebelum attempt berikutnya |
|---|---|
| 1 | 2 s (+0–1 s) |
| 2 | 4 s |
| 3 | 8 s |
| 4 | 16 s |
| 5 | job dead (default `max_attempts = 5`) |

## Apa yang terjadi kalau fitur ini dihapus?

- **Tanpa backoff (jeda 0):** gangguan 10 detik saja sudah cukup untuk membuat semua job yang terdampak `dead`. Dependency yang sedang pulih dihujani request.
- **Tanpa jitter:** job yang gagal bersamaan akan retry bersamaan selamanya. Pada setiap gelombang retry, beban puncaknya sama dengan beban saat gangguan pertama.
- **Tanpa cap:** `attempts = 30` dengan base 2 s berarti jeda 2^29 × 2 s, sekitar 34 tahun. Lebih buruk lagi, tanpa loop bertahap nilainya bisa overflow menjadi negatif sehingga job di-retry seketika.

## Trade-off dan alternatif

| Pilihan | Untung | Rugi |
|---|---|---|
| **Additive jitter** (ForgeQ) | Jeda minimum terjamin (`≥ base·2^n`), mudah dijelaskan dan ditest | Sebaran hanya selebar `Jitter` (1 s). Untuk ribuan job dengan jeda 32 s, sebaran 1 s relatif sempit. |
| Full jitter | Paling efektif menyebar beban (rekomendasi AWS) | Jeda bisa mendekati 0, jadi kurang bisa ditebak |
| Policy per job (lewat API) | Klien bisa memilih | Belum didukung. Policy ditetapkan per proses worker (dicatat di bagian Limitations di README). |
| Retry dari reaper | Langsung (`run_at = now()`), tanpa backoff | Job yang membuat worker crash akan langsung dicoba lagi. Ini sudah dicatat sebagai keterbatasan. |

Catatan jam: `retryAt` dihitung dengan **jam worker** (`time.Now()`), tetapi dibandingkan dengan **jam database** (`run_at <= now()`). Kalau jam worker 5 detik tertinggal, jeda efektifnya 5 detik lebih pendek. Lihat [13-clock-skew.md](13-clock-skew.md).

## Eksperimen

Unit test, tanpa Docker. Branch: `git switch -c coba/backoff`.

### Eksperimen A: eksponen salah (`attempts` alih-alih `attempts-1`)

```diff
-	exp := max(attempts-1, 0)
+	exp := max(attempts, 0)
```

```sh
go test -count=1 -v ./internal/backoff/
```

**Hasil yang diharapkan:**

```
backoff_test.go:33: Delay(1) = 2s, want 1s
backoff_test.go:33: Delay(2) = 4s, want 2s
backoff_test.go:33: Delay(5) = 32s, want 16s
backoff_test.go:33: Delay(3) = 8.25s, want 4.25s
backoff_test.go:33: Delay(6) = 1m0s, want 32.495s
--- FAIL: TestDelay
backoff_test.go:44: Delay(2) = 4.444146274s, want in [2s, 3s)
--- FAIL: TestDelayJitterBounds
--- PASS: TestDelayNeverExceedsMax
```

Semua jeda dua kali lebih panjang dari seharusnya. `TestDelayNeverExceedsMax` tetap lolos karena cap masih bekerja. Sub-test `capped_at_max`, `huge_attempts_do_not_overflow` dan `zero_attempts_treated_as_first` juga tetap lolos.

### Eksperimen B: hapus jitter

```diff
-		d += time.Duration(r() * float64(p.Jitter))
+		_ = r
```

**Hasil yang diharapkan:**

```
backoff_test.go:33: Delay(3) = 4s, want 4.25s
backoff_test.go:33: Delay(6) = 32s, want 32.495s
--- FAIL: TestDelay
    --- FAIL: TestDelay/jitter_is_added
    --- FAIL: TestDelay/cap_includes_jitter
--- PASS: TestDelayJitterBounds
```

Menarik: `TestDelayJitterBounds` **lolos**, karena `2s` masih berada dalam rentang `[2s, 3s)`. Test itu hanya memeriksa **batas**, bukan bahwa jitter benar-benar ada. Yang menangkap hilangnya jitter adalah test tabel dengan `Rand` yang di-fix ke 0,5 dan 0,99. Ini contoh bagus kenapa nilai acak perlu bisa diinjeksi.

Bersihkan: `git restore . && git switch main && git branch -D coba/backoff`.

## Latihan interview

1. Kenapa eksponennya `attempts-1`, bukan `attempts`?
2. Bagaimana `Delay` menghindari overflow untuk attempts yang sangat besar?
3. Apa itu thundering herd, dan bagaimana jitter mengatasinya?
4. Bandingkan additive jitter dengan full jitter. Kapan kamu memilih yang mana?
5. Kenapa `Rand` dibuat sebagai field yang bisa diinjeksi?

### Jawaban

1. `attempts` dinaikkan **saat claim**, sehingga ketika `Delay` dipanggil setelah kegagalan pertama, nilainya sudah 1. Retry pertama seharusnya menunggu `base`, yaitu `base × 2^0`, jadi eksponennya `attempts-1`. `max(..., 0)` menjaga kasus `attempts = 0`.
2. Alih-alih menghitung `2^exp` sekaligus, ia menggandakan `d` satu langkah per iterasi dan **berhenti** begitu `d >= Max`. Nilai tidak pernah melewati sekitar `2 × Max`, sehingga tidak pernah overflow. Loop paling banyak berjalan sekitar log2(Max/Base) kali walaupun attempts 10.000.
3. Thundering herd terjadi ketika banyak klien yang gagal bersamaan juga mencoba lagi bersamaan, sehingga beban puncak berulang di setiap gelombang retry dan bisa menjatuhkan layanan yang baru pulih. Jitter menambah jeda acak yang berbeda per job, sehingga retry tersebar dalam satu jendela waktu dan tidak menumpuk di satu detik.
4. Additive menjamin jeda minimum dan mudah ditebak, tetapi sebarannya hanya selebar `Jitter`. Full jitter (`rand[0, cap)`) menyebar beban paling rata, dan dalam simulasi AWS menghasilkan total kerja terkecil, tetapi jedanya bisa sangat pendek. Untuk dependency yang rapuh dan banyak klien, pilih full jitter. Kalau kepastian "minimal tunggu X" lebih penting (misalnya rate limit yang ketat), pilih additive.
5. Supaya test deterministik. Dengan `Rand: fixed(0.5)`, `Delay(3)` pasti `4.25s` dan bisa dibandingkan persis. Tanpa injeksi, test hanya bisa memeriksa rentang, dan Eksperimen B membuktikan test rentang tidak mendeteksi jitter yang hilang.
