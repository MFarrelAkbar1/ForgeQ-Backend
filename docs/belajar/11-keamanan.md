# 11 · Keamanan: API key, constant-time compare, CSRF

## Masalahnya apa?

1. **Siapa pun bisa enqueue.** API ForgeQ terbuka di jaringan. Orang iseng bisa mengirim 1 juta job `send_email`, atau membaca payload job yang berisi alamat email dan data pelanggan.
2. **Menebak key lewat waktu respons.** Kalau key dibandingkan dengan `==`, perbandingan berhenti di byte pertama yang berbeda. Penyerang yang mengukur waktu respons dengan sangat teliti (ribuan sampel) bisa menebak key **byte demi byte**: tebakan yang byte pertamanya benar sedikit lebih lambat ditolak.
3. **CSRF lewat dashboard.** Operator sudah login ke dashboard (basic auth). Ia lalu membuka situs jahat yang berisi `<form method="POST" action="http://forgeq.internal/ui/jobs/42/retry">` yang otomatis di-submit. Browser **otomatis melampirkan kredensial basic auth** yang tersimpan, sehingga job ter-retry atau terhapus tanpa sepengetahuan operator.

## Konsep umumnya

- **Shared secret.** Satu `API_KEY`. Klien mesin mengirimnya sebagai `Authorization: Bearer <key>`, sedangkan browser memakai basic auth (dialog login bawaan browser).
- **Constant-time comparison.** Waktu perbandingan tidak boleh bergantung pada **isi** input. `crypto/subtle.ConstantTimeCompare` membandingkan semua byte tanpa berhenti lebih awal. Tetapi fungsi ini langsung mengembalikan 0 kalau **panjangnya** berbeda, sehingga panjang key bisa bocor. Solusinya: hash dulu kedua sisi menjadi 32 byte.
- **CSRF.** Request lintas-situs yang membawa kredensial otomatis (cookie, basic auth). Pertahanan klasiknya adalah token CSRF di setiap form. Pertahanan modernnya: browser mengirim header `Sec-Fetch-Site` (dan `Origin`), sehingga server bisa menolak request **state-changing** yang datang dari origin lain.

## Implementasi di ForgeQ

### Pemeriksaan key: [`internal/auth/auth.go`](../../internal/auth/auth.go)

```go
// Equal reports whether given matches want in constant time. Both values are
// hashed first so the comparison time does not reveal the key's length either.
func Equal(given, want string) bool {
    g := sha256.Sum256([]byte(given))
    w := sha256.Sum256([]byte(want))
    return subtle.ConstantTimeCompare(g[:], w[:]) == 1
}
```

- SHA-256 membuat kedua sisi selalu 32 byte, jadi panjang key tidak bocor.
- `ConstantTimeCompare` memeriksa ke-32 byte tanpa keluar lebih awal.
- Hash yang sama untuk input yang sama membuat hasilnya tetap benar. Peluang tabrakan SHA-256 bisa diabaikan.

`BearerToken` mem-parse `Authorization: Bearer <token>` (skema tidak case-sensitive). `ValidBearer`/`ValidBasic` mengembalikan `true` kalau key kosong, yaitu mode pengembangan. Di mode ini `cmd/api/main.go` mencatat peringatan saat start.

### Cakupan

- **Seluruh `/api/v1`**, termasuk GET. Middleware `requireAPIKey` dipasang di `r.Route("/api/v1", ...)` dalam `api.NewRouter` ([`internal/api/api.go`](../../internal/api/api.go)). Payload job bisa berisi data sensitif, jadi membaca juga dilindungi. Respons 401 menyertakan `WWW-Authenticate: Bearer realm="forgeq"`.
- **Seluruh dashboard `/ui`**: `requireBasicAuth` di [`internal/dashboard/dashboard.go`](../../internal/dashboard/dashboard.go). Password = `API_KEY`, username bebas.
- **Terbuka**: `/healthz`, `/readyz`, `/metrics`, untuk probe dan Prometheus. Ini keputusan sadar yang tercatat sebagai keterbatasan: `/metrics` membocorkan nama queue.

### CSRF: `http.CrossOriginProtection`

Di akhir `dashboard.New`:

```go
// Browsers send basic-auth credentials automatically, even on requests
// triggered by another site, so state-changing requests need CSRF
// protection. CrossOriginProtection rejects cross-origin POST/DELETE using
// the Sec-Fetch-Site and Origin headers; no tokens are needed.
return http.NewCrossOriginProtection().Handler(r), nil
```

`http.CrossOriginProtection` (stdlib Go 1.25+) menolak request non-safe (POST, DELETE, ...) yang datang dari **origin lain** dengan **403**. Origin lain terdeteksi kalau `Sec-Fetch-Site` bernilai selain `same-origin`/`none`, atau, kalau header itu tidak ada, kalau host di `Origin` tidak sama dengan `Host`. GET, HEAD dan OPTIONS selalu lolos, karena metode itu memang tidak boleh mengubah state (dan dashboard ForgeQ mematuhinya: retry dan delete memakai POST dan DELETE).

API `/api/v1` tidak butuh ini, karena browser tidak melampirkan header `Authorization: Bearer` secara otomatis. Penyerang tidak bisa memaksa browser korban mengirimnya.

### Lapisan lain

- **Escaping**: `html/template` meng-escape data job secara otomatis. Test dashboard memakai `last_error` berisi `<script>alert(1)</script>`.
- **Error tidak bocor**: `(*server).internalError` mencatat detail ke log dan klien hanya menerima `"internal server error"` (`TestInternalErrorHidesDetails`). `/readyz` juga tidak menampilkan penyebab.
- **Batas body**: `http.MaxBytesReader` 1 MiB di `decodeJSON`.
- **Timeout server**: `ReadHeaderTimeout`, `ReadTimeout`, `WriteTimeout`, `IdleTimeout` di `cmd/api/main.go` mencegah serangan *slowloris*.

## Apa yang terjadi kalau fitur ini dihapus?

- **Tanpa API key**: siapa pun bisa enqueue, membaca payload, me-retry, dan menghapus job.
- **Dengan `==` biasa**: test fungsional **tetap lolos**, karena timing attack tidak terlihat oleh test yang hanya mengecek benar/salah. Kebocorannya hanya terlihat lewat pengukuran waktu (Eksperimen C).
- **Tanpa CrossOriginProtection**: situs mana pun bisa me-retry atau menghapus job lewat browser operator yang sedang login.

## Trade-off dan alternatif

| Pilihan ForgeQ | Untung | Rugi / alternatif |
|---|---|---|
| Satu shared key | Sangat sederhana | Tidak ada identitas per user, tidak ada peran (read-only vs admin), dan rotasi key berarti semua klien diganti sekaligus. Alternatif: beberapa key per klien, OAuth/OIDC, mTLS. |
| Basic auth untuk dashboard | Tanpa halaman login atau sesi | Tidak ada logout yang sebenarnya; kredensial dikirim di setiap request (wajib HTTPS di produksi) |
| `Sec-Fetch-Site`/`Origin` | Tanpa token, tanpa state | Bergantung pada browser modern. Browser yang sangat tua tanpa header ini akan lolos (Go menerima request tanpa kedua header sebagai bukan-browser). |
| `/metrics` terbuka | Prometheus mudah men-scrape | Lindungi lewat jaringan (network policy) |

## Eksperimen

Branch: `git switch -c coba/keamanan`.

### Eksperimen A: hapus CSRF protection

Di akhir `dashboard.New`:

```diff
-	return http.NewCrossOriginProtection().Handler(r), nil
+	return r, nil
```

```sh
go test -count=1 -v -run '^TestCrossSiteRequestsAreRejected$' ./internal/dashboard/
```

**Hasil yang diharapkan:**

```
dashboard_test.go:208: cross-site retry = 200, want 403
dashboard_test.go:211: cross-site request changed the job
dashboard_test.go:215: same-origin retry = 409, want 200
--- FAIL: TestCrossSiteRequestsAreRejected
```

Baris ketiga membuktikan serangannya berhasil. Request "jahat" sudah me-retry job itu, sehingga request sah dari operator mendapat 409 karena job-nya sudah bukan `dead`.

### Eksperimen B: lupa memasang API key di `/api/v1`

Di `api.NewRouter`, hapus baris `r.Use(requireAPIKey(o.APIKey))`.

```sh
go test -count=1 -v -run '^TestAPIKeyProtectsAllAPIRoutes$' ./internal/api/
```

**Hasil yang diharapkan** (dipotong):

```
api_test.go:372: POST /api/v1/jobs with token "" = 201, want 401
api_test.go:372: GET /api/v1/jobs with token "nope" = 200, want 401
api_test.go:372: POST /api/v1/jobs/1/retry with token "" = 200, want 401
...
--- FAIL: TestAPIKeyProtectsAllAPIRoutes
```

### Eksperimen C: `==` biasa, dan kenapa test tidak menangkapnya

Ganti isi `auth.Equal`. Baris `_ = ...` menjaga import tetap terpakai supaya kode masih bisa di-compile:

```diff
-	g := sha256.Sum256([]byte(given))
-	w := sha256.Sum256([]byte(want))
-	return subtle.ConstantTimeCompare(g[:], w[:]) == 1
+	_, _ = sha256.Sum256, subtle.ConstantTimeCompare
+	return given == want
```

```sh
go test -count=1 -v ./internal/auth/
```

**Hasil yang diharapkan:** semua test **PASS**. Tidak ada test fungsional yang bisa melihat kebocoran waktu.

Untuk **melihat** kebocorannya, kembalikan `Equal` (`git restore internal/auth/auth.go`), lalu tambahkan benchmark sementara `internal/auth/timing_test.go`:

```go
package auth

import (
	"strings"
	"testing"
)

func BenchmarkCompare(b *testing.B) {
	want := strings.Repeat("a", 1<<16)          // key 64 KiB supaya efeknya mudah terlihat
	awal := "b" + want[1:]                      // beda di byte pertama
	akhir := want[:len(want)-1] + "b"           // beda di byte terakhir
	plain := func(g, w string) bool { return g == w }
	for _, f := range []struct {
		name string
		fn   func(string, string) bool
	}{{"plain", plain}, {"Equal", Equal}} {
		b.Run(f.name+"/beda-di-awal", func(b *testing.B) {
			for b.Loop() {
				f.fn(awal, want)
			}
		})
		b.Run(f.name+"/beda-di-akhir", func(b *testing.B) {
			for b.Loop() {
				f.fn(akhir, want)
			}
		})
	}
}
```

```sh
go test -run '^$' -bench BenchmarkCompare -benchtime 20000x -count 3 ./internal/auth/
```

**Hasil yang diharapkan** (output asli, satu dari tiga putaran):

```
BenchmarkCompare/plain/beda-di-awal-24     20000        12.88 ns/op
BenchmarkCompare/plain/beda-di-akhir-24    20000      1393    ns/op
BenchmarkCompare/Equal/beda-di-awal-24     20000    126866    ns/op
BenchmarkCompare/Equal/beda-di-akhir-24    20000    169891    ns/op
```

- `==` jelas bocor: ~12 ns kalau beda di awal, ~1.350 ns kalau beda di akhir. Waktu itu memberi tahu penyerang **seberapa panjang prefiks yang benar**.
- `Equal` tidak menunjukkan pola menurut posisi. Angkanya didominasi biaya SHA-256 atas 64 KiB dan bervariasi antar-putaran (127–175 µs untuk "awal", 170–172 µs untuk "akhir"), tetapi variasi itu adalah noise mesin, bukan fungsi dari isi input.
- Dengan key berukuran normal (32–64 byte), selisih `==` hanya beberapa nanodetik. Itu sulit tetapi bukan mustahil diukur lewat jaringan dengan banyak sampel. Constant-time compare menutup celah itu dengan biaya hampir nol.

Bersihkan: `git restore . && rm -f internal/auth/timing_test.go && git switch main && git branch -D coba/keamanan`.

## Latihan interview

1. Kenapa membandingkan secret dengan `==` berbahaya?
2. Kenapa `auth.Equal` meng-hash kedua sisi sebelum `ConstantTimeCompare`?
3. Kenapa dashboard butuh proteksi CSRF, sedangkan `/api/v1` tidak?
4. Bagaimana `http.CrossOriginProtection` mendeteksi request lintas-situs tanpa token?
5. Kenapa `/healthz`, `/readyz`, dan `/metrics` tidak dilindungi API key? Apa risikonya?

### Jawaban

1. `==` berhenti di byte pertama yang berbeda, sehingga waktu eksekusinya bergantung pada panjang prefiks yang cocok. Penyerang bisa menebak secret byte demi byte dengan mengukur waktu respons dari banyak percobaan (timing attack). Eksperimen C menunjukkan selisih ~100× pada key panjang.
2. `subtle.ConstantTimeCompare` langsung mengembalikan 0 kalau panjang kedua input berbeda, sehingga panjang key bocor. Setelah di-hash SHA-256, kedua sisi selalu 32 byte, perbandingan selalu memeriksa 32 byte penuh, dan panjang asli tidak terlihat.
3. Browser **otomatis** melampirkan kredensial basic auth (dan cookie) ke request ke origin yang sama, **termasuk request yang dipicu situs lain**. Header `Authorization: Bearer` untuk API harus dipasang secara eksplisit oleh kode klien, dan situs jahat tidak bisa memaksa browser korban menambahkannya, sehingga CSRF tidak berlaku untuk API.
4. Browser modern (semua sejak 2023) mengirim `Sec-Fetch-Site` (`same-origin`, `same-site`, `cross-site`, `none`), dan JavaScript halaman tidak bisa memalsukannya. Untuk metode non-safe, Go hanya menerima `same-origin` dan `none` (yang terakhir artinya diketik pengguna sendiri). Kalau header itu tidak ada, Go membandingkan host di `Origin` dengan `Host`. Request tanpa kedua header dianggap bukan dari browser (misalnya curl) dan diizinkan, karena alat seperti itu tidak membawa kredensial korban secara otomatis.
5. Probe dipanggil orkestrator (Docker, Kubernetes) dan `/metrics` di-scrape Prometheus. Memberi mereka secret menambah kerumitan, dan isinya tidak sensitif (status, angka). Risikonya: `/metrics` membocorkan nama queue dan volume trafik. Di produksi, endpoint ini sebaiknya hanya bisa dijangkau dari jaringan internal.
