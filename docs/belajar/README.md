# Panduan belajar ForgeQ

Panduan berbahasa Indonesia untuk benar-benar **memahami** ForgeQ, bukan hanya memiliki kodenya. Ditulis untuk pembaca yang paham dasar Go dan SQL tetapi belum pernah membangun job queue.

## Mulai dari sini

1. [00-gambaran-besar.md](00-gambaran-besar.md): masalah yang diselesaikan, analogi, komponen, dan diagram
2. [01-perjalanan-satu-job.md](01-perjalanan-satu-job.md): satu job dari `curl` sampai `succeeded`, lalu gagal → retry → dead → retry manual
3. [urutan-membaca-kode.md](urutan-membaca-kode.md): urutan membaca file, dari yang paling sederhana

## Konsep (satu file per konsep)

| File | Konsep |
|---|---|
| [02-skip-locked-dan-claim.md](02-skip-locked-dan-claim.md) | `FOR UPDATE SKIP LOCKED` dan claim atomik |
| [03-lease-heartbeat-fencing.md](03-lease-heartbeat-fencing.md) | Lease, heartbeat, fencing |
| [04-backoff-jitter.md](04-backoff-jitter.md) | Exponential backoff dan jitter |
| [05-dead-letter-queue.md](05-dead-letter-queue.md) | Dead-letter queue, error permanen, retry manual |
| [06-at-least-once-idempotency.md](06-at-least-once-idempotency.md) | At-least-once dan idempotent enqueue |
| [07-listen-notify.md](07-listen-notify.md) | LISTEN/NOTIFY dan bottleneck trigger yang ditemukan lewat load test |
| [08-graceful-shutdown.md](08-graceful-shutdown.md) | Graceful shutdown dan release |
| [09-reaper-retensi.md](09-reaper-retensi.md) | Reaper dan retensi |
| [10-observability.md](10-observability.md) | Health probe, metrics, kardinalitas |
| [11-keamanan.md](11-keamanan.md) | API key, constant-time compare, CSRF |
| [12-anggaran-koneksi.md](12-anggaran-koneksi.md) | Anggaran koneksi database |
| [13-clock-skew.md](13-clock-skew.md) | Clock skew |

Setiap file konsep berisi: masalahnya (dengan skenario konkret), konsep umumnya, implementasi di ForgeQ, apa yang terjadi kalau fiturnya dihapus, trade-off dan alternatif, **eksperimen** untuk sengaja merusak fitur itu, dan latihan interview dengan jawaban di bagian bawah.

Istilah asing dijelaskan di [glosarium.md](glosarium.md).

## Tentang verifikasi

Panduan ini ditulis terhadap commit `cf760f4` (6 Oktober 2026). Saat ditulis:

- Setiap referensi file, fungsi, test, env var, dan metrik diperiksa otomatis terhadap kode.
- Setiap eksperimen dijalankan di branch sementara (`tmp/eksperimen-belajar`, sudah dihapus). Output yang dikutip di bagian "Hasil yang diharapkan" adalah output asli dari laptop pengembang (Windows 11, 24 core, Docker Desktop/WSL2). Angka waktu dan throughput akan berbeda di mesinmu, tetapi test yang gagal dan pesan errornya seharusnya sama.
- Eksperimen yang butuh stack hidup (load test trigger NOTIFY, demo psql, perintah `curl`, anggaran koneksi, clock skew) dijalankan terhadap `task up` dengan `.env` lokal (Postgres di 5433, API di 8081).

Kalau kode berubah, nomor baris di output test bisa bergeser. Nama test dan pesan errornya adalah pegangan yang lebih stabil.
