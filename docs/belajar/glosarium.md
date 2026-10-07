# Glosarium

Istilah diurutkan secara alfabetis. Kolom "Di ForgeQ" menunjuk ke tempat istilah itu muncul di kode.

| Istilah | Arti | Di ForgeQ |
|---|---|---|
| **Advisory lock** | Lock buatan aplikasi di PostgreSQL (`pg_advisory_lock`) yang tidak terikat ke baris tabel. Biasa dipakai untuk *leader election*. | **Tidak** dipakai. Reaper aman berjalan di semua proses berkat `SKIP LOCKED` ([09](09-reaper-retensi.md)). |
| **At-least-once** | Jaminan pengiriman: setiap job pasti dijalankan **minimal** sekali, tetapi bisa lebih dari sekali. | Jaminan ForgeQ. Dibuktikan oleh `TestStalledWorkerCausesDuplicateExecution` ([06](06-at-least-once-idempotency.md)). |
| **At-most-once** | Kebalikannya: maksimal sekali, tetapi bisa hilang. | Tidak dipakai. Kehilangan job dianggap lebih buruk daripada duplikat. |
| **Attempt** | Satu kali percobaan menjalankan job. Kolom `attempts` naik **saat claim**. | `claimQuery` di `internal/store/store.go`. |
| **Backoff** | Jeda yang makin panjang di antara percobaan ulang. | `backoff.Policy.Delay` ([04](04-backoff-jitter.md)). |
| **Basic auth** | Skema HTTP `Authorization: Basic base64(user:pass)`. Browser menampilkan dialog login untuknya. | Melindungi dashboard; password = `API_KEY` ([11](11-keamanan.md)). |
| **Bearer token** | Skema HTTP `Authorization: Bearer <token>`. | Melindungi `/api/v1` ([11](11-keamanan.md)). |
| **Cardinality (kardinalitas)** | Jumlah kombinasi nilai label unik pada sebuah metrik. Setiap kombinasi adalah satu *time series* di Prometheus. | Dijaga dengan label `unknown` dan pola rute ([10](10-observability.md)). |
| **Claim** | Mengambil job dari antrean dan menandainya sebagai milik worker tertentu secara atomik. | `(*Store).Claim` ([02](02-skip-locked-dan-claim.md)). |
| **Clock skew** | Selisih jam antara dua mesin (misalnya proses Go dan server PostgreSQL). | [13](13-clock-skew.md). |
| **Coalescing** | Menggabungkan banyak permintaan sejenis menjadi satu. | `Notifier` menggabungkan wake-up per queue per 50 ms; channel `wake` ber-buffer 1 ([07](07-listen-notify.md)). |
| **Constant-time compare** | Perbandingan yang waktunya tidak bergantung pada letak byte pertama yang berbeda, sehingga penyerang tidak bisa menebak secret dari waktu respons. | `auth.Equal` ([11](11-keamanan.md)). |
| **Context cause** | Alasan pembatalan `context` di Go (`context.WithCancelCause`, `context.Cause`). | Membedakan `store.ErrLeaseLost`, `worker.ErrShutdown`, dan timeout. |
| **CSRF** | *Cross-Site Request Forgery*: situs jahat membuat browser korban mengirim request ke situs lain lengkap dengan kredensialnya. | Dicegah dengan `http.CrossOriginProtection` ([11](11-keamanan.md)). |
| **CTE** | *Common Table Expression*, yaitu klausa `WITH ... AS (...)` di SQL. | `claimQuery` dan `reapQuery`. |
| **Dead-letter queue (DLQ)** | Tempat job yang menyerah, menunggu tindakan manusia. | `status = 'dead'` di tabel yang sama ([05](05-dead-letter-queue.md)). |
| **Delayed job** | Job dengan `run_at` di masa depan (`scheduled_at` di API). | Ditemukan lewat polling, tanpa NOTIFY. |
| **Durable** | Tersimpan permanen, tetap ada meski proses mati. Di PostgreSQL berarti transaksi sudah commit dan WAL sudah di-flush. | Arti `201 Created` dari API. |
| **Exactly-once** | Jaminan tepat sekali. Untuk efek samping ke sistem luar, praktis mustahil tanpa kerja sama sistem tersebut. | **Tidak** diklaim ForgeQ. |
| **Fencing** | Setiap tulisan worker menyertakan bukti kepemilikan (`locked_by = $worker`), sehingga pemilik lama yang sudah kehilangan hak ditolak. | `Heartbeat`, `Complete`, `Fail`, `Release` ([03](03-lease-heartbeat-fencing.md)). |
| **Fetch loop** | Loop yang mengambil job untuk satu queue. | `(*Worker).runQueue`. |
| **Graceful shutdown** | Berhenti dengan rapi: menyelesaikan atau mengembalikan pekerjaan yang sedang berjalan. | `(*Worker).Run` ([08](08-graceful-shutdown.md)). |
| **Handler** | Fungsi yang mengerjakan satu tipe job. | `worker.HandlerFunc`, didaftarkan lewat `Registry.RegisterHandler`. |
| **Heartbeat** | Sinyal "saya masih hidup" berkala yang memperpanjang lease. | `(*Worker).heartbeat` → `(*Store).Heartbeat`. |
| **Idempotent** | Menjalankan operasi berkali-kali memberi hasil yang sama dengan menjalankannya sekali. | Syarat untuk handler; enqueue dengan `idempotency_key` ([06](06-at-least-once-idempotency.md)). |
| **Idempotency key** | Kunci unik dari klien yang membuat request berulang tidak menghasilkan job ganda. | Kolom `idempotency_key`, indeks unik parsial `jobs_idempotency_idx`. |
| **Jitter** | Tambahan acak pada jeda retry supaya job yang gagal bersamaan tidak retry bersamaan. | `Policy.Jitter`. |
| **Keyset pagination** | Paginasi dengan "id < kursor terakhir" alih-alih `OFFSET`, sehingga tetap cepat di halaman jauh. | `ListFilter.Cursor`, `next_cursor`. |
| **Lease** | Hak sementara atas sebuah job: "milik worker X sampai jam T". | Kolom `locked_by` dan `locked_until` ([03](03-lease-heartbeat-fencing.md)). |
| **LISTEN/NOTIFY** | Mekanisme pub/sub bawaan PostgreSQL. | Kanal `forgeq_jobs` ([07](07-listen-notify.md)). |
| **Liveness probe** | Pemeriksaan "apakah proses masih hidup". Jika gagal, orkestrator me-restart proses. | `/healthz` ([10](10-observability.md)). |
| **Partial index** | Indeks dengan klausa `WHERE`, sehingga hanya sebagian baris yang diindeks. | `jobs_claimable_idx`, `jobs_lease_idx`, `jobs_idempotency_idx`, `jobs_succeeded_finished_idx`. |
| **Permanent error** | Error yang tidak akan sembuh dengan retry (misalnya payload tidak valid). | `worker.Permanent(err)` ([05](05-dead-letter-queue.md)). |
| **Poison job** | Job yang selalu gagal atau selalu membuat worker crash. | Dibatasi oleh `max_attempts` yang dihitung saat claim. |
| **Polling** | Bertanya berkala "ada job?". | `POLL_INTERVAL`, timer di `runQueue`. |
| **Reaper** | Proses latar yang mengambil kembali job dengan lease kedaluwarsa. | `worker.Reaper` ([09](09-reaper-retensi.md)). |
| **Readiness probe** | Pemeriksaan "apakah proses siap menerima kerja" (misalnya DB bisa dihubungi). Jika gagal, proses dikeluarkan dari load balancer tetapi tidak di-restart. | `/readyz`. |
| **Release** | Mengembalikan job yang sedang berjalan ke `pending` tanpa memakan attempt (saat shutdown). | `(*Store).Release`. |
| **Retention (retensi)** | Berapa lama data disimpan sebelum dihapus. | `JOB_RETENTION`, `DeleteSucceededBefore`. |
| **Run** | "Putaran" job: naik setiap kali job dead di-retry manual. | Kolom `run`, migrasi 00003. |
| **Semaphore** | Penghitung yang membatasi berapa banyak pekerjaan berjalan bersamaan. | Channel `slots` di `runQueue`. |
| **Sentinel error** | Nilai error tetap yang dibandingkan dengan `errors.Is`. | `store.ErrNotFound`, `ErrLeaseLost`, `ErrInvalidState`, `worker.ErrShutdown`. |
| **SKIP LOCKED** | Opsi `SELECT ... FOR UPDATE` yang melewati baris yang sedang dikunci transaksi lain alih-alih menunggu. | `claimQuery`, `reapQuery`, retensi ([02](02-skip-locked-dan-claim.md)). |
| **Source of truth** | Satu-satunya tempat data dianggap benar. | PostgreSQL. |
| **Thundering herd** | Banyak klien menyerbu sumber daya yang sama pada saat yang sama. | Dicegah jitter ([04](04-backoff-jitter.md)). |
| **Time series** | Satu deret nilai metrik untuk satu kombinasi label. | Lihat *cardinality*. |
| **Transaction / commit** | Sekelompok perubahan yang berlaku semua atau tidak sama sekali. Commit membuatnya permanen. | `pgx.BeginFunc` di `Fail`. |
| **WAL** | *Write-Ahead Log*: log PostgreSQL yang ditulis (dan di-fsync) saat commit. | Alasan biaya commit ~3,8 ms di laptop ini. |
| **Wake-up** | Sinyal ke worker bahwa ada job baru. | `Notifier` → `Listen` → `(*Worker).Notify`. |
| **Worker** | Proses yang mengambil dan menjalankan job. | `cmd/worker`, `internal/worker`. |
