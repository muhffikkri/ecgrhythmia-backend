# Changelog

Format mengikuti [Keep a Changelog](https://keepachangelog.com/id-ID/1.1.0/) dan
versi mengikuti [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Diperbaiki

- `accounts.created_at` kini dijamin `NOT NULL` di semua lingkungan (migrasi
  `0002` + DDL inline `run_migrations`). Sebelumnya kolom itu nullable di
  database Supabase yang sudah ada, sehingga macro `sqlx::query!` yang
  menyiapkan query saat kompilasi mengetik kolom sebagai `Option` dan membuat
  `cargo build` di VPS gagal dengan `no method to_rfc3339 ... for Option`.
  Selalu pakai `SQLX_OFFLINE=true` pada build agar kompilasi konsisten memakai
  offline cache yang sudah di-commit di `.sqlx/`.
- `run_migrations` di startup kini **best-effort**: DDL yang dijalankan pada
  waktu server mulai tidak lagi mematikan proses bila gagal (sebelumnya panic
  `Database migration failed: prepared statement "sqlx_s_*" already exists`).
  Pooler Supabase tidak mendukung DDL via extended/prepared protocol, sehingga
  skema produksi diprovisi lewat psql/sqlx-cli (simple protocol):
  `psql "$DATABASE_URL" --set ON_ERROR_STOP=1 -f migrations/0002_postgres_evolution.sql`.
  Server tetap berjalan meski ada statement yang dilewati dan mencatat warning.

## [1.1.0] - 2026-09-14

### Ditambahkan

- Migrasi skema PostgreSQL versioned: `migrations/0001_initial.sql` (skema dasar)
  dan `migrations/0002_postgres_evolution.sql` (evolusi idempoten untuk database
  yang sudah ada — aman dijalankan berulang kali tanpa menghapus data).
- CLI migrasi data `scripts/migrate_sqlite_to_postgres.py` untuk memindahkan data
  dari database lama SQLite/SQLCipher ke PostgreSQL (upsert idempoten + konversi
  tipe otomatis), serta runner `scripts/run_migration.sh` khusus Linux/VPS.
- Binary `src/bin/migrate_db.rs` untuk menjalankan migrasi skema tanpa menyalakan
  server.
- Alat administrasi database di `src/bin/`: `check_db`, `check_sessions`,
  `cleanup_sessions`, `inspect_db`, `repair_sessions`.
- Offline cache sqlx (`.sqlx/`) sehingga build tidak lagi memerlukan koneksi
  database saat kompilasi: `SQLX_OFFLINE=true cargo build`.
- Berkas `CHANGELOG.md` serta panduan migrasi & build offline di `README.md`.
- `.env.example` diperbarui sesuai variabel yang benar-benar dipakai
  (`DATABASE_URL`, `SUPABASE_JWT_SECRET`, `MQTT_*`).

### Diubah

- Transisi penuh ke PostgreSQL/Supabase; `db::sync::sync_databases` dijadikan
  validasi konfigurasi (bukan lagi sinkronisasi SQLite legacy).
- Evolusi skema untuk database yang tersisa dari versi lama:
  - `accounts`: kolom `password_hash`.
  - `patients`: `date_of_birth DATE`, default `age`, kolom `gender` boleh kosong.
  - `sessions`: `doctor_id`, `dev_note`, `ecg_paper`, dan `patient_id` boleh
    `NULL` (sesi tanpa pasien).
  - `frame_records`: default/`DROPPED NOT NULL` pada `start_time`, `end_time`,
    `time_interval`, `label`; tambahan `dev_note`, `doc_note`, `doc_classification`,
    `hidden`, `created_by`.
- `db::postgres::create_pool` menggunakan `PgConnectOptions` dengan
  `statement_cache_capacity(0)` agar aman lewat pooler (PgBouncer/Supabase).
- Struktur modul API: `auth` diekspor dengan urutan konsisten setelah `routes`.
- Readme diperbarui: bagian database, panduan migrasi SQLite → PostgreSQL, dan
  catatan build offline.

### Keamanan

- `validate_jwt`/`create_jwt` sekarang melakukan verifikasi dan penandatanganan
  rill dengan algoritma **HS256** dan kunci `SUPABASE_JWT_SECRET` (sebelumnya
  menonaktifkan verifikasi sigature dan memakai kunci kosong, sehingga token
  dapat dipalsukan).

### Diperbaiki

- Daftar pengguna admin: konversi `created_at` yang memakai `.map()` pada nilai
  non-`Option` (`registered_at`).
- Skrip migrasi data kini menangani akun dengan **email yang sama tapi id
  berbeda** di PostgreSQL (mis. akun admin bawaan). Akun digabung ke id yang
  sudah ada dan seluruh referensi id di tabel anak (`doctors`, `patients`,
  `frame_records`) di-remap otomatis, sehingga migrasi tidak lagi gagal dengan
  `duplicate key value violates unique constraint "accounts_email_key"`.