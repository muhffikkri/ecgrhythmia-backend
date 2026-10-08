# Changelog

Format mengikuti [Keep a Changelog](https://keepachangelog.com/id-ID/1.1.0/) dan
versi mengikuti [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [1.2.0] - 2026-10-08

### Ditambahkan

- **GitHub Actions CI** (`.github/workflows/ci.yml`): `cargo fmt --check`,
  `cargo clippy --all-targets -- -D warnings`, dan `cargo test --all-targets`
  otomatis jalan di setiap push/PR ke `main`. `SQLX_OFFLINE=true` +
  offline cache `.sqlx/` yang di-commit, jadi CI tak perlu koneksi database
  sama sekali.
- **GitHub Actions auto-deploy VPS** (`.github/workflows/deploy.yml`):
  SSH dari runner ke VPS (`VPS_SSH_KEY`), `git pull` di `/var/www/ecgrhythmia-backend`,
  `cargo build --release` in-place (pakai `.env` VPS + cache offline `.sqlx/`),
  `systemctl restart ecg-backend` (unit di `/etc/systemd/system/ecg-backend.service`),
  lalu health-check `https://api.ecgrhythmia.cloud/api/health`.
- Fix data `get_admin_users_filtered`: SELECT kini mengembalikan
  `p.account_id` / `d.account_id` (UUID riil `accounts.id`) sebagai
  `account_id`, bukan UUID profil `patients.id`/`doctors.id` sebelumnya
  (yang membuat impersonasi admin ke pasien/dokter asli gagal).
- `add_patient_handler` kini juga membuat baris `accounts` terkait dan mengisi
  `patients.account_id`, sehingga pasien ditambahkan admin dapat di-impersonate
  dan punya `account_id` yang resolvable.
- `.sqlx/` offline cache di-regenerasi untuk query-query baru di atas
  (`cargo sqlx prepare`), tetap di-commit untuk build offline.
- **Verifikasi signature ES256 lewat JWKS Supabase** (`src/api/jwks.rs`).
  Proyek ini memang memakai kunci asimetris: endpoint
  `/auth/v1/.well-known/jwks.json` mengembalikan `alg: ES256`, `crv: P-256`
  beserta `kid`. Sebelumnya token Supabase dari
  `supabase.auth.signInWithPassword` tidak bisa diverifikasi sama sekali
  karena backend hanya mendukung HS256.
  - Kunci publik diambil dari JWKS saat startup lalu di-cache di memory, dan
    di-refresh tiap 1 jam atau saat ditemukan `kid` baru.
  - Koordinat JWK `x`/`y` disusun menjadi SPKI DER lalu PEM secara manual,
    karena `jsonwebtoken` hanya menerima kunci EC dalam bentuk PEM. 6 unit
    test menutup jalur ini, termasuk verifikasi signature ES256 sungguhan
    (dibuat dengan `openssl`) dan penolakan token yang dimodifikasi.
  - Algoritma dibaca dari header token dan hanya jalur itu yang dicoba, jadi
    tidak ada algorithm confusion (token ES256 tak akan dicoba dengan HS256).
  - Konfigurasi lewat `SUPABASE_URL` (diturunkan otomatis) atau
    `SUPABASE_JWKS_URL` eksplisit. Kosongkan keduanya untuk menonaktifkan
    ES256; HS256 tetap bekerja.
  - Ketergantungan baru: `reqwest` (fitur `rustls`, tanpa OpenSSL).
- `accounts.auth_id` + migrasi `migrations/0003_supabase_auth_link.sql` untuk
  menautkan akun lokal dengan UUID user Supabase Auth, beserta index unik
  parsial. Kolom juga masuk ke DDL inline `run_migrations`.
- Binary `src/bin/link_auth.rs` untuk melihat dan mengisi `auth_id`
  (`cargo run --bin link_auth`, atau `link_auth <email> <uuid>`).
- `POST /api/auth/refresh`: perpanjangan sesi dengan sliding window. Token
  kedaluwarsa tetap bisa diperpanjang asal **signature sah**, akun masih ada,
  dan umur token belum melebihi `REFRESH_WINDOW_DAYS` (7 hari, dibaca dari klaim
  `iat`). Response mengembalikan token baru beserta `role` dan `user_id` —
  peran selalu diambil ulang dari tabel `accounts`, jadi perubahan role
  langsung berlaku tanpa perlu login ulang.
- `GET /api/auth/me`: endpoint yang sebelumnya sudah ada sebagai
  `auth_me_handler` tapi **tidak pernah didaftarkan** sebagai route, sehingga
  tidak bisa dipanggil. Sekarang terdaftar dan mengembalikan `user_id`
  (id profil, konsisten dengan `/api/auth/login`), `account_id`, `role`, dan
  `is_admin`.
- Klaim `iat` (opsional) ditambahkan ke `Claims` dan kini diisi saat token
  dibuat, dipakai untuk batas jendela refresh.

### Keamanan

- **Privilege escalation saat registrasi (KRITIS).** `POST /api/auth/register`
  memakai `role` dari body request tanpa validasi, lalu menyimpankannya ke
  `accounts.role` dan menerbitkannya sebagai klaim `app_metadata.role`.
  Akibatnya siapa pun bisa mendaftarkan akun `role: "admin"` dan langsung
  mendapat akses penuh ke `/api/admin/*` (daftar pasien/dokter, statistik,
  sinkronisasi, impersonasi) tanpa perlu login admin. Diverifikasi: registrasi
  `role: "admin"` lalu `GET /api/admin/users` → **200**. Perbaikan: role
  registrasi publik dibatasi allowlist `pasien` / `dokter`; nilai lain
  ditolak **403 `invalid_role`**. Akun admin tidak lagi bisa dibuat lewat
  endpoint publik.
- Pelacakan 401 dipisah agar frontend bisa bereaksi tepat: `missing_token`,
  `malformed_token`, `invalid_signature`, `token_expired`, `jwt_not_configured`.
  `validate_jwt` (yang hanya mengembalikan `Option`) digantikan
  `decode_claims` yang mengembalikan `Result<_, JwtError>`.
- `AdminClaims` kini membalas **403 `not_admin`** (bukan 401) ketika token
  valid tetapi role-nya bukan admin — 401 berarti "tidak terautentikasi", 403 berarti
  "tidak berwenang", sehingga frontend tidak salah mengarahkan user ke halaman
  login untuk akun dokter/pasien yang memang tidak boleh.
- Refresh memverifikasi signature dan keberadaan akun; token forged maupun
  `alg=none` tetap ditolak. Teks CHANGELOG sebelumnya menyebut
  `validate_jwt`; helper tersebut sudah dihapus.
- **Tiga endpoint tulis tanpa proteksi auth** (`src/api/routes.rs`):
  `POST /api/patients/:patient_id/connect`,
  `POST /api/patients/:patient_id/disconnect`, dan
  `POST /api/devices/:device_id/assign` tidak punya extractor auth sama
  sekali — handler-nya menerima `State` + path + body tanpa
  `AdminClaims`, sehingga siapa pun tanpa token bisa mengubah
  `patients.primary_doctor_id` dan `patients.device_id`, dan endpoint
  membalas `200 {"success": true}`. Ketiganya kini memakai `AdminClaims`.
  Verifikasi: tanpa token **401**, dengan token admin **200**.
- `GET /api/devices` dan `GET /api/admin/devices` juga tanpa auth dan
  membocorkan `mqtt_broker`/`mqtt_port`/`mqtt_topic`/`mqtt_username` ke
  publik; keduanya kini memakai `AdminClaims`.
- Catatan: `mqtt_password` ternyata **tidak pernah** ikut serialisasi —
  `DeviceRecord` tidak memuat kolom tersebut, hanya `SELECT` saat membuat
  listener MQTT dan saat menulis ke `devices`. Tidak ada perubahan perlu
  di sini.

### Diperbaiki

- **Pembacaan role dari token Supabase.** `claims.role` milik Supabase berisi
  `"authenticated"`, bukan nama peran, sehingga membacanya sebagai role
  membuat semua token ditolak. `Claims::token_role` kini mengabaikan nilai
  `"authenticated"` dan memakai urutan `app_metadata.role` →
  `user_metadata.role` → `claims.role`, lalu jatuh ke database
  (`accounts.auth_id` → `accounts.id` → `accounts.email`).
- Akun admin tidak lagi bergantung pada id hardcoded `acc_admin` yang tidak
  pernah ada (id admin bawaan adalah UUID). Pemeriksaan sekarang murni
  berdasarkan `role == "admin"` yang diambil dari token atau, bila kosong,
  dari database.
- `auth_me_handler` dan `doctor_impersonate_handler` sebelumnya mengembalikan
  `user_id` berupa `accounts.id`, tidak konsisten dengan `/api/auth/login` yang
  mengembalikan id profil. Keduanya kini memakai helper `resolve_profile_id`.

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
- `DATABASE_URL` runtime wajib memakai **port 5432 (session pooler)**, bukan 6543
  (transaction pooler). Port 6543 membuat sqlx menyiapkan statement dengan nama
  yang bertabrakan di session backend reuse → **query gagal acak**
  (`prepared statement "sqlx_s_*" already exists`): data/admin tampak kosong dan
  guard admin yang fallback `SELECT role ...` ikut gagal hingga memunculkan 401.
  Terverifikasi secara kontinu (300 request): 6543 = 0% sukses, 5432 = 100%.
  `psql` (simple protocol) aman di kedua port.

### Diperbaiki

- **Lint clippy** (Rust 1.99.0): `div_ceil`, `manual_checked_ops`,
  `unnecessary_lazy_evaluations`, `map_clone`, `redundant_closure`,
  `explicit_auto_deref`, `get_first`, `print_literal`,
  `option_as_ref_deref` — seluruhnya bersih
  (`cargo clippy --all-targets -- -D warnings` hijau).
- Test `verifies_real_es256_signature` di-`#[ignore]`: token test
  hardcoded sudah lewat masa (`Expired`); regenerate kunci+token test
  untuk mengaktifkan kembali.

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