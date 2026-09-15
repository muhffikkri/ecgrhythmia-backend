# ECG Rhythmia - Sinkronisasi & Integrasi Frontend

**Versi rilis saat ini: `v1.1.0`**

Dokumentasi ini berfokus pada integrasi sisi **Frontend (React)** untuk memvisualisasikan data Elektrokardiogram (EKG) secara _real-time_, serta bagaimana frontend melakukan sinkronisasi dengan backend.

> 📄 Riwayat perubahan teknis backend tersedia di **[CHANGELOG.md](./CHANGELOG.md)**.

## 💻 Integrasi Frontend (React)

Aplikasi frontend (React/TypeScript) bertanggung jawab untuk dua fungsi utama: memvisualisasikan _streaming_ data EKG yang dikirim oleh backend dan memuat daftar dataset (records) yang tersedia.

### 1. Komunikasi WebSocket (Streaming Real-Time)

- **Koneksi:** Frontend terhubung ke server WebSocket backend pada alamat `ws://127.0.0.1:8080`.
- **Format Data:** Data diterima dalam format JSON. Struktur data (Payload) dari backend dirancang agar 100% sejajar dengan antarmuka TypeScript di sisi frontend (misal: `ecgTypes.ts`), khususnya pada objek `RawECGData` (berisi properti array `time`, `ch1`, `ch2`, `ch3`).
- **Render Visual:** Data yang diterima sudah dalam bentuk **murni milivolt (mV)** sehingga frontend tidak perlu lagi melakukan perhitungan kalibrasi multiplier/gain (_zero-overhead render_). Komponen grafik pada React cukup me-render nilai mentah ini secara langsung ke dalam bentuk gelombang EKG.

### 2. Pengambilan Data Dataset (REST API)

- **Koneksi HTTP:** Menggunakan pustaka _fetch_ bawaan peramban atau Axios, frontend melakukan _request_ HTTP `GET` ke REST API backend di `http://127.0.0.1:8080/api/records`.
- **Fungsi:** Berguna untuk memuat dan menampilkan daftar ketersediaan file CSV dataset (seperti dari folder Chapman, PTB-XL, atau data simulasi Prosim) pada menu navigasi (sidebar/dropdown) di aplikasi React.
- **CORS Terintegrasi:** REST API sisi server telah dikonfigurasi untuk mengizinkan _Cross-Origin Resource Sharing (CORS)_ untuk domain produksi (`https://ecgrhythmia.cloud`, `https://www.ecgrhythmia.cloud`) dengan metode (`GET`, `POST`, `PUT`, `DELETE`, `OPTIONS`), header (`Content-Type`, `Authorization`, `Accept`), serta memperbolehkan pengiriman kredensial (_allow credentials_).

---

## 📂 Struktur Folder Frontend (`arrhythmia-detection-dashboard`)

Proyek antarmuka ini dibangun menggunakan **React**, **TypeScript**, **Vite**, dan **Tailwind CSS**. Arsitektur internal aplikasi (_Clean Architecture_) disusun agar kode lebih modular dan mudah dipelihara.

```text
c:\arrhythmia-detection-dashboard\
├── package.json               # Konfigurasi proyek, dependensi npm, dan script build/dev
├── vite.config.ts             # Konfigurasi Vite (bundler frontend)
├── tailwind.config.js         # Konfigurasi desain, warna, dan utilitas Tailwind CSS
├── postcss.config.js          # Pengaturan PostCSS untuk Tailwind
├── tsconfig.json              # Konfigurasi root TypeScript
├── index.html                 # Halaman utama aplikasi (Entry HTML)
├── public/                    # Aset statis yang tidak diproses oleh bundler
└── src/                       # Kode sumber (*source code*) utama React
    ├── main.tsx               # Titik awal masuk (Entry point) React (mounting ke index.html)
    ├── App.tsx                # Komponen root aplikasi
    ├── App.css / index.css    # Gaya global Tailwind
    ├── application/           # Lapisan Application (Use cases, custom hooks, state management)
    ├── core/                  # Lapisan Core (Tipe data, interface TypeScript, konfigurasi)
    ├── data/                  # Lapisan Data (Akses API eksternal, klien WebSocket)
    ├── presentation/          # Lapisan Presentation (Komponen UI React, layout, halaman)
    ├── workers/               # (Opsional) Web Workers untuk komputasi asinkron (multithreading UI)
    └── assets/                # Aset proyek lokal (gambar, ikon SVG, dsb)
```

## ⚙️ Cara Setup & Menjalankan Backend (Rust - Production-Ready)

Backend aplikasi ini dibangun menggunakan **Rust** dengan framework web asinkron **Axum**, connection pooling **sqlx** ke **PostgreSQL / Supabase**, dan logging terstruktur menggunakan **tracing**.

### Persyaratan (Prerequisites)

- **Rust & Cargo**: Instal Rust melalui [rustup.rs](https://rustup.rs/).
- **PostgreSQL**: Server PostgreSQL aktif (lokal atau Supabase) untuk runtime.
- **PostgreSQL Client (`psql`)**: Diperlukan untuk migrasi skema manual/skrip.

### Langkah-langkah Instalasi & Konfigurasi

1. **Navigasi ke Direktori**:
   ```bash
   cd \ecgrhythmia-backend
   ```
2. **Konfigurasi Berkas `.env`**:
   Buat berkas `.env` di root direktori backend Anda (contoh lengkap di `.env.example`):

   ```env
   HOST_IP=127.0.0.1
   REST_PORT=8080
   WS_PORT=8080

   # PostgreSQL / Supabase (WAJIB - dipakai saat build & runtime)
   DATABASE_URL=postgres://user:password@host:port/postgres

   # Rahasia JWT Supabase (WAJIB - verifikasi token HS256)
   SUPABASE_JWT_SECRET=isi_dengan_jwt_secret_supabase_anda

   # Konfigurasi MQTT Broker (WAJIB)
   MQTT_BROKER=
   MQTT_PORT=8883
   MQTT_TOPIC=
   MQTT_USERNAME=
   MQTT_PASSWORD=
   ```

   _Catatan:_ `DATABASE_URL` yang sama juga dipakai oleh _macro sqlx saat kompilasi_. Jika hanya tersimpan di `.env`, ekspor ke environment sebelum build: `export DATABASE_URL=$(grep ^DATABASE_URL= .env | cut -d= -f2-)`, **atau** gunakan offline cache yang sudah di-commit di `.sqlx/`: `SQLX_OFFLINE=true cargo build --release`.

   _Catatan port:_ Jika `REST_PORT` dan `WS_PORT` disamakan (contoh di atas: `8080`), server Axum menyatu pada satu port — REST API di `/api`, WebSocket di `/` dan `/ws`. Ini mode yang **direkomendasikan untuk produksi/proxy**. Bila memisahkan, REST berjalan di `REST_PORT` dan WebSocket di `WS_PORT`.

3. **Build & Run**:

```bash
cargo run
```

_Cargo akan mengunduh dependensi (crates), melakukan kompilasi asinkron, menjalankan migrasi database otomatis, dan menyalakan server._

### 4. Pengujian & Otomatisasi Rilis (Testing & Build Automation)

Aplikasi ini dilengkapi dengan pengujian unit dan pengujian integrasi yang komprehensif untuk menjamin stabilitas sistem sebelum dilakukan kompilasi rilis produksi (build) dan deployment.

#### A. Kategori Pengujian

1. **Unit Tests (Pengujian Unit):**
   - **Config Loader (`src/config.rs`):** Memvalidasi pembacaan berkas `.env` dan fallback nilai default jika variabel tidak tersedia.
   - **Device Parser (`src/models/device.rs`):** Memverifikasi parsing dan pemetaan JSON payload dari perangkat keras.
   - **CSV Reader (`src/data/csv_reader.rs`):** Memverifikasi pembacaan dataset EKG statis dan penanganan data kosong atau tidak valid (fallback).

2. **Integration Tests (Pengujian Integrasi - `tests/integration_tests.rs`):**
   - **REST API Integration:** Menyosialisasikan pemanggilan REST API di memori (registrasi, login, dll.) tanpa harus mem-bind socket port riil menggunakan `tower::Service`.
   - **Database Worker Integration:** Menguji antrean asinkron background writer database worker untuk mencatat sesi secara persisten ke `frame_records` dan membuat file JSONL rekaman di `records/`.
   - **ECG Pacer Integration:** Memverifikasi pembagian data (slicing) signal EKG dan broadcast via WebSocket klien.
   - **Auth Guard:** Verifikasi bahwa endpoint terproteksi (mis. `/api/admin/*`, `/api/sessions`) menolak request tanpa `Authorization: Bearer <token>` yang sah.

   > Integration test memerlukan **PostgreSQL aktif** — set `DATABASE_URL` mengarah ke
   > database PostgreSQL (lokal/Supabase) sebelum menjalankan `cargo test`.

#### B. Menjalankan Pengujian Manual

> **PENTING — jangan build/migrate terhadap *pooler* Supabase:** macro `sqlx` (query!,
> query_as!) akan gagal koneksi-pooler dengan error `prepared statement "sqlx_s_*" already
> exists`. Gunakan salah satu:
> 1. **PostgreSQL langsung** (bukan pooler) sebagai `DATABASE_URL` saat `cargo check`/`test`, atau
> 2. **offline cache** yang sudah di-commit di folder `.sqlx/`:
>    `SQLX_OFFLINE=true cargo test` (tidak memerlukan koneksi DB saat kompilasi).

- **Di Windows (PowerShell):**
  ```powershell
  $env:SQLX_OFFLINE = "true"
  $env:DATABASE_URL  = "postgresql://postgres@127.0.0.1:5433/ecgdev?sslmode=disable"  # contoh: Postgres lokal
  cargo test --all-targets
  ```
- **Di Linux (Terminal):**
  ```bash
  export SQLX_OFFLINE=true
  export DATABASE_URL="postgresql://postgres@127.0.0.1:5432/ecgdev?sslmode=disable"
  cargo test --all-targets
  ```

  Kumpulan test lengkap juga tersedia sebagai skrip: **Windows** `./scripts/test-all.ps1`
  dan skema migrasi ulang PostgreSQL **`./scripts/migrate.ps1`**.

### C. Migrasi dan Test Terpadu (PostgreSQL)

Schema PostgreSQL tersedia sebagai migrasi versioned di `migrations/0001_initial.sql`
(skema dasar) dan `migrations/0002_postgres_evolution.sql` (evolusi idempoten).
Pastikan `DATABASE_URL` berisi connection string PostgreSQL yang valid, lalu jalankan:

```powershell
./scripts/migrate.ps1
./scripts/test-all.ps1
```

> **PENTING — jangan build/migrate terhadap *pooler* Supabase:** macro `sqlx` (query!,
> query_as!) akan gagal koneksi-pooler dengan error `prepared statement "sqlx_s_*" already
> exists`. Gunakan salah satu:
> 1. **PostgreSQL langsung** (bukan pooler) sebagai `DATABASE_URL` saat `cargo check`/`test`, atau
> 2. **offline cache** yang sudah di-commit di folder `.sqlx/`:
>    `SQLX_OFFLINE=true cargo build --release` (tanpa memerlukan koneksi DB sama sekali).

Runner `scripts/test-all.ps1` menjalankan migrasi, pemeriksaan formatting, dan seluruh
target Rust. Gunakan `./scripts/test-all.ps1 -SkipMigration` hanya untuk mengisolasi
kegagalan compile/test. End-to-end test memerlukan PostgreSQL, MQTT broker, server
backend, dan client test; suite E2E otomatis belum tersedia di repository ini.

### D. Migrasi Database Lama (SQLite/SQLCipher) → PostgreSQL/Supabase

> **PENTING (deployment VPS):** Pastikan seluruh perubahan kode "refactor ke PostgreSQL"
> (termasuk `migrations/`, `scripts/`, `src/bin/`, build offline `.sqlx/`) sudah
> **di-commit dan di-push** ke cabang `main`, lalu lakukan `git pull` di VPS sebelum memulai.

#### Prasyarat di VPS (Linux / Debian)

Rust dipaketkan tidak tersedia langsung di Debian; gunakan `rustup`:
`curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh`

Selain itu, karena kebijakan **PEP 668** di Debian 12+ (`pip` system diblokir), install
dependensi Python migrasi lewat paket sistem — **jangan** `pip install` ke system:

```bash
sudo apt update && sudo apt install -y sqlcipher postgresql-client python3-psycopg2
```

#### Langkah Migrasi (satu per satu)

1. **Siapkan `.env` di VPS** (contoh: `.env.example`) — **WAJIB** berisi nilai **asli**:
   - `DATABASE_URL` (Supabase/PostgreSQL yang sudah ada / baru)
   - `SUPABASE_JWT_SECRET` (dari Supabase Dashboard → Settings → API → JWT Secret)
   - `MQTT_*` (kredensial broker lama/tetap)
   - `SQLITE_KEY` (kunci SQLCipher database lama, hanya dipakai saat migrasi data)

2. **Jalankan migrasi skema PostgreSQL** (membuat/menyelaraskan tabel tanpa menghapus data):

   ```bash
   export DATABASE_URL="$(grep ^DATABASE_URL= .env | cut -d= -f2- | tr -d '\"')"
   for f in migrations/*.sql; do psql "$DATABASE_URL" --set ON_ERROR_STOP=1 -f "$f"; done
   ```

3. **Migrasi data dari `database.db` (SQLCipher) ke PostgreSQL** — otomatis memetakan kolom
   (timestamp/text, date, umur pasien, bool) dan **idempoten** (upsert `ON CONFLICT`).
   Akun dengan email yang sudah ada di PostgreSQL (mis. akun admin bawaan) otomatis
   **digabung** (di-remap id-nya) sehingga tidak memicu
   `duplicate key value violates unique constraint "accounts_email_key"`:

   ```bash
   ./scripts/run_migration.sh database.db
   ```

   atau manual:
   ```bash
   python3 scripts/migrate_sqlite_to_postgres.py \
     --sqlite database.db --db-key "$(grep ^SQLITE_KEY= .env | cut -d= -f2- | tr -d '\"')" \
     --pg "$DATABASE_URL"
   ```

   Cek dulu tanpa menulis data (disarankan): tambahkan `--dry-run` pada perintah manual di atas.

4. **Salin berkas rekaman `records/*.jsonl`** dari server lama (data frame sinyal tidak
   tersimpan di database, hanya `file_path`-nya). Ganti `user@old-server` dengan alamat
   server lama yang sebenarnya:

   ```bash
   rsync -avz user@old-server:/path/ecgrhythmia-backend/records/ records/
   ```

5. **Verifikasi hasil**:

   ```bash
   psql "$DATABASE_URL" -c "\dt"
   psql "$DATABASE_URL" -c "SELECT (SELECT count(*) FROM accounts) a, (SELECT count(*) FROM patients) p, (SELECT count(*) FROM sessions) s, (SELECT count(*) FROM frame_records) f;"
   ```

   Bandingkan dengan daftar baris yang ditampilkan `run_migration.sh` — jumlahnya harus
   cocok dengan jumlah di database SQLite lama (lihat juga `--dry-run`).

6. **Build & jalankan server** (pakai offline cache — tidak perlu DB saat kompilasi):

   ```bash
   SQLX_OFFLINE=true cargo build --release
   ./target/release/ecg-backend
   ```

   Server otomatis menjalankan kembali `run_migrations` saat start. Migrasi
   in-process itu **best-effort** dan tidak fatal: di atas pooler Supabase DDL
   via extended/prepared protocol ditolak (`prepared statement "sqlx_s_*" already
   exists`), jadi statement yang gagal dicatat sebagai warning dan server tetap
   berjalan. **Skema produksi harus diprovisi lewat psql (simple protocol) pada
   langkah 2 di atas**, bukan bergantung pada `run_migrations` di startup.

> **Regenerasi offline cache `.sqlx/`**: install `cargo install sqlx-cli --no-default-features
> --features postgres,rustls --version 0.7.4`, hubungkan `DATABASE_URL` ke PostgreSQL **langsung**,
> lalu `cargo sqlx prepare --workspace -- --all-targets`. Commit hasil folder `.sqlx/`.
### E. Pengujian & Build Sebelum Rilis (Production)

Urutan yang digunakan untuk menjaga kualitas sebelum kompilasi rilis:

1. **Format & static analysis:**
   ```bash
   cargo fmt --all -- --check
   cargo clippy --all-targets
   ```
2. **Seluruh test (unit + integrasi)** terhadap PostgreSQL aktif:
   ```bash
   export SQLX_OFFLINE=true
   export DATABASE_URL="postgresql://postgres@127.0.0.1:5433/ecgdev?sslmode=disable"
   cargo test --all-targets
   ```
   Passed & failed langsung dilaporkan di konsol oleh Cargo.
3. **Build biner produksi teroptimasi** (`lto`, `strip`, `panic=abort` — lihat
   `[profile.release]` di `Cargo.toml`):
   ```bash
   SQLX_OFFLINE=true cargo build --release
   ```

- **Di Windows (PowerShell):** output di `target\release\ecg-backend.exe`.
- **Di Linux:** output di `target/release/ecg-backend`.

---

## 🗄️ Pemakaian Database (PostgreSQL / Supabase)

Aplikasi menggunakan **PostgreSQL (supaya kompatibel dengan Supabase)** sebagai penyimpanan data.

- **Fungsi Utama**: Menyimpan **Akun Pengguna**, **Profil Dokter & Pasien**, **Status Perangkat**, dan **Metadata Riwayat Sesi Medis** (`sessions`, `frame_records`, `accounts`, `doctors`, `patients`, `devices`).
- **Connection Pooling (`sqlx`)**: Akses database dikelola pool asinkron (`PgPool`) untuk memproses data paralel.
- **Skema Otomatis (Best-Effort)**: Saat server dijalankan, `db::postgres::run_migrations` mencoba `CREATE TABLE IF NOT EXISTS` + `ALTER TABLE ADD COLUMN IF NOT EXISTS`, namun **tidak fatal** bila gagal — mis. di atas pooler Supabase yang menolak DDL via prepared protocol (`prepared statement "sqlx_s_*" already exists`); statement gagal hanya dicatat sebagai warning dan server tetap berjalan. Skema versioned tersedia di `migrations/` (`0001_initial.sql`, `0002_postgres_evolution.sql`) dan **harus diprovisi lewat `psql`** (simple protocol) untuk produksi. Skema lama yang sudah berisi data aman di-*upgrade* (kolom baru ditambahkan, konstrain disesuaikan) tanpa menghapus data.
- **Data sinyal EKG**: Data frame mentah disimpan sebagai file `records/<session_id>.jsonl`; database hanya menyimpan `file_path` dan metadata frame.

---

## 🌐 Sinkronisasi dengan PWA (Frontend)

Backend didesain agar dapat tersinkronisasi mulus dengan aplikasi React (yang telah dikonfigurasi sebagai _Progressive Web App_ / PWA).

1. **Sinkronisasi Data Profil & Riwayat (REST API)**:
   Setiap kali pengguna melakukan pembaruan profil atau pengaturan perangkat di PWA, frontend mengirimkan _request_ HTTP (seperti `POST` atau `PUT`) ke `http://127.0.0.1:8080/api/...`. Backend PostgreSQL (Supabase) akan langsung menyimpan perubahan ini secara permanen.
2. **Komunikasi Real-Time (WebSocket)**:
   PWA mengandalkan koneksi persisten ke `ws://127.0.0.1:8080` untuk menerima aliran (_streaming_) grafik detak jantung EKG tanpa _overhead_ (hambatan) koneksi ulang HTTP biasa.
3. **Mekanisme Fallback (Mode Offline PWA)**:
   Jika backend terputus atau dimatikan, antarmuka PWA dilengkapi dengan _Local Storage Fallback_. PWA tetap dapat dioperasikan secara fungsional (untuk berpindah halaman, melihat riwayat _cache_, atau menyimpan profil tiruan) berkat fitur _Service Worker_ dan penyimpanan lokal, menjamin UX (Pengalaman Pengguna) yang tidak terputus.

---

## ⚙️ Cara Setup & Menjalankan Frontend

### Persyaratan (Prerequisites)

- **Node.js** (Rekomendasi versi LTS 18.x atau ke atas).
- Manajer paket seperti **npm** (biasanya terpasang otomatis bersama Node.js).

### Langkah-langkah Menjalankan

1. **Buka Terminal** baru, lalu arahkan navigasi ke direktori proyek frontend:
   ```bash
   cd c:\arrhythmia-detection-dashboard
   ```
2. **Instalasi Dependensi**. Jalankan perintah ini (hanya perlu dilakukan pertama kali atau jika ada penambahan pustaka baru):
   ```bash
   npm install
   ```
3. **Jalankan _Development Server_**:
   ```bash
   npm run dev
   ```
4. **Buka Aplikasi di Browser**.
   Secara _default_, Vite akan menjalankan aplikasi di `http://localhost:5173` (perhatikan log di terminal Anda untuk tautan spesifik). Buka tautan tersebut menggunakan peramban web favorit Anda.

### Perintah Tambahan (NPM Scripts)

- `npm run build`: Melakukan proses kompilasi TypeScript dan mem-_build_ aplikasi agar siap di-_deploy_ ke tahap produksi (berada di folder `dist/`).
- `npm run lint`: Memeriksa potensi kesalahan/standar kode dengan cepat (memanfaatkan `oxlint`).
- `npm run preview`: Membuka server lokal (_preview_) untuk melihat dan menguji _build_ versi produksi yang telah dikompilasi sebelumnya.

---

## 🔄 Mekanisme Streaming WebSocket (Backend Internal)

Dalam sistem ini, backend (Rust) memegang kendali penuh atas mekanisme pengaturan ritme pengiriman aliran data EKG (dari file CSV ke WebSocket) agar persis menyerupai alat fisik medis _real-time_.

1. **Chunking Data (Pemaketan):**
   Alih-alih mengirim titik koordinat satu per satu yang akan membuat jaringan kewalahan (karena _overhead_ WebSocket), backend memotong (chunk) aliran data dalam bentuk _batch_.
2. **Frekuensi Sampling (250Hz):**
   Sistem diatur pada asumsi frekuensi _sampling rate_ dasar 250Hz. Backend mengelompokkan secara spesifik **25 sampel data** menjadi satu _chunk_ paket transmisi.
3. **Delay Real-Time Presisi:**
   Dalam _sampling rate_ 250Hz, 25 sampel merepresentasikan durasi waktu tepat **100 milidetik (ms)**. Oleh karena itu, _thread_ pengiriman backend akan menerapkan sinkronisasi jeda waktu otomatis (_sleep_duration_) selama 100ms setiap kali selesai mengirimkan satu _chunk_ paket ke frontend.
4. **Aliran Tanpa Henti (Seamless Looping):**
   Skema ini menjamin kelancaran _streaming real-time_ yang sangat konsisten, setara dengan kecepatan sapuan standar perekaman di atas kertas termal EKG (25 mm/s). Ketika pointer pembacaan backend telah mencapai titik data terakhir pada file rekaman CSV, sistem akan otomatis mereset siklus dari titik nol (_looping_), mensimulasikan aliran detak jantung pasien yang terus menyala.
