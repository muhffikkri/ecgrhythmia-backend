-- =========================================================================
-- 0002_postgres_evolution.sql
-- Evolusi skema PostgreSQL yang IDEMPOTEN untuk database yang sudah ada
-- (aman dijalankan berulang kali; tidak menghapus data).
--
-- Konteks: versi kode sebelumnya (HEAD lama berbasis SQLite, atau refactor
-- awal menuju PostgreSQL) membuat tabel dengan kolom yang berbeda/missing.
-- Skrip ini menyelaraskan database target agar kompatibel penuh dengan kode
-- saat ini (accounts.password_hash, patients.date_of_birth/default age,
-- sessions.doctor_id/dev_note, frame_records kolom label/waktu, dsb).
--
-- Alternatif yang lebih sederhana: jalankan server / `migrate_db` sekali,
-- karena `db::postgres::run_migrations` juga menjalankan ALTER ADD COLUMN
-- IF NOT EXISTS. Skrip ini menambah perubahan constraint/default yang tidak
-- ditangani run_migrations (DROP NOT NULL, SET DEFAULT) + index.
-- =========================================================================

BEGIN;

-- ---------------------------------------------------------------
-- accounts
-- ---------------------------------------------------------------
ALTER TABLE accounts ADD COLUMN IF NOT EXISTS password_hash TEXT;
-- created_at harus NOT NULL supaya konsisten dengan macro/offline cache sqlx
-- (aman: akun yang ada tidak pernah menyimpan NULL di kolom ini).
ALTER TABLE accounts ALTER COLUMN created_at SET NOT NULL;

-- ---------------------------------------------------------------
-- patients
-- ---------------------------------------------------------------
ALTER TABLE patients ADD COLUMN IF NOT EXISTS date_of_birth DATE;
ALTER TABLE patients ALTER COLUMN age SET DEFAULT 0;
ALTER TABLE patients ALTER COLUMN age DROP NOT NULL;
ALTER TABLE patients ALTER COLUMN gender DROP NOT NULL;

-- ---------------------------------------------------------------
-- sessions
-- ---------------------------------------------------------------
ALTER TABLE sessions ADD COLUMN IF NOT EXISTS doctor_id TEXT REFERENCES doctors(id) ON DELETE SET NULL;
ALTER TABLE sessions ADD COLUMN IF NOT EXISTS dev_note TEXT;
ALTER TABLE sessions ADD COLUMN IF NOT EXISTS ecg_paper TEXT;
-- Kode baru mengizinkan sesi tanpa pasien (upload tanpa patient_id).
ALTER TABLE sessions ALTER COLUMN patient_id DROP NOT NULL;

-- ---------------------------------------------------------------
-- frame_records
-- ---------------------------------------------------------------
ALTER TABLE frame_records ADD COLUMN IF NOT EXISTS start_time DOUBLE PRECISION DEFAULT 0;
ALTER TABLE frame_records ADD COLUMN IF NOT EXISTS end_time DOUBLE PRECISION DEFAULT 0;
ALTER TABLE frame_records ADD COLUMN IF NOT EXISTS time_interval TEXT DEFAULT '';
ALTER TABLE frame_records ADD COLUMN IF NOT EXISTS label TEXT DEFAULT 'Normal';
ALTER TABLE frame_records ADD COLUMN IF NOT EXISTS dev_note TEXT;
ALTER TABLE frame_records ADD COLUMN IF NOT EXISTS doc_note TEXT;
ALTER TABLE frame_records ADD COLUMN IF NOT EXISTS confirmation BOOLEAN DEFAULT NULL;
ALTER TABLE frame_records ADD COLUMN IF NOT EXISTS doc_classification TEXT;
ALTER TABLE frame_records ADD COLUMN IF NOT EXISTS hidden BOOLEAN DEFAULT FALSE;
ALTER TABLE frame_records ADD COLUMN IF NOT EXISTS created_by TEXT REFERENCES accounts(id) ON DELETE SET NULL;
ALTER TABLE frame_records ADD COLUMN IF NOT EXISTS created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP;
-- Kode baru sering meng-INSERT tanpa start/end/label/time_interval.
ALTER TABLE frame_records ALTER COLUMN start_time SET DEFAULT 0;
ALTER TABLE frame_records ALTER COLUMN end_time SET DEFAULT 0;
ALTER TABLE frame_records ALTER COLUMN time_interval SET DEFAULT '';
ALTER TABLE frame_records ALTER COLUMN label SET DEFAULT 'Normal';
ALTER TABLE frame_records ALTER COLUMN start_time DROP NOT NULL;
ALTER TABLE frame_records ALTER COLUMN end_time DROP NOT NULL;
ALTER TABLE frame_records ALTER COLUMN time_interval DROP NOT NULL;
ALTER TABLE frame_records ALTER COLUMN label DROP NOT NULL;

-- ---------------------------------------------------------------
-- Index pendukung (amankan jika hilang pada tabel lama)
-- ---------------------------------------------------------------
CREATE INDEX IF NOT EXISTS idx_sessions_patient_started   ON sessions (patient_id, started_at DESC);
CREATE INDEX IF NOT EXISTS idx_sessions_device_active     ON sessions (device_id, ended_at, started_at DESC);
CREATE INDEX IF NOT EXISTS idx_frame_records_session      ON frame_records (session_id);

COMMIT;