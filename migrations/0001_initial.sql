BEGIN;

CREATE TABLE IF NOT EXISTS accounts (
    id TEXT PRIMARY KEY,
    email TEXT NOT NULL UNIQUE,
    password_hash TEXT,
    role TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    profile_photo TEXT,
    status TEXT NOT NULL DEFAULT 'Offline'
);

CREATE TABLE IF NOT EXISTS doctors (
    id TEXT PRIMARY KEY,
    account_id TEXT NOT NULL REFERENCES accounts(id) ON DELETE CASCADE,
    first_name TEXT NOT NULL,
    last_name TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS patients (
    id TEXT PRIMARY KEY,
    account_id TEXT REFERENCES accounts(id) ON DELETE CASCADE,
    first_name TEXT NOT NULL,
    last_name TEXT NOT NULL,
    date_of_birth DATE,
    age INTEGER NOT NULL DEFAULT 0,
    gender TEXT,
    primary_doctor_id TEXT REFERENCES doctors(id) ON DELETE SET NULL,
    device_id TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS devices (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL UNIQUE,
    mqtt_broker TEXT,
    mqtt_port INTEGER,
    mqtt_topic TEXT,
    mqtt_username TEXT,
    mqtt_password TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS sessions (
    id TEXT PRIMARY KEY,
    device_id TEXT NOT NULL REFERENCES devices(id) ON DELETE CASCADE,
    patient_id TEXT REFERENCES patients(id) ON DELETE CASCADE,
    doctor_id TEXT REFERENCES doctors(id) ON DELETE SET NULL,
    started_at TIMESTAMPTZ NOT NULL,
    ended_at TIMESTAMPTZ,
    file_path TEXT,
    ecg_paper TEXT,
    dev_note TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS frame_records (
    id TEXT PRIMARY KEY,
    session_id TEXT NOT NULL REFERENCES sessions(id) ON DELETE CASCADE,
    start_time DOUBLE PRECISION NOT NULL DEFAULT 0,
    end_time DOUBLE PRECISION NOT NULL DEFAULT 0,
    time_interval TEXT NOT NULL DEFAULT '',
    label TEXT NOT NULL DEFAULT 'Normal',
    dev_note TEXT,
    doc_note TEXT,
    confirmation BOOLEAN,
    doc_classification TEXT,
    hidden BOOLEAN NOT NULL DEFAULT FALSE,
    created_by TEXT REFERENCES accounts(id) ON DELETE SET NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_sessions_patient_started
    ON sessions (patient_id, started_at DESC);
CREATE INDEX IF NOT EXISTS idx_sessions_device_active
    ON sessions (device_id, ended_at, started_at DESC);
CREATE INDEX IF NOT EXISTS idx_frame_records_session
    ON frame_records (session_id);

COMMIT;