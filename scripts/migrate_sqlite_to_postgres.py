#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
migrate_sqlite_to_postgres.py

Migrasi data dari database lama SQLite (database.db, terenkripsi SQLCipher)
ke PostgreSQL/Supabase yang dipakai kode saat ini.

Alur:
  1. (Opsional) Jika database masih terenkripsi SQLCipher dan --db-key
     diberikan, skrip mendekripsi dulu ke file plaintext memakai CLI `sqlcipher`.
  2. Membaca tabel dari SQLite plaintext.
  3. Memetakan tipe/kolom ke skema PostgreSQL target (upsert ON CONFLICT).
  4. Menampilkan ringkasan jumlah baris.

Kebutuhan di VPS (Linux):
  apt install sqlcipher python3-pip postgresql-client
  pip install psycopg2-binary

Contoh:
  # 1) Database sudah plaintext (mis. sudah didekripsi manual):
  python3 scripts/migrate_sqlite_to_postgres.py \
      --sqlite plain.db --pg "$DATABASE_URL"

  # 2) Database masih terenkripsi (otomatis didekripsi ke folder workspace):
  python3 scripts/migrate_sqlite_to_postgres.py \
      --sqlite database.db --db-key "ISI_SQLITE_KEY" \
      --pg "$DATABASE_URL"

  # 3) Hanya menampilkan apa yang akan dimigrasi, tanpa menulis:
  python3 scripts/migrate_sqlite_to_postgres.py --sqlite plain.db --pg "$DATABASE_URL" --dry-run

CATATAN:
  - Setelah menjalankan skrip, SALIN folder records/*.jsonl dari server lama
    ke server baru agar file_path di sessions tetap bisa dibaca:
      rsync -avz user@old-server:/path/ecgrhythmia-backend/records/ records/
  - Jalankan migrasi skema dulu (0001 + 0002) sebelum/bersamaan dengan tool ini.
"""

import argparse
import shutil
import sqlite3
import subprocess
import sys
import tempfile
from datetime import date, datetime, timezone
from pathlib import Path

# --- Koneksi PostgreSQL (dukung psycopg2 dan psycopg v3) -------------------
def connect_pg(conn_str: str):
    try:
        import psycopg2  # type: ignore
        return psycopg2.connect(conn_str)
    except ImportError:
        try:
            import psycopg  # type: ignore
            return psycopg.connect(conn_str)
        except ImportError:
            sys.exit(
                "Library PostgreSQL tidak ditemukan. Jalankan: pip install psycopg2-binary"
            )


# --- Helpers konversi tipe -------------------------------------------------
def parse_ts(value):
    """SQLite TEXT timestamp -> datetime/None untuk kolom TIMESTAMPTZ."""
    if value is None:
        return None
    s = str(value).strip()
    if not s:
        return None
    if s.endswith("Z"):
        s = s[:-1] + "+00:00"
    if " " in s and "T" not in s:
        s = s.replace(" ", "T")
    try:
        return datetime.fromisoformat(s).astimezone(timezone.utc)
    except ValueError:
        return None


def parse_date(value):
    """SQLite TEXT 'YYYY-MM-DD' -> date/None untuk kolom DATE."""
    if value is None:
        return None
    s = str(value).strip()
    if not s:
        return None
    try:
        return date.fromisoformat(s[:10])
    except ValueError:
        return None


def calc_age(dob):
    """Hitung umur dari date_of_birth; None jika DOB tidak tersedia."""
    if not dob:
        return None
    today = date.today()
    return today.year - dob.year - ((today.month, today.day) < (dob.month, dob.day))


def to_bool(value):
    """SQLite INTEGER 1/0 -> Python bool/None."""
    if value is None:
        return None
    return bool(value)


# --- Dekripsi SQLCipher -----------------------------------------------
def decrypt_sqlcipher(src: Path, key: str, workspace: Path) -> Path:
    sqlcipher = shutil.which("sqlcipher")
    if not sqlcipher:
        sys.exit(
            "Perintah `sqlcipher` tidak ditemukan. Instal: apt install sqlcipher (Linux) "
            "atau unduh biner sqlcipher untuk Windows."
        )
    plain = workspace / "plain.db"
    if plain.exists():
        plain.unlink()
    escaped = key.replace("'", "''")
    sql = (
        f"PRAGMA key = '{escaped}'; "
        "PRAGMA cipher_memory_security = OFF; "
        f"ATTACH DATABASE '{str(plain).replace(chr(39), chr(39)+chr(39))}' AS plaintext KEY ''; "
        "SELECT sqlcipher_export('plaintext'); "
        "DETACH DATABASE plaintext;"
    )
    print(f"[decrypt] Mengekspor {src} -> {plain} memakai sqlcipher ...")
    proc = subprocess.run([sqlcipher, str(src), sql], capture_output=True, text=True)
    if proc.returncode != 0 or not plain.exists():
        print("[decrypt] stdout:", proc.stdout)
        print("[decrypt] stderr:", proc.stderr)
        sys.exit("Dekripsi SQLCipher gagal. Periksa SQLITE_KEY/.env.")
    print("[decrypt] Berhasil.")
    return plain


# --- Migrasi per tabel ------------------------------------------------
def migrate(conn_src, conn_pg, dry_run):
    cur_src = conn_src.cursor()
    total = {}

    def apply(sql, rows, label):
        if dry_run:
            total[label] = len(rows)
            return
        with conn_pg.cursor() as cur_pg:
            cur_pg.executemany(sql, rows)
        conn_pg.commit()
        total[label] = len(rows)

    # 1. accounts -------------------------------------------------------
    rows = [
        (
            r[0], r[1], r[2], r[3], parse_ts(r[4]), r[5], r[6] if len(r) > 6 else None,
        )
        for r in cur_src.execute(
            "SELECT id, email, password_hash, role, created_at, profile_photo, status FROM accounts"
        )
    ]
    apply(
        """INSERT INTO accounts (id, email, password_hash, role, created_at, profile_photo, status)
           VALUES (%s,%s,%s,%s,%s,%s,%s)
           ON CONFLICT (id) DO UPDATE SET
             email = EXCLUDED.email,
             password_hash = EXCLUDED.password_hash,
             role = EXCLUDED.role,
             created_at = EXCLUDED.created_at,
             profile_photo = EXCLUDED.profile_photo,
             status = EXCLUDED.status""",
        rows,
        "accounts",
    )

    # 2. doctors --------------------------------------------------------
    rows = [
        (r[0], r[1], r[2], r[3])
        for r in cur_src.execute(
            "SELECT id, account_id, first_name, last_name FROM doctors"
        )
    ]
    apply(
        """INSERT INTO doctors (id, account_id, first_name, last_name, created_at)
           VALUES (%s,%s,%s,%s, NOW())
           ON CONFLICT (id) DO UPDATE SET
             account_id = EXCLUDED.account_id,
             first_name = EXCLUDED.first_name,
             last_name = EXCLUDED.last_name""",
        rows,
        "doctors",
    )

    # 3. patients -------------------------------------------------------
    rows_pat = []
    for r in cur_src.execute(
        "SELECT id, account_id, primary_doctor_id, first_name, last_name, date_of_birth, gender, device_id FROM patients"
    ):
        dob = parse_date(r[5])
        rows_pat.append(
            (r[0], r[1], r[2], r[3], r[4], dob, calc_age(dob), r[6], r[7])
        )
    apply(
        """INSERT INTO patients (id, account_id, primary_doctor_id, first_name, last_name, date_of_birth, age, gender, device_id, created_at)
           VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s, NOW())
           ON CONFLICT (id) DO UPDATE SET
             account_id = EXCLUDED.account_id,
             primary_doctor_id = EXCLUDED.primary_doctor_id,
             first_name = EXCLUDED.first_name,
             last_name = EXCLUDED.last_name,
             date_of_birth = EXCLUDED.date_of_birth,
             age = EXCLUDED.age,
             gender = EXCLUDED.gender,
             device_id = EXCLUDED.device_id""",
        rows_pat,
        "patients",
    )

    # 4. devices --------------------------------------------------------
    cols = ["id", "name", "mqtt_broker", "mqtt_port", "mqtt_topic", "mqtt_username", "mqtt_password"]
    sel = "SELECT " + ", ".join(cols) + " FROM devices"
    rows = [tuple(r) for r in cur_src.execute(sel)]
    apply(
        """INSERT INTO devices (id, name, mqtt_broker, mqtt_port, mqtt_topic, mqtt_username, mqtt_password, created_at)
           VALUES (%s,%s,%s,%s,%s,%s,%s, NOW())
           ON CONFLICT (id) DO UPDATE SET
             name = EXCLUDED.name,
             mqtt_broker = EXCLUDED.mqtt_broker,
             mqtt_port = EXCLUDED.mqtt_port,
             mqtt_topic = EXCLUDED.mqtt_topic,
             mqtt_username = EXCLUDED.mqtt_username,
             mqtt_password = EXCLUDED.mqtt_password""",
        rows,
        "devices",
    )

    # 5. sessions -------------------------------------------------------
    rows = []
    for r in cur_src.execute(
        "SELECT id, device_id, patient_id, started_at, ended_at, file_path FROM sessions"
    ):
        rows.append((r[0], r[1], r[2], parse_ts(r[3]), parse_ts(r[4]), r[5]))
    apply(
        """INSERT INTO sessions (id, device_id, patient_id, started_at, ended_at, file_path, created_at)
           VALUES (%s,%s,%s,%s,%s,%s, NOW())
           ON CONFLICT (id) DO UPDATE SET
             device_id = EXCLUDED.device_id,
             patient_id = EXCLUDED.patient_id,
             started_at = EXCLUDED.started_at,
             ended_at = EXCLUDED.ended_at,
             file_path = EXCLUDED.file_path""",
        rows,
        "sessions",
    )

    # 6. frame_records --------------------------------------------------
    cols_fr = [
        "id", "session_id", "time_interval", "confirmation", "doc_classification",
    ]
    # Cari kolom tambahan yang mungkin ada di skema SQLite lama
    cur_src.execute("PRAGMA table_info(frame_records)")
    avail = {row[1] for row in cur_src.fetchall()}
    extra = {"start_time", "end_time", "label", "dev_note", "doc_note", "hidden", "created_by"}.intersection(avail)

    sel = "SELECT " + ", ".join(cols_fr + sorted(extra)) + " FROM frame_records"
    rows = []
    for r in cur_src.execute(sel):
        d = dict(zip(cols_fr + sorted(extra), r))
        rows.append(
            (
                d["id"], d["session_id"], d.get("time_interval") or "",
                d.get("start_time") or 0, d.get("end_time") or 0,
                d.get("label") or "Normal", to_bool(d.get("confirmation")),
                d.get("doc_classification"), d.get("dev_note"), d.get("doc_note"),
                d.get("hidden", False) in (1, "1", True), d.get("created_by"),
            )
        )
    apply(
        """INSERT INTO frame_records (id, session_id, time_interval, start_time, end_time, label, confirmation, doc_classification, dev_note, doc_note, hidden, created_by, created_at)
           VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s, NOW())
           ON CONFLICT (id) DO UPDATE SET
             session_id = EXCLUDED.session_id,
             time_interval = EXCLUDED.time_interval,
             start_time = EXCLUDED.start_time,
             end_time = EXCLUDED.end_time,
             label = EXCLUDED.label,
             confirmation = EXCLUDED.confirmation,
             doc_classification = EXCLUDED.doc_classification,
             dev_note = EXCLUDED.dev_note,
             doc_note = EXCLUDED.doc_note,
             hidden = EXCLUDED.hidden,
             created_by = EXCLUDED.created_by""",
        rows,
        "frame_records",
    )

    print("\n=== RINGKASAN MIGRASI ===")
    if dry_run:
        print("(dry-run, tidak ada data ditulis)")
    for k, v in total.items():
        print(f"  {k:14s}: {v} baris")


def main():
    ap = argparse.ArgumentParser(description="Migrasi database SQLite (SQLCipher) ke PostgreSQL")
    ap.add_argument("--sqlite", required=True, help="Path database SQLite lama (database.db)")
    ap.add_argument("--pg", required=True, help="Connection string PostgreSQL (DATABASE_URL)")
    ap.add_argument("--db-key", help="Kunci SQLite (SQLITE_KEY) bila file masih terenkripsi SQLCipher")
    ap.add_argument("--dry-run", action="store_true", help="Hitung saja, jangan menulis ke DB")
    args = ap.parse_args()

    src = Path(args.sqlite)
    if not src.exists():
        sys.exit(f"File SQLite tidak ditemukan: {src}")

    cursor = None
    workspace = None
    plain_db = src
    try:
        if args.db_key:
            workspace = Path(tempfile.mkdtemp(prefix="ecg_migrate_"))
            plain_db = decrypt_sqlcipher(src, args.db_key, workspace)
        if not args.dry_run:
            print("[pg] Menyambung ke PostgreSQL ...")
        conn_src = sqlite3.connect(str(plain_db))
        conn_pg = None if args.dry_run else connect_pg(args.pg)
        migrate(conn_src, conn_pg, args.dry_run)
        conn_src.close()
        if conn_pg:
            conn_pg.close()
    except Exception as e:  # noqa: BLE001
        sys.exit(f"[ERROR] Migrasi gagal: {e}")
    finally:
        if workspace:
            shutil.rmtree(workspace, ignore_errors=True)


if __name__ == "__main__":
    main()