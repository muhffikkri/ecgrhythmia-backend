#!/usr/bin/env bash
# =========================================================================
# run_migration.sh - Migrasi database lama SQLite (SQLCipher) ke PostgreSQL
#
# Urutan:
#   1. Membaca DATABASE_URL & SQLITE_KEY dari .env
#   2. Menjalankan migrasi skema PostgreSQL (0001 + 0002) via psql
#   3. Menjalankan tool migrasi data SQLite -> PostgreSQL
#
# Prasyarat (VPS/Linux):
#   sudo apt update && sudo apt install -y sqlcipher postgresql-client python3-pip
#   python3 -m pip install --user psycopg2-binary
#
# Penggunaan:
#   chmod +x scripts/run_migration.sh
#   ./scripts/run_migration.sh /path/ke/database.db
#   ./scripts/run_migration.sh database.db            # bila di folder repo
#   cat .env | PATH_DB=... ./scripts/run_migration.sh # URL diambil dari .env
# =========================================================================
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

if [ ! -f .env ]; then
  echo "[!] .env tidak ditemukan di $ROOT" >&2
  exit 1
fi

# Ambil variabel penting dari .env (toleransi spasi/kutip)
get_env() {
  local key="$1"
  grep -E "^${key}=" .env | head -n1 | sed -E "s/^${key}=//" | tr -d '"' | tr -d "'"
}

DATABASE_URL="${DATABASE_URL:-$(get_env DATABASE_URL)}"
SQLITE_KEY="${SQLITE_KEY:-$(get_env SQLITE_KEY)}"
DB_PATH="${1:-$(get_env DB_PATH)}"
DB_PATH="${DB_PATH:-database.db}"

if [ -z "${DATABASE_URL}" ]; then
  echo "[!] DATABASE_URL tidak tersedia di .env" >&2
  exit 1
fi

if [ ! -f "${DB_PATH}" ]; then
  echo "[!] File database SQLite tidak ditemukan: ${DB_PATH}" >&2
  exit 1
fi

command -v psql >/dev/null || { echo "[!] psql belum terinstall (postgresql-client)" >&2; exit 1; }
command -v python3 >/dev/null || { echo "[!] python3 belum terinstall" >&2; exit 1; }
command -v sqlcipher >/dev/null || { echo "[!] sqlcipher belum terinstall (perlu untuk SQLCipher/terenkripsi)" >&2; exit 1; }

echo "==> 1. Menjalankan migrasi skema PostgreSQL =="
for f in migrations/*.sql; do
  echo "    -> $f"
  psql "$DATABASE_URL" --set ON_ERROR_STOP=1 -f "$f"
done

echo ""
echo "==> 2. Migrasi data SQLite -> PostgreSQL =="
if python3 -c "import psycopg2" 2>/dev/null || python3 -c "import psycopg" 2>/dev/null; then
  :
else
  echo "[!] Library psycopg2/psycopg tidak ditemukan. Instal: python3 -m pip install --user psycopg2-binary" >&2
  exit 1
fi

python3 scripts/migrate_sqlite_to_postgres.py \
  --sqlite "${DB_PATH}" \
  --db-key "${SQLITE_KEY}" \
  --pg "${DATABASE_URL}"

echo ""
echo "======================================================================"
echo "SELESAI. Jangan lupa salin folder records/ dari server lama:"
echo "  rsync -avz user@old-server:/path/ecgrhythmia-backend/records/ records/"
echo "======================================================================"