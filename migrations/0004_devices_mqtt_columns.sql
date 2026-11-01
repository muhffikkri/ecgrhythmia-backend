-- =========================================================================
-- 0004_devices_mqtt_columns.sql
-- Menambah kolom konfigurasi MQTT ke tabel `devices` bila belum ada.
--
-- Konteks: skema produksi Supabase dibuat dari versi awal kode (hanya
-- `id`, `name`), sementara kode backend saat ini (macro `sqlx::query!` dan
-- runtime query) memakai kolom `mqtt_broker`, `mqtt_port`, `mqtt_topic`,
-- `mqtt_username`, `mqtt_password`. Tanpa kolom ini:
--   * `SELECT ... FROM devices WHERE mqtt_broker IS NOT NULL` (main.rs,
--     startup) gagal -> `mqtt_clients` kosong -> `POST /api/devices/:id/command`
--     selalu membalas 404 "Perangkat tidak memiliki koneksi MQTT aktif"
--   * live data ECG tidak pernah masuk ke WebSocket -> monitor page tidak
--     menampilkan gelombang.
--
-- Idempoten: aman dijalankan berulang kali.
-- =========================================================================

BEGIN;

ALTER TABLE public.devices ADD COLUMN IF NOT EXISTS mqtt_broker TEXT;
ALTER TABLE public.devices ADD COLUMN IF NOT EXISTS mqtt_port INTEGER;
ALTER TABLE public.devices ADD COLUMN IF NOT EXISTS mqtt_topic TEXT;
ALTER TABLE public.devices ADD COLUMN IF NOT EXISTS mqtt_username TEXT;
ALTER TABLE public.devices ADD COLUMN IF NOT EXISTS mqtt_password TEXT;

COMMIT;
