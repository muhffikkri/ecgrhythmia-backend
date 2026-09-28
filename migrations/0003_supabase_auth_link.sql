-- =========================================================================
-- 0003_supabase_auth_link.sql
-- Penambahan kolom accounts.auth_id untuk menautkan akun lokal dengan
-- user Supabase Auth.
--
-- Konteks: backend menerbitkan token sendiri (HS256 + SUPABASE_JWT_SECRET)
-- melalui POST /api/auth/login, di mana claims.sub = accounts.id. Frontend
-- berbasis Supabase (supabase.auth.signInWithPassword) memakai token milik
-- Supabase, yang claims.sub = UUID user Supabase Auth dan TIDAK ada di
-- accounts.id. Tanpa kolom ini, role tidak bisa dipetakan dan guard admin
-- menolak request.
--
-- auth_id NULL untuk akun yang hanya login lewat backend. Setelah user
-- Supabase pertama kali login, isi manual atau lewat backfill:
--   UPDATE accounts SET auth_id = '<supabase auth uid>' WHERE email = '<email>';
--
-- Index unik parsial mencegah dua akun memakai auth_id yang sama, sementara
-- tetap mengizinkan banyak NULL.
-- =========================================================================

BEGIN;

ALTER TABLE accounts ADD COLUMN IF NOT EXISTS auth_id TEXT;

CREATE UNIQUE INDEX IF NOT EXISTS accounts_auth_id_key
    ON accounts (auth_id)
    WHERE auth_id IS NOT NULL;

CREATE INDEX IF NOT EXISTS accounts_auth_id_idx
    ON accounts (auth_id)
    WHERE auth_id IS NOT NULL;

COMMIT;
