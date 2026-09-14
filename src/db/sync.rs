use sqlx::PgPool;

/// PostgreSQL adalah satu-satunya sumber data. Tidak ada SQLite legacy yang perlu disinkronkan lagi.
/// Fungsi ini menjadi no-op yang berhasil (sukses parsial 0 record) selama konfigurasi valid.
pub fn sync_databases(_pool: &PgPool) -> Result<usize, String> {
    match std::env::var("DATABASE_URL") {
        Ok(url) if !url.is_empty() && !url.contains("[PASSWORD") => Ok(0),
        _ => Err("DATABASE_URL tidak diatur di file .env".to_string()),
    }
}
