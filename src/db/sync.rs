use sqlx::PgPool;

/// PostgreSQL menjadi satu-satunya sumber data; sinkronisasi SQLite legacy tidak didukung.
pub fn sync_databases(_pool: &PgPool) -> Result<usize, String> {
    Err("Legacy SQLite synchronization is not supported with the PostgreSQL backend".to_string())
}
