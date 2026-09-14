use ecg_backend::db::postgres;
use std::process::exit;

/// Jalankan auto-migrations PostgreSQL (CREATE TABLE IF NOT EXISTS + ALTER TABLE ADD COLUMN IF NOT EXISTS).
#[tokio::main]
async fn main() {
    let database_url = std::env::var("DATABASE_URL").unwrap_or_else(|_| {
        eprintln!("DATABASE_URL tidak diatur di environment / .env");
        exit(1);
    });
    let pool = postgres::create_pool(&database_url).await;
    match postgres::run_migrations(&pool).await {
        Ok(_) => {
            println!("Migrasi PostgreSQL berhasil.");
            exit(0);
        }
        Err(e) => {
            eprintln!("Migrasi PostgreSQL gagal: {}", e);
            exit(1);
        }
    }
}
