use sqlx::postgres::PgPoolOptions;

#[tokio::main]
async fn main() -> Result<(), sqlx::Error> {
    let database_url =
        std::env::var("DATABASE_URL").expect("DATABASE_URL tidak diatur di environment / .env");
    let pool = PgPoolOptions::new()
        .max_connections(5)
        .connect(&database_url)
        .await?;

    let rows = sqlx::query!("SELECT id, file_path FROM sessions LIMIT 10")
        .fetch_all(&pool)
        .await?;

    for row in rows {
        println!("Session ID: {}, file_path: {:?}", row.id, row.file_path);
    }
    Ok(())
}
