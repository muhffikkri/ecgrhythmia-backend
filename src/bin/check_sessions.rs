use sqlx::postgres::PgPoolOptions;

#[tokio::main]
async fn main() -> Result<(), sqlx::Error> {
    let pool = PgPoolOptions::new()
        .max_connections(5)
        .connect("postgresql://postgres.xzjxkplsgzcvdcjdhpcp:bapakkauperangbarengidf@aws-0-ap-northeast-1.pooler.supabase.com:5432/postgres?sslmode=require").await?;

    let rows = sqlx::query!("SELECT id, file_path FROM sessions LIMIT 10")
        .fetch_all(&pool)
        .await?;

    for row in rows {
        println!("Session ID: {}, file_path: {:?}", row.id, row.file_path);
    }
    Ok(())
}
