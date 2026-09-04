use sqlx::postgres::PgPoolOptions;

#[tokio::main]
async fn main() -> Result<(), sqlx::Error> {
    let pool = PgPoolOptions::new()
        .max_connections(5)
        .connect("postgresql://postgres.xzjxkplsgzcvdcjdhpcp:bapakkauperangbarengidf@aws-0-ap-northeast-1.pooler.supabase.com:5432/postgres?sslmode=require").await?;

    let rows = sqlx::query!("SELECT id, name, mqtt_topic FROM devices")
        .fetch_all(&pool)
        .await?;

    for row in rows {
        println!("ID: {}, Name: {}, Topic: {:?}", row.id, row.name, row.mqtt_topic);
    }
    Ok(())
}
