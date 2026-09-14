use sqlx::postgres::PgPoolOptions;

#[tokio::main]
async fn main() -> Result<(), sqlx::Error> {
    let database_url =
        std::env::var("DATABASE_URL").expect("DATABASE_URL tidak diatur di environment / .env");
    let pool = PgPoolOptions::new()
        .max_connections(5)
        .connect(&database_url)
        .await?;

    let rows = sqlx::query!("SELECT id, name, mqtt_topic FROM devices")
        .fetch_all(&pool)
        .await?;

    for row in rows {
        println!(
            "ID: {}, Name: {}, Topic: {:?}",
            row.id, row.name, row.mqtt_topic
        );
    }
    Ok(())
}
