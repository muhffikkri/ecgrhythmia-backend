use sqlx::{postgres::PgPoolOptions, Row};

#[tokio::main]
async fn main() -> Result<(), sqlx::Error> {
    let database_url = std::env::var("DATABASE_URL").expect("DATABASE_URL");
    let pool = PgPoolOptions::new()
        .max_connections(5)
        .connect(&database_url)
        .await?;

    // Runtime query (bukan `sqlx::query!`) supaya binary debug ini tidak butuh
    // cache offline `.sqlx` saat CI menjalankan SQLX_OFFLINE=true.
    let rows = sqlx::query(
        "SELECT id, name, mqtt_broker, mqtt_port, mqtt_topic, mqtt_username, mqtt_password FROM devices",
    )
    .fetch_all(&pool)
    .await?;

    for row in rows {
        let id: String = row.get("id");
        let name: String = row.get("name");
        let broker: Option<String> = row.get("mqtt_broker");
        let port: Option<i32> = row.get("mqtt_port");
        let topic: Option<String> = row.get("mqtt_topic");
        let user: Option<String> = row.get("mqtt_username");
        let pass: Option<String> = row.get("mqtt_password");
        println!(
            "id:{id} name:{name} broker:{broker:?} port:{port:?} topic:{topic:?} user:{user:?} pass:{pass:?}"
        );
    }
    Ok(())
}
