use sqlx::postgres::PgPoolOptions;

#[tokio::main]
async fn main() -> Result<(), sqlx::Error> {
    let database_url = std::env::var("DATABASE_URL").expect("DATABASE_URL");
    let pool = PgPoolOptions::new().max_connections(5).connect(&database_url).await?;
    let rows = sqlx::query!("SELECT id, name, mqtt_broker, mqtt_port, mqtt_topic, mqtt_username, mqtt_password FROM devices").fetch_all(&pool).await?;
    for row in rows {
        println!("id:{} name:{} broker:{:?} port:{:?} topic:{:?} user:{:?} pass:{:?}", row.id, row.name, row.mqtt_broker, row.mqtt_port, row.mqtt_topic, row.mqtt_username, row.mqtt_password);
    }
    Ok(())
}
