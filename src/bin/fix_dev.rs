use sqlx::postgres::PgPoolOptions;

#[tokio::main]
async fn main() -> Result<(), sqlx::Error> {
    let database_url = std::env::var("DATABASE_URL").expect("DATABASE_URL");
    let pool = PgPoolOptions::new()
        .max_connections(5)
        .connect(&database_url)
        .await?;

    // Runtime query (bukan `sqlx::query!`) supaya binary debug ini tidak butuh
    // cache offline `.sqlx` saat CI menjalankan SQLX_OFFLINE=true.
    sqlx::query("UPDATE devices SET mqtt_password = $1, mqtt_username = $2 WHERE id = $3")
        .bind("undipjaya")
        .bind("ecg-undip")
        .bind("dev_001")
        .execute(&pool)
        .await?;

    println!("updated");
    Ok(())
}
