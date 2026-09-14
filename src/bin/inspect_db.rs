use ecg_backend::db::postgres;
use std::process::exit;

#[tokio::main]
async fn main() {
    println!("=== Memuat konfigurasi dan membuka database ===");
    let database_url = std::env::var("DATABASE_URL").unwrap_or_else(|_| {
        eprintln!("DATABASE_URL tidak diatur di environment / .env");
        exit(1);
    });
    let pool = postgres::create_pool(&database_url).await;

    println!("\n=== DAFTAR PASIEN (10 Terbaru) ===");
    let rows =
        sqlx::query!("SELECT id, first_name, last_name FROM patients ORDER BY id DESC LIMIT 10")
            .fetch_all(&pool)
            .await
            .unwrap_or_default();
    for row in rows {
        println!("{} - {} {}", row.id, row.first_name, row.last_name);
    }

    println!("\n=== DAFTAR SESI (10 Terbaru) ===");
    let rows = sqlx::query!("SELECT id, patient_id, started_at, file_path FROM sessions ORDER BY started_at DESC LIMIT 10")
        .fetch_all(&pool).await.unwrap_or_default();
    for row in rows {
        let pid = row.patient_id.unwrap_or_else(|| "NONE".to_string());
        let fp = row.file_path.unwrap_or_else(|| "NONE".to_string());
        println!(
            "{} | Pasien: {} | Mulai: {} | File: {}",
            row.id, pid, row.started_at, fp
        );
    }

    println!("\n=== DAFTAR DEVICES ===");
    let rows = sqlx::query!("SELECT id, name, mqtt_topic, mqtt_broker, mqtt_port FROM devices")
        .fetch_all(&pool)
        .await
        .unwrap_or_default();
    for row in rows {
        let topic = row.mqtt_topic.unwrap_or_else(|| "NONE".to_string());
        println!(
            "ID: {} | Name: {} | Topic: {} | Broker: {:?} | Port: {:?}",
            row.id, row.name, topic, row.mqtt_broker, row.mqtt_port
        );
    }
}
