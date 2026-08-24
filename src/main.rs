use ecg_backend::{models, network, api, db, config};

use tracing::{info, error, Level};
use tracing_subscriber::FmtSubscriber;

#[tokio::main]
async fn main() {
    // 1. Inisialisasi Tracing/Logging
    let subscriber = FmtSubscriber::builder()
        .with_max_level(Level::INFO)
        .finish();
    tracing::subscriber::set_global_default(subscriber)
        .expect("Gagal mengatur global default tracing subscriber");

    info!("Memulai inisialisasi sistem medis (Mode Asinkron Axum + SQLite + SQLCipher)...");

    // 2. Muat Konfigurasi dari berkas .env
    let config = config::AppConfig::load();

    // 3. Inisialisasi Database Pool SQLite dengan enkripsi SQLCipher
    let pool = db::sqlite::create_pool(&config.db_path, &config.sqlite_key);

    // Lakukan auto-migration skema database pada saat startup
    {
        let conn = pool.get().expect("Gagal mendapatkan koneksi DB awal untuk migrasi");
        if let Err(e) = db::sqlite::run_migrations(&conn, &config.default_admin_email, &config.default_admin_password) {
            error!("Gagal menjalankan auto-migrations database: {}", e);
            panic!("Database migration failed: {}", e);
        }
        info!("Auto-migrations database SQLite berhasil diselesaikan.");
    }

    // 4. Buat daftar klien WebSocket (ClientList) asinkron yang thread-safe
    let clients = network::websocket::ClientList::default();

    // 5. Jalankan Pacer asinkron (pemotongan data EKG & forward ke WebSocket)
    let pacer_tx = network::pacer::start_pacer(clients.clone());

    // 6. Jalankan Background Database Worker untuk menulis data asinkron
    let db_tx = db::sqlite::start_db_worker(pool.clone(), pacer_tx.clone());

    // 7. Load Devices and start MQTT Listeners dynamically
    let mqtt_clients = std::sync::Arc::new(tokio::sync::RwLock::new(std::collections::HashMap::new()));
    
    {
        if let Ok(conn) = pool.get() {
            if let Ok(mut stmt) = conn.prepare("SELECT name, mqtt_broker, mqtt_port, mqtt_topic, mqtt_username, mqtt_password FROM devices WHERE mqtt_broker IS NOT NULL AND mqtt_port IS NOT NULL") {
                if let Ok(device_iter) = stmt.query_map([], |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, u16>(2)?,
                        row.get::<_, String>(3)?,
                        row.get::<_, String>(4)?,
                        row.get::<_, String>(5)?,
                    ))
                }) {
                    for device in device_iter {
                        if let Ok((name, broker, port, topic, username, password)) = device {
                            let db_tx_clone = db_tx.clone();
                            
                            let client = network::mqtt_listener::start_mqtt_listener(
                                &broker,
                                port,
                                &topic,
                                &username,
                                &password,
                                move |payload_str| {
                                    match serde_json::from_str::<models::device::DevicePayload>(&payload_str) {
                                        Ok(device_payload) => {
                                            let _ = db_tx_clone.send(device_payload);
                                        }
                                        Err(e) => {
                                            tracing::error!(
                                                "Gagal mem-parsing payload EKG dari perangkat: {}. Payload: {}",
                                                e,
                                                payload_str
                                            );
                                        }
                                    }
                                }
                            );
                            
                            let mut clients_map = mqtt_clients.write().await;
                            clients_map.insert(name, client);
                        }
                    }
                }
            }
        }
    }

    // 8. Setup Router Axum untuk REST API + WebSocket
    let app_state = api::routes::AppState {
        pool: pool.clone(),
        mqtt_clients: mqtt_clients.clone(),
        pacer_tx: pacer_tx.clone(),
        db_tx: db_tx.clone(),
        jwt_secret: config.jwt_secret.clone(),
        api_url: format!("http://{}:{}", config.host_ip, config.rest_port),
    };

    let mut app = api::routes::create_router(app_state);
    
    // Pasang endpoint WebSocket pada root "/" dan "/ws" untuk mendukung proxy produksi
    app = app
        .route("/", axum::routing::get(network::websocket::ws_handler).with_state(clients.clone()))
        .route("/ws", axum::routing::get(network::websocket::ws_handler).with_state(clients.clone()));

    // 8.5. Jalankan Loop Sinkronisasi Latar Belakang (Setiap 10 Jam)
    let pool_clone = pool.clone();
    tokio::spawn(async move {
        info!("[Background Sync] Loop sinkronisasi database asinkron berjalan...");
        loop {
            // Tunggu 10 jam di awal loop atau setelah sinkronisasi selesai?
            // Biasanya jalankan sinkronisasi pertama kali setelah 10 jam atau langsung jalankan sekali pada saat startup.
            // Jalankan sekali pada saat startup, lalu tunggu 10 jam.
            match db::sync::sync_databases(&pool_clone) {
                Ok(count) => info!("[Background Sync] Berhasil melakukan sinkronisasi database. Terproses: {} data.", count),
                Err(e) => info!("[Background Sync] Sinkronisasi otomatis dilewati/gagal: {}", e),
            }
            tokio::time::sleep(tokio::time::Duration::from_secs(10 * 3600)).await;
        }
    });

    // 9. Jalankan Server HTTP & WebSocket
    let addr_ws = format!("{}:{}", config.host_ip, config.ws_port);
    let addr_rest = format!("{}:{}", config.host_ip, config.rest_port);

    if config.ws_port == config.rest_port {
        info!("Menjalankan server terpadu Axum di http://{}", addr_ws);
        let listener = tokio::net::TcpListener::bind(&addr_ws).await.unwrap();
        axum::serve(listener, app).await.unwrap();
    } else {
        let app_clone = app.clone();
        
        let ws_handle = tokio::spawn(async move {
            info!("Menjalankan server WebSocket di ws://{}", addr_ws);
            let listener = tokio::net::TcpListener::bind(&addr_ws).await.unwrap();
            axum::serve(listener, app_clone).await.unwrap();
        });

        let rest_handle = tokio::spawn(async move {
            info!("Menjalankan server REST API di http://{}", addr_rest);
            let listener = tokio::net::TcpListener::bind(&addr_rest).await.unwrap();
            axum::serve(listener, app).await.unwrap();
        });

        let _ = tokio::join!(ws_handle, rest_handle);
    }
}