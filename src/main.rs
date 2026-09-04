use ecg_backend::{models, network, api, db, config};

use tracing::{info, error, Level};
use tracing_subscriber::fmt::format::FmtSpan;
use tracing_subscriber::fmt::time::ChronoLocal;

#[tokio::main]
async fn main() {
    // 1. Inisialisasi Tracing/Logging
    let timer = ChronoLocal::new("%Y-%m-%d %H:%M:%S".to_string());
    
    let subscriber = tracing_subscriber::fmt()
        .with_max_level(Level::INFO)
        .with_timer(timer)
        .with_target(true)
        .with_thread_ids(false)
        .with_thread_names(false)
        .with_span_events(FmtSpan::CLOSE)
        .finish();
        
    tracing::subscriber::set_global_default(subscriber)
        .expect("Gagal mengatur global default tracing subscriber");

<<<<<<< HEAD
    // Cetak ASCII Banner
    println!("\x1b[36m");
    println!("   ____  ____  ____    ____             _                  _ ");
    println!("  | ___|/ ___|/ ___|  | __ )  __ _  ___| | _____ _ __   __| |");
    println!("  |  _|| |   | |  _   |  _ \\ / _` |/ __| |/ / _ \\ '_ \\ / _` |");
    println!("  | |__| |___| |_| |  | |_) | (_| | (__|   <  __/ | | | (_| |");
    println!("  |____|\\____|\\____|  |____/ \\__,_|\\___|_|\\_\\___|_| |_|\\__,_|");
    println!("                                                             ");
    println!("  [ MEDICAL BACKEND ENGINE - V1.0 ]\x1b[0m\n");

    info!("Memulai inisialisasi sistem medis (Mode Asinkron Axum + PostgreSQL (Supabase))...");
=======
    info!("=== ecg-backend DIMULAI | v{} ===", env!("CARGO_PKG_VERSION"));
    info!("Memulai inisialisasi sistem medis (Mode Asinkron Axum + SQLite + SQLCipher)...");
>>>>>>> d4e4ff69c48c853c58f915b255502ea5f0968312

    // 2. Muat Konfigurasi dari berkas .env
    let config = config::AppConfig::load();

    // 3. Inisialisasi Database Pool PostgreSQL asinkron menggunakan sqlx
    let pool = db::postgres::create_pool(&config.database_url).await;

    // Lakukan auto-migration skema database pada saat startup
    if let Err(e) = db::postgres::run_migrations(&pool).await {
        error!("Gagal menjalankan auto-migrations database: {}", e);
        panic!("Database migration failed: {}", e);
    }
    info!("Auto-migrations database PostgreSQL berhasil diselesaikan.");

    // 4. Buat daftar klien WebSocket (ClientList) asinkron yang thread-safe
    let clients = network::websocket::ClientList::default();

    // 5. Jalankan Pacer asinkron (pemotongan data EKG & forward ke WebSocket)
    let pacer_tx = network::pacer::start_pacer(clients.clone());

    // 6. Jalankan Background Database Worker untuk menulis data asinkron
    let db_tx = db::postgres::start_db_worker(pool.clone(), pacer_tx.clone());

    // 7. Load Devices and start MQTT Listeners dynamically
    let mqtt_clients = std::sync::Arc::new(tokio::sync::RwLock::new(std::collections::HashMap::new()));
    
    {
<<<<<<< HEAD
        if let Ok(devices) = sqlx::query!("SELECT id, name, mqtt_broker, mqtt_port, mqtt_topic, mqtt_username, mqtt_password FROM devices WHERE mqtt_broker IS NOT NULL AND mqtt_port IS NOT NULL")
            .fetch_all(&pool).await 
        {
            for device in devices {
                if let (Some(broker), Some(port), Some(topic), Some(username), Some(password)) = (
                    device.mqtt_broker, device.mqtt_port, device.mqtt_topic, device.mqtt_username, device.mqtt_password
                ) {
                    let db_tx_clone = db_tx.clone();
                    let port_u16 = port as u16;
                    
                    let client = network::mqtt_listener::start_mqtt_listener(
                        &broker,
                        port_u16,
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
=======
        if let Ok(conn) = pool.get() {
            if let Ok(mut stmt) = conn.prepare("SELECT id, name, mqtt_broker, mqtt_port, mqtt_topic, mqtt_username, mqtt_password FROM devices WHERE mqtt_broker IS NOT NULL AND mqtt_port IS NOT NULL") {
                if let Ok(device_iter) = stmt.query_map([], |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, String>(2)?,
                        row.get::<_, u16>(3)?,
                        row.get::<_, String>(4)?,
                        row.get::<_, String>(5)?,
                        row.get::<_, String>(6)?,
                    ))
                }) {
                    for device in device_iter {
                        if let Ok((id, name, broker, port, topic, username, password)) = device {
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
                            // Daftarkan menggunakan ID perangkat agar cocok dengan route parameter API
                            clients_map.insert(id, client);
>>>>>>> d4e4ff69c48c853c58f915b255502ea5f0968312
                        }
                    );
                    
                    let mut clients_map = mqtt_clients.write().await;
                    clients_map.insert(device.id, client);
                }
            }
        }
    }

    // 8. Setup Router Axum untuk REST API + WebSocket
    let app_state = api::routes::AppState {
        pool: pool.clone(),
        mqtt_clients: mqtt_clients.clone(),
        clients: clients.clone(),
        pacer_tx: pacer_tx.clone(),
        db_tx: db_tx.clone(),
        jwt_secret: config.supabase_jwt_secret.clone(),
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
