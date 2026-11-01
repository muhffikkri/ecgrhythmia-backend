use ecg_backend::{api, config, db, models, network};

use tracing::{error, info, Level};
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

    // 2. Muat Konfigurasi dari berkas .env
    let config = config::AppConfig::load();

    // 3. Inisialisasi Database Pool PostgreSQL asinkron menggunakan sqlx.
    // Koneksi gagal TIDAK lagi menghentikan proses: create_pool mencoba
    // berulang dengan backoff dan service tetap boot supaya endpoint auth,
    // /api/health, dan WebSocket masih bisa dihubungi.
    let pool = db::postgres::create_pool(&config.database_url).await;

    let db_health = db::postgres::DbHealth::new();
    db_health.prime(db::postgres::is_healthy(&pool).await);
    if !db_health.cached().unwrap_or(false) {
        error!("Database tidak dapat dihubungi saat startup — service jalan dalam mode degraded (endpoint data akan membalas 503).");
    }

    // Auto-migration bersifat best-effort. Kegagalan tidak boleh mematikan
    // service: release profile memakai `panic = "abort"`, sehingga panic di
    // sini akan membuat systemd gagal start terus-menerus.
    if let Err(e) = db::postgres::run_migrations(&pool).await {
        error!("Auto-migrations database gagal: {}", e);
    }
    info!("Auto-migrations database PostgreSQL selesai (best-effort).");

    // 4. Buat daftar klien WebSocket dan cache frame terbaru
    let clients = network::websocket::ClientList::default();
    let latest_frame = network::websocket::LatestFrame::default();

    // 5. Jalankan Pacer asinkron (pemotongan data EKG & forward ke WebSocket)
    let pacer_tx = network::pacer::start_pacer(clients.clone(), latest_frame.clone());

    // 6. Jalankan Background Database Worker untuk menulis data asinkron
    let db_tx = db::postgres::start_db_worker(pool.clone(), pacer_tx.clone());

    // 7. Load Devices and start MQTT Listeners dynamically
    let mqtt_clients =
        std::sync::Arc::new(tokio::sync::RwLock::new(std::collections::HashMap::new()));

    {
        if let Ok(devices) = sqlx::query!("SELECT id, name, mqtt_broker, mqtt_port, mqtt_topic, mqtt_username, mqtt_password FROM devices WHERE mqtt_broker IS NOT NULL AND mqtt_port IS NOT NULL")
            .fetch_all(&pool).await
        {
            for device in devices {
                let broker = device.mqtt_broker.as_ref().unwrap().as_str();
                let port = device.mqtt_port.unwrap() as u16;
                let topic = device.mqtt_topic.as_deref().unwrap_or("");
                let username = device.mqtt_username.as_deref().unwrap_or("");
                let password = device.mqtt_password.as_deref().unwrap_or("");

                let db_tx_clone = db_tx.clone();

                tracing::info!(device_id = %device.id, device_name = %device.name, broker = %broker, port, topic = %topic, "[TRACE STARTUP] MQTT listener dibuat untuk device");
                let client = network::mqtt_listener::start_mqtt_listener(
                    broker,
                    port,
                    topic,
                    username,
                    password,
                    move |payload_str| {
                        match serde_json::from_str::<models::device::DevicePayload>(&payload_str) {
                            Ok(device_payload) => {
                                tracing::info!(device_id = %device_payload.device_id, frame_id = %device_payload.frame_id, samples = device_payload.ecg.samples.len(), "[TRACE PARSER->DB] DevicePayload valid, masuk queue DB");
                                if let Err(e) = db_tx_clone.send(device_payload) {
                                    tracing::error!(error = %e, "[TRACE PARSER->DB] Queue DB gagal menerima payload");
                                }
                            }
                            Err(e) => {
                                tracing::error!(error = %e, "[TRACE PARSER->DB] Payload DevicePayload gagal diparse");
                            }
                        }
                    }
                );

                let mut clients_map = mqtt_clients.write().await;
                clients_map.insert(device.id, client);
            }
        }
    }

    // 8. Setup Router Axum untuk REST API + WebSocket
    let jwks = api::jwks::Jwks::new(config.supabase_jwks_url.clone());
    jwks.warmup().await;

    let app_state = api::routes::AppState {
        pool: pool.clone(),
        mqtt_clients: mqtt_clients.clone(),
        clients: clients.clone(),
        latest_frame: latest_frame.clone(),
        pacer_tx: pacer_tx.clone(),
        db_tx: db_tx.clone(),
        jwt_secret: config.supabase_jwt_secret.clone(),
        jwks,
        api_url: format!("http://{}:{}", config.host_ip, config.rest_port),
        db_health,
    };

    // Pantau kesehatan database terus-menerus; statusnya dibaca middleware
    // `require_db` tanpa query tambahan per request.
    api::routes::spawn_db_health_monitor(app_state.clone());

    let mut app = api::routes::create_router(app_state);

    // Pasang endpoint WebSocket dengan cache frame terbaru untuk replay saat connect.
    let stream_state = network::websocket::StreamState {
        clients: clients.clone(),
        latest_frame: latest_frame.clone(),
    };
    app = app
        .route(
            "/",
            axum::routing::get(network::websocket::ws_handler).with_state(stream_state.clone()),
        )
        .route(
            "/ws",
            axum::routing::get(network::websocket::ws_handler).with_state(stream_state),
        );

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
