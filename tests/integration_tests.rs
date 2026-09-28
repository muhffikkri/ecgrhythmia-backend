use axum::{
    body::Body,
    http::{Request, StatusCode},
};
use ecg_backend::api::routes::{AppState, AuthResponse, LoginRequest, RegisterRequest};
use ecg_backend::models::device::{DeviceEcg, DevicePayload, DevicePrediction, DeviceValidation};
use std::collections::HashMap;
use tower::ServiceExt; // for oneshot

fn database_url() -> Option<String> {
    let url = std::env::var("DATABASE_URL").unwrap_or_else(|_| String::new());
    if url.trim().is_empty() {
        None
    } else {
        Some(url)
    }
}

async fn setup_test_state() -> Option<(
    AppState,
    tokio::sync::mpsc::UnboundedReceiver<DevicePayload>,
    tokio::sync::mpsc::UnboundedReceiver<DevicePayload>,
)> {
    let url = database_url()?;
    let pool = ecg_backend::db::postgres::create_pool(&url).await;
    if let Err(e) = ecg_backend::db::postgres::run_migrations(&pool).await {
        eprintln!("Migrations failed: {}", e);
        return None;
    }

    // Membersihkan sisa data pengujian dari eksekusi sebelumnya
    let _ = sqlx::query("DELETE FROM frame_records WHERE session_id LIKE 'session_%'")
        .execute(&pool)
        .await;
    let _ = sqlx::query("DELETE FROM sessions WHERE id LIKE 'session_%'")
        .execute(&pool)
        .await;
    let _ = sqlx::query("DELETE FROM sessions WHERE id LIKE 'ses%'")
        .execute(&pool)
        .await;
    let _ = sqlx::query("DELETE FROM patients WHERE email NOT LIKE '%@%' AND id LIKE 'test_%'")
        .execute(&pool)
        .await;

    let (pacer_tx, pacer_rx) = tokio::sync::mpsc::unbounded_channel();
    let (db_tx, db_rx) = tokio::sync::mpsc::unbounded_channel();
    let mqtt_clients = std::sync::Arc::new(tokio::sync::RwLock::new(HashMap::new()));

    let state = AppState {
        pool,
        mqtt_clients,
        clients: ecg_backend::network::websocket::ClientList::default(),
        pacer_tx,
        db_tx,
        jwt_secret: "test_jwt_secret_key_extremely_long_and_secure".to_string(),
        jwks: ecg_backend::api::jwks::Jwks::new(None),
        db_health: ecg_backend::db::postgres::DbHealth::new(),
        api_url: "http://127.0.0.1:8081".to_string(),
    };

    Some((state, pacer_rx, db_rx))
}

#[tokio::test]
async fn test_pacer_streaming() {
    let clients = ecg_backend::network::websocket::ClientList::default();
    let (ws_tx, mut ws_rx) = tokio::sync::mpsc::unbounded_channel::<String>();

    // Daftarkan channel penerima ws broadcast
    {
        let mut lock = clients.lock().unwrap();
        lock.push(ws_tx);
    }

    let pacer_tx = ecg_backend::network::pacer::start_pacer(clients);

    let payload = DevicePayload {
        message_id: "pacer_msg_001".to_string(),
        device_id: "device01".to_string(),
        session_id: "session_pacer".to_string(),
        patient_id: None,
        frame_id: "frame_001".to_string(),
        created_at: "2026-08-10T10:00:00+07:00".to_string(),
        sampling_rate_hz: 250.0,
        duration_s: 0.2,
        validation: DeviceValidation {
            status: "PASS".to_string(),
            warnings: vec![],
        },
        ecg: DeviceEcg {
            format: "samples_by_time".to_string(),
            samples: vec![vec![0.5, 0.6, 0.7]; 50],
        },
        prediction: DevicePrediction {
            status: "PASS".to_string(),
            label: "Normal".to_string(),
            confidence_percent: 99.5,
            probabilities: None,
            threshold: None,
            latency_ms: None,
            runtime: None,
        },
        system: None,
        stress_test: None,
        network: None,
    };

    pacer_tx.send(payload).unwrap();

    // Terima chunk ke-1
    let msg1 = tokio::time::timeout(std::time::Duration::from_millis(500), ws_rx.recv())
        .await
        .unwrap()
        .unwrap();
    let parsed1: serde_json::Value = serde_json::from_str(&msg1).unwrap();
    assert_eq!(parsed1["type"], "live_data");
    assert_eq!(parsed1["measurement_id"], "pacer_msg_001");
    let raw_data1 = &parsed1["data_payload"]["raw"];
    assert_eq!(raw_data1["ch1"].as_array().unwrap().len(), 25);

    // Terima chunk ke-2
    let msg2 = tokio::time::timeout(std::time::Duration::from_millis(500), ws_rx.recv())
        .await
        .unwrap()
        .unwrap();
    let parsed2: serde_json::Value = serde_json::from_str(&msg2).unwrap();
    let raw_data2 = &parsed2["data_payload"]["raw"];
    assert_eq!(raw_data2["ch1"].as_array().unwrap().len(), 25);
}

#[tokio::test]
async fn test_api_register_and_login() {
    let Some((state, _pacer_rx, _db_rx)) = setup_test_state().await else {
        eprintln!("SKIP test_api_register_and_login: DATABASE_URL tidak tersedia");
        return;
    };
    let app = ecg_backend::api::routes::create_router(state);

    let email = format!("pasien_{}@test.com", chrono::Utc::now().timestamp_millis());
    let reg_req = RegisterRequest {
        role: "pasien".to_string(),
        email: email.clone(),
        password: "password123".to_string(),
        first_name: "John".to_string(),
        last_name: "Doe".to_string(),
        date_of_birth: Some("1995-05-15".to_string()),
        gender: Some("L".to_string()),
    };
    let req_body = serde_json::to_vec(&reg_req).unwrap();

    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/auth/register")
                .header("Content-Type", "application/json")
                .body(Body::from(req_body))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let body_bytes = axum::body::to_bytes(response.into_body(), 1024 * 10)
        .await
        .unwrap();
    let reg_res: AuthResponse = serde_json::from_slice(&body_bytes).unwrap();
    assert!(reg_res.success);

    // 2. Uji Login Pasien
    let login_req = LoginRequest {
        email: email.clone(),
        password: "password123".to_string(),
        role: Some("pasien".to_string()),
    };
    let login_body = serde_json::to_vec(&login_req).unwrap();

    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/auth/login")
                .header("Content-Type", "application/json")
                .body(Body::from(login_body))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let body_bytes = axum::body::to_bytes(response.into_body(), 1024 * 10)
        .await
        .unwrap();
    let login_res: AuthResponse = serde_json::from_slice(&body_bytes).unwrap();
    assert!(login_res.success);
    assert_eq!(login_res.role.unwrap(), "pasien");
    assert!(login_res.token.is_some());
}

#[tokio::test]
async fn test_db_sync_not_configured() {
    std::env::remove_var("DATABASE_URL");
    // connect_lazy tidak melakukan koneksi nyata; cukup untuk memenuhi signature fungsi
    let pool = sqlx::postgres::PgPoolOptions::new()
        .connect_lazy("postgres://localhost:5432/nonexistent")
        .expect("lazy pool must build");
    let result = ecg_backend::db::sync::sync_databases(&pool);
    assert!(result.is_err());
    assert_eq!(
        result.unwrap_err(),
        "DATABASE_URL tidak diatur di file .env"
    );
}

#[tokio::test]
async fn test_db_worker_session_writing() {
    let Some((state, _pacer_rx, _db_rx)) = setup_test_state().await else {
        eprintln!("SKIP test_db_worker_session_writing: DATABASE_URL tidak tersedia");
        return;
    };
    let pool = state.pool.clone();

    // Buat device & sesi aktif agar worker dapat menulis
    let _ = sqlx::query!("INSERT INTO devices (id, name) VALUES ('dev_worker_test', 'device-worker-test') ON CONFLICT (id) DO NOTHING").execute(&pool).await;
    let _ = sqlx::query!(
        "INSERT INTO sessions (id, device_id, started_at, file_path) VALUES ('ses_worker_test', 'dev_worker_test', NOW(), 'records/ses_worker_test.jsonl') ON CONFLICT (id) DO NOTHING"
    ).execute(&pool).await;

    let (_pacer_tx_for_worker, _pacer_rx) = tokio::sync::mpsc::unbounded_channel();
    let db_tx = ecg_backend::db::postgres::start_db_worker(pool.clone(), _pacer_tx_for_worker);

    let payload = DevicePayload {
        message_id: "msg_worker_test".to_string(),
        device_id: "device-worker-test".to_string(),
        session_id: "ses_worker_test".to_string(),
        patient_id: None,
        frame_id: "frame_001".to_string(),
        created_at: "2026-08-10T10:00:00+07:00".to_string(),
        sampling_rate_hz: 250.0,
        duration_s: 1.0,
        validation: DeviceValidation {
            status: "PASS".to_string(),
            warnings: vec![],
        },
        ecg: DeviceEcg {
            format: "samples_by_time".to_string(),
            samples: vec![vec![0.1, 0.2, 0.3]],
        },
        prediction: DevicePrediction {
            status: "PASS".to_string(),
            label: "Normal".to_string(),
            confidence_percent: 99.5,
            probabilities: None,
            threshold: None,
            latency_ms: None,
            runtime: None,
        },
        system: None,
        stress_test: None,
        network: None,
    };

    db_tx.send(payload).unwrap();

    // Tunggu worker menulis ke file & DB (polling dengan timeout)
    let mut frame_exists = false;
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
    while std::time::Instant::now() < deadline {
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
        frame_exists = sqlx::query_scalar::<_, bool>(
            "SELECT EXISTS(SELECT 1 FROM frame_records WHERE session_id = $1)",
        )
        .bind("ses_worker_test")
        .fetch_one(&pool)
        .await
        .unwrap_or(false);
        if frame_exists {
            break;
        }
    }
    assert!(frame_exists);

    let path = std::path::Path::new("records/ses_worker_test.jsonl");
    let mut file_written = false;
    let deadline2 = std::time::Instant::now() + std::time::Duration::from_secs(5);
    while std::time::Instant::now() < deadline2 {
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
        if path.exists() {
            file_written = true;
            break;
        }
    }
    assert!(file_written);

    // Bersihkan berkas dan data uji
    let _ = std::fs::remove_file(path);
    let _ = sqlx::query!(
        "DELETE FROM frame_records WHERE session_id = $1",
        "ses_worker_test"
    )
    .execute(&pool)
    .await;
    let _ = sqlx::query!("DELETE FROM sessions WHERE id = $1", "ses_worker_test")
        .execute(&pool)
        .await;
}

#[tokio::test]
async fn test_get_devices_endpoint() {
    let Some((state, _pacer_rx, _db_rx)) = setup_test_state().await else {
        eprintln!("SKIP test_get_devices_endpoint: DATABASE_URL tidak tersedia");
        return;
    };
    let app = ecg_backend::api::routes::create_router(state);

    let response = app
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/api/devices")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
}
