use axum::{
    body::Body,
    http::{Request, StatusCode},
};
use tower::ServiceExt; // for oneshot
use std::collections::HashMap;
use ecg_backend::api::routes::{AppState, RegisterRequest, LoginRequest, AuthResponse};
use ecg_backend::models::device::{DevicePayload, DeviceValidation, DeviceEcg, DevicePrediction};
use ecg_backend::db::sqlite;

fn setup_test_state() -> (AppState, tokio::sync::mpsc::UnboundedReceiver<DevicePayload>, tokio::sync::mpsc::UnboundedReceiver<DevicePayload>) {
    // Gunakan shared in-memory SQLite DB cache agar pool connection mengakses DB yang sama
    let db_path = "file::memory_db?mode=memory&cache=shared";
    let sqlite_key = "test_secure_key_123";
    let pool = sqlite::create_pool(db_path, sqlite_key);

    // Jalankan migrasi
    {
        let conn = pool.get().unwrap();
        sqlite::run_migrations(&conn, "admin@test.com", "adminpassword").unwrap();
    }

    let (pacer_tx, pacer_rx) = tokio::sync::mpsc::unbounded_channel();
    let (db_tx, db_rx) = tokio::sync::mpsc::unbounded_channel();
    let mqtt_clients = std::sync::Arc::new(tokio::sync::RwLock::new(HashMap::new()));

    let state = AppState {
        pool,
        mqtt_clients,
        pacer_tx,
        db_tx,
        jwt_secret: "test_jwt_secret_key_extremely_long_and_secure".to_string(),
        api_url: "http://127.0.0.1:8081".to_string(),
    };

    (state, pacer_rx, db_rx)
}

#[tokio::test]
async fn test_api_register_and_login() {
    let (state, _pacer_rx, _db_rx) = setup_test_state();
    let app = ecg_backend::api::routes::create_router(state);

    // 1. Uji Register Pasien
    let reg_req = RegisterRequest {
        role: "pasien".to_string(),
        email: "pasien@test.com".to_string(),
        password: "password123".to_string(),
        first_name: "John".to_string(),
        last_name: "Doe".to_string(),
        date_of_birth: Some("1995-05-15".to_string()),
        gender: Some("L".to_string()),
    };
    let req_body = serde_json::to_vec(&reg_req).unwrap();

    let response = app.clone()
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
    let body_bytes = axum::body::to_bytes(response.into_body(), 1024 * 10).await.unwrap();
    let reg_res: AuthResponse = serde_json::from_slice(&body_bytes).unwrap();
    assert!(reg_res.success);

    // 2. Uji Login Pasien
    let login_req = LoginRequest {
        email: "pasien@test.com".to_string(),
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
    let body_bytes = axum::body::to_bytes(response.into_body(), 1024 * 10).await.unwrap();
    let login_res: AuthResponse = serde_json::from_slice(&body_bytes).unwrap();
    assert!(login_res.success);
    assert_eq!(login_res.role.unwrap(), "pasien");
    assert!(login_res.token.is_some());
}

#[tokio::test]
async fn test_db_worker_session_writing() {
    let db_path = "file::memory_worker_db?mode=memory&cache=shared";
    let sqlite_key = "worker_secure_key_123";
    let pool = sqlite::create_pool(db_path, sqlite_key);

    // Jalankan migrasi
    {
        let conn = pool.get().unwrap();
        sqlite::run_migrations(&conn, "admin@test.com", "adminpassword").unwrap();
        
        // Daftarkan perangkat agar relasi asing (foreign key) ke devices valid
        conn.execute("INSERT OR IGNORE INTO devices (id, name) VALUES ('dev_001', 'device01')", []).unwrap();
    }

    // Jalankan Db Worker asinkron
    let (pacer_tx, _) = tokio::sync::mpsc::unbounded_channel();
    let db_tx = sqlite::start_db_worker(pool.clone(), pacer_tx);

    // Kirim Payload dummy
    let payload = DevicePayload {
        message_id: "msg_001".to_string(),
        device_id: "device01".to_string(),
        session_id: "session_integration_test".to_string(),
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

    // Tunggu worker menulis ke DB & File
    tokio::time::sleep(std::time::Duration::from_millis(500)).await;

    // Verifikasi sesi tersimpan di DB
    let conn = pool.get().unwrap();
    let session_exists: bool = conn.query_row(
        "SELECT EXISTS(SELECT 1 FROM sessions WHERE device_id = 'dev_001')",
        [],
        |row| row.get(0)
    ).unwrap();
    assert!(session_exists);

    // Verifikasi berkas .jsonl dibuat
    let session_id_in_db: String = conn.query_row(
        "SELECT id FROM sessions WHERE device_id = 'dev_001' LIMIT 1",
        [],
        |row| row.get(0)
    ).unwrap();
    let expected_file_path = format!("records/{}.jsonl", session_id_in_db);
    let path = std::path::Path::new(&expected_file_path);
    assert!(path.exists());

    // Bersihkan berkas dan folder pengujian
    let _ = std::fs::remove_file(path);
    let _ = std::fs::remove_dir("records");
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

    // Mengirim 50 data ekg sampel dengan sampling rate 250Hz.
    // Ukuran chunk: fs * 0.1 = 25 sampel.
    // Berarti 50 sampel dipecah menjadi 2 chunk berdurasi @100ms.
    let payload = DevicePayload {
        message_id: "pacer_msg_001".to_string(),
        device_id: "device01".to_string(),
        session_id: "session_pacer".to_string(),
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
<<<<<<< HEAD
async fn test_doctor_impersonate() {
    let (state, _pacer_rx, _db_rx) = setup_test_state();
    let app = ecg_backend::api::routes::create_router(state);

    // 1. Register Dokter
    let doc_reg = RegisterRequest {
        role: "dokter".to_string(),
        email: "doctor@test.com".to_string(),
        password: "password123".to_string(),
        first_name: "Dr. House".to_string(),
        last_name: "MD".to_string(),
        date_of_birth: None,
        gender: None,
    };
    let doc_req_body = serde_json::to_vec(&doc_reg).unwrap();
    let _ = app.clone().oneshot(
        Request::builder()
            .method("POST")
            .uri("/api/auth/register")
            .header("Content-Type", "application/json")
            .body(Body::from(doc_req_body))
            .unwrap(),
    ).await.unwrap();

    // 2. Register Pasien
    let pat_reg = RegisterRequest {
        role: "pasien".to_string(),
        email: "patient@test.com".to_string(),
        password: "password123".to_string(),
        first_name: "John".to_string(),
        last_name: "Doe".to_string(),
        date_of_birth: Some("1995-05-15".to_string()),
        gender: Some("L".to_string()),
    };
    let pat_req_body = serde_json::to_vec(&pat_reg).unwrap();
    let _ = app.clone().oneshot(
        Request::builder()
            .method("POST")
            .uri("/api/auth/register")
            .header("Content-Type", "application/json")
            .body(Body::from(pat_req_body))
            .unwrap(),
    ).await.unwrap();

    // 3. Login Dokter & Get Token
    let doc_login = LoginRequest {
        email: "doctor@test.com".to_string(),
        password: "password123".to_string(),
        role: Some("dokter".to_string()),
    };
    let doc_login_body = serde_json::to_vec(&doc_login).unwrap();
    let login_resp = app.clone().oneshot(
        Request::builder()
            .method("POST")
            .uri("/api/auth/login")
            .header("Content-Type", "application/json")
            .body(Body::from(doc_login_body))
            .unwrap(),
    ).await.unwrap();
    let body_bytes = axum::body::to_bytes(login_resp.into_body(), 1024 * 10).await.unwrap();
    let login_res: AuthResponse = serde_json::from_slice(&body_bytes).unwrap();
    let doc_token = login_res.token.unwrap();

    // 4. Login Pasien untuk mendapatkan patient_id
    let pat_login = LoginRequest {
        email: "patient@test.com".to_string(),
        password: "password123".to_string(),
        role: Some("pasien".to_string()),
    };
    let pat_login_body = serde_json::to_vec(&pat_login).unwrap();
    let pat_login_resp = app.clone().oneshot(
        Request::builder()
            .method("POST")
            .uri("/api/auth/login")
            .header("Content-Type", "application/json")
            .body(Body::from(pat_login_body))
            .unwrap(),
    ).await.unwrap();
    let body_bytes_pat = axum::body::to_bytes(pat_login_resp.into_body(), 1024 * 10).await.unwrap();
    let pat_login_res: AuthResponse = serde_json::from_slice(&body_bytes_pat).unwrap();
    let patient_id = pat_login_res.user_id.unwrap();

    // 5. Test Impersonation Route using Doctor Token
    let imp_resp = app.oneshot(
        Request::builder()
            .method("POST")
            .uri(format!("/api/doctors/impersonate/{}", patient_id))
            .header("Authorization", format!("Bearer {}", doc_token))
            .body(Body::empty())
            .unwrap(),
    ).await.unwrap();

    assert_eq!(imp_resp.status(), StatusCode::OK);
    let imp_body_bytes = axum::body::to_bytes(imp_resp.into_body(), 1024 * 10).await.unwrap();
    let imp_res: AuthResponse = serde_json::from_slice(&imp_body_bytes).unwrap();
    
    assert!(imp_res.success);
    assert_eq!(imp_res.role.unwrap(), "pasien");
    assert!(imp_res.token.is_some());
=======
async fn test_ekg_crud_endpoints() {
    let (state, _pacer_rx, _db_rx) = setup_test_state();
    let app = ecg_backend::api::routes::create_router(state.clone());

    // 1. Uji Tambah Pasien Manually
    let add_patient_body = serde_json::json!({
        "first_name": "Jane",
        "last_name": "Smith",
        "date_of_birth": "1990-01-01",
        "gender": "P",
        "device_id": Some("dev_002".to_string())
    });
    
    let response = app.clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/patients")
                .header("Content-Type", "application/json")
                .body(Body::from(serde_json::to_vec(&add_patient_body).unwrap()))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let body_bytes = axum::body::to_bytes(response.into_body(), 1024 * 10).await.unwrap();
    let add_patient_res: serde_json::Value = serde_json::from_slice(&body_bytes).unwrap();
    assert!(add_patient_res["success"].as_bool().unwrap());
    let patient_id = add_patient_res["id"].as_str().unwrap().to_string();

    // Seed a device and a session directly in DB to test session PUT/DELETE/GET
    {
        let conn = state.pool.get().unwrap();
        conn.execute("INSERT OR IGNORE INTO devices (id, name) VALUES ('dev_002', 'device02')", []).unwrap();
        conn.execute(
            "INSERT INTO sessions (id, device_id, patient_id, started_at, file_path) VALUES ('session_test_crud', 'dev_002', ?1, '2026-08-24T00:00:00Z', 'records/session_test_crud.jsonl')",
            [&patient_id],
        ).unwrap();
        
        // Write dummy file for records/session_test_crud.jsonl
        let _ = std::fs::create_dir_all("records");
        std::fs::write("records/session_test_crud.jsonl", "{\"dummy\":\"data\"}").unwrap();
    }

    // 2. Uji Edit Sesi (PUT /api/sessions/:session_id)
    let edit_session_body = serde_json::json!({
        "ended_at": "2026-08-24T01:00:00Z"
    });
    
    let response = app.clone()
        .oneshot(
            Request::builder()
                .method("PUT")
                .uri("/api/sessions/session_test_crud")
                .header("Content-Type", "application/json")
                .body(Body::from(serde_json::to_vec(&edit_session_body).unwrap()))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    
    // Verifikasi di DB
    {
        let conn = state.pool.get().unwrap();
        let ended_at: String = conn.query_row(
            "SELECT ended_at FROM sessions WHERE id = 'session_test_crud'",
            [],
            |row| row.get(0)
        ).unwrap();
        assert_eq!(ended_at, "2026-08-24T01:00:00Z");
    }

    // 3. Uji Download Sesi (GET /api/records/:session_id/download)
    let response = app.clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/api/records/session_test_crud/download")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let body_bytes = axum::body::to_bytes(response.into_body(), 1024 * 10).await.unwrap();
    let download_content = String::from_utf8(body_bytes.to_vec()).unwrap();
    assert_eq!(download_content, "{\"dummy\":\"data\"}");

    // 4. Uji Hapus Sesi (DELETE /api/sessions/:session_id)
    let response = app.clone()
        .oneshot(
            Request::builder()
                .method("DELETE")
                .uri("/api/sessions/session_test_crud")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    
    // Verifikasi terhapus di DB dan file terhapus
    {
        let conn = state.pool.get().unwrap();
        let session_exists: bool = conn.query_row(
            "SELECT EXISTS(SELECT 1 FROM sessions WHERE id = 'session_test_crud')",
            [],
            |row| row.get(0)
        ).unwrap();
        assert!(!session_exists);
        assert!(!std::path::Path::new("records/session_test_crud.jsonl").exists());
    }

    // 5. Uji Hapus Pasien (DELETE /api/patients/:patient_id)
    let response = app.clone()
        .oneshot(
            Request::builder()
                .method("DELETE")
                .uri(&format!("/api/patients/{}", patient_id))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    
    // Verifikasi terhapus di DB
    {
        let conn = state.pool.get().unwrap();
        let patient_exists: bool = conn.query_row(
            "SELECT EXISTS(SELECT 1 FROM patients WHERE id = ?1)",
            [&patient_id],
            |row| row.get(0)
        ).unwrap();
        assert!(!patient_exists);
    }
}

#[tokio::test]
async fn test_ekg_session_upload() {
    // Ensure cleanup of previous failed test runs
    let _ = std::fs::remove_file("records/ses000000000001.jsonl");

    let (state, _pacer_rx, _db_rx) = setup_test_state();
    
    // Seed patient first to avoid foreign key constraint error when inserting session
    {
        let conn = state.pool.get().unwrap();
        conn.execute(
            "INSERT INTO patients (id, first_name, last_name, date_of_birth, gender) VALUES ('pat_upload_test', 'Upload', 'Test', '1990-01-01', 'L')",
            []
        ).unwrap();
    }
    
    let app = ecg_backend::api::routes::create_router(state.clone());

    let boundary = "------------------------1234567890123456";
    let body = format!(
        "--{boundary}\r\n\
         Content-Disposition: form-data; name=\"patient_id\"\r\n\r\n\
         pat_upload_test\r\n\
         --{boundary}\r\n\
         Content-Disposition: form-data; name=\"frame_000001_mv.json\"; filename=\"frame_000001_mv.json\"\r\n\
         Content-Type: application/json\r\n\r\n\
         {{\"source_frame\":\"frame_000001\",\"source_metadata\":{{\"device_id\":\"device01\",\"session_id\":\"session_upload_test\",\"csv_file\":\"frame_000001_mv.csv\",\"sample_rate_hz\":250.0,\"duration_seconds\":10.0}}}}\r\n\
         --{boundary}\r\n\
         Content-Disposition: form-data; name=\"frame_000001_prediction.json\"; filename=\"frame_000001_prediction.json\"\r\n\
         Content-Type: application/json\r\n\r\n\
         {{\"status\":\"PASS\",\"prediction\":\"Normal\",\"system\":\"SystemV1\",\"network\":\"NetV1\",\"warnings\":[]}}\r\n\
         --{boundary}\r\n\
         Content-Disposition: form-data; name=\"frame_000001_mv.csv\"; filename=\"frame_000001_mv.csv\"\r\n\
         Content-Type: text/csv\r\n\r\n\
         time,ch1,ch2,ch3\r\n\
         0.0,0.1,0.2,0.3\r\n\
         0.004,0.11,0.21,0.31\r\n\
         --{boundary}\r\n\
         Content-Disposition: form-data; name=\"frame_000002_mv.json\"; filename=\"frame_000002_mv.json\"\r\n\
         Content-Type: application/json\r\n\r\n\
         {{\"source_frame\":\"frame_000002\",\"source_metadata\":{{\"device_id\":\"device01\",\"session_id\":\"session_upload_test\",\"csv_file\":\"frame_000002_mv.csv\",\"sample_rate_hz\":250.0,\"duration_seconds\":10.0}}}}\r\n\
         --{boundary}\r\n\
         Content-Disposition: form-data; name=\"frame_000002_prediction.json\"; filename=\"frame_000002_prediction.json\"\r\n\
         Content-Type: application/json\r\n\r\n\
         {{\"status\":\"PASS\",\"prediction\":\"Abnormal\",\"system\":\"SystemV1\",\"network\":\"NetV1\",\"warnings\":[\"Noise detect\"]}}\r\n\
         --{boundary}\r\n\
         Content-Disposition: form-data; name=\"frame_000002_mv.csv\"; filename=\"frame_000002_mv.csv\"\r\n\
         Content-Type: text/csv\r\n\r\n\
         time,ch1,ch2,ch3\r\n\
         10.0,0.5,0.6,0.7\r\n\
         10.004,0.51,0.61,0.71\r\n\
         --{boundary}--\r\n"
    );

    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/sessions/upload")
                .header("Content-Type", format!("multipart/form-data; boundary={}", boundary))
                .body(Body::from(body))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let body_bytes = axum::body::to_bytes(response.into_body(), 1024 * 10).await.unwrap();
    let res_json: serde_json::Value = serde_json::from_slice(&body_bytes).unwrap();
    assert!(res_json["success"].as_bool().unwrap());
    
    // session_id is auto-generated
    let generated_session_id = res_json["session_id"].as_str().unwrap().to_string();
    assert!(generated_session_id.starts_with("ses"));

    // Verify exactly 1 session and exactly 2 frame records were created in DB
    {
        let conn = state.pool.get().unwrap();
        let session_count: i64 = conn.query_row(
            "SELECT COUNT(*) FROM sessions WHERE id = ?1",
            [&generated_session_id],
            |row| row.get(0)
        ).unwrap();
        assert_eq!(session_count, 1, "There should be exactly 1 session created");
        
        let session_patient: String = conn.query_row(
            "SELECT patient_id FROM sessions WHERE id = ?1",
            [&generated_session_id],
            |row| row.get(0)
        ).unwrap();
        assert_eq!(session_patient, "pat_upload_test");

        let frame_count: i64 = conn.query_row(
            "SELECT COUNT(*) FROM frame_records WHERE session_id = ?1",
            [&generated_session_id],
            |row| row.get(0)
        ).unwrap();
        assert_eq!(frame_count, 2, "There should be exactly 2 frames recorded for the new session");
    }

    // Read the created .jsonl file and verify its contents
    let file_path = format!("records/{}.jsonl", generated_session_id);
    let file_content = std::fs::read_to_string(&file_path).expect("Failed to read generated .jsonl file");
    
    let mut lines = file_content.lines();
    
    // Check first frame
    let frame1_str = lines.next().expect("Expected first frame in .jsonl");
    let frame1: DevicePayload = serde_json::from_str(frame1_str).expect("Failed to parse frame 1");
    assert_eq!(frame1.frame_id, "frame_000001");
    assert_eq!(frame1.session_id, generated_session_id);
    assert_eq!(frame1.validation.status, "PASS");
    
    // Check second frame
    let frame2_str = lines.next().expect("Expected second frame in .jsonl");
    let frame2: DevicePayload = serde_json::from_str(frame2_str).expect("Failed to parse frame 2");
    assert_eq!(frame2.frame_id, "frame_000002");
    assert_eq!(frame2.session_id, generated_session_id);
    assert_eq!(frame2.validation.status, "PASS");

    // Clean up file
    let _ = std::fs::remove_file(&file_path);
    // Ignore error if records dir isn't empty
    let _ = std::fs::remove_dir("records");
}

#[tokio::test]
async fn test_db_sync_not_configured() {
    let (state, _pacer_rx, _db_rx) = setup_test_state();
    std::env::remove_var("DATABASE_URL");
    let result = ecg_backend::db::sync::sync_databases(&state.pool);
    assert!(result.is_err());
    assert_eq!(result.unwrap_err(), "DATABASE_URL tidak diatur di file .env");
>>>>>>> d4e4ff69c48c853c58f915b255502ea5f0968312
}
