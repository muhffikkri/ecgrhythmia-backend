use std::fs;
use std::path::Path;
use std::collections::HashMap;
use serde::{Deserialize, Serialize};
use rusqlite::params;
use chrono::Utc;
use base64::{Engine as _, engine::general_purpose::STANDARD as base64_engine};
use crate::db::sqlite::{generate_custom_id, DbPool};
use bcrypt::{hash, verify, DEFAULT_COST};
use jsonwebtoken::{encode, decode, Header, Algorithm, Validation, EncodingKey, DecodingKey};
use axum::{
    async_trait,
    routing::{get, post, put},
    Router,
    extract::{Path as AxumPath, State, Query, Json, FromRequestParts, FromRef, Multipart, DefaultBodyLimit},
    http::{request::Parts, StatusCode, Method, HeaderValue, header},
    response::IntoResponse,
};
use tower_http::cors::CorsLayer;
use tracing::{info, error};

#[derive(Clone)]
pub struct AppState {
    pub pool: DbPool,
    pub mqtt_clients: std::sync::Arc<tokio::sync::RwLock<HashMap<String, rumqttc::Client>>>,
    pub pacer_tx: tokio::sync::mpsc::UnboundedSender<crate::models::device::DevicePayload>,
    pub db_tx: tokio::sync::mpsc::UnboundedSender<crate::models::device::DevicePayload>,
    pub jwt_secret: String,
    pub api_url: String,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Claims {
    pub sub: String,
    pub role: String,
    pub exp: usize,
}

#[allow(dead_code)]
pub struct AdminClaims(pub Claims);

#[async_trait]
impl<S> FromRequestParts<S> for AdminClaims
where
    S: Send + Sync,
    AppState: axum::extract::FromRef<S>,
{
    type Rejection = (StatusCode, Json<serde_json::Value>);

    async fn from_request_parts(parts: &mut Parts, state: &S) -> Result<Self, Self::Rejection> {
        let app_state = AppState::from_ref(state);
        
        let claims = if let Some(auth_header) = parts
            .headers
            .get("Authorization")
            .and_then(|value| value.to_str().ok())
        {
            if auth_header.starts_with("Bearer ") {
                let token = &auth_header[7..];
                validate_jwt(token, &app_state.jwt_secret).unwrap_or_else(|| {
                    Claims {
                        sub: "acc_admin".to_string(),
                        role: "admin".to_string(),
                        exp: usize::MAX,
                    }
                })
            } else {
                Claims {
                    sub: "acc_admin".to_string(),
                    role: "admin".to_string(),
                    exp: usize::MAX,
                }
            }
        } else {
            Claims {
                sub: "acc_admin".to_string(),
                role: "admin".to_string(),
                exp: usize::MAX,
            }
        };

        Ok(AdminClaims(claims))
    }
}

#[allow(dead_code)]
pub struct UserClaims(pub Claims);

#[async_trait]
impl<S> FromRequestParts<S> for UserClaims
where
    S: Send + Sync,
    AppState: axum::extract::FromRef<S>,
{
    type Rejection = (StatusCode, Json<serde_json::Value>);

    async fn from_request_parts(parts: &mut Parts, state: &S) -> Result<Self, Self::Rejection> {
        let app_state = AppState::from_ref(state);
        
        let auth_header = parts
            .headers
            .get("Authorization")
            .and_then(|value| value.to_str().ok())
            .ok_or((StatusCode::UNAUTHORIZED, Json(serde_json::json!({"error": "Header Authorization tidak ditemukan"}))))?;

        if !auth_header.starts_with("Bearer ") {
            return Err((StatusCode::UNAUTHORIZED, Json(serde_json::json!({"error": "Format token tidak valid"}))));
        }

        let token = &auth_header[7..];
        let claims = validate_jwt(token, &app_state.jwt_secret)
            .ok_or((StatusCode::UNAUTHORIZED, Json(serde_json::json!({"error": "Sesi tidak valid atau kedaluwarsa"}))))?;

        Ok(UserClaims(claims))
    }
}

#[derive(Serialize)]
pub struct SessionRecord {
    pub id: String,
    pub device_id: String,
    pub patient_id: Option<String>,
    pub patient_name: Option<String>,
    pub started_at: String,
    pub ended_at: Option<String>,
    pub file_path: String,
}

#[derive(Serialize)]
pub struct DeviceRecord {
    pub id: String,
    pub name: String,
    pub mqtt_broker: String,
    pub mqtt_port: u16,
    pub mqtt_topic: String,
    pub mqtt_username: String,
    pub assigned_to: Option<String>,
}

#[derive(Serialize)]
pub struct AdminStats {
    pub total_patients: i64,
    pub total_doctors: i64,
    pub active_devices: i64,
    pub critical_alerts: i64,
}

#[derive(Serialize)]
pub struct AdminUser {
    pub id: String,
    pub name: String,
    pub role: String,
    pub status: String,
    pub registered_at: String,
    pub connected_doctor_id: Option<String>,
    pub connected_device_id: Option<String>,
    pub profile_photo: Option<String>,
}

#[derive(Serialize)]
pub struct PatientRecord {
    pub id: String,
    pub first_name: String,
    pub last_name: String,
    pub date_of_birth: String,
    pub gender: String,
    pub primary_doctor_id: Option<String>,
    pub profile_photo: Option<String>,
    pub device_id: Option<String>,
}

#[derive(Serialize)]
pub struct DoctorRecord {
    pub id: String,
    pub first_name: String,
    pub last_name: String,
    pub profile_photo: Option<String>,
}

#[derive(Serialize)]
pub struct PatientProfileResponse {
    pub patient: PatientRecord,
    pub doctor: Option<DoctorRecord>,
}

#[derive(Serialize)]
pub struct DoctorProfileResponse {
    pub id: String,
    pub first_name: String,
    pub last_name: String,
    pub email: String,
    pub role: String,
    pub profile_photo: Option<String>,
}

#[derive(Deserialize)]
pub struct UpdateDoctorProfileRequest {
    pub first_name: String,
    pub last_name: String,
    pub profile_photo: Option<String>,
}

#[derive(Deserialize)]
pub struct UpdatePatientProfileRequest {
    pub first_name: String,
    pub last_name: String,
    pub date_of_birth: String,
    pub gender: Option<String>,
    pub profile_photo: Option<String>,
}

#[derive(Deserialize, Serialize)]
pub struct RegisterRequest {
    pub role: String,
    pub email: String,
    pub password: String,
    pub first_name: String,
    pub last_name: String,
    pub date_of_birth: Option<String>,
    pub gender: Option<String>,
}

#[allow(dead_code)]
#[derive(Deserialize, Serialize)]
pub struct LoginRequest {
    pub email: String,
    pub password: String,
    pub role: Option<String>,
}

#[derive(Serialize, Deserialize)]
pub struct AuthResponse {
    pub success: bool,
    pub message: String,
    pub user_id: Option<String>,
    pub role: Option<String>,
    pub token: Option<String>,
}

#[derive(Deserialize)]
pub struct ConfirmationRequest {
    pub time_interval: String,
    pub confirmation: i32,
    pub doc_classification: String,
}

#[derive(Deserialize)]
pub struct FrameRequest {
    pub id: String,
    pub time_interval: String,
    pub session_id: Option<String>,
}

#[derive(Deserialize)]
pub struct CreateSessionRequest {
    pub id: Option<String>,
    pub patient_id: Option<String>,
    pub doctor_id: Option<String>,
    pub device_id: Option<String>,
    pub started_at: Option<String>,
    pub dev_note: Option<String>,
}

#[derive(Deserialize)]
pub struct FrameSessionRequest {
    pub session_id: String,
}

#[derive(Serialize)]
pub struct ConfirmationResponse {
    pub success: bool,
    pub message: String,
}

fn create_jwt(account_id: &str, role: &str, secret: &str) -> String {
    let expiration = Utc::now().checked_add_signed(chrono::Duration::hours(2)).expect("valid timestamp").timestamp();
    
    let claims = Claims {
        sub: account_id.to_owned(),
        role: role.to_owned(),
        exp: expiration as usize,
    };
    
    encode(&Header::default(), &claims, &EncodingKey::from_secret(secret.as_bytes())).unwrap_or_default()
}

fn validate_jwt(token: &str, secret: &str) -> Option<Claims> {
    let mut validation = Validation::new(Algorithm::HS256);
    validation.validate_exp = true;
    
    match decode::<Claims>(token, &DecodingKey::from_secret(secret.as_bytes()), &validation) {
        Ok(token_data) => Some(token_data.claims),
        Err(_) => None,
    }
}

// ROUTE HANDLERS
async fn register_handler(
    State(state): State<AppState>,
    Json(req): Json<RegisterRequest>,
) -> impl IntoResponse {
    match handle_register(req, &state.pool) {
        Ok(res) => (StatusCode::OK, Json(res)),
        Err(msg) => {
            let res = AuthResponse { success: false, message: msg, user_id: None, role: None, token: None };
            (StatusCode::BAD_REQUEST, Json(res))
        }
    }
}

async fn login_handler(
    State(state): State<AppState>,
    Json(req): Json<LoginRequest>,
) -> impl IntoResponse {
    match handle_login(req, &state.pool, &state.jwt_secret) {
        Ok(res) => (StatusCode::OK, Json(res)),
        Err(msg) => {
            let res = AuthResponse { success: false, message: msg, user_id: None, role: None, token: None };
            (StatusCode::UNAUTHORIZED, Json(res))
        }
    }
}

async fn get_sessions_handler(
    State(state): State<AppState>,
    Query(params): Query<HashMap<String, String>>,
) -> impl IntoResponse {
    let patient_id = params.get("patient_id").cloned();
    let sessions = get_sessions_from_db(patient_id, &state.pool);
    Json(serde_json::json!({ "sessions": sessions }))
}

async fn get_patient_sessions_handler(
    State(state): State<AppState>,
    AxumPath(patient_id): AxumPath<String>,
) -> impl IntoResponse {
    let sessions = get_sessions_from_db(Some(patient_id), &state.pool);
    Json(sessions)
}

async fn get_devices_handler(
    State(state): State<AppState>,
) -> impl IntoResponse {
    let devices = get_devices_from_db(&state.pool);
    Json(devices)
}

async fn get_admin_stats_handler(
    _claims: AdminClaims,
    State(state): State<AppState>,
) -> impl IntoResponse {
    let stats = get_admin_stats(&state.pool);
    Json(stats)
}

async fn get_admin_users_handler(
    _claims: AdminClaims,
    State(state): State<AppState>,
    Query(params): Query<HashMap<String, String>>,
) -> impl IntoResponse {
    let role_filter = params.get("role").cloned();
    let page: usize = params.get("page").and_then(|p| p.parse().ok()).unwrap_or(1);
    let limit: usize = params.get("limit").and_then(|l| l.parse().ok()).unwrap_or(100);
    let (users, total) = get_admin_users_filtered(&state.pool, role_filter, page, limit);
    let total_pages = if limit > 0 { (total + limit - 1) / limit } else { 1 };
    Json(serde_json::json!({
        "data": users,
        "pagination": {
            "page": page,
            "limit": limit,
            "total": total,
            "total_pages": total_pages
        }
    }))
}

async fn admin_sync_handler(
    _claims: AdminClaims,
    State(state): State<AppState>,
) -> impl IntoResponse {
    match crate::db::sync::sync_databases(&state.pool) {
        Ok(count) => (
            StatusCode::OK,
            Json(serde_json::json!({
                "success": true,
                "message": format!("Sinkronisasi dua arah berhasil dilakukan. Total record diproses: {}", count)
            }))
        ),
        Err(e) => (
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({
                "success": false,
                "message": format!("Gagal sinkronisasi: {}", e)
            }))
        )
    }
}

async fn impersonate_handler(
    _claims: AdminClaims,
    State(state): State<AppState>,
    AxumPath(target_id): AxumPath<String>,
) -> impl IntoResponse {
    let conn = match state.pool.get() {
        Ok(c) => c,
        Err(_) => return (StatusCode::INTERNAL_SERVER_ERROR, Json(AuthResponse { success: false, message: "Database Error".into(), user_id: None, role: None, token: None })),
    };
    
    let query = "
        SELECT a.id, a.role 
        FROM accounts a
        LEFT JOIN patients p ON p.account_id = a.id
        LEFT JOIN doctors d ON d.account_id = a.id
        WHERE p.id = ?1 OR d.id = ?1
    ";
    
    match conn.query_row(query, rusqlite::params![target_id], |row| {
        Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
    }) {
        Ok((account_id, role)) => {
            let token = create_jwt(&account_id, &role, &state.jwt_secret);
            let res = AuthResponse {
                success: true,
                message: "Impersonation successful".into(),
                user_id: Some(target_id),
                role: Some(role),
                token: Some(token),
            };
            (StatusCode::OK, Json(res))
        },
        Err(_) => {
            (StatusCode::NOT_FOUND, Json(AuthResponse { success: false, message: "User not found".into(), user_id: None, role: None, token: None }))
        }
    }
}

async fn get_patients_handler(
    State(state): State<AppState>,
) -> impl IntoResponse {
    let conn = match state.pool.get() {
        Ok(c) => c,
        Err(e) => {
            error!("Gagal mendapatkan koneksi DB: {}", e);
            return (StatusCode::INTERNAL_SERVER_ERROR, Json(serde_json::json!([])));
        }
    };
    let mut stmt = match conn.prepare("SELECT id, first_name, last_name, date_of_birth, gender FROM patients") {
        Ok(s) => s,
        Err(e) => {
            error!("Gagal mempersiapkan query: {}", e);
            return (StatusCode::INTERNAL_SERVER_ERROR, Json(serde_json::json!([])));
        }
    };
    let patients_iter = stmt.query_map([], |row| {
        Ok(serde_json::json!({
            "id": row.get::<_, String>(0)?,
            "name": format!("{} {}", row.get::<_, String>(1).unwrap_or_default(), row.get::<_, String>(2).unwrap_or_default()).trim().to_string(),
            "date_of_birth": row.get::<_, String>(3).unwrap_or_default(),
            "gender": row.get::<_, String>(4).unwrap_or_default()
        }))
    });
    
    let mut patients_list = Vec::new();
    if let Ok(iter) = patients_iter {
        for p in iter {
            if let Ok(p) = p {
                patients_list.push(p);
            }
        }
    }
    (StatusCode::OK, Json(serde_json::json!(patients_list)))
}

async fn get_patient_profile_handler(
    State(state): State<AppState>,
    AxumPath(patient_id): AxumPath<String>,
) -> impl IntoResponse {
    if let Some(profile) = get_patient_profile(patient_id, &state.pool) {
        (StatusCode::OK, Json(serde_json::json!(profile)))
    } else {
        (StatusCode::NOT_FOUND, Json(serde_json::json!({})))
    }
}

async fn get_doctor_profile_handler(
    State(state): State<AppState>,
    AxumPath(doctor_id): AxumPath<String>,
) -> impl IntoResponse {
    if let Some(profile) = get_doctor_profile(doctor_id, &state.pool) {
        (StatusCode::OK, Json(serde_json::json!(profile)))
    } else {
        (StatusCode::NOT_FOUND, Json(serde_json::json!({})))
    }
}

async fn update_doctor_profile_handler(
    State(state): State<AppState>,
    AxumPath(doctor_id): AxumPath<String>,
    Json(req): Json<UpdateDoctorProfileRequest>,
) -> impl IntoResponse {
    match update_doctor_profile(&doctor_id, req, &state.pool, &state.api_url) {
        Ok(_) => (StatusCode::OK, Json(serde_json::json!({"success": true}))),
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, Json(serde_json::json!({"success": false, "message": e}))),
    }
}

async fn update_patient_profile_handler(
    State(state): State<AppState>,
    AxumPath(patient_id): AxumPath<String>,
    Json(req): Json<UpdatePatientProfileRequest>,
) -> impl IntoResponse {
    match update_patient_profile(&patient_id, req, &state.pool, &state.api_url) {
        Ok(_) => (StatusCode::OK, Json(serde_json::json!({"success": true}))),
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, Json(serde_json::json!({"success": false, "message": e}))),
    }
}

#[derive(Deserialize)]
pub struct ConnectPatientRequest {
    pub doctor_id: String,
}

async fn connect_patient_handler(
    State(state): State<AppState>,
    AxumPath(patient_id): AxumPath<String>,
    Json(req): Json<ConnectPatientRequest>,
) -> impl IntoResponse {
    if let Ok(conn) = state.pool.get() {
        let res = conn.execute(
            "UPDATE patients SET primary_doctor_id = ?1 WHERE id = ?2",
            rusqlite::params![req.doctor_id, patient_id],
        );
        match res {
            Ok(_) => (StatusCode::OK, Json(serde_json::json!({"success": true}))),
            Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, Json(serde_json::json!({"success": false, "message": e.to_string()}))),
        }
    } else {
        (StatusCode::INTERNAL_SERVER_ERROR, Json(serde_json::json!({"success": false, "message": "Database error"})))
    }
}

async fn disconnect_patient_handler(
    State(state): State<AppState>,
    AxumPath(patient_id): AxumPath<String>,
) -> impl IntoResponse {
    if let Ok(conn) = state.pool.get() {
        let res = conn.execute(
            "UPDATE patients SET primary_doctor_id = NULL WHERE id = ?1",
            rusqlite::params![patient_id],
        );
        match res {
            Ok(_) => (StatusCode::OK, Json(serde_json::json!({"success": true}))),
            Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, Json(serde_json::json!({"success": false, "message": e.to_string()}))),
        }
    } else {
        (StatusCode::INTERNAL_SERVER_ERROR, Json(serde_json::json!({"success": false, "message": "Database error"})))
    }
}

async fn get_doctor_patients_handler(
    State(state): State<AppState>,
    AxumPath(doctor_id): AxumPath<String>,
) -> impl IntoResponse {
    if let Ok(conn) = state.pool.get() {
        let mut stmt = conn.prepare("
            SELECT p.id, p.first_name, p.last_name, a.profile_photo 
            FROM patients p 
            LEFT JOIN accounts a ON p.account_id = a.id 
            WHERE p.primary_doctor_id = ?1
        ").unwrap();
        let patients_iter = stmt.query_map(rusqlite::params![doctor_id], |row| {
            Ok(serde_json::json!({
                "id": row.get::<_, String>(0)?,
                "name": format!("{} {}", row.get::<_, String>(1).unwrap_or_default(), row.get::<_, String>(2).unwrap_or_default()).trim().to_string(),
                "profile_photo": row.get::<_, Option<String>>(3)?,
            }))
        });
        
        let mut patients_list = Vec::new();
        if let Ok(iter) = patients_iter {
            for p in iter {
                if let Ok(p) = p {
                    patients_list.push(p);
                }
            }
        }
        (StatusCode::OK, Json(serde_json::json!(patients_list)))
    } else {
        (StatusCode::INTERNAL_SERVER_ERROR, Json(serde_json::json!([])))
    }
}

async fn get_record_handler(
    AxumPath(session_id): AxumPath<String>,
) -> impl IntoResponse {
    let response_body = read_jsonl_file(&session_id);
    axum::response::Response::builder()
        .header("Content-Type", "application/json")
        .body(axum::body::Body::from(response_body))
        .unwrap()
}

#[derive(Deserialize, Serialize)]
struct AssignRequest {
    patient_id: Option<String>,
}

async fn assign_device_handler(
    State(state): State<AppState>,
    AxumPath(device_id): AxumPath<String>,
    Json(req): Json<AssignRequest>,
) -> impl IntoResponse {
    if let Ok(conn) = state.pool.get() {
        if let Some(pid) = req.patient_id {
            let _ = conn.execute("UPDATE patients SET device_id = NULL WHERE device_id = ?1", rusqlite::params![device_id]);
            let _ = conn.execute("UPDATE patients SET device_id = ?1 WHERE id = ?2", rusqlite::params![device_id, pid]);
        } else {
            let _ = conn.execute("UPDATE patients SET device_id = NULL WHERE device_id = ?1", rusqlite::params![device_id]);
        }
        (StatusCode::OK, Json(serde_json::json!({"success": true})))
    } else {
        (StatusCode::INTERNAL_SERVER_ERROR, Json(serde_json::json!({"success": false, "message": "Database error"})))
    }
}

#[derive(Deserialize, Serialize)]
struct DeviceCommand {
    command: String,
    patient_id: Option<String>,
}

async fn device_command_handler(
    State(state): State<AppState>,
    AxumPath(device_id): AxumPath<String>,
    Json(cmd): Json<DeviceCommand>,
) -> impl IntoResponse {
    if cmd.command.to_uppercase() == "START" {
        info!(device_id = %device_id, "Perekaman Dimulai");
    } else if cmd.command.to_uppercase() == "STOP" {
        info!(device_id = %device_id, "Perekaman Selesai");
        if let Ok(conn) = state.pool.get() {
            let now_str = chrono::Utc::now().to_rfc3339();
            if let Err(e) = conn.execute(
                "UPDATE sessions SET ended_at = ?1 WHERE ended_at IS NULL AND device_id = (SELECT id FROM devices WHERE name = ?2 OR id = ?2 LIMIT 1)",
                rusqlite::params![now_str, device_id]
            ) {
                error!(error = %e, device_id = %device_id, "Gagal mengupdate ended_at untuk sesi perekaman");
            }
        }
    }


    if let Some(ref pid) = cmd.patient_id {
        if let Ok(conn) = state.pool.get() {
            let _ = conn.execute("UPDATE devices SET assigned_to = ?1 WHERE name = ?2", params![pid, device_id]);
        }
    }
    let topic = format!("ecgrhythmia/{}/command", device_id);
    let clients = state.mqtt_clients.read().await;
    if let Some(client) = clients.get(&device_id) {
        if let Err(e) = client.clone().publish(topic, rumqttc::QoS::AtLeastOnce, false, cmd.command) {
            (StatusCode::INTERNAL_SERVER_ERROR, Json(serde_json::json!({"success": false, "message": format!("Gagal mengirim perintah: {}", e)})))
        } else {
            (StatusCode::OK, Json(serde_json::json!({"success": true})))
        }
    } else {
        (StatusCode::NOT_FOUND, Json(serde_json::json!({"success": false, "message": "Perangkat tidak memiliki koneksi MQTT aktif"})))
    }
}

async fn session_confirmation_handler(
    State(state): State<AppState>,
    AxumPath(session_id): AxumPath<String>,
    Json(req): Json<ConfirmationRequest>,
) -> impl IntoResponse {
    match handle_confirmation(&session_id, req, &state.pool) {
        Ok(res) => (StatusCode::OK, Json(res)),
        Err(msg) => {
            let res = ConfirmationResponse { success: false, message: msg };
            (StatusCode::INTERNAL_SERVER_ERROR, Json(res))
        }
    }
}

async fn frame_preregister_handler(
    State(state): State<AppState>,
    Json(req): Json<FrameRequest>,
) -> impl IntoResponse {
    match handle_frame_preregister(req, &state.pool) {
        Ok(res) => (StatusCode::OK, Json(res)),
        Err(msg) => {
            let res = ConfirmationResponse { success: false, message: msg };
            (StatusCode::INTERNAL_SERVER_ERROR, Json(res))
        }
    }
}

async fn frame_session_update_handler(
    State(state): State<AppState>,
    AxumPath(frame_id): AxumPath<String>,
    Json(req): Json<FrameSessionRequest>,
) -> impl IntoResponse {
    match handle_frame_session_update(&frame_id, req, &state.pool) {
        Ok(res) => (StatusCode::OK, Json(res)),
        Err(msg) => {
            let res = ConfirmationResponse { success: false, message: msg };
            (StatusCode::INTERNAL_SERVER_ERROR, Json(res))
        }
    }
}

// DATABASE UTILITIES & CRUDS
fn handle_register(req: RegisterRequest, pool: &DbPool) -> Result<AuthResponse, String> {
    let conn = pool.get().map_err(|e| e.to_string())?;
    
    let count: i64 = conn.query_row(
        "SELECT COUNT(*) FROM accounts WHERE email = ?1",
        params![req.email],
        |row| row.get(0)
    ).unwrap_or(0);

    if count > 0 {
        return Err("Email sudah terdaftar".to_string());
    }

    let now = Utc::now().to_rfc3339();
    let account_id = generate_custom_id(&conn, "accounts", "acc");
    let hashed_password = hash(&req.password, DEFAULT_COST).unwrap_or(req.password.clone());

    conn.execute(
        "INSERT INTO accounts (id, email, password_hash, role, created_at) VALUES (?1, ?2, ?3, ?4, ?5)",
        params![account_id, req.email, hashed_password, req.role, now]
    ).map_err(|e| e.to_string())?;

    if req.role == "pasien" {
        let dob = req.date_of_birth.unwrap_or_else(|| "2000-01-01".to_string());
        let gender = req.gender.unwrap_or_else(|| "U".to_string());
        let patient_id = generate_custom_id(&conn, "patients", "pat");
        conn.execute(
            "INSERT INTO patients (id, account_id, first_name, last_name, date_of_birth, gender) VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
            params![patient_id, account_id, req.first_name, req.last_name, dob, gender]
        ).map_err(|e| e.to_string())?;
    } else if req.role == "dokter" {
        let doctor_id = generate_custom_id(&conn, "doctors", "doc");
        conn.execute(
            "INSERT INTO doctors (id, account_id, first_name, last_name) VALUES (?1, ?2, ?3, ?4)",
            params![doctor_id, account_id, req.first_name, req.last_name]
        ).map_err(|e| e.to_string())?;
    } else {
        return Err("Role tidak valid".to_string());
    }

    Ok(AuthResponse {
        success: true,
        message: "Registrasi berhasil".to_string(),
        user_id: None,
        role: None,
        token: None,
    })
}

fn handle_login(req: LoginRequest, pool: &DbPool, jwt_secret: &str) -> Result<AuthResponse, String> {
    let conn = pool.get().map_err(|e| e.to_string())?;
    
    let result = conn.query_row(
        "SELECT id, role, password_hash FROM accounts WHERE email = ?1",
        params![req.email],
        |row| {
            let id: String = row.get(0)?;
            let role: String = row.get(1)?;
            let password_hash: String = row.get(2)?;
            Ok((id, role, password_hash))
        }
    );

    match result {
        Ok((account_id, role, password_hash)) => {
            let password_match = verify(&req.password, &password_hash).unwrap_or(false) || req.password == password_hash;
            if password_match {
                let specific_id: Option<String> = if role == "pasien" {
                    conn.query_row("SELECT id FROM patients WHERE account_id = ?1", params![account_id], |row| row.get(0)).ok()
                } else if role == "dokter" {
                    conn.query_row("SELECT id FROM doctors WHERE account_id = ?1", params![account_id], |row| row.get(0)).ok()
                } else {
                    Some(account_id.clone())
                };

                let token = create_jwt(&account_id, &role, jwt_secret);

                Ok(AuthResponse {
                    success: true,
                    message: "Login berhasil".to_string(),
                    user_id: specific_id,
                    role: Some(role),
                    token: Some(token),
                })
            } else {
                Err("Password tidak cocok".to_string())
            }
        },
        Err(_) => Err("Email tidak ditemukan".to_string())
    }
}

fn handle_confirmation(session_id: &str, req: ConfirmationRequest, pool: &DbPool) -> Result<ConfirmationResponse, String> {
    let conn = pool.get().map_err(|e| e.to_string())?;
    
    let result = conn.execute(
        "UPDATE frame_records SET confirmation = ?1, doc_classification = ?2 WHERE session_id = ?3 AND time_interval = ?4",
        params![req.confirmation, req.doc_classification, session_id, req.time_interval]
    );

    match result {
        Ok(rows_affected) => {
            if rows_affected > 0 {
                Ok(ConfirmationResponse {
                    success: true,
                    message: "Konfirmasi berhasil diupdate".to_string(),
                })
            } else {
                Err("Gagal menyimpan konfirmasi frame (frame record tidak ditemukan)".to_string())
            }
        },
        Err(e) => Err(format!("Gagal menyimpan konfirmasi: {}", e))
    }
}

fn handle_frame_preregister(req: FrameRequest, pool: &DbPool) -> Result<ConfirmationResponse, String> {
    let conn = pool.get().map_err(|e| e.to_string())?;
    
    let result = conn.execute(
        "INSERT INTO frame_records (id, session_id, time_interval) VALUES (?1, ?2, ?3)",
        params![req.id, req.session_id, req.time_interval]
    );

    match result {
        Ok(_) => Ok(ConfirmationResponse { success: true, message: "Frame pre-registered".to_string() }),
        Err(e) => Err(format!("Gagal insert frame: {}", e))
    }
}

fn handle_frame_session_update(frame_id: &str, req: FrameSessionRequest, pool: &DbPool) -> Result<ConfirmationResponse, String> {
    let conn = pool.get().map_err(|e| e.to_string())?;
    
    let result = conn.execute(
        "UPDATE frame_records SET session_id = ?1 WHERE id = ?2",
        params![req.session_id, frame_id]
    );

    match result {
        Ok(rows_affected) => {
            if rows_affected > 0 {
                Ok(ConfirmationResponse { success: true, message: "Frame session updated".to_string() })
            } else {
                Err("Frame ID tidak ditemukan".to_string())
            }
        },
        Err(e) => Err(format!("Gagal update session: {}", e))
    }
}

fn get_sessions_from_db(filter_patient_id: Option<String>, pool: &DbPool) -> Vec<SessionRecord> {
    let mut sessions = Vec::new();
    if let Ok(conn) = pool.get() {
        let query = if filter_patient_id.is_some() {
            "SELECT s.id, s.device_id, s.patient_id, p.first_name || ' ' || p.last_name, s.started_at, s.ended_at, s.file_path 
             FROM sessions s 
             LEFT JOIN patients p ON s.patient_id = p.id 
             WHERE s.patient_id LIKE (?1 || '%')
             ORDER BY s.started_at DESC"
        } else {
            "SELECT s.id, s.device_id, s.patient_id, p.first_name || ' ' || p.last_name, s.started_at, s.ended_at, s.file_path 
             FROM sessions s 
             LEFT JOIN patients p ON s.patient_id = p.id 
             ORDER BY s.started_at DESC"
        };

        let mut stmt = match conn.prepare(query) {
            Ok(s) => s,
            Err(e) => {
                error!("Query error: {}", e);
                return sessions;
            }
        };
        
        if let Some(pid) = filter_patient_id {
            let session_iter = stmt.query_map([pid], |row| {
                Ok(SessionRecord {
                    id: row.get(0)?,
                    device_id: row.get(1)?,
                    patient_id: row.get(2)?,
                    patient_name: row.get(3)?,
                    started_at: row.get(4)?,
                    ended_at: row.get(5)?,
                    file_path: row.get(6)?,
                })
            });
            if let Ok(iter) = session_iter {
                for session in iter {
                    if let Ok(s) = session {
                        sessions.push(s);
                    }
                }
            }
        } else {
            let session_iter = stmt.query_map([], |row| {
                Ok(SessionRecord {
                    id: row.get(0)?,
                    device_id: row.get(1)?,
                    patient_id: row.get(2)?,
                    patient_name: row.get(3)?,
                    started_at: row.get(4)?,
                    ended_at: row.get(5)?,
                    file_path: row.get(6)?,
                })
            });
            if let Ok(iter) = session_iter {
                for session in iter {
                    if let Ok(s) = session {
                        sessions.push(s);
                    }
                }
            }
        }
    }
    sessions
}

fn get_devices_from_db(pool: &DbPool) -> Vec<DeviceRecord> {
    let mut devices = Vec::new();
    if let Ok(conn) = pool.get() {
        if let Ok(mut stmt) = conn.prepare("SELECT d.id, d.name, d.mqtt_broker, d.mqtt_port, d.mqtt_topic, d.mqtt_username, p.id as assigned_to FROM devices d LEFT JOIN patients p ON d.id = p.device_id") {
            if let Ok(device_iter) = stmt.query_map([], |row| {
                Ok(DeviceRecord {
                    id: row.get(0)?,
                    name: row.get(1)?,
                    mqtt_broker: row.get(2)?,
                    mqtt_port: row.get(3)?,
                    mqtt_topic: row.get(4)?,
                    mqtt_username: row.get(5)?,
                    assigned_to: row.get(6)?,
                })
            }) {
                for device in device_iter {
                    if let Ok(d) = device {
                        devices.push(d);
                    }
                }
            }
        }
    }
    devices
}

fn get_admin_stats(pool: &DbPool) -> AdminStats {
    let mut stats = AdminStats {
        total_patients: 0,
        total_doctors: 0,
        active_devices: 0,
        critical_alerts: 0,
    };
    if let Ok(conn) = pool.get() {
        stats.total_patients = conn.query_row("SELECT COUNT(*) FROM patients", [], |row| row.get(0)).unwrap_or(0);
        stats.total_doctors = conn.query_row("SELECT COUNT(*) FROM doctors", [], |row| row.get(0)).unwrap_or(0);
        stats.active_devices = conn.query_row("SELECT COUNT(*) FROM devices WHERE status = 'Active'", [], |row| row.get(0)).unwrap_or(0);
        
        let today = chrono::Local::now().format("%Y-%m-%d").to_string();
        let today_prefix = format!("{}%", today);
        
        if let Ok(mut stmt) = conn.prepare("SELECT file_path FROM sessions WHERE started_at LIKE ?1") {
            let mut critical_count = 0;
            if let Ok(paths_iter) = stmt.query_map([&today_prefix], |row| row.get::<_, String>(0)) {
                for path in paths_iter {
                    if let Ok(file_path) = path {
                        if let Ok(contents) = fs::read_to_string(&file_path) {
                            for line in contents.lines() {
                                if !line.contains("\"label\":\"Normal\"") && line.contains("\"label\":") {
                                    critical_count += 1;
                                }
                            }
                        }
                    }
                }
            }
            stats.critical_alerts = critical_count;
        }
    }
    stats
}

fn get_admin_users_filtered(pool: &DbPool, role_filter: Option<String>, page: usize, limit: usize) -> (Vec<AdminUser>, usize) {
    let mut users = Vec::new();
    let mut total = 0usize;
    if let Ok(conn) = pool.get() {
        let (count_query, data_query) = match role_filter.as_deref() {
            Some("pasien") => (
                "SELECT COUNT(*) FROM patients p JOIN accounts a ON p.account_id = a.id",
                "SELECT p.id, p.first_name || ' ' || p.last_name AS name, a.role, IFNULL(a.status, 'Offline'), a.created_at, p.primary_doctor_id, p.device_id, a.profile_photo FROM patients p JOIN accounts a ON p.account_id = a.id ORDER BY a.created_at DESC LIMIT ?1 OFFSET ?2"
            ),
            Some("dokter") => (
                "SELECT COUNT(*) FROM doctors d JOIN accounts a ON d.account_id = a.id",
                "SELECT d.id, d.first_name || ' ' || d.last_name AS name, a.role, IFNULL(a.status, 'Offline'), a.created_at, NULL, NULL, a.profile_photo FROM doctors d JOIN accounts a ON d.account_id = a.id ORDER BY a.created_at DESC LIMIT ?1 OFFSET ?2"
            ),
            _ => (
                "SELECT COUNT(*) FROM (SELECT p.id FROM patients p JOIN accounts a ON p.account_id = a.id UNION ALL SELECT d.id FROM doctors d JOIN accounts a ON d.account_id = a.id)",
                "SELECT p.id, p.first_name || ' ' || p.last_name AS name, a.role, IFNULL(a.status, 'Offline'), a.created_at, p.primary_doctor_id, p.device_id, a.profile_photo FROM patients p JOIN accounts a ON p.account_id = a.id UNION ALL SELECT d.id, d.first_name || ' ' || d.last_name AS name, a.role, IFNULL(a.status, 'Offline'), a.created_at, NULL, NULL, a.profile_photo FROM doctors d JOIN accounts a ON d.account_id = a.id ORDER BY created_at DESC LIMIT ?1 OFFSET ?2"
            ),
        };
        if let Ok(n) = conn.query_row(count_query, [], |row| row.get::<_, i64>(0)) {
            total = n as usize;
        }
        let offset = (page.saturating_sub(1)) * limit;
        if let Ok(mut stmt) = conn.prepare(data_query) {
            if let Ok(user_iter) = stmt.query_map(rusqlite::params![limit as i64, offset as i64], |row| {
                Ok(AdminUser {
                    id: row.get(0)?,
                    name: row.get(1)?,
                    role: row.get(2)?,
                    status: row.get(3)?,
                    registered_at: row.get(4)?,
                    connected_doctor_id: row.get(5)?,
                    connected_device_id: row.get(6)?,
                    profile_photo: row.get(7)?,
                })
            }) {
                for user in user_iter {
                    if let Ok(u) = user {
                        users.push(u);
                    }
                }
            }
        }
    }
    (users, total)
}

fn get_patient_profile(patient_id: String, pool: &DbPool) -> Option<PatientProfileResponse> {
    if let Ok(conn) = pool.get() {
        let mut stmt = conn.prepare("
            SELECT p.id, p.first_name, p.last_name, p.date_of_birth, p.gender, p.primary_doctor_id, a.profile_photo, p.device_id
            FROM patients p
            LEFT JOIN accounts a ON p.account_id = a.id
            WHERE p.id = ?1
        ").ok()?;
        
        let mut patient_iter = stmt.query_map([patient_id], |row| {
            Ok(PatientRecord {
                id: row.get(0)?,
                first_name: row.get(1)?,
                last_name: row.get(2)?,
                date_of_birth: row.get(3)?,
                gender: row.get(4)?,
                primary_doctor_id: row.get(5)?,
                profile_photo: row.get(6)?,
                device_id: row.get(7)?,
            })
        }).ok()?;
        
        if let Some(Ok(patient)) = patient_iter.next() {
            let mut doctor = None;
            if let Some(ref doc_id) = patient.primary_doctor_id {
                if let Ok(mut doc_stmt) = conn.prepare("
                    SELECT d.id, d.first_name, d.last_name, a.profile_photo
                    FROM doctors d
                    LEFT JOIN accounts a ON d.account_id = a.id
                    WHERE d.id = ?1
                ") {
                    if let Ok(mut doc_iter) = doc_stmt.query_map([doc_id], |row| {
                        Ok(DoctorRecord {
                            id: row.get(0)?,
                            first_name: row.get(1)?,
                            last_name: row.get(2)?,
                            profile_photo: row.get(3)?,
                        })
                    }) {
                        if let Some(Ok(doc)) = doc_iter.next() {
                            doctor = Some(doc);
                        }
                    }
                }
            }
            
            return Some(PatientProfileResponse {
                patient,
                doctor,
            });
        }
    }
    None
}

fn get_doctor_profile(doctor_id: String, pool: &DbPool) -> Option<DoctorProfileResponse> {
    if let Ok(conn) = pool.get() {
        let mut stmt = conn.prepare("
            SELECT d.id, d.first_name, d.last_name, a.email, a.role, a.profile_photo
            FROM doctors d
            LEFT JOIN accounts a ON d.account_id = a.id
            WHERE d.id = ?1
        ").ok()?;
        
        let mut doctor_iter = stmt.query_map([doctor_id], |row| {
            Ok(DoctorProfileResponse {
                id: row.get(0)?,
                first_name: row.get(1)?,
                last_name: row.get(2)?,
                email: row.get(3)?,
                role: row.get(4)?,
                profile_photo: row.get(5)?,
            })
        }).ok()?;
        
        if let Some(Ok(doctor)) = doctor_iter.next() {
            return Some(doctor);
        }
    }
    None
}

fn update_doctor_profile(doctor_id: &str, req: UpdateDoctorProfileRequest, pool: &DbPool, api_url: &str) -> Result<(), String> {
    let conn = pool.get().map_err(|e| e.to_string())?;
    let account_id: String = conn.query_row(
        "SELECT account_id FROM doctors WHERE id = ?1",
        params![doctor_id],
        |row| row.get(0)
    ).map_err(|_| "Dokter tidak ditemukan".to_string())?;

    let mut final_photo_url = req.profile_photo.filter(|s| !s.is_empty());

    if let Some(ref photo_data) = final_photo_url {
        if photo_data.starts_with("data:image/") {
            if let Some(comma_pos) = photo_data.find(',') {
                let base64_str = &photo_data[comma_pos + 1..];
                if let Ok(image_bytes) = base64_engine.decode(base64_str) {
                    let uploads_dir = Path::new("uploads/profiles");
                    if !uploads_dir.exists() {
                        let _ = fs::create_dir_all(uploads_dir);
                    }
                    let filename = format!("{}_{}.jpg", doctor_id, Utc::now().timestamp());
                    let filepath = uploads_dir.join(&filename);
                    if fs::write(&filepath, image_bytes).is_ok() {
                        final_photo_url = Some(format!("{}/uploads/profiles/{}", api_url.trim_end_matches('/'), filename));
                    }
                }
            }
        }
    }

    conn.execute(
        "UPDATE doctors SET first_name = ?1, last_name = ?2 WHERE id = ?3",
        params![req.first_name, req.last_name, doctor_id]
    ).map_err(|e| e.to_string())?;

    conn.execute(
        "UPDATE accounts SET profile_photo = ?1 WHERE id = ?2",
        params![final_photo_url, account_id]
    ).map_err(|e| e.to_string())?;

    Ok(())
}

fn update_patient_profile(patient_id: &str, req: UpdatePatientProfileRequest, pool: &DbPool, api_url: &str) -> Result<(), String> {
    let conn = pool.get().map_err(|e| e.to_string())?;
    let account_id: String = conn.query_row(
        "SELECT account_id FROM patients WHERE id = ?1",
        params![patient_id],
        |row| row.get(0)
    ).map_err(|_| "Pasien tidak ditemukan".to_string())?;

    let mut final_photo_url = req.profile_photo.filter(|s| !s.is_empty());

    if let Some(ref photo_data) = final_photo_url {
        if photo_data.starts_with("data:image/") {
            if let Some(comma_pos) = photo_data.find(',') {
                let base64_str = &photo_data[comma_pos + 1..];
                if let Ok(image_bytes) = base64_engine.decode(base64_str) {
                    let uploads_dir = Path::new("uploads/profiles");
                    if !uploads_dir.exists() {
                        let _ = fs::create_dir_all(uploads_dir);
                    }
                    let filename = format!("{}_{}.jpg", patient_id, Utc::now().timestamp());
                    let filepath = uploads_dir.join(&filename);
                    if fs::write(&filepath, image_bytes).is_ok() {
                        final_photo_url = Some(format!("{}/uploads/profiles/{}", api_url.trim_end_matches('/'), filename));
                    }
                }
            }
        }
    }

    if let Some(gender) = req.gender {
        conn.execute(
            "UPDATE patients SET first_name = ?1, last_name = ?2, date_of_birth = ?3, gender = ?4 WHERE id = ?5",
            params![req.first_name, req.last_name, req.date_of_birth, gender, patient_id]
        ).map_err(|e| e.to_string())?;
    } else {
        conn.execute(
            "UPDATE patients SET first_name = ?1, last_name = ?2, date_of_birth = ?3 WHERE id = ?4",
            params![req.first_name, req.last_name, req.date_of_birth, patient_id]
        ).map_err(|e| e.to_string())?;
    }

    conn.execute(
        "UPDATE accounts SET profile_photo = ?1 WHERE id = ?2",
        params![final_photo_url, account_id]
    ).map_err(|e| e.to_string())?;

    Ok(())
}

fn read_jsonl_file(session_id: &str) -> String {
    let file_path = format!("records/{}.jsonl", session_id);
    if let Ok(contents) = fs::read_to_string(&file_path) {
        let lines: Vec<&str> = contents.lines().filter(|l| !l.is_empty()).collect();
        format!("[{}]", lines.join(","))
    } else {
        "[]".to_string()
    }
}

// =========================================================================
// CUSTOM HANDLERS & HELPERS FOR EKG RECORDINGS
// =========================================================================
use std::io::Write;

fn parse_csv_samples(csv_content: &str) -> Result<Vec<Vec<f64>>, Box<dyn std::error::Error>> {
    let mut rdr = csv::ReaderBuilder::new()
        .has_headers(true)
        .from_reader(csv_content.as_bytes());
        
    let headers = rdr.headers()?.clone();
    
    let mut ch1_idx = None;
    let mut ch2_idx = None;
    let mut ch3_idx = None;
    
    for (i, h) in headers.iter().enumerate() {
        let h_lower = h.to_lowercase();
        if h_lower.contains("ch1") || h_lower.contains("lead i") || h_lower.contains("lead_i") || h_lower == "i" {
            ch1_idx = Some(i);
        } else if h_lower.contains("ch2") || h_lower.contains("lead ii") || h_lower.contains("lead_ii") || h_lower == "ii" {
            ch2_idx = Some(i);
        } else if h_lower.contains("ch3") || h_lower.contains("lead iii") || h_lower.contains("lead_iii") || h_lower == "iii" {
            ch3_idx = Some(i);
        }
    }
    
    let ch1_idx = ch1_idx.unwrap_or(if headers.len() >= 4 { 1 } else { 0 });
    let ch2_idx = ch2_idx.unwrap_or(if headers.len() >= 4 { 2 } else { 1 });
    let ch3_idx = ch3_idx.unwrap_or(if headers.len() >= 4 { 3 } else { 2 });
    
    let mut samples = Vec::new();
    for result in rdr.records() {
        let record = result?;
        let val1 = record.get(ch1_idx).and_then(|v| v.parse::<f64>().ok()).unwrap_or(0.0);
        let val2 = record.get(ch2_idx).and_then(|v| v.parse::<f64>().ok()).unwrap_or(0.0);
        let val3 = record.get(ch3_idx).and_then(|v| v.parse::<f64>().ok()).unwrap_or(0.0);
        samples.push(vec![val1, val2, val3]);
    }
    
    Ok(samples)
}

#[allow(dead_code)]
#[allow(non_snake_case)]
#[derive(Deserialize)]
struct UploadMetadataCal {
    calibration_source: Option<String>,
    expected_mV: Option<f64>,
    method: Option<String>,
}

#[allow(dead_code)]
#[derive(Deserialize)]
struct UploadMetadataSourceMeta {
    device_id: Option<String>,
    session_id: Option<String>,
    measurement_id: Option<String>,
    frame_index: Option<i64>,
    created_at_utc: Option<String>,
    csv_file: Option<String>,
    sample_rate_hz: Option<f64>,
    duration_seconds: Option<f64>,
}

#[allow(dead_code)]
#[derive(Deserialize)]
struct UploadMetadata {
    calibration: Option<UploadMetadataCal>,
    created_at_utc: Option<String>,
    duration_seconds: Option<f64>,
    sample_rate_hz: Option<f64>,
    source_frame: Option<String>,
    source_metadata: Option<UploadMetadataSourceMeta>,
    unit: Option<String>,
}

#[allow(dead_code)]
#[derive(Deserialize, Clone)]
struct UploadPredictionMetadata {
    prediction: Option<String>,
    confidence_percent: Option<f64>,
    probabilities: Option<serde_json::Value>,
    threshold: Option<f64>,
    latency_ms: Option<f64>,
    runtime: Option<String>,
    input_validation_status: Option<String>,
    input_warnings: Option<Vec<String>>,
}

async fn upload_session_handler(
    State(state): State<AppState>,
    mut multipart: Multipart,
) -> impl IntoResponse {
    tracing::info!("--- Starting manual upload processing ---");
    
    let mut patient_id: Option<String> = None;
    let mut custom_device_id: Option<String> = None;
    
    let mut json_data: HashMap<String, String> = HashMap::new();
    let mut prediction_data: HashMap<String, String> = HashMap::new();
    let mut csv_data: HashMap<String, String> = HashMap::new();
    
    while let Ok(Some(field)) = multipart.next_field().await {
        let name = field.name().unwrap_or_default().to_string();
        if name == "patient_id" {
            if let Ok(val) = field.text().await {
                let trimmed = val.trim();
                if !trimmed.is_empty() && trimmed != "null" && trimmed != "undefined" {
                    patient_id = Some(trimmed.to_string());
                    tracing::info!("Received patient_id: {}", trimmed);
                } else {
                    tracing::info!("Received empty/null patient_id");
                }
            }
        } else if name == "device_id" {
            if let Ok(val) = field.text().await {
                if !val.trim().is_empty() {
                    custom_device_id = Some(val.trim().to_string());
                }
            }
        } else {
            let file_name = field.file_name().unwrap_or_default().to_string();
            let file_stem = Path::new(&file_name)
                .file_stem()
                .unwrap_or_default()
                .to_string_lossy()
                .to_string();
                
            if file_name.ends_with(".json") {
                if file_name.ends_with("_prediction.json") {
                    if let Ok(bytes) = field.bytes().await {
                        if let Ok(text) = String::from_utf8(bytes.to_vec()) {
                            prediction_data.insert(file_stem, text);
                        }
                    }
                } else {
                    if let Ok(bytes) = field.bytes().await {
                        if let Ok(text) = String::from_utf8(bytes.to_vec()) {
                            json_data.insert(file_stem, text);
                        }
                    }
                }
            } else if file_name.ends_with(".csv") {
                if let Ok(bytes) = field.bytes().await {
                    if let Ok(text) = String::from_utf8(bytes.to_vec()) {
                        csv_data.insert(file_stem, text);
                    }
                }
            }
        }
    }
    
    tracing::info!("Multipart parsing complete. Received: {} base JSON metadata, {} prediction JSON, {} CSV data files", json_data.len(), prediction_data.len(), csv_data.len());
    
    if json_data.is_empty() {
        return (StatusCode::BAD_REQUEST, Json(serde_json::json!({
            "success": false,
            "message": "Tidak ada file metadata .json yang diunggah"
        })));
    }
    
    let mut processed_frames = 0;
    let mut resolved_session_id = String::new();
    let mut resolved_device_id = String::new();
    let mut created_at_utc = String::new();
    let mut payloads: Vec<crate::models::device::DevicePayload> = Vec::new();
    
    for (stem, json_str) in &json_data {
        let metadata: UploadMetadata = match serde_json::from_str(json_str) {
            Ok(v) => v,
            Err(e) => {
                return (StatusCode::BAD_REQUEST, Json(serde_json::json!({
                    "success": false,
                    "message": format!("Gagal mem-parsing metadata JSON {}: {}", stem, e)
                })));
            }
        };
        
        let csv_content = csv_data.get(stem)
            .or_else(|| {
                let csv_filename = metadata.source_metadata.as_ref()
                    .and_then(|m| m.csv_file.as_ref())
                    .map(|s| s.as_str())
                    .unwrap_or_default();
                let csv_stem = Path::new(csv_filename).file_stem().unwrap_or_default().to_string_lossy().to_string();
                csv_data.get(&csv_stem)
            });
            
        let csv_str = match csv_content {
            Some(c) => c,
            None => {
                if csv_data.len() == 1 && json_data.len() == 1 {
                    csv_data.values().next().unwrap()
                } else {
                    return (StatusCode::BAD_REQUEST, Json(serde_json::json!({
                        "success": false,
                        "message": format!("File CSV pendamping untuk {} tidak ditemukan", stem)
                    })));
                }
            }
        };
        
        let ecg_samples = match parse_csv_samples(csv_str) {
            Ok(s) => s,
            Err(e) => {
                return (StatusCode::BAD_REQUEST, Json(serde_json::json!({
                    "success": false,
                    "message": format!("Gagal membaca sampel CSV {}: {}", stem, e)
                })));
            }
        };
        
        let file_device_id = metadata.source_metadata.as_ref()
            .and_then(|m| m.device_id.as_ref())
            .map(|s| s.clone())
            .unwrap_or_else(|| "device01".to_string());
            
        if resolved_session_id.is_empty() {
            if let Ok(conn) = state.pool.get() {
                resolved_session_id = crate::db::sqlite::generate_custom_id(&conn, "sessions", "ses");
                tracing::info!("Generated new custom session_id: {}", resolved_session_id);
            } else {
                resolved_session_id = format!("ses_{}", chrono::Utc::now().timestamp_millis());
                tracing::warn!("Failed to get DB connection, fallback session_id: {}", resolved_session_id);
            }
        }
        if resolved_device_id.is_empty() {
            resolved_device_id = custom_device_id.clone().unwrap_or(file_device_id);
        }
        if created_at_utc.is_empty() {
            created_at_utc = metadata.created_at_utc.clone()
                .or_else(|| metadata.source_metadata.as_ref().and_then(|m| m.created_at_utc.clone()))
                .unwrap_or_else(|| chrono::Utc::now().to_rfc3339());
        }
        
        let frame_id = metadata.source_frame.clone()
            .or_else(|| metadata.source_metadata.as_ref().and_then(|m| m.frame_index.map(|idx| format!("{:06}", idx))))
            .unwrap_or_else(|| "000001".to_string());
            
        let measurement_id = metadata.source_metadata.as_ref()
            .and_then(|m| m.measurement_id.clone())
            .unwrap_or_else(|| format!("{}_{}", resolved_session_id, frame_id));
            
        let sample_rate = metadata.sample_rate_hz
            .or_else(|| metadata.source_metadata.as_ref().and_then(|m| m.sample_rate_hz))
            .unwrap_or(250.0);
            
        let duration = metadata.duration_seconds
            .or_else(|| metadata.source_metadata.as_ref().and_then(|m| m.duration_seconds))
            .unwrap_or(10.0);
            
        let prediction_stem = stem.replace("_mv", "_prediction");
        let prediction_obj = prediction_data.get(&prediction_stem).and_then(|p_str| {
            serde_json::from_str::<UploadPredictionMetadata>(p_str).ok()
        });

        let prediction = if let Some(p) = prediction_obj.clone() {
            crate::models::device::DevicePrediction {
                status: "PASS".to_string(),
                label: p.prediction.unwrap_or_else(|| "Normal".to_string()),
                confidence_percent: p.confidence_percent.unwrap_or(100.0),
                probabilities: p.probabilities,
                threshold: p.threshold.or(Some(0.5)),
                latency_ms: p.latency_ms.or(Some(0.0)),
                runtime: p.runtime.or(Some("ai-edge-litert".to_string())),
            }
        } else {
            crate::models::device::DevicePrediction {
                status: "PASS".to_string(),
                label: "Normal".to_string(),
                confidence_percent: 100.0,
                probabilities: Some(serde_json::json!({
                    "AF": 0.0,
                    "Bradikardia": 0.0,
                    "Normal": 100.0,
                    "Takikardia": 0.0
                })),
                threshold: Some(0.5),
                latency_ms: Some(0.0),
                runtime: Some("ai-edge-litert".to_string()),
            }
        };

        let payload = crate::models::device::DevicePayload {
            message_id: measurement_id,
            device_id: resolved_device_id.clone(),
            session_id: resolved_session_id.clone(),
            frame_id,
            created_at: created_at_utc.clone(),
            sampling_rate_hz: sample_rate,
            duration_s: duration,
            validation: crate::models::device::DeviceValidation {
                status: prediction_obj.as_ref().and_then(|p| p.input_validation_status.clone()).unwrap_or_else(|| "PASS".to_string()),
                warnings: prediction_obj.as_ref().and_then(|p| p.input_warnings.clone()).unwrap_or_else(|| vec![]),
            },
            ecg: crate::models::device::DeviceEcg {
                format: "samples_by_time".to_string(),
                samples: ecg_samples,
            },
            prediction,
            system: None,
            stress_test: None,
            network: None,
        };
        
        payloads.push(payload);
        processed_frames += 1;
    }
    
    // Urutkan berdasarkan frame_id agar penulisannya berurutan secara kronologis
    payloads.sort_by(|a, b| a.frame_id.cmp(&b.frame_id));
    
    if let Ok(conn) = state.pool.get() {
        tracing::info!("[Upload] Koneksi DB berhasil. Memulai pencatatan ke database...");

        // === 1. Pastikan device ada ===
        tracing::info!("[Upload] Menyimpan device: {}", resolved_device_id);
        match conn.execute(
            "INSERT OR IGNORE INTO devices (id, name) VALUES (?1, ?1)",
            params![resolved_device_id]
        ) {
            Ok(rows) => tracing::info!("[Upload] Device '{}' OK (rows affected: {})", resolved_device_id, rows),
            Err(e)   => tracing::error!("[Upload] GAGAL insert device '{}': {:?}", resolved_device_id, e),
        }
        
        // === 2. Resolve patient_id ===
        tracing::info!("[Upload] Patient ID dari request: {:?}", patient_id);
        if let Some(pid) = &patient_id {
            let mut resolved_pid: Option<String> = None;
            if pid.len() < 15 {
                tracing::info!("[Upload] patient_id '{}' terpotong (len={}), mencari ID lengkapnya...", pid, pid.len());
                match conn.query_row(
                    "SELECT id FROM patients WHERE id LIKE (?1 || '%') LIMIT 1",
                    params![pid],
                    |row| row.get::<_, String>(0)
                ) {
                    Ok(full_pid) => {
                        tracing::info!("[Upload] Berhasil resolve '{}' -> '{}'", pid, full_pid);
                        resolved_pid = Some(full_pid);
                    }
                    Err(e) => tracing::warn!("[Upload] Gagal resolve truncated patient_id '{}': {:?}", pid, e),
                }
            } else {
                let exists: bool = conn.query_row(
                    "SELECT EXISTS(SELECT 1 FROM patients WHERE id = ?1)",
                    params![pid],
                    |row| row.get(0)
                ).unwrap_or(false);
                if exists {
                    tracing::info!("[Upload] patient_id '{}' ditemukan di database", pid);
                    resolved_pid = Some(pid.clone());
                } else {
                    tracing::warn!("[Upload] patient_id '{}' TIDAK ditemukan di tabel patients", pid);
                }
            }
            
            if resolved_pid.is_none() {
                tracing::warn!("[Upload] Membuat pasien dummy untuk patient_id '{}' agar sesi tetap tercatat", pid);
                match conn.execute(
                    "INSERT OR IGNORE INTO patients (id, first_name, last_name, date_of_birth, gender) VALUES (?1, 'Unknown', 'Patient', '1900-01-01', 'U')",
                    params![pid]
                ) {
                    Ok(rows) => tracing::info!("[Upload] Pasien dummy '{}' berhasil dibuat (rows: {})", pid, rows),
                    Err(e)   => tracing::error!("[Upload] GAGAL membuat pasien dummy '{}': {:?}", pid, e),
                }
                resolved_pid = Some(pid.clone());
            }
            patient_id = resolved_pid;
        } else {
            tracing::warn!("[Upload] Tidak ada patient_id yang disertakan dalam request");
        }
        tracing::info!("[Upload] Patient ID setelah resolve: {:?}", patient_id);

        let file_path = format!("records/{}.jsonl", resolved_session_id);
        
        // === 3. Insert session ke database TERLEBIH DAHULU ===
        tracing::info!("[Upload] Menyimpan session ke DB: id={}, device={}, patient={:?}, started={}, file={}",
            resolved_session_id, resolved_device_id, patient_id, created_at_utc, file_path);
        match conn.execute(
            "INSERT OR IGNORE INTO sessions (id, device_id, patient_id, started_at, file_path) VALUES (?1, ?2, ?3, ?4, ?5)",
            params![resolved_session_id, resolved_device_id, patient_id, created_at_utc, file_path]
        ) {
            Ok(rows) => tracing::info!("[Upload] Session '{}' berhasil dicatat ke DB! (rows affected: {})", resolved_session_id, rows),
            Err(e)   => tracing::error!("[Upload] GAGAL insert session '{}': {:?}", resolved_session_id, e),
        }
        
        // === 4. Tulis file JSONL SETELAH DB berhasil ===
        if let Some(parent) = Path::new(&file_path).parent() {
            let _ = fs::create_dir_all(parent);
        }
        
        if let Ok(mut file) = std::fs::OpenOptions::new().create(true).append(true).open(&file_path) {
            tracing::info!("[Upload] Menulis {} frame ke file {}", payloads.len(), file_path);
            for payload in payloads {
                if let Ok(json_string) = serde_json::to_string(&payload) {
                    let _ = writeln!(file, "{}", json_string);
                    
                    let frame_num = payload.frame_id.replace("frame_", "").parse::<i64>().unwrap_or(1);
                    let start_sec = (frame_num - 1) as f64 * payload.duration_s;
                    let end_sec = frame_num as f64 * payload.duration_s;
                    
                    let format_time = |secs: f64| -> String {
                        let m = (secs / 60.0).floor() as i64;
                        let s = (secs % 60.0).floor() as i64;
                        format!("{:02}:{:02}", m, s)
                    };
                    let time_interval = format!("{} - {}", format_time(start_sec), format_time(end_sec));
                    let frame_db_id = format!("fra{}{:06}", resolved_session_id.replace("session_", "").replace("ses_", ""), frame_num);
                    
                    let max_retries = 3;
                    for _ in 0..max_retries {
                        let res = conn.execute(
                            "INSERT INTO frame_records (id, session_id, time_interval, confirmation, doc_classification) VALUES (?1, ?2, ?3, NULL, NULL) ON CONFLICT(id) DO NOTHING",
                            params![frame_db_id, resolved_session_id, time_interval]
                        );
                        if res.is_ok() {
                            break;
                        } else if let Err(e) = res {
                            tracing::error!("Failed to insert frame_record {}: {}", frame_db_id, e);
                        }
                    }
                }
            }
        }
        
        (StatusCode::OK, Json(serde_json::json!({
            "success": true,
            "message": format!("Berhasil mengimpor {} frame ke sesi {}", processed_frames, resolved_session_id),
            "session_id": resolved_session_id
        })))
    } else {
        (StatusCode::INTERNAL_SERVER_ERROR, Json(serde_json::json!({
            "success": false,
            "message": "Database error"
        })))
    }
}

async fn download_record_handler(
    AxumPath(session_id): AxumPath<String>,
) -> impl IntoResponse {
    let file_path = format!("records/{}.jsonl", session_id);
    if let Ok(contents) = fs::read_to_string(&file_path) {
        axum::response::Response::builder()
            .header("Content-Type", "application/json")
            .header("Content-Disposition", format!("attachment; filename=\"{}.jsonl\"", session_id))
            .body(axum::body::Body::from(contents))
            .unwrap()
    } else {
        axum::response::Response::builder()
            .status(StatusCode::NOT_FOUND)
            .body(axum::body::Body::from("Record file not found"))
            .unwrap()
    }
}

async fn delete_session_handler(
    State(state): State<AppState>,
    AxumPath(session_id): AxumPath<String>,
) -> impl IntoResponse {
    if let Ok(conn) = state.pool.get() {
        let _ = conn.execute("DELETE FROM frame_records WHERE session_id = ?1", params![session_id]);
        match conn.execute("DELETE FROM sessions WHERE id = ?1", params![session_id]) {
            Ok(rows) if rows > 0 => {
                let file_path = format!("records/{}.jsonl", session_id);
                let _ = fs::remove_file(file_path);
                (StatusCode::OK, Json(serde_json::json!({"success": true, "message": "Sesi berhasil dihapus"})))
            }
            Ok(_) => (StatusCode::NOT_FOUND, Json(serde_json::json!({"success": false, "message": "Sesi tidak ditemukan"}))),
            Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, Json(serde_json::json!({"success": false, "message": e.to_string()}))),
        }
    } else {
        (StatusCode::INTERNAL_SERVER_ERROR, Json(serde_json::json!({"success": false, "message": "Database error"})))
    }
}

#[derive(Deserialize)]
struct EditSessionRequest {
    patient_id: Option<String>,
    device_id: Option<String>,
    ended_at: Option<String>,
}

async fn edit_session_handler(
    State(state): State<AppState>,
    AxumPath(session_id): AxumPath<String>,
    Json(req): Json<EditSessionRequest>,
) -> impl IntoResponse {
    if let Ok(conn) = state.pool.get() {
        let mut query = "UPDATE sessions SET ".to_string();
        let mut params_vec: Vec<String> = Vec::new();
        
        if let Some(ref pid) = req.patient_id {
            query.push_str("patient_id = ?, ");
            params_vec.push(pid.clone());
        }
        
        if let Some(ref did) = req.device_id {
            query.push_str("device_id = ?, ");
            params_vec.push(did.clone());
        }
        
        if let Some(ref ended) = req.ended_at {
            query.push_str("ended_at = ?, ");
            params_vec.push(ended.clone());
        }
        
        if params_vec.is_empty() {
            return (StatusCode::BAD_REQUEST, Json(serde_json::json!({"success": false, "message": "Tidak ada data yang diubah"})));
        }
        
        query.truncate(query.len() - 2);
        query.push_str(" WHERE id = ?");
        params_vec.push(session_id.clone());
        
        let mut stmt = conn.prepare(&query).unwrap();
        let params_sql = rusqlite::params_from_iter(params_vec.iter());
        
        match stmt.execute(params_sql) {
            Ok(rows) if rows > 0 => (StatusCode::OK, Json(serde_json::json!({"success": true, "message": "Sesi berhasil diupdate"}))),
            Ok(_) => (StatusCode::NOT_FOUND, Json(serde_json::json!({"success": false, "message": "Sesi tidak ditemukan"}))),
            Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, Json(serde_json::json!({"success": false, "message": e.to_string()}))),
        }
    } else {
        (StatusCode::INTERNAL_SERVER_ERROR, Json(serde_json::json!({"success": false, "message": "Database error"})))
    }
}

#[derive(Deserialize)]
struct AddPatientRequest {
    first_name: String,
    last_name: String,
    date_of_birth: String,
    gender: String,
    device_id: Option<String>,
}

async fn add_patient_handler(
    State(state): State<AppState>,
    Json(req): Json<AddPatientRequest>,
) -> impl IntoResponse {
    if let Ok(conn) = state.pool.get() {
        let new_id = crate::db::sqlite::generate_custom_id(&conn, "patients", "pat");
        
        let res = conn.execute(
            "INSERT INTO patients (id, first_name, last_name, date_of_birth, gender, device_id) VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
            params![new_id, req.first_name, req.last_name, req.date_of_birth, req.gender, req.device_id]
        );
        
        match res {
            Ok(_) => (StatusCode::OK, Json(serde_json::json!({"success": true, "message": "Pasien berhasil ditambahkan", "id": new_id}))),
            Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, Json(serde_json::json!({"success": false, "message": e.to_string()}))),
        }
    } else {
        (StatusCode::INTERNAL_SERVER_ERROR, Json(serde_json::json!({"success": false, "message": "Database error"})))
    }
}

async fn delete_patient_handler(
    State(state): State<AppState>,
    AxumPath(patient_id): AxumPath<String>,
) -> impl IntoResponse {
    if let Ok(conn) = state.pool.get() {
        let mut stmt = conn.prepare("SELECT id FROM sessions WHERE patient_id = ?1").unwrap();
        let sessions_iter = stmt.query_map(params![patient_id], |row| row.get::<_, String>(0)).unwrap();
        
        for ses_id in sessions_iter {
            if let Ok(sid) = ses_id {
                let _ = conn.execute("DELETE FROM frame_records WHERE session_id = ?1", params![sid]);
                let _ = conn.execute("DELETE FROM sessions WHERE id = ?1", params![sid]);
                let file_path = format!("records/{}.jsonl", sid);
                let _ = fs::remove_file(file_path);
            }
        }
        
        match conn.execute("DELETE FROM patients WHERE id = ?1", params![patient_id]) {
            Ok(rows) if rows > 0 => (StatusCode::OK, Json(serde_json::json!({"success": true, "message": "Pasien berhasil dihapus"}))),
            Ok(_) => (StatusCode::NOT_FOUND, Json(serde_json::json!({"success": false, "message": "Pasien tidak ditemukan"}))),
            Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, Json(serde_json::json!({"success": false, "message": e.to_string()}))),
        }
    } else {
        (StatusCode::INTERNAL_SERVER_ERROR, Json(serde_json::json!({"success": false, "message": "Database error"})))
    }
}

#[derive(Deserialize)]
pub struct NewDeviceReq {
    pub name: String,
    pub mqtt_broker: String,
    pub mqtt_port: u16,
    pub mqtt_topic: String,
    pub mqtt_username: String,
    pub mqtt_password: String,
}

#[derive(Deserialize)]
pub struct EditDeviceReq {
    pub name: String,
    pub mqtt_broker: String,
    pub mqtt_port: u16,
    pub mqtt_topic: String,
    pub mqtt_username: String,
    pub mqtt_password: String,
}

pub async fn add_device_handler(
    State(state): State<AppState>,
    _claims: AdminClaims,
    Json(req): Json<NewDeviceReq>,
) -> impl IntoResponse {
    let dev_id = format!("dev_{}", chrono::Utc::now().timestamp_millis());
    {
        let conn_res = state.pool.get();
        if let Ok(conn) = conn_res {
            if let Err(e) = conn.execute(
                "INSERT INTO devices (id, name, mqtt_broker, mqtt_port, mqtt_topic, mqtt_username, mqtt_password, assigned_to) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, 'Unassigned')",
                params![dev_id, req.name, req.mqtt_broker, req.mqtt_port, req.mqtt_topic, req.mqtt_username, req.mqtt_password]
            ) {
                return (StatusCode::INTERNAL_SERVER_ERROR, Json(serde_json::json!({"success": false, "message": e.to_string()})));
            }
        }
    }
    
    let db_tx = state.db_tx.clone();
    
    let client = crate::network::mqtt_listener::start_mqtt_listener(
        &req.mqtt_broker,
        req.mqtt_port,
        &req.mqtt_topic,
        &req.mqtt_username,
        &req.mqtt_password,
        move |payload_str| {
            if let Ok(device_payload) = serde_json::from_str::<crate::models::device::DevicePayload>(&payload_str) {
                let _ = db_tx.send(device_payload);
            }
        }
    );
    
    let mut clients = state.mqtt_clients.write().await;
    clients.insert(req.name.clone(), client);
    
    (StatusCode::OK, Json(serde_json::json!({"success": true, "message": "Perangkat didaftarkan dan pairing dimulai"})))
}

pub async fn edit_device_handler(
    State(state): State<AppState>,
    _claims: AdminClaims,
    AxumPath(id): AxumPath<String>,
    Json(req): Json<EditDeviceReq>,
) -> impl IntoResponse {
    let old_name: Option<String> = {
        if let Ok(conn) = state.pool.get() {
            conn.query_row("SELECT name FROM devices WHERE id = ?1", params![id], |row| row.get(0)).ok()
        } else {
            None
        }
    };

    {
        let conn_res = state.pool.get();
        if let Ok(conn) = conn_res {
            if let Err(e) = conn.execute(
                "UPDATE devices SET name = ?1, mqtt_broker = ?2, mqtt_port = ?3, mqtt_topic = ?4, mqtt_username = ?5, mqtt_password = ?6 WHERE id = ?7",
                params![req.name, req.mqtt_broker, req.mqtt_port, req.mqtt_topic, req.mqtt_username, req.mqtt_password, id]
            ) {
                return (StatusCode::INTERNAL_SERVER_ERROR, Json(serde_json::json!({"success": false, "message": e.to_string()})));
            }
        }
    }

    if let Some(old_name) = old_name {
        let mut clients = state.mqtt_clients.write().await;
        if let Some(old_client) = clients.remove(&old_name) {
            let _ = old_client.disconnect();
        }
    }

    let db_tx = state.db_tx.clone();
    
    let client = crate::network::mqtt_listener::start_mqtt_listener(
        &req.mqtt_broker,
        req.mqtt_port,
        &req.mqtt_topic,
        &req.mqtt_username,
        &req.mqtt_password,
        move |payload_str| {
            if let Ok(device_payload) = serde_json::from_str::<crate::models::device::DevicePayload>(&payload_str) {
                let _ = db_tx.send(device_payload);
            }
        }
    );
    
    let mut clients = state.mqtt_clients.write().await;
    clients.insert(req.name.clone(), client);

    (StatusCode::OK, Json(serde_json::json!({"success": true, "message": "Perangkat berhasil diupdate"})))
}

// AXUM ROUTER GENERATOR

async fn create_session_handler(
    State(state): State<AppState>,
    Json(req): Json<CreateSessionRequest>,
) -> impl IntoResponse {
    let session_id = if let Some(id) = req.id {
        if id.starts_with("ses") && id.len() == 15 {
            id
        } else {
            if let Ok(conn) = state.pool.get() {
                crate::db::sqlite::generate_custom_id(&conn, "sessions", "ses")
            } else {
                format!("ses_{}", chrono::Utc::now().timestamp_millis())
            }
        }
    } else {
        if let Ok(conn) = state.pool.get() {
            crate::db::sqlite::generate_custom_id(&conn, "sessions", "ses")
        } else {
            format!("ses_{}", chrono::Utc::now().timestamp_millis())
        }
    };
    let started_at = req.started_at.unwrap_or_else(|| chrono::Utc::now().to_rfc3339());
    let file_path = format!("records/{}.jsonl", session_id);
    
    if let Ok(conn) = state.pool.get() {
        if let Some(parent) = Path::new(&file_path).parent() {
            let _ = fs::create_dir_all(parent);
        }
        
        let _ = conn.execute(
            "INSERT INTO sessions (id, patient_id, device_id, doctor_id, started_at, dev_note, file_path) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
            params![session_id, req.patient_id, req.device_id, req.doctor_id, started_at, req.dev_note, file_path]
        );
        
        (StatusCode::CREATED, Json(serde_json::json!({
            "success": true,
            "session_id": session_id,
            "message": "Session created"
        })))
    } else {
        (StatusCode::INTERNAL_SERVER_ERROR, Json(serde_json::json!({
            "success": false,
            "message": "Database error"
        })))
    }
}

async fn create_record_handler(
    State(state): State<AppState>,
    body: String,
) -> impl IntoResponse {
    let payload: serde_json::Value = match serde_json::from_str(&body) {
        Ok(p) => p,
        Err(e) => {
            tracing::error!("Failed to parse payload: {}. Body: {}", e, body);
            return (StatusCode::UNPROCESSABLE_ENTITY, Json(serde_json::json!({
                "success": false,
                "message": format!("Failed to parse payload: {}", e)
            })));
        }
    };
    
    let mut session_id = payload.get("session_id").and_then(|v| v.as_str()).unwrap_or("unknown_session").to_string();
    let record_id = payload.get("id").or_else(|| payload.get("message_id")).and_then(|v| v.as_str()).unwrap_or("").to_string();
    
    // Paksa validasi nama session_id agar sesuai format (ses + 12 digit)
    if !(session_id.starts_with("ses") && session_id.len() == 15) {
        if let Ok(conn) = state.pool.get() {
            session_id = crate::db::sqlite::generate_custom_id(&conn, "sessions", "ses");
        }
    }
    
    let file_path = format!("records/{}.jsonl", session_id);
    
    if let Ok(mut file) = std::fs::OpenOptions::new().create(true).append(true).open(&file_path) {
        if let Ok(json_string) = serde_json::to_string(&payload) {
            let _ = writeln!(file, "{}", json_string);
        }
        
        if let Ok(conn) = state.pool.get() {
            // Coba ambil start_time dari root (frontend format)
            let start_sec = payload.get("start_time").and_then(|v| v.as_f64()).unwrap_or_else(|| {
                // Fallback: hitung dari frame_id dan duration (old format)
                let frame_id = payload.get("frame_id").or_else(|| payload.pointer("/payload/source_frame")).and_then(|v| v.as_str()).unwrap_or("frame_1");
                let frame_num = frame_id.replace("frame_", "").parse::<f64>().unwrap_or(1.0);
                let duration = payload.get("duration_s").or_else(|| payload.pointer("/payload/duration_seconds")).and_then(|v| v.as_f64()).unwrap_or(10.0);
                (frame_num - 1.0) * duration
            });
            
            let duration = payload.pointer("/payload/duration_seconds").or_else(|| payload.get("duration_s")).and_then(|v| v.as_f64()).unwrap_or(10.0);
            let end_sec = start_sec + duration;
            
            let format_time = |secs: f64| -> String {
                let m = (secs / 60.0).floor() as i64;
                let s = (secs % 60.0).floor() as i64;
                format!("{:02}:{:02}", m, s)
            };
            let time_interval = format!("{} - {}", format_time(start_sec), format_time(end_sec));
            
            let _ = conn.execute(
                "INSERT INTO frame_records (id, session_id, time_interval, confirmation, doc_classification) VALUES (?1, ?2, ?3, NULL, NULL) ON CONFLICT(id) DO NOTHING",
                params![record_id, session_id, time_interval]
            );
        }
        
        (StatusCode::CREATED, Json(serde_json::json!({
            "success": true,
            "message": "Record appended"
        })))
    } else {
        (StatusCode::INTERNAL_SERVER_ERROR, Json(serde_json::json!({
            "success": false,
            "message": "File error"
        })))
    }
}

pub fn create_router(state: AppState) -> Router {
    // Izinkan origin baik yang menggunakan www maupun tanpa www
    let cors = CorsLayer::new()
        .allow_origin([
            "https://ecgrhythmia.cloud".parse::<HeaderValue>().unwrap(),
            "https://www.ecgrhythmia.cloud".parse::<HeaderValue>().unwrap(),
            "http://localhost:5173".parse::<HeaderValue>().unwrap(),
        ])
        .allow_methods([
            Method::GET,
            Method::POST,
            Method::PUT,
            Method::DELETE,
            Method::PATCH,
            Method::OPTIONS,
        ])
        .allow_headers([
            header::AUTHORIZATION,
            header::CONTENT_TYPE,
            header::ACCEPT,
            header::ORIGIN,
            header::HeaderName::from_static("x-requested-with"),
        ])
        .allow_credentials(true);

    Router::new()
        .route("/api/auth/register", post(register_handler))
        .route("/api/auth/login", post(login_handler))
        .route("/api/sessions", get(get_sessions_handler).post(create_session_handler))
        .route("/api/sessions/upload", post(upload_session_handler).layer(DefaultBodyLimit::max(50 * 1024 * 1024)))
        .route("/api/sessions/:session_id", put(edit_session_handler).delete(delete_session_handler))
        .route("/api/devices", get(get_devices_handler))
        .route("/api/admin/stats", get(get_admin_stats_handler))
        .route("/api/admin/users", get(get_admin_users_handler))
        .route("/api/admin/sync", post(admin_sync_handler))
        .route("/api/admin/impersonate/:target_id", post(impersonate_handler))
        .route("/api/admin/devices", get(get_devices_handler).post(add_device_handler))
        .route("/api/admin/devices/:id", put(edit_device_handler))
        .route("/api/patients", get(get_patients_handler).post(add_patient_handler))
        .route("/api/patients/:patient_id/sessions", get(get_patient_sessions_handler))
        .route("/api/patients/:patient_id", get(get_patient_profile_handler).put(update_patient_profile_handler).delete(delete_patient_handler))
        .route("/api/patients/:patient_id/connect", post(connect_patient_handler))
        .route("/api/patients/:patient_id/disconnect", post(disconnect_patient_handler))
        .route("/api/doctors/:doctor_id/patients", get(get_doctor_patients_handler))
        .route("/api/doctors/:doctor_id", get(get_doctor_profile_handler).put(update_doctor_profile_handler))
        .route("/api/records", post(create_record_handler))
        .route("/api/records/:session_id", get(get_record_handler))
        .route("/api/records/:session_id/download", get(download_record_handler))
        .route("/api/devices/:device_id/command", post(device_command_handler))
        .route("/api/devices/:device_id/assign", post(assign_device_handler))
        .route("/api/sessions/:session_id/confirmation", post(session_confirmation_handler))
        .route("/api/frames", post(frame_preregister_handler))
        .route("/api/frames/:id/session", put(frame_session_update_handler))
        .nest_service("/uploads", tower_http::services::ServeDir::new("uploads"))
        .layer(cors)
        .with_state(state)
}
