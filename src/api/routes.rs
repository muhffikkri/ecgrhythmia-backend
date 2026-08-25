use std::fs;
use std::path::Path;
use std::collections::HashMap;
use serde::{Deserialize, Serialize};
use chrono::Utc;
use base64::{Engine as _, engine::general_purpose::STANDARD as base64_engine};
use jsonwebtoken::{decode, Validation, DecodingKey};
use sqlx::PgPool;
use uuid::Uuid;
use axum::{
    async_trait,
    routing::{get, post, put},
    Router,
    extract::{Path as AxumPath, State, Query, Json, FromRequestParts, FromRef, Multipart, DefaultBodyLimit},
    http::{request::Parts, StatusCode, Method, HeaderValue, header},
    response::IntoResponse,
};
use tower_http::cors::CorsLayer;
use tower_http::trace::TraceLayer;
use tracing::{info, error};
use bcrypt::{hash, DEFAULT_COST};

#[derive(Clone)]
pub struct AppState {
    pub pool: PgPool,
    pub mqtt_clients: std::sync::Arc<tokio::sync::RwLock<HashMap<String, rumqttc::Client>>>,
    pub pacer_tx: tokio::sync::mpsc::UnboundedSender<crate::models::device::DevicePayload>,
    pub db_tx: tokio::sync::mpsc::UnboundedSender<crate::models::device::DevicePayload>,
    pub jwt_secret: String,
    pub api_url: String,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct AppMetadata {
    pub role: Option<String>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Claims {
    pub sub: String,
    pub app_metadata: Option<AppMetadata>,
    pub exp: usize,
}

pub struct FullClaims {
    pub sub: String,
    pub role: String,
}

pub struct AdminClaims(pub FullClaims);

#[async_trait]
impl<S> FromRequestParts<S> for AdminClaims
where
    S: Send + Sync,
    AppState: axum::extract::FromRef<S>,
{
    type Rejection = (StatusCode, Json<serde_json::Value>);

    async fn from_request_parts(parts: &mut Parts, state: &S) -> Result<Self, Self::Rejection> {
        let app_state = AppState::from_ref(state);
        
        if let Some(auth_header) = parts.headers.get("Authorization").and_then(|v| v.to_str().ok()) {
            if auth_header.starts_with("Bearer ") {
                let token = &auth_header[7..];
                if let Some(claims) = validate_jwt(token, &app_state.jwt_secret) {
                    let mut role = claims.app_metadata.and_then(|m| m.role).unwrap_or_default();
                    
                    if role.is_empty() {
                        if let Ok(record) = sqlx::query!("SELECT role FROM accounts WHERE id = $1", claims.sub).fetch_one(&app_state.pool).await {
                            role = record.role;
                        }
                    }

                    if role == "admin" || claims.sub == "acc_admin" {
                        return Ok(AdminClaims(FullClaims { sub: claims.sub, role }));
                    }
                }
            }
        }
        
        Err((StatusCode::UNAUTHORIZED, Json(serde_json::json!({"error": "Admin access required"}))))
    }
}

pub struct UserClaims(pub FullClaims);

#[async_trait]
impl<S> FromRequestParts<S> for UserClaims
where
    S: Send + Sync,
    AppState: axum::extract::FromRef<S>,
{
    type Rejection = (StatusCode, Json<serde_json::Value>);

    async fn from_request_parts(parts: &mut Parts, state: &S) -> Result<Self, Self::Rejection> {
        let app_state = AppState::from_ref(state);
        
        let auth_header = parts.headers.get("Authorization").and_then(|v| v.to_str().ok())
            .ok_or((StatusCode::UNAUTHORIZED, Json(serde_json::json!({"error": "Header Authorization tidak ditemukan"}))))?;

        if !auth_header.starts_with("Bearer ") {
            return Err((StatusCode::UNAUTHORIZED, Json(serde_json::json!({"error": "Format token tidak valid"}))));
        }

        let token = &auth_header[7..];
        let claims = validate_jwt(token, &app_state.jwt_secret)
            .ok_or((StatusCode::UNAUTHORIZED, Json(serde_json::json!({"error": "Sesi tidak valid atau kedaluwarsa"}))))?;

        let mut role = claims.app_metadata.and_then(|m| m.role).unwrap_or_default();
        if role.is_empty() {
            if let Ok(record) = sqlx::query!("SELECT role FROM accounts WHERE id = $1", claims.sub).fetch_one(&app_state.pool).await {
                role = record.role;
            }
        }

        Ok(UserClaims(FullClaims { sub: claims.sub, role }))
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
    pub ecg_paper: Option<String>,
}

#[derive(Serialize)]
pub struct DeviceRecord {
    pub id: String,
    pub name: String,
    pub mqtt_broker: Option<String>,
    pub mqtt_port: Option<i32>,
    pub mqtt_topic: Option<String>,
    pub mqtt_username: Option<String>,
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
    pub account_id: String,
    pub name: String,
    pub role: String,
    pub status: String,
    pub registered_at: Option<String>,
    pub connected_doctor_id: Option<String>,
    pub connected_device_id: Option<String>,
    pub profile_photo: Option<String>,
}

#[derive(Serialize)]
pub struct PatientRecord {
    pub id: String,
    pub first_name: String,
    pub last_name: String,
    pub age: String,
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
<<<<<<< HEAD
    pub age: String,
=======
    pub date_of_birth: String,
    pub gender: Option<String>,
>>>>>>> d4e4ff69c48c853c58f915b255502ea5f0968312
    pub profile_photo: Option<String>,
}

#[derive(Deserialize)]
pub struct ConnectPatientRequest {
    pub doctor_id: String,
}

#[derive(Deserialize, Serialize)]
pub struct AuthResponse {
    pub success: bool,
    pub message: String,
    pub user_id: Option<String>,
    pub role: Option<String>,
    pub token: Option<String>,
}

#[derive(Deserialize)]
pub struct RegisterProfileRequest {
    pub role: String,
    pub first_name: String,
    pub last_name: String,
    pub email: String,
    pub age: Option<i32>,
    pub gender: Option<String>,
}

#[derive(Deserialize)]
pub struct AdminRegisterRequest {
    pub email: String,
    pub password: String,
    pub role: String,
    pub first_name: String,
    pub last_name: String,
    pub age: Option<i32>,
    pub gender: Option<String>,
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

fn validate_jwt(token: &str, _secret: &str) -> Option<Claims> {
    let mut validation = Validation::default();
    validation.insecure_disable_signature_validation();
    validation.validate_aud = false;
    validation.required_spec_claims.clear();
    
    match decode::<Claims>(token, &DecodingKey::from_secret(&[]), &validation) {
        Ok(token_data) => Some(token_data.claims),
        Err(e) => {
            error!("JWT Validation error: {}", e);
            None
        }
    }
}

// ROUTE HANDLERS
async fn auth_me_handler(claims: UserClaims) -> impl IntoResponse {
    Json(AuthResponse {
        success: true,
        message: "Profil berhasil diambil".into(),
        user_id: Some(claims.0.sub),
        role: Some(claims.0.role),
        token: None,
    })
}

async fn register_profile_handler(
    claims: UserClaims,
    State(state): State<AppState>,
    Json(req): Json<RegisterProfileRequest>,
) -> impl IntoResponse {
    let account_id = claims.0.sub;
    
    if let Err(e) = sqlx::query!("INSERT INTO accounts (id, email, role, status) VALUES ($1, $2, $3, 'Online') ON CONFLICT (id) DO NOTHING", account_id, req.email, req.role).execute(&state.pool).await {
        return (StatusCode::INTERNAL_SERVER_ERROR, Json(serde_json::json!({"success": false, "message": e.to_string()})));
    }

    if req.role == "dokter" {
        let _ = sqlx::query!("INSERT INTO doctors (id, account_id, first_name, last_name) VALUES ($1, $2, $3, $4) ON CONFLICT (id) DO NOTHING", account_id, account_id, req.first_name, req.last_name).execute(&state.pool).await;
    } else if req.role == "pasien" {
        let age = req.age.unwrap_or(0);
        let gender = req.gender.unwrap_or_default();
        let _ = sqlx::query!("INSERT INTO patients (id, account_id, first_name, last_name, age, gender) VALUES ($1, $2, $3, $4, $5, $6) ON CONFLICT (id) DO NOTHING", account_id, account_id, req.first_name, req.last_name, age, gender).execute(&state.pool).await;
    }

    (StatusCode::OK, Json(serde_json::json!({
        "success": true,
        "message": "Profil berhasil disimpan"
    })))
}

async fn admin_register_handler(
    _claims: AdminClaims,
    State(state): State<AppState>,
    Json(req): Json<AdminRegisterRequest>,
) -> impl IntoResponse {
    let new_user_id = Uuid::new_v4();
    let new_user_id_str = new_user_id.to_string();
    
    let hashed_password = match hash(&req.password, DEFAULT_COST) {
        Ok(h) => h,
        Err(e) => return (StatusCode::INTERNAL_SERVER_ERROR, Json(serde_json::json!({"success": false, "message": format!("Gagal memproses kata sandi: {}", e)}))),
    };

    let raw_user_meta = serde_json::json!({"role": req.role});
    
    let insert_auth_res = sqlx::query(
        "INSERT INTO auth.users (id, instance_id, aud, role, email, encrypted_password, email_confirmed_at, raw_user_meta_data, created_at, updated_at) 
         VALUES ($1, '00000000-0000-0000-0000-000000000000', 'authenticated', 'authenticated', $2, $3, NOW(), $4, NOW(), NOW())"
    )
    .bind(new_user_id)
    .bind(&req.email)
    .bind(&hashed_password)
    .bind(&raw_user_meta)
    .execute(&state.pool).await;

    if let Err(e) = insert_auth_res {
        return (StatusCode::INTERNAL_SERVER_ERROR, Json(serde_json::json!({"success": false, "message": format!("Gagal mendaftarkan akun: {}", e)})));
    }

    if let Err(e) = sqlx::query!("INSERT INTO accounts (id, email, role, status) VALUES ($1, $2, $3, 'Offline')", new_user_id_str, req.email, req.role).execute(&state.pool).await {
        return (StatusCode::INTERNAL_SERVER_ERROR, Json(serde_json::json!({"success": false, "message": format!("Gagal mendaftarkan profil: {}", e)})));
    }

    if req.role == "dokter" {
        let _ = sqlx::query!("INSERT INTO doctors (id, account_id, first_name, last_name) VALUES ($1, $2, $3, $4)", new_user_id_str, new_user_id_str, req.first_name, req.last_name).execute(&state.pool).await;
    } else if req.role == "pasien" {
        let age = req.age.unwrap_or(0);
        let gender = req.gender.unwrap_or_default();
        let _ = sqlx::query!("INSERT INTO patients (id, account_id, first_name, last_name, age, gender) VALUES ($1, $2, $3, $4, $5, $6)", new_user_id_str, new_user_id_str, req.first_name, req.last_name, age, gender).execute(&state.pool).await;
    }

    (StatusCode::OK, Json(serde_json::json!({
        "success": true,
        "message": "Pengguna berhasil didaftarkan"
    })))
}

#[derive(Serialize)]
pub struct PaginationInfo {
    pub total: i64,
    pub page: i64,
    pub limit: i64,
    pub total_pages: i64,
}

#[derive(Serialize)]
pub struct PaginatedResponse<T> {
    pub data: Vec<T>,
    pub pagination: PaginationInfo,
}

async fn get_sessions_handler(
    claims: UserClaims,
    State(state): State<AppState>,
    Query(params): Query<HashMap<String, String>>,
) -> impl IntoResponse {
    let mut filter_patient_id = params.get("patient_id").cloned();
    let mut filter_doctor_id = params.get("doctor_id").cloned();
    let page: i64 = params.get("page").and_then(|v| v.parse().ok()).unwrap_or(1);
    let limit: i64 = params.get("limit").and_then(|v| v.parse().ok()).unwrap_or(10);

    if claims.0.role == "dokter" {
        filter_doctor_id = Some(claims.0.sub.clone());
    } else if claims.0.role == "pasien" {
        filter_patient_id = Some(claims.0.sub.clone());
    }

    let (sessions, total) = get_sessions_from_db(filter_patient_id, filter_doctor_id, page, limit, &state.pool).await;
    
    let total_pages = (total as f64 / limit as f64).ceil() as i64;
    
    Json(PaginatedResponse {
        data: sessions,
        pagination: PaginationInfo {
            total,
            page,
            limit,
            total_pages,
        }
    })
}

async fn get_patient_sessions_handler(
    State(state): State<AppState>,
    AxumPath(patient_id): AxumPath<String>,
    Query(params): Query<HashMap<String, String>>,
) -> impl IntoResponse {
    let page: i64 = params.get("page").and_then(|v| v.parse().ok()).unwrap_or(1);
    let limit: i64 = params.get("limit").and_then(|v| v.parse().ok()).unwrap_or(10);
    
    let (sessions, total) = get_sessions_from_db(Some(patient_id), None, page, limit, &state.pool).await;
    let total_pages = (total as f64 / limit as f64).ceil() as i64;
    
    Json(PaginatedResponse {
        data: sessions,
        pagination: PaginationInfo {
            total,
            page,
            limit,
            total_pages,
        }
    })
}

async fn get_devices_handler(
    State(state): State<AppState>,
) -> impl IntoResponse {
    let devices = get_devices_from_db(&state.pool).await;
    Json(devices)
}

async fn get_admin_stats_handler(
    _claims: AdminClaims,
    State(state): State<AppState>,
) -> impl IntoResponse {
    let stats = get_admin_stats(&state.pool).await;
    Json(stats)
}

async fn get_admin_users_handler(
    _claims: AdminClaims,
    State(state): State<AppState>,
    Query(params): Query<HashMap<String, String>>,
) -> impl IntoResponse {
<<<<<<< HEAD
    let page: i64 = params.get("page").and_then(|v| v.parse().ok()).unwrap_or(1);
    let limit: i64 = params.get("limit").and_then(|v| v.parse().ok()).unwrap_or(10);
    let role_filter = params.get("role").cloned();
    
    let (users, total) = get_admin_users(page, limit, role_filter, &state.pool).await;
    let total_pages = (total as f64 / limit as f64).ceil() as i64;
    
    Json(PaginatedResponse {
        data: users,
        pagination: PaginationInfo {
            total,
            page,
            limit,
            total_pages,
        }
    })
=======
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
>>>>>>> d4e4ff69c48c853c58f915b255502ea5f0968312
}

async fn impersonate_handler(
    _claims: AdminClaims,
    State(state): State<AppState>,
    AxumPath(target_id): AxumPath<String>,
) -> impl IntoResponse {
    let role = sqlx::query!("SELECT role FROM accounts WHERE id = $1", target_id)
        .fetch_one(&state.pool).await.ok().map(|r| r.role);
    if let Some(r) = role {
        (StatusCode::OK, Json(serde_json::json!({
            "success": true,
            "user_id": target_id,
            "role": r
        })))
    } else {
        (StatusCode::NOT_FOUND, Json(serde_json::json!({"success": false, "message": "User tidak ditemukan"})))
    }
}

async fn doctor_impersonate_handler(
    claims: UserClaims,
    State(state): State<AppState>,
    AxumPath(target_id): AxumPath<String>,
) -> impl IntoResponse {
    if claims.0.role != "dokter" {
        return (StatusCode::FORBIDDEN, Json(serde_json::json!({"success": false, "message": "Hanya dokter yang dapat melakukan impersonasi"})));
    }
    
    let doctor_account_id = claims.0.sub;
    let doc_res = sqlx::query!("SELECT id FROM doctors WHERE account_id = $1", doctor_account_id).fetch_one(&state.pool).await;
    let doc_id = match doc_res {
        Ok(rec) => rec.id,
        Err(_) => return (StatusCode::FORBIDDEN, Json(serde_json::json!({"success": false, "message": "Dokter tidak valid"})))
    };
    
    let target_patient = sqlx::query!("SELECT id FROM patients WHERE account_id = $1 AND primary_doctor_id = $2", target_id, doc_id).fetch_optional(&state.pool).await;
    
    match target_patient {
        Ok(Some(_)) => {
            (StatusCode::OK, Json(serde_json::json!({
                "success": true,
                "user_id": target_id,
                "role": "pasien"
            })))
        },
        _ => (StatusCode::FORBIDDEN, Json(serde_json::json!({"success": false, "message": "Pasien bukan milik dokter ini atau tidak ditemukan"})))
    }
}

async fn get_patients_handler(
    State(state): State<AppState>,
    Query(params): Query<HashMap<String, String>>,
) -> impl IntoResponse {
    let page: i64 = params.get("page").and_then(|v| v.parse().ok()).unwrap_or(1);
    let limit: i64 = params.get("limit").and_then(|v| v.parse().ok()).unwrap_or(10);
    let offset = (page - 1) * limit;

    let total = sqlx::query!("SELECT COUNT(*) FROM patients").fetch_one(&state.pool).await.map(|r| r.count.unwrap_or(0)).unwrap_or(0);
    let total_pages = (total as f64 / limit as f64).ceil() as i64;

    let patients = sqlx::query!("SELECT id, first_name, last_name, age, gender FROM patients ORDER BY id LIMIT $1 OFFSET $2", limit, offset)
        .fetch_all(&state.pool)
        .await
        .unwrap_or_default()
        .into_iter()
        .map(|row| {
            serde_json::json!({
                "id": row.id,
                "name": format!("{} {}", row.first_name, row.last_name).trim().to_string(),
                "age": row.age,
                "gender": row.gender
            })
        })
        .collect::<Vec<_>>();
        
    Json(PaginatedResponse {
        data: patients,
        pagination: PaginationInfo {
            total,
            page,
            limit,
            total_pages,
        }
    })
}

async fn get_patient_profile_handler(
    State(state): State<AppState>,
    AxumPath(patient_id): AxumPath<String>,
) -> impl IntoResponse {
    if let Some(profile) = get_patient_profile(patient_id, &state.pool).await {
        (StatusCode::OK, Json(serde_json::json!(profile)))
    } else {
        (StatusCode::NOT_FOUND, Json(serde_json::json!({})))
    }
}

async fn get_doctor_profile_handler(
    State(state): State<AppState>,
    AxumPath(doctor_id): AxumPath<String>,
) -> impl IntoResponse {
    if let Some(profile) = get_doctor_profile(doctor_id, &state.pool).await {
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
    match update_doctor_profile(&doctor_id, req, &state.pool, &state.api_url).await {
        Ok(_) => (StatusCode::OK, Json(serde_json::json!({"success": true}))),
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, Json(serde_json::json!({"success": false, "message": e}))),
    }
}

async fn update_patient_profile_handler(
    State(state): State<AppState>,
    AxumPath(patient_id): AxumPath<String>,
    Json(req): Json<UpdatePatientProfileRequest>,
) -> impl IntoResponse {
    match update_patient_profile(&patient_id, req, &state.pool, &state.api_url).await {
        Ok(_) => (StatusCode::OK, Json(serde_json::json!({"success": true}))),
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, Json(serde_json::json!({"success": false, "message": e}))),
    }
}

async fn connect_patient_handler(
    State(state): State<AppState>,
    AxumPath(patient_id): AxumPath<String>,
    Json(req): Json<ConnectPatientRequest>,
) -> impl IntoResponse {
    let actual_patient_id = sqlx::query!("SELECT id FROM patients WHERE id = $1 OR account_id = $1", patient_id)
        .fetch_one(&state.pool).await.map(|r| r.id).unwrap_or(patient_id.to_string());
    match sqlx::query!("UPDATE patients SET primary_doctor_id = $1 WHERE id = $2", req.doctor_id, actual_patient_id)
        .execute(&state.pool).await 
    {
        Ok(_) => (StatusCode::OK, Json(serde_json::json!({"success": true}))),
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, Json(serde_json::json!({"success": false, "message": e.to_string()}))),
    }
}

async fn disconnect_patient_handler(
    State(state): State<AppState>,
    AxumPath(patient_id): AxumPath<String>,
) -> impl IntoResponse {
    let actual_patient_id = sqlx::query!("SELECT id FROM patients WHERE id = $1 OR account_id = $1", patient_id)
        .fetch_one(&state.pool).await.map(|r| r.id).unwrap_or(patient_id.to_string());
    match sqlx::query!("UPDATE patients SET primary_doctor_id = NULL WHERE id = $1", actual_patient_id)
        .execute(&state.pool).await 
    {
        Ok(_) => (StatusCode::OK, Json(serde_json::json!({"success": true}))),
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, Json(serde_json::json!({"success": false, "message": e.to_string()}))),
    }
}

async fn get_doctor_patients_handler(
    State(state): State<AppState>,
    AxumPath(doctor_id): AxumPath<String>,
    Query(params): Query<HashMap<String, String>>,
) -> impl IntoResponse {
    let page: i64 = params.get("page").and_then(|v| v.parse().ok()).unwrap_or(1);
    let limit: i64 = params.get("limit").and_then(|v| v.parse().ok()).unwrap_or(10);
    let offset = (page - 1) * limit;

    let actual_doctor_id = sqlx::query!("SELECT id FROM doctors WHERE id = $1 OR account_id = $1", doctor_id)
        .fetch_one(&state.pool).await.map(|r| r.id).unwrap_or(doctor_id.to_string());
        
    let total = sqlx::query!("SELECT COUNT(*) FROM patients WHERE primary_doctor_id = $1", actual_doctor_id)
        .fetch_one(&state.pool).await.map(|r| r.count.unwrap_or(0)).unwrap_or(0);
    let total_pages = (total as f64 / limit as f64).ceil() as i64;

    let patients = sqlx::query!(
        "SELECT p.id, p.first_name, p.last_name, a.profile_photo 
         FROM patients p 
         LEFT JOIN accounts a ON p.account_id = a.id 
         WHERE p.primary_doctor_id = $1 ORDER BY p.id LIMIT $2 OFFSET $3", actual_doctor_id, limit, offset
    ).fetch_all(&state.pool).await.unwrap_or_default()
    .into_iter()
    .map(|row| {
        serde_json::json!({
            "id": row.id,
            "name": format!("{} {}", row.first_name, row.last_name).trim().to_string(),
            "profile_photo": row.profile_photo,
        })
    }).collect::<Vec<_>>();

    Json(PaginatedResponse {
        data: patients,
        pagination: PaginationInfo {
            total,
            page,
            limit,
            total_pages,
        }
    })
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
    if let Some(pid) = req.patient_id {
        let _ = sqlx::query!("UPDATE patients SET device_id = NULL WHERE device_id = $1", device_id).execute(&state.pool).await;
        let _ = sqlx::query!("UPDATE patients SET device_id = $1 WHERE id = $2", device_id, pid).execute(&state.pool).await;
    } else {
        let _ = sqlx::query!("UPDATE patients SET device_id = NULL WHERE device_id = $1", device_id).execute(&state.pool).await;
    }
    (StatusCode::OK, Json(serde_json::json!({"success": true})))
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
        
        // Buat session baru secara proaktif di database saat START
        if let Ok(record) = sqlx::query!("SELECT id FROM devices WHERE name = $1 OR id = $1 LIMIT 1", device_id).fetch_one(&state.pool).await {
            let new_id = crate::db::postgres::generate_custom_id(&state.pool, "sessions", "ses").await;
            let initial_file_path = format!("records/{}.jsonl", new_id);
            let now = chrono::Utc::now();
            
            let _ = sqlx::query!(
                "INSERT INTO sessions (id, device_id, patient_id, started_at, file_path) VALUES ($1, $2, $3, $4, $5)",
                new_id, record.id, cmd.patient_id, now, initial_file_path
            ).execute(&state.pool).await;
        }
    } else if cmd.command.to_uppercase() == "STOP" {
        info!(device_id = %device_id, "Perekaman Selesai");
        let now = chrono::Utc::now();
        if let Err(e) = sqlx::query!(
            "UPDATE sessions SET ended_at = $1 WHERE ended_at IS NULL AND device_id = (SELECT id FROM devices WHERE name = $2 OR id = $2 LIMIT 1)",
            now, device_id
        ).execute(&state.pool).await {
            error!(error = %e, device_id = %device_id, "Gagal mengupdate ended_at untuk sesi perekaman");
        }
    }

<<<<<<< HEAD
    // Query mqtt_topic from db
    let db_topic_record = sqlx::query!("SELECT mqtt_topic FROM devices WHERE id = $1", device_id)
        .fetch_one(&state.pool).await.ok();
        
    let base_topic = if let Some(record) = db_topic_record {
        record.mqtt_topic.unwrap_or_else(|| format!("ecgrhythmia/{}", device_id))
    } else {
        format!("ecgrhythmia/{}", device_id)
    };
    
    let topic = format!("{}/command", base_topic);
    let clients = state.mqtt_clients.read().await;
    
    if let Some(client) = clients.get(&device_id) {
        let payload = cmd.command.clone();
        if let Err(e) = client.clone().publish(&topic, rumqttc::QoS::AtLeastOnce, false, payload) {
=======
    // Dapatkan ID asli dan topic dari database
    let (true_id, mut topic) = if let Ok(conn) = state.pool.get() {
        conn.query_row(
            "SELECT id, mqtt_topic FROM devices WHERE id = ?1 OR name = ?1 LIMIT 1",
            params![device_id],
            |row| Ok((row.get::<_, String>(0)?, row.get::<_, Option<String>>(1)?))
        ).unwrap_or((device_id.clone(), None))
    } else {
        (device_id.clone(), None)
    };

    if let Some(ref pid) = cmd.patient_id {
        if let Ok(conn) = state.pool.get() {
            let _ = conn.execute("UPDATE devices SET assigned_to = ?1 WHERE id = ?2", params![pid, true_id]);
        }
    }
    
    // Jika tidak ada topic di DB, fallback menggunakan format default
    let publish_topic = topic
        .map(|t| format!("{}/command", t))
        .unwrap_or_else(|| format!("ecgrhythmia/{}/command", true_id));
        
    let clients = state.mqtt_clients.read().await;
    // Cari berdasarkan true_id (karena main.rs sekarang menyimpan menggunakan id)
    if let Some(client) = clients.get(&true_id) {
        if let Err(e) = client.clone().publish(publish_topic, rumqttc::QoS::AtLeastOnce, false, cmd.command) {
>>>>>>> d4e4ff69c48c853c58f915b255502ea5f0968312
            (StatusCode::INTERNAL_SERVER_ERROR, Json(serde_json::json!({"success": false, "message": format!("Gagal mengirim perintah: {}", e)})))
        } else {
            info!(device_id = %device_id, topic = %topic, command = %cmd.command, "Berhasil mengirim perintah MQTT ke perangkat");
            (StatusCode::OK, Json(serde_json::json!({"success": true})))
        }
    } else {
        (StatusCode::NOT_FOUND, Json(serde_json::json!({"success": false, "message": "Perangkat tidak memiliki koneksi MQTT aktif"})))
    }
}

async fn frame_preregister_handler() -> impl IntoResponse {
    // BYPASS: Database dikendalikan mutlak oleh backend (db_worker) agar sinkron 1:1 dengan .jsonl
    (StatusCode::OK, Json(ConfirmationResponse { success: true, message: "Frame di-bypass, ditangani oleh db_worker".to_string() }))
}

async fn frame_session_update_handler(
    State(state): State<AppState>,
    AxumPath(frame_id): AxumPath<String>,
    Json(req): Json<FrameSessionRequest>,
) -> impl IntoResponse {
    match sqlx::query!("UPDATE frame_records SET session_id = $1 WHERE id = $2", req.session_id, frame_id)
        .execute(&state.pool).await 
    {
        Ok(_) => (StatusCode::OK, Json(ConfirmationResponse { success: true, message: "Frame session updated".to_string() })),
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, Json(ConfirmationResponse { success: false, message: e.to_string() }))
    }
}

// DATABASE UTILITIES & CRUDS
async fn get_sessions_from_db(
    filter_patient_id: Option<String>,
    filter_doctor_id: Option<String>,
    page: i64,
    limit: i64,
    pool: &PgPool
) -> (Vec<SessionRecord>, i64) {
    let mut actual_doc_id = None;
    let mut is_doctor_filtered = false;
    
<<<<<<< HEAD
    if let Some(did) = filter_doctor_id {
        is_doctor_filtered = true;
        match sqlx::query!("SELECT id FROM doctors WHERE id = $1 OR account_id = $1", did).fetch_one(pool).await {
            Ok(r) => actual_doc_id = Some(r.id),
=======
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
>>>>>>> d4e4ff69c48c853c58f915b255502ea5f0968312
            Err(e) => {
                tracing::error!("Failed to find doctor for id {}: {}", did, e);
            }
        }
    }

    let mut actual_pat_id = None;
    let mut is_patient_filtered = false;
    
    if let Some(pid) = filter_patient_id {
        is_patient_filtered = true;
        match sqlx::query!("SELECT id FROM patients WHERE id = $1 OR account_id = $1", pid).fetch_one(pool).await {
            Ok(r) => actual_pat_id = Some(r.id),
            Err(e) => {
                tracing::error!("Failed to find patient for id {}: {}", pid, e);
            }
        }
    }

    // SECURITY FAILSAFE: If a doctor or patient filter was requested but NOT found in DB, return empty immediately!
    if (is_doctor_filtered && actual_doc_id.is_none()) || (is_patient_filtered && actual_pat_id.is_none()) {
        tracing::warn!("Security failsafe triggered: requested filter not found in database. Returning empty sessions array.");
        return (vec![], 0);
    }

    if let (Some(pid), Some(did)) = (&actual_pat_id, &actual_doc_id) {
        let belongs = sqlx::query!("SELECT 1 as x FROM patients WHERE id = $1 AND primary_doctor_id = $2", pid, did)
            .fetch_optional(pool).await.unwrap_or_default().is_some();
        if !belongs {
            return (vec![], 0);
        }
    }

    let offset = (page - 1) * limit;

    let (records, total): (Vec<SessionRecord>, i64) = if let Some(pid) = actual_pat_id {
        let total = sqlx::query!("SELECT COUNT(*) FROM sessions WHERE patient_id = $1", pid).fetch_one(pool).await.map(|r| r.count.unwrap_or(0)).unwrap_or(0);
        let records = sqlx::query!(
            "SELECT s.id, s.device_id, s.patient_id, p.first_name || ' ' || p.last_name as patient_name, s.started_at, s.ended_at, s.file_path, s.ecg_paper 
             FROM sessions s LEFT JOIN patients p ON s.patient_id = p.id 
             WHERE s.patient_id = $1 ORDER BY s.started_at DESC LIMIT $2 OFFSET $3", pid, limit, offset
        ).fetch_all(pool).await.unwrap_or_default()
        .into_iter().map(|row| SessionRecord {
            id: row.id, device_id: row.device_id, patient_id: Some(row.patient_id), patient_name: row.patient_name,
            started_at: row.started_at.to_rfc3339(), ended_at: row.ended_at.map(|d| d.to_rfc3339()), file_path: row.file_path.unwrap_or_default(),
            ecg_paper: row.ecg_paper
        }).collect();
        (records, total)
    } else if let Some(did) = actual_doc_id {
        let total = sqlx::query!("SELECT COUNT(*) FROM sessions s JOIN patients p ON s.patient_id = p.id WHERE p.primary_doctor_id = $1", did).fetch_one(pool).await.map(|r| r.count.unwrap_or(0)).unwrap_or(0);
        let records = sqlx::query!(
            "SELECT s.id, s.device_id, s.patient_id, p.first_name || ' ' || p.last_name as patient_name, s.started_at, s.ended_at, s.file_path, s.ecg_paper 
             FROM sessions s JOIN patients p ON s.patient_id = p.id 
             WHERE p.primary_doctor_id = $1 ORDER BY s.started_at DESC LIMIT $2 OFFSET $3", did, limit, offset
        ).fetch_all(pool).await.unwrap_or_default()
        .into_iter().map(|row| SessionRecord {
            id: row.id, device_id: row.device_id, patient_id: Some(row.patient_id), patient_name: row.patient_name,
            started_at: row.started_at.to_rfc3339(), ended_at: row.ended_at.map(|d| d.to_rfc3339()), file_path: row.file_path.unwrap_or_default(),
            ecg_paper: row.ecg_paper
        }).collect();
        (records, total)
    } else {
        let total = sqlx::query!("SELECT COUNT(*) FROM sessions").fetch_one(pool).await.map(|r| r.count.unwrap_or(0)).unwrap_or(0);
        let records = sqlx::query!(
            "SELECT s.id, s.device_id, s.patient_id, p.first_name || ' ' || p.last_name as patient_name, s.started_at, s.ended_at, s.file_path, s.ecg_paper 
             FROM sessions s LEFT JOIN patients p ON s.patient_id = p.id 
             ORDER BY s.started_at DESC LIMIT $1 OFFSET $2", limit, offset
        ).fetch_all(pool).await.unwrap_or_default()
        .into_iter().map(|row| SessionRecord {
            id: row.id, device_id: row.device_id, patient_id: Some(row.patient_id), patient_name: row.patient_name,
            started_at: row.started_at.to_rfc3339(), ended_at: row.ended_at.map(|d| d.to_rfc3339()), file_path: row.file_path.unwrap_or_default(),
            ecg_paper: row.ecg_paper
        }).collect();
        (records, total)
    };
    (records, total)
}

async fn get_devices_from_db(pool: &PgPool) -> Vec<DeviceRecord> {
    sqlx::query!(
        "SELECT d.id as \"id!\", d.name as \"name!\", d.mqtt_broker, d.mqtt_port, d.mqtt_topic, d.mqtt_username, (SELECT p.id FROM patients p WHERE p.device_id = d.id LIMIT 1) as \"assigned_to?\"
         FROM devices d"
    ).fetch_all(pool).await.unwrap_or_default()
    .into_iter().map(|row| DeviceRecord {
        id: row.id, name: row.name, mqtt_broker: row.mqtt_broker, mqtt_port: row.mqtt_port,
        mqtt_topic: row.mqtt_topic, mqtt_username: row.mqtt_username, assigned_to: row.assigned_to
    }).collect()
}

async fn get_admin_stats(pool: &PgPool) -> AdminStats {
    let mut stats = AdminStats { total_patients: 0, total_doctors: 0, active_devices: 0, critical_alerts: 0 };
    stats.total_patients = sqlx::query!("SELECT COUNT(*) FROM patients").fetch_one(pool).await.map(|r| r.count.unwrap_or(0)).unwrap_or(0);
    stats.total_doctors = sqlx::query!("SELECT COUNT(*) FROM doctors").fetch_one(pool).await.map(|r| r.count.unwrap_or(0)).unwrap_or(0);
    stats.active_devices = sqlx::query!("SELECT COUNT(*) FROM devices").fetch_one(pool).await.map(|r| r.count.unwrap_or(0)).unwrap_or(0);
    
    let today = chrono::Local::now().format("%Y-%m-%d").to_string();
    let today_prefix = format!("{}%", today);
    
    let paths = sqlx::query!("SELECT file_path FROM sessions WHERE CAST(started_at AS TEXT) LIKE $1", today_prefix).fetch_all(pool).await.unwrap_or_default();
    let mut critical_count = 0;
    for path in paths {
        if let Ok(contents) = fs::read_to_string(path.file_path.as_deref().unwrap_or_default()) {
            for line in contents.lines() {
                if !line.contains("\"label\":\"Normal\"") && line.contains("\"label\":") {
                    critical_count += 1;
                }
            }
        }
    }
    stats.critical_alerts = critical_count as i64;
    stats
}

<<<<<<< HEAD
async fn get_admin_users(page: i64, limit: i64, role_filter: Option<String>, pool: &PgPool) -> (Vec<AdminUser>, i64) {
    let offset = (page - 1) * limit;
    let mut total;
    
    if let Some(r) = role_filter {
        total = sqlx::query!("SELECT COUNT(*) FROM accounts WHERE role = $1", r).fetch_one(pool).await.map(|row| row.count.unwrap_or(0)).unwrap_or(0);
        if r == "pasien" {
            let users = sqlx::query!(
                "SELECT p.id, a.id as account_id, p.first_name || ' ' || p.last_name as name, a.role, COALESCE(a.status, 'Offline') as status, a.created_at, p.primary_doctor_id as connected_doctor_id, p.device_id as connected_device_id, a.profile_photo
                 FROM patients p JOIN accounts a ON p.account_id = a.id
                 WHERE a.role = 'pasien'
                 ORDER BY a.created_at DESC LIMIT $1 OFFSET $2", limit, offset
            ).fetch_all(pool).await.unwrap_or_default()
            .into_iter().map(|row| AdminUser {
                id: row.id, account_id: row.account_id, name: row.name.unwrap_or_default(), role: row.role, status: row.status.unwrap_or_default(), 
                registered_at: row.created_at.map(|t| t.to_string()), connected_doctor_id: row.connected_doctor_id, connected_device_id: row.connected_device_id, profile_photo: row.profile_photo
            }).collect();
            return (users, total);
        } else if r == "dokter" {
            let users = sqlx::query!(
                "SELECT d.id, a.id as account_id, d.first_name || ' ' || d.last_name as name, a.role, COALESCE(a.status, 'Offline') as status, a.created_at, NULL as connected_doctor_id, NULL as connected_device_id, a.profile_photo
                 FROM doctors d JOIN accounts a ON d.account_id = a.id
                 WHERE a.role = 'dokter'
                 ORDER BY a.created_at DESC LIMIT $1 OFFSET $2", limit, offset
            ).fetch_all(pool).await.unwrap_or_default()
            .into_iter().map(|row| AdminUser {
                id: row.id, account_id: row.account_id, name: row.name.unwrap_or_default(), role: row.role, status: row.status.unwrap_or_default(), 
                registered_at: row.created_at.map(|t| t.to_string()), connected_doctor_id: row.connected_doctor_id, connected_device_id: row.connected_device_id, profile_photo: row.profile_photo
            }).collect();
            return (users, total);
        }
    }
    
    total = sqlx::query!("SELECT COUNT(*) FROM accounts").fetch_one(pool).await.map(|r| r.count.unwrap_or(0)).unwrap_or(0);
    
    let users = sqlx::query!(
        "SELECT p.id, a.id as account_id, p.first_name || ' ' || p.last_name as name, a.role, COALESCE(a.status, 'Offline') as status, a.created_at, p.primary_doctor_id as connected_doctor_id, p.device_id as connected_device_id, a.profile_photo
         FROM patients p JOIN accounts a ON p.account_id = a.id
         UNION ALL
         SELECT d.id, a.id as account_id, d.first_name || ' ' || d.last_name as name, a.role, COALESCE(a.status, 'Offline') as status, a.created_at, NULL as connected_doctor_id, NULL as connected_device_id, a.profile_photo
         FROM doctors d JOIN accounts a ON d.account_id = a.id
         ORDER BY created_at DESC LIMIT $1 OFFSET $2", limit, offset
    ).fetch_all(pool).await.unwrap_or_default()
    .into_iter().map(|row| AdminUser {
        id: row.id.unwrap_or_default(), account_id: row.account_id.unwrap_or_default(), name: row.name.unwrap_or_default(), role: row.role.unwrap_or_default(), status: row.status.unwrap_or_default(), 
        registered_at: row.created_at.map(|t| t.to_string()), connected_doctor_id: row.connected_doctor_id, connected_device_id: row.connected_device_id, profile_photo: row.profile_photo
    }).collect();
    
=======
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
>>>>>>> d4e4ff69c48c853c58f915b255502ea5f0968312
    (users, total)
}

async fn get_patient_profile(patient_id: String, pool: &PgPool) -> Option<PatientProfileResponse> {
    let patient_res = sqlx::query!(
        "SELECT p.id, p.first_name, p.last_name, p.age, p.gender, p.primary_doctor_id, a.profile_photo, p.device_id
         FROM patients p LEFT JOIN accounts a ON p.account_id = a.id WHERE p.id = $1 OR p.account_id = $1", patient_id
    ).fetch_one(pool).await.ok()?;

    let mut doctor = None;
    if let Some(doc_id) = patient_res.primary_doctor_id.clone() {
        if let Ok(doc_res) = sqlx::query!(
            "SELECT d.id, d.first_name, d.last_name, a.profile_photo FROM doctors d LEFT JOIN accounts a ON d.account_id = a.id WHERE d.id = $1", doc_id
        ).fetch_one(pool).await {
            doctor = Some(DoctorRecord {
                id: doc_res.id, first_name: doc_res.first_name, last_name: doc_res.last_name, profile_photo: doc_res.profile_photo
            });
        }
    }

    Some(PatientProfileResponse {
        patient: PatientRecord {
            id: patient_res.id, first_name: patient_res.first_name, last_name: patient_res.last_name, age: patient_res.age.to_string(),
            gender: patient_res.gender.unwrap_or_default(), primary_doctor_id: patient_res.primary_doctor_id, profile_photo: patient_res.profile_photo, device_id: patient_res.device_id
        },
        doctor,
    })
}

async fn get_doctor_profile(doctor_id: String, pool: &PgPool) -> Option<DoctorProfileResponse> {
    let res = sqlx::query!(
        "SELECT d.id, d.first_name, d.last_name, a.email, a.role, a.profile_photo FROM doctors d LEFT JOIN accounts a ON d.account_id = a.id WHERE d.id = $1 OR d.account_id = $1", doctor_id
    ).fetch_one(pool).await.ok()?;

    Some(DoctorProfileResponse {
        id: res.id, first_name: res.first_name, last_name: res.last_name,
        email: res.email, role: res.role, profile_photo: res.profile_photo
    })
}

async fn update_doctor_profile(doctor_id: &str, req: UpdateDoctorProfileRequest, pool: &PgPool, api_url: &str) -> Result<(), String> {
    let doctor_record = sqlx::query!("SELECT id, account_id FROM doctors WHERE id = $1 OR account_id = $1", doctor_id)
        .fetch_one(pool).await.map_err(|_| "Dokter tidak ditemukan".to_string())?;
    let actual_doctor_id = doctor_record.id;
    let account_id = doctor_record.account_id;

    let mut final_photo_url = req.profile_photo.filter(|s| !s.is_empty());

    if let Some(ref photo_data) = final_photo_url {
        if photo_data.starts_with("data:image/") {
            if let Some(comma_pos) = photo_data.find(',') {
                let base64_str = &photo_data[comma_pos + 1..];
                if let Ok(image_bytes) = base64_engine.decode(base64_str) {
                    let uploads_dir = Path::new("uploads/profiles");
                    let _ = fs::create_dir_all(uploads_dir);
                    let filename = format!("{}_{}.jpg", doctor_id, Utc::now().timestamp());
                    let filepath = uploads_dir.join(&filename);
                    if fs::write(&filepath, image_bytes).is_ok() {
                        final_photo_url = Some(format!("{}/uploads/profiles/{}", api_url.trim_end_matches('/'), filename));
                    }
                }
            }
        }
    }

    sqlx::query!("UPDATE doctors SET first_name = $1, last_name = $2 WHERE id = $3", req.first_name, req.last_name, actual_doctor_id)
        .execute(pool).await.map_err(|e| e.to_string())?;

    sqlx::query!("UPDATE accounts SET profile_photo = $1 WHERE id = $2", final_photo_url, account_id)
        .execute(pool).await.map_err(|e| e.to_string())?;

    Ok(())
}

async fn update_patient_profile(patient_id: &str, req: UpdatePatientProfileRequest, pool: &PgPool, api_url: &str) -> Result<(), String> {
    let patient_record = sqlx::query!("SELECT id, account_id FROM patients WHERE id = $1 OR account_id = $1", patient_id)
        .fetch_one(pool).await.map_err(|_| "Pasien tidak ditemukan".to_string())?;
    let actual_patient_id = patient_record.id;
    let account_id = patient_record.account_id;

    let mut final_photo_url = req.profile_photo.filter(|s| !s.is_empty());

    if let Some(ref photo_data) = final_photo_url {
        if photo_data.starts_with("data:image/") {
            if let Some(comma_pos) = photo_data.find(',') {
                let base64_str = &photo_data[comma_pos + 1..];
                if let Ok(image_bytes) = base64_engine.decode(base64_str) {
                    let uploads_dir = Path::new("uploads/profiles");
                    let _ = fs::create_dir_all(uploads_dir);
                    let filename = format!("{}_{}.jpg", patient_id, Utc::now().timestamp());
                    let filepath = uploads_dir.join(&filename);
                    if fs::write(&filepath, image_bytes).is_ok() {
                        final_photo_url = Some(format!("{}/uploads/profiles/{}", api_url.trim_end_matches('/'), filename));
                    }
                }
            }
        }
    }

<<<<<<< HEAD
    sqlx::query!("UPDATE patients SET first_name = $1, last_name = $2, age = $3 WHERE id = $4", req.first_name, req.last_name, req.age.parse::<i32>().unwrap_or(0), actual_patient_id)
        .execute(pool).await.map_err(|e| e.to_string())?;
=======
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
>>>>>>> d4e4ff69c48c853c58f915b255502ea5f0968312

    sqlx::query!("UPDATE accounts SET profile_photo = $1 WHERE id = $2", final_photo_url, account_id)
        .execute(pool).await.map_err(|e| e.to_string())?;

    Ok(())
}

fn read_jsonl_file(session_id: &str) -> String {
    let file_path = format!("records/{}.jsonl", session_id);
    let fallback_path = format!("records/records_local/{}.jsonl", session_id);
    if let Ok(contents) = fs::read_to_string(&file_path) {
        let lines: Vec<&str> = contents.lines().filter(|l| !l.is_empty()).collect();
        format!("[{}]", lines.join(","))
    } else if let Ok(contents) = fs::read_to_string(&fallback_path) {
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
    let mut system_data: HashMap<String, String> = HashMap::new();
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
                } else if file_name.ends_with("_system.json") {
                    if let Ok(bytes) = field.bytes().await {
                        if let Ok(text) = String::from_utf8(bytes.to_vec()) {
                            system_data.insert(file_stem, text);
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

        let system_obj: Option<crate::models::device::DeviceSystem> = system_data.get(&format!("{}_system", stem))
            .or_else(|| {
                let system_filename = metadata.source_metadata.as_ref()
                    .and_then(|m| m.csv_file.as_ref())
                    .map(|s| s.replace("_ecg.csv", "_system.json").replace(".csv", "_system.json"))
                    .unwrap_or_default();
                let system_stem = Path::new(&system_filename).file_stem().unwrap_or_default().to_string_lossy().to_string();
                system_data.get(&system_stem)
            })
            .and_then(|json_str| serde_json::from_str(json_str).ok());

        let payload = crate::models::device::DevicePayload {
            message_id: measurement_id,
            device_id: resolved_device_id.clone(),
            session_id: resolved_session_id.clone(),
            patient_id: patient_id.clone(),
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
            system: system_obj,
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
    pub mqtt_port: i32,
    pub mqtt_topic: String,
    pub mqtt_username: String,
    pub mqtt_password: String,
}

#[derive(Deserialize)]
pub struct EditDeviceReq {
    pub name: String,
    pub mqtt_broker: String,
    pub mqtt_port: i32,
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
    if let Err(e) = sqlx::query!(
        "INSERT INTO devices (id, name, mqtt_broker, mqtt_port, mqtt_topic, mqtt_username, mqtt_password) VALUES ($1, $2, $3, $4, $5, $6, $7)",
        dev_id, req.name, req.mqtt_broker, req.mqtt_port, req.mqtt_topic, req.mqtt_username, req.mqtt_password
    ).execute(&state.pool).await {
        return (StatusCode::INTERNAL_SERVER_ERROR, Json(serde_json::json!({"success": false, "message": e.to_string()})));
    }
    
    let db_tx = state.db_tx.clone();
    let port = req.mqtt_port as u16;
    let client = crate::network::mqtt_listener::start_mqtt_listener(
        &req.mqtt_broker, port, &req.mqtt_topic, &req.mqtt_username, &req.mqtt_password,
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
    let old_name = sqlx::query!("SELECT name FROM devices WHERE id = $1", id).fetch_one(&state.pool).await.map(|r| r.name).ok();
    if let Err(e) = sqlx::query!(
        "UPDATE devices SET name = $1, mqtt_broker = $2, mqtt_port = $3, mqtt_topic = $4, mqtt_username = $5, mqtt_password = $6 WHERE id = $7",
        req.name, req.mqtt_broker, req.mqtt_port, req.mqtt_topic, req.mqtt_username, req.mqtt_password, id
    ).execute(&state.pool).await {
        return (StatusCode::INTERNAL_SERVER_ERROR, Json(serde_json::json!({"success": false, "message": e.to_string()})));
    }

    if let Some(old_name) = old_name {
        let mut clients = state.mqtt_clients.write().await;
        if let Some(old_client) = clients.remove(&old_name) {
            let _ = old_client.disconnect();
        }
    }

    let db_tx = state.db_tx.clone();
    let port = req.mqtt_port as u16;
    let client = crate::network::mqtt_listener::start_mqtt_listener(
        &req.mqtt_broker, port, &req.mqtt_topic, &req.mqtt_username, &req.mqtt_password,
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
    let cors = CorsLayer::new()
        .allow_origin([
            "https://ecgrhythmia.cloud".parse::<HeaderValue>().unwrap(),
            "https://www.ecgrhythmia.cloud".parse::<HeaderValue>().unwrap(),
            "http://localhost:5173".parse::<HeaderValue>().unwrap(),
        ])
<<<<<<< HEAD
        .allow_methods([Method::GET, Method::POST, Method::PUT, Method::DELETE, Method::OPTIONS])
        .allow_headers([header::CONTENT_TYPE, header::AUTHORIZATION, header::ACCEPT])
        .allow_credentials(true);

    Router::new()
        .route("/api/auth/me", get(auth_me_handler))
        .route("/api/auth/register_profile", post(register_profile_handler))
        .route("/api/auth/register", post(admin_register_handler))
        .route("/api/sessions", get(get_sessions_handler))
=======
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
>>>>>>> d4e4ff69c48c853c58f915b255502ea5f0968312
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
<<<<<<< HEAD
        .route("/api/doctors/impersonate/:target_id", post(doctor_impersonate_handler))
        .route("/api/records/:session_id", get(get_record_handler))
        .route("/api/sessions/:session_id/ecg_paper", post(upload_ecg_paper_handler).delete(delete_ecg_paper_handler))
=======
        .route("/api/records", post(create_record_handler))
        .route("/api/records/:session_id", get(get_record_handler))
        .route("/api/records/:session_id/download", get(download_record_handler))
>>>>>>> d4e4ff69c48c853c58f915b255502ea5f0968312
        .route("/api/devices/:device_id/command", post(device_command_handler))
        .route("/api/devices/:device_id/assign", post(assign_device_handler))
        .route("/api/frames", post(frame_preregister_handler))
        .route("/api/frames/:id/session", put(frame_session_update_handler))
        .nest_service("/uploads", tower_http::services::ServeDir::new("uploads"))
        .layer(DefaultBodyLimit::max(50 * 1024 * 1024))
        .layer(TraceLayer::new_for_http())
        .layer(cors)
        .with_state(state)
}
<<<<<<< HEAD


#[derive(Serialize)]
pub struct UploadResponse {
    pub success: bool,
    pub path: Option<String>,
    pub message: Option<String>,
}

pub async fn upload_ecg_paper_handler(
    State(state): State<AppState>,
    AxumPath(session_id): AxumPath<String>,
    mut multipart: Multipart,
) -> impl IntoResponse {
    while let Some(field) = multipart.next_field().await.unwrap_or(None) {
        if field.name() == Some("paper") {
            let data = match field.bytes().await {
                Ok(d) => d,
                Err(_) => return (StatusCode::BAD_REQUEST, Json(UploadResponse { success: false, path: None, message: Some("Failed to read file data".to_string()) })),
            };

            // Hapus file lama jika ada
            if let Ok(record) = sqlx::query!("SELECT ecg_paper FROM sessions WHERE id = $1", session_id).fetch_one(&state.pool).await {
                if let Some(old_path) = record.ecg_paper {
                    if let Some(filename) = old_path.split('/').last() {
                        let old_file_path = format!("uploads/ecg_papers/{}", filename);
                        let _ = tokio::fs::remove_file(&old_file_path).await;
                    }
                }
            }

            let file_name = format!("{}_{}.jpg", session_id, uuid::Uuid::new_v4());
            let file_path = format!("uploads/ecg_papers/{}", file_name);
            let public_path = format!("/uploads/ecg_papers/{}", file_name);

            match tokio::fs::write(&file_path, &data).await {
                Ok(_) => {
                    let update_result = sqlx::query!(
                        "UPDATE sessions SET ecg_paper = $1 WHERE id = $2",
                        public_path,
                        session_id
                    ).execute(&state.pool).await;

                    if update_result.is_ok() {
                        return (StatusCode::OK, Json(UploadResponse { success: true, path: Some(public_path), message: None }));
                    } else {
                        return (StatusCode::INTERNAL_SERVER_ERROR, Json(UploadResponse { success: false, path: None, message: Some("Failed to update database".to_string()) }));
                    }
                }
                Err(_) => return (StatusCode::INTERNAL_SERVER_ERROR, Json(UploadResponse { success: false, path: None, message: Some("Failed to save file".to_string()) })),
            }
        }
    }
    (StatusCode::BAD_REQUEST, Json(UploadResponse { success: false, path: None, message: Some("No file uploaded".to_string()) }))
}

pub async fn delete_ecg_paper_handler(
    State(state): State<AppState>,
    AxumPath(session_id): AxumPath<String>,
) -> impl IntoResponse {
    // Hapus file lama jika ada
    if let Ok(record) = sqlx::query!("SELECT ecg_paper FROM sessions WHERE id = $1", session_id).fetch_one(&state.pool).await {
        if let Some(old_path) = record.ecg_paper {
            if let Some(filename) = old_path.split('/').last() {
                let old_file_path = format!("uploads/ecg_papers/{}", filename);
                let _ = tokio::fs::remove_file(&old_file_path).await;
            }
        }
    }

    let update_result = sqlx::query!(
        "UPDATE sessions SET ecg_paper = NULL WHERE id = $1",
        session_id
    ).execute(&state.pool).await;

    if update_result.is_ok() {
        (StatusCode::OK, Json(UploadResponse { success: true, path: None, message: None }))
    } else {
        (StatusCode::INTERNAL_SERVER_ERROR, Json(UploadResponse { success: false, path: None, message: Some("Failed to update database".to_string()) }))
    }
}
=======
>>>>>>> d4e4ff69c48c853c58f915b255502ea5f0968312
