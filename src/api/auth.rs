use axum::{
    async_trait,
    extract::FromRequestParts,
    http::request::Parts,
    http::StatusCode,
};
use jsonwebtoken::{decode, DecodingKey, Validation, Algorithm};
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
pub struct Claims {
    pub sub: String,
    pub role: Option<String>,
    pub exp: usize,
}

pub struct JwtAuth(pub String); // Menyimpan user_id (UUID)

#[async_trait]
impl<S> FromRequestParts<S> for JwtAuth
where
    S: Send + Sync,
{
    type Rejection = (StatusCode, String);

    async fn from_request_parts(parts: &mut Parts, _state: &S) -> Result<Self, Self::Rejection> {
        let auth_header = parts
            .headers
            .get("Authorization")
            .and_then(|h| h.to_str().ok());

        let token = match auth_header {
            Some(header) if header.starts_with("Bearer ") => &header[7..],
            _ => return Err((StatusCode::UNAUTHORIZED, "Missing or invalid Authorization header".to_string())),
        };

        // Karena kita tidak memiliki akses langsung ke AppState di sini dengan mudah jika kita
        // tidak ingin membuat extractor yang spesifik ke state, kita bisa mengambil rahasianya 
        // langsung dari env untuk middleware JWT, atau kita bisa menggunakan Extension.
        // Di sini kita akan menggunakan env::var.
        let jwt_secret = std::env::var("SUPABASE_JWT_SECRET")
            .unwrap_or_else(|_| String::new());

        if jwt_secret.is_empty() {
            return Err((StatusCode::INTERNAL_SERVER_ERROR, "JWT secret not configured".to_string()));
        }

        let mut validation = Validation::new(Algorithm::HS256);
        validation.set_audience(&["authenticated"]); // Opsional, Supabase menggunakan aud: "authenticated"

        let token_data = decode::<Claims>(
            token,
            &DecodingKey::from_secret(jwt_secret.as_bytes()),
            &validation,
        ).map_err(|e| {
            tracing::error!("JWT Validation Error: {}", e);
            (StatusCode::UNAUTHORIZED, "Invalid token".to_string())
        })?;

        Ok(JwtAuth(token_data.claims.sub))
    }
}
