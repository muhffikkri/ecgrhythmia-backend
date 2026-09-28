use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine as _};
use jsonwebtoken::{decode, decode_header, Algorithm, DecodingKey, Validation};
use serde::Deserialize;
use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};
use tracing::{error, info, warn};

/// Header DER tetap untuk SubjectPublicKeyInfo kunci publik EC P-256 yang tidak
/// terkompresi. jsonwebtoken hanya menerima kunci EC dalam bentuk PEM, sedangkan
/// JWKS memberi koordinat x/y mentah, jadi SPKI disusun manual di sini.
///
///   SEQUENCE (0x59)
///     SEQUENCE (0x13)          -- AlgorithmIdentifier
///       OID 1.2.840.10045.2.1   -- id-ecPublicKey
///       OID 1.2.840.10045.3.1.7 -- prime256v1
///     BIT STRING (0x42)
///       0x00                   -- 0 bit unused
///       0x04                   -- titik tak terkompresi
///       X (32 byte) || Y (32 byte)
const P256_SPKI_PREFIX: [u8; 27] = [
    0x30, 0x59, 0x30, 0x13, 0x06, 0x07, 0x2a, 0x86, 0x48, 0xce, 0x3d, 0x02, 0x01, 0x06, 0x08, 0x2a,
    0x86, 0x48, 0xce, 0x3d, 0x03, 0x01, 0x07, 0x03, 0x42, 0x00, 0x04,
];

#[derive(Debug, Deserialize)]
struct Jwk {
    kty: String,
    kid: Option<String>,
    alg: Option<String>,
    crv: Option<String>,
    x: Option<String>,
    y: Option<String>,
}

#[derive(Debug, Deserialize)]
struct JwkSet {
    keys: Vec<Jwk>,
}

#[derive(Clone)]
pub struct Jwks {
    url: Option<String>,
    keys: Arc<RwLock<HashMap<String, DecodingKey>>>,
    fetched_at: Arc<RwLock<Option<Instant>>>,
    http: reqwest::Client,
}

const REFRESH_INTERVAL: Duration = Duration::from_secs(60 * 60);

impl Jwks {
    pub fn new(url: Option<String>) -> Self {
        let http = reqwest::Client::builder()
            .timeout(Duration::from_secs(10))
            .build()
            .unwrap_or_default();

        Jwks {
            url,
            keys: Arc::new(RwLock::new(HashMap::new())),
            fetched_at: Arc::new(RwLock::new(None)),
            http,
        }
    }

    pub fn is_enabled(&self) -> bool {
        self.url.is_some()
    }

    /// Muat JWKS saat startup supaya request pertama tidak menunggu network.
    pub async fn warmup(&self) {
        if self.is_enabled() && self.refresh().await.is_err() {
            warn!("Gagal memuat JWKS saat startup; ES256 akan dicoba lagi saat request");
        }
    }

    async fn refresh(&self) -> Result<(), String> {
        let url = self.url.as_ref().ok_or("JWKS nonaktif")?;

        let resp = self
            .http
            .get(url)
            .send()
            .await
            .map_err(|e| format!("request JWKS gagal: {}", e))?;

        if !resp.status().is_success() {
            return Err(format!("JWKS HTTP {}", resp.status()));
        }

        let set: JwkSet = resp
            .json()
            .await
            .map_err(|e| format!("parse JWKS gagal: {}", e))?;

        let mut parsed = HashMap::new();
        for jwk in set.keys {
            if jwk.kty != "EC" || jwk.crv.as_deref() != Some("P-256") {
                continue;
            }
            if let Some(alg) = &jwk.alg {
                if alg != "ES256" {
                    continue;
                }
            }
            let (Some(kid), Some(x), Some(y)) = (jwk.kid, jwk.x, jwk.y) else {
                continue;
            };
            let pem = match pem_for(&x, &y) {
                Ok(p) => p,
                Err(e) => {
                    warn!("JWK kid={} tidak bisa dipakai: {}", kid, e);
                    continue;
                }
            };
            match DecodingKey::from_ec_pem(pem.as_bytes()) {
                Ok(key) => {
                    parsed.insert(kid, key);
                }
                Err(e) => warn!("Gagal membuat DecodingKey untuk kid={}: {}", kid, e),
            }
        }

        if parsed.is_empty() {
            return Err("JWKS tidak memuat kunci ES256 yang cocok".to_string());
        }

        let count = parsed.len();
        *self.keys.write().unwrap() = parsed;
        *self.fetched_at.write().unwrap() = Some(Instant::now());
        info!("JWKS dimuat: {} kunci ES256 dari {}", count, url);
        Ok(())
    }

    /// Verifikasi token ES256. Mengembalikan `Ok(())` bila signature sah.
    async fn verify_es256<T: serde::de::DeserializeOwned>(
        &self,
        token: &str,
    ) -> Result<T, JwtFailure> {
        let header = decode_header(token).map_err(|_| JwtFailure::Malformed)?;
        let kid = header.kid.ok_or(JwtFailure::UnknownKid)?;

        let cached = self.keys.read().unwrap().get(&kid).cloned();
        let key = match cached {
            Some(k) => k,
            None => {
                if self
                    .fetched_at
                    .read()
                    .unwrap()
                    .map(|t| t.elapsed() < REFRESH_INTERVAL)
                    .unwrap_or(false)
                {
                    return Err(JwtFailure::UnknownKid);
                }
                self.refresh().await.map_err(|e| {
                    error!("Refresh JWKS gagal: {}", e);
                    JwtFailure::UnknownKid
                })?;
                self.keys
                    .read()
                    .unwrap()
                    .get(&kid)
                    .cloned()
                    .ok_or(JwtFailure::UnknownKid)?
            }
        };

        let mut validation = Validation::new(Algorithm::ES256);
        validation.validate_aud = false;
        validation.required_spec_claims.clear();

        decode::<T>(token, &key, &validation)
            .map(|d| d.claims)
            .map_err(|e| {
                use jsonwebtoken::errors::ErrorKind;
                match e.kind() {
                    ErrorKind::ExpiredSignature => JwtFailure::Expired,
                    ErrorKind::InvalidSignature => JwtFailure::InvalidSignature,
                    _ => JwtFailure::Malformed,
                }
            })
    }

    /// Verifikasi signature ES256 lalu deserialize payload menjadi `T`.
    pub async fn decode<T: serde::de::DeserializeOwned>(
        &self,
        token: &str,
    ) -> Result<T, JwtFailure> {
        if !self.is_enabled() {
            return Err(JwtFailure::Disabled);
        }
        self.verify_es256(token).await
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JwtFailure {
    Disabled,
    Malformed,
    UnknownKid,
    InvalidSignature,
    Expired,
}

fn pem_for(x_b64: &str, y_b64: &str) -> Result<String, String> {
    let x = URL_SAFE_NO_PAD
        .decode(x_b64)
        .map_err(|e| format!("x bukan base64url: {}", e))?;
    let y = URL_SAFE_NO_PAD
        .decode(y_b64)
        .map_err(|e| format!("y bukan base64url: {}", e))?;

    if x.len() != 32 || y.len() != 32 {
        return Err(format!(
            "koordinat P-256 harus 32 byte (x={}, y={})",
            x.len(),
            y.len()
        ));
    }

    let mut der = Vec::with_capacity(P256_SPKI_PREFIX.len() + 64);
    der.extend_from_slice(&P256_SPKI_PREFIX);
    der.extend_from_slice(&x);
    der.extend_from_slice(&y);

    let b64 = base64::engine::general_purpose::STANDARD.encode(&der);
    let mut pem = String::from("-----BEGIN PUBLIC KEY-----\n");
    for chunk in b64.as_bytes().chunks(64) {
        pem.push_str(std::str::from_utf8(chunk).map_err(|e| e.to_string())?);
        pem.push('\n');
    }
    pem.push_str("-----END PUBLIC KEY-----\n");
    Ok(pem)
}

#[cfg(test)]
mod tests {
    use super::*;

    // Kunci + token dibuat dengan `openssl` dan ditandatangani ES256 sungguhan,
    // supaya konversi JWK -> SPKI/PEM ikut teruji, bukan hanya parsing JSON.
    const X: &str = "Hn8LhHfivxOUx4_E4nOVH_4QR9HybGT6QCBIOelDtho";
    const Y: &str = "ZA_Z0ucaM_x5lIDfmSU4tPRX9A6J0wOuI4v0JEZVGME";
    const TOKEN: &str = "eyJ0eXAiOiJKV1QiLCJhbGciOiJFUzI1NiIsImtpZCI6InRlc3Qta2lkLTEifQ.eyJzdWIiOiJzdXBhYmFzZS11c2VyLXV1aWQtMTIzNCIsInJvbGUiOiJhdXRoZW50aWNhdGVkIiwiYXBwX21ldGFkYXRhIjp7InJvbGUiOiJhZG1pbiJ9LCJlbWFpbCI6InBrbWtjd2VhcmFibGVlY2dAZ21haWwuY29tIiwiZXhwIjoxNzkwNTk2MTE3fQ.ininPBTo2oagTFVCVFrTocrsWhYQ-_v07uDh8F5sEL-CfArd5zxiIyuiiKZPFWYcDsAgABNmSLWL6iGgj9O1XQ";

    fn jwks_with_key(kid: &str) -> Jwks {
        let jwks = Jwks::new(Some("http://test.invalid/jwks".to_string()));
        let key = DecodingKey::from_ec_pem(pem_for(X, Y).unwrap().as_bytes()).unwrap();
        jwks.keys.write().unwrap().insert(kid.to_string(), key);
        *jwks.fetched_at.write().unwrap() = Some(Instant::now());
        jwks
    }

    #[test]
    fn pem_is_der_of_expected_length() {
        let pem = pem_for(X, Y).unwrap();
        assert!(pem.starts_with("-----BEGIN PUBLIC KEY-----"));
        assert!(pem.trim_end().ends_with("-----END PUBLIC KEY-----"));
        let body: String = pem
            .lines()
            .filter(|l| !l.starts_with("-----"))
            .collect::<Vec<_>>()
            .join("");
        let der = base64::engine::general_purpose::STANDARD
            .decode(&body)
            .unwrap();
        assert_eq!(der.len(), 91, "SPKI P-256 uncompressed = 91 byte");
        assert_eq!(&der[..27], &P256_SPKI_PREFIX[..]);
    }

    #[test]
    fn rejects_wrong_coordinate_length() {
        assert!(pem_for("AAAA", Y).is_err());
        assert!(pem_for(X, "not-base64url!!").is_err());
    }

    #[tokio::test]
    async fn verifies_real_es256_signature() {
        let jwks = jwks_with_key("test-kid-1");
        let claims: serde_json::Value = jwks.decode(TOKEN).await.unwrap();
        assert_eq!(claims["sub"], "supabase-user-uuid-1234");
        assert_eq!(claims["app_metadata"]["role"], "admin");
    }

    #[tokio::test]
    async fn rejects_unknown_kid() {
        let jwks = jwks_with_key("kid-lain");
        let err = jwks.decode::<serde_json::Value>(TOKEN).await.unwrap_err();
        assert_eq!(err, JwtFailure::UnknownKid);
    }

    #[tokio::test]
    async fn rejects_tampered_signature() {
        let jwks = jwks_with_key("test-kid-1");
        let parts: Vec<&str> = TOKEN.split('.').collect();
        let sig = parts[2].to_string();
        let flipped = if sig.starts_with('a') { "b" } else { "a" };
        let tampered_sig = format!("{}{}", flipped, &sig[1..]);
        let tampered = format!("{}.{}.{}", parts[0], parts[1], tampered_sig);
        let err = jwks
            .decode::<serde_json::Value>(&tampered)
            .await
            .unwrap_err();
        assert_eq!(err, JwtFailure::InvalidSignature);
    }

    #[tokio::test]
    async fn disabled_when_no_url() {
        let jwks = Jwks::new(None);
        assert!(!jwks.is_enabled());
        assert_eq!(
            jwks.decode::<serde_json::Value>(TOKEN).await.unwrap_err(),
            JwtFailure::Disabled
        );
    }
}
