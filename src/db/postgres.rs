use crate::models::device::DevicePayload;
use sqlx::postgres::{PgConnectOptions, PgPoolOptions};
use sqlx::PgPool;
use std::collections::HashMap;
use std::fs::OpenOptions;
use std::io::Write;
use std::str::FromStr;
use std::time::{Duration, Instant};
use tokio::sync::mpsc::{unbounded_channel, UnboundedSender};
use tracing::{error, info};

/// Berapa lama menunggu koneksi pertama ke PostgreSQL sebelum menyerah.
const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);

/// Berapa kali mencoba koneksi sebelum menyerah dan boot dalam mode degraded.
const CONNECT_ATTEMPTS: u32 = 3;

/// Batas bawah dan atas jeda percobaan ulang koneksi.
const RETRY_MIN: Duration = Duration::from_secs(1);
const RETRY_MAX: Duration = Duration::from_secs(5);

/// Koneksi dianggap hidup setelah `SELECT 1` berhasil.
pub async fn create_pool(database_url: &str) -> PgPool {
    let connect_opts = match PgConnectOptions::from_str(database_url) {
        Ok(o) => o.statement_cache_capacity(0),
        Err(e) => {
            // URL rusak tidak akan membaik dengan sendirinya, tapi service tetap
            // harus start supaya endpoint lain (auth, health) tetap bisa probed.
            error!("URL PostgreSQL tidak valid: {}", e);
            return match PgPoolOptions::new().connect_lazy(database_url) {
                Ok(p) => p,
                Err(e2) => panic!("URL PostgreSQL tidak valid: {} / {}", e, e2),
            };
        }
    };

    let mut delay = RETRY_MIN;

    for attempt in 1..=CONNECT_ATTEMPTS {
        let opts = connect_opts.clone();

        match tokio::time::timeout(
            CONNECT_TIMEOUT,
            PgPoolOptions::new()
                .max_connections(5)
                .acquire_timeout(CONNECT_TIMEOUT)
                .connect_with(opts),
        )
        .await
        {
            Ok(Ok(pool)) => {
                if attempt > 1 {
                    info!(
                        "Berhasil terhubung ke PostgreSQL setelah {} percobaan.",
                        attempt
                    );
                }
                return pool;
            }
            Ok(Err(e)) => {
                error!(
                    "Gagal terhubung ke PostgreSQL (percobaan {}/{}): {}",
                    attempt, CONNECT_ATTEMPTS, e
                );
            }
            Err(_) => {
                error!(
                    "Koneksi PostgreSQL timeout > {:?} (percobaan {}/{}).",
                    CONNECT_TIMEOUT, attempt, CONNECT_ATTEMPTS
                );
            }
        }

        if attempt < CONNECT_ATTEMPTS {
            tokio::time::sleep(delay).await;
            delay = (delay * 2).min(RETRY_MAX);
        }
    }

    // Semua percobaan gagal. Kembalikan pool LAZY: tidak ada koneksi yang
    // dicoba sampai ada query, sehingga startup tetap lanjut dan endpoint
    // tetap bisa dihubungi. Query akan gagal saat dipanggil, dan middleware
    // `require_db` mengubahnya jadi 503 `database_unavailable`.
    error!(
        "PostgreSQL tidak dapat dihubungi setelah {} percobaan — boot dalam mode degraded.",
        CONNECT_ATTEMPTS
    );
    match PgPoolOptions::new()
        .max_connections(5)
        .acquire_timeout(CONNECT_TIMEOUT)
        .connect_lazy(database_url)
    {
        Ok(pool) => pool,
        Err(e) => panic!("URL PostgreSQL tidak valid: {}", e),
    }
}

/// `true` bila pool bisa menjalankan query sekarang juga.
pub async fn is_healthy(pool: &PgPool) -> bool {
    tokio::time::timeout(
        Duration::from_secs(5),
        sqlx::query_scalar::<_, i32>("SELECT 1").fetch_one(pool),
    )
    .await
    .map(|r| r.is_ok())
    .unwrap_or(false)
}

/// Status kesehatan database yang di-cache.
///
/// `SELECT 1` di setiap request akan menambah latensi dan membebani pool saat
/// database sedang lambat, jadi hasil probe disimpan sebentar. Status awal
/// `None` (belum pernah dicek) dianggap "boleh lewat" supaya request pertama
/// tidak tertolak hanya karena probe belum selesai.
#[derive(Clone)]
pub struct DbHealth {
    state: std::sync::Arc<std::sync::RwLock<Option<(bool, Instant)>>>,
    ttl: Duration,
}

impl DbHealth {
    pub fn new() -> Self {
        Self {
            state: std::sync::Arc::new(std::sync::RwLock::new(None)),
            ttl: Duration::from_secs(5),
        }
    }

    /// Kembalikan status cache bila masih segar, tanpa menyentuh jaringan.
    pub fn cached(&self) -> Option<bool> {
        self.state
            .read()
            .ok()
            .and_then(|g| *g)
            .filter(|(_, at)| at.elapsed() < self.ttl)
            .map(|(up, _)| up)
    }

    /// Catat hasil probe terbaru.
    pub fn record(&self, up: bool) {
        if let Ok(mut g) = self.state.write() {
            *g = Some((up, Instant::now()));
        }
    }

    /// Set status awal saat boot supaya probe pertama tidak perlu menunggu.
    pub fn prime(&self, up: bool) {
        self.record(up);
    }

    /// `true` bila request boleh diteruskan ke handler.
    pub fn allows(&self) -> bool {
        self.cached().unwrap_or(true)
    }
}

impl Default for DbHealth {
    fn default() -> Self {
        Self::new()
    }
}

pub async fn run_migrations(pool: &PgPool) -> Result<(), sqlx::Error> {
    // CATATAN (Supabase pooler): DDL tidak boleh dijalankan lewat extended
    // protocol yang dipersiapkan (prepared) di atas pooler — koneksi baru yang
    // menyiapkan statement dengan nama ulang "sqlx_s_*" akan bertabrakan dengan
    // statement yang masih tersisa di session backend reuse, memunculkan
    // `prepared statement "sqlx_s_*" already exists`.
    // Karenanya migrasi BERSIFAT BEST-EFFORT: bila gagal (mis. di atas pooler)
    // dicatat sebagai warning dan server tetap berjalan. Skema produksi yang
    // benar diprovisi lewat psql/sqlx-cli (simple protocol):
    //   psql "$DATABASE_URL" --set ON_ERROR_STOP=1 -f migrations/0001_initial.sql
    //   psql "$DATABASE_URL" --set ON_ERROR_STOP=1 -f migrations/0002_postgres_evolution.sql
    fn warn_skip(sql: &str, e: &sqlx::Error) {
        tracing::warn!(
            "Migrasi skema dilewati — provision lewat psql/sqlx-cli ({}): {}",
            sql,
            e
        );
    }

    let queries = [
        "CREATE TABLE IF NOT EXISTS accounts (id TEXT PRIMARY KEY, email TEXT UNIQUE NOT NULL, password_hash TEXT, role TEXT NOT NULL, created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP, profile_photo TEXT, status TEXT DEFAULT 'Offline', auth_id TEXT)",
        "CREATE TABLE IF NOT EXISTS doctors (id TEXT PRIMARY KEY, account_id TEXT REFERENCES accounts(id) ON DELETE CASCADE, first_name TEXT NOT NULL, last_name TEXT NOT NULL, created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP)",
        "CREATE TABLE IF NOT EXISTS patients (id TEXT PRIMARY KEY, account_id TEXT REFERENCES accounts(id) ON DELETE CASCADE, first_name TEXT NOT NULL, last_name TEXT NOT NULL, date_of_birth DATE, age INTEGER DEFAULT 0, gender TEXT, primary_doctor_id TEXT REFERENCES doctors(id) ON DELETE SET NULL, device_id TEXT, created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP)",
        "CREATE TABLE IF NOT EXISTS devices (id TEXT PRIMARY KEY, name TEXT NOT NULL UNIQUE, mqtt_broker TEXT, mqtt_port INTEGER, mqtt_topic TEXT, mqtt_username TEXT, mqtt_password TEXT, created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP)",
        "CREATE TABLE IF NOT EXISTS sessions (id TEXT PRIMARY KEY, device_id TEXT NOT NULL REFERENCES devices(id) ON DELETE CASCADE, patient_id TEXT REFERENCES patients(id) ON DELETE CASCADE, doctor_id TEXT REFERENCES doctors(id) ON DELETE SET NULL, started_at TIMESTAMP WITH TIME ZONE NOT NULL, ended_at TIMESTAMP WITH TIME ZONE, file_path TEXT, ecg_paper TEXT, dev_note TEXT, created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP)",
        "CREATE TABLE IF NOT EXISTS frame_records (id TEXT PRIMARY KEY, session_id TEXT NOT NULL REFERENCES sessions(id) ON DELETE CASCADE, start_time DOUBLE PRECISION DEFAULT 0, end_time DOUBLE PRECISION DEFAULT 0, time_interval TEXT DEFAULT '', label TEXT DEFAULT 'Normal', dev_note TEXT, doc_note TEXT, confirmation BOOLEAN DEFAULT NULL, doc_classification TEXT, hidden BOOLEAN DEFAULT FALSE, created_by TEXT REFERENCES accounts(id) ON DELETE SET NULL, created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP)"
    ];

    for q in queries.iter() {
        if let Err(e) = sqlx::query(*q).execute(pool).await {
            warn_skip(q, &e);
        }
    }

    // ALTER TABLE ADD COLUMN IF NOT EXISTS untuk evolusi skema pada database yang sudah ada
    // (CREATE TABLE IF NOT EXISTS tidak akan mengubah tabel eksisting)
    let alter_queries = [
        "ALTER TABLE accounts ADD COLUMN IF NOT EXISTS password_hash TEXT",
        "ALTER TABLE accounts ADD COLUMN IF NOT EXISTS auth_id TEXT",
        "ALTER TABLE accounts ALTER COLUMN created_at SET NOT NULL",
        "ALTER TABLE patients ADD COLUMN IF NOT EXISTS date_of_birth DATE",
        "ALTER TABLE patients ADD COLUMN IF NOT EXISTS age INTEGER DEFAULT 0",
        "ALTER TABLE sessions ADD COLUMN IF NOT EXISTS doctor_id TEXT REFERENCES doctors(id) ON DELETE SET NULL",
        "ALTER TABLE sessions ADD COLUMN IF NOT EXISTS dev_note TEXT",
        "ALTER TABLE sessions ADD COLUMN IF NOT EXISTS ecg_paper TEXT",
        "ALTER TABLE frame_records ADD COLUMN IF NOT EXISTS end_time DOUBLE PRECISION DEFAULT 0",
        "ALTER TABLE frame_records ADD COLUMN IF NOT EXISTS time_interval TEXT DEFAULT ''",
        "ALTER TABLE frame_records ADD COLUMN IF NOT EXISTS label TEXT DEFAULT 'Normal'",
        "ALTER TABLE frame_records ADD COLUMN IF NOT EXISTS dev_note TEXT",
        "ALTER TABLE frame_records ADD COLUMN IF NOT EXISTS doc_classification TEXT",
    ];

    for q in alter_queries.iter() {
        if let Err(e) = sqlx::query(*q).execute(pool).await {
            warn_skip(q, &e);
        }
    }

    Ok(())
}

pub async fn generate_custom_id(pool: &PgPool, table: &str, prefix: &str) -> String {
    let expected_len = prefix.len() + 12;
    // We cannot use bound parameters for table names in Postgres, so we format securely.
    let query_str = format!(
        "SELECT id FROM {} WHERE id LIKE '{}%' AND LENGTH(id) = {} ORDER BY id DESC LIMIT 1",
        table, prefix, expected_len
    );

    // Instead of sqlx::query! macro, we use sqlx::query to allow dynamic SQL strings for this specific helper.
    let res: Result<(String,), _> = sqlx::query_as(&query_str).fetch_one(pool).await;

    let last_id = match res {
        Ok((id,)) => id,
        Err(_) => format!("{}000000000000", prefix),
    };

    if last_id.starts_with(prefix) && last_id.len() == expected_len {
        if let Ok(num) = last_id[prefix.len()..].parse::<i64>() {
            return format!("{}{:012}", prefix, num + 1);
        }
    }
    format!("{}000000000001", prefix)
}

pub fn start_db_worker(
    pool: PgPool,
    pacer_tx: UnboundedSender<DevicePayload>,
) -> UnboundedSender<DevicePayload> {
    let (tx, mut rx) = unbounded_channel::<DevicePayload>();

    tokio::spawn(async move {
        info!("[Database] Background writer task berjalan...");
        let mut device_map: HashMap<String, String> = HashMap::new();
        // session_map dan ended_sessions DIHAPUS. Kita gunakan database sebagai single source of truth.
        let mut session_frame_counts: HashMap<String, i64> = HashMap::new();

        while let Some(mut payload) = rx.recv().await {
            // 1. Dapatkan atau buat device ID internal
            let dev_id = if let Some(id) = device_map.get(&payload.device_id) {
                id.clone()
            } else {
                // Try to find the device
                let dev_res =
                    sqlx::query!("SELECT id FROM devices WHERE name = $1", payload.device_id)
                        .fetch_one(&pool)
                        .await;

                match dev_res {
                    Ok(record) => {
                        device_map.insert(payload.device_id.clone(), record.id.clone());
                        record.id
                    }
                    Err(_) => {
                        let new_id = generate_custom_id(&pool, "devices", "dev").await;
                        if let Err(e) = sqlx::query!(
                            "INSERT INTO devices (id, name) VALUES ($1, $2)",
                            new_id,
                            payload.device_id
                        )
                        .execute(&pool)
                        .await
                        {
                            error!("[Database] Gagal INSERT device: {}", e);
                            continue;
                        }
                        device_map.insert(payload.device_id.clone(), new_id.clone());
                        new_id
                    }
                }
            };

            // 2. Dapatkan sesi AKTIF untuk perangkat ini dari pangkalan data
            // Ini menjamin sinkronisasi dengan aksi UI (START / STOP) dan menuntaskan masalah Restart Bug.
            let ses_res = sqlx::query!("SELECT id FROM sessions WHERE device_id = $1 AND ended_at IS NULL ORDER BY started_at DESC LIMIT 1", dev_id)
                .fetch_optional(&pool)
                .await;

            let ses_id = match ses_res {
                Ok(Some(record)) => record.id,
                _ => {
                    // Jika tidak ada sesi aktif, buang payload!
                    continue;
                }
            };

            // Hitung ulang frame_id mulai dari 1 untuk sesi ini
            let count = session_frame_counts.entry(ses_id.clone()).or_insert(1);
            payload.frame_id = format!("{:06}", count);
            // Timpa juga message_id agar sinkron dengan yang dialirkan WebSocket (Pacer)
            payload.message_id = format!("{}-{}-frame_{:06}", payload.device_id, ses_id, count);
            *count += 1;

            let label = &payload.prediction.label;
            let status = &payload.prediction.status;
            info!(
                device_id = %payload.device_id,
                frame_id = %payload.frame_id,
                prediction_label = %label,
                status = %status,
                "Menerima paket sensor EKG"
            );

            payload.session_id = ses_id.clone();
            let file_path = format!("records/{}.jsonl", ses_id);

            let json_string = match serde_json::to_string(&payload) {
                Ok(val) => val,
                Err(e) => {
                    error!("[Database] Gagal serialisasi payload ke JSON: {}", e);
                    continue;
                }
            };

            if let Some(parent) = std::path::Path::new(&file_path).parent() {
                if !parent.exists() {
                    let _ = std::fs::create_dir_all(parent);
                }
            }

            let mut file = match OpenOptions::new()
                .create(true)
                .append(true)
                .open(&file_path)
            {
                Ok(f) => f,
                Err(e) => {
                    error!("[Database] Gagal membuka file rekaman {}: {}", file_path, e);
                    continue;
                }
            };

            if let Err(e) = writeln!(file, "{}", json_string) {
                error!(
                    "[Database] Gagal menulis baris ke file {}: {}",
                    file_path, e
                );
                continue;
            }

            // Update database classification label dynamically
            let duration_s = payload.duration_s;
            let frame_num = payload.frame_id.parse::<i64>().unwrap_or(1);
            let start_sec = (frame_num - 1) as f64 * duration_s;
            let end_sec = frame_num as f64 * duration_s;

            let format_time = |secs: f64| -> String {
                let m = (secs / 60.0).floor() as i64;
                let s = (secs % 60.0).floor() as i64;
                format!("{:02}:{:02}", m, s)
            };

            let time_interval = format!("{} - {}", format_time(start_sec), format_time(end_sec));

            let mut best_label = "Normal".to_string();
            if let Some(probs) = &payload.prediction.probabilities {
                if let Some(obj) = probs.as_object() {
                    let mut max_prob = -1.0;
                    for (k, v) in obj {
                        if let Some(p) = v.as_f64() {
                            if p > max_prob {
                                max_prob = p;
                                best_label = k.clone();
                            }
                        }
                    }
                }
            } else {
                best_label = payload.prediction.label.clone();
            }

            let frame_db_id = format!("fra{}{:06}", ses_id.replace("ses", ""), frame_num);
            let _ = sqlx::query!(
                "INSERT INTO frame_records (id, session_id, time_interval, start_time, end_time, label, hidden, confirmation) VALUES ($1, $2, $3, $4, $5, $6, FALSE, NULL) ON CONFLICT (id) DO UPDATE SET label = EXCLUDED.label",
                frame_db_id, ses_id, time_interval, start_sec, end_sec, best_label
            ).execute(&pool).await;

            let _ = pacer_tx.send(payload);
        }
    });

    tx
}
