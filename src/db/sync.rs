use crate::db::sqlite::DbPool;
use tracing::info;
use std::env;
use native_tls::TlsConnector;
use postgres_native_tls::MakeTlsConnector;
use rusqlite::params;

pub fn sync_databases(pool: &DbPool) -> Result<usize, String> {
    let database_url = match env::var("DATABASE_URL") {
        Ok(url) if !url.trim().is_empty() => url,
        _ => {
            info!("[Sync] DATABASE_URL tidak diatur. Sinkronisasi dibatalkan.");
            return Err("DATABASE_URL tidak diatur di file .env".to_string());
        }
    };

    // Buat TLS Connector karena Supabase mewajibkan SSL
    let connector = TlsConnector::builder()
        .danger_accept_invalid_certs(true) // Untuk menghindari error self-signed di server lokal/vps
        .build()
        .map_err(|e| format!("Gagal membuat TLS Connector: {}", e))?;
    let connector = MakeTlsConnector::new(connector);

    info!("[Sync] Menghubungkan ke database PostgreSQL Supabase...");
    let mut pg_client = postgres::Client::connect(&database_url, connector)
        .map_err(|e| format!("Gagal terhubung ke remote PostgreSQL: {}", e))?;

    info!("[Sync] Memulai sinkronisasi tabel...");
    
    // SQLite Pool Connection
    let conn = pool.get().map_err(|e| e.to_string())?;
    
    let mut synced_count = 0;

    // 1. Sync accounts
    let mut stmt = conn.prepare("SELECT id, email, password_hash, role, created_at, profile_photo, status FROM accounts").unwrap();
    let accounts_iter = stmt.query_map([], |row| {
        Ok((
            row.get::<_, String>(0)?,
            row.get::<_, String>(1)?,
            row.get::<_, String>(2)?,
            row.get::<_, String>(3)?,
            row.get::<_, String>(4)?,
            row.get::<_, Option<String>>(5)?,
            row.get::<_, Option<String>>(6)?,
        ))
    }).unwrap();

    for acc in accounts_iter {
        if let Ok((id, email, hash, role, created, photo, status)) = acc {
            let res = pg_client.execute(
                "INSERT INTO accounts (id, email, password_hash, role, created_at, profile_photo, status) \
                 VALUES ($1, $2, $3, $4, $5, $6, $7) \
                 ON CONFLICT (id) DO UPDATE SET \
                 email = EXCLUDED.email, password_hash = EXCLUDED.password_hash, role = EXCLUDED.role, \
                 profile_photo = EXCLUDED.profile_photo, status = EXCLUDED.status",
                &[&id, &email, &hash, &role, &created, &photo, &status]
            );
            if res.is_ok() { synced_count += 1; }
        }
    }

    // 2. Sync doctors
    let mut stmt = conn.prepare("SELECT id, account_id, first_name, last_name FROM doctors").unwrap();
    let docs_iter = stmt.query_map([], |row| {
        Ok((
            row.get::<_, String>(0)?,
            row.get::<_, Option<String>>(1)?,
            row.get::<_, String>(2)?,
            row.get::<_, String>(3)?,
        ))
    }).unwrap();

    for doc in docs_iter {
        if let Ok((id, acc_id, first, last)) = doc {
            let res = pg_client.execute(
                "INSERT INTO doctors (id, account_id, first_name, last_name) \
                 VALUES ($1, $2, $3, $4) \
                 ON CONFLICT (id) DO UPDATE SET \
                 account_id = EXCLUDED.account_id, first_name = EXCLUDED.first_name, last_name = EXCLUDED.last_name",
                &[&id, &acc_id, &first, &last]
            );
            if res.is_ok() { synced_count += 1; }
        }
    }

    // 3. Sync patients
    let mut stmt = conn.prepare("SELECT id, account_id, primary_doctor_id, first_name, last_name, date_of_birth, gender, device_id FROM patients").unwrap();
    let patients_iter = stmt.query_map([], |row| {
        Ok((
            row.get::<_, String>(0)?,
            row.get::<_, Option<String>>(1)?,
            row.get::<_, Option<String>>(2)?,
            row.get::<_, String>(3)?,
            row.get::<_, String>(4)?,
            row.get::<_, String>(5)?,
            row.get::<_, String>(6)?,
            row.get::<_, Option<String>>(7)?,
        ))
    }).unwrap();

    for pat in patients_iter {
        if let Ok((id, acc_id, doc_id, first, last, dob, gender, dev_id)) = pat {
            let res = pg_client.execute(
                "INSERT INTO patients (id, account_id, primary_doctor_id, first_name, last_name, date_of_birth, gender, device_id) \
                 VALUES ($1, $2, $3, $4, $5, $6, $7, $8) \
                 ON CONFLICT (id) DO UPDATE SET \
                 account_id = EXCLUDED.account_id, primary_doctor_id = EXCLUDED.primary_doctor_id, \
                 first_name = EXCLUDED.first_name, last_name = EXCLUDED.last_name, date_of_birth = EXCLUDED.date_of_birth, \
                 gender = EXCLUDED.gender, device_id = EXCLUDED.device_id",
                &[&id, &acc_id, &doc_id, &first, &last, &dob, &gender, &dev_id]
            );
            if res.is_ok() { synced_count += 1; }
        }
    }

    // 4. Sync devices
    let mut stmt = conn.prepare("SELECT id, name, mqtt_broker, mqtt_port, mqtt_topic, mqtt_username, mqtt_password FROM devices").unwrap();
    let devs_iter = stmt.query_map([], |row| {
        Ok((
            row.get::<_, String>(0)?,
            row.get::<_, String>(1)?,
            row.get::<_, Option<String>>(2)?,
            row.get::<_, Option<i64>>(3)?,
            row.get::<_, Option<String>>(4)?,
            row.get::<_, Option<String>>(5)?,
            row.get::<_, Option<String>>(6)?,
        ))
    }).unwrap();

    for dev in devs_iter {
        if let Ok((id, name, broker, port, topic, user, pass)) = dev {
            let port_i32 = port.map(|p| p as i32);
            let res = pg_client.execute(
                "INSERT INTO devices (id, name, mqtt_broker, mqtt_port, mqtt_topic, mqtt_username, mqtt_password) \
                 VALUES ($1, $2, $3, $4, $5, $6, $7) \
                 ON CONFLICT (id) DO UPDATE SET \
                 name = EXCLUDED.name, mqtt_broker = EXCLUDED.mqtt_broker, mqtt_port = EXCLUDED.mqtt_port, \
                 mqtt_topic = EXCLUDED.mqtt_topic, mqtt_username = EXCLUDED.mqtt_username, mqtt_password = EXCLUDED.mqtt_password",
                &[&id, &name, &broker, &port_i32, &topic, &user, &pass]
            );
            if res.is_ok() { synced_count += 1; }
        }
    }

    // 5. Sync sessions
    let mut stmt = conn.prepare("SELECT id, device_id, patient_id, started_at, ended_at, file_path FROM sessions").unwrap();
    let sessions_iter = stmt.query_map([], |row| {
        Ok((
            row.get::<_, String>(0)?,
            row.get::<_, String>(1)?,
            row.get::<_, Option<String>>(2)?,
            row.get::<_, String>(3)?,
            row.get::<_, Option<String>>(4)?,
            row.get::<_, String>(5)?,
        ))
    }).unwrap();

    for ses in sessions_iter {
        if let Ok((id, dev_id, pat_id, start, end, path)) = ses {
            let res = pg_client.execute(
                "INSERT INTO sessions (id, device_id, patient_id, started_at, ended_at, file_path) \
                 VALUES ($1, $2, $3, $4, $5, $6) \
                 ON CONFLICT (id) DO UPDATE SET \
                 device_id = EXCLUDED.device_id, patient_id = EXCLUDED.patient_id, \
                 started_at = EXCLUDED.started_at, ended_at = EXCLUDED.ended_at, file_path = EXCLUDED.file_path",
                &[&id, &dev_id, &pat_id, &start, &end, &path]
            );
            if res.is_ok() { synced_count += 1; }
        }
    }

    // 6. Sync frame_records
    let mut stmt = conn.prepare("SELECT id, session_id, time_interval, confirmation, doc_classification FROM frame_records").unwrap();
    let frames_iter = stmt.query_map([], |row| {
        Ok((
            row.get::<_, String>(0)?,
            row.get::<_, Option<String>>(1)?,
            row.get::<_, String>(2)?,
            row.get::<_, Option<i64>>(3)?,
            row.get::<_, Option<String>>(4)?,
        ))
    }).unwrap();

    for fr in frames_iter {
        if let Ok((id, ses_id, interval, conf, classification)) = fr {
            let conf_i32 = conf.map(|c| c as i32);
            let res = pg_client.execute(
                "INSERT INTO frame_records (id, session_id, time_interval, confirmation, doc_classification) \
                 VALUES ($1, $2, $3, $4, $5) \
                 ON CONFLICT (id) DO UPDATE SET \
                 session_id = EXCLUDED.session_id, time_interval = EXCLUDED.time_interval, \
                 confirmation = EXCLUDED.confirmation, doc_classification = EXCLUDED.doc_classification",
                &[&id, &ses_id, &interval, &conf_i32, &classification]
            );
            if res.is_ok() { synced_count += 1; }
        }
    }

    // PULL data from PostgreSQL to SQLite (Sinkronisasi Dua Arah)
    info!("[Sync] Menarik data baru dari PostgreSQL ke SQLite...");

    // Pull accounts
    if let Ok(rows) = pg_client.query("SELECT id, email, password_hash, role, created_at, profile_photo, status FROM accounts", &[]) {
        for row in rows {
            let id: String = row.get(0);
            let email: String = row.get(1);
            let password_hash: String = row.get(2);
            let role: String = row.get(3);
            let created_at: String = row.get(4);
            let profile_photo: Option<String> = row.get(5);
            let status: Option<String> = row.get(6);

            let _ = conn.execute(
                "INSERT INTO accounts (id, email, password_hash, role, created_at, profile_photo, status) \
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7) \
                 ON CONFLICT(id) DO UPDATE SET \
                 email = excluded.email, password_hash = excluded.password_hash, role = excluded.role, \
                 profile_photo = excluded.profile_photo, status = excluded.status",
                params![id, email, password_hash, role, created_at, profile_photo, status]
            );
        }
    }

    // Pull patients
    if let Ok(rows) = pg_client.query("SELECT id, account_id, primary_doctor_id, first_name, last_name, date_of_birth, gender, device_id FROM patients", &[]) {
        for row in rows {
            let id: String = row.get(0);
            let account_id: Option<String> = row.get(1);
            let primary_doctor_id: Option<String> = row.get(2);
            let first_name: String = row.get(3);
            let last_name: String = row.get(4);
            let date_of_birth: String = row.get(5);
            let gender: String = row.get(6);
            let device_id: Option<String> = row.get(7);

            let _ = conn.execute(
                "INSERT INTO patients (id, account_id, primary_doctor_id, first_name, last_name, date_of_birth, gender, device_id) \
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8) \
                 ON CONFLICT(id) DO UPDATE SET \
                 account_id = excluded.account_id, primary_doctor_id = excluded.primary_doctor_id, \
                 first_name = excluded.first_name, last_name = excluded.last_name, date_of_birth = excluded.date_of_birth, \
                 gender = excluded.gender, device_id = excluded.device_id",
                params![id, account_id, primary_doctor_id, first_name, last_name, date_of_birth, gender, device_id]
            );
        }
    }

    // Pull sessions
    if let Ok(rows) = pg_client.query("SELECT id, device_id, patient_id, started_at, ended_at, file_path FROM sessions", &[]) {
        for row in rows {
            let id: String = row.get(0);
            let device_id: String = row.get(1);
            let patient_id: Option<String> = row.get(2);
            let started_at: String = row.get(3);
            let ended_at: Option<String> = row.get(4);
            let file_path: String = row.get(5);

            let _ = conn.execute(
                "INSERT INTO sessions (id, device_id, patient_id, started_at, ended_at, file_path) \
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6) \
                 ON CONFLICT(id) DO UPDATE SET \
                 device_id = excluded.device_id, patient_id = excluded.patient_id, \
                 started_at = excluded.started_at, ended_at = excluded.ended_at, file_path = excluded.file_path",
                params![id, device_id, patient_id, started_at, ended_at, file_path]
            );
        }
    }

    // Pull frame_records
    if let Ok(rows) = pg_client.query("SELECT id, session_id, time_interval, confirmation, doc_classification FROM frame_records", &[]) {
        for row in rows {
            let id: String = row.get(0);
            let session_id: Option<String> = row.get(1);
            let time_interval: String = row.get(2);
            let confirmation: Option<i32> = row.get(3);
            let doc_classification: Option<String> = row.get(4);

            let _ = conn.execute(
                "INSERT INTO frame_records (id, session_id, time_interval, confirmation, doc_classification) \
                 VALUES (?1, ?2, ?3, ?4, ?5) \
                 ON CONFLICT(id) DO UPDATE SET \
                 session_id = excluded.session_id, time_interval = excluded.time_interval, \
                 confirmation = excluded.confirmation, doc_classification = excluded.doc_classification",
                params![id, session_id, time_interval, confirmation, doc_classification]
            );
        }
    }

    info!("[Sync] Sinkronisasi dua arah selesai. Total record diproses: {}", synced_count);
    Ok(synced_count)
}
