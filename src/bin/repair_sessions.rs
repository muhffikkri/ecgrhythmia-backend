use ecg_backend::{config, db};
use rusqlite::params;
use std::fs;
use std::io::{BufRead, BufReader};
use std::process::exit;

fn main() {
    println!("=== repair_sessions: Memperbaiki sesi yang belum tercatat di database ===");
    let app_config = config::AppConfig::load();
    let pool = db::sqlite::create_pool(&app_config.db_path, &app_config.sqlite_key);

    let conn = match pool.get() {
        Ok(c) => c,
        Err(e) => {
            eprintln!("Gagal mendapatkan koneksi ke database: {}", e);
            exit(1);
        }
    };

    let records_dir = "records";
    let entries = match fs::read_dir(records_dir) {
        Ok(e) => e,
        Err(e) => {
            eprintln!("Gagal membaca folder records/: {}", e);
            exit(1);
        }
    };

    let mut checked = 0;
    let mut repaired = 0;
    let mut skipped = 0;

    for entry in entries {
        let entry = match entry {
            Ok(e) => e,
            Err(_) => continue,
        };
        let path = entry.path();
        if path.extension().and_then(|e| e.to_str()) != Some("jsonl") {
            continue;
        }

        let file_name = path.file_name().unwrap().to_string_lossy().to_string();
        let session_id = file_name.trim_end_matches(".jsonl").to_string();
        let file_path_str = format!("records/{}", file_name);

        checked += 1;

        // Cek apakah session sudah ada di database
        let exists: bool = conn
            .query_row(
                "SELECT EXISTS(SELECT 1 FROM sessions WHERE id = ?1)",
                params![session_id],
                |row| row.get(0),
            )
            .unwrap_or(false);

        if exists {
            println!("[OK]      {} sudah ada di database", session_id);
            skipped += 1;
            continue;
        }

        // Baca baris pertama dari file JSONL untuk mendapatkan metadata
        let file = match fs::File::open(&path) {
            Ok(f) => f,
            Err(e) => {
                eprintln!("[ERROR]   Gagal membuka {}: {}", file_name, e);
                continue;
            }
        };
        let mut reader = BufReader::new(file);
        let mut first_line = String::new();
        if reader.read_line(&mut first_line).is_err() || first_line.is_empty() {
            eprintln!("[SKIP]    {} kosong atau tidak dapat dibaca", file_name);
            continue;
        }

        let parsed: serde_json::Value = match serde_json::from_str(&first_line) {
            Ok(v) => v,
            Err(e) => {
                eprintln!("[ERROR]   Gagal parse JSON dari {}: {}", file_name, e);
                continue;
            }
        };

        let device_id = parsed["device_id"].as_str().unwrap_or("device01").to_string();
        let patient_id = parsed["patient_id"].as_str().map(|s| s.to_string());
        let created_at = parsed["created_at"].as_str().unwrap_or("1970-01-01T00:00:00Z").to_string();

        println!(
            "[REPAIR]  {} | device={} | patient={:?} | started={}",
            session_id, device_id, patient_id, created_at
        );

        // Pastikan device ada
        let _ = conn.execute(
            "INSERT OR IGNORE INTO devices (id, name) VALUES (?1, ?1)",
            params![device_id],
        );

        // Pastikan patient ada (jika session punya patient_id)
        if let Some(ref pid) = patient_id {
            let patient_exists: bool = conn
                .query_row(
                    "SELECT EXISTS(SELECT 1 FROM patients WHERE id = ?1)",
                    params![pid],
                    |row| row.get(0),
                )
                .unwrap_or(false);

            if !patient_exists {
                eprintln!(
                    "[WARN]    patient_id '{}' tidak ada di DB. Memasukkan data dummy.",
                    pid
                );
                let _ = conn.execute(
                    "INSERT OR IGNORE INTO patients (id, first_name, last_name, date_of_birth, gender) VALUES (?1, 'Unknown', 'Patient', '1900-01-01', 'U')",
                    params![pid],
                );
            }
        }

        // Insert session
        match conn.execute(
            "INSERT OR IGNORE INTO sessions (id, device_id, patient_id, started_at, file_path) VALUES (?1, ?2, ?3, ?4, ?5)",
            params![session_id, device_id, patient_id, created_at, file_path_str],
        ) {
            Ok(rows) if rows > 0 => {
                println!("[SUCCESS] Session {} berhasil ditambahkan ke database!", session_id);
                repaired += 1;
            }
            Ok(_) => {
                println!("[IGNORE]  Session {} sudah ada (INSERT OR IGNORE).", session_id);
                skipped += 1;
            }
            Err(e) => {
                eprintln!("[ERROR]   Gagal insert session {}: {:?}", session_id, e);
            }
        }
    }

    println!("\n=== Selesai ===");
    println!("Diperiksa : {}", checked);
    println!("Diperbaiki: {}", repaired);
    println!("Dilewati  : {}", skipped);
}
