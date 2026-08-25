/// cleanup_sessions: Perbaiki sesi di database yang tidak memiliki patient_id.
/// 
/// Tool ini akan:
/// 1. Mencari semua sessions dengan patient_id = NULL di database
/// 2. Membaca file JSONL yang terkait untuk mencari patient_id yang tersimpan di dalamnya
/// 3. Jika ditemukan, update session di database
/// 4. Jika tidak, tampilkan daftar sesi yang perlu diperbaiki secara manual

use ecg_backend::{config, db};
use rusqlite::params;
use std::fs;
use std::io::{BufRead, BufReader};
use std::process::exit;

fn main() {
    println!("=== cleanup_sessions: Membersihkan sesi tanpa pasien di database ===\n");
    let app_config = config::AppConfig::load();
    let pool = db::sqlite::create_pool(&app_config.db_path, &app_config.sqlite_key);

    let conn = match pool.get() {
        Ok(c) => c,
        Err(e) => {
            eprintln!("Gagal mendapatkan koneksi ke database: {}", e);
            exit(1);
        }
    };

    // Tampilkan semua session dengan patient_id NULL
    println!("--- Sesi tanpa pasien (patient_id = NULL) ---");
    {
        let mut stmt = conn.prepare(
            "SELECT id, device_id, started_at, file_path FROM sessions WHERE patient_id IS NULL ORDER BY started_at DESC"
        ).unwrap();

        let null_sessions: Vec<(String, String, String, String)> = stmt.query_map([], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
                row.get::<_, String>(3)?,
            ))
        }).unwrap().filter_map(|r| r.ok()).collect();

        if null_sessions.is_empty() {
            println!("[OK] Tidak ada sesi tanpa pasien di database!\n");
        } else {
            println!("Ditemukan {} sesi tanpa pasien:\n", null_sessions.len());
            for (session_id, device_id, started_at, file_path) in &null_sessions {
                println!("  {} | device={} | started={} | file={}", session_id, device_id, started_at, file_path);
            }
        }

        println!("\n--- Mencoba memulihkan patient_id dari file JSONL ---");
        let mut recovered = 0;
        let mut manual_fix_needed = vec![];

        for (session_id, _device_id, _started_at, file_path) in &null_sessions {
            let patient_from_file = read_patient_id_from_jsonl(file_path);

            match patient_from_file {
                Some(pid) => {
                    // Verifikasi bahwa pasien ada di DB
                    let patient_exists: bool = conn.query_row(
                        "SELECT EXISTS(SELECT 1 FROM patients WHERE id = ?1)",
                        params![pid],
                        |row| row.get(0),
                    ).unwrap_or(false);

                    if patient_exists {
                        match conn.execute(
                            "UPDATE sessions SET patient_id = ?1 WHERE id = ?2",
                            params![pid, session_id],
                        ) {
                            Ok(_) => {
                                println!("[FIXED]  {} -> patient_id diset ke '{}'", session_id, pid);
                                recovered += 1;
                            }
                            Err(e) => eprintln!("[ERROR]  Gagal update {}: {:?}", session_id, e),
                        }
                    } else {
                        println!("[WARN]   {} -> patient_id '{}' dari JSONL tidak ada di tabel patients", session_id, pid);
                        manual_fix_needed.push((session_id.clone(), Some(pid)));
                    }
                }
                None => {
                    println!("[SKIP]   {} -> tidak ada patient_id di file JSONL (file lama sebelum perbaikan)", session_id);
                    manual_fix_needed.push((session_id.clone(), None));
                }
            }
        }

        println!("\n=== Ringkasan ===");
        println!("Dipulihkan otomatis : {}", recovered);
        println!("Perlu perbaikan manual: {}", manual_fix_needed.len());

        if !manual_fix_needed.is_empty() {
            println!("\n--- Sesi yang perlu diperbaiki manual (gunakan perintah di bawah) ---");
            for (sid, _) in &manual_fix_needed {
                println!(
                    "  UPDATE sessions SET patient_id = '<ID_PASIEN>' WHERE id = '{}';",
                    sid
                );
            }
        }
    }

    // Tampilkan 10 sesi terbaru untuk verifikasi
    println!("\n--- 10 Sesi Terbaru Setelah Pembersihan ---");
    {
        let mut stmt = conn.prepare(
            "SELECT id, patient_id, started_at, file_path FROM sessions ORDER BY started_at DESC LIMIT 10"
        ).unwrap();
        let rows = stmt.query_map([], |row| {
            let id: String = row.get(0)?;
            let patient_id: Option<String> = row.get(1)?;
            let started_at: String = row.get(2)?;
            let file_path: String = row.get(3)?;
            Ok(format!(
                "{} | Pasien: {} | Mulai: {} | File: {}",
                id,
                patient_id.unwrap_or_else(|| "NULL".to_string()),
                started_at,
                file_path
            ))
        }).unwrap();
        for row in rows {
            println!("{}", row.unwrap());
        }
    }
}

fn read_patient_id_from_jsonl(file_path: &str) -> Option<String> {
    let file = fs::File::open(file_path).ok()?;
    let mut reader = BufReader::new(file);
    let mut first_line = String::new();
    reader.read_line(&mut first_line).ok()?;
    if first_line.is_empty() {
        return None;
    }
    let parsed: serde_json::Value = serde_json::from_str(&first_line).ok()?;
    parsed["patient_id"].as_str().map(|s| s.to_string())
}
