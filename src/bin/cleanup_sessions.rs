/// cleanup_sessions: Laporan dan perbaikan sesi tanpa patient_id di database.
/// 
/// Filosofi:
/// - Database adalah sumber kebenaran untuk hubungan pasien-sesi
/// - JSONL adalah arsip data sinyal (tidak wajib menyimpan patient_id)
/// - patient_id harus berasal dari form saat upload, bukan dari file JSONL
///
/// Tool ini akan:
/// 1. Tampilkan semua sesi tanpa patient_id di database (perlu perbaikan manual)
/// 2. Coba pulihkan dari JSONL HANYA untuk file baru yang memang menyimpannya
/// 3. Untuk file lama: tampilkan perintah SQL yang bisa dijalankan admin

use ecg_backend::{config, db};
use rusqlite::params;
use std::fs;
use std::io::{BufRead, BufReader};
use std::process::exit;

fn main() {
    println!("=================================================================");
    println!("  cleanup_sessions: Audit & Perbaikan Sesi Tanpa Pasien          ");
    println!("  Filosofi: database adalah sumber kebenaran, bukan file JSONL   ");
    println!("=================================================================\n");

    let app_config = config::AppConfig::load();
    let pool = db::sqlite::create_pool(&app_config.db_path, &app_config.sqlite_key);

    let conn = match pool.get() {
        Ok(c) => c,
        Err(e) => {
            eprintln!("Gagal mendapatkan koneksi ke database: {}", e);
            exit(1);
        }
    };

    // === 1. Temukan semua sesi tanpa patient_id ===
    let null_sessions: Vec<(String, String, String, String)> = {
        let mut stmt = conn.prepare(
            "SELECT id, device_id, started_at, file_path FROM sessions WHERE patient_id IS NULL ORDER BY started_at DESC"
        ).unwrap();
        stmt.query_map([], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
                row.get::<_, String>(3)?,
            ))
        }).unwrap().filter_map(|r| r.ok()).collect()
    };

    if null_sessions.is_empty() {
        println!("[OK] Tidak ada sesi tanpa pasien di database. Semua data bersih!\n");
    } else {
        println!("[!] Ditemukan {} sesi tanpa patient_id:\n", null_sessions.len());
        for (session_id, device_id, started_at, file_path) in &null_sessions {
            println!("  {} | device={} | started={} | file={}", session_id, device_id, started_at, file_path);
        }
    }

    // === 2. Coba pulihkan dari JSONL (hanya untuk file baru yang menyimpan patient_id) ===
    let mut recovered = 0;
    let mut manual_needed: Vec<(String, String)> = vec![];

    println!("\n--- Mencoba pemulihan otomatis dari JSONL (hanya file baru) ---");
    for (session_id, _device_id, started_at, file_path) in &null_sessions {
        let patient_from_jsonl = read_patient_id_from_jsonl(file_path);

        match patient_from_jsonl {
            Some(pid) => {
                // Verifikasi pasien ada di DB
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
                            println!("[PULIH]  {} -> patient_id = '{}'", session_id, pid);
                            recovered += 1;
                        }
                        Err(e) => {
                            eprintln!("[ERROR]  Gagal update {}: {:?}", session_id, e);
                            manual_needed.push((session_id.clone(), started_at.clone()));
                        }
                    }
                } else {
                    println!("[WARN]   {} -> patient '{}' ada di JSONL tapi tidak ada di tabel patients", session_id, pid);
                    manual_needed.push((session_id.clone(), started_at.clone()));
                }
            }
            None => {
                // File lama - tidak ada patient_id di JSONL, perlu perbaikan manual
                manual_needed.push((session_id.clone(), started_at.clone()));
            }
        }
    }

    // === 3. Tampilkan laporan akhir ===
    println!("\n=== RINGKASAN ===");
    println!("Total sesi tanpa pasien : {}", null_sessions.len());
    println!("Dipulihkan otomatis     : {}", recovered);
    println!("Perlu perbaikan manual  : {}", manual_needed.len());

    if !manual_needed.is_empty() {
        println!("\n--- Perintah SQL untuk perbaikan manual ---");
        println!("Jalankan di database untuk mengaitkan sesi ke pasien yang tepat:");
        println!("(Ganti <PATIENT_ID> dengan ID pasien yang sesuai, contoh: pat000000000030)\n");
        for (sid, started_at) in &manual_needed {
            println!("  -- Sesi: {} (waktu: {})", sid, started_at);
            println!("  UPDATE sessions SET patient_id = '<PATIENT_ID>' WHERE id = '{}';", sid);
            println!();
        }
        println!("Lihat daftar pasien yang tersedia:");
        println!("  SELECT id, first_name, last_name FROM patients ORDER BY id;");
    }

    // === 4. Tampilkan status akhir DB ===
    println!("\n--- 10 Sesi Terbaru di Database (setelah pembersihan) ---");
    {
        let mut stmt = conn.prepare(
            "SELECT s.id, p.first_name, p.last_name, s.started_at FROM sessions s LEFT JOIN patients p ON s.patient_id = p.id ORDER BY s.started_at DESC LIMIT 10"
        ).unwrap();
        let rows = stmt.query_map([], |row| {
            let id: String = row.get(0)?;
            let fname: Option<String> = row.get(1)?;
            let lname: Option<String> = row.get(2)?;
            let started_at: String = row.get(3)?;
            let patient_name = match (fname, lname) {
                (Some(f), Some(l)) => format!("{} {}", f, l),
                _ => "!!! TANPA PASIEN !!!".to_string(),
            };
            Ok(format!("{} | {} | {}", id, patient_name, started_at))
        }).unwrap();
        for row in rows {
            println!("  {}", row.unwrap());
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
