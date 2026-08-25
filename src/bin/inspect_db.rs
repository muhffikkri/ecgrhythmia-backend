use ecg_backend::{config, db};
use std::process::exit;

fn main() {
    println!("=== Memuat konfigurasi dan membuka database ===");
    let app_config = config::AppConfig::load();
    let pool = db::sqlite::create_pool(&app_config.db_path, &app_config.sqlite_key);

    let conn = match pool.get() {
        Ok(c) => c,
        Err(e) => {
            eprintln!("Gagal mendapatkan koneksi ke database: {}", e);
            exit(1);
        }
    };

    println!("\n=== DAFTAR PASIEN (5 Terbaru) ===");
    {
        let mut stmt = conn.prepare("SELECT id, first_name, last_name FROM patients ORDER BY id DESC LIMIT 5").unwrap();
        let rows = stmt.query_map([], |row| {
            let id: String = row.get(0)?;
            let fname: String = row.get(1)?;
            let lname: String = row.get(2)?;
            Ok(format!("{} - {} {}", id, fname, lname))
        }).unwrap();
        for row in rows {
            println!("{}", row.unwrap());
        }
    }

    println!("\n=== DAFTAR SESI (5 Terbaru) ===");
    {
        let mut stmt = conn.prepare("SELECT id, patient_id, started_at, file_path FROM sessions ORDER BY started_at DESC LIMIT 5").unwrap();
        let rows = stmt.query_map([], |row| {
            let id: String = row.get(0)?;
            let patient_id: Option<String> = row.get(1)?;
            let started_at: String = row.get(2)?;
            let file_path: String = row.get(3)?;
            Ok(format!("{} | Pasien: {} | Mulai: {} | File: {}", id, patient_id.unwrap_or_else(|| "NONE".to_string()), started_at, file_path))
        }).unwrap();
        for row in rows {
            println!("{}", row.unwrap());
        }
    }
}
