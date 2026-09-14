use crate::models::payload::RawECGData;
/**
 * @fileoverview Modul Data: Ekstraksi CSV (Rust)
 * Bertugas membaca file dataset EKG statis (.csv) dan mengubahnya
 * menjadi struktur data RawECGData.
 *
 * PENTING: File CSV yang dibaca diasumsikan sudah dikonversi secara offline
 * dan memiliki minimal 4 kolom dengan urutan: Waktu, Ch1, Ch2, Ch3.
 * Data di dalam CSV diasumsikan SUDAH DALAM BENTUK MILIVOLT (mV).
 */
use std::error::Error;
use std::fs::File;

#[allow(dead_code)]
pub fn read_ecg_data(file_path: &str) -> Result<RawECGData, Box<dyn Error>> {
    let file = File::open(file_path)?;

    // Inisialisasi pembaca CSV. Asumsi file memiliki baris header (has_headers = true).
    let mut rdr = csv::ReaderBuilder::new()
        .has_headers(true)
        .from_reader(file);

    // Siapkan vektor memori untuk menampung data
    let mut time = Vec::new();
    let mut ch1 = Vec::new();
    let mut ch2 = Vec::new();
    let mut ch3 = Vec::new();

    // Iterasi baris demi baris secara efisien

    for result in rdr.records() {
        let record = result?;

        // Memastikan baris ini setidaknya memiliki 4 kolom data
        if record.len() >= 4 {
            // Parsing string menjadi float (f64).
            // Jika ada nilai kosong/NaN, default ke 0.0 agar sistem tidak crash.
            time.push(record[0].parse::<f64>().unwrap_or(0.0));
            ch1.push(record[1].parse::<f64>().unwrap_or(0.0));
            ch2.push(record[2].parse::<f64>().unwrap_or(0.0));
            ch3.push(record[3].parse::<f64>().unwrap_or(0.0));
        }
    }

    Ok(RawECGData {
        time,
        ch1,
        ch2,
        ch3,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    #[test]
    fn test_read_ecg_data_valid() {
        let temp_dir = std::env::temp_dir();
        let file_path = temp_dir.join("test_ecg.csv");
        let file_path_str = file_path.to_str().unwrap();

        let mut file = File::create(file_path_str).unwrap();
        writeln!(file, "time,ch1,ch2,ch3").unwrap();
        writeln!(file, "0.0,1.2,2.3,3.4").unwrap();
        writeln!(file, "0.04,1.5,,3.8").unwrap(); // missing value
        writeln!(file, "0.08,invalid,2.1,3.9").unwrap(); // invalid value
        drop(file);

        let result = read_ecg_data(file_path_str);
        assert!(result.is_ok());

        let data = result.unwrap();
        assert_eq!(data.time.len(), 3);
        assert_eq!(data.time[0], 0.0);
        assert_eq!(data.ch1[0], 1.2);
        assert_eq!(data.ch2[0], 2.3);
        assert_eq!(data.ch3[0], 3.4);

        // check parsed fallbacks
        assert_eq!(data.ch2[1], 0.0);
        assert_eq!(data.ch1[2], 0.0);

        let _ = std::fs::remove_file(file_path_str);
    }
}
