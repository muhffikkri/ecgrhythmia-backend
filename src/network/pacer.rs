use crate::models::device::DevicePayload;
use crate::models::payload::{ECGDataPayload, RawECGData, ServerMessage};
use crate::network::websocket::ClientList;
use std::time::Duration;
use tokio::sync::mpsc::{unbounded_channel, UnboundedSender};
use tracing::{error, info};

pub fn start_pacer(clients: ClientList) -> UnboundedSender<DevicePayload> {
    let (tx, mut rx) = unbounded_channel::<DevicePayload>();

    tokio::spawn(async move {
        info!("[Pacer] Thread pengatur laju (pacer) berjalan secara asinkron...");

        while let Some(device_data) = rx.recv().await {
            let total_samples = device_data.ecg.samples.len();
            if total_samples == 0 {
                continue;
            }

            let fs = device_data.sampling_rate_hz;
            if fs <= 0.0 {
                error!(
                    "[Pacer] Sampling rate tidak valid ({} Hz). Mengabaikan data.",
                    fs
                );
                continue;
            }

            // Ubah matriks (array of array) menjadi vektor kolom
            let mut time_vec = Vec::with_capacity(total_samples);
            let mut ch1_vec = Vec::with_capacity(total_samples);
            let mut ch2_vec = Vec::with_capacity(total_samples);
            let mut ch3_vec = Vec::with_capacity(total_samples);

            let time_step = 1.0 / fs;
            for (i, sample) in device_data.ecg.samples.iter().enumerate() {
                time_vec.push((i as f64) * time_step);
                ch1_vec.push(*sample.first().unwrap_or(&0.0));
                ch2_vec.push(*sample.get(1).unwrap_or(&0.0));
                ch3_vec.push(*sample.get(2).unwrap_or(&0.0));
            }

            // Atur ukuran pemotongan (chunking)
            // Misalnya 25 sampel (100ms) untuk 250Hz
            let chunk_size = (fs * 0.1) as usize;
            let chunk_size = if chunk_size == 0 { 25 } else { chunk_size };
            let sleep_duration = Duration::from_millis((1000.0 * chunk_size as f64 / fs) as u64);

            let mut i = 0;
            while i < total_samples {
                let end = std::cmp::min(i + chunk_size, total_samples);

                let chunk_data = RawECGData {
                    time: time_vec[i..end].to_vec(),
                    ch1: ch1_vec[i..end].to_vec(),
                    ch2: ch2_vec[i..end].to_vec(),
                    ch3: ch3_vec[i..end].to_vec(),
                };

                let payload = ECGDataPayload {
                    raw: chunk_data,
                    classification_result: device_data.prediction.label.clone(),
                    confidence: device_data.prediction.confidence_percent.to_string(),
                    anomaly_indices: vec![],
                    validation: Some(device_data.validation.clone()),
                    prediction_details: Some(device_data.prediction.clone()),
                    system: device_data.system.clone(),
                    network: device_data.network.clone(),
                    stress_test: device_data.stress_test.clone(),
                };

                let msg = ServerMessage {
                    r#type: "live_data".to_string(),
                    measurement_id: Some(device_data.message_id.clone()),
                    device_id: Some(device_data.device_id.clone()),
                    session_id: Some(device_data.session_id.clone()),
                    timestamp: Some(device_data.created_at.clone()),
                    sha256_checksum: Some("bypass".to_string()),
                    data_payload: Some(payload),
                    data: None,
                    message: None,
                };

                if let Ok(json_string) = serde_json::to_string(&msg) {
                    let mut clients_lock = clients.lock().unwrap();
                    clients_lock.retain(|sender| sender.send(json_string.clone()).is_ok());
                }

                tokio::time::sleep(sleep_duration).await;
                i = end;
            }
        }
    });

    tx
}
