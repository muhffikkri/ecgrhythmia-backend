use serde::{Deserialize, Serialize};

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct DeviceValidation {
    pub status: String,
    pub warnings: Vec<String>,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct DeviceEcg {
    pub format: String,
    pub samples: Vec<Vec<f64>>,
}



#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct DevicePrediction {
    pub status: String,
    pub label: String,
    pub confidence_percent: f64,
    pub probabilities: Option<serde_json::Value>, // Use Value to avoid strict key matching
    pub threshold: Option<f64>,
    pub latency_ms: Option<f64>,
    pub runtime: Option<String>,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct DeviceSystem {
    pub cpu_usage_percent: Option<f64>,
    pub memory_usage_percent: Option<f64>,
    pub memory_usage_mb: Option<i64>,
    pub cpu_temperature_c: Option<f64>,
    pub uptime_s: Option<i64>,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct DeviceStressTest {
    pub enabled: Option<bool>,
    pub frame_counter: Option<i64>,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct DeviceNetwork {
    pub mqtt_publish_latency_ms: Option<f64>,
    pub wifi_rssi_dbm: Option<i64>,
    pub mqtt_connected: Option<bool>,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct DevicePayload {
    pub message_id: String,
    pub device_id: String,
    pub session_id: String,
    /// ID pasien yang terkait dengan sesi ini. Disimpan agar dapat dipulihkan dari file JSONL.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub patient_id: Option<String>,
    pub frame_id: String,
    pub created_at: String,
    pub sampling_rate_hz: f64,
    pub duration_s: f64,
    pub validation: DeviceValidation,
    pub ecg: DeviceEcg,
    pub prediction: DevicePrediction,
    pub system: Option<DeviceSystem>,
    pub stress_test: Option<DeviceStressTest>,
    pub network: Option<DeviceNetwork>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json;

    #[test]
    fn test_parse_json() {
        let json_str = r#"{
  "schema_version": 1,
  "message_id": "device01-default-frame_000001",
  "device_id": "device01",
  "session_id": "default",
  "frame_id": "000001",
  "created_at": "2026-07-28T15:00:00+07:00",
  "sampling_rate_hz": 250.0,
  "duration_s": 10.0,
  "unit": "mV",
  "shape": [2500, 3],
  "channel_order": ["Lead I", "Lead II", "Lead III"],
  "validation": {
    "status": "PASS",
    "warnings": []
  },
  "ecg": {
    "format": "samples_by_time",
    "samples": [
      [1.0, 2.0, 3.0],
      [1.0, 2.0, 3.0]
    ]
  },
  "prediction": {
    "status": "PASS",
    "label": "Normal",
    "confidence_percent": 98.75,
    "probabilities": {
      "Normal": 98.75,
      "AF": 0.2,
      "Takikardia": 0.1,
      "Bradikardia": 0.95
    },
    "threshold": 0.5,
    "latency_ms": 12.0,
    "runtime": "ai-edge-litert"
  },
  "system": {
    "cpu_usage_percent": 13.2,
    "memory_usage_percent": 69.8,
    "memory_usage_mb": 2815,
    "cpu_temperature_c": 27.8,
    "uptime_s": 19352
  },
  "stress_test": {
    "enabled": true,
    "frame_counter": 1
  },
  "network": {
    "mqtt_publish_latency_ms": 8.7,
    "wifi_rssi_dbm": -59,
    "mqtt_connected": true
  }
}"#;
        let payload: DevicePayload = serde_json::from_str(json_str).unwrap();
        assert_eq!(payload.message_id, "device01-default-frame_000001");
        assert_eq!(payload.system.unwrap().memory_usage_mb, Some(2815));
    }
}
