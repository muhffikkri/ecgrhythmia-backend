use crate::models::device::{
    DeviceNetwork, DevicePrediction, DeviceStressTest, DeviceSystem, DeviceValidation,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct RawECGData {
    pub time: Vec<f64>,
    pub ch1: Vec<f64>, // Lead I (Murni milivolt)
    pub ch2: Vec<f64>, // Lead II (Murni milivolt)
    pub ch3: Vec<f64>, // Lead III / aVR / Kalibrasi
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct ECGDataPayload {
    pub raw: RawECGData,
    pub classification_result: String,
    pub confidence: String,
    pub anomaly_indices: Vec<usize>,

    // New fields forwarded from device
    #[serde(skip_serializing_if = "Option::is_none")]
    pub validation: Option<DeviceValidation>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub prediction_details: Option<DevicePrediction>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub system: Option<DeviceSystem>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub network: Option<DeviceNetwork>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub stress_test: Option<DeviceStressTest>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct ServerMessage {
    pub r#type: String,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub measurement_id: Option<String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub device_id: Option<String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub session_id: Option<String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub timestamp: Option<String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub sha256_checksum: Option<String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub data_payload: Option<ECGDataPayload>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub data: Option<Vec<Value>>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
}
