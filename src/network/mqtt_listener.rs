/**
 * @fileoverview Modul Network: MQTT Listener (Rust)
 * Bertugas berlangganan (subscribe) data EKG dari MQTT Broker (Mosquitto)
 * dan meneruskannya ke handler WebSocket.
 */

use rumqttc::{Client, MqttOptions, QoS, Event, Packet, Transport, TlsConfiguration};
use std::thread;
use std::time::Duration;
use tracing::{info, warn, error};

pub fn start_mqtt_listener<F>(
    broker_host: &str,
    broker_port: u16,
    topic: &str,
    username: &str,
    password: &str,
    on_message: F
) -> Client
where
    F: Fn(String) + Send + 'static,
{
    let host = broker_host.to_string();
    let topic_name = topic.to_string();

    use std::time::{SystemTime, UNIX_EPOCH};
    let timestamp = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis();
    let client_id = format!("rust_ecg_bridge_{}", timestamp);
    
    let mut mqttoptions = MqttOptions::new(&client_id, &host, broker_port);
    mqttoptions.set_keep_alive(Duration::from_secs(60));
    // Perbesar limit ukuran payload hingga 10MB agar tidak error "payload size limit exceeded"
    mqttoptions.set_max_packet_size(10 * 1024 * 1024, 10 * 1024 * 1024);
    
    // --- ADDED CREDENTIALS & TLS FOR HIVEMQ CLOUD ---
    mqttoptions.set_credentials(username, password);
    
    if broker_port == 8883 {
        mqttoptions.set_transport(Transport::Tls(TlsConfiguration::default()));
    }

    let (client, mut connection) = Client::new(mqttoptions, 10);
    let client_clone = client.clone();
    
    info!(host = %host, port = broker_port, "Mencoba menghubungkan ke Broker MQTT...");

    // Spawn thread khusus agar listener MQTT tidak mengganggu server WebSocket
    thread::spawn(move || {
        // Loop mendengarkan pesan yang masuk
        for notification in connection.iter() {
            match notification {
                Ok(Event::Incoming(Packet::ConnAck(_))) => {
                    info!("Berhasil terhubung secara riil ke Broker HiveMQ Cloud!");
                    // Resubscribe every time connection is established
                    if let Err(e) = client_clone.subscribe(&topic_name, QoS::AtLeastOnce) {
                        error!(error = %e, topic = %topic_name, "Gagal mengirim permintaan subscribe");
                    }
                }
                Ok(Event::Incoming(Packet::SubAck(_))) => {
                    info!(topic = %topic_name, "Berhasil berlangganan secara resmi ke topik");
                }
                Ok(Event::Incoming(Packet::Publish(publish))) => {
                    if let Ok(payload_str) = String::from_utf8(publish.payload.to_vec()) {
                        if serde_json::from_str::<serde_json::Value>(&payload_str).is_err() {
                            info!("Menerima paket sensor EKG (Invalid JSON format)");
                        }
                        
                        // Teruskan pesan JSON murni ke callback WebSocket
                        on_message(payload_str);
                    } else {
                        info!(
                            payload_len = publish.payload.len(),
                            topic = %publish.topic,
                            "Menerima data payload dari topik (bukan format teks)"
                        );
                    }
                }
                Err(e) => {
                    warn!(error = ?e, "Koneksi MQTT terputus atau error. Mencoba menghubungkan kembali...");
                    thread::sleep(Duration::from_secs(5));
                }
                _ => {}
            }
        }
    });

    client
}
