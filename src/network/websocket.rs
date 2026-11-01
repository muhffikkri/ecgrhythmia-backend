use axum::{
    extract::{
        ws::{Message, WebSocket, WebSocketUpgrade},
        State,
    },
    response::IntoResponse,
};
use futures_util::{sink::SinkExt, stream::StreamExt};
use std::sync::{Arc, Mutex};
use tokio::sync::mpsc::UnboundedSender;
use tracing::{info, warn};

pub type ClientList = Arc<Mutex<Vec<UnboundedSender<String>>>>;
pub type LatestFrame = Arc<Mutex<Option<String>>>;

#[derive(Clone)]
pub struct StreamState {
    pub clients: ClientList,
    pub latest_frame: LatestFrame,
}

pub async fn ws_handler(
    ws: WebSocketUpgrade,
    State(stream): State<StreamState>,
) -> impl IntoResponse {
    ws.on_upgrade(move |socket| handle_socket(socket, stream))
}

async fn handle_socket(socket: WebSocket, stream: StreamState) {
    let clients = stream.clients;
    let latest_frame = stream.latest_frame;
    info!("[WebSocket] Koneksi WebSocket baru sedang dinegosiasikan...");
    let (mut ws_sender, mut ws_receiver) = socket.split();
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<String>();

    let latest = latest_frame.lock().unwrap().clone();
    {
        let mut clients_lock = clients.lock().unwrap();
        clients_lock.push(tx.clone());
        info!(
            clients = clients_lock.len(),
            has_latest_frame = latest.is_some(),
            "[TRACE WS] Client terdaftar"
        );
    }
    if let Some(frame) = latest {
        if tx.send(frame).is_ok() {
            info!("[TRACE WS->CLIENT] Latest frame dikirim saat connect");
        } else {
            warn!("[TRACE WS->CLIENT] Gagal kirim latest frame");
        }
    } else {
        info!("[TRACE WS->CLIENT] Belum ada latest frame untuk replay");
    }

    // Task to forward messages to the websocket client
    let mut send_task = tokio::spawn(async move {
        while let Some(msg) = rx.recv().await {
            if ws_sender.send(Message::Text(msg)).await.is_err() {
                break;
            }
        }
    });

    // Task to receive incoming messages from client (optional, logging only)
    let mut recv_task = tokio::spawn(async move {
        while let Some(Ok(msg)) = ws_receiver.next().await {
            if let Ok(text) = msg.to_text() {
                info!("[WebSocket] Menerima pesan dari client: {}", text);
            }
        }
    });

    // Wait until either task completes, then close the other
    tokio::select! {
        _ = (&mut send_task) => {},
        _ = (&mut recv_task) => {},
    }

    info!("[WebSocket] Koneksi client ditutup.");
}
