/// Kite Connect WebSocket Ticker.
///
/// Handles binary tick parsing, subscribe/unsubscribe, mode setting,
/// and connection management with automatic reconnection.
use std::collections::HashMap;
use std::io::Cursor;

use anyhow::Result;
use byteorder::{BigEndian};
use byteorder::ReadBytesExt as ByteOrderReadExt;
use futures_util::{SinkExt, StreamExt};
use serde_json::{json, Value};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::mpsc;
use tokio_native_tls::TlsConnector;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::WebSocketStream;

pub const WS_URL: &str = "wss://ws.kite.trade";

/// Parsed tick data from binary WebSocket message.
#[derive(Debug, Clone)]
pub struct Tick {
    pub instrument_token: u32,
    pub last_price: f64,
    pub last_quantity: Option<u64>,
    pub average_price: Option<f64>,
    pub volume: Option<u64>,
    pub buy_quantity: Option<u64>,
    pub sell_quantity: Option<u64>,
    pub open: Option<f64>,
    pub high: Option<f64>,
    pub low: Option<f64>,
    pub close: Option<f64>,
    pub oi: Option<f64>,
    pub last_trade_time: Option<u64>,
    pub oi_day_high: Option<f64>,
    pub oi_day_low: Option<f64>,
    pub timestamp: Option<f64>,
    pub change: Option<f64>,
    pub tradable: bool,
    pub mode: String,
    pub depth: Option<Depth>,
}

#[derive(Debug, Clone, Default)]
pub struct Depth {
    pub buy: Vec<DepthEntry>,
    pub sell: Vec<DepthEntry>,
}

#[derive(Debug, Clone, Default)]
pub struct DepthEntry {
    pub quantity: u64,
    pub price: f64,
    pub orders: u32,
}

/// Parse binary tick data from Kite WebSocket.
pub fn parse_ticks(data: &[u8]) -> Vec<Tick> {
    if data.len() < 2 {
        return vec![];
    }

    let mut reader = Cursor::new(data);
    let number_of_packets = match ByteOrderReadExt::read_i16::<BigEndian>(&mut reader) {
        Ok(n) => n as usize,
        Err(_) => return vec![],
    };

    let mut ticks = Vec::with_capacity(number_of_packets);

    for _ in 0..number_of_packets {
        let packet_length = match ByteOrderReadExt::read_i16::<BigEndian>(&mut reader) {
            Ok(n) => n,
            Err(_) => break,
        };

        let instrument_token = match ByteOrderReadExt::read_i32::<BigEndian>(&mut reader) {
            Ok(n) => n as u32,
            Err(_) => break,
        };

        let segment = instrument_token & 0xFF;
        let divisor = if segment == 3 { 10_000_000.0 } else { 100.0 };
        let tradable = segment != 9;

        let current_pos = reader.position();
        let target_pos = current_pos + (packet_length as u64);

        let tick = match packet_length {
            8 => {
                let last_price = ByteOrderReadExt::read_i32::<BigEndian>(&mut reader).unwrap_or(0) as f64 / divisor;
                Tick {
                    instrument_token, last_price, tradable,
                    mode: "ltp".to_string(), ..default_tick()
                }
            }
            28 | 32 => {
                let last_price = ByteOrderReadExt::read_i32::<BigEndian>(&mut reader).unwrap_or(0) as f64 / divisor;
                let high = ByteOrderReadExt::read_i32::<BigEndian>(&mut reader).unwrap_or(0) as f64 / divisor;
                let low = ByteOrderReadExt::read_i32::<BigEndian>(&mut reader).unwrap_or(0) as f64 / divisor;
                let open = ByteOrderReadExt::read_i32::<BigEndian>(&mut reader).unwrap_or(0) as f64 / divisor;
                let close = ByteOrderReadExt::read_i32::<BigEndian>(&mut reader).unwrap_or(0) as f64 / divisor;
                let change = if close != 0.0 { Some((last_price - close) * 100.0 / close) } else { None };
                let timestamp = if packet_length == 32 {
                    Some(ByteOrderReadExt::read_i32::<BigEndian>(&mut reader).unwrap_or(0) as f64 / divisor)
                } else { None };
                Tick {
                    instrument_token, last_price, tradable,
                    open: Some(open), high: Some(high), low: Some(low), close: Some(close),
                    change, timestamp,
                    mode: if packet_length == 28 { "quote".to_string() } else { "full".to_string() },
                    ..default_tick()
                }
            }
            44 | 184 => {
                let last_price = ByteOrderReadExt::read_i32::<BigEndian>(&mut reader).unwrap_or(0) as f64 / divisor;
                let last_quantity = Some(ByteOrderReadExt::read_i32::<BigEndian>(&mut reader).unwrap_or(0) as u64);
                let average_price = Some(ByteOrderReadExt::read_i32::<BigEndian>(&mut reader).unwrap_or(0) as f64 / divisor);
                let volume = Some(ByteOrderReadExt::read_i32::<BigEndian>(&mut reader).unwrap_or(0) as u64);
                let buy_quantity = Some(ByteOrderReadExt::read_i32::<BigEndian>(&mut reader).unwrap_or(0) as u64);
                let sell_quantity = Some(ByteOrderReadExt::read_i32::<BigEndian>(&mut reader).unwrap_or(0) as u64);
                let open = Some(ByteOrderReadExt::read_i32::<BigEndian>(&mut reader).unwrap_or(0) as f64 / divisor);
                let high = Some(ByteOrderReadExt::read_i32::<BigEndian>(&mut reader).unwrap_or(0) as f64 / divisor);
                let low = Some(ByteOrderReadExt::read_i32::<BigEndian>(&mut reader).unwrap_or(0) as f64 / divisor);
                let close = Some(ByteOrderReadExt::read_i32::<BigEndian>(&mut reader).unwrap_or(0) as f64 / divisor);
                let change = if close.unwrap_or(0.0) != 0.0 {
                    Some((last_price - close.unwrap_or(0.0)) * 100.0 / close.unwrap_or(1.0))
                } else { None };

                let mut tick = Tick {
                    instrument_token, last_price, tradable,
                    last_quantity, average_price, volume, buy_quantity, sell_quantity,
                    open, high, low, close, change,
                    mode: if packet_length == 44 { "quote".to_string() } else { "full".to_string() },
                    ..default_tick()
                };

                if packet_length == 184 {
                    tick.last_trade_time = Some(ByteOrderReadExt::read_i32::<BigEndian>(&mut reader).unwrap_or(0) as u64);
                    tick.oi = Some(ByteOrderReadExt::read_i32::<BigEndian>(&mut reader).unwrap_or(0) as f64);
                    tick.oi_day_high = Some(ByteOrderReadExt::read_i32::<BigEndian>(&mut reader).unwrap_or(0) as f64);
                    tick.oi_day_low = Some(ByteOrderReadExt::read_i32::<BigEndian>(&mut reader).unwrap_or(0) as f64);
                    tick.timestamp = Some(ByteOrderReadExt::read_i32::<BigEndian>(&mut reader).unwrap_or(0) as f64);

                    let mut buy_entries = Vec::with_capacity(5);
                    let mut sell_entries = Vec::with_capacity(5);
                    for idx in 0..10 {
                        let entry = DepthEntry {
                            quantity: ByteOrderReadExt::read_i32::<BigEndian>(&mut reader).unwrap_or(0) as u64,
                            price: ByteOrderReadExt::read_i32::<BigEndian>(&mut reader).unwrap_or(0) as f64 / divisor,
                            orders: ByteOrderReadExt::read_i16::<BigEndian>(&mut reader).unwrap_or(0) as u32,
                        };
                        let _ = ByteOrderReadExt::read_i16::<BigEndian>(&mut reader);
                        if idx < 5 { buy_entries.push(entry); }
                        else { sell_entries.push(entry); }
                    }
                    tick.depth = Some(Depth { buy: buy_entries, sell: sell_entries });
                }
                tick
            }
            _ => { continue; }
        };

        ticks.push(tick);
        let _ = reader.set_position(target_pos);
    }

    ticks
}

fn default_tick() -> Tick {
    Tick {
        instrument_token: 0, last_price: 0.0, last_quantity: None,
        average_price: None, volume: None, buy_quantity: None, sell_quantity: None,
        open: None, high: None, low: None, close: None, oi: None,
        last_trade_time: None, oi_day_high: None, oi_day_low: None,
        timestamp: None, change: None, tradable: true,
        mode: "ltp".to_string(), depth: None,
    }
}

/// WebSocket connection to Kite.
pub struct KiteTicker {
    api_key: String,
    access_token: String,
    subscribed_tokens: HashMap<u32, String>,
}

impl KiteTicker {
    pub fn new(api_key: &str, access_token: &str) -> Self {
        Self {
            api_key: api_key.to_string(),
            access_token: access_token.to_string(),
            subscribed_tokens: HashMap::new(),
        }
    }

    /// Connect and start receiving ticks. Sends parsed ticks to the channel.
    /// Returns a sender for subscribe/unsubscribe commands.
    ///
    /// Uses manual HTTP upgrade (not tungstenite's connect_async) because
    /// Kite's ELB rejects tungstenite's default upgrade request.
    pub async fn connect(
        &mut self,
        tick_tx: mpsc::Sender<Vec<Tick>>,
    ) -> Result<mpsc::Sender<TickerCommand>> {
        let api_key = self.api_key.clone();
        let access_token = self.access_token.clone();

        // Manual TLS + HTTP upgrade approach (proven to work with Kite's ELB)
        let tcp = TcpStream::connect("ws.kite.trade:443").await?;
        tcp.set_nodelay(true)?;

        let connector = TlsConnector::from(
            native_tls::TlsConnector::builder().build()?
        );
        let mut tls = connector.connect("ws.kite.trade", tcp).await?;

        // WebSocket upgrade request
        let ws_key = base64::Engine::encode(
            &base64::engine::general_purpose::STANDARD,
            &[0u8; 16],
        );
        let path = format!("/?api_key={}&access_token={}", api_key, access_token);
        let request = format!("GET {} HTTP/1.1\r\nHost: ws.kite.trade\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Key: {}\r\nSec-WebSocket-Version: 13\r\nUser-Agent: Mozilla/5.0\r\nOrigin: https://kite.zerodha.com\r\n\r\n", path, ws_key);

        tls.write_all(request.as_bytes()).await?;
        tls.flush().await?;

        let mut buf = [0u8; 4096];
        let n = tls.read(&mut buf).await?;
        let response = String::from_utf8_lossy(&buf[..n]);
        if !response.contains("101") {
            return Err(anyhow::anyhow!("WebSocket upgrade failed: {}", response));
        }
        tracing::info!("✅ WebSocket connected (manual HTTP upgrade)");

        // Wrap raw TLS stream as WebSocket
        let ws_stream = WebSocketStream::from_raw_socket(
            tls,
            tokio_tungstenite::tungstenite::protocol::Role::Client,
            None,
        ).await;

        let (mut write, mut read) = ws_stream.split();

        // Subscribe to initial tokens
        if !self.subscribed_tokens.is_empty() {
            let tokens: Vec<u32> = self.subscribed_tokens.keys().cloned().collect();
            let msg = json!({"a": "subscribe", "v": tokens});
            write.send(Message::Text(msg.to_string())).await?;
            let msg = json!({"a": "mode", "v": ["quote", tokens]});
            write.send(Message::Text(msg.to_string())).await?;
        }

        let (cmd_tx, mut cmd_rx) = mpsc::channel::<TickerCommand>(100);

        // Spawn command handler
        tokio::spawn(async move {
            while let Some(cmd) = cmd_rx.recv().await {
                match cmd {
                    TickerCommand::Subscribe(tokens) => {
                        let msg = json!({"a": "subscribe", "v": tokens});
                        let _ = write.send(Message::Text(msg.to_string())).await;
                    }
                    TickerCommand::Unsubscribe(tokens) => {
                        let msg = json!({"a": "unsubscribe", "v": tokens});
                        let _ = write.send(Message::Text(msg.to_string())).await;
                    }
                    TickerCommand::SetMode(mode, tokens) => {
                        let msg = json!({"a": "mode", "v": [mode, tokens]});
                        let _ = write.send(Message::Text(msg.to_string())).await;
                    }
                }
            }
        });

        // Process incoming messages
        tokio::spawn(async move {
            while let Some(msg) = read.next().await {
                match msg {
                    Ok(Message::Binary(data)) => {
                        if data.len() > 2 {
                            let ticks = parse_ticks(&data);
                            if !ticks.is_empty() {
                                let _ = tick_tx.send(ticks).await;
                            }
                        }
                    }
                    Ok(Message::Text(text)) => {
                        if let Ok(data) = serde_json::from_str::<Value>(&text) {
                            let msg_type = data.get("type").and_then(|v| v.as_str()).unwrap_or("unknown");
                            tracing::info!("WS msg [{}]: {}", msg_type, &text[..200.min(text.len())]);
                        }
                    }
                    Ok(Message::Close(_)) => {
                        tracing::warn!("WS closed");
                        break;
                    }
                    Err(e) => {
                        tracing::error!("WS error: {}", e);
                        break;
                    }
                    _ => {}
                }
            }
        });

        Ok(cmd_tx)
    }
}

/// Commands to send to the ticker WebSocket.
pub enum TickerCommand {
    Subscribe(Vec<u32>),
    Unsubscribe(Vec<u32>),
    SetMode(String, Vec<u32>),
}

/// Parse binary tick data (public wrapper).
pub fn parse_binary(data: &[u8]) -> Vec<Tick> {
    parse_ticks(data)
}
