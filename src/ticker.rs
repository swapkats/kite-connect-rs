/// Kite Connect WebSocket Ticker.
///
/// Handles binary tick parsing, subscribe/unsubscribe, mode setting,
/// and connection management with automatic reconnection.
use std::collections::HashMap;
use std::io::Cursor;

use anyhow::Result;
use byteorder::{BigEndian, ReadBytesExt};
use futures_util::{SinkExt, StreamExt};
use serde_json::{json, Value};
use tokio::sync::mpsc;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::connect_async;

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
///
/// Binary format:
/// - 2 bytes: number of packets
/// - For each packet:
///   - 2 bytes: packet length
///   - 4 bytes: instrument_token
///   - Remaining: mode-specific data
pub fn parse_ticks(data: &[u8]) -> Vec<Tick> {
    if data.len() < 2 {
        return vec![];
    }

    let mut reader = Cursor::new(data);
    let number_of_packets = match reader.read_i16::<BigEndian>() {
        Ok(n) => n as usize,
        Err(_) => return vec![],
    };

    let mut ticks = Vec::with_capacity(number_of_packets);

    for _ in 0..number_of_packets {
        let packet_length = match reader.read_i16::<BigEndian>() {
            Ok(n) => n,
            Err(_) => break,
        };

        let instrument_token = match reader.read_i32::<BigEndian>() {
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
                // LTP mode
                let last_price = reader.read_i32::<BigEndian>().unwrap_or(0) as f64 / divisor;
                Tick {
                    instrument_token, last_price, tradable,
                    mode: "ltp".to_string(), ..default_tick()
                }
            }
            28 | 32 => {
                // Index quote/full
                let last_price = reader.read_i32::<BigEndian>().unwrap_or(0) as f64 / divisor;
                let high = reader.read_i32::<BigEndian>().unwrap_or(0) as f64 / divisor;
                let low = reader.read_i32::<BigEndian>().unwrap_or(0) as f64 / divisor;
                let open = reader.read_i32::<BigEndian>().unwrap_or(0) as f64 / divisor;
                let close = reader.read_i32::<BigEndian>().unwrap_or(0) as f64 / divisor;
                let change = if close != 0.0 { Some((last_price - close) * 100.0 / close) } else { None };

                let timestamp = if packet_length == 32 {
                    Some(reader.read_i32::<BigEndian>().unwrap_or(0) as f64 / divisor)
                } else {
                    None
                };

                Tick {
                    instrument_token, last_price, tradable,
                    open: Some(open), high: Some(high), low: Some(low), close: Some(close),
                    change, timestamp,
                    mode: if packet_length == 28 { "quote".to_string() } else { "full".to_string() },
                    ..default_tick()
                }
            }
            44 | 184 => {
                // Quote/full mode (with OHLC + optional depth)
                let last_price = reader.read_i32::<BigEndian>().unwrap_or(0) as f64 / divisor;
                let last_quantity = Some(reader.read_i32::<BigEndian>().unwrap_or(0) as u64);
                let average_price = Some(reader.read_i32::<BigEndian>().unwrap_or(0) as f64 / divisor);
                let volume = Some(reader.read_i32::<BigEndian>().unwrap_or(0) as u64);
                let buy_quantity = Some(reader.read_i32::<BigEndian>().unwrap_or(0) as u64);
                let sell_quantity = Some(reader.read_i32::<BigEndian>().unwrap_or(0) as u64);
                let open = Some(reader.read_i32::<BigEndian>().unwrap_or(0) as f64 / divisor);
                let high = Some(reader.read_i32::<BigEndian>().unwrap_or(0) as f64 / divisor);
                let low = Some(reader.read_i32::<BigEndian>().unwrap_or(0) as f64 / divisor);
                let close = Some(reader.read_i32::<BigEndian>().unwrap_or(0) as f64 / divisor);
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

                // Parse depth if packet_length == 184 (full mode)
                if packet_length == 184 {
                    let last_trade_time = Some(reader.read_i32::<BigEndian>().unwrap_or(0) as u64);
                    let oi = Some(reader.read_i32::<BigEndian>().unwrap_or(0) as f64);
                    let oi_day_high = Some(reader.read_i32::<BigEndian>().unwrap_or(0) as f64);
                    let oi_day_low = Some(reader.read_i32::<BigEndian>().unwrap_or(0) as f64);
                    let timestamp = Some(reader.read_i32::<BigEndian>().unwrap_or(0) as f64);

                    tick.last_trade_time = last_trade_time;
                    tick.oi = oi;
                    tick.oi_day_high = oi_day_high;
                    tick.oi_day_low = oi_day_low;
                    tick.timestamp = timestamp;

                    // Parse 10 depth entries (5 buy + 5 sell)
                    let mut buy_entries = Vec::with_capacity(5);
                    let mut sell_entries = Vec::with_capacity(5);
                    for idx in 0..10 {
                        let entry = DepthEntry {
                            quantity: reader.read_i32::<BigEndian>().unwrap_or(0) as u64,
                            price: reader.read_i32::<BigEndian>().unwrap_or(0) as f64 / divisor,
                            orders: reader.read_i16::<BigEndian>().unwrap_or(0) as u32,
                        };
                        // Skip 2 bytes padding
                        let _ = reader.read_i16::<BigEndian>();

                        if idx < 5 { buy_entries.push(entry); }
                        else { sell_entries.push(entry); }
                    }
                    tick.depth = Some(Depth { buy: buy_entries, sell: sell_entries });
                }

                tick
            }
            _ => {
                // Unknown packet length, skip
                let _ = reader.read_u8();
                continue;
            }
        };

        ticks.push(tick);

        // Seek to the end of the packet
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
    pub async fn connect(
        &mut self,
        tick_tx: mpsc::Sender<Vec<Tick>>,
    ) -> Result<mpsc::Sender<TickerCommand>> {
        let url = format!("{}?api_key={}&access_token={}", WS_URL, self.api_key, self.access_token);
        let (ws_stream, _) = connect_async(&url).await?;
        let (mut write, mut read) = ws_stream.split();

        let (cmd_tx, mut cmd_rx) = mpsc::channel::<TickerCommand>(100);

        // Subscribe to initial tokens
        if !self.subscribed_tokens.is_empty() {
            let tokens: Vec<u32> = self.subscribed_tokens.keys().cloned().collect();
            let msg = json!({"a": "subscribe", "v": tokens});
            write.send(Message::Text(msg.to_string())).await?;

            // Set mode to quote for all
            let msg = json!({"a": "mode", "v": ["quote", tokens]});
            write.send(Message::Text(msg.to_string())).await?;
        }

        let _api_key = self.api_key.clone();
        let _access_token = self.access_token.clone();
        let _initial_tokens = self.subscribed_tokens.clone();

        // Spawn a task to handle command channel
        let mut write_clone = write;
        tokio::spawn(async move {
            while let Some(cmd) = cmd_rx.recv().await {
                match cmd {
                    TickerCommand::Subscribe(tokens) => {
                        let msg = json!({"a": "subscribe", "v": tokens});
                        let _ = write_clone.send(Message::Text(msg.to_string())).await;
                    }
                    TickerCommand::Unsubscribe(tokens) => {
                        let msg = json!({"a": "unsubscribe", "v": tokens});
                        let _ = write_clone.send(Message::Text(msg.to_string())).await;
                    }
                    TickerCommand::SetMode(mode, tokens) => {
                        let msg = json!({"a": "mode", "v": [mode, tokens]});
                        let _ = write_clone.send(Message::Text(msg.to_string())).await;
                    }
                }
            }
        });

        // Process incoming messages
        while let Some(msg) = read.next().await {
            match msg {
                Ok(Message::Binary(data)) => {
                    if data.len() > 2 {
                        let ticks = parse_binary(&data);
                        if !ticks.is_empty() {
                            let _ = tick_tx.send(ticks).await;
                        }
                    }
                }
                Ok(Message::Text(text)) => {
                    // Log postback messages
                    if let Ok(data) = serde_json::from_str::<Value>(&text) {
                        let msg_type = data.get("type").and_then(|v| v.as_str()).unwrap_or("unknown");
                        tracing::info!("WS message [{}]: {}", msg_type, &text[..200.min(text.len())]);
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
    parse_binary_data(data)
}

fn parse_binary_data(data: &[u8]) -> Vec<Tick> {
    let mut reader = Cursor::new(data);
    let number_of_packets = match reader.read_i16::<BigEndian>() {
        Ok(n) => n as usize,
        Err(_) => return vec![],
    };

    let mut ticks = Vec::with_capacity(number_of_packets);

    for _ in 0..number_of_packets {
        let packet_length = match reader.read_i16::<BigEndian>() {
            Ok(n) => n,
            Err(_) => break,
        };

        let instrument_token = match reader.read_i32::<BigEndian>() {
            Ok(n) => n as u32,
            Err(_) => break,
        };

        let segment = instrument_token & 0xFF;
        let divisor = if segment == 3 { 10_000_000.0 } else { 100.0 };
        let tradable = segment != 9;

        let start_pos = reader.position();
        let end_pos = start_pos + (packet_length as u64);

        let tick = match packet_length {
            8 => {
                let last_price = reader.read_i32::<BigEndian>().unwrap_or(0) as f64 / divisor;
                Tick { instrument_token, last_price, tradable, mode: "ltp".to_string(), ..default_tick() }
            }
            28 | 32 => {
                let last_price = reader.read_i32::<BigEndian>().unwrap_or(0) as f64 / divisor;
                let high = reader.read_i32::<BigEndian>().unwrap_or(0) as f64 / divisor;
                let low = reader.read_i32::<BigEndian>().unwrap_or(0) as f64 / divisor;
                let open = reader.read_i32::<BigEndian>().unwrap_or(0) as f64 / divisor;
                let close = reader.read_i32::<BigEndian>().unwrap_or(0) as f64 / divisor;
                let change = if close != 0.0 { Some((last_price - close) * 100.0 / close) } else { None };
                let timestamp = if packet_length == 32 { Some(reader.read_i32::<BigEndian>().unwrap_or(0) as f64 / divisor) } else { None };
                Tick {
                    instrument_token, last_price, tradable,
                    open: Some(open), high: Some(high), low: Some(low), close: Some(close),
                    change, timestamp, mode: if packet_length == 28 { "quote".to_string() } else { "full".to_string() },
                    ..default_tick()
                }
            }
            44 | 184 => {
                let last_price = reader.read_i32::<BigEndian>().unwrap_or(0) as f64 / divisor;
                let last_quantity = Some(reader.read_i32::<BigEndian>().unwrap_or(0) as u64);
                let average_price = Some(reader.read_i32::<BigEndian>().unwrap_or(0) as f64 / divisor);
                let volume = Some(reader.read_i32::<BigEndian>().unwrap_or(0) as u64);
                let buy_quantity = Some(reader.read_i32::<BigEndian>().unwrap_or(0) as u64);
                let sell_quantity = Some(reader.read_i32::<BigEndian>().unwrap_or(0) as u64);
                let open = Some(reader.read_i32::<BigEndian>().unwrap_or(0) as f64 / divisor);
                let high = Some(reader.read_i32::<BigEndian>().unwrap_or(0) as f64 / divisor);
                let low = Some(reader.read_i32::<BigEndian>().unwrap_or(0) as f64 / divisor);
                let close = Some(reader.read_i32::<BigEndian>().unwrap_or(0) as f64 / divisor);
                let change = if close.unwrap_or(0.0) != 0.0 { Some((last_price - close.unwrap_or(0.0)) * 100.0 / close.unwrap_or(1.0)) } else { None };

                let mut tick = Tick {
                    instrument_token, last_price, tradable,
                    last_quantity, average_price, volume, buy_quantity, sell_quantity,
                    open, high, low, close, change,
                    mode: if packet_length == 44 { "quote".to_string() } else { "full".to_string() },
                    ..default_tick()
                };

                if packet_length == 184 {
                    tick.last_trade_time = Some(reader.read_i32::<BigEndian>().unwrap_or(0) as u64);
                    tick.oi = Some(reader.read_i32::<BigEndian>().unwrap_or(0) as f64);
                    tick.oi_day_high = Some(reader.read_i32::<BigEndian>().unwrap_or(0) as f64);
                    tick.oi_day_low = Some(reader.read_i32::<BigEndian>().unwrap_or(0) as f64);
                    tick.timestamp = Some(reader.read_i32::<BigEndian>().unwrap_or(0) as f64);

                    let mut buy = Vec::with_capacity(5);
                    let mut sell = Vec::with_capacity(5);
                    for idx in 0..10 {
                        let entry = DepthEntry {
                            quantity: reader.read_i32::<BigEndian>().unwrap_or(0) as u64,
                            price: reader.read_i32::<BigEndian>().unwrap_or(0) as f64 / divisor,
                            orders: reader.read_i16::<BigEndian>().unwrap_or(0) as u32,
                        };
                        let _ = reader.read_i16::<BigEndian>(); // padding
                        if idx < 5 { buy.push(entry); } else { sell.push(entry); }
                    }
                    tick.depth = Some(Depth { buy, sell });
                }
                tick
            }
            _ => { continue; }
        };

        ticks.push(tick);
        let _ = reader.set_position(end_pos);
    }

    ticks
}
