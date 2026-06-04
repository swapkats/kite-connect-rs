# kite-connect-rs

Modern async Rust client for the [Kite Connect API](https://kite.trade/docs/connect/v3/).

## Features

- **Fully async** — tokio + reqwest + tokio-tungstenite
- **REST API** — orders, quotes, historical data, GTT, positions, holdings, profile, margins
- **WebSocket ticker** — binary tick parsing for all modes (LTP, quote, full)
- **Market depth** — 5-level buy/sell depth in full mode
- **GTT (Good Till Triggered)** — place, modify, get, delete triggers
- **Session management** — token exchange, refresh, expiry detection
- **Typed responses** — deserialized structs, not raw JSON

## Quick Start

```rust
use kite_connect::KiteClient;

#[tokio::main]
async fn main() {
    let client = KiteClient::new("api_key", "access_token");

    // Get user profile
    let profile = client.profile().await.unwrap();
    println!("User: {:?}", profile);

    // Get LTP
    let ltp = client.ltp(&["NSE:INFY", "NSE:RELIANCE"]).await.unwrap();
    println!("LTP: {}", ltp);

    // Place an order
    let order = client.place_order(
        "regular", "NSE", "INFY-EQ", "BUY", 1, "CNC", "LIMIT",
        Some(1500.0), Some("DAY"), None,
    ).await.unwrap();
    println!("Order: {}", order);
}
```

## WebSocket Ticker

```rust
use kite_connect::ticker::{KiteTicker, TickerCommand};
use tokio::sync::mpsc;

#[tokio::main]
async fn main() {
    let mut ticker = KiteTicker::new("api_key", "access_token");
    let (tick_tx, mut tick_rx) = mpsc::channel(5000);

    let cmd_tx = ticker.connect(tick_tx).await.unwrap();

    // Subscribe to instruments
    cmd_tx.send(TickerCommand::Subscribe(vec![53496579, 53496071])).await.unwrap();

    // Receive ticks
    while let Some(ticks) = tick_rx.recv().await {
        for tick in &ticks {
            println!("{}: ₹{:.2}", tick.instrument_token, tick.last_price);
        }
    }
}
```

## API Coverage

| Endpoint | Method | Status |
|----------|--------|--------|
| `/user/profile` | `profile()` | Done |
| `/user/margins` | `margins()` | Done |
| `/portfolio/holdings` | `holdings()` | Done |
| `/portfolio/positions` | `positions()` | Done |
| `/orders` | `orders()` | Done |
| `/orders/{id}` | `order_history()` | Done |
| `/trades` | `trades()` | Done |
| `/orders/regular` | `place_order()` | Done |
| `/orders/{variety}/{id}` | `modify_order()` | Done |
| `/orders/{variety}/{id}` | `cancel_order()` | Done |
| `/quote/ltp` | `ltp()` | Done |
| `/quote` | `quote()` | Done |
| `/quote/ohlc` | `ohlc()` | Done |
| `/instruments/historical/{id}/{interval}` | `historical_data()` | Done |
| `/gtt/triggers` | `place_gtt()` | Done |
| `/gtt/triggers/{id}` | `get_gtt()` / `modify_gtt()` / `delete_gtt()` | Done |
| `/instruments` | `instruments_raw()` | Done |
| `/session/token` | `generate_session()` | Done |
| `/session/refresh_token` | `renew_token()` | Done |
| WebSocket `ws.kite.trade` | `KiteTicker::connect()` | Done |

## Binary Tick Format

The WebSocket ticker receives binary data in Kite's packed format:

- **8 bytes** — LTP mode: instrument_token + last_price
- **28/32 bytes** — Index quote/full: last_price + OHLC + optional timestamp
- **44/184 bytes** — Quote/full: last_price + quantity + volume + OHLC + optional 5-level depth

Ported from [zerodhatech/kiteconnect-rs](https://github.com/zerodhatech/kiteconnect-rust) with async support and proper error handling.

## Building

```bash
cargo build
cargo test
```

## License

MIT
