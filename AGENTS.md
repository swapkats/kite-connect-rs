# Kite Connect Rust — Agent Guide

> **Purpose:** Async Rust client for the Kite Connect trading API. Used by the Rust trading daemon.

## Architecture

```
src/
├── lib.rs          - public API: re-exports KiteClient, KiteError
├── connect.rs      - REST API client (all HTTP endpoints)
├── ticker.rs       - WebSocket binary tick parser + async connection
├── models.rs       - typed response structs (serde deserialize)
└── error.rs        - KiteError enum
```

## Key Types

### `KiteClient` (connect.rs)
All Kite REST API operations. Takes `&self` (immutable), so can be `Arc<KiteClient>` for concurrent use.

```rust
let client = KiteClient::new("api_key", "access_token");
```

Key methods:
- `profile()`, `margins()` — user/account
- `holdings()`, `positions()` — portfolio
- `orders()`, `place_order()`, `modify_order()`, `cancel_order()` — order lifecycle
- `ltp()`, `quote()`, `ohlc()` — market data
- `historical_data()` — OHLC candles
- `place_gtt()`, `modify_gtt()`, `get_gtt()`, `delete_gtt()` — GTT triggers
- `generate_session()`, `renew_token()`, `invalidate_token()` — auth

### `KiteTicker` (ticker.rs)
WebSocket connection for real-time ticks.

```rust
let mut ticker = KiteTicker::new("api_key", "access_token");
let (tick_tx, mut tick_rx) = mpsc::channel(5000);
let cmd_tx = ticker.connect(tick_tx).await.unwrap();
```

Commands via `cmd_tx`:
- `TickerCommand::Subscribe(vec![token1, token2])`
- `TickerCommand::Unsubscribe(vec![token])`
- `TickerCommand::SetMode("quote".into(), vec![token])`

### `Tick` (ticker.rs)
Parsed tick data from binary WebSocket message. Fields vary by mode:
- LTP: just `instrument_token`, `last_price`
- Quote: adds OHLC, volume, buy/sell quantity
- Full: adds depth (5-level), oi, timestamps

### `parse_binary(data: &[u8]) -> Vec<Tick>`
Public function to parse raw binary WebSocket data into ticks. Can be used independently.

## Error Handling

`KiteError` variants:
- `Api { status, message }` — Kite API returned an error
- `SessionExpired` — 403 with TokenException (need re-auth)
- `Http(String)` — reqwest/network error
- `WebSocket(String)` — WS connection error
- `Serialization(String)` — JSON parse error

## Dependencies

- `reqwest` (async HTTP)
- `tokio` (async runtime)
- `tokio-tungstenite` (async WebSocket)
- `serde` / `serde_json` (typed deserialization)
- `sha2` / `hex` (session token checksum)
- `byteorder` (binary tick parsing)
- `chrono` (timestamps)
- `tracing` (logging)

## Common Pitfalls

1. **Session expiry** — All methods return `KiteError::SessionExpired` on 403 with TokenException. Caller must refresh and retry.
2. **Binary tick parsing** — Divisor is 100 for NFO/MCX, 10000000 for CDS. Segment is `instrument_token & 0xFF`.
3. **GTT limit orders** — Kite GTT only supports LIMIT orders (not MARKET). The daemon applies a 0.1% buffer from SL price.
4. **Quote depth** — Only available in "full" mode (packet length 184). Quote mode (44) has no depth.
5. **Instruments CSV** — `instruments_raw()` returns CSV text, not JSON. Parse with a CSV reader.
