/// Typed response models for Kite Connect API.
use serde::Deserialize;

#[derive(Debug, Clone, Deserialize)]
pub struct Profile {
    pub user_id: Option<String>,
    pub user_name: Option<String>,
    pub email: Option<String>,
    pub broker: Option<String>,
    pub exchanges: Option<Vec<String>>,
    pub products: Option<Vec<String>>,
    pub order_types: Option<Vec<String>>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct Margins {
    pub equity: Option<SegmentMargins>,
    pub commodity: Option<SegmentMargins>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct SegmentMargins {
    pub net: Option<f64>,
    pub available: Option<MarginAvailable>,
    pub used: Option<MarginUsed>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct MarginAvailable {
    pub cash: Option<f64>,
    pub collateral: Option<f64>,
    pub intraday_margin: Option<f64>,
    pub available_margin: Option<f64>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct MarginUsed {
    pub debits: Option<f64>,
    pub exposure: Option<f64>,
    pub m2m: Option<f64>,
    pub premium: Option<f64>,
    pub used_margin: Option<f64>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct Order {
    pub order_id: Option<String>,
    pub exchange: Option<String>,
    pub tradingsymbol: Option<String>,
    pub transaction_type: Option<String>,
    pub order_type: Option<String>,
    pub price: Option<f64>,
    pub quantity: Option<u32>,
    pub status: Option<String>,
    pub average_price: Option<f64>,
    #[serde(default)]
    pub filled_quantity: Option<u32>,
    pub timestamp: Option<String>,
    pub product: Option<String>,
    pub validity: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct Trade {
    pub trade_id: Option<String>,
    pub order_id: Option<String>,
    pub exchange: Option<String>,
    pub tradingsymbol: Option<String>,
    pub transaction_type: Option<String>,
    pub quantity: Option<u32>,
    pub price: Option<f64>,
    pub average_price: Option<f64>,
    pub product: Option<String>,
    pub fill_timestamp: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct Position {
    pub exchange: Option<String>,
    pub tradingsymbol: Option<String>,
    pub product: Option<String>,
    pub quantity: Option<i32>,
    pub overnight_quantity: Option<i32>,
    pub multiplier: Option<f64>,
    pub average_price: Option<f64>,
    pub close_price: Option<f64>,
    pub last_price: Option<f64>,
    pub pnl: Option<f64>,
    pub m2m: Option<f64>,
    pub unrealised: Option<f64>,
    pub realised: Option<f64>,
    pub buy_quantity: Option<i32>,
    pub buy_price: Option<f64>,
    pub sell_quantity: Option<i32>,
    pub sell_price: Option<f64>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct Holding {
    pub exchange: Option<String>,
    pub tradingsymbol: Option<String>,
    pub isin: Option<String>,
    pub product: Option<String>,
    pub quantity: Option<i32>,
    pub average_price: Option<f64>,
    pub last_price: Option<f64>,
    pub pnl: Option<f64>,
    pub close_price: Option<f64>,
    pub day_change: Option<f64>,
    pub day_change_percentage: Option<f64>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct HistoricalCandle {
    pub date: String,
    pub open: f64,
    pub high: f64,
    pub low: f64,
    pub close: f64,
    pub volume: u64,
}

#[derive(Debug, Clone, Deserialize)]
pub struct QuoteData {
    pub last_price: Option<f64>,
    pub ohlc: Option<Ohlc>,
    pub depth: Option<MarketDepth>,
    pub volume: Option<u64>,
    pub average_price: Option<f64>,
    pub buy_quantity: Option<u64>,
    pub sell_quantity: Option<u64>,
    pub oi: Option<f64>,
    pub upper_circuit_limit: Option<f64>,
    pub lower_circuit_limit: Option<f64>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct Ohlc {
    pub open: Option<f64>,
    pub high: Option<f64>,
    pub low: Option<f64>,
    pub close: Option<f64>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct MarketDepth {
    #[serde(default)]
    pub buy: Vec<DepthLevel>,
    #[serde(default)]
    pub sell: Vec<DepthLevel>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct DepthLevel {
    pub quantity: Option<u64>,
    pub price: Option<f64>,
    pub orders: Option<u32>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct GttTrigger {
    pub trigger_id: Option<u64>,
    pub tradingsymbol: Option<String>,
    pub exchange: Option<String>,
    pub trigger_values: Option<Vec<f64>>,
    pub status: Option<String>,
    pub orders: Option<Vec<GttOrder>>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct GttOrder {
    pub transaction_type: Option<String>,
    pub quantity: Option<u32>,
    pub price: Option<f64>,
    pub order_type: Option<String>,
    pub product: Option<String>,
}

/// Token exchange response
#[derive(Debug, Clone, Deserialize)]
pub struct SessionToken {
    pub access_token: Option<String>,
    pub refresh_token: Option<String>,
    pub user_id: Option<String>,
    pub api_key: Option<String>,
}
