/// Kite Connect REST API client.
use anyhow::Result;
use reqwest::Client;
use serde_json::{json, Value};

use crate::error::KiteError;
use crate::models::*;

const BASE_URL: &str = "https://api.kite.trade";
const KITE_VERSION: &str = "3";

/// Kite Connect API client.
///
/// # Example
/// ```rust,no_run
/// use kite_connect::KiteClient;
///
/// #[tokio::main]
/// async fn main() {
///     let client = KiteClient::new("api_key", "access_token");
///     let profile = client.profile().await.unwrap();
/// }
/// ```
pub struct KiteClient {
    api_key: String,
    access_token: String,
    http: Client,
}

impl KiteClient {
    /// Create a new Kite Connect client.
    pub fn new(api_key: &str, access_token: &str) -> Self {
        Self {
            api_key: api_key.to_string(),
            access_token: access_token.to_string(),
            http: Client::new(),
        }
    }

    /// Set a new access token (e.g., after refresh).
    pub fn set_access_token(&mut self, token: &str) {
        self.access_token = token.to_string();
    }

    /// Get the Kite login URL for browser authentication.
    pub fn login_url(&self) -> String {
        format!("https://kite.trade/connect/login?api_key={}&v3", self.api_key)
    }

    /// Exchange a request_token for an access_token.
    pub async fn generate_session(&self, request_token: &str, api_secret: &str) -> Result<SessionToken, KiteError> {
        use sha2::{Sha256, Digest};
        let checksum_input = format!("{}{}{}", self.api_key, request_token, api_secret);
        let mut hasher = Sha256::new();
        hasher.update(checksum_input.as_bytes());
        let checksum = hex::encode(hasher.finalize());

        let resp = self.post("/session/token", json!({
            "api_key": self.api_key,
            "request_token": request_token,
            "checksum": checksum,
        })).await?;
        Ok(serde_json::from_value(resp)?)
    }

    /// Invalidate an access token.
    pub async fn invalidate_token(&self, token: &str) -> Result<Value, KiteError> {
        self.delete("/session/token", Some(json!({"access_token": token}))).await
    }

    /// Renew access token using a refresh_token.
    pub async fn renew_token(&self, access_token: &str, api_secret: &str) -> Result<Value, KiteError> {
        use sha2::{Sha256, Digest};
        let checksum_input = format!("{}{}{}", self.api_key, access_token, api_secret);
        let mut hasher = Sha256::new();
        hasher.update(checksum_input.as_bytes());
        let checksum = hex::encode(hasher.finalize());

        self.post("/session/refresh_token", json!({
            "api_key": self.api_key,
            "access_token": access_token,
            "checksum": checksum,
        })).await
    }

    // ── User & Account ──────────────────────────────────────────────────

    pub async fn profile(&self) -> Result<Profile, KiteError> {
        let resp = self.get("/user/profile", None).await?;
        Ok(serde_json::from_value(resp)?)
    }

    pub async fn margins(&self, segment: Option<&str>) -> Result<Margins, KiteError> {
        let path = match segment {
            Some(s) => format!("/user/margins/{}", s),
            None => "/user/margins".to_string(),
        };
        let resp = self.get(&path, None).await?;
        Ok(serde_json::from_value(resp)?)
    }

    // ── Portfolio ───────────────────────────────────────────────────────

    pub async fn holdings(&self) -> Result<Vec<Holding>, KiteError> {
        let resp = self.get("/portfolio/holdings", None).await?;
        Ok(serde_json::from_value(resp)?)
    }

    pub async fn positions(&self) -> Result<Vec<Position>, KiteError> {
        let resp = self.get("/portfolio/positions", None).await?;
        Ok(serde_json::from_value(resp)?)
    }

    // ── Orders ──────────────────────────────────────────────────────────

    pub async fn orders(&self) -> Result<Vec<Order>, KiteError> {
        let resp = self.get("/orders", None).await?;
        Ok(serde_json::from_value(resp)?)
    }

    pub async fn order_history(&self, order_id: &str) -> Result<Vec<Order>, KiteError> {
        let resp = self.get(&format!("/orders/{}", order_id), None).await?;
        Ok(serde_json::from_value(resp)?)
    }

    pub async fn trades(&self) -> Result<Vec<Trade>, KiteError> {
        let resp = self.get("/trades", None).await?;
        Ok(serde_json::from_value(resp)?)
    }

    pub async fn order_trades(&self, order_id: &str) -> Result<Vec<Trade>, KiteError> {
        let resp = self.get(&format!("/orders/{}/trades", order_id), None).await?;
        Ok(serde_json::from_value(resp)?)
    }

    /// Place a new order.
    pub async fn place_order(
        &self,
        variety: &str,
        exchange: &str,
        tradingsymbol: &str,
        transaction_type: &str,
        quantity: u32,
        product: &str,
        order_type: &str,
        price: Option<f64>,
        validity: Option<&str>,
        trigger_price: Option<f64>,
    ) -> Result<Value, KiteError> {
        let mut params = json!({
            "variety": variety,
            "exchange": exchange,
            "tradingsymbol": tradingsymbol,
            "transaction_type": transaction_type,
            "quantity": quantity,
            "product": product,
            "order_type": order_type,
        });
        if let Some(p) = price {
            params["price"] = json!(p);
        }
        if let Some(v) = validity {
            params["validity"] = json!(v);
        }
        if let Some(tp) = trigger_price {
            params["trigger_price"] = json!(tp);
        }
        self.post("/orders/regular", params).await
    }

    /// Modify an existing order.
    pub async fn modify_order(
        &self,
        order_id: &str,
        variety: &str,
        params: Value,
    ) -> Result<Value, KiteError> {
        self.put(&format!("/orders/{}/{}", variety, order_id), params).await
    }

    /// Cancel an order.
    pub async fn cancel_order(&self, order_id: &str, variety: &str) -> Result<Value, KiteError> {
        self.delete(&format!("/orders/{}/{}", variety, order_id), None).await
    }

    // ── Quotes ──────────────────────────────────────────────────────────

    /// Get LTP for a list of instruments.
    pub async fn ltp(&self, instruments: &[&str]) -> Result<Value, KiteError> {
        self.get_with_params("/quote/ltp", instruments).await
    }

    /// Get market quote with depth for a list of instruments.
    pub async fn quote(&self, instruments: &[&str]) -> Result<Value, KiteError> {
        self.get_with_params("/quote", instruments).await
    }

    /// Get OHLC for a list of instruments.
    pub async fn ohlc(&self, instruments: &[&str]) -> Result<Value, KiteError> {
        self.get_with_params("/quote/ohlc", instruments).await
    }

    // ── Historical Data ─────────────────────────────────────────────────

    /// Fetch historical candle data for an instrument.
    pub async fn historical_data(
        &self,
        instrument_token: &str,
        from_date: &str,
        to_date: &str,
        interval: &str,
    ) -> Result<Vec<HistoricalCandle>, KiteError> {
        let mut url = format!(
            "{}/instruments/historical/{}/{}?from={}&to={}",
            BASE_URL, instrument_token, interval, from_date, to_date
        );
        // Add continuous flag
        url.push_str("&continuous=false");

        let resp = self.http.get(&url)
            .header("X-Kite-Version", KITE_VERSION)
            .header("Authorization", format!("token {}:{}", self.api_key, self.access_token))
            .send()
            .await?
            .json::<Value>()
            .await?;

        // Handle 403 session expiry
        if let Some(status) = resp.get("error_type").and_then(|v| v.as_str()) {
            if status == "TokenException" {
                return Err(KiteError::SessionExpired);
            }
        }

        // Kite returns {"data": {"candles": [[date, open, high, low, close, volume], ...]}}
        let candles_obj = resp.get("data");
        let candle_arrays = candles_obj
            .and_then(|d| d.get("candles"))
            .and_then(|v| v.as_array());

        match candle_arrays {
            Some(arr) => {
                let mut result = Vec::with_capacity(arr.len());
                for candle in arr {
                    if let Some(inner) = candle.as_array() {
                        if inner.len() >= 6 {
                            result.push(HistoricalCandle {
                                date: inner[0].as_str().unwrap_or("").to_string(),
                                open: inner[1].as_f64().unwrap_or(0.0),
                                high: inner[2].as_f64().unwrap_or(0.0),
                                low: inner[3].as_f64().unwrap_or(0.0),
                                close: inner[4].as_f64().unwrap_or(0.0),
                                volume: inner[5].as_u64().unwrap_or(0),
                            });
                        }
                    }
                }
                Ok(result)
            }
            None => Ok(vec![]),
        }
    }

    // ── GTT (Good Till Triggered) ──────────────────────────────────────

    /// Place a GTT trigger.
    pub async fn place_gtt(
        &self,
        trigger_type: &str,
        tradingsymbol: &str,
        exchange: &str,
        trigger_values: Vec<f64>,
        last_price: f64,
        orders: Vec<Value>,
    ) -> Result<Value, KiteError> {
        self.post("/gtt/triggers", json!({
            "trigger_type": trigger_type,
            "tradingsymbol": tradingsymbol,
            "exchange": exchange,
            "trigger_values": trigger_values,
            "last_price": last_price,
            "orders": orders,
        })).await
    }

    /// Modify an existing GTT trigger.
    pub async fn modify_gtt(
        &self,
        trigger_id: u64,
        trigger_type: &str,
        tradingsymbol: &str,
        exchange: &str,
        trigger_values: Vec<f64>,
        last_price: f64,
        orders: Vec<Value>,
    ) -> Result<Value, KiteError> {
        self.put(&format!("/gtt/triggers/{}", trigger_id), json!({
            "trigger_type": trigger_type,
            "tradingsymbol": tradingsymbol,
            "exchange": exchange,
            "trigger_values": trigger_values,
            "last_price": last_price,
            "orders": orders,
        })).await
    }

    /// Get a GTT trigger by ID.
    pub async fn get_gtt(&self, trigger_id: u64) -> Result<GttTrigger, KiteError> {
        let resp = self.get(&format!("/gtt/triggers/{}", trigger_id), None).await?;
        Ok(serde_json::from_value(resp)?)
    }

    /// Delete a GTT trigger.
    pub async fn delete_gtt(&self, trigger_id: u64) -> Result<Value, KiteError> {
        self.delete(&format!("/gtt/triggers/{}", trigger_id), None).await
    }

    // ── Instruments ─────────────────────────────────────────────────────

    /// Get all instruments for an exchange (returns CSV text).
    pub async fn instruments_raw(&self, exchange: Option<&str>) -> Result<String, KiteError> {
        let path = match exchange {
            Some(e) => format!("/instruments/{}", e),
            None => "/instruments".to_string(),
        };
        let url = format!("{}{}", BASE_URL, &path);
        let resp = self.http.get(&url)
            .header("X-Kite-Version", KITE_VERSION)
            .header("Authorization", format!("token {}:{}", self.api_key, self.access_token))
            .send()
            .await?
            .text()
            .await?;
        Ok(resp)
    }

    // ── HTTP Helpers ────────────────────────────────────────────────────

    async fn get(&self, path: &str, params: Option<Value>) -> Result<Value, KiteError> {
        let url = format!("{}{}", BASE_URL, path);
        let mut req = self.http.get(&url)
            .header("X-Kite-Version", KITE_VERSION)
            .header("Authorization", format!("token {}:{}", self.api_key, self.access_token));

        if let Some(p) = params {
            req = req.query(&p.as_object().map(|m| {
                m.iter().map(|(k, v)| (k.as_str(), v.as_str().unwrap_or(""))).collect::<Vec<_>>()
            }).unwrap_or_default());
        }

        let resp = req.send().await?;
        self.handle_response(resp).await
    }

    async fn get_with_params(&self, path: &str, instruments: &[&str]) -> Result<Value, KiteError> {
        let url = format!("{}{}", BASE_URL, path);
        let params: Vec<(&str, &str)> = instruments.iter().map(|i| ("i", *i)).collect();
        let resp = self.http.get(&url)
            .header("X-Kite-Version", KITE_VERSION)
            .header("Authorization", format!("token {}:{}", self.api_key, self.access_token))
            .query(&params)
            .send()
            .await?;
        self.handle_response(resp).await
    }

    async fn post(&self, path: &str, body: Value) -> Result<Value, KiteError> {
        let url = format!("{}{}", BASE_URL, path);
        let resp = self.http.post(&url)
            .header("X-Kite-Version", KITE_VERSION)
            .header("Authorization", format!("token {}:{}", self.api_key, self.access_token))
            .form(&body.as_object().unwrap_or(&serde_json::Map::new()))
            .send()
            .await?;
        self.handle_response(resp).await
    }

    async fn put(&self, path: &str, body: Value) -> Result<Value, KiteError> {
        let url = format!("{}{}", BASE_URL, path);
        let resp = self.http.put(&url)
            .header("X-Kite-Version", KITE_VERSION)
            .header("Authorization", format!("token {}:{}", self.api_key, self.access_token))
            .form(&body.as_object().unwrap_or(&serde_json::Map::new()))
            .send()
            .await?;
        self.handle_response(resp).await
    }

    async fn delete(&self, path: &str, body: Option<Value>) -> Result<Value, KiteError> {
        let url = format!("{}{}", BASE_URL, path);
        let mut req = self.http.delete(&url)
            .header("X-Kite-Version", KITE_VERSION)
            .header("Authorization", format!("token {}:{}", self.api_key, self.access_token));

        if let Some(b) = body {
            req = req.json(&b);
        }

        let resp = req.send().await?;
        self.handle_response(resp).await
    }

    async fn handle_response(&self, resp: reqwest::Response) -> Result<Value, KiteError> {
        let status = resp.status().as_u16();
        let text = resp.text().await.unwrap_or_default();

        // Parse body
        let body: Value = serde_json::from_str(&text).unwrap_or(json!({"data": text}));

        if status == 403 {
            let error_type = body.get("error_type").and_then(|v| v.as_str()).unwrap_or("");
            if error_type.contains("Token") || error_type.contains("TokenException") {
                return Err(KiteError::SessionExpired);
            }
        }

        if status >= 400 {
            let message = body.get("message").and_then(|v| v.as_str()).unwrap_or("Unknown error").to_string();
            return Err(KiteError::Api { status, message });
        }

        // Return the data field if present, else the full body
        Ok(body.get("data").cloned().unwrap_or(body))
    }
}
