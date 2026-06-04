use thiserror::Error;

#[derive(Error, Debug)]
pub enum KiteError {
    #[error("HTTP error: {0}")]
    Http(String),

    #[error("Kite API error: {status} — {message}")]
    Api { status: u16, message: String },

    #[error("Session expired (403)")]
    SessionExpired,

    #[error("WebSocket error: {0}")]
    WebSocket(String),

    #[error("Serialization error: {0}")]
    Serialization(String),

    #[error("Connection error: {0}")]
    Connection(String),
}

impl From<reqwest::Error> for KiteError {
    fn from(e: reqwest::Error) -> Self {
        KiteError::Http(e.to_string())
    }
}

impl From<serde_json::Error> for KiteError {
    fn from(e: serde_json::Error) -> Self {
        KiteError::Serialization(e.to_string())
    }
}
