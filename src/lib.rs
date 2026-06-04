/// Modern async Rust client for Kite Connect API.
///
/// # Quick Start
/// ```rust,no_run
/// use kite_connect::KiteClient;
///
/// #[tokio::main]
/// async fn main() {
///     let client = KiteClient::new("api_key", "access_token");
///     let profile = client.profile().await.unwrap();
///     println!("User: {:?}", profile);
/// }
/// ```

pub mod connect;
pub mod ticker;
pub mod models;
pub mod error;

pub use connect::KiteClient;
pub use error::KiteError;
