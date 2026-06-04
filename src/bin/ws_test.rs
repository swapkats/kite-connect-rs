use tokio_tungstenite::connect_async;
use futures_util::StreamExt;

#[tokio::main]
async fn main() {
    let client = reqwest::Client::new();
    let resp: serde_json::Value = client
        .get("https://trade-worker.c22.space/api/kite-session-data")
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let token = resp["data"]["access_token"].as_str().unwrap();

    let dotenv = std::fs::read_to_string("/home/charlie/trading/.env").unwrap();
    let api_key: String = dotenv
        .lines()
        .find(|l| l.starts_with("KITE_API_KEY="))
        .map(|l| l.trim_start_matches("KITE_API_KEY=").trim().to_string())
        .unwrap();

    let url = format!(
        "wss://ws.kite.trade?api_key={}&access_token={}",
        api_key, token
    );
    println!(
        "Connecting to wss://ws.kite.trade?api_key={}...",
        &api_key[..8]
    );

    match connect_async(&url).await {
        Ok((ws_stream, response)) => {
            println!("Connected! Status: {:?}", response.status());
            let (_, mut read) = ws_stream.split();
            let mut count = 0;
            while let Some(msg) = read.next().await {
                match msg {
                    Ok(tokio_tungstenite::tungstenite::Message::Binary(data)) => {
                        count += 1;
                        println!("Binary #{}: {} bytes", count, data.len());
                        if count >= 3 {
                            break;
                        }
                    }
                    Ok(tokio_tungstenite::tungstenite::Message::Text(text)) => {
                        let preview = if text.len() > 80 {
                            &text[..80]
                        } else {
                            &text
                        };
                        println!("Text: {}", preview);
                    }
                    Ok(tokio_tungstenite::tungstenite::Message::Close(_)) => {
                        println!("Closed by server");
                        break;
                    }
                    Err(e) => {
                        println!("Error: {}", e);
                        break;
                    }
                    _ => {}
                }
            }
            println!("Done. {} messages.", count);
        }
        Err(e) => println!("FAILED: {:?}", e),
    }
}
