mod client;
use client::Client;
use log::info;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    env_logger::init();

    let mut client = Client::new("localhost", 11211).await?;
    let result = client.set("abhi", 0, 0, "rust").await?;
    info!("{:?}", result);
    let result = client.get("abhi").await?;
    info!("{:?}", result);
    Ok(())
}
