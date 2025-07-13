use core::cluster_client::ClusterClient;
// use client::Client;
use log::info;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    env_logger::init();

    // let mut client = Client::new("localhost", 11211).await?;
    // let result = client.set("abhi", 0, 0, "rust").await?;
    // info!("{:?}", result);
    // let result = client.get("abhi").await?;
    // info!("{:?}", result);

    let mut client = ClusterClient::new(&[
        ("127.0.0.1", 11211),
        ("127.0.0.2", 11211),
        ("127.0.0.3", 11211),
    ]);
    client.set("abhi", 0, 200, "rust").await?;
    client.set("lilb", 0, 200, ".net").await?;
    client.set("pads", 0, 200, "react").await?;
    client.set("nisc", 0, 200, "java").await?;
    client.set("ashu", 0, 200, "java").await?;

    info!("{}", client.get("abhi").await?);
    info!("{}", client.get("lilb").await?);
    info!("{}", client.get("pads").await?);
    info!("{}", client.get("nisc").await?);
    info!("{}", client.get("ashu").await?);

    Ok(())
}
