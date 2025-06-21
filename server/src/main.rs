mod commands;

use std::env::current_exe;
use bytes::Bytes;
use dashmap::DashMap;
use log::{error, info};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::io::AsyncReadExt;
use tokio::net::{TcpListener, TcpStream};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    env_logger::init();

    info!("memcached-rust v.0.1.0");

    let listener = TcpListener::bind(("0.0.0.0", 11211)).await?;
    info!("server listening on 0.0.0.0:11211");

    let map: Arc<DashMap<String, (usize, Bytes)>> = Arc::new(DashMap::new());

    while let Ok((stream, _)) = listener.accept().await {
        let map = map.clone();

        tokio::spawn(async move { handle(stream, map).await });
    }

    Ok(())
}

async fn handle(mut stream: TcpStream, map: Arc<DashMap<String, (usize, Bytes)>>) -> anyhow::Result<()> {
    let mut buf = vec![0; 1024];

    loop {
        let bytes_read = stream.read(&mut buf).await?;
        if bytes_read == 0 {
            return Ok(());
        }
        println!("bytes read: {}, {:?}", bytes_read, &buf[..bytes_read]);
        buf.fill(0);
    }
}

