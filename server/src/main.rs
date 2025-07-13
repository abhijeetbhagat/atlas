mod commands;

use crate::commands::parse_input;
use bytes::Bytes;
use clap::Parser;
use core::cache::LruCache;
use log::info;
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

#[derive(Parser, Debug)]
struct Args {
    #[arg(long, default_value = "127.0.0.1")]
    addr: String,

    #[arg(long, default_value = "11211")]
    port: u16,

    #[arg(long, default_value = "100")]
    cache_limit: usize,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    env_logger::init();

    let args = Args::parse();

    info!("memcached-rust v{}", env!("CARGO_PKG_VERSION"));

    let listener = TcpListener::bind((args.addr.clone(), args.port)).await?;
    info!(
        "{}",
        format!("server listening on {}:{}", args.addr, args.port)
    );

    let map: Arc<LruCache<String, (u128, Bytes)>> = Arc::new(LruCache::new(args.cache_limit));

    while let Ok((stream, _)) = listener.accept().await {
        let map = map.clone();

        tokio::spawn(async move { handle(stream, map).await });
    }

    Ok(())
}

async fn handle(
    mut stream: TcpStream,
    map: Arc<LruCache<String, (u128, Bytes)>>,
) -> anyhow::Result<()> {
    let mut buf = vec![0; 1024];

    loop {
        let bytes_read = stream.read(&mut buf).await?;
        if bytes_read == 0 {
            return Ok(());
        }
        println!("bytes read: {}, {:?}", bytes_read, &buf[..bytes_read]);
        let result = parse_input(str::from_utf8(&buf[..bytes_read])?).handle(map.clone())?;
        stream.write(&result).await?;
        stream.flush().await?;

        buf.fill(0);
    }
}
