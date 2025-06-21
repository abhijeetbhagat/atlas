mod commands;

use crate::commands::parse_input;
use bytes::Bytes;
use dashmap::DashMap;
use log::{error, info};
use std::env::current_exe;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use clap::Parser;

#[derive(Parser, Debug)]
struct Args {
    #[arg(long, default_value = "127.0.0.1")]
    addr: String,

    #[arg(long, default_value = "11211")]
    port: u16,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    env_logger::init();
    
    let args = Args::parse();

    info!("memcached-rust v.0.1.0");

    let listener = TcpListener::bind((args.addr.clone(), args.port)).await?;
    info!("{}", format!("server listening on {}:{}", args.addr, args.port));

    let map: Arc<DashMap<String, (u128, Bytes)>> = Arc::new(DashMap::new());

    while let Ok((stream, _)) = listener.accept().await {
        let map = map.clone();

        tokio::spawn(async move { handle(stream, map).await });
    }

    Ok(())
}

async fn handle(
    mut stream: TcpStream,
    map: Arc<DashMap<String, (u128, Bytes)>>,
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
