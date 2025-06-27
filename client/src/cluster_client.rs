use log::info;
use murmur3::murmur3_32;
use std::hash::Hash;
use std::io::Cursor;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

pub struct ClusterClient {
    streams: Vec<Option<TcpStream>>,
    cluster: Vec<(String, u16)>,
}

impl ClusterClient {
    pub fn new(cluster: &[(&str, u16)]) -> Self {
        Self {
            streams: vec![None, None, None],
            cluster: cluster.iter().map(|&(k, v)| (k.to_string(), v)).collect(),
        }
    }

    pub async fn set(
        &mut self,
        key: &str,
        flags: u32,
        exp_time: u32,
        value: &str,
    ) -> anyhow::Result<String> {
        let stream = self.get_stream(key).await?;
        info!("storing key in {:?}", stream.peer_addr());

        let _ = stream
            .write(format!("set {} {} {} {}", key, flags, exp_time, value).as_bytes())
            .await?;
        stream.flush().await?;

        let mut buf = vec![0; 1024];
        let size = stream.read(&mut buf).await?;
        Ok(String::from_utf8_lossy(&buf[..size]).into())
    }

    pub async fn get(&mut self, key: &str) -> anyhow::Result<String> {
        let stream = self.get_stream(key).await?;

        let _ = stream.write(format!("get {}", key).as_bytes()).await?;
        stream.flush().await?;

        let mut buf = vec![0; 1024];
        let size = stream.read(&mut buf).await?;
        Ok(String::from_utf8_lossy(&buf[..size]).into())
    }

    async fn get_stream(&mut self, key: &str) -> anyhow::Result<&mut TcpStream> {
        let hash = murmur3_32(&mut Cursor::new(key), 0)? as usize;
        let server_index = hash % self.cluster.len();
        if self.streams[server_index].is_none() {
            self.streams[server_index] = Some(
                TcpStream::connect(format!(
                    "{}:{}",
                    self.cluster[server_index].0, self.cluster[server_index].1
                ))
                .await?,
            );
        }

        Ok(self.streams[server_index].as_mut().unwrap())
    }
}
