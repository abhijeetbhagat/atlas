use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

pub struct Client {
    stream: TcpStream,
}

impl Client {
    pub async fn new(server: &str, port: u16) -> anyhow::Result<Self> {
        Ok(Self {
            stream: TcpStream::connect(format!("{}:{}", server, port)).await?,
        })
    }

    pub async fn set(
        &mut self,
        key: &str,
        flags: u32,
        exp_time: u32,
        value: &str,
    ) -> anyhow::Result<String> {
        let _size = self
            .stream
            .write(format!("set {} {} {} {}", key, flags, exp_time, value).as_bytes())
            .await?;
        self.stream.flush().await?;
        let mut buf = vec![0; 1024];
        let size = self.stream.read(&mut buf).await?;
        Ok(String::from_utf8_lossy(&buf[..size]).into())
    }

    pub async fn get(&mut self, key: &str) -> anyhow::Result<String> {
        let _size = self.stream.write(format!("get {}", key).as_bytes()).await?;
        self.stream.flush().await?;
        let mut buf = vec![0; 1024];
        let size = self.stream.read(&mut buf).await?;
        Ok(String::from_utf8_lossy(&buf[..size]).into())
    }
}
