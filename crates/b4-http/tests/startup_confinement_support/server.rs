use std::time::Duration;
const WAIT: Duration = Duration::from_secs(15);
use std::net::SocketAddr;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::oneshot,
};

pub struct Server {
    address: SocketAddr,
    stop: oneshot::Sender<()>,
    task: tokio::task::JoinHandle<()>,
}
impl Server {
    pub async fn start(router: axum::Router) -> Self {
        let listener = tokio::time::timeout(WAIT, TcpListener::bind("127.0.0.1:0"))
            .await
            .expect("loopback binding remained finite")
            .unwrap();
        let address = listener.local_addr().unwrap();
        let (stop, receiver) = oneshot::channel();
        let task = tokio::spawn(async move {
            axum::serve(listener, router)
                .with_graceful_shutdown(async {
                    let _ = receiver.await;
                })
                .await
                .unwrap();
        });
        Self {
            address,
            stop,
            task,
        }
    }

    pub async fn get(&self, path: &str) -> (u16, String) {
        self.request("GET", path, None, &[], 0).await
    }
    pub async fn request(
        &self,
        method: &str,
        path: &str,
        authorization: Option<&str>,
        body: &[u8],
        declared_length: usize,
    ) -> (u16, String) {
        tokio::time::timeout(WAIT, async {
            let mut socket = TcpStream::connect(self.address).await.unwrap();
            let authorization=authorization.map(|value|format!("Authorization: {value}\r\n")).unwrap_or_default();
            socket
                .write_all(
                    format!(
                        "{method} {path} HTTP/1.1\r\nHost: {}\r\n{authorization}Content-Type: application/json\r\nContent-Length: {declared_length}\r\nConnection: close\r\n\r\n",
                        self.address
                    )
                    .as_bytes(),
                )
                .await
                .unwrap();
            socket.write_all(body).await.unwrap();
            let mut raw = Vec::new();
            socket.take(65_537).read_to_end(&mut raw).await.unwrap();
            assert!(raw.len() <= 65_536, "response exceeded fixed fixture cap");
            let raw = String::from_utf8(raw).unwrap();
            let status = raw.split_whitespace().nth(1).unwrap().parse().unwrap();
            (status, raw)
        })
        .await
        .expect("connect, write and complete response remained finite")
    }

    pub async fn finish(self) {
        self.stop.send(()).unwrap();
        tokio::time::timeout(WAIT, self.task)
            .await
            .expect("normal server retirement remained finite")
            .unwrap();
    }
}
