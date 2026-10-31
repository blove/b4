use b4_http::HttpHost;
use b4_runtime::Store;
use std::{net::SocketAddr, time::Duration};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::oneshot,
};

pub struct Server {
    pub address: SocketAddr,
    stop: oneshot::Sender<()>,
    task: tokio::task::JoinHandle<()>,
}
impl Server {
    pub async fn start<S: Store + 'static>(host: &HttpHost<S>) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("authorized local loopback");
        let address = listener.local_addr().unwrap();
        let router = host.router();
        let (stop, receive) = oneshot::channel();
        let task = tokio::spawn(async move {
            axum::serve(listener, router)
                .with_graceful_shutdown(async {
                    let _ = receive.await;
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
    pub async fn stop(self) {
        self.stop.send(()).unwrap();
        tokio::time::timeout(Duration::from_secs(10), self.task)
            .await
            .unwrap()
            .unwrap();
    }
}
pub async fn open(address: SocketAddr, method: &str, path: &str, body: &str) -> TcpStream {
    open_with_timeout(address, method, path, body, Duration::from_secs(10))
        .await
        .unwrap()
}
pub async fn open_with_timeout(
    address: SocketAddr,
    method: &str,
    path: &str,
    body: &str,
    timeout: Duration,
) -> Result<TcpStream, tokio::time::error::Elapsed> {
    tokio::time::timeout(timeout, async {
        let mut stream = TcpStream::connect(address).await.unwrap();
        stream.write_all(format!("{method} {path} HTTP/1.1\r\nHost: {address}\r\nAuthorization: Bearer cooperative-owner\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",body.len()).as_bytes()).await.unwrap();
        stream
    }).await
}
pub struct Response {
    pub status: u16,
    pub body: String,
}
pub async fn finish(mut stream: TcpStream, mut bytes: Vec<u8>) -> Response {
    tokio::time::timeout(Duration::from_secs(10), stream.read_to_end(&mut bytes))
        .await
        .unwrap()
        .unwrap();
    let split = bytes.windows(4).position(|b| b == b"\r\n\r\n").unwrap();
    let head = std::str::from_utf8(&bytes[..split]).unwrap();
    let mut body = bytes[split + 4..].to_vec();
    if head
        .to_ascii_lowercase()
        .contains("transfer-encoding: chunked")
    {
        let mut rest = body.as_slice();
        let mut decoded = Vec::new();
        loop {
            let end = rest.windows(2).position(|x| x == b"\r\n").unwrap();
            let size = usize::from_str_radix(
                std::str::from_utf8(&rest[..end])
                    .unwrap()
                    .split(';')
                    .next()
                    .unwrap(),
                16,
            )
            .unwrap();
            rest = &rest[end + 2..];
            if size == 0 {
                assert_eq!(rest, b"\r\n");
                break;
            }
            decoded.extend_from_slice(&rest[..size]);
            assert_eq!(&rest[size..size + 2], b"\r\n");
            rest = &rest[size + 2..];
        }
        body = decoded;
    }
    Response {
        status: head.split_whitespace().nth(1).unwrap().parse().unwrap(),
        body: String::from_utf8(body).unwrap(),
    }
}
pub async fn exchange(address: SocketAddr, method: &str, path: &str, body: &str) -> Response {
    finish(open(address, method, path, body).await, Vec::new()).await
}
pub async fn prefix(stream: &mut TcpStream, marker: &[u8]) -> Vec<u8> {
    let mut bytes = Vec::new();
    tokio::time::timeout(Duration::from_secs(10), async {
        while !bytes.windows(marker.len()).any(|x| x == marker) {
            let mut part = [0; 1024];
            let n = stream.read(&mut part).await.unwrap();
            assert_ne!(n, 0);
            bytes.extend_from_slice(&part[..n]);
        }
    })
    .await
    .unwrap();
    bytes
}
