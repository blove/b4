use b4_http::HttpHost;
use b4_runtime::Store;
use std::{collections::BTreeMap, net::SocketAddr, time::Duration};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::oneshot,
    task::JoinHandle,
};

pub struct Server {
    pub address: SocketAddr,
    stop: Option<oneshot::Sender<()>>,
    task: Option<JoinHandle<()>>,
}
impl Server {
    pub async fn start<S: Store + 'static>(host: &HttpHost<S>) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("actual diagnostics TCP fixture requires loopback binding");
        let address = listener.local_addr().unwrap();
        let (stop, stopped) = oneshot::channel();
        let router = host.router();
        let task = tokio::spawn(async move {
            axum::serve(listener, router)
                .with_graceful_shutdown(async {
                    let _ = stopped.await;
                })
                .await
                .unwrap();
        });
        Self {
            address,
            stop: Some(stop),
            task: Some(task),
        }
    }
    pub async fn stop(mut self) {
        self.stop
            .take()
            .unwrap()
            .send(())
            .expect("actual fixture listener must still be live before its retirement signal");
        let mut task = self.task.take().unwrap();
        match tokio::time::timeout(Duration::from_secs(5), &mut task).await {
            Ok(result) => result.expect("actual listener task must complete normally"),
            Err(_) => {
                task.abort();
                let _ = task.await;
                panic!("actual listener retirement must complete within fixture bound");
            }
        }
    }
}
impl Drop for Server {
    fn drop(&mut self) {
        if let Some(task) = &self.task {
            // Failure-only cleanup never counts as positive retirement evidence.
            task.abort();
        }
    }
}

pub async fn open(
    address: SocketAddr,
    method: &str,
    path: &str,
    authorization: Option<&str>,
    body: &str,
) -> TcpStream {
    let mut stream = tokio::time::timeout(Duration::from_secs(5), TcpStream::connect(address))
        .await
        .unwrap()
        .unwrap();
    let authorization =
        authorization.map_or_else(String::new, |value| format!("Authorization: {value}\r\n"));
    let bytes=format!("{method} {path} HTTP/1.1\r\nHost: {address}\r\n{authorization}x-b4-request-id: private-tcp-caller-reference\r\nCookie: private-tcp-cookie\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",body.len());
    tokio::time::timeout(Duration::from_secs(5), stream.write_all(bytes.as_bytes()))
        .await
        .unwrap()
        .unwrap();
    stream
}

pub async fn prefix(stream: &mut TcpStream, marker: &[u8]) -> Vec<u8> {
    let mut bytes = Vec::new();
    tokio::time::timeout(Duration::from_secs(5), async {
        while !bytes.windows(marker.len()).any(|part| part == marker) {
            let mut part = [0; 512];
            let read = stream.read(&mut part).await.unwrap();
            assert!(
                read != 0,
                "actual TCP response must begin before stream closes"
            );
            bytes.extend_from_slice(&part[..read]);
            assert!(
                bytes.len() <= 262_144,
                "actual TCP prefix must remain bounded"
            );
        }
    })
    .await
    .unwrap();
    bytes
}

pub struct WireResponse {
    pub status: u16,
    pub headers: BTreeMap<String, String>,
    pub body: Vec<u8>,
}
pub fn headers(bytes: &[u8]) -> (u16, BTreeMap<String, String>) {
    let split = bytes
        .windows(4)
        .position(|part| part == b"\r\n\r\n")
        .unwrap();
    let text = std::str::from_utf8(&bytes[..split]).unwrap();
    let status = text
        .lines()
        .next()
        .unwrap()
        .split_whitespace()
        .nth(1)
        .unwrap()
        .parse()
        .unwrap();
    let mut headers = BTreeMap::new();
    for line in text.lines().skip(1) {
        let (name, value) = line.split_once(':').unwrap();
        assert!(
            headers
                .insert(name.to_ascii_lowercase(), value.trim().to_owned())
                .is_none(),
            "actual TCP response must not duplicate request reference headers"
        );
    }
    (status, headers)
}

pub async fn finish(stream: TcpStream, mut bytes: Vec<u8>) -> WireResponse {
    tokio::time::timeout(
        Duration::from_secs(5),
        stream.take(262_145).read_to_end(&mut bytes),
    )
    .await
    .unwrap()
    .unwrap();
    assert!(bytes.len() <= 262_144);
    let split = bytes
        .windows(4)
        .position(|part| part == b"\r\n\r\n")
        .unwrap();
    let (status, headers) = headers(&bytes);
    let mut body = bytes[split + 4..].to_vec();
    if headers
        .get("transfer-encoding")
        .is_some_and(|value| value.eq_ignore_ascii_case("chunked"))
    {
        let mut rest = body.as_slice();
        let mut decoded = Vec::new();
        loop {
            let end = rest.windows(2).position(|part| part == b"\r\n").unwrap();
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
                break;
            }
            assert!(size <= rest.len().saturating_sub(2));
            decoded.extend_from_slice(&rest[..size]);
            assert!(&rest[size..size + 2] == b"\r\n");
            rest = &rest[size + 2..];
        }
        body = decoded;
    }
    WireResponse {
        status,
        headers,
        body,
    }
}
pub async fn exchange(
    address: SocketAddr,
    method: &str,
    path: &str,
    authorization: Option<&str>,
    body: &str,
) -> WireResponse {
    finish(
        open(address, method, path, authorization, body).await,
        Vec::new(),
    )
    .await
}
