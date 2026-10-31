use super::{model::*, CREDENTIAL, WAIT};
use serde_json::Value;
use std::net::{Ipv4Addr, SocketAddr};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::oneshot,
};
const HTTP_BYTES: usize = 1_048_576;
const HEADER_BYTES: usize = 16_384;

pub struct Server {
    pub address: SocketAddr,
    stop: oneshot::Sender<()>,
    task: tokio::task::JoinHandle<std::io::Result<()>>,
}
impl Server {
    pub async fn start(router: axum::Router) -> Result<Self> {
        let listener = tokio::time::timeout(WAIT, TcpListener::bind((Ipv4Addr::LOCALHOST, 0)))
            .await
            .map_err(|_| Failure::Timeout)?
            .map_err(|_| Failure::Io)?;
        let address = listener.local_addr().map_err(|_| Failure::Io)?;
        let (stop, rx) = oneshot::channel();
        let task = tokio::spawn(async move {
            axum::serve(listener, router)
                .with_graceful_shutdown(async {
                    let _ = rx.await;
                })
                .await
        });
        Ok(Self {
            address,
            stop,
            task,
        })
    }
    pub async fn finish(self) -> Result<()> {
        self.stop.send(()).map_err(|_| Failure::Cleanup)?;
        tokio::time::timeout(WAIT, self.task)
            .await
            .map_err(|_| Failure::Timeout)?
            .map_err(|_| Failure::Cleanup)?
            .map_err(|_| Failure::Cleanup)
    }
}
pub struct Response {
    pub status: u16,
    pub body: Vec<u8>,
    pub raw: Vec<u8>,
}
impl Response {
    pub fn json(&self) -> Result<Value> {
        serde_json::from_slice(&self.body).map_err(|_| Failure::Protocol)
    }
    pub fn done(&self) -> Result<Value> {
        let text = std::str::from_utf8(&self.body)
            .map_err(|_| Failure::Protocol)?
            .replace("\r\n", "\n");
        let mut done = None;
        let mut count = 0usize;
        for block in text.split("\n\n") {
            if block.trim().is_empty() || block.starts_with(':') {
                continue;
            }
            count = count.checked_add(1).ok_or(Failure::Bound)?;
            if count > 128 {
                return Err(Failure::Bound);
            }
            let mut event = None;
            let mut data = None;
            for line in block.lines() {
                if let Some(s) = line.strip_prefix("event: ") {
                    if event.replace(s).is_some() {
                        return Err(Failure::Protocol);
                    }
                } else if let Some(s) = line.strip_prefix("data: ") {
                    if data.replace(s).is_some() {
                        return Err(Failure::Protocol);
                    }
                } else if !line.starts_with(':') {
                    return Err(Failure::Protocol);
                }
            }
            if event == Some("error") {
                return Err(Failure::Semantics);
            }
            if event == Some("done") {
                let value = serde_json::from_str(data.ok_or(Failure::Protocol)?)
                    .map_err(|_| Failure::Protocol)?;
                if done.replace(value).is_some() {
                    return Err(Failure::Protocol);
                }
            }
        }
        done.ok_or(Failure::Semantics)
    }
}
pub async fn open(port: u16, method: &str, path: &str, body: &str) -> Result<TcpStream> {
    if !matches!(method, "GET" | "POST")
        || !path.starts_with('/')
        || path.bytes().any(|b| matches!(b, b'\r' | b'\n'))
        || body.len() > 65_536
    {
        return Err(Failure::Protocol);
    }
    tokio::time::timeout(WAIT,async{
        let mut stream=TcpStream::connect((Ipv4Addr::LOCALHOST,port)).await.map_err(|_|Failure::Io)?;
        let header=format!("{method} {path} HTTP/1.1\r\nHost: 127.0.0.1:{port}\r\nAuthorization: {CREDENTIAL}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",body.len());
        stream.write_all(header.as_bytes()).await.map_err(|_|Failure::Io)?;stream.write_all(body.as_bytes()).await.map_err(|_|Failure::Io)?;Ok(stream)
    }).await.map_err(|_|Failure::Timeout)?
}
pub async fn exchange(port: u16, method: &str, path: &str, body: &str) -> Result<Response> {
    let stream = open(port, method, path, body).await?;
    let raw = read_eof(stream, false).await?;
    decode(raw)
}
/// Actual crash cut: preserve bounded bytes and EOF/reset without claiming that
/// killed chunked framing has a final zero chunk, complete body, or done event.
pub async fn crash_eof(stream: TcpStream) -> Result<Vec<u8>> {
    read_eof(stream, true).await
}
async fn read_eof(mut stream: TcpStream, allow_reset: bool) -> Result<Vec<u8>> {
    tokio::time::timeout(WAIT, async {
        let mut raw = Vec::new();
        let mut chunk = [0u8; 4096];
        loop {
            let n = match stream.read(&mut chunk).await {
                Ok(n) => n,
                Err(e)
                    if allow_reset
                        && matches!(
                            e.kind(),
                            std::io::ErrorKind::ConnectionReset | std::io::ErrorKind::UnexpectedEof
                        ) =>
                {
                    break
                }
                Err(_) => return Err(Failure::Io),
            };
            if n == 0 {
                break;
            }
            if raw
                .len()
                .checked_add(n)
                .is_none_or(|size| size > HTTP_BYTES)
            {
                return Err(Failure::Bound);
            }
            raw.extend_from_slice(&chunk[..n]);
        }
        Ok(raw)
    })
    .await
    .map_err(|_| Failure::Timeout)?
}
fn decode(raw: Vec<u8>) -> Result<Response> {
    let split = raw
        .windows(4)
        .position(|p| p == b"\r\n\r\n")
        .ok_or(Failure::Protocol)?;
    if split > HEADER_BYTES {
        return Err(Failure::Bound);
    }
    let headers = std::str::from_utf8(&raw[..split]).map_err(|_| Failure::Protocol)?;
    let mut lines = headers.split("\r\n");
    let first = lines.next().ok_or(Failure::Protocol)?;
    let mut parts = first.splitn(3, ' ');
    if parts.next() != Some("HTTP/1.1") {
        return Err(Failure::Protocol);
    }
    let status_text = parts.next().ok_or(Failure::Protocol)?;
    if status_text.len() != 3 || !status_text.bytes().all(|b| b.is_ascii_digit()) {
        return Err(Failure::Protocol);
    }
    let status = status_text.parse::<u16>().map_err(|_| Failure::Protocol)?;
    let mut length = None;
    let mut chunked = false;
    for line in lines {
        let (name, value) = line.split_once(':').ok_or(Failure::Protocol)?;
        let value = value.trim();
        if name.eq_ignore_ascii_case("content-length") {
            let n = value.parse::<usize>().map_err(|_| Failure::Protocol)?;
            if length.replace(n).is_some() {
                return Err(Failure::Protocol);
            }
        }
        if name.eq_ignore_ascii_case("transfer-encoding") {
            if chunked || !value.eq_ignore_ascii_case("chunked") {
                return Err(Failure::Protocol);
            }
            chunked = true;
        }
    }
    let encoded = &raw[split + 4..];
    let body = if chunked {
        if length.is_some() {
            return Err(Failure::Protocol);
        }
        let mut at = 0usize;
        let mut body = Vec::new();
        let mut chunks = 0usize;
        loop {
            chunks = chunks.checked_add(1).ok_or(Failure::Bound)?;
            if chunks > 4096 {
                return Err(Failure::Bound);
            }
            let end = encoded[at..]
                .windows(2)
                .position(|p| p == b"\r\n")
                .ok_or(Failure::Protocol)?;
            if end > 32 {
                return Err(Failure::Bound);
            }
            let text =
                std::str::from_utf8(&encoded[at..at + end]).map_err(|_| Failure::Protocol)?;
            // Selected Axum fixture framing has plain hex sizes, no extensions.
            if text.is_empty() || !text.bytes().all(|b| b.is_ascii_hexdigit()) {
                return Err(Failure::Protocol);
            }
            let size = usize::from_str_radix(text, 16).map_err(|_| Failure::Protocol)?;
            at = at.checked_add(end + 2).ok_or(Failure::Bound)?;
            if size == 0 {
                if encoded.get(at..) != Some(b"\r\n".as_slice()) {
                    return Err(Failure::Protocol);
                }
                break;
            }
            let limit = at.checked_add(size).ok_or(Failure::Bound)?;
            if encoded.get(limit..limit.checked_add(2).ok_or(Failure::Bound)?)
                != Some(b"\r\n".as_slice())
            {
                return Err(Failure::Protocol);
            }
            if body.len().checked_add(size).is_none_or(|n| n > HTTP_BYTES) {
                return Err(Failure::Bound);
            }
            body.extend_from_slice(encoded.get(at..limit).ok_or(Failure::Protocol)?);
            at = limit + 2;
        }
        body
    } else {
        let n = length.ok_or(Failure::Protocol)?;
        if n != encoded.len() {
            return Err(Failure::Protocol);
        }
        encoded.to_vec()
    };
    Ok(Response { status, body, raw })
}
