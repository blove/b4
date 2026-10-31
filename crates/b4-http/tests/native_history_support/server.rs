use super::WAIT;
use serde_json::Value;
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
            .expect("actual loopback bind remained finite")
            .unwrap();
        let address = listener.local_addr().unwrap();
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
    pub async fn request(
        &self,
        method: &str,
        path: &str,
        payload: Option<Value>,
        authenticated: bool,
    ) -> (u16, String) {
        tokio::time::timeout(WAIT,async {
            let mut stream=TcpStream::connect(self.address).await.unwrap();
            let body=payload.map(|v|v.to_string()).unwrap_or_default();
            let auth=if authenticated { "Authorization: Bearer native-history-fixture\r\n" } else { "" };
            let head=format!("{method} {path} HTTP/1.1\r\nHost: {}\r\n{auth}Content-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",self.address,body.len());
            stream.write_all(head.as_bytes()).await.unwrap();stream.write_all(body.as_bytes()).await.unwrap();
            let mut raw=Vec::new();stream.take(65_537).read_to_end(&mut raw).await.unwrap();assert!(raw.len()<=65_536);
            let raw=String::from_utf8(raw).unwrap();let status=raw.split_whitespace().nth(1).unwrap().parse().unwrap();(status,raw)
        }).await.expect("actual request terminal EOF remained finite")
    }
    pub async fn finish(self) {
        self.stop.send(()).unwrap();
        tokio::time::timeout(WAIT, self.task)
            .await
            .expect("normal real server join remained finite")
            .unwrap();
    }
}
