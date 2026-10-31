//! Provider-free native transport fixtures shared by the TCP and crash tests.
#![allow(dead_code)]

use axum::{http::HeaderMap, routing::post};
use b4_contract::*;
use b4_http::*;
use b4_runtime::{approval::*, *};
use serde_json::{json, Value};
use std::{
    collections::BTreeMap,
    net::SocketAddr,
    path::PathBuf,
    sync::{Arc, Condvar, Mutex},
    time::Duration,
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
};

pub fn principal() -> Principal {
    Principal {
        application: "native-test".into(),
        tenant: "tenant".into(),
        principal: "owner".into(),
    }
}
pub struct Auth;
impl Authenticator for Auth {
    fn authenticate(&self, headers: &HeaderMap) -> Result<Principal, AuthenticationError> {
        if headers.get("authorization").and_then(|v| v.to_str().ok())
            != Some("Bearer fixture-owner")
        {
            return Err(AuthenticationError);
        }
        Ok(principal())
    }
}
pub struct Owner;
impl ResourcePolicy for Owner {
    fn authorize(&self, p: &Principal, c: &ResourceContext) -> bool {
        c.origin.as_ref().is_none_or(|o| {
            o.application == p.application && o.tenant == p.tenant && o.principal == p.principal
        })
    }
}
pub struct Directory(pub PathBuf);
impl Directory {
    pub fn new() -> Self {
        Self(std::env::temp_dir().join(format!("b4-native-{}", uuid::Uuid::new_v4())))
    }
}
impl Drop for Directory {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}
#[derive(Default)]
pub struct Gate {
    state: Mutex<(bool, bool)>,
    changed: Condvar,
}
impl Gate {
    pub fn hold(&self) {
        let mut s = self.state.lock().unwrap();
        s.0 = true;
        self.changed.notify_all();
        while !s.1 {
            let (next, timeout) = self
                .changed
                .wait_timeout(s, Duration::from_secs(10))
                .unwrap();
            s = next;
            assert!(!timeout.timed_out(), "native fixture gate was not released");
        }
    }
    pub async fn entered(&self) {
        until(|| self.state.lock().unwrap().0).await;
    }
    pub fn release(&self) {
        self.state.lock().unwrap().1 = true;
        self.changed.notify_all();
    }
}
pub struct Release(pub Arc<Gate>);
impl Drop for Release {
    fn drop(&mut self) {
        self.0.release();
    }
}

pub fn definition(
    route: &str,
    revision: &str,
    tasks: &[(&str, bool, RecoveryPolicy)],
) -> Definition {
    Definition {
        route: RouteKey::new(route).unwrap(),
        revision: ExecutionRevision::new(revision).unwrap(),
        max_attempts: 4,
        batches: vec![tasks
            .iter()
            .map(|(id, required, recovery)| TaskSpec {
                id: TaskId::new(*id).unwrap(),
                effect_id: EffectId::new(*id).unwrap(),
                requires_approval: *required,
                recovery: *recovery,
            })
            .collect()],
    }
}
pub fn application(
    definition: Definition,
    callback: Arc<dyn NativeEffect>,
    validator: Option<Arc<dyn InputValidator>>,
) -> Arc<NativeApplication> {
    let effects = definition
        .batches
        .iter()
        .flatten()
        .map(|t| {
            (
                t.effect_id.clone(),
                EffectBinding {
                    callback: callback.clone(),
                    output_kind: OutputKind::Value,
                },
            )
        })
        .collect();
    Arc::new(
        NativeApplication::new(
            ApplicationIdentity {
                application: "native-test".into(),
                revision: "application-v1".into(),
            },
            vec![RouteRegistration {
                definition,
                effects,
                input_validator: validator,
            }],
        )
        .unwrap(),
    )
}
pub fn config(
    application: Arc<NativeApplication>,
    durable: bool,
    capacity: usize,
) -> NativeHostConfig {
    let prompts = application
        .definitions()
        .filter_map(|d| {
            let tasks: BTreeMap<_, _> = d
                .batches
                .iter()
                .flatten()
                .filter(|t| t.requires_approval)
                .map(|t| (t.id.clone(), json!({"question":"native approval"})))
                .collect();
            (!tasks.is_empty()).then(|| (d.route.clone(), tasks))
        })
        .collect();
    let mut approval = ApprovalHostConfig::new("policy-v1".into(), prompts);
    if !durable {
        approval.service_config = ApprovalServiceConfig::VolatileFixture;
    }
    let mut c = NativeHostConfig::new(application, approval);
    c.limits.max_active_runs = capacity;
    c.limits.heartbeat_ms = 10;
    c
}
pub fn snapshot<S: Store>(store: &S, id: &str) -> Snapshot {
    store
        .thread_view(&ThreadId::new(id).unwrap())
        .unwrap()
        .unwrap()
        .execution
        .unwrap()
        .snapshot
}
pub async fn until(mut condition: impl FnMut() -> bool) {
    tokio::time::timeout(Duration::from_secs(5), async {
        while !condition() {
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
    })
    .await
    .expect("native fixture did not reach its observable boundary");
}
pub struct Server {
    pub address: SocketAddr,
    task: tokio::task::JoinHandle<()>,
}
impl Server {
    pub async fn start<S: Store + 'static>(
        host: &HttpHost<S>,
        recovery: Option<RecoveryTarget>,
    ) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("native TCP fixtures require loopback bind permission");
        let address = listener.local_addr().unwrap();
        let mut router = host.router();
        if let Some(target) = recovery {
            // A fixture-only HTTP adapter for the trusted embedding operation;
            // the production router has no recovery route or client target DTO.
            let host = host.clone();
            router = router.route(
                "/fixture/recover",
                post(move || {
                    let host = host.clone();
                    let target = target.clone();
                    async move { host.recover_execution(principal(), target).await }
                }),
            );
        }
        Self {
            address,
            task: tokio::spawn(async move { axum::serve(listener, router).await.unwrap() }),
        }
    }
    pub async fn stop(self) {
        self.task.abort();
        let _ = self.task.await;
    }
}
pub async fn open(address: SocketAddr, method: &str, path: &str, body: &str) -> TcpStream {
    let mut s = TcpStream::connect(address).await.unwrap();
    s.write_all(format!("{method} {path} HTTP/1.1\r\nHost: {address}\r\nAuthorization: Bearer fixture-owner\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}", body.len()).as_bytes()).await.unwrap();
    s
}
pub struct Response {
    pub status: u16,
    pub body: String,
}
impl Response {
    pub fn json(&self) -> Value {
        serde_json::from_str(&self.body).expect("native response must be JSON")
    }
}
pub async fn prefix(s: &mut TcpStream, marker: &[u8]) -> Vec<u8> {
    let mut bytes = vec![];
    tokio::time::timeout(Duration::from_secs(5), async {
        while !bytes.windows(marker.len()).any(|v| v == marker) {
            let mut part = [0; 1024];
            let count = s.read(&mut part).await.unwrap();
            assert!(count != 0, "native stream ended before expected marker");
            bytes.extend_from_slice(&part[..count]);
        }
    })
    .await
    .unwrap();
    bytes
}
pub async fn finish(mut s: TcpStream, mut bytes: Vec<u8>) -> Response {
    tokio::time::timeout(Duration::from_secs(5), s.read_to_end(&mut bytes))
        .await
        .unwrap()
        .unwrap();
    let split = bytes.windows(4).position(|v| v == b"\r\n\r\n").unwrap();
    let headers = std::str::from_utf8(&bytes[..split]).unwrap();
    let mut body = bytes[split + 4..].to_vec();
    if headers
        .to_ascii_lowercase()
        .contains("transfer-encoding: chunked")
    {
        let mut remaining = body.as_slice();
        let mut decoded = vec![];
        loop {
            let end = remaining.windows(2).position(|v| v == b"\r\n").unwrap();
            let size = usize::from_str_radix(
                std::str::from_utf8(&remaining[..end])
                    .unwrap()
                    .split(';')
                    .next()
                    .unwrap(),
                16,
            )
            .unwrap();
            remaining = &remaining[end + 2..];
            if size == 0 {
                break;
            }
            decoded.extend_from_slice(&remaining[..size]);
            assert_eq!(&remaining[size..size + 2], b"\r\n");
            remaining = &remaining[size + 2..];
        }
        body = decoded;
    }
    Response {
        status: headers.split_whitespace().nth(1).unwrap().parse().unwrap(),
        body: String::from_utf8(body).unwrap(),
    }
}
pub async fn exchange(address: SocketAddr, method: &str, path: &str, body: &str) -> Response {
    finish(open(address, method, path, body).await, vec![]).await
}
pub fn frames(body: &str) -> Vec<(String, Value)> {
    body.replace("\r\n", "\n")
        .split("\n\n")
        .filter_map(|b| {
            let event = b.lines().find_map(|l| l.strip_prefix("event: "))?;
            let data = b
                .lines()
                .filter_map(|l| l.strip_prefix("data: "))
                .collect::<Vec<_>>()
                .join("\n");
            Some((event.to_owned(), serde_json::from_str(&data).unwrap()))
        })
        .collect()
}
pub fn cancelled(response: &Response) {
    assert_eq!(response.status, 200);
    let events = frames(&response.body);
    let done: Vec<_> = events.iter().filter(|(e, _)| e == "done").collect();
    assert_eq!(done.len(), 1);
    assert!(
        done[0].1 == json!({"output":{"cancelled":true}}),
        "native cancellation terminal must remain selected"
    );
    assert!(!events.iter().any(|(e, _)| e == "error"));
}
