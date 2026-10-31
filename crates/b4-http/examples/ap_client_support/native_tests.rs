//! Native fixture qualification only: handwritten unit probes, no original callers.
use super::*;
use b4_http::HttpHost;
use b4_runtime::NativeApplication;
use std::{net::SocketAddr, sync::Weak};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
};
struct Native {
    base: PathBuf,
    state: PathBuf,
    store: Option<Arc<SqliteStore>>,
    host: Option<HttpHost<SqliteStore>>,
    weak_store: Weak<SqliteStore>,
    weak_app: Weak<NativeApplication>,
    gates: Gates,
    shutdown: b4_http::ShutdownHandle,
    server: Server,
    collector: Option<super::super::capture::Collector>,
    address: SocketAddr,
}
impl Native {
    async fn start() -> Self {
        let base = std::env::temp_dir().join(format!("b4-ap-native-{}", uuid::Uuid::new_v4()));
        files::private_dir(&base).unwrap();
        let state = base.join("state");
        files::prepare_store(&state).unwrap();
        let store = Arc::new(SqliteStore::open(&state).unwrap());
        files::verify_store(&state).unwrap();
        let weak_store = Arc::downgrade(&store);
        let gates = Gates::new();
        let (host, weak_app) = application::host(store.clone(), "unit-token", &gates).unwrap();
        let shutdown = host.shutdown_handle();
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let (listener, collector) = CaptureListener::new(listener);
        let (stop, stopped) = oneshot::channel();
        let router = host.router();
        let server = Server {
            stop: Some(stop),
            task: Some(tokio::spawn(async move {
                axum::serve(listener, router)
                    .with_graceful_shutdown(async {
                        let _ = stopped.await;
                    })
                    .await
                    .map_err(|_| Error("server_failed"))
            })),
        };
        Self {
            base,
            state,
            store: Some(store),
            host: Some(host),
            weak_store,
            weak_app,
            gates,
            shutdown,
            server,
            collector: Some(collector),
            address,
        }
    }
    async fn finish(mut self) -> Vec<Record> {
        assert!(self.gates.all_positive());
        self.shutdown.begin_shutdown();
        self.server.stop().await.unwrap();
        assert_eq!(
            self.shutdown.wait_quiescent(FINAL_WAIT).await.unwrap(),
            ShutdownOutcome::Quiescent { faulted: false }
        );
        let (summary, records) = self.collector.take().unwrap().finish().await.unwrap();
        assert_eq!(summary.accepted, summary.retired);
        drop(self.host.take());
        drop(self.store.take());
        absent(|| self.weak_store.upgrade().is_none() && self.weak_app.upgrade().is_none())
            .await
            .unwrap();
        let until = Instant::now() + FINAL_WAIT;
        let writer = loop {
            match SqliteStore::open(&self.state) {
                Ok(writer) => break writer,
                Err(StoreError::AlreadyOwned) if Instant::now() < until => {
                    tokio::task::yield_now().await
                }
                Err(_) => panic!("actual fixture writer must reopen"),
            }
        };
        assert_eq!(writer.owner_generation(), 2);
        assert!(writer.readiness().is_ok());
        files::verify_store(&self.state).unwrap();
        assert!(!records.is_empty());
        drop(writer);
        std::fs::remove_dir_all(&self.base).unwrap();
        records
    }
}
impl Drop for Native {
    fn drop(&mut self) {
        self.gates.cleanup();
    }
}
async fn open(address: SocketAddr, method: &str, path: &str, body: &str) -> (TcpStream, Vec<u8>) {
    let mut stream = TcpStream::connect(address).await.unwrap();
    let request=format!("{method} {path} HTTP/1.1\r\nHost: {address}\r\nAuthorization: Bearer unit-token\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",body.len()).into_bytes();
    stream.write_all(&request).await.unwrap();
    (stream, request)
}
async fn prefix(stream: &mut TcpStream, marker: &[u8]) -> Vec<u8> {
    tokio::time::timeout(FINAL_WAIT, async {
        let mut bytes = Vec::new();
        while !bytes.windows(marker.len()).any(|b| b == marker) {
            let mut part = [0; 512];
            let n = stream.read(&mut part).await.unwrap();
            assert!(n > 0);
            bytes.extend_from_slice(&part[..n]);
            assert!(bytes.len() <= 262144);
        }
        bytes
    })
    .await
    .unwrap()
}
fn body(bytes: &[u8]) -> (u16, Vec<u8>) {
    let split = bytes.windows(4).position(|b| b == b"\r\n\r\n").unwrap();
    let head = std::str::from_utf8(&bytes[..split]).unwrap();
    let status = head.split_whitespace().nth(1).unwrap().parse().unwrap();
    let mut input = &bytes[split + 4..];
    if !head
        .to_ascii_lowercase()
        .contains("transfer-encoding: chunked")
    {
        return (status, input.to_vec());
    }
    let mut output = Vec::new();
    loop {
        let end = input.windows(2).position(|b| b == b"\r\n").unwrap();
        let n = usize::from_str_radix(std::str::from_utf8(&input[..end]).unwrap(), 16).unwrap();
        input = &input[end + 2..];
        if n == 0 {
            assert_eq!(input, b"\r\n");
            break;
        }
        assert!(input.len() >= n + 2);
        output.extend_from_slice(&input[..n]);
        assert_eq!(&input[n..n + 2], b"\r\n");
        input = &input[n + 2..];
    }
    (status, output)
}
async fn exchange(
    address: SocketAddr,
    method: &str,
    path: &str,
    content: &str,
) -> (u16, Vec<u8>, Vec<u8>, Vec<u8>) {
    let (mut stream, request) = open(address, method, path, content).await;
    let mut response = Vec::new();
    tokio::time::timeout(FINAL_WAIT, stream.read_to_end(&mut response))
        .await
        .unwrap()
        .unwrap();
    let (status, decoded) = body(&response);
    (status, decoded, request, response)
}
fn terminal(bytes: &[u8]) -> serde_json::Value {
    let text = std::str::from_utf8(bytes).unwrap();
    let done = text
        .split("\n\n")
        .find(|part| part.starts_with("event: done\n"))
        .unwrap();
    let data = done
        .lines()
        .find_map(|line| line.strip_prefix("data: "))
        .unwrap();
    serde_json::from_str(data).unwrap()
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn actual_native_factory_waits_and_tcp_capture_preserve_exact_bytes() {
    let native = Native::start().await;
    let routes = [
        "/work-orders/create#workflow",
        "/work-orders/dispatch#workflow",
        "/work-orders/approve#workflow",
        "/work-orders/retry#workflow",
        "/work-orders/deny#workflow",
        "/work-orders/intake#workflow",
        "/work-orders/approve-intake#workflow",
        "/work-orders/reject-intake#workflow",
        "/work-orders/cancel#workflow",
        "/reconcile#workflow",
    ];
    let mut pairs = Vec::new();
    for (index, route) in routes.iter().enumerate() {
        let input = json!({"route":route,"input":{"operationKey":"ordinary"}}).to_string();
        let (status, decoded, request, response) = exchange(
            native.address,
            "POST",
            &format!("/threads/unit-{index}/runs/wait"),
            &input,
        )
        .await;
        assert_eq!(status, 200);
        let value: serde_json::Value = serde_json::from_slice(&decoded).unwrap();
        if route == &"/work-orders/cancel#workflow" {
            assert!(value.is_null());
        } else {
            assert!(value == json!({"ok":true,"message":"fixture-value"}));
        }
        pairs.push((request, response));
    }
    let fail=json!({"route":"/work-orders/dispatch#workflow","input":{"operationKey":"fixture-known-failure"}}).to_string();
    let (status, decoded, request, response) = exchange(
        native.address,
        "POST",
        "/threads/unit-failure/runs/wait",
        &fail,
    )
    .await;
    assert_eq!(status, 500);
    let value: serde_json::Value = serde_json::from_slice(&decoded).unwrap();
    assert!(value["error"]["details"]["code"].is_null());
    pairs.push((request, response));
    let records = native.finish().await;
    assert_eq!(records.len(), pairs.len());
    for record in records {
        assert!(
            pairs
                .iter()
                .any(|(request, response)| record.read_bytes() == request
                    && record.write_bytes() == response),
            "actual accepted TCP transfers must match original unit peer bytes"
        );
    }
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn actual_native_worker_value_void_failure_and_durable_park_are_untranslated() {
    let native = Native::start().await;
    for (index, route) in [
        application::WORKER_VALUE,
        application::WORKER_VOID,
        application::WORKER_FAILURE,
        application::WORKER_PARK,
    ]
    .iter()
    .enumerate()
    {
        let input = json!({"route":route,"input":{"messages":[{"role":"user","content":"unit"}]}})
            .to_string();
        let (status, decoded, _, _) = exchange(
            native.address,
            "POST",
            &format!("/threads/unit-stream-{index}/runs/stream"),
            &input,
        )
        .await;
        assert_eq!(status, 200);
        let done = terminal(&decoded);
        match index {
            0 => assert!(done["output"].is_object()),
            1 => assert!(done == json!({})),
            2 => assert!(done["output"]["error"].is_string()),
            _ => assert!(done["output"].is_null()),
        }
    }
    let id = ThreadId::new("unit-stream-3").unwrap();
    let store = native.store.as_ref().unwrap();
    let view = store.thread_view(&id).unwrap().unwrap();
    assert_eq!(
        view.execution.as_ref().unwrap().snapshot.status,
        b4_contract::ExecutionStatus::WaitingForApproval
    );
    let before = store
        .approval_view(&view.execution.as_ref().unwrap().snapshot.execution_id)
        .unwrap()
        .unwrap();
    assert_eq!(before.grants.len(), 1);
    assert!(!before.grants[0].consumed);
    let actual_id = before.grants[0].interrupt_id.as_str();
    let resume=json!({"route":application::WORKER_PARK,"resume":[{"interruptId":actual_id,"status":"resolved","payload":"once"}]}).to_string();
    let (status, decoded, _, _) = exchange(
        native.address,
        "POST",
        "/threads/unit-stream-3/resume",
        &resume,
    )
    .await;
    assert_eq!(status, 400);
    let value: serde_json::Value = serde_json::from_slice(&decoded).unwrap();
    assert_eq!(value["error"]["details"]["code"], "grant_required");
    let after = store
        .approval_view(&view.execution.as_ref().unwrap().snapshot.execution_id)
        .unwrap()
        .unwrap();
    assert!(
        before == after,
        "grantless refusal must not alter actual authority state"
    );
    native.finish().await;
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn actual_single_native_held_callback_survives_viewer_cuts_busy_refusal_and_cancel() {
    let native = Native::start().await;
    let generation = native.gates.worker.arm().unwrap();
    let input=json!({"route":application::WORKER_HELD,"input":{"messages":[{"role":"user","content":"unit"}]}}).to_string();
    let (mut primary, _) = open(
        native.address,
        "POST",
        "/threads/unit-held/runs/stream",
        &input,
    )
    .await;
    native.gates.worker.await_entered(generation).unwrap();
    let head = prefix(&mut primary, b"\r\n\r\n").await;
    assert!(!head
        .windows(b"event: chunk".len())
        .any(|b| b == b"event: chunk"));
    assert!(!head
        .windows(b"event: state".len())
        .any(|b| b == b"event: state"));
    let store = native.store.as_ref().unwrap();
    let id = ThreadId::new("unit-held").unwrap();
    assert_eq!(
        store
            .thread_view(&id)
            .unwrap()
            .unwrap()
            .execution
            .unwrap()
            .snapshot
            .tasks[0]
            .status,
        b4_contract::TaskStatus::InFlight
    );
    let (status, decoded, _, _) = exchange(
        native.address,
        "POST",
        "/threads/unit-held/runs/wait",
        &input,
    )
    .await;
    assert_eq!(status, 409);
    let value: serde_json::Value = serde_json::from_slice(&decoded).unwrap();
    assert_eq!(value["error"]["details"]["code"], "thread_busy");
    let (mut attached, _) = open(native.address, "GET", "/threads/unit-held/runs/stream", "").await;
    let state = prefix(&mut attached, b"event: state").await;
    assert!(state
        .windows(b"event: state".len())
        .any(|b| b == b"event: state"));
    drop(attached);
    drop(primary);
    assert!(native.shutdown.status().active_owners > 0);
    assert!(matches!(
        SqliteStore::open(&native.state),
        Err(StoreError::AlreadyOwned)
    ));
    assert_eq!(
        exchange(native.address, "POST", "/threads/unit-held/cancel", "")
            .await
            .0,
        200
    );
    assert!(!native.gates.worker.status(generation).unwrap().returned);
    let flags = native.gates.worker.release(generation).unwrap();
    assert!(flags.returned && flags.explicit_release && !flags.expired);
    native.finish().await;
}
