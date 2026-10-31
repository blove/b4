#![cfg(unix)]

use b4_contract::*;
use b4_http::*;
use b4_runtime::{approval::*, *};
use b4_sqlite::SqliteStore;
use rusqlite::Connection;
use serde_json::json;
use std::{
    fs::{File, OpenOptions},
    io::Write,
    os::unix::process::ExitStatusExt,
    path::{Path, PathBuf},
    process::{Child, Command, Stdio},
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

#[path = "native_support/store.rs"]
mod interception;
#[path = "native_support/mod.rs"]
mod support;
use interception::InterceptStore;
use support::*;

const CHILD_DIRECTORY: &str = "B4_NATIVE_CRASH_DIRECTORY";
const CHILD_SCENARIO: &str = "B4_NATIVE_CRASH_SCENARIO";
const THREAD: &str = "native-crash";
const ROUTE: &str = "/native-crash#workflow";

fn program(scenario: &str) -> Definition {
    match scenario {
        "park" => definition(
            ROUTE,
            "native-crash-v1",
            &[
                ("sibling", false, RecoveryPolicy::Pure),
                ("approval", true, RecoveryPolicy::Pure),
            ],
        ),
        "accepted" => definition(
            ROUTE,
            "native-crash-v1",
            &[("approval", true, RecoveryPolicy::Pure)],
        ),
        "unsafe" => definition(
            ROUTE,
            "native-crash-v1",
            &[("unsafe", false, RecoveryPolicy::Unsafe)],
        ),
        _ => panic!("unknown native crash scenario"),
    }
}
fn marker(directory: &Path, name: &str) {
    File::create(directory.join(name))
        .unwrap()
        .sync_all()
        .unwrap();
}
fn hold_until_kill() -> ! {
    loop {
        std::thread::park();
    }
}
fn ledger(directory: &Path) -> Vec<String> {
    match std::fs::read_to_string(directory.join("native.ledger")) {
        Ok(contents) => contents.lines().map(str::to_owned).collect(),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => vec![],
        Err(_) => panic!("native ledger could not be read"),
    }
}
struct Ledger {
    directory: PathBuf,
    stop_after_unsafe_effect: bool,
    calls: Arc<AtomicUsize>,
}
impl NativeEffect for Ledger {
    fn invoke(
        &self,
        invocation: &EffectInvocation,
        context: &InvocationContext,
    ) -> Result<NativeOutcome, EffectUncertain> {
        assert_eq!(context.thread_id.as_str(), THREAD);
        assert_eq!(context.origin.application, "native-test");
        self.calls.fetch_add(1, Ordering::SeqCst);
        let task = invocation.task_id.as_str();
        let mut file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(self.directory.join("native.ledger"))
            .unwrap();
        writeln!(file, "{task}").unwrap();
        file.sync_all().unwrap();
        if task == "unsafe" && self.stop_after_unsafe_effect {
            marker(&self.directory, "post-effect-precommit");
            hold_until_kill();
        }
        Ok(NativeOutcome::Succeeded(TaskResult {
            output: json!(task),
            writes: vec![StateWrite::Append {
                field: "joined".into(),
                items: vec![json!(task)],
            }],
        }))
    }
}
struct OwnedChild(Child);
impl OwnedChild {
    fn start(directory: &Directory, scenario: &str) -> Self {
        Self(
            Command::new(std::env::current_exe().unwrap())
                .args([
                    "--exact",
                    "native_crash_child_helper",
                    "--ignored",
                    "--test-threads=1",
                ])
                .env(CHILD_DIRECTORY, &directory.0)
                .env(CHILD_SCENARIO, scenario)
                // Tokens remain in the ephemeral database/child memory. Never relay
                // raw child diagnostics or native approval responses to test output.
                .stdin(Stdio::null())
                .stdout(Stdio::null())
                .stderr(Stdio::null())
                .spawn()
                .unwrap(),
        )
    }
    fn reached(&mut self, directory: &Directory, name: &str) {
        let deadline = Instant::now() + Duration::from_secs(10);
        while !directory.0.join(name).is_file() {
            assert!(
                self.0.try_wait().unwrap().is_none(),
                "native child exited before checkpoint (resume status: {})",
                std::fs::read_to_string(directory.0.join("resume-status"))
                    .unwrap_or_else(|_| "not returned".into())
            );
            assert!(
                Instant::now() < deadline,
                "native child did not reach synchronized checkpoint"
            );
            std::thread::sleep(Duration::from_millis(2));
        }
    }
    fn kill_and_wait(&mut self) {
        self.0.kill().unwrap();
        assert_eq!(
            self.0.wait().unwrap().signal(),
            Some(9),
            "native fixture must stop by SIGKILL without Rust cleanup"
        );
    }
}
impl Drop for OwnedChild {
    fn drop(&mut self) {
        if self.0.try_wait().is_ok_and(|status| status.is_none()) {
            let _ = self.0.kill();
        }
        let _ = self.0.wait();
    }
}
fn raw_snapshot(directory: &Directory) -> Snapshot {
    let bytes: String = Connection::open(directory.0.join("runtime.sqlite3"))
        .unwrap()
        .query_row(
            "SELECT snapshot FROM executions WHERE thread_id=?1",
            [THREAD],
            |r| r.get(0),
        )
        .unwrap();
    serde_json::from_str(&bytes).unwrap()
}
fn authority_records(directory: &Directory) -> (Vec<String>, Vec<String>) {
    let connection = Connection::open(directory.0.join("runtime.sqlite3")).unwrap();
    let records = |table| {
        connection
            .prepare(&format!("SELECT record FROM {table} ORDER BY record"))
            .unwrap()
            .query_map([], |r| r.get(0))
            .unwrap()
            .collect::<Result<Vec<String>, _>>()
            .unwrap()
    };
    (records("approval_grants"), records("approval_acceptances"))
}
fn target(store: &SqliteStore) -> RecoveryTarget {
    RecoveryTarget::from_view(
        &store
            .thread_view(&ThreadId::new(THREAD).unwrap())
            .unwrap()
            .unwrap(),
    )
    .unwrap()
}
fn host(
    store: Arc<SqliteStore>,
    directory: &Directory,
    definition: Definition,
    calls: Arc<AtomicUsize>,
) -> HttpHost<SqliteStore> {
    let callback = Arc::new(Ledger {
        directory: directory.0.clone(),
        stop_after_unsafe_effect: false,
        calls,
    });
    let app = application(definition, callback, None);
    HttpHost::new_with_application(store, Arc::new(Auth), Arc::new(Owner), config(app, true, 2))
        .unwrap()
}
async fn refusal_probes(
    store: Arc<SqliteStore>,
    directory: &Directory,
    scenario: &str,
    calls: &Arc<AtomicUsize>,
) {
    let captured = target(&store);
    let original = snapshot(store.as_ref(), THREAD);
    for variant in 0..3 {
        let mut selected = program(scenario);
        if variant == 0 {
            selected.revision = ExecutionRevision::new("changed-native-code-v2").unwrap();
        } else if variant == 1 {
            selected.route = RouteKey::new("/unregistered-old-route#workflow").unwrap();
        } else {
            selected.max_attempts += 1;
        }
        let wrong = host(store.clone(), directory, selected, calls.clone());
        let server = Server::start(&wrong, Some(captured.clone())).await;
        let response = exchange(server.address, "POST", "/fixture/recover", "").await;
        assert_eq!(response.status, 409);
        assert_eq!(
            response.json()["error"]["details"]["code"],
            "recovery_definition_unavailable"
        );
        assert_eq!(snapshot(store.as_ref(), THREAD), original);
        assert_eq!(calls.load(Ordering::SeqCst), 0);
        server.stop().await;
    }
}
async fn resume(address: std::net::SocketAddr) -> Response {
    let response = exchange(
        address,
        "GET",
        &format!("/threads/{THREAD}/pending_interrupts"),
        "",
    )
    .await;
    assert_eq!(response.status, 200);
    let pending = response.json();
    let entries = pending["interrupts"].as_array().unwrap();
    assert_eq!(entries.len(), 1);
    let entry = &entries[0];
    let body = json!({"route":ROUTE,"resume":[{"status":"resolved","interruptId":entry["interruptId"],"payload":"once","grant":entry["grant"]}]}).to_string();
    exchange(address, "POST", &format!("/threads/{THREAD}/resume"), &body).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sigkill_native_http_stable_park_preserves_sibling_and_same_once_grant_across_reopen() {
    let directory = Directory::new();
    let mut child = OwnedChild::start(&directory, "park");
    child.reached(&directory, "stable-park");
    let before = raw_snapshot(&directory);
    let records = authority_records(&directory);
    assert_eq!(before.status, ExecutionStatus::WaitingForApproval);
    assert_eq!(before.tasks[0].status, TaskStatus::Succeeded);
    assert_eq!(before.tasks[1].status, TaskStatus::WaitingForApproval);
    assert_eq!(ledger(&directory.0), vec!["sibling"]);
    assert_eq!(records.0.len(), 1);
    assert!(records.1.is_empty());
    let original_grant: GrantRecord = serde_json::from_str(&records.0[0]).unwrap();
    assert!(!original_grant.consumed);
    child.kill_and_wait();
    let store = Arc::new(SqliteStore::open(&directory.0).unwrap());
    assert_eq!(store.owner_generation(), 2);
    let calls = Arc::new(AtomicUsize::new(0));
    refusal_probes(store.clone(), &directory, "park", &calls).await;
    let native = host(store.clone(), &directory, program("park"), calls.clone());
    let server = Server::start(&native, Some(target(&store))).await;
    // Inactive attachment may disclose a stable prior-owner park read-only.
    let attached = exchange(
        server.address,
        "GET",
        &format!("/threads/{THREAD}/runs/stream"),
        "",
    )
    .await;
    assert_eq!(attached.status, 200);
    let state = frames(&attached.body)
        .into_iter()
        .find(|(event, _)| event == "state")
        .unwrap()
        .1;
    assert_eq!(state["live"], false);
    assert_eq!(state["interrupts"].as_array().unwrap().len(), 1);
    assert!(
        state["interrupts"][0]["grant"].as_str() == Some(original_grant.token.as_str()),
        "prior-owner attachment must disclose the exact unchanged native grant"
    );
    assert!(
        authority_records(&directory) == records,
        "read-only attachment must preserve original authority rows"
    );
    let recovered = exchange(server.address, "POST", "/fixture/recover", "").await;
    assert_eq!(recovered.status, 200);
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    assert!(
        authority_records(&directory) == records,
        "park recovery must preserve exact original grants"
    );
    let saved = snapshot(store.as_ref(), THREAD);
    assert_eq!(saved.tasks[0], before.tasks[0]);
    assert_eq!(saved.attempts_used, before.attempts_used);
    let response = resume(server.address).await;
    assert_eq!(response.status, 200);
    let events = frames(&response.body);
    assert!(
        events
            .iter()
            .any(|(event, value)| event == "done" && value == &json!({"output":"approval"})),
        "Once must complete native approval callback"
    );
    let final_snapshot = snapshot(store.as_ref(), THREAD);
    assert_eq!(final_snapshot.status, ExecutionStatus::Completed);
    assert_eq!(
        final_snapshot.values["joined"],
        json!(["sibling", "approval"])
    );
    assert_eq!(final_snapshot.attempts_used, before.attempts_used + 1);
    assert_eq!(ledger(&directory.0), vec!["sibling", "approval"]);
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    let after = authority_records(&directory);
    assert_eq!(after.0.len(), 1);
    assert_eq!(after.1.len(), 1);
    let consumed_grant: GrantRecord = serde_json::from_str(&after.0[0]).unwrap();
    assert!(consumed_grant.consumed);
    assert!(
        consumed_grant.token == original_grant.token,
        "Once consumption must preserve the original token"
    );
    server.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sigkill_native_http_accepted_once_continues_after_expiry_without_reapproval_or_budget_reset(
) {
    let directory = Directory::new();
    let mut child = OwnedChild::start(&directory, "accepted");
    child.reached(&directory, "accepted-before-dispatch");
    let before = raw_snapshot(&directory);
    let records = authority_records(&directory);
    // Kernel Resume has committed Running/Ready, distinct from initial admission
    // Accepted. The approval decision is durable; no dispatch has begun.
    assert_eq!(before.status, ExecutionStatus::Running);
    assert!(before.tasks[0].approved);
    assert_eq!(before.tasks[0].status, TaskStatus::Ready);
    assert_eq!(before.attempts_used, 0);
    assert!(ledger(&directory.0).is_empty());
    assert_eq!(records.0.len(), 1);
    assert_eq!(records.1.len(), 1);
    let grant: GrantRecord = serde_json::from_str(&records.0[0]).unwrap();
    assert!(grant.consumed);
    child.kill_and_wait();
    let expires = grant.expires_at_ms;
    until(|| {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis()
            > u128::from(expires)
    })
    .await;
    let store = Arc::new(SqliteStore::open(&directory.0).unwrap());
    assert_eq!(store.owner_generation(), 2);
    let calls = Arc::new(AtomicUsize::new(0));
    refusal_probes(store.clone(), &directory, "accepted", &calls).await;
    let native = host(
        store.clone(),
        &directory,
        program("accepted"),
        calls.clone(),
    );
    let server = Server::start(&native, Some(target(&store))).await;
    let response = exchange(server.address, "POST", "/fixture/recover", "").await;
    assert_eq!(response.status, 200);
    assert!(
        frames(&response.body)
            .iter()
            .any(|(event, value)| event == "done" && value == &json!({"output":"approval"})),
        "accepted Once must continue without another token request"
    );
    let final_snapshot = snapshot(store.as_ref(), THREAD);
    assert_eq!(final_snapshot.status, ExecutionStatus::Completed);
    assert_eq!(final_snapshot.owner_generation, 2);
    assert_eq!(
        final_snapshot.definition.max_attempts,
        before.definition.max_attempts
    );
    assert_eq!(final_snapshot.attempts_used, 1);
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert_eq!(ledger(&directory.0), vec!["approval"]);
    assert!(
        authority_records(&directory) == records,
        "consumed Once and acceptance audit must remain byte-identical"
    );
    server.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sigkill_native_http_unsafe_effect_before_completion_blocks_exact_recovery_without_replay()
{
    let directory = Directory::new();
    let mut child = OwnedChild::start(&directory, "unsafe");
    child.reached(&directory, "post-effect-precommit");
    let before = raw_snapshot(&directory);
    assert_eq!(before.status, ExecutionStatus::Running);
    assert_eq!(before.tasks[0].status, TaskStatus::InFlight);
    assert!(before.tasks[0].result.is_none());
    assert_eq!(before.attempts_used, 1);
    assert_eq!(ledger(&directory.0), vec!["unsafe"]);
    child.kill_and_wait();
    let store = Arc::new(SqliteStore::open(&directory.0).unwrap());
    assert_eq!(store.owner_generation(), 2);
    let calls = Arc::new(AtomicUsize::new(0));
    refusal_probes(store.clone(), &directory, "unsafe", &calls).await;
    let native = host(store.clone(), &directory, program("unsafe"), calls.clone());
    let server = Server::start(&native, Some(target(&store))).await;
    let response = exchange(server.address, "POST", "/fixture/recover", "").await;
    assert_eq!(response.status, 409);
    assert_eq!(
        response.json()["error"]["details"]["code"],
        "recovery_blocked"
    );
    let blocked = snapshot(store.as_ref(), THREAD);
    assert_eq!(blocked.status, ExecutionStatus::RecoveryBlocked);
    assert_eq!(blocked.owner_generation, 2);
    assert_eq!(blocked.tasks[0].status, TaskStatus::OutcomeUnknown);
    assert!(blocked.tasks[0].result.is_none());
    assert_eq!(blocked.attempts_used, before.attempts_used);
    assert_eq!(blocked.definition, before.definition);
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    assert_eq!(ledger(&directory.0), vec!["unsafe"]);
    assert_eq!(
        exchange(server.address, "GET", "/readyz", "").await.status,
        200
    );
    server.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "invoked only by the separate-process native SIGKILL harness"]
async fn native_crash_child_helper() {
    let directory =
        PathBuf::from(std::env::var_os(CHILD_DIRECTORY).expect("child directory required"));
    let scenario = std::env::var(CHILD_SCENARIO).unwrap();
    let store = Arc::new(SqliteStore::open(&directory).unwrap());
    let after_directory = directory.clone();
    let accepted = scenario == "accepted";
    let wrapper = Arc::new(InterceptStore {
        inner: store,
        before_commit: None,
        after_approval: Some(Arc::new(move |current, mutation| {
            if accepted && matches!(mutation, ApprovalMutation::Accept(_)) {
                assert_eq!(current.snapshot.status, ExecutionStatus::Running);
                assert!(current.snapshot.tasks[0].approved);
                assert_eq!(current.snapshot.tasks[0].status, TaskStatus::Ready);
                assert_eq!(current.snapshot.attempts_used, 0);
                marker(&after_directory, "accepted-before-dispatch");
                hold_until_kill();
            }
        })),
    });
    let app = application(
        program(&scenario),
        Arc::new(Ledger {
            directory: directory.clone(),
            stop_after_unsafe_effect: scenario == "unsafe",
            calls: Arc::new(AtomicUsize::new(0)),
        }),
        None,
    );
    let mut c = config(app, true, 2);
    if accepted {
        c.approval.ttl_ms = 1_000;
    }
    let native =
        HttpHost::new_with_application(wrapper, Arc::new(Auth), Arc::new(Owner), c).unwrap();
    let server = Server::start(&native, None).await;
    let body = json!({"route":ROUTE,"input":{"from":"original-child"}}).to_string();
    let response = exchange(
        server.address,
        "POST",
        &format!("/threads/{THREAD}/runs/wait"),
        &body,
    )
    .await;
    assert_eq!(response.status, 200);
    if scenario == "park" {
        assert!(response.json().is_null());
        marker(&directory, "stable-park");
    } else if accepted {
        let returned = resume(server.address).await;
        // Only a numeric HTTP status is retained for failed fixture setup.
        std::fs::write(directory.join("resume-status"), returned.status.to_string()).unwrap();
        panic!("accepted checkpoint returned before kill");
    }
    hold_until_kill();
}
