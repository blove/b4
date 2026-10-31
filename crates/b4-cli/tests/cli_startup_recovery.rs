#![cfg(unix)]

// Reuse only the protected helper module, which has no test declarations.
#[allow(dead_code)]
#[path = "cli_support/mod.rs"]
mod cli_support;

use b4_contract::{ExecutionStatus, Snapshot};
use b4_runtime::{approval::ApprovalStore, JournalEntry, Store};
use b4_sqlite::SqliteStore;
use cli_support::*;
use rusqlite::{Connection, OpenFlags};
use serde_json::{json, Value};
use std::io::{Read, Write};
use std::net::{SocketAddr, TcpStream};
use std::path::Path;
use std::time::{Duration, Instant};

fn current_owner(data: &Path) -> u64 {
    let connection = Connection::open_with_flags(
        data.join("runtime.sqlite3"),
        OpenFlags::SQLITE_OPEN_READ_ONLY,
    )
    .expect("the real current owner must be read without acquiring ownership");
    let bytes: Vec<u8> = connection
        .query_row("SELECT owner_epoch FROM metadata WHERE id = 1", [], |row| {
            row.get(0)
        })
        .expect("the existing actual metadata owner must be readable");
    u64::from_be_bytes(
        bytes
            .try_into()
            .expect("owner encoding must be exactly eight bytes"),
    )
}

fn ready(address: SocketAddr) {
    let deadline = Instant::now() + Duration::from_secs(12);
    loop {
        let mut stream = TcpStream::connect_timeout(&address, Duration::from_millis(500))
            .expect("the announced actual listener must accept readiness observations");
        stream
            .set_read_timeout(Some(Duration::from_millis(500)))
            .unwrap();
        stream
            .set_write_timeout(Some(Duration::from_millis(500)))
            .unwrap();
        stream.write_all(format!("GET /readyz HTTP/1.1\r\nHost: {address}\r\nAuthorization: Bearer {TOKEN}\r\nConnection: close\r\n\r\n").as_bytes()).unwrap();
        let mut bytes = Vec::new();
        stream
            .take(65_537)
            .read_to_end(&mut bytes)
            .expect("readiness transport must return normally within its finite bound");
        assert!(
            bytes.len() <= 65_536,
            "readiness output must remain bounded"
        );
        let status: u16 = std::str::from_utf8(&bytes)
            .unwrap()
            .split_whitespace()
            .nth(1)
            .unwrap()
            .parse()
            .unwrap();
        if status == 200 {
            return;
        }
        assert_eq!(
            status, 503,
            "pending readiness must use the existing closed response"
        );
        assert!(
            Instant::now() < deadline,
            "the finite startup must reach readiness"
        );
        std::thread::sleep(Duration::from_millis(5));
    }
}

fn recover(data: &Path, sandbox: &Sandbox) -> (Process, SocketAddr, u64) {
    let mut process = Process::spawn(
        command(data).args(["serve", "--demo", "--recover"]),
        sandbox,
    );
    let address = process.listening();
    assert!(address.is_ipv4() && address.ip().is_loopback() && address.port() != 0);
    let owner = current_owner(data);
    ready(address);
    (process, address, owner)
}

fn stop_recovery(process: &mut Process) {
    process.signal("-TERM");
    let output = process.wait();
    assert!(
        output.status.success(),
        "normal signal exit must be positively observed"
    );
    assert!(
        output.stderr.is_empty(),
        "healthy restart must emit no error or private payload"
    );
    assert!(output.stdout.ends_with(b"b4run_stopped\n"));
    let text = std::str::from_utf8(&output.stdout).unwrap();
    assert!(text.lines().any(|line| line == "b4run_draining"));
    for line in text.lines() {
        let listener = line
            .strip_prefix("b4run_listening http://")
            .and_then(|address| address.parse::<SocketAddr>().ok())
            .is_some_and(|address| {
                address.is_ipv4() && address.ip().is_loopback() && address.port() != 0
            });
        assert!(
            listener
                || [
                    "b4run_preparing",
                    "b4run_ready",
                    "b4run_draining",
                    "b4run_drain_incomplete",
                    "b4run_stopped"
                ]
                .contains(&line),
            "only fixed public lifecycle diagnostics may be emitted"
        );
    }
}

fn park(address: SocketAddr, thread: &str) -> Value {
    let response = exchange(
        address,
        "POST",
        &format!("/threads/{thread}/runs/wait"),
        r#"{"route":"/approve#workflow","input":null}"#,
    );
    assert!(response.status == 200 && response.body == b"null");
    let response = exchange(
        address,
        "GET",
        &format!("/threads/{thread}/pending_interrupts"),
        "",
    );
    assert!(response.status == 200 && response.headers.contains("cache-control: no-store"));
    let interrupts = response.json()["interrupts"].clone();
    assert!(interrupts.as_array().is_some_and(|items| items.len() == 1));
    let interrupt = interrupts[0].clone();
    assert!(interrupt["grant"]
        .as_str()
        .is_some_and(|grant| grant.starts_with("b4ag_")));
    interrupt
}

#[test]
fn explicit_cli_restart_adopts_the_original_park_and_resumes_only_approved_work() {
    let sandbox = Sandbox::new();
    let data = sandbox.path.join("private-state");
    let thread = "explicit-cli-park";
    let (mut seed, address) = serve(&data, &sandbox);
    let interrupt = park(address, thread);
    let seed_snapshot: Snapshot = serde_json::from_value(snapshot(&data, thread)).unwrap();
    let execution = seed_snapshot.execution_id.clone();
    let seed_owner = current_owner(&data);
    assert_eq!(seed_snapshot.owner_generation, seed_owner);
    assert_eq!(seed_snapshot.status, ExecutionStatus::WaitingForApproval);
    private_modes(&data);
    clean_stop(&mut seed, "-TERM");
    let original =
        SqliteStore::open(&data).expect("normal seed retirement must permit a genuine next writer");
    assert_eq!(original.owner_generation(), seed_owner + 1);
    let before = original.approval_view(&execution).unwrap().unwrap();
    let journal_before = original.journal(&execution).unwrap();
    assert_eq!(before.execution.snapshot, seed_snapshot);
    assert_eq!(before.grants.len(), 1);
    assert!(!before.grants[0].consumed && before.acceptances.is_empty());
    drop(original);
    let original_grants = sql_rows(&data, "approval_grants");
    let original_acceptances = sql_rows(&data, "approval_acceptances");

    let (mut restarted, _, restart_owner) = recover(&data, &sandbox);
    assert_eq!(restart_owner, seed_owner + 2);
    stop_recovery(&mut restarted);
    let next = SqliteStore::open(&data)
        .expect("normal restart retirement must permit a genuine next writer");
    assert_eq!(next.owner_generation(), restart_owner + 1);
    let after = next.approval_view(&execution).unwrap().unwrap();
    let journal_after = next.journal(&execution).unwrap();
    drop(next);
    private_modes(&data);

    // The intended initial RED is reached only after successful ordinary exit,
    // reap and actual exclusive next-writer acquisition. No future startup gate
    // is awaited on the parser-only ordinary-serving scaffold.
    assert_eq!(
        after.execution.snapshot.owner_generation, restart_owner,
        "explicit CLI recovery must adopt the saved approval under its genuine current owner"
    );
    let mut expected = before.clone();
    expected.thread.resource_revision += 1;
    expected.execution.snapshot.owner_generation = restart_owner;
    expected.execution.snapshot.revision += 1;
    expected.execution.snapshot.turn_id = after.execution.snapshot.turn_id.clone();
    assert_ne!(
        expected.execution.snapshot.turn_id,
        before.execution.snapshot.turn_id
    );
    assert!(!before
        .execution
        .snapshot
        .turn_history
        .contains(&expected.execution.snapshot.turn_id));
    expected
        .execution
        .snapshot
        .turn_history
        .push(expected.execution.snapshot.turn_id.clone());
    assert!(after.execution.snapshot.updated_at_ms >= before.execution.snapshot.updated_at_ms);
    expected.execution.snapshot.updated_at_ms = after.execution.snapshot.updated_at_ms;
    assert_eq!(
        after, expected,
        "the complete original approval view may change only by legal adoption"
    );
    assert_eq!(sql_rows(&data, "approval_grants"), original_grants);
    assert_eq!(
        sql_rows(&data, "approval_acceptances"),
        original_acceptances
    );
    let mut expected_journal = journal_before;
    expected_journal.push(JournalEntry::from(&after.execution.snapshot));
    assert_eq!(journal_after, expected_journal);

    // This branch has zero initial RED credit until the adoption assertion above
    // passes. It exercises the original issued grant through the real CLI AP.
    let (mut resumed, address, resume_owner) = recover(&data, &sandbox);
    assert_eq!(resume_owner, restart_owner + 2);
    let pending = exchange(
        address,
        "GET",
        &format!("/threads/{thread}/pending_interrupts"),
        "",
    );
    assert!(pending.status == 200 && pending.headers.contains("cache-control: no-store"));
    assert_eq!(pending.json()["interrupts"], json!([interrupt.clone()]));
    let response = exchange(address, "POST", &format!("/threads/{thread}/resume"),
        &json!({"route":"/approve#workflow","resume":[{"status":"resolved","interruptId":interrupt["interruptId"],"payload":"once","grant":interrupt["grant"]}]}).to_string());
    assert_eq!(response.status, 200);
    assert!(response.headers.contains("content-type: text/event-stream"));
    let done: Vec<_> = response
        .frames()
        .into_iter()
        .filter(|(name, _)| name == "done")
        .collect();
    assert_eq!(
        done,
        vec![("done".to_owned(), json!({"output":{"released":true}}))]
    );
    stop_recovery(&mut resumed);
    let final_writer = SqliteStore::open(&data)
        .expect("approved completion must retire before another genuine writer");
    assert_eq!(final_writer.owner_generation(), resume_owner + 1);
    let completed = final_writer.approval_view(&execution).unwrap().unwrap();
    drop(final_writer);
    assert_eq!(
        completed.execution.snapshot.status,
        ExecutionStatus::Completed
    );
    assert_eq!(
        completed.execution.snapshot.values["items"],
        json!(["prepared", "released"])
    );
    assert_eq!(
        completed.execution.snapshot.definition,
        before.execution.snapshot.definition
    );
    assert_eq!(
        completed.execution.snapshot.tasks[0],
        before.execution.snapshot.tasks[0]
    );
    assert_eq!(completed.grants.len(), 1);
    assert!(completed.grants[0].consumed);
    assert_eq!(completed.acceptances.len(), 1);
    private_modes(&data);
}
