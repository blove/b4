#![cfg(unix)]

mod request_diagnostics_cli_support;

use request_diagnostics_cli_support::{exchange, Process, Sandbox};
use serde_json::json;
use std::collections::BTreeSet;

#[test]
fn shipped_demo_responses_have_request_references_and_unchanged_bodies() {
    let sandbox = Sandbox::new();
    let process = Process::start(&sandbox);
    let address = process.address;
    let mut references = BTreeSet::new();
    for (path, expected) in [
        ("/healthz", json!({"status":"ok"})),
        ("/readyz", json!({"status":"ready"})),
    ] {
        let response = exchange(address, "GET", path, false, "");
        assert!(
            response.status == 200 && response.json() == expected,
            "native probes must retain their legacy JSON"
        );
        assert!(
            references.insert(response.request_id()),
            "actual native request references must be distinct"
        );
    }
    let refused = exchange(address, "POST", "/threads", false, "{}");
    assert!(
        refused.status == 401
            && refused.json()
                == json!({"error":{"kind":"request_error","message":"authentication_required","details":{"code":"authentication_required"}}}),
        "early native authentication refusal body must stay exact"
    );
    assert!(references.insert(refused.request_id()));
    let run = r#"{"route":"/native#workflow","input":{"marker":"private-process-input"}}"#;
    let wait = exchange(
        address,
        "POST",
        "/threads/private-process-wait/runs/wait",
        true,
        run,
    );
    assert!(
        wait.status == 200 && wait.json()["task"] == "second",
        "real immutable native wait must preserve selected output order"
    );
    assert!(references.insert(wait.request_id()));
    let stream = exchange(
        address,
        "POST",
        "/threads/private-process-stream/runs/stream",
        true,
        run,
    );
    assert!(
        stream.status == 200
            && stream
                .headers
                .get("content-type")
                .is_some_and(|value| value.starts_with("text/event-stream")),
        "real native stream must retain SSE framing"
    );
    assert!(references.insert(stream.request_id()));
    let text = std::str::from_utf8(&stream.body).unwrap();
    assert!(
        text.lines().filter(|line| *line == "event: done").count() == 1,
        "finite native SSE must retain one terminal frame"
    );
    process.stop();
    let writer = b4_sqlite::SqliteStore::open(sandbox.path.join("state"));
    assert!(
        writer.is_ok(),
        "real writer must reacquire after positively observed native shutdown"
    );
}

#[test]
fn positively_retired_native_hosts_receive_independent_request_namespaces() {
    let sandbox = Sandbox::new();
    let first = Process::start(&sandbox);
    let first_id = exchange(first.address, "GET", "/healthz", false, "").request_id();
    first.stop();
    let writer = b4_sqlite::SqliteStore::open(sandbox.path.join("state"));
    assert!(
        writer.is_ok(),
        "a real intervening writer must acquire the native database"
    );
    drop(writer);
    let second = Process::start(&sandbox);
    let second_id = exchange(second.address, "GET", "/healthz", false, "").request_id();
    assert!(
        first_id[..36] != second_id[..36],
        "independent native host construction must use a fresh diagnostic namespace"
    );
    second.stop();
}
