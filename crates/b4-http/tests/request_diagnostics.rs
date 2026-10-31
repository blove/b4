#![cfg(unix)]

mod request_diagnostics_support;

use axum::{
    body::{to_bytes, Body},
    http::{Request, StatusCode},
};
use b4_contract::{RouteKey, TaskId, ThreadId};
use b4_http::{ApprovalHostConfig, HostConfig, ScriptEffect};
use b4_runtime::{approval::ApprovalServiceConfig, MemoryStore, Store};
use request_diagnostics_support::{
    batch, body, error_body, memory_host,
    native::{native_host, until, Directory, Gate, Release},
    request, response, run_input, script_route, scripted_host, tcp, Auth, OwnerPolicy,
    AUTHORIZATION, ROUTE,
};
use serde_json::{json, Value};
use std::{
    collections::{BTreeMap, BTreeSet},
    sync::{Arc, Barrier},
    time::Duration,
};
use tower::ServiceExt;

#[tokio::test]
async fn response_has_server_generated_request_reference() {
    let host = memory_host();
    let response = host
        .router()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/healthz")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let request_id = response.headers().get("x-b4-request-id").cloned();
    let body = to_bytes(response.into_body(), 1024).await.unwrap();
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&body).unwrap(),
        json!({"status":"ok"})
    );
    assert!(
        request_id.is_some(),
        "actual fallback response is missing server-generated x-b4-request-id"
    );
}

#[tokio::test]
async fn actual_fallback_response_drains_one_fixed_response_event() {
    let host = memory_host();
    let response = host
        .router()
        .oneshot(
            Request::builder()
                .uri("/healthz")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let batch = host
        .diagnostics()
        .drain()
        .expect("actual fallback event must have an encoded diagnostic batch");
    let value: serde_json::Value = serde_json::from_slice(batch.as_ref()).unwrap();
    assert!(
        value["records"]
            .as_array()
            .is_some_and(|records| records.len() == 1),
        "an actual fallback response must retain exactly one observation"
    );
    assert!(value["records"][0]["event"] == "response_ready");
    assert!(value["records"][0]["method"] == "get");
    assert!(value["records"][0]["status_code"] == 200);
}

#[tokio::test]
async fn empty_owned_batch_has_exact_schema_and_survives_host_retirement() {
    let store = Arc::new(MemoryStore::new());
    let weak = Arc::downgrade(&store);
    let host = scripted_host(
        store,
        HostConfig::new(vec![script_route(ScriptEffect::Input, false)]),
    );
    let diagnostics = host.diagnostics();
    let clone = diagnostics.clone();
    let (owned, value) = batch::drain(&diagnostics);
    assert!(value["records"].as_array().unwrap().is_empty());
    assert!(
        value["record_loss"] == false
            && value["total_evicted_records"] == 0
            && value["correlation_exhausted"] == false
    );
    drop(host);
    assert!(
        weak.upgrade().is_none(),
        "diagnostics handles and encoded batch must not retain actual MemoryStore"
    );
    assert!(batch::validate(&owned) == value);
    assert!(batch::drain(&clone).1["records"]
        .as_array()
        .unwrap()
        .is_empty());
}

#[tokio::test]
async fn probes_and_early_refusals_preserve_bodies_and_match_response_events() {
    let host = memory_host();
    let cases = [
        (
            "GET",
            "/healthz",
            None,
            "",
            200,
            json!({"status":"ok"}),
            "get",
        ),
        (
            "GET",
            "/readyz",
            None,
            "",
            200,
            json!({"status":"ready"}),
            "get",
        ),
        (
            "POST",
            "/threads",
            None,
            "{}",
            401,
            error_body("authentication_required"),
            "post",
        ),
        (
            "GET",
            "/threads/private-missing",
            Some(AUTHORIZATION),
            "",
            404,
            error_body("resource_unavailable"),
            "get",
        ),
        (
            "GET",
            "/threads/%FF",
            Some(AUTHORIZATION),
            "",
            400,
            error_body("invalid_thread_id"),
            "get",
        ),
        (
            "POST",
            "/threads",
            Some(AUTHORIZATION),
            "{",
            400,
            error_body("invalid_json"),
            "post",
        ),
        (
            "POST",
            "/threads",
            Some(AUTHORIZATION),
            r#"{"private-field":"private-dto-value"}"#,
            400,
            error_body("invalid_request"),
            "post",
        ),
        (
            "PRIVATE-METHOD",
            "/private-unsupported-path",
            Some(AUTHORIZATION),
            "",
            501,
            error_body("unsupported_operation"),
            "other",
        ),
    ];
    let mut expected = Vec::new();
    for (method, path, authorization, input, status, expected_body, label) in cases {
        let result = response(&host, method, path, authorization, input).await;
        assert!(
            result.status().as_u16() == status,
            "actual legacy fallback status must be preserved"
        );
        let id = batch::header(&result);
        assert!(
            body(result).await == expected_body,
            "actual legacy fallback JSON must be preserved"
        );
        expected.push((id, label, status));
    }
    let deleted = response(
        &host,
        "DELETE",
        "/threads/private-missing",
        Some(AUTHORIZATION),
        "",
    )
    .await;
    let deleted_id = batch::header(&deleted);
    assert!(
        deleted.status() == StatusCode::NO_CONTENT,
        "legacy missing-thread DELETE must remain204"
    );
    assert!(
        to_bytes(deleted.into_body(), 1024)
            .await
            .unwrap()
            .is_empty(),
        "legacy missing-thread DELETE body must stay empty"
    );
    expected.push((deleted_id, "delete", 204));
    let (owned, value) = batch::drain(&host.diagnostics());
    let records = value["records"].as_array().unwrap();
    assert!(records.len() == expected.len());
    for (record, (id, method, status)) in records.iter().zip(expected) {
        batch::response_record(record, &id, method, status);
    }
    batch::excludes(
        &owned,
        &[
            "private-caller-correlation",
            "private-request-cookie",
            "PRIVATE-METHOD",
            "private-unsupported-path",
            "private-missing",
            "private-dto-value",
            AUTHORIZATION,
        ],
    );
}

#[tokio::test]
async fn input_and_body_limits_are_correlated_without_retaining_rejected_content() {
    let mut config = HostConfig::new(vec![script_route(ScriptEffect::Input, false)]);
    config.max_input_bytes = 8;
    let host = scripted_host(Arc::new(MemoryStore::new()), config);
    let oversized = "private-oversized-body".repeat(4000);
    let input = run_input(json!("private-input-over-limit"));
    let missing_route = json!({"route":"/private-unknown#workflow","input":null}).to_string();
    for (path, text, status, code) in [
        ("/threads", oversized.as_str(), 413, "body_too_large"),
        (
            "/threads/private-input/runs/wait",
            input.as_str(),
            400,
            "input_limit",
        ),
        (
            "/threads/private-route/runs/wait",
            missing_route.as_str(),
            404,
            "route_not_found",
        ),
    ] {
        let result = response(&host, "POST", path, Some(AUTHORIZATION), text).await;
        let id = batch::header(&result);
        assert!(result.status().as_u16() == status && body(result).await == error_body(code));
        let (owned, value) = batch::drain(&host.diagnostics());
        assert!(value["records"].as_array().unwrap().len() == 1);
        batch::response_record(&value["records"][0], &id, "post", status);
        batch::excludes(
            &owned,
            &[
                "private-oversized-body",
                "private-input-over-limit",
                "private-unknown",
                "private-route",
                AUTHORIZATION,
            ],
        );
    }
}

#[tokio::test]
async fn actual_thread_and_execution_context_are_excluded_from_success_records() {
    let host = memory_host();
    let create = response(
        &host,
        "POST",
        "/threads",
        Some(AUTHORIZATION),
        r#"{"metadata":{"note":"private-thread-metadata"}}"#,
    )
    .await;
    let create_id = batch::header(&create);
    assert!(create.status() == StatusCode::OK);
    let created = body(create).await;
    let thread = created["thread_id"].as_str().unwrap();
    let run = response(
        &host,
        "POST",
        &format!("/threads/{thread}/runs/wait"),
        Some(AUTHORIZATION),
        &run_input(json!({"private-input":"private-output"})),
    )
    .await;
    let run_id = batch::header(&run);
    assert!(
        run.status() == StatusCode::OK
            && body(run).await == json!({"private-input":"private-output"})
    );
    let view = host
        .store()
        .thread_view(&ThreadId::new(thread).unwrap())
        .unwrap()
        .unwrap();
    let snapshot = &view.execution.as_ref().unwrap().snapshot;
    let (owned, value) = batch::drain(&host.diagnostics());
    assert!(value["records"].as_array().unwrap().len() == 2);
    batch::response_record(&value["records"][0], &create_id, "post", 200);
    batch::response_record(&value["records"][1], &run_id, "post", 200);
    batch::excludes(
        &owned,
        &[
            thread,
            snapshot.execution_id.as_str(),
            snapshot.turn_id.as_str(),
            view.thread.generation.as_str(),
            ROUTE,
            "diagnostics-effect",
            "diagnostics-task",
            "diagnostics-revision",
            "diagnostics-application",
            "diagnostics-tenant",
            "diagnostics-principal",
            "private-thread-metadata",
            "private-input",
            "private-output",
            AUTHORIZATION,
            "private-caller-correlation",
            "private-request-cookie",
        ],
    );
}

#[tokio::test]
async fn real_policy_and_application_scope_denials_have_only_fixed_records() {
    let host = memory_host();
    let created = body(response(&host, "POST", "/threads", Some(AUTHORIZATION), "{}").await).await;
    host.diagnostics().drain().unwrap();
    let thread = created["thread_id"].as_str().unwrap();
    let denied = response(
        &host,
        "GET",
        &format!("/threads/{thread}"),
        Some("Bearer diagnostics-other"),
        "",
    )
    .await;
    let id = batch::header(&denied);
    assert!(
        denied.status() == StatusCode::NOT_FOUND
            && body(denied).await == error_body("resource_unavailable")
    );
    let (owned, value) = batch::drain(&host.diagnostics());
    batch::response_record(&value["records"][0], &id, "get", 404);
    batch::excludes(
        &owned,
        &[thread, "private-other-principal", "diagnostics-principal"],
    );
    let (native, _) = native_host(Arc::new(MemoryStore::new()), None, None);
    let denied = response(
        &native,
        "POST",
        "/threads",
        Some("Bearer diagnostics-other-application"),
        "{}",
    )
    .await;
    let id = batch::header(&denied);
    assert!(
        denied.status() == StatusCode::NOT_FOUND
            && body(denied).await == error_body("resource_unavailable")
    );
    let (owned, value) = batch::drain(&native.diagnostics());
    batch::response_record(&value["records"][0], &id, "post", 404);
    batch::excludes(
        &owned,
        &[
            "private-other-application",
            "private-application-revision",
            "private-policy-revision",
        ],
    );
}

#[tokio::test]
async fn shutdown_cut_keeps_probe_and_draining_response_semantics() {
    let host = memory_host();
    host.shutdown_handle().begin_shutdown();
    for (path, authorization, status, expected) in [
        ("/healthz", None, 200, json!({"status":"ok"})),
        ("/readyz", None, 503, json!({"status":"unready"})),
        (
            "/threads/private-thread",
            Some(AUTHORIZATION),
            503,
            error_body("host_draining"),
        ),
    ] {
        let result = response(&host, "GET", path, authorization, "").await;
        let id = batch::header(&result);
        assert!(result.status().as_u16() == status && body(result).await == expected);
        let (_, value) = batch::drain(&host.diagnostics());
        assert!(value["records"].as_array().unwrap().len() == 1);
        batch::response_record(&value["records"][0], &id, "get", status);
    }
    assert!(host.shutdown_handle().status().active_owners == 0);
}

#[tokio::test]
async fn actual_256_responses_fit_and_the_257th_evicts_only_the_oldest() {
    let host = memory_host();
    let diagnostics = host.diagnostics();
    let mut ids = Vec::new();
    for _ in 0..256 {
        let result = response(&host, "GET", "/healthz", None, "").await;
        ids.push(batch::header(&result));
    }
    let (_, full) = batch::drain(&diagnostics);
    assert!(
        full["records"].as_array().unwrap().len() == 256
            && full["record_loss"] == false
            && full["total_evicted_records"] == 0
    );
    for (record, id) in full["records"].as_array().unwrap().iter().zip(&ids) {
        batch::response_record(record, id, "get", 200);
    }
    ids.clear();
    for _ in 0..257 {
        ids.push(batch::header(
            &response(&host, "GET", "/healthz", None, "").await,
        ));
    }
    let (_, evicted) = batch::drain(&diagnostics);
    assert!(
        evicted["records"].as_array().unwrap().len() == 256
            && evicted["record_loss"] == true
            && evicted["total_evicted_records"] == 1
            && evicted["correlation_exhausted"] == false
    );
    for (record, id) in evicted["records"].as_array().unwrap().iter().zip(&ids[1..]) {
        batch::response_record(record, id, "get", 200);
    }
    let (_, empty) = batch::drain(&diagnostics);
    assert!(
        empty["records"].as_array().unwrap().is_empty()
            && empty["record_loss"] == true
            && empty["total_evicted_records"] == 1
    );
    let next = batch::header(&response(&host, "GET", "/healthz", None, "").await);
    let (_, after) = batch::drain(&diagnostics);
    assert!(
        after["records"].as_array().unwrap().len() == 1
            && after["record_loss"] == true
            && after["total_evicted_records"] == 1
    );
    batch::response_record(&after["records"][0], &next, "get", 200);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_real_responses_and_separate_hosts_get_unique_references() {
    let host = memory_host();
    let mut tasks = Vec::new();
    for _ in 0..64 {
        let host = host.clone();
        tasks.push(tokio::spawn(async move {
            let result = response(&host, "GET", "/healthz", None, "").await;
            assert!(result.status() == StatusCode::OK);
            batch::header(&result)
        }));
    }
    let mut ids = BTreeSet::new();
    for task in tasks {
        assert!(
            ids.insert(task.await.unwrap()),
            "actual concurrent responses must not reuse host correlation IDs"
        );
    }
    let (_, value) = batch::drain(&host.diagnostics());
    let records = value["records"].as_array().unwrap();
    assert!(!records.is_empty() && records.len() <= 64);
    assert!(
        value["record_loss"] == true || records.len() == 64,
        "best-effort contention losses must remain conservatively visible"
    );
    let mut retained = BTreeSet::new();
    for record in records {
        let id = record["request_id"].as_str().unwrap();
        assert!(ids.contains(id) && retained.insert(id));
    }
    let other = memory_host();
    let first = batch::header(&response(&host, "GET", "/healthz", None, "").await);
    let second = batch::header(&response(&other, "GET", "/healthz", None, "").await);
    assert!(
        first[..36] != second[..36],
        "independent host constructors must obtain separate probabilistic namespaces"
    );
}

#[tokio::test]
async fn concurrent_cloned_drains_partition_retained_observations_once() {
    let host = memory_host();
    let mut expected = BTreeSet::new();
    for _ in 0..64 {
        expected.insert(batch::header(
            &response(&host, "GET", "/healthz", None, "").await,
        ));
    }
    let diagnostics = host.diagnostics();
    let barrier = Arc::new(Barrier::new(2));
    let results = std::thread::scope(|scope| {
        let mut tasks = Vec::new();
        for _ in 0..2 {
            let clone = diagnostics.clone();
            let barrier = barrier.clone();
            tasks.push(scope.spawn(move || {
                barrier.wait();
                clone.drain()
            }));
        }
        tasks
            .into_iter()
            .map(|task| task.join().unwrap())
            .collect::<Vec<_>>()
    });
    let mut seen = BTreeSet::new();
    for result in results
        .into_iter()
        .chain(std::iter::once(diagnostics.drain()))
    {
        match result {
            Ok(owned) => {
                let value = batch::validate(&owned);
                for record in value["records"].as_array().unwrap() {
                    assert!(
                        seen.insert(record["request_id"].as_str().unwrap().to_owned()),
                        "cloned destructive drains must never return the same retained event twice"
                    );
                }
            }
            Err(error) => assert!(
                error.code() == "diagnostics_busy",
                "actual drain contention may only use its static busy code"
            ),
        }
    }
    assert!(
        seen == expected,
        "cloned drains plus final drain must partition all retained observations"
    );
    assert!(batch::drain(&diagnostics).1["records"]
        .as_array()
        .unwrap()
        .is_empty());
}

#[tokio::test]
async fn an_unpolled_real_router_future_produces_no_observation() {
    let host = memory_host();
    let future = host.router().oneshot(request("GET", "/healthz", None, ""));
    drop(future);
    assert!(batch::drain(&host.diagnostics()).1["records"]
        .as_array()
        .unwrap()
        .is_empty());
}

#[tokio::test]
async fn actual_approval_grant_and_prompt_are_absent_from_diagnostic_batches() {
    let mut config = ApprovalHostConfig::new(
        "private-grant-policy".into(),
        BTreeMap::from([(
            RouteKey::new(ROUTE).unwrap(),
            BTreeMap::from([(
                TaskId::new("diagnostics-task").unwrap(),
                json!({"question":"private-approval-prompt"}),
            )]),
        )]),
    );
    config.service_config = ApprovalServiceConfig::VolatileFixture;
    let host = b4_http::HttpHost::new_with_approvals(
        Arc::new(MemoryStore::new()),
        Arc::new(Auth),
        Arc::new(OwnerPolicy),
        HostConfig::new(vec![script_route(ScriptEffect::Input, true)]),
        config,
    )
    .unwrap();
    let parked = response(
        &host,
        "POST",
        "/threads/private-grant-thread/runs/wait",
        Some(AUTHORIZATION),
        &run_input(json!("private-approved-input")),
    )
    .await;
    let parked_id = batch::header(&parked);
    assert!(parked.status() == StatusCode::OK && body(parked).await.is_null());
    let pending = response(
        &host,
        "GET",
        "/threads/private-grant-thread/pending_interrupts",
        Some(AUTHORIZATION),
        "",
    )
    .await;
    let pending_id = batch::header(&pending);
    assert!(pending.status() == StatusCode::OK);
    let pending = body(pending).await;
    let grant = pending["interrupts"][0]["grant"].as_str().unwrap();
    assert!(
        grant.starts_with("b4ag_"),
        "private fixture must contain an actual issued approval grant"
    );
    let (owned, value) = batch::drain(&host.diagnostics());
    assert!(value["records"].as_array().unwrap().len() == 2);
    batch::response_record(&value["records"][0], &parked_id, "post", 200);
    batch::response_record(&value["records"][1], &pending_id, "get", 200);
    batch::excludes(
        &owned,
        &[
            grant,
            "private-approval-prompt",
            "private-grant-policy",
            "private-grant-thread",
            "private-approved-input",
            ROUTE,
            AUTHORIZATION,
        ],
    );
}

#[tokio::test]
async fn actual_sqlite_failure_keeps_static_legacy_errors_and_redacted_events() {
    let directory = Directory::new();
    let store = Arc::new(b4_sqlite::SqliteStore::open(&directory.0).unwrap());
    let host = scripted_host(
        store,
        HostConfig::new(vec![script_route(ScriptEffect::Input, false)]),
    );
    rusqlite::Connection::open(directory.0.join("runtime.sqlite3"))
        .unwrap()
        .execute_batch("DROP TABLE threads;")
        .unwrap();
    let failed = response(
        &host,
        "GET",
        "/threads/private-sql-thread",
        Some(AUTHORIZATION),
        "",
    )
    .await;
    let id = batch::header(&failed);
    assert!(
        failed.status() == StatusCode::SERVICE_UNAVAILABLE
            && body(failed).await == error_body("host_unready")
    );
    let ready = response(&host, "GET", "/readyz", None, "").await;
    let ready_id = batch::header(&ready);
    assert!(
        ready.status() == StatusCode::SERVICE_UNAVAILABLE
            && body(ready).await == json!({"status":"unready"})
    );
    let (owned, value) = batch::drain(&host.diagnostics());
    assert!(value["records"].as_array().unwrap().len() == 2);
    batch::response_record(&value["records"][0], &id, "get", 503);
    batch::response_record(&value["records"][1], &ready_id, "get", 503);
    batch::excludes(
        &owned,
        &[
            "private-sql-thread",
            "no such table",
            "DROP TABLE",
            "runtime.sqlite3",
            AUTHORIZATION,
        ],
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sse_response_ready_precedes_held_effect_and_body_delivery() {
    let gate = Arc::new(Gate::default());
    let _release = Release(gate.clone());
    let store = Arc::new(MemoryStore::new());
    let (host, _) = native_host(store.clone(), Some(gate.clone()), None);
    let result = response(
        &host,
        "POST",
        "/threads/private-stream/runs/stream",
        Some(AUTHORIZATION),
        &run_input(json!("private-stream-input")),
    )
    .await;
    assert!(
        result.status() == StatusCode::OK
            && result.headers()["content-type"]
                .to_str()
                .unwrap()
                .starts_with("text/event-stream")
    );
    let id = batch::header(&result);
    gate.entered().await;
    let view = store
        .thread_view(&ThreadId::new("private-stream").unwrap())
        .unwrap()
        .unwrap();
    assert!(
        view.execution.unwrap().snapshot.tasks[0].status == b4_contract::TaskStatus::InFlight,
        "actual native effect must remain in flight when SSE response is ready"
    );
    let (owned, value) = batch::drain(&host.diagnostics());
    assert!(value["records"].as_array().unwrap().len() == 1);
    batch::response_record(&value["records"][0], &id, "post", 200);
    batch::excludes(
        &owned,
        &[
            "private-stream",
            "private-stream-input",
            "private-native-output",
            ROUTE,
        ],
    );
    assert!(host.shutdown_handle().status().active_owners > 0);
    // The response body is never polled before the already-produced observation.
    drop(result);
    assert!(
        batch::drain(&host.diagnostics()).1["records"]
            .as_array()
            .unwrap()
            .is_empty(),
        "dropping an SSE body after response construction must not fabricate a future-drop event"
    );
    let busy = response(
        &host,
        "DELETE",
        "/threads/private-stream",
        Some(AUTHORIZATION),
        "",
    )
    .await;
    assert!(
        busy.status() == StatusCode::CONFLICT && body(busy).await == error_body("run_in_flight"),
        "body abandonment must preserve actual owned worker exclusion"
    );
    gate.release();
    assert!(matches!(
        host.shutdown_handle()
            .wait_quiescent(Duration::from_secs(5))
            .await
            .unwrap(),
        b4_http::ShutdownOutcome::Quiescent { faulted: false }
    ));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn dropped_polled_wait_retains_work_and_records_completion_order() {
    let gate = Arc::new(Gate::default());
    let _release = Release(gate.clone());
    let store = Arc::new(MemoryStore::new());
    let weak = Arc::downgrade(&store);
    let (host, weak_application) = native_host(store.clone(), Some(gate.clone()), None);
    let diagnostics = host.diagnostics();
    let task = tokio::spawn(host.router().oneshot(request(
        "POST",
        "/threads/private-dropped-wait/runs/wait",
        Some(AUTHORIZATION),
        &run_input(json!("private-dropped-input")),
    )));
    gate.entered().await;
    assert!(batch::drain(&diagnostics).1["records"]
        .as_array()
        .unwrap()
        .is_empty());
    tokio::time::sleep(Duration::from_millis(30)).await;
    let later = response(&host, "GET", "/healthz", None, "").await;
    let later_id = batch::header(&later);
    task.abort();
    assert!(
        task.await.unwrap_err().is_cancelled(),
        "actual polled fallback future must be positively dropped"
    );
    let (owned, value) = batch::drain(&diagnostics);
    let records = value["records"].as_array().unwrap();
    assert!(records.len() == 2);
    batch::response_record(&records[0], &later_id, "get", 200);
    let dropped = &records[1];
    assert!(
        dropped["event"] == "request_future_dropped"
            && dropped["method"] == "post"
            && dropped["status_code"].is_null()
    );
    let first_id = dropped["request_id"].as_str().unwrap();
    assert!(
        first_id[..36] == later_id[..36]
            && &first_id[37..] == "0000000000000001"
            && &later_id[37..] == "0000000000000002",
        "completion-order records must preserve independent start sequence references"
    );
    assert!(
        dropped["elapsed_ms"].as_u64().unwrap() >= 20,
        "elapsed time must include the real held request interval"
    );
    batch::excludes(
        &owned,
        &[
            "private-dropped-wait",
            "private-dropped-input",
            "private-native-output",
            ROUTE,
        ],
    );
    let busy = response(
        &host,
        "DELETE",
        "/threads/private-dropped-wait",
        Some(AUTHORIZATION),
        "",
    )
    .await;
    assert!(
        busy.status() == StatusCode::CONFLICT && body(busy).await == error_body("run_in_flight"),
        "dropped request must retain actual same-thread exclusion while effect is held"
    );
    let capacity = response(
        &host,
        "POST",
        "/threads/private-capacity-blocked/runs/wait",
        Some(AUTHORIZATION),
        &run_input(Value::Null),
    )
    .await;
    assert!(
        capacity.status() == StatusCode::SERVICE_UNAVAILABLE
            && body(capacity).await == error_body("capacity_exhausted"),
        "dropped request must retain actual global capacity while effect is held"
    );
    assert!(store
        .thread_view(&ThreadId::new("private-capacity-blocked").unwrap())
        .unwrap()
        .is_none());
    let shutdown = host.shutdown_handle();
    shutdown.begin_shutdown();
    assert!(matches!(
        shutdown.wait_quiescent(Duration::ZERO).await.unwrap(),
        b4_http::ShutdownOutcome::Incomplete {
            active_owners: 1,
            ..
        }
    ));
    drop(host);
    drop(store);
    assert!(
        weak.upgrade().is_some() && weak_application.upgrade().is_some(),
        "actual owned work must retain resources through effect hold"
    );
    gate.release();
    assert!(matches!(
        shutdown
            .wait_quiescent(Duration::from_secs(5))
            .await
            .unwrap(),
        b4_http::ShutdownOutcome::Quiescent { faulted: false }
    ));
    until(|| weak.upgrade().is_none() && weak_application.upgrade().is_none()).await;
    assert!(
        batch::validate(&owned) == value,
        "owned redacted bytes must remain usable after actual resources retire"
    );
    let (_, remaining) = batch::drain(&diagnostics);
    assert!(
        remaining["records"]
            .as_array()
            .unwrap()
            .iter()
            .all(|record| record["event"] == "response_ready"),
        "late worker completion must not duplicate the consumed request-drop observation"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn loopback_response_and_refusal_headers_match_encoded_events() {
    let host = memory_host();
    let server = tcp::Server::start(&host).await;
    let run_body = run_input(json!("private-tcp-input"));
    for (method, path, authorization, input, status, expected, label) in [
        (
            "GET",
            "/healthz",
            None,
            "",
            200,
            json!({"status":"ok"}),
            "get",
        ),
        (
            "POST",
            "/threads",
            None,
            "{}",
            401,
            error_body("authentication_required"),
            "post",
        ),
        (
            "POST",
            "/threads/private-tcp-run/runs/wait",
            Some(AUTHORIZATION),
            run_body.as_str(),
            200,
            json!("private-tcp-input"),
            "post",
        ),
    ] {
        let result = tcp::exchange(server.address, method, path, authorization, input).await;
        assert!(
            result.status == status
                && serde_json::from_slice::<Value>(&result.body).unwrap() == expected
        );
        let id = result
            .headers
            .get("x-b4-request-id")
            .expect("actual loopback response must contain the server request reference");
        batch::assert_id(id);
        let (owned, value) = batch::drain(&host.diagnostics());
        assert!(value["records"].as_array().unwrap().len() == 1);
        batch::response_record(&value["records"][0], id, label, status);
        batch::excludes(
            &owned,
            &[
                "private-tcp-run",
                "private-tcp-input",
                "private-tcp-caller-reference",
                "private-tcp-cookie",
                AUTHORIZATION,
            ],
        );
    }
    server.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn loopback_sse_header_and_record_arrive_while_native_effect_is_held() {
    let gate = Arc::new(Gate::default());
    let _release = Release(gate.clone());
    let store = Arc::new(MemoryStore::new());
    let (host, _) = native_host(store.clone(), Some(gate.clone()), None);
    let server = tcp::Server::start(&host).await;
    let mut socket = tcp::open(
        server.address,
        "POST",
        "/threads/private-tcp-held/runs/stream",
        Some(AUTHORIZATION),
        &run_input(json!("private-tcp-stream-input")),
    )
    .await;
    let first = tcp::prefix(&mut socket, b"\r\n\r\n").await;
    let (status, headers) = tcp::headers(&first);
    assert!(status == 200 && headers["content-type"].starts_with("text/event-stream"));
    let id = &headers["x-b4-request-id"];
    batch::assert_id(id);
    gate.entered().await;
    assert!(
        store
            .thread_view(&ThreadId::new("private-tcp-held").unwrap())
            .unwrap()
            .unwrap()
            .execution
            .unwrap()
            .snapshot
            .tasks[0]
            .status
            == b4_contract::TaskStatus::InFlight
    );
    let (owned, value) = batch::drain(&host.diagnostics());
    assert!(value["records"].as_array().unwrap().len() == 1);
    batch::response_record(&value["records"][0], id, "post", 200);
    batch::excludes(
        &owned,
        &[
            "private-tcp-held",
            "private-tcp-stream-input",
            "private-native-output",
            AUTHORIZATION,
        ],
    );
    gate.release();
    let finished = tcp::finish(socket, first).await;
    let text = std::str::from_utf8(&finished.body).unwrap();
    assert!(
        text.lines().filter(|line| *line == "event: done").count() == 1,
        "actual held native SSE must retain its selected terminal framing"
    );
    assert!(
        batch::drain(&host.diagnostics()).1["records"]
            .as_array()
            .unwrap()
            .is_empty(),
        "body completion must not create a new request-boundary event"
    );
    server.stop().await;
    assert!(matches!(
        host.shutdown_handle()
            .wait_quiescent(Duration::from_secs(5))
            .await
            .unwrap(),
        b4_http::ShutdownOutcome::Quiescent { faulted: false }
    ));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn full_buffer_and_live_consumer_do_not_retain_retired_sqlite_server() {
    use std::sync::{
        atomic::{AtomicBool, Ordering},
        mpsc,
    };
    struct Stop(Arc<AtomicBool>);
    impl Drop for Stop {
        fn drop(&mut self) {
            self.0.store(true, Ordering::Release);
        }
    }
    let directory = Directory::new();
    let store = Arc::new(b4_sqlite::SqliteStore::open(&directory.0).unwrap());
    let weak = Arc::downgrade(&store);
    let host = scripted_host(
        store.clone(),
        HostConfig::new(vec![script_route(ScriptEffect::Input, false)]),
    );
    let diagnostics = host.diagnostics();
    for _ in 0..256 {
        assert!(response(&host, "GET", "/healthz", None, "").await.status() == StatusCode::OK);
    }
    let (owned, value) = batch::drain(&diagnostics);
    assert!(value["records"].as_array().unwrap().len() == 256);
    for _ in 0..256 {
        response(&host, "GET", "/healthz", None, "").await;
    }
    let server = tcp::Server::start(&host).await;
    assert!(
        matches!(
            b4_sqlite::SqliteStore::open(&directory.0),
            Err(b4_runtime::StoreError::AlreadyOwned)
        ),
        "actual live host/server must exclude a second writer"
    );
    let stop = Arc::new(AtomicBool::new(false));
    let _stop_on_failure = Stop(stop.clone());
    let consumer_stop = stop.clone();
    let consumer_handle = diagnostics.clone();
    let (entered_tx, entered_rx) = mpsc::channel();
    let (done_tx, done_rx) = mpsc::channel();
    let consumer = std::thread::spawn(move || {
        let first = consumer_handle
            .drain()
            .expect("live consumer must actually take the full ring");
        assert!(batch::validate(&first)["records"].as_array().unwrap().len() == 256);
        entered_tx.send(()).unwrap();
        while !consumer_stop.load(Ordering::Acquire) {
            match consumer_handle.drain() {
                Ok(value) => {
                    batch::validate(&value);
                }
                Err(error) => assert!(error.code() == "diagnostics_busy"),
            }
            std::thread::yield_now();
        }
        done_tx.send(()).unwrap();
    });
    entered_rx
        .recv_timeout(Duration::from_secs(5))
        .expect("actual full-ring consumer must begin before resource retirement");
    let ready = tcp::exchange(server.address, "GET", "/readyz", None, "").await;
    assert!(
        ready.status == 200
            && serde_json::from_slice::<Value>(&ready.body).unwrap() == json!({"status":"ready"}),
        "diagnostic consumer contention must not change host readiness"
    );
    let shutdown = host.shutdown_handle();
    shutdown.begin_shutdown();
    server.stop().await;
    drop(host);
    drop(store);
    assert!(matches!(
        shutdown
            .wait_quiescent(Duration::from_secs(5))
            .await
            .unwrap(),
        b4_http::ShutdownOutcome::Quiescent { faulted: false }
    ));
    until(|| weak.upgrade().is_none()).await;
    let writer = b4_sqlite::SqliteStore::open(&directory.0);
    assert!(writer.is_ok(),"actual writer must reacquire after positive server and host retirement with handle/batch/consumer alive");
    assert!(writer.unwrap().readiness().is_ok());
    assert!(batch::validate(&owned) == value);
    stop.store(true, Ordering::Release);
    done_rx
        .recv_timeout(Duration::from_secs(5))
        .expect("resource-free diagnostics consumer must end within fixture bound");
    consumer.join().unwrap();
    batch::drain(&diagnostics);
}

#[tokio::test]
async fn settled_application_failure_keeps_body_reason_out_of_records() {
    let host = scripted_host(
        Arc::new(MemoryStore::new()),
        HostConfig::new(vec![script_route(
            ScriptEffect::Failure("private-application-failure-reason".into()),
            false,
        )]),
    );
    let result = response(
        &host,
        "POST",
        "/threads/private-failed-thread/runs/wait",
        Some(AUTHORIZATION),
        &run_input(json!("private-failed-input")),
    )
    .await;
    let id = batch::header(&result);
    assert!(
        result.status() == StatusCode::INTERNAL_SERVER_ERROR
            && body(result).await
                == json!({"error":{"kind":"execution_error","message":"private-application-failure-reason"}}),
        "settled application failure must preserve its established execution error envelope"
    );
    let (owned, value) = batch::drain(&host.diagnostics());
    assert!(value["records"].as_array().unwrap().len() == 1);
    batch::response_record(&value["records"][0], &id, "post", 500);
    batch::excludes(
        &owned,
        &[
            "private-application-failure-reason",
            "private-failed-input",
            "private-failed-thread",
            ROUTE,
            AUTHORIZATION,
        ],
    );
    assert!(response(&host,"GET","/readyz",None,"").await.status()==StatusCode::OK,"settled application failure must not become a diagnostic or readiness infrastructure fault");
}

#[tokio::test]
async fn native_input_validation_refusal_is_correlated_without_private_input() {
    struct Reject;
    impl b4_runtime::InputValidator for Reject {
        fn validate(&self, _: &Value) -> Result<(), b4_runtime::InvalidApplicationInput> {
            Err(b4_runtime::InvalidApplicationInput)
        }
    }
    let (host, _) = native_host(Arc::new(MemoryStore::new()), None, Some(Arc::new(Reject)));
    let result = response(
        &host,
        "POST",
        "/threads/private-invalid-native/runs/wait",
        Some(AUTHORIZATION),
        &run_input(json!({"private-validator-input":"private-validator-value"})),
    )
    .await;
    let id = batch::header(&result);
    assert!(
        result.status() == StatusCode::BAD_REQUEST
            && body(result).await == error_body("invalid_application_input"),
        "native validator refusal must keep its established static error envelope"
    );
    assert!(
        host.store()
            .thread_view(&ThreadId::new("private-invalid-native").unwrap())
            .unwrap()
            .is_none(),
        "native refusal must precede actual thread admission"
    );
    let (owned, value) = batch::drain(&host.diagnostics());
    assert!(value["records"].as_array().unwrap().len() == 1);
    batch::response_record(&value["records"][0], &id, "post", 400);
    batch::excludes(
        &owned,
        &[
            "private-validator-input",
            "private-validator-value",
            "private-invalid-native",
            ROUTE,
            "private-application-revision",
            "private-policy-revision",
            AUTHORIZATION,
        ],
    );
}
