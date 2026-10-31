use super::*;
use axum::{body::Body, http::StatusCode, response::Response};
use serde_json::Value;

fn exported(diagnostics: &RequestDiagnostics) -> Value {
    serde_json::from_slice(diagnostics.drain().unwrap().as_ref()).unwrap()
}

fn seeded() -> RequestDiagnostics {
    RequestDiagnostics::from_entropy(Ok([0xa5; 16])).unwrap()
}

fn response() -> Response {
    Response::builder()
        .status(StatusCode::NO_CONTENT)
        .body(Body::empty())
        .unwrap()
}

#[test]
fn finished_observation_adds_the_server_reference_to_a_real_response() {
    let diagnostics = RequestDiagnostics::new().unwrap();
    let mut response = response();
    diagnostics.start(&Method::GET).finish(&mut response);
    assert_eq!(response.status(), StatusCode::NO_CONTENT);
    assert!(
        response.headers().contains_key(REQUEST_ID_HEADER),
        "finished observation must add the server reference"
    );
}

#[test]
fn finished_observation_exports_one_correlated_fixed_record() {
    let diagnostics = RequestDiagnostics::new().unwrap();
    let mut response = response();
    diagnostics.start(&Method::GET).finish(&mut response);
    let batch = diagnostics.drain();
    assert!(batch.is_ok(), "one finished observation must be exportable");
    let batch = batch.unwrap();
    let json: Value = serde_json::from_slice(batch.as_ref()).unwrap();
    assert_eq!(json["records"].as_array().unwrap().len(), 1);
    assert_eq!(json["records"][0]["event"], "response_ready");
    assert_eq!(json["records"][0]["method"], "get");
    assert_eq!(json["records"][0]["status_code"], 204);
    assert_eq!(
        json["records"][0]["request_id"],
        response.headers()[REQUEST_ID_HEADER].to_str().unwrap()
    );
}

#[test]
fn event_257_evicts_only_the_oldest_and_drain_preserves_loss_metadata() {
    let diagnostics = RequestDiagnostics::new().unwrap();
    let mut ids = Vec::new();
    for _ in 0..=REQUEST_DIAGNOSTIC_CAPACITY {
        let mut response = response();
        diagnostics.start(&Method::POST).finish(&mut response);
        ids.push(response.headers().get(REQUEST_ID_HEADER).cloned());
    }
    let batch = diagnostics.drain();
    assert!(batch.is_ok(), "a full bounded ring must be exportable");
    let json: Value = serde_json::from_slice(batch.unwrap().as_ref()).unwrap();
    let records = json["records"].as_array().unwrap();
    assert_eq!(records.len(), REQUEST_DIAGNOSTIC_CAPACITY);
    assert_eq!(
        records[0]["request_id"],
        ids[1].as_ref().unwrap().to_str().unwrap()
    );
    assert_eq!(
        records.last().unwrap()["request_id"],
        ids.last().unwrap().as_ref().unwrap().to_str().unwrap()
    );
    assert_eq!(json["record_loss"], true);
    assert_eq!(json["total_evicted_records"], 1);
    let empty: Value = serde_json::from_slice(diagnostics.drain().unwrap().as_ref()).unwrap();
    assert_eq!(empty["records"], serde_json::json!([]));
    assert_eq!(empty["record_loss"], true);
    assert_eq!(empty["total_evicted_records"], 1);
}

#[test]
fn dropped_pending_observation_records_one_null_status_without_response() {
    let diagnostics = RequestDiagnostics::new().unwrap();
    drop(diagnostics.start(&Method::DELETE));
    let batch = diagnostics.drain();
    assert!(batch.is_ok(), "a dropped pending guard must be exportable");
    let json: Value = serde_json::from_slice(batch.unwrap().as_ref()).unwrap();
    assert_eq!(json["records"].as_array().unwrap().len(), 1);
    assert_eq!(json["records"][0]["event"], "request_future_dropped");
    assert_eq!(json["records"][0]["method"], "delete");
    assert_eq!(json["records"][0]["status_code"], Value::Null);
}

#[test]
fn actual_unwind_consumes_pending_guard_once_without_retaining_panic_text() {
    let diagnostics = seeded();
    let unwind = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let _pending = diagnostics.start(&Method::PATCH);
        panic!("private unwind text must never enter records");
    }));
    assert!(unwind.is_err());
    let batch = diagnostics.drain().unwrap();
    let json: Value = serde_json::from_slice(batch.as_ref()).unwrap();
    assert_eq!(json["records"].as_array().unwrap().len(), 1);
    assert_eq!(json["records"][0]["event"], "request_future_dropped");
    assert_eq!(json["records"][0]["status_code"], Value::Null);
    assert!(!std::str::from_utf8(batch.as_ref())
        .unwrap()
        .contains("private unwind text"));
    assert_eq!(exported(&diagnostics)["records"], serde_json::json!([]));
}

#[test]
fn method_projection_has_only_four_labels_and_clones_share_destructive_drain() {
    let diagnostics = RequestDiagnostics::new().unwrap();
    let clone = diagnostics.clone();
    for method in [Method::GET, Method::POST, Method::DELETE, Method::PATCH] {
        diagnostics.start(&method).finish(&mut response());
    }
    let batch = clone.drain();
    assert!(batch.is_ok(), "the clone must export the shared ring");
    let json: Value = serde_json::from_slice(batch.unwrap().as_ref()).unwrap();
    let labels: Vec<_> = json["records"]
        .as_array()
        .unwrap()
        .iter()
        .map(|record| record["method"].as_str().unwrap())
        .collect();
    assert_eq!(labels, ["get", "post", "delete", "other"]);
    let empty: Value = serde_json::from_slice(diagnostics.drain().unwrap().as_ref()).unwrap();
    assert_eq!(empty["records"], serde_json::json!([]));
}

#[test]
fn insertion_order_follows_finish_rather_than_allocated_request_counter() {
    let diagnostics = RequestDiagnostics::new().unwrap();
    let first = diagnostics.start(&Method::GET);
    let second = diagnostics.start(&Method::POST);
    let mut first_response = response();
    let mut second_response = response();
    second.finish(&mut second_response);
    first.finish(&mut first_response);
    let batch = diagnostics.drain();
    assert!(batch.is_ok(), "completed observations must be exportable");
    let json: Value = serde_json::from_slice(batch.unwrap().as_ref()).unwrap();
    assert_eq!(json["records"][0]["method"], "post");
    assert_eq!(json["records"][1]["method"], "get");
    assert_eq!(
        json["records"][0]["request_id"],
        second_response.headers()[REQUEST_ID_HEADER]
            .to_str()
            .unwrap()
    );
    assert_eq!(
        json["records"][1]["request_id"],
        first_response.headers()[REQUEST_ID_HEADER]
            .to_str()
            .unwrap()
    );
}

#[test]
fn empty_export_has_exact_fixed_metadata_and_no_newline() {
    let diagnostics = RequestDiagnostics::new().unwrap();
    let batch = diagnostics.drain();
    assert!(
        batch.is_ok(),
        "an empty ring must export its fixed metadata"
    );
    let batch = batch.unwrap();
    let expected = serde_json::json!({
        "diagnostic_batch_version":1,
        "scope":"request_boundary",
        "redacted":true,
        "capacity":256,
        "record_loss":false,
        "total_evicted_records":0,
        "correlation_exhausted":false,
        "records":[]
    });
    assert_eq!(
        serde_json::from_slice::<Value>(batch.as_ref()).unwrap(),
        expected
    );
    assert!(!batch.as_ref().contains(&b'\n'));
    assert!(batch.as_ref().len() <= MAX_DIAGNOSTIC_BATCH_BYTES);
}

#[test]
fn reader_errors_have_only_the_exact_static_codes() {
    for (error, code) in [
        (DiagnosticsReadError::Busy, "diagnostics_busy"),
        (DiagnosticsReadError::Unavailable, "diagnostics_unavailable"),
        (
            DiagnosticsReadError::EncodeFailed,
            "diagnostics_encode_failed",
        ),
        (
            DiagnosticsReadError::LimitExceeded,
            "diagnostics_limit_exceeded",
        ),
    ] {
        assert_eq!(error.code(), code);
        assert_eq!(error.to_string(), code);
        assert_eq!(format!("{error:?}"), code);
    }
}

#[test]
fn held_real_ring_lock_drops_producers_but_keeps_headers_and_queued_records() {
    let diagnostics = seeded();
    diagnostics.start(&Method::GET).finish(&mut response());
    let lock = diagnostics.inner.ring.lock().unwrap();
    let mut ready = response();
    diagnostics.start(&Method::POST).finish(&mut ready);
    drop(diagnostics.start(&Method::DELETE));
    assert!(ready.headers().contains_key(REQUEST_ID_HEADER));
    assert_eq!(diagnostics.drain().err(), Some(DiagnosticsReadError::Busy));
    drop(lock);
    let json = exported(&diagnostics);
    assert_eq!(json["records"].as_array().unwrap().len(), 1);
    assert_eq!(json["records"][0]["method"], "get");
    assert_eq!(json["record_loss"], true);
    assert_eq!(json["total_evicted_records"], 0);
}

#[test]
fn actually_poisoned_ring_refuses_drain_without_touching_existing_records() {
    let diagnostics = seeded();
    diagnostics.start(&Method::GET).finish(&mut response());
    let poisoned = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let _lock = diagnostics.inner.ring.lock().unwrap();
        panic!("private diagnostic ring poison fixture");
    }));
    assert!(poisoned.is_err());
    assert_eq!(
        diagnostics.drain().err(),
        Some(DiagnosticsReadError::Unavailable)
    );
    let mut ready = response();
    diagnostics.start(&Method::POST).finish(&mut ready);
    assert!(ready.headers().contains_key(REQUEST_ID_HEADER));
    assert!(diagnostics.inner.record_loss.load(Ordering::Relaxed));
    let retained = diagnostics
        .inner
        .ring
        .lock()
        .err()
        .unwrap()
        .into_inner()
        .take();
    assert_eq!(retained.len, 1);
    assert_eq!(retained.total_evicted_records, 0);
}

#[test]
fn final_u64_counter_is_emitted_once_then_exhaustion_omits_headers_and_events() {
    let diagnostics = seeded();
    diagnostics
        .inner
        .counter
        .store(u64::MAX - 1, Ordering::Relaxed);
    let mut final_response = response();
    diagnostics.start(&Method::GET).finish(&mut final_response);
    assert_eq!(
        final_response.headers()[REQUEST_ID_HEADER]
            .to_str()
            .unwrap(),
        format!("b4r_{}_ffffffffffffffff", "a5".repeat(16))
    );
    assert!(!diagnostics
        .inner
        .correlation_exhausted
        .load(Ordering::Relaxed));
    let mut exhausted_response = response();
    diagnostics
        .start(&Method::POST)
        .finish(&mut exhausted_response);
    drop(diagnostics.start(&Method::DELETE));
    assert!(!exhausted_response.headers().contains_key(REQUEST_ID_HEADER));
    assert_eq!(diagnostics.inner.counter.load(Ordering::Relaxed), u64::MAX);
    let json = exported(&diagnostics);
    assert_eq!(json["records"].as_array().unwrap().len(), 1);
    assert_eq!(json["record_loss"], true);
    assert_eq!(json["correlation_exhausted"], true);
    assert_eq!(exported(&diagnostics)["correlation_exhausted"], true);
}

#[test]
fn concurrent_checked_sequence_allocation_has_no_duplicates_or_zero_counter() {
    let diagnostics = seeded();
    let workers: Vec<_> = (0..16)
        .map(|_| {
            let diagnostics = diagnostics.clone();
            std::thread::spawn(move || {
                (0..32)
                    .map(|_| diagnostics.inner.next_id().unwrap().0)
                    .collect::<Vec<_>>()
            })
        })
        .collect();
    let mut ids: Vec<_> = workers
        .into_iter()
        .flat_map(|worker| worker.join().unwrap())
        .collect();
    ids.sort_unstable();
    ids.dedup();
    assert_eq!(ids.len(), 512);
    assert!(std::str::from_utf8(&ids[0])
        .unwrap()
        .ends_with("_0000000000000001"));
    assert!(std::str::from_utf8(ids.last().unwrap())
        .unwrap()
        .ends_with("_0000000000000200"));
}

#[test]
fn cumulative_evictions_saturate_and_survive_repeated_destructive_drains() {
    let diagnostics = seeded();
    diagnostics.inner.ring.lock().unwrap().total_evicted_records = u64::MAX - 1;
    for _ in 0..REQUEST_DIAGNOSTIC_CAPACITY + 2 {
        drop(diagnostics.start(&Method::GET));
    }
    let json = exported(&diagnostics);
    assert_eq!(
        json["records"].as_array().unwrap().len(),
        REQUEST_DIAGNOSTIC_CAPACITY
    );
    assert_eq!(json["total_evicted_records"], u64::MAX);
    assert_eq!(json["record_loss"], true);
    assert_eq!(exported(&diagnostics)["total_evicted_records"], u64::MAX);
}

#[test]
fn private_entropy_refusal_maps_only_to_static_configuration_error() {
    let error = RequestDiagnostics::from_entropy(Err(())).err().unwrap();
    assert_eq!(error.0, "diagnostics_random_unavailable");
    assert_eq!(error.to_string(), "diagnostics_random_unavailable");
    // This injects a diagnostic constructor refusal; it is not an OS outage.
    let actual = RequestDiagnostics::new().unwrap();
    assert_eq!(exported(&actual)["records"], serde_json::json!([]));
}

#[test]
fn elapsed_duration_truncates_to_milliseconds_and_saturates_u64() {
    assert_eq!(
        observation::elapsed_ms(std::time::Duration::from_nanos(999_999)),
        0
    );
    assert_eq!(
        observation::elapsed_ms(std::time::Duration::from_micros(1999)),
        1
    );
    assert_eq!(
        observation::elapsed_ms(std::time::Duration::new(u64::MAX, 999_999_999)),
        u64::MAX
    );
}

#[test]
fn largest_private_fixed_dto_fits_and_capped_serializer_accepts_n_refuses_n_minus_one() {
    use buffer::{Event, MethodLabel};
    let record = Record {
        request_id: RequestId(*b"b4r_ffffffffffffffffffffffffffffffff_ffffffffffffffff"),
        event: Event::RequestFutureDropped,
        method: MethodLabel::Delete,
        status_code: None,
        elapsed_ms: u64::MAX,
    };
    let taken = Taken {
        records: [Some(record); REQUEST_DIAGNOSTIC_CAPACITY],
        len: REQUEST_DIAGNOSTIC_CAPACITY,
        total_evicted_records: u64::MAX,
    };
    let batch = BatchDto {
        diagnostic_batch_version: 1,
        scope: "request_boundary",
        redacted: true,
        capacity: REQUEST_DIAGNOSTIC_CAPACITY,
        record_loss: false,
        total_evicted_records: u64::MAX,
        correlation_exhausted: false,
        records: Records(&taken),
    };
    let bytes = encode(&batch, MAX_DIAGNOSTIC_BATCH_BYTES).unwrap();
    assert_eq!(bytes.as_ref().len(), 44_995);
    assert_eq!(encode(&batch, 44_995).unwrap().as_ref(), bytes.as_ref());
    assert_eq!(
        encode(&batch, 44_994).err(),
        Some(DiagnosticsReadError::LimitExceeded)
    );
    // This is a bounded serializer DTO proof, not an actual elapsed-time fixture.
}

#[test]
fn destructive_encoding_limit_marks_loss_and_does_not_restore_taken_records() {
    let diagnostics = seeded();
    diagnostics.start(&Method::GET).finish(&mut response());
    assert_eq!(
        diagnostics.drain_with_cap(0).err(),
        Some(DiagnosticsReadError::LimitExceeded)
    );
    let json = exported(&diagnostics);
    assert_eq!(json["records"], serde_json::json!([]));
    assert_eq!(json["record_loss"], true);
    assert_eq!(json["total_evicted_records"], 0);
}

#[test]
fn private_invalid_id_encoding_is_static_destructive_and_distinct_from_output_cap() {
    let diagnostics = seeded();
    diagnostics.inner.observe(Record {
        request_id: RequestId([0xff; 53]),
        event: buffer::Event::RequestFutureDropped,
        method: buffer::MethodLabel::Other,
        status_code: None,
        elapsed_ms: 0,
    });
    assert_eq!(
        diagnostics.drain().err(),
        Some(DiagnosticsReadError::EncodeFailed)
    );
    let json = exported(&diagnostics);
    assert_eq!(json["records"], serde_json::json!([]));
    assert_eq!(json["record_loss"], true);
    // Public producers cannot construct this record; this is a private refusal seam.
}

#[tokio::test]
async fn actual_header_map_capacity_refusal_consumes_observation_without_fabricated_drop() {
    use axum::{
        body::to_bytes,
        http::{HeaderName, HeaderValue},
    };
    let diagnostics = seeded();
    let mut response = Response::builder()
        .status(StatusCode::NO_CONTENT)
        .body(Body::from("safe body"))
        .unwrap();
    let mut full = false;
    for index in 0..65_536 {
        let name = HeaderName::from_bytes(format!("x-private-fixture-{index}").as_bytes()).unwrap();
        if response
            .headers_mut()
            .try_insert(name, HeaderValue::from_static("v"))
            .is_err()
        {
            full = true;
            break;
        }
    }
    assert!(
        full,
        "test must reach the actual HeaderMap capacity refusal"
    );
    diagnostics.start(&Method::GET).finish(&mut response);
    assert_eq!(response.status(), StatusCode::NO_CONTENT);
    assert!(!response.headers().contains_key(REQUEST_ID_HEADER));
    assert_eq!(
        to_bytes(response.into_body(), 32).await.unwrap().as_ref(),
        b"safe body"
    );
    let json = exported(&diagnostics);
    assert_eq!(json["records"], serde_json::json!([]));
    assert_eq!(json["record_loss"], true);
    assert_eq!(json["correlation_exhausted"], false);
}

#[test]
fn direct_endpoint_held_finish_has_no_extra_store_capture_in_the_new_wrapper() {
    use crate::*;
    use axum::{
        extract::State,
        http::{HeaderMap, Request},
    };
    use b4_contract::*;
    use b4_runtime::MemoryStore;
    use std::{collections::BTreeMap, sync::mpsc, time::Duration};

    struct Auth;
    impl Authenticator for Auth {
        fn authenticate(&self, _: &HeaderMap) -> Result<Principal, AuthenticationError> {
            Ok(Principal {
                application: "fixture".into(),
                tenant: "fixture".into(),
                principal: "fixture".into(),
            })
        }
    }
    struct Policy;
    impl ResourcePolicy for Policy {
        fn authorize(&self, _: &Principal, _: &ResourceContext) -> bool {
            true
        }
    }
    struct Release(Arc<observation::FinishGate>);
    impl Drop for Release {
        fn drop(&mut self) {
            self.0.release();
        }
    }

    let store = Arc::new(MemoryStore::new());
    let weak = Arc::downgrade(&store);
    let effect_id = EffectId::new("effect").unwrap();
    let config = HostConfig::new(vec![ScriptRoute {
        definition: Definition {
            route: RouteKey::new("/weak#workflow").unwrap(),
            revision: ExecutionRevision::new("v1").unwrap(),
            batches: vec![vec![TaskSpec {
                id: TaskId::new("task").unwrap(),
                effect_id: effect_id.clone(),
                recovery: RecoveryPolicy::Pure,
                requires_approval: false,
            }]],
            max_attempts: 1,
        },
        effects: BTreeMap::from([(effect_id, ScriptEffect::Void)]),
    }]);
    let host = HttpHost::new(store.clone(), Arc::new(Auth), Arc::new(Policy), config).unwrap();
    let diagnostics = host.diagnostics();
    let gate = diagnostics.hold_finish();
    let release = Release(gate.clone());
    let shared = host.shared.clone();
    let (completed_tx, completed_rx) = mpsc::channel();
    let worker = std::thread::spawn(move || {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let response = runtime.block_on(crate::transport::endpoint(
            State(shared),
            Request::builder()
                .uri("/healthz")
                .body(Body::empty())
                .unwrap(),
        ));
        let _ = completed_tx.send(response);
    });
    drop(host);
    drop(store);
    assert!(
        gate.wait_entered(),
        "direct endpoint must reach the private finite finish gate"
    );
    assert!(
        weak.upgrade().is_none(),
        "new wrapper must retire its Shared before resource-free finish"
    );
    drop(release);
    let response = completed_rx.recv_timeout(Duration::from_secs(15)).unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert!(response.headers().contains_key(REQUEST_ID_HEADER));
    worker.join().unwrap();
    assert!(
        gate.was_explicitly_released(),
        "finish must exit by explicit release, never by finite gate timeout"
    );
    let batch = diagnostics.drain().unwrap();
    assert_eq!(
        serde_json::from_slice::<Value>(batch.as_ref()).unwrap()["records"]
            .as_array()
            .unwrap()
            .len(),
        1
    );
    assert!(weak.upgrade().is_none());
    // This calls the direct wrapper, not Router/axum Handler::call state retention.
}

#[test]
fn finite_finish_gate_distinguishes_expiry_from_explicit_release() {
    let expired = observation::FinishGate::new();
    expired.pause_for(std::time::Duration::ZERO);
    expired.release();
    assert!(
        !expired.was_explicitly_released(),
        "release after finite timeout must not turn expiry into a held-finish witness"
    );

    let released = observation::FinishGate::new();
    released.release();
    released.pause_for(std::time::Duration::ZERO);
    assert!(released.was_explicitly_released());
}
