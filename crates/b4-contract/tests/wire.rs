use b4_contract::{
    heartbeat, ApError, ApprovalDecision, ApprovalGrant, ErrorEnvelope, ErrorKind, ResumeEntry,
    ResumeRequest, RouteKey, RunRequest, SseEvent, Thread, ThreadId, ThreadStatus,
};
use serde_json::{json, Map};

#[test]
fn run_input_default_does_not_erase_present_null_or_scalars() {
    let omitted: RunRequest = serde_json::from_value(json!({"route":"/test#agent"})).unwrap();
    assert_eq!(omitted.input, json!({}));
    for input in [
        json!(null),
        json!("snow\n雪"),
        json!(42),
        json!(true),
        json!([1, 2]),
        json!({"x":1}),
    ] {
        let request: RunRequest =
            serde_json::from_value(json!({"route":"/test#agent","input":input})).unwrap();
        assert_eq!(request.input, input);
    }
}

#[test]
fn run_control_fields_are_required_and_strict() {
    for body in [
        json!({}),
        json!({"route":null}),
        json!({"route":"/test"}),
        json!({"route":"/test#agent","owner_generation":3}),
    ] {
        assert!(serde_json::from_value::<RunRequest>(body).is_err());
    }
}

#[test]
fn resume_accepts_resolved_decisions_and_cancelled_without_payload() {
    for payload in ["once", "always", "deny"] {
        let body = json!({"route":"/test#agent","resume":[{"interruptId":"i-1","status":"resolved","payload":payload,"grant":"b4ag_secret"}]});
        let request: ResumeRequest = serde_json::from_value(body.clone()).unwrap();
        assert_eq!(request.route, RouteKey::new("/test#agent").unwrap());
        assert_eq!(serde_json::to_value(request).unwrap(), body);
    }
    let body = json!({"route":"/test#agent","resume":[{"interruptId":"i-1","status":"cancelled"}]});
    let request: ResumeRequest = serde_json::from_value(body.clone()).unwrap();
    assert!(matches!(&request.resume[0], ResumeEntry::Cancelled { .. }));
    assert_eq!(request.resume[0].decision(), ApprovalDecision::Deny);
    assert_eq!(request.resume[0].interrupt_id().as_str(), "i-1");
    assert_eq!(serde_json::to_value(request).unwrap(), body);
}

#[test]
fn resume_rejects_legacy_shapes_unknown_fields_and_invalid_payloads() {
    let invalid = [
        json!({"interrupt_id":"i-1","decision":"once"}),
        json!({"resume":[]}),
        json!({"route":"/test#agent","resume":[],"execution_id":"e-1"}),
        json!({"route":"/test#agent","resume":[{"interruptId":"i-1","status":"resolved"}]}),
        json!({"route":"/test#agent","resume":[{"interruptId":"i-1","status":"resolved","payload":null}]}),
        json!({"route":"/test#agent","resume":[{"interruptId":"i-1","status":"resolved","payload":"yes"}]}),
        json!({"route":"/test#agent","resume":[{"interruptId":"i-1","status":"cancelled","payload":"deny"}]}),
        json!({"route":"/test#agent","resume":[{"interruptId":"i-1","status":"cancelled","payload":null}]}),
        json!({"route":"/test#agent","resume":[{"interruptId":"i-1","status":"resolved","payload":"once","grant":null}]}),
        json!({"route":"/test#agent","resume":[{"interruptId":"i-1","status":"resolved","payload":"once","owner":1}]}),
    ];
    for body in invalid {
        assert!(
            serde_json::from_value::<ResumeRequest>(body.clone()).is_err(),
            "accepted {body}"
        );
    }
}

#[test]
fn resume_requires_at_least_one_decision() {
    assert!(
        serde_json::from_value::<ResumeRequest>(json!({"route":"/test#agent","resume":[]}))
            .is_err()
    );
}

#[test]
fn grant_debug_never_discloses_sensitive_value() {
    let grant = ApprovalGrant::new("b4ag_sensitive-value").unwrap();
    assert_eq!(grant.expose(), "b4ag_sensitive-value");
    assert!(!format!("{grant:?}").contains("sensitive-value"));
    let request: ResumeRequest = serde_json::from_value(json!({"route":"/test#agent","resume":[{"interruptId":"i-1","status":"resolved","payload":"once","grant":"b4ag_sensitive-value"}]})).unwrap();
    assert!(!format!("{request:?}").contains("sensitive-value"));
    for invalid in ["", "b4ag_\nsecret"] {
        assert!(ApprovalGrant::new(invalid).is_err());
        assert!(serde_json::from_value::<ApprovalGrant>(json!(invalid)).is_err());
    }
}

#[test]
fn sse_frames_use_fixed_events_and_direct_json_payloads() {
    let events = [
        (
            SseEvent::Chunk(json!("line\n雪\"")),
            "chunk",
            json!("line\n雪\""),
        ),
        (
            SseEvent::Chunk(json!({"value":true})),
            "chunk",
            json!({"value":true}),
        ),
        (
            SseEvent::ToolCall {
                id: "call-1".into(),
                name: "search".into(),
                input: json!({"q":"b4"}),
            },
            "tool_call",
            json!({"id":"call-1","name":"search","input":{"q":"b4"}}),
        ),
        (
            SseEvent::ToolResult {
                id: "call-1".into(),
                name: "search".into(),
                output: json!({"ok":true}),
            },
            "tool_result",
            json!({"id":"call-1","name":"search","output":{"ok":true}}),
        ),
        (
            SseEvent::Done {
                output: json!(null),
            },
            "done",
            json!({"output":null}),
        ),
        (
            SseEvent::Done {
                output: json!({"cancelled":true}),
            },
            "done",
            json!({"output":{"cancelled":true}}),
        ),
    ];
    for (event, name, payload) in events {
        let encoded = event.encode().unwrap();
        assert!(encoded.starts_with(&format!("event: {name}\ndata: ")));
        assert!(encoded.ends_with("\n\n"));
        assert_eq!(encoded.lines().count(), 3);
        let data = encoded
            .split_once("data: ")
            .unwrap()
            .1
            .trim_end_matches('\n');
        assert_eq!(
            serde_json::from_str::<serde_json::Value>(data).unwrap(),
            payload
        );
    }
    assert_eq!(heartbeat(), ": ping\n\n");
}

#[test]
fn thread_status_and_error_envelope_keep_the_ap_shape() {
    let thread = Thread {
        thread_id: ThreadId::new("t-1").unwrap(),
        created_at: "2026-09-30T20:00:00.000Z".into(),
        updated_at: "2026-09-30T20:00:00.000Z".into(),
        metadata: Map::new(),
        status: ThreadStatus::Idle,
    };
    let encoded = serde_json::to_value(&thread).unwrap();
    assert_eq!(
        encoded,
        json!({"thread_id":"t-1","created_at":"2026-09-30T20:00:00.000Z","updated_at":"2026-09-30T20:00:00.000Z","metadata":{},"status":"idle"})
    );
    assert_eq!(serde_json::from_value::<Thread>(encoded).unwrap(), thread);
    for status in [ThreadStatus::Busy, ThreadStatus::Interrupted] {
        assert!(matches!(
            serde_json::to_value(status).unwrap().as_str(),
            Some("busy" | "interrupted")
        ));
    }
    let envelope = ErrorEnvelope {
        error: ApError {
            kind: ErrorKind::RequestError,
            message: "A run is already in flight for this thread".into(),
            details: Map::from_iter([("code".into(), json!("run_in_flight"))]),
            code: None,
            docs_url: None,
        },
    };
    assert_eq!(
        serde_json::to_value(&envelope).unwrap(),
        json!({"error":{"kind":"request_error","message":"A run is already in flight for this thread","details":{"code":"run_in_flight"}}})
    );
    assert_eq!(
        serde_json::from_value::<ErrorEnvelope>(serde_json::to_value(&envelope).unwrap()).unwrap(),
        envelope
    );
}
