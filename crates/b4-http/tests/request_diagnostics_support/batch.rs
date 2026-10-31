use b4_http::{
    DiagnosticBatch, RequestDiagnostics, MAX_DIAGNOSTIC_BATCH_BYTES, REQUEST_DIAGNOSTIC_CAPACITY,
    REQUEST_ID_HEADER,
};
use serde_json::{json, Value};
use std::collections::BTreeSet;

pub fn assert_id(value: &str) {
    let bytes = value.as_bytes();
    assert!(
        bytes.len() == 53
            && &bytes[..4] == b"b4r_"
            && bytes[36] == b'_'
            && bytes[4..36]
                .iter()
                .chain(bytes[37..].iter())
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(byte))
            && &bytes[37..] != b"0000000000000000",
        "server request reference must have exact safe syntax and a nonzero sequence"
    );
}

pub fn header(response: &axum::response::Response) -> String {
    assert!(REQUEST_ID_HEADER == "x-b4-request-id");
    let headers = response.headers().get_all(REQUEST_ID_HEADER);
    assert!(
        headers.iter().count() == 1,
        "actual fallback response must contain exactly one server request reference"
    );
    let id = headers.iter().next().unwrap().to_str().unwrap().to_owned();
    assert_id(&id);
    id
}

pub fn validate(batch: &DiagnosticBatch) -> Value {
    let bytes = batch.as_ref();
    const { assert!(REQUEST_DIAGNOSTIC_CAPACITY == 256 && MAX_DIAGNOSTIC_BATCH_BYTES == 65_536) };
    assert!(
        !bytes.is_empty()
            && bytes.len() <= 65_536
            && bytes.first() == Some(&b'{')
            && bytes.last() == Some(&b'}')
            && !bytes.contains(&b'\n'),
        "encoded diagnostic batch must be bounded compact JSON without newline"
    );
    let value: Value =
        serde_json::from_slice(bytes).expect("owned diagnostic bytes must contain valid JSON");
    let keys: BTreeSet<_> = value
        .as_object()
        .unwrap()
        .keys()
        .map(String::as_str)
        .collect();
    let expected = BTreeSet::from([
        "diagnostic_batch_version",
        "scope",
        "redacted",
        "capacity",
        "record_loss",
        "total_evicted_records",
        "correlation_exhausted",
        "records",
    ]);
    assert!(
        keys == expected,
        "diagnostic top-level keys must match the exact embedding contract"
    );
    assert!(
        value["diagnostic_batch_version"] == 1
            && value["scope"] == "request_boundary"
            && value["redacted"] == true
            && value["capacity"] == 256
    );
    assert!(
        value["record_loss"].is_boolean()
            && value["correlation_exhausted"].is_boolean()
            && value["total_evicted_records"].as_u64().is_some()
    );
    let records = value["records"].as_array().unwrap();
    assert!(records.len() <= 256);
    for record in records {
        let keys: BTreeSet<_> = record
            .as_object()
            .unwrap()
            .keys()
            .map(String::as_str)
            .collect();
        assert!(
            keys == BTreeSet::from(["request_id", "event", "method", "status_code", "elapsed_ms"]),
            "request observation keys must match the exact redacted contract"
        );
        assert_id(record["request_id"].as_str().unwrap());
        assert!(["get", "post", "delete", "other"].contains(&record["method"].as_str().unwrap()));
        match record["event"].as_str().unwrap() {
            "response_ready" => assert!(record["status_code"]
                .as_u64()
                .is_some_and(|code| (100..=999).contains(&code))),
            "request_future_dropped" => assert!(record["status_code"].is_null()),
            _ => panic!("only the two documented request boundary events may be encoded"),
        }
        assert!(record["elapsed_ms"].as_u64().is_some());
    }
    value
}

pub fn drain(diagnostics: &RequestDiagnostics) -> (DiagnosticBatch, Value) {
    let batch = diagnostics
        .drain()
        .expect("actual retained observations must drain as bounded bytes");
    let value = validate(&batch);
    (batch, value)
}

pub fn response_record(record: &Value, id: &str, method: &str, status: u16) {
    assert!(
        record["request_id"].as_str() == Some(id)
            && record["event"] == "response_ready"
            && record["method"].as_str() == Some(method)
            && record["status_code"] == json!(status),
        "actual response header and one fixed event must agree"
    );
}

pub fn excludes(batch: &DiagnosticBatch, private_values: &[&str]) {
    let text = std::str::from_utf8(batch.as_ref()).unwrap();
    for value in private_values {
        assert!(
            !text.contains(value),
            "diagnostic batch must exclude private fixture context and authority"
        );
    }
}
