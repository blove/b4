//! Authored schema examples must also be accepted by the native DTOs.
use b4_contract::{ErrorEnvelope, ResumeRequest, RunRequest, Snapshot, Thread};

#[test]
fn authored_wire_examples_parse_as_native_dtos() {
    for source in [
        include_str!("../../../contracts/agent-protocol/examples/run-omitted-input.json"),
        include_str!("../../../contracts/agent-protocol/examples/run-null-input.json"),
        include_str!("../../../contracts/agent-protocol/examples/run-array-input.json"),
    ] {
        serde_json::from_str::<RunRequest>(source).unwrap();
    }
    for source in [
        include_str!("../../../contracts/agent-protocol/examples/resume-resolved.json"),
        include_str!("../../../contracts/agent-protocol/examples/resume-cancelled.json"),
    ] {
        serde_json::from_str::<ResumeRequest>(source).unwrap();
    }
    serde_json::from_str::<ErrorEnvelope>(include_str!(
        "../../../contracts/agent-protocol/examples/error.json"
    ))
    .unwrap();
    serde_json::from_str::<Thread>(include_str!(
        "../../../contracts/agent-protocol/examples/thread.json"
    ))
    .unwrap();
    let snapshot = serde_json::from_str::<Snapshot>(include_str!(
        "../../../contracts/agent-protocol/examples/internal-execution-snapshot.json"
    ))
    .unwrap();
    assert!(snapshot.output.is_none());
}
