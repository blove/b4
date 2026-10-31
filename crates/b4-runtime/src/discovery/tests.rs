use super::backend::*;
use super::shape::JsonShapeGuard;
use super::*;
use b4_contract::*;
use serde_json::{json, Map, Value};
use std::io::Write;
use std::sync::Arc;

fn initial() -> Snapshot {
    b4_kernel::new_execution(
        ExecutionId::new("fixture-execution").unwrap(),
        TurnId::new("fixture-turn").unwrap(),
        1,
        Definition {
            route: RouteKey::new("/fixture#workflow").unwrap(),
            revision: ExecutionRevision::new("fixture-v1").unwrap(),
            batches: vec![],
            max_attempts: 8,
        },
        Map::new(),
        TransitionFacts { now_ms: 1 },
    )
    .unwrap()
}

fn projected(
    snapshot: &Snapshot,
    current_owner: u64,
) -> Result<Option<UnfinishedExecutionRef>, DiscoveryError> {
    let id = ThreadId::new("fixture-thread").unwrap();
    let generation = ThreadGeneration::new("a".repeat(64)).unwrap();
    project_snapshot(
        snapshot,
        SnapshotIndex {
            execution_id: &snapshot.execution_id,
            thread_id: &id,
            revision: snapshot.revision,
            owner_generation: snapshot.owner_generation,
        },
        ThreadIndex {
            id: &id,
            generation: &generation,
            active_execution_id: Some(&snapshot.execution_id),
            resource_revision: 7,
            active_relationship_valid: true,
            foreign_active_pointer: false,
        },
        current_owner,
    )
}

#[test]
fn origin_borrows_only_its_live_cursor_and_generation() {
    let origin = DiscoveryOrigin::default();
    let id = ExecutionId::new("fixture-last").unwrap();
    let cursor = origin.cursor(1, &id);
    assert_eq!(origin.checked_after(1, None), Ok(None));
    let actual = origin.checked_after(1, Some(&cursor)).unwrap().unwrap();
    assert!(std::ptr::eq(actual, &cursor.after));
    assert_eq!(actual, &id);
    assert_eq!(
        origin.checked_after(2, Some(&cursor)),
        Err(DiscoveryError::CursorMismatch)
    );
    assert_eq!(
        DiscoveryOrigin::default().checked_after(1, Some(&cursor)),
        Err(DiscoveryError::CursorMismatch)
    );
    assert_eq!(
        origin.checked_after(0, Some(&cursor)),
        Err(DiscoveryError::OwnerConflict)
    );
}

#[test]
fn cursor_retains_only_the_marker_after_origin_drop() {
    let origin = DiscoveryOrigin::default();
    let id = ExecutionId::new("fixture-last").unwrap();
    let cursor = origin.cursor(1, &id);
    let weak = Arc::downgrade(&cursor.marker);
    drop(origin);
    assert!(weak.upgrade().is_some());
    drop(cursor);
    assert!(weak.upgrade().is_none());
}

#[test]
fn shape_counts_keys_containers_and_primitives() {
    let mut guard = JsonShapeGuard::new();
    guard.consume(br#"[{"k":0}]"#).unwrap();
    let count = guard.finish().unwrap();
    assert_eq!((count.bytes, count.nodes, count.depth), (9, 4, 0));
}

#[test]
fn shape_accounts_escapes_unicode_and_scalars_at_every_split() {
    let bytes =
        serde_json::to_vec(&json!({"key":"\\\"[]{}λ", "other":[-1.2e3,true,null]})).unwrap();
    let mut reference = JsonShapeGuard::new();
    reference.consume(&bytes).unwrap();
    let expected = reference.finish().unwrap();
    for at in 0..=bytes.len() {
        let mut guard = JsonShapeGuard::new();
        guard.consume(&bytes[..at]).unwrap();
        guard.consume(&bytes[at..]).unwrap();
        assert_eq!(guard.finish(), Ok(expected));
    }
    let mut bytes_at_a_time = JsonShapeGuard::new();
    for byte in &bytes {
        bytes_at_a_time.consume(std::slice::from_ref(byte)).unwrap();
    }
    assert_eq!(bytes_at_a_time.finish(), Ok(expected));
}

#[test]
fn shape_depth_accepts64_and_refuses65_before_child_visit() {
    let accepted = format!("{}null{}", "[".repeat(64), "]".repeat(64));
    let mut guard = JsonShapeGuard::new();
    guard.consume(accepted.as_bytes()).unwrap();
    assert!(guard.finish().is_ok());
    let mut refused = JsonShapeGuard::new();
    assert_eq!(
        refused.consume("[".repeat(65).as_bytes()),
        Err(DiscoveryError::LimitExceeded)
    );
    assert!(refused.own_limit_refusal());
    assert_eq!(refused.finish(), Err(DiscoveryError::LimitExceeded));
}

#[test]
fn shape_nodes_accept16384_and_refuse16385() {
    let accepted = format!("[{}]", vec!["0"; 16_383].join(","));
    let mut guard = JsonShapeGuard::new();
    guard.consume(accepted.as_bytes()).unwrap();
    assert_eq!(guard.finish().unwrap().nodes, 16_384);
    let refused = format!("[{}]", vec!["0"; 16_384].join(","));
    let mut guard = JsonShapeGuard::new();
    assert_eq!(
        guard.consume(refused.as_bytes()),
        Err(DiscoveryError::LimitExceeded)
    );
}

#[test]
fn shape_wire_bytes_accept_one_mib_and_refuse_the_next_byte() {
    let accepted = format!("\"{}\"", "a".repeat(DISCOVERY_SNAPSHOT_BYTES - 2));
    let mut guard = JsonShapeGuard::new();
    guard.consume(accepted.as_bytes()).unwrap();
    assert_eq!(guard.finish().unwrap().bytes, DISCOVERY_SNAPSHOT_BYTES);
    assert_eq!(guard.consume(b" "), Err(DiscoveryError::LimitExceeded));
    assert!(guard.own_limit_refusal());
}

#[test]
fn shape_rejects_unclosed_or_mismatched_containers_and_strings() {
    for bytes in [b"".as_slice(), b"[", b"[}", b"\"unterminated\\"] {
        let mut guard = JsonShapeGuard::new();
        let result = guard.consume(bytes).and_then(|()| guard.finish());
        assert_eq!(result, Err(DiscoveryError::CorruptRecord));
        assert!(!guard.own_limit_refusal());
    }
}

#[test]
fn shape_writer_marks_only_its_own_limit_failures() {
    let mut guard = JsonShapeGuard::new();
    assert!(guard
        .write_all(&vec![b'a'; DISCOVERY_SNAPSHOT_BYTES + 1])
        .is_err());
    assert!(guard.own_limit_refusal());
    assert_eq!(guard.finish(), Err(DiscoveryError::LimitExceeded));
}

#[test]
fn borrowed_snapshot_measurement_matches_compact_json_and_decoder() {
    let mut snapshot = initial();
    snapshot.values.insert(
        "fixture".into(),
        json!({"escaped":"\\\"λ[]", "items":[0,true,null]}),
    );
    let bytes = serde_json::to_vec(&snapshot).unwrap();
    assert_eq!(measure_snapshot(&snapshot), Ok(bytes.len()));
    assert_eq!(decode_snapshot(&bytes), Ok(snapshot));
}

#[test]
fn borrowed_snapshot_measurement_refuses_depth_before_kernel_validation() {
    let mut snapshot = initial();
    let mut value = Value::Null;
    for _ in 0..65 {
        value = Value::Array(vec![value]);
    }
    snapshot.values.insert("deep".into(), value);
    assert_eq!(b4_kernel::validate_snapshot(&snapshot), Ok(()));
    assert_eq!(
        measure_snapshot(&snapshot),
        Err(DiscoveryError::LimitExceeded)
    );
}

#[test]
fn borrowed_snapshot_measurement_refuses_a_large_plain_string() {
    let mut snapshot = initial();
    snapshot.values.insert(
        "large".into(),
        Value::String("a".repeat(DISCOVERY_SNAPSHOT_BYTES)),
    );
    assert_eq!(b4_kernel::validate_snapshot(&snapshot), Ok(()));
    assert_eq!(
        measure_snapshot(&snapshot),
        Err(DiscoveryError::LimitExceeded)
    );
}

#[test]
fn shared_decoder_classifies_unsupported_version_and_escaped_root_key() {
    let mut value = serde_json::to_value(initial()).unwrap();
    value["schema_version"] = json!(u64::MAX);
    assert_eq!(
        decode_snapshot(&serde_json::to_vec(&value).unwrap()),
        Err(DiscoveryError::UnsupportedSnapshot)
    );
    let bytes = serde_json::to_vec(&initial()).unwrap();
    let escaped = String::from_utf8(bytes)
        .unwrap()
        .replace("\"schema_version\"", "\"schema_\\u0076ersion\"");
    assert_eq!(decode_snapshot(escaped.as_bytes()), Ok(initial()));
}

#[test]
fn shared_decoder_rejects_missing_duplicate_version_and_malformed_json() {
    for bytes in [
        br#"{}"#.as_slice(),
        br#"{"schema_version":1,"schema_version":1}"#,
        br#"{"schema_version":"1"}"#,
        br#"{"schema_version":1} trailing"#,
    ] {
        assert_eq!(decode_snapshot(bytes), Err(DiscoveryError::CorruptRecord));
    }
}

#[test]
fn shared_decoder_budget_precedes_recursive_probe_and_decode() {
    assert_eq!(
        decode_snapshot(&vec![b' '; DISCOVERY_SNAPSHOT_BYTES + 1]),
        Err(DiscoveryError::LimitExceeded)
    );
    let deep = format!("{{\"schema_version\":1,\"unknown\":{}}}", "[".repeat(65));
    assert_eq!(
        decode_snapshot(deep.as_bytes()),
        Err(DiscoveryError::LimitExceeded)
    );
}

#[test]
fn shared_projection_preserves_exact_current_and_prior_fences() {
    let snapshot = initial();
    let current = projected(&snapshot, 1).unwrap().unwrap();
    assert_eq!(current.owner_relation, DiscoveryOwnerRelation::Current);
    assert_eq!(
        current.execution_fence,
        crate::ExecutionFence::from(&snapshot)
    );
    assert_eq!(current.resource_revision, 7);
    assert_eq!(current.route, snapshot.definition.route);
    assert_eq!(current.definition_revision, snapshot.definition.revision);
    assert_eq!(current.status, ExecutionStatus::Accepted);
    assert!(!current.has_in_flight_task && !current.has_outcome_unknown_task);
    assert_eq!(
        projected(&snapshot, 2).unwrap().unwrap().owner_relation,
        DiscoveryOwnerRelation::Prior
    );
}

#[test]
fn shared_projection_distinguishes_zero_snapshot_and_future_owner() {
    let mut snapshot = initial();
    snapshot.owner_generation = 0;
    assert_eq!(projected(&snapshot, 1), Err(DiscoveryError::CorruptRecord));
    snapshot.owner_generation = 2;
    assert_eq!(projected(&snapshot, 1), Err(DiscoveryError::OwnerConflict));
    snapshot.owner_generation = 1;
    assert_eq!(projected(&snapshot, 0), Err(DiscoveryError::OwnerConflict));
}

#[test]
fn shared_projection_checks_indexes_and_thread_before_terminal_filter() {
    let mut snapshot = b4_kernel::transition(
        &initial(),
        KernelCommand::Start,
        TransitionFacts { now_ms: 2 },
    )
    .unwrap()
    .snapshot;
    assert_eq!(snapshot.status, ExecutionStatus::Completed);
    assert_eq!(projected(&snapshot, 1), Ok(None));
    snapshot.schema_version = 2;
    assert_eq!(
        projected(&snapshot, 1),
        Err(DiscoveryError::UnsupportedSnapshot)
    );
}

#[test]
fn page_budget_accepts_all32_capped_rows_without_stalling() {
    let mut budget = PageBudget::default();
    for _ in 0..DISCOVERY_SCANNED_ROWS {
        budget.add_snapshot_bytes(DISCOVERY_SNAPSHOT_BYTES).unwrap();
    }
    assert_eq!(budget.total, DISCOVERY_TOTAL_SNAPSHOT_BYTES);
    assert_eq!(
        budget.add_snapshot_bytes(1),
        Err(DiscoveryError::LimitExceeded)
    );
}

#[test]
fn helper_budget_small_cap_and_checked_overflow_preserve_last_total() {
    let mut budget = PageBudget::default();
    budget.add_with_limit(3, 7).unwrap();
    budget.add_with_limit(4, 7).unwrap();
    assert_eq!(budget.total, 7);
    assert_eq!(
        budget.add_with_limit(1, 7),
        Err(DiscoveryError::LimitExceeded)
    );
    assert_eq!(budget.total, 7);
    let mut overflow = PageBudget::default();
    overflow.add_with_limit(usize::MAX, usize::MAX).unwrap();
    assert_eq!(
        overflow.add_with_limit(1, usize::MAX),
        Err(DiscoveryError::LimitExceeded)
    );
    assert_eq!(overflow.total, usize::MAX);
}

#[test]
fn projection_rejects_every_index_relationship_before_terminal_filter() {
    for terminal in [false, true] {
        let initial = initial();
        let snapshot = if terminal {
            b4_kernel::transition(
                &initial,
                KernelCommand::Start,
                TransitionFacts { now_ms: 2 },
            )
            .unwrap()
            .snapshot
        } else {
            initial
        };
        // These fixtures are preflighted, just as each backend must preflight
        // its borrowed serialization or persisted raw bytes before projection.
        measure_snapshot(&snapshot).unwrap();
        let thread_id = ThreadId::new("fixture-thread").unwrap();
        let foreign_thread = ThreadId::new("foreign-thread").unwrap();
        let foreign_execution = ExecutionId::new("foreign-execution").unwrap();
        let generation = ThreadGeneration::new("a".repeat(64)).unwrap();
        for case in 0..6 {
            let mut index = SnapshotIndex {
                execution_id: &snapshot.execution_id,
                thread_id: &thread_id,
                revision: snapshot.revision,
                owner_generation: snapshot.owner_generation,
            };
            let mut thread = ThreadIndex {
                id: &thread_id,
                generation: &generation,
                active_execution_id: Some(&snapshot.execution_id),
                resource_revision: 7,
                active_relationship_valid: true,
                foreign_active_pointer: false,
            };
            match case {
                0 => index.execution_id = &foreign_execution,
                1 => index.revision += 1,
                2 => index.owner_generation += 1,
                3 => thread.id = &foreign_thread,
                4 => thread.active_relationship_valid = false,
                _ => thread.foreign_active_pointer = true,
            }
            assert_eq!(
                project_snapshot(&snapshot, index, thread, 1),
                Err(DiscoveryError::CorruptRecord)
            );
        }
        for active in [None, Some(&foreign_execution)] {
            let result = project_snapshot(
                &snapshot,
                SnapshotIndex {
                    execution_id: &snapshot.execution_id,
                    thread_id: &thread_id,
                    revision: snapshot.revision,
                    owner_generation: snapshot.owner_generation,
                },
                ThreadIndex {
                    id: &thread_id,
                    generation: &generation,
                    active_execution_id: active,
                    resource_revision: 7,
                    active_relationship_valid: true,
                    foreign_active_pointer: false,
                },
                1,
            );
            assert_eq!(
                result,
                if terminal {
                    Ok(None)
                } else {
                    Err(DiscoveryError::CorruptRecord)
                }
            );
        }
    }
}

#[test]
fn serializer_opening_refusal_precedes_the_65th_container_children() {
    use serde::ser::{Serialize, SerializeSeq, Serializer};
    use std::cell::Cell;
    struct Nested<'a> {
        remaining: usize,
        visited: &'a Cell<usize>,
    }
    impl Serialize for Nested<'_> {
        fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
            let mut sequence = serializer.serialize_seq(Some(1))?;
            self.visited.set(self.visited.get() + 1);
            if self.remaining == 1 {
                sequence.serialize_element(&())?;
            } else {
                sequence.serialize_element(&Nested {
                    remaining: self.remaining - 1,
                    visited: self.visited,
                })?;
            }
            sequence.end()
        }
    }
    let visited = Cell::new(0);
    let mut guard = JsonShapeGuard::new();
    assert!(serde_json::to_writer(
        &mut guard,
        &Nested {
            remaining: 65,
            visited: &visited
        }
    )
    .is_err());
    assert_eq!(visited.get(), 64);
    assert!(guard.own_limit_refusal());
    assert_eq!(guard.finish(), Err(DiscoveryError::LimitExceeded));
}

#[test]
fn unsupported_version_probe_still_consumes_and_validates_the_entire_document() {
    assert_eq!(
        decode_snapshot(br#"{"schema_version":99,"unknown":[false,]}"#),
        Err(DiscoveryError::CorruptRecord)
    );
    assert_eq!(
        decode_snapshot(br#"{"schema_version":99,"unknown":[false]}"#),
        Err(DiscoveryError::UnsupportedSnapshot)
    );
    assert_eq!(
        decode_snapshot(br#"{"schema_version":-1}"#),
        Err(DiscoveryError::CorruptRecord)
    );
}

#[test]
fn discovery_error_codes_are_static_and_have_no_source() {
    use std::error::Error;
    for (error, code) in [
        (
            DiscoveryError::UnsupportedOperation,
            "discovery_unsupported_operation",
        ),
        (DiscoveryError::CursorMismatch, "discovery_cursor_mismatch"),
        (DiscoveryError::LimitExceeded, "discovery_limit_exceeded"),
        (
            DiscoveryError::UnsupportedSnapshot,
            "discovery_unsupported_snapshot",
        ),
        (
            DiscoveryError::UnsupportedSchema,
            "discovery_unsupported_schema",
        ),
        (DiscoveryError::OwnerConflict, "discovery_owner_conflict"),
        (DiscoveryError::CorruptRecord, "discovery_corrupt_record"),
        (DiscoveryError::Unavailable, "discovery_unavailable"),
    ] {
        assert_eq!(error.to_string(), code);
        assert!(error.source().is_none());
    }
}
