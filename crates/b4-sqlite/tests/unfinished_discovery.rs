#[path = "../../b4-runtime/tests/unfinished_discovery_support/mod.rs"]
mod fixture;
mod unfinished_discovery_support;

use b4_runtime::DiscoveryError;
use b4_runtime::{DiscoveryOwnerRelation, Store, StoreError};
use b4_sqlite::SqliteStore;
use fixture::scenarios;
use fixture::AcceptedFixture;
use unfinished_discovery_support::Directory;

#[test]
fn reopened_unfinished_execution_is_discovered() {
    let directory = Directory::new();
    let store = SqliteStore::open(directory.path()).unwrap();
    directory.assert_private();
    let prior_owner = store.owner_generation();
    let fixture = AcceptedFixture::admit(&store);
    assert_eq!(prior_owner, 1);
    assert!(matches!(
        SqliteStore::open(directory.path()),
        Err(StoreError::AlreadyOwned)
    ));
    // The fixture retains pure captured records, never a Store or connection.
    drop(store);
    let reopened = SqliteStore::open(directory.path()).unwrap();
    let current_owner = reopened.owner_generation();
    assert_eq!(current_owner, prior_owner.checked_add(1).unwrap());
    reopened.readiness().unwrap();
    fixture.assert_unchanged(&reopened);
    directory.assert_private();

    let result = reopened.discover_unfinished(None);
    fixture.assert_unchanged(&reopened);
    assert_eq!(reopened.owner_generation(), current_owner);
    eprintln!(
        "successful real SQLite admission, owned-handle close and owner+1 reopen: original full views/journal preserved"
    );
    let page = result.unwrap_or_else(|error| {
        panic!("expected one persisted Prior observation; discovery refused: {error}")
    });
    fixture.assert_page(&page, current_owner, DiscoveryOwnerRelation::Prior);
    drop(reopened);
}

#[test]
fn sqlite_selects_all_unfinished_states_and_task_uncertainties_without_mutation() {
    let directory = Directory::new();
    let store = SqliteStore::open(directory.path()).unwrap();
    scenarios::selection(&store);
}

#[test]
fn sqlite_pages_thirty_two_rows_with_identity_only_lookahead() {
    let directory = Directory::new();
    let store = SqliteStore::open(directory.path()).unwrap();
    scenarios::pagination(&store, false);
}

#[test]
fn sqlite_terminal_prefix_returns_an_empty_advancing_page() {
    let directory = Directory::new();
    let store = SqliteStore::open(directory.path()).unwrap();
    scenarios::pagination(&store, true);
}

#[test]
fn sqlite_per_snapshot_byte_limit_preserves_known_id_access() {
    let directory = Directory::new();
    let store = SqliteStore::open(directory.path()).unwrap();
    scenarios::byte_boundary(&store);
}

#[test]
fn sqlite_full_page_at_each_snapshot_byte_cap_does_not_stall() {
    let directory = Directory::new();
    let store = SqliteStore::open(directory.path()).unwrap();
    scenarios::full_page_byte_cap(&store);
}

#[test]
fn sqlite_depth_limit_preserves_valid_known_id_snapshot() {
    let directory = Directory::new();
    let store = SqliteStore::open(directory.path()).unwrap();
    scenarios::shape_boundary(&store, true);
}

#[test]
fn sqlite_node_limit_preserves_valid_known_id_snapshot() {
    let directory = Directory::new();
    let store = SqliteStore::open(directory.path()).unwrap();
    scenarios::shape_boundary(&store, false);
}

#[test]
fn sqlite_cursor_rejects_a_different_source_with_the_same_owner() {
    let first_directory = Directory::new();
    let second_directory = Directory::new();
    let first = SqliteStore::open(first_directory.path()).unwrap();
    let second = SqliteStore::open(second_directory.path()).unwrap();
    scenarios::foreign_cursor(&first, &second);
}

#[test]
fn sqlite_continuation_observes_interpage_changes_without_a_global_snapshot() {
    let directory = Directory::new();
    let store = SqliteStore::open(directory.path()).unwrap();
    scenarios::interpage_mutation(&store);
}

#[test]
fn sqlite_raw_json_spelling_counts_persisted_bytes_without_normalization() {
    use b4_runtime::DISCOVERY_SNAPSHOT_BYTES;
    let directory = Directory::new();
    let store = SqliteStore::open(directory.path()).unwrap();
    let execution = scenarios::admit(
        &store,
        "raw-spelling",
        scenarios::State::Accepted,
        serde_json::Map::new(),
    );
    let compact = serde_json::to_string(&execution.snapshot).unwrap();
    let connection = directory.connection();
    let exactly = format!(
        "{compact}{}",
        " ".repeat(DISCOVERY_SNAPSHOT_BYTES - compact.len())
    );
    connection
        .execute(
            "UPDATE executions SET snapshot = ?1 WHERE id = ?2",
            rusqlite::params![exactly, execution.snapshot.execution_id.as_str()],
        )
        .unwrap();
    let before = directory.rows();
    let page = store.discover_unfinished(None).unwrap();
    assert_eq!(page.records.len(), 1);
    assert_eq!(directory.rows(), before);
    assert_eq!(
        store.execution(&execution.snapshot.execution_id).unwrap(),
        Some(execution.clone())
    );
    connection
        .execute(
            "UPDATE executions SET snapshot = snapshot || ' ' WHERE id = ?1",
            [execution.snapshot.execution_id.as_str()],
        )
        .unwrap();
    let before = directory.rows();
    assert_eq!(
        store.discover_unfinished(None).unwrap_err(),
        DiscoveryError::LimitExceeded
    );
    assert_eq!(directory.rows(), before);
    assert_eq!(
        store.execution(&execution.snapshot.execution_id).unwrap(),
        Some(execution)
    );
}

#[test]
fn sqlite_pages_and_cursors_retain_no_store_or_owner_lock() {
    use std::sync::Arc;
    let directory = Directory::new();
    let store = Arc::new(SqliteStore::open(directory.path()).unwrap());
    let mut captures = Vec::new();
    for index in 0..33 {
        let execution = scenarios::admit(
            store.as_ref(),
            &format!("release-{index:02}"),
            scenarios::State::Accepted,
            serde_json::Map::new(),
        );
        captures.push(scenarios::Captured::new(store.as_ref(), &execution));
    }
    let before = directory.rows();
    let page = store.discover_unfinished(None).unwrap();
    let cursor = page.next_cursor.as_ref().unwrap().clone();
    assert_eq!(directory.rows(), before);
    assert!(matches!(
        SqliteStore::open(directory.path()),
        Err(StoreError::AlreadyOwned)
    ));
    let weak = Arc::downgrade(&store);
    let alias = store.clone();
    let owner = store.owner_generation();
    drop(store);
    assert!(weak.upgrade().is_some());
    drop(alias);
    assert!(weak.upgrade().is_none());
    let successor = SqliteStore::open(directory.path()).unwrap();
    assert_eq!(successor.owner_generation(), owner + 1);
    assert_eq!(
        successor.discover_unfinished(Some(&cursor)).unwrap_err(),
        DiscoveryError::CursorMismatch
    );
    let next = successor.discover_unfinished(None).unwrap();
    assert_eq!(next.records.len(), 32);
    for record in &next.records {
        assert_eq!(record.owner_relation, DiscoveryOwnerRelation::Prior);
    }
    for captured in captures {
        captured.unchanged(&successor);
    }
    // DTOs remain readable after positive physical writer reacquisition.
    assert_eq!(page.owner_generation, owner);
    assert_eq!(page.records.len(), 32);
    drop(successor);
}

fn corrupt_source(sql: &str, expected: DiscoveryError) {
    let directory = Directory::new();
    let store = SqliteStore::open(directory.path()).unwrap();
    scenarios::admit(
        &store,
        "corrupt-selected",
        scenarios::State::Accepted,
        serde_json::Map::new(),
    );
    let other = b4_contract::ThreadId::new("foreign-thread").unwrap();
    store.create_thread(other).unwrap();
    let connection = directory.connection();
    // These raw writes deliberately bypass trusted Store validation. The
    // original owner remains live; this connection is not a writer-lock proof.
    connection
        .execute_batch("PRAGMA foreign_keys = OFF; PRAGMA ignore_check_constraints = ON;")
        .unwrap();
    connection.execute_batch(sql).unwrap();
    let before = directory.rows();
    assert_eq!(store.discover_unfinished(None).unwrap_err(), expected);
    assert_eq!(directory.rows(), before);
}

#[test]
fn sqlite_refuses_corrupt_index_thread_and_active_relationships_read_only() {
    for sql in [
        "UPDATE executions SET revision = x'01'",
        "UPDATE executions SET owner_epoch = x'01'",
        "UPDATE executions SET revision = x'0000000000000001'",
        "UPDATE executions SET id = 'index-disagrees'",
        "UPDATE executions SET thread_id = 'missing-thread'",
        "UPDATE threads SET active_execution_id = NULL WHERE id = 'thread-corrupt-selected'",
        "UPDATE threads SET active_execution_id = 'corrupt-selected' WHERE id = 'foreign-thread'",
        "UPDATE threads SET generation = 'not-hex' WHERE id = 'thread-corrupt-selected'",
        "UPDATE threads SET resource_revision = x'01' WHERE id = 'thread-corrupt-selected'",
        "DELETE FROM threads WHERE id = 'thread-corrupt-selected'",
    ] {
        corrupt_source(sql, DiscoveryError::CorruptRecord);
    }
}

#[test]
fn sqlite_refuses_future_owner_and_unsupported_metadata_without_repair() {
    for (sql, error) in [
        ("UPDATE executions SET owner_epoch = x'0000000000000002', snapshot = json_set(snapshot, '$.owner_generation', 2)", DiscoveryError::OwnerConflict),
        ("UPDATE metadata SET owner_epoch = x'0000000000000000'", DiscoveryError::OwnerConflict),
        ("UPDATE metadata SET owner_epoch = x'0000000000000002'", DiscoveryError::OwnerConflict),
        ("UPDATE metadata SET schema_version = 999", DiscoveryError::UnsupportedSchema),
        ("UPDATE metadata SET migration_checksum = 'wrong'", DiscoveryError::UnsupportedSchema),
        ("DELETE FROM metadata", DiscoveryError::UnsupportedSchema),
        ("UPDATE executions SET snapshot = json_set(snapshot, '$.schema_version', 999)", DiscoveryError::UnsupportedSnapshot),
    ] { corrupt_source(sql, error); }
}

#[test]
fn sqlite_validates_hidden_terminal_history_before_filtering() {
    let directory = Directory::new();
    let store = SqliteStore::open(directory.path()).unwrap();
    let terminal = scenarios::admit(
        &store,
        "terminal-history",
        scenarios::State::Cancelled,
        serde_json::Map::new(),
    );
    let thread = store.thread(&terminal.thread_id).unwrap().unwrap();
    let initial = scenarios::initial(&store, "replacement-active", false, serde_json::Map::new());
    store
        .admit_fenced(
            &thread.id,
            &thread.generation,
            thread.resource_revision,
            initial,
        )
        .unwrap();
    let before = directory.rows();
    let page = store.discover_unfinished(None).unwrap();
    assert_eq!(page.scanned_rows, 2);
    assert_eq!(page.records.len(), 1);
    assert_eq!(page.records[0].execution_id.as_str(), "replacement-active");
    assert_eq!(directory.rows(), before);
    directory.connection().execute("UPDATE executions SET snapshot = json_set(snapshot, '$.turn_history', json('[]')) WHERE id = 'terminal-history'", []).unwrap();
    let before = directory.rows();
    assert_eq!(
        store.discover_unfinished(None).unwrap_err(),
        DiscoveryError::CorruptRecord
    );
    assert_eq!(directory.rows(), before);
}

#[test]
fn sqlite_lookahead_does_not_decode_the_thirty_third_snapshot() {
    let directory = Directory::new();
    let store = SqliteStore::open(directory.path()).unwrap();
    for index in 0..33 {
        scenarios::admit(
            &store,
            &format!("lookahead-{index:02}"),
            scenarios::State::Accepted,
            serde_json::Map::new(),
        );
    }
    directory
        .connection()
        .execute(
            "UPDATE executions SET snapshot = 'private malformed JSON' WHERE id = 'lookahead-32'",
            [],
        )
        .unwrap();
    let before = directory.rows();
    let first = store.discover_unfinished(None).unwrap();
    assert_eq!(first.scanned_rows, 32);
    assert_eq!(first.records.len(), 32);
    assert_eq!(directory.rows(), before);
    assert_eq!(
        store
            .discover_unfinished(first.next_cursor.as_ref())
            .unwrap_err(),
        DiscoveryError::CorruptRecord
    );
    assert_eq!(directory.rows(), before);
}

#[test]
fn sqlite_lookahead_identity_limit_counts_utf8_bytes_before_decoding() {
    for (identity, expected) in [
        ("é".repeat(128), None),
        ("é".repeat(129), Some(DiscoveryError::CorruptRecord)),
        ("z".repeat(257), Some(DiscoveryError::CorruptRecord)),
    ] {
        let directory = Directory::new();
        let store = SqliteStore::open(directory.path()).unwrap();
        for index in 0..33 {
            scenarios::admit(
                &store,
                &format!("identity-{index:02}"),
                scenarios::State::Accepted,
                serde_json::Map::new(),
            );
        }
        let connection = directory.connection();
        connection
            .execute_batch("PRAGMA foreign_keys = OFF;")
            .unwrap();
        connection.execute("UPDATE executions SET id = ?1, snapshot = 'not decoded lookahead' WHERE id = 'identity-32'", [identity]).unwrap();
        let before = directory.rows();
        match expected {
            Some(error) => assert_eq!(store.discover_unfinished(None).unwrap_err(), error),
            None => {
                let page = store.discover_unfinished(None).unwrap();
                assert_eq!(page.scanned_rows, 32);
                assert_eq!(page.records.len(), 32);
                assert!(page.next_cursor.is_some());
            }
        }
        assert_eq!(directory.rows(), before);
    }
}

#[test]
fn sqlite_snapshot_version_probe_handles_escaped_and_duplicate_root_keys() {
    let directory = Directory::new();
    let store = SqliteStore::open(directory.path()).unwrap();
    let execution = scenarios::admit(
        &store,
        "version-probe",
        scenarios::State::Accepted,
        serde_json::Map::new(),
    );
    let ordinary = serde_json::to_string(&execution.snapshot).unwrap();
    let escaped = ordinary.replace("\"schema_version\"", "\"\\u0073chema_version\"");
    let connection = directory.connection();
    connection
        .execute("UPDATE executions SET snapshot = ?1", [escaped])
        .unwrap();
    let before = directory.rows();
    assert_eq!(store.discover_unfinished(None).unwrap().records.len(), 1);
    assert_eq!(directory.rows(), before);
    for (payload, error) in [
        (
            ordinary.replacen("{", "{\"\\u0073chema_version\":1,", 1),
            DiscoveryError::CorruptRecord,
        ),
        (
            ordinary.replace(
                "\"schema_version\":1",
                "\"schema_version\":18446744073709551615",
            ),
            DiscoveryError::UnsupportedSnapshot,
        ),
        (
            ordinary.replace("\"schema_version\":1,", ""),
            DiscoveryError::CorruptRecord,
        ),
        (
            ordinary.replace("\"schema_version\":1", "\"schema_version\":1.0"),
            DiscoveryError::CorruptRecord,
        ),
        ("not JSON".into(), DiscoveryError::CorruptRecord),
    ] {
        connection
            .execute("UPDATE executions SET snapshot = ?1", [payload])
            .unwrap();
        let before = directory.rows();
        assert_eq!(store.discover_unfinished(None).unwrap_err(), error);
        assert_eq!(directory.rows(), before);
    }
}
