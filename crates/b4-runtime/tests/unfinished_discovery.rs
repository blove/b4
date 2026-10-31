#[path = "unfinished_discovery_support/custom.rs"]
mod unfinished_discovery_custom;
mod unfinished_discovery_support;

use b4_runtime::{DiscoveryOwnerRelation, MemoryStore, Store};
use unfinished_discovery_support::scenarios;
use unfinished_discovery_support::AcceptedFixture;

#[test]
fn memory_admitted_execution_is_discovered() {
    let store = MemoryStore::new();
    let fixture = AcceptedFixture::admit(&store);
    let owner = store.owner_generation();
    assert_eq!(owner, 1);

    let result = store.discover_unfinished(None);
    fixture.assert_unchanged(&store);
    assert_eq!(store.owner_generation(), owner);
    eprintln!("successful real Memory admission: Accepted, original full views/journal preserved");
    let page = result.unwrap_or_else(|error| {
        panic!("expected one admitted Current observation; discovery refused: {error}")
    });
    fixture.assert_page(&page, owner, DiscoveryOwnerRelation::Current);
}

#[test]
fn memory_selects_all_unfinished_states_and_task_uncertainties_without_mutation() {
    scenarios::selection(&MemoryStore::new());
}

#[test]
fn memory_pages_thirty_two_rows_with_identity_only_lookahead() {
    scenarios::pagination(&MemoryStore::new(), false);
}

#[test]
fn memory_terminal_prefix_returns_an_empty_advancing_page() {
    scenarios::pagination(&MemoryStore::new(), true);
}

#[test]
fn memory_per_snapshot_byte_limit_preserves_known_id_access() {
    scenarios::byte_boundary(&MemoryStore::new());
}

#[test]
fn memory_full_page_at_each_snapshot_byte_cap_does_not_stall() {
    scenarios::full_page_byte_cap(&MemoryStore::new());
}

#[test]
fn memory_depth_limit_preserves_valid_known_id_snapshot() {
    scenarios::shape_boundary(&MemoryStore::new(), true);
}

#[test]
fn memory_node_limit_preserves_valid_known_id_snapshot() {
    scenarios::shape_boundary(&MemoryStore::new(), false);
}

#[test]
fn memory_cursor_rejects_a_different_source_with_the_same_owner() {
    scenarios::foreign_cursor(&MemoryStore::new(), &MemoryStore::new());
}

#[test]
fn memory_continuation_observes_interpage_changes_without_a_global_snapshot() {
    scenarios::interpage_mutation(&MemoryStore::new());
}

#[test]
fn custom_store_discovery_defaults_to_refusal_without_probing() {
    unfinished_discovery_custom::defaults();
}

#[test]
fn existing_arc_and_reference_wrappers_forward_builtin_discovery() {
    use std::sync::Arc;
    let store = Arc::new(MemoryStore::new());
    let fixture = AcceptedFixture::admit(store.as_ref());
    let dynamic: Arc<dyn Store> = store.clone();
    let reference: &dyn Store = store.as_ref();
    for page in [
        Store::discover_unfinished(&store, None).unwrap(),
        Store::discover_unfinished(&dynamic, None).unwrap(),
        Store::discover_unfinished(&reference, None).unwrap(),
    ] {
        fixture.assert_page(&page, 1, DiscoveryOwnerRelation::Current);
    }
    fixture.assert_unchanged(store.as_ref());
}

#[test]
fn memory_pages_and_cursors_retain_no_store_after_a_move() {
    use std::sync::Arc;
    let store = MemoryStore::new();
    for index in 0..33 {
        scenarios::admit(
            &store,
            &format!("move-{index:02}"),
            scenarios::State::Accepted,
            serde_json::Map::new(),
        );
    }
    let page = store.discover_unfinished(None).unwrap();
    let cursor = page.next_cursor.as_ref().unwrap().clone();
    let moved = Arc::new(store);
    assert_eq!(
        moved
            .discover_unfinished(Some(&cursor))
            .unwrap()
            .scanned_rows,
        1
    );
    let weak = Arc::downgrade(&moved);
    drop(moved);
    assert!(weak.upgrade().is_none());
    assert_eq!(page.records.len(), 32);
    let successor = MemoryStore::new();
    assert_eq!(successor.owner_generation(), page.owner_generation);
    assert_eq!(
        successor.discover_unfinished(Some(&cursor)).unwrap_err(),
        b4_runtime::DiscoveryError::CursorMismatch
    );
}
