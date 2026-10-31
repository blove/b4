#[path = "../../b4-runtime/tests/attachment_pending/support.rs"]
mod support;
use b4_runtime::approval::*;
use b4_runtime::*;
use b4_sqlite::SqliteStore;
use rusqlite::Connection;
use std::{
    path::PathBuf,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
};
use support::*;

struct Directory(PathBuf);
impl Directory {
    fn new() -> Self {
        static NEXT: AtomicUsize = AtomicUsize::new(0);
        let path = std::env::temp_dir().join(format!(
            "b4-attachment-pending-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::SeqCst)
        ));
        std::fs::create_dir_all(&path).unwrap();
        Self(path)
    }
    fn connection(&self) -> Connection {
        Connection::open(self.0.join("runtime.sqlite3")).unwrap()
    }
    fn rows(&self) -> Vec<(String, String)> {
        let db = self.connection();
        [
            "threads",
            "executions",
            "journal",
            "approval_grants",
            "approval_acceptances",
        ]
        .into_iter()
        .flat_map(|table| {
            let mut statement = db
                .prepare(&format!("SELECT * FROM {table} ORDER BY rowid"))
                .unwrap();
            let columns = statement.column_count();
            statement
                .query_map([], |row| {
                    (0..columns)
                        .map(|index| {
                            row.get::<_, rusqlite::types::Value>(index)
                                .map(|value| format!("{value:?}"))
                        })
                        .collect::<Result<Vec<_>, _>>()
                })
                .unwrap()
                .map(|row| (table.into(), row.unwrap().join("|")))
                .collect::<Vec<_>>()
        })
        .collect()
    }
}
impl Drop for Directory {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}
struct Deny;
impl ApprovalAuthority for Deny {
    fn authorize(
        &self,
        _: &ApprovalContext,
        _: ApprovalOperation,
        _: &ApprovalResource,
    ) -> Result<(), ApprovalError> {
        Err(ApprovalError::Unauthorized)
    }
}

#[test]
fn actual_owner_reopen_reads_expired_park_without_adoption_or_record_changes() {
    let directory = Directory::new();
    let store = Arc::new(SqliteStore::open(&directory.0).unwrap());
    let (captured, pending, before) = fixture(store.clone());
    let rows = directory.rows();
    let original_owner = store.owner_generation();
    drop(store);
    let reopened = SqliteStore::open(&directory.0).unwrap();
    assert!(reopened.owner_generation() > original_owner);
    let service = ApprovalService::new(reopened, Allow).unwrap();
    assert!(service.attachment_pending(&context(), &captured).unwrap() == pending);
    assert_eq!(pending[0].expires_at_ms, 8);
    assert!(
        service
            .store()
            .approval_view(&execution())
            .unwrap()
            .unwrap()
            == before
    );
    assert!(directory.rows() == rows);
    assert_eq!(
        service.pending(&context(), &execution(), fence(&captured)),
        Err(ApprovalError::Store(StoreError::OwnerConflict))
    );
    assert_eq!(
        service.accept(
            &execution(),
            fence(&captured),
            acceptance(&captured, &pending, 100, "later-turn")
        ),
        Err(ApprovalError::Store(StoreError::OwnerConflict))
    );
}

#[test]
fn authorization_denial_precedes_actual_corrupt_sqlite_grant_decode() {
    let directory = Directory::new();
    let store = Arc::new(SqliteStore::open(&directory.0).unwrap());
    let (captured, _, _) = fixture(store.clone());
    drop(store);
    let store = Arc::new(SqliteStore::open(&directory.0).unwrap());
    directory
        .connection()
        .execute(
            "UPDATE approval_grants SET record='{}' WHERE consumed=0",
            [],
        )
        .unwrap();
    let rows = directory.rows();
    let service = ApprovalService::new(store, Deny).unwrap();
    assert_eq!(
        service.attachment_pending(&context(), &captured),
        Err(ApprovalError::Unauthorized)
    );
    assert!(directory.rows() == rows);
    service.store().readiness().unwrap();
}

#[test]
fn actual_reopened_typed_history_corruption_is_not_a_healthy_grant_refusal() {
    for corruption in [
        "grant_origin",
        "audit_origin",
        "expired_consumption",
        "missing_history",
    ] {
        let directory = Directory::new();
        let store = Arc::new(SqliteStore::open(&directory.0).unwrap());
        let (captured, _, _) = fixture(store.clone());
        drop(store);
        let reopened = SqliteStore::open(&directory.0).unwrap();
        let db = directory.connection();
        match corruption {
            "audit_origin" => {
                db.execute("UPDATE approval_acceptances SET record=json_set(record, '$.context.actor.principal', 'bob')", []).unwrap();
            }
            "grant_origin" => {
                db.execute("UPDATE approval_grants SET record=json_set(record, '$.origin.principal', 'bob') WHERE consumed=1", []).unwrap();
            }
            "expired_consumption" => {
                db.execute("UPDATE approval_grants SET record=json_set(record, '$.expires_at_ms', 4) WHERE consumed=1", []).unwrap();
            }
            "missing_history" => {
                db.execute("DELETE FROM approval_grants WHERE consumed=1", [])
                    .unwrap();
                db.execute("DELETE FROM approval_acceptances", []).unwrap();
            }
            _ => unreachable!(),
        }
        let rows = directory.rows();
        let service = ApprovalService::new(reopened, Allow).unwrap();
        assert!(matches!(
            service.attachment_pending(&context(), &captured),
            Err(ApprovalError::Store(StoreError::Corrupt(_)))
        ));
        assert!(directory.rows() == rows);
    }
}

#[test]
fn reopened_missing_parallel_prompt_is_corruption_even_when_current_policy_changed() {
    let directory = Directory::new();
    let store = Arc::new(SqliteStore::open(&directory.0).unwrap());
    let captured = parallel_fixture(store.clone());
    drop(store);
    let reopened = SqliteStore::open(&directory.0).unwrap();
    directory
        .connection()
        .execute("DELETE FROM approval_grants WHERE task_id='second'", [])
        .unwrap();
    let before = directory.rows();
    let service = ApprovalService::new(reopened, Allow).unwrap();
    let mut current_policy = context();
    current_policy.policy_revision = "policy-2".into();
    assert!(matches!(
        service.attachment_pending(&current_policy, &captured),
        Err(ApprovalError::Store(StoreError::Corrupt(_)))
    ));
    assert!(directory.rows() == before);
}
