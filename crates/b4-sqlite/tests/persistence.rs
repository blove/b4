use b4_contract::*;
use b4_kernel::{new_execution, transition};
use b4_runtime::{Store, StoreError};
use b4_sqlite::SqliteStore;
use rusqlite::{params, Connection};
use serde_json::{json, Map};
use std::io::{BufRead, BufReader, Write};
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::atomic::{AtomicU64, Ordering};

#[path = "../../b4-runtime/tests/support/mod.rs"]
mod support;

static NEXT_DIRECTORY: AtomicU64 = AtomicU64::new(0);

struct TestDirectory(PathBuf);

impl TestDirectory {
    fn new() -> Self {
        let path = std::env::temp_dir().join(format!(
            "b4-sqlite-{}-{}",
            std::process::id(),
            NEXT_DIRECTORY.fetch_add(1, Ordering::Relaxed)
        ));
        std::fs::create_dir_all(&path).unwrap();
        Self(path)
    }

    fn path(&self) -> &Path {
        &self.0
    }
    fn database(&self) -> PathBuf {
        self.0.join("runtime.sqlite3")
    }
}

impl Drop for TestDirectory {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

fn initial(id: &str, owner: u64, approval: bool) -> Snapshot {
    new_execution(
        ExecutionId::new(id).unwrap(),
        TurnId::new(format!("turn-{id}")).unwrap(),
        owner,
        Definition {
            route: RouteKey::new("/sqlite#agent").unwrap(),
            revision: ExecutionRevision::new("definition-1").unwrap(),
            batches: if approval {
                vec![vec![TaskSpec {
                    id: TaskId::new("task").unwrap(),
                    effect_id: EffectId::new("effect").unwrap(),
                    recovery: RecoveryPolicy::Pure,
                    requires_approval: true,
                }]]
            } else {
                vec![]
            },
            max_attempts: 4,
        },
        Map::new(),
        TransitionFacts { now_ms: 100 },
    )
    .unwrap()
}

fn apply(store: &SqliteStore, snapshot: &Snapshot, command: KernelCommand) -> Snapshot {
    let plan = transition(snapshot, command, TransitionFacts { now_ms: 101 }).unwrap();
    store
        .commit(&snapshot.execution_id, snapshot.into(), plan)
        .unwrap()
        .snapshot
}

fn recover(store: &SqliteStore, snapshot: &Snapshot) -> Snapshot {
    apply(
        store,
        snapshot,
        KernelCommand::Recover {
            available_revision: snapshot.definition.revision.clone(),
            turn_id: TurnId::new(format!("recover-{}", store.owner_generation())).unwrap(),
            owner_generation: store.owner_generation(),
        },
    )
}

#[test]
fn reopen_advances_owner_and_retains_terminal_history() {
    let directory = TestDirectory::new();
    let thread = ThreadId::new("thread").unwrap();
    let store = SqliteStore::open(directory.path()).unwrap();
    assert_eq!(store.owner_generation(), 1);
    store.create_thread(thread.clone()).unwrap();
    let first = store
        .admit(&thread, initial("first", 1, false))
        .unwrap()
        .snapshot;
    let first = apply(&store, &first, KernelCommand::Start);
    assert_eq!(first.status, ExecutionStatus::Completed);
    let first_journal = store.journal(&first.execution_id).unwrap();
    let second = store.admit(&thread, initial("second", 1, false)).unwrap();
    assert_eq!(
        store.thread(&thread).unwrap().unwrap().active_execution_id,
        Some(second.snapshot.execution_id.clone())
    );
    drop(store);
    let store = SqliteStore::open(directory.path()).unwrap();
    assert_eq!(store.owner_generation(), 2);
    assert_eq!(
        store
            .execution(&first.execution_id)
            .unwrap()
            .unwrap()
            .snapshot,
        first
    );
    assert_eq!(store.journal(&first.execution_id).unwrap(), first_journal);
    assert_eq!(
        store.execution(&second.snapshot.execution_id).unwrap(),
        Some(second.clone())
    );
    let start = transition(
        &second.snapshot,
        KernelCommand::Start,
        TransitionFacts { now_ms: 101 },
    )
    .unwrap();
    assert_eq!(
        store.commit(
            &second.snapshot.execution_id,
            (&second.snapshot).into(),
            start
        ),
        Err(StoreError::OwnerConflict)
    );
    let restored = recover(&store, &second.snapshot);
    assert_eq!(restored.owner_generation, 2);
    drop(store);
    assert_eq!(
        SqliteStore::open(directory.path())
            .unwrap()
            .owner_generation(),
        3
    );
    assert!(directory.path().join("owner.lock").exists());
}

#[test]
fn parked_snapshot_persists_and_continues_blocking_admission() {
    let directory = TestDirectory::new();
    let thread = ThreadId::new("thread").unwrap();
    let store = SqliteStore::open(directory.path()).unwrap();
    store.create_thread(thread.clone()).unwrap();
    let snapshot = store
        .admit(&thread, initial("parked", 1, true))
        .unwrap()
        .snapshot;
    let snapshot = apply(&store, &snapshot, KernelCommand::Start);
    let snapshot = apply(
        &store,
        &snapshot,
        KernelCommand::Park {
            task_id: TaskId::new("task").unwrap(),
            interrupt_id: InterruptId::new("approval").unwrap(),
            prompt: json!({"permission":"run"}),
        },
    );
    assert_eq!(snapshot.status, ExecutionStatus::WaitingForApproval);
    drop(store);
    let store = SqliteStore::open(directory.path()).unwrap();
    assert_eq!(
        store
            .execution(&snapshot.execution_id)
            .unwrap()
            .unwrap()
            .snapshot,
        snapshot
    );
    assert_eq!(
        store.admit(&thread, initial("next", 2, false)),
        Err(StoreError::ThreadBusy)
    );
    assert_eq!(
        recover(&store, &snapshot).status,
        ExecutionStatus::WaitingForApproval
    );
}

#[test]
fn lock_is_exclusive_and_failed_open_does_not_advance_owner() {
    let directory = TestDirectory::new();
    let store = SqliteStore::open(directory.path()).unwrap();
    assert!(matches!(
        SqliteStore::open(directory.path()),
        Err(StoreError::AlreadyOwned)
    ));
    drop(store);
    assert_eq!(
        SqliteStore::open(directory.path())
            .unwrap()
            .owner_generation(),
        2
    );
}

#[test]
fn malformed_and_semantically_corrupt_payloads_fail_loads() {
    for corrupt in ["malformed", "shape", "identity", "revision", "owner"] {
        let directory = TestDirectory::new();
        let thread = ThreadId::new("thread").unwrap();
        let store = SqliteStore::open(directory.path()).unwrap();
        store.create_thread(thread.clone()).unwrap();
        let snapshot = store
            .admit(&thread, initial("execution", 1, false))
            .unwrap()
            .snapshot;
        drop(store);
        let connection = Connection::open(directory.database()).unwrap();
        let payload = if corrupt == "malformed" {
            "{".to_owned()
        } else {
            let mut payload = serde_json::to_value(&snapshot).unwrap();
            match corrupt {
                "shape" => payload["attempts_used"] = json!(1),
                "identity" => payload["execution_id"] = json!("other"),
                "revision" => payload["revision"] = json!(1),
                "owner" => payload["owner_generation"] = json!(99),
                _ => unreachable!(),
            }
            serde_json::to_string(&payload).unwrap()
        };
        connection
            .execute("UPDATE executions SET snapshot = ?1", [payload])
            .unwrap();
        drop(connection);
        let store = SqliteStore::open(directory.path()).unwrap();
        assert!(
            matches!(
                store.execution(&snapshot.execution_id),
                Err(StoreError::Corrupt(_))
            ),
            "{corrupt}"
        );
        assert!(
            matches!(
                store.admit(&thread, initial("next", 2, false)),
                Err(StoreError::Corrupt(_))
            ),
            "{corrupt}"
        );
    }
}

#[test]
fn unknown_schema_and_migration_checksum_fail_before_epoch_increment() {
    for mutation in [
        "UPDATE metadata SET schema_version = 999",
        "UPDATE metadata SET migration_checksum = 'incorrect'",
        "ALTER TABLE executions ADD COLUMN unrecognized TEXT",
        "CREATE TABLE sqliteXunexpected (value TEXT)",
        "CREATE TRIGGER suppress_epoch BEFORE UPDATE ON metadata BEGIN SELECT RAISE(IGNORE); END",
    ] {
        let directory = TestDirectory::new();
        drop(SqliteStore::open(directory.path()).unwrap());
        let connection = Connection::open(directory.database()).unwrap();
        connection.execute_batch(mutation).unwrap();
        assert!(matches!(
            SqliteStore::open(directory.path()),
            Err(StoreError::UnsupportedSchema)
        ));
        let epoch: Vec<u8> = connection
            .query_row("SELECT owner_epoch FROM metadata", [], |row| row.get(0))
            .unwrap();
        assert_eq!(epoch, 1_u64.to_be_bytes());
    }
}

#[test]
fn journal_insert_failure_rolls_back_snapshot_and_active_slot() {
    let directory = TestDirectory::new();
    let thread = ThreadId::new("thread").unwrap();
    let store = SqliteStore::open(directory.path()).unwrap();
    store.create_thread(thread.clone()).unwrap();
    let original = store
        .admit(&thread, initial("execution", 1, false))
        .unwrap();
    let journal = store.journal(&original.snapshot.execution_id).unwrap();
    let original_thread = store.thread(&thread).unwrap();
    drop(store);
    let store = SqliteStore::open(directory.path()).unwrap();
    let connection = Connection::open(directory.database()).unwrap();
    connection.execute_batch("CREATE TRIGGER abort_commit BEFORE INSERT ON journal WHEN NEW.kind = 'commit' BEGIN SELECT RAISE(ABORT, 'injected journal failure'); END;").unwrap();
    drop(connection);
    let plan = transition(
        &original.snapshot,
        KernelCommand::Recover {
            available_revision: original.snapshot.definition.revision.clone(),
            turn_id: TurnId::new("recovered").unwrap(),
            owner_generation: 2,
        },
        TransitionFacts { now_ms: 101 },
    )
    .unwrap();
    assert!(matches!(
        store.commit(
            &original.snapshot.execution_id,
            (&original.snapshot).into(),
            plan
        ),
        Err(StoreError::Unavailable(_))
    ));
    assert_eq!(
        store.execution(&original.snapshot.execution_id).unwrap(),
        Some(original.clone())
    );
    assert_eq!(store.thread(&thread).unwrap(), original_thread);
    assert_eq!(
        store.journal(&original.snapshot.execution_id).unwrap(),
        journal
    );
    assert_eq!(
        store.admit(&thread, initial("next", 2, false)),
        Err(StoreError::ThreadBusy)
    );
    drop(store);
    let connection = Connection::open(directory.database()).unwrap();
    connection
        .execute_batch("DROP TRIGGER abort_commit")
        .unwrap();
    drop(connection);
    let store = SqliteStore::open(directory.path()).unwrap();
    assert_eq!(recover(&store, &original.snapshot).owner_generation, 3);
}

#[test]
fn execution_with_missing_thread_is_corrupt() {
    let directory = TestDirectory::new();
    let store = SqliteStore::open(directory.path()).unwrap();
    let thread = ThreadId::new("missing-thread").unwrap();
    store.create_thread(thread.clone()).unwrap();
    let execution = store.admit(&thread, initial("orphaned", 1, false)).unwrap();
    drop(store);
    let connection = Connection::open(directory.database()).unwrap();
    connection
        .pragma_update(None, "foreign_keys", "OFF")
        .unwrap();
    connection.execute("DELETE FROM threads", []).unwrap();
    drop(connection);
    let store = SqliteStore::open(directory.path()).unwrap();
    assert!(matches!(
        store.execution(&execution.snapshot.execution_id),
        Err(StoreError::Corrupt(_))
    ));
}

#[test]
fn admission_journal_failure_rolls_back_uniqueness_and_thread_slot() {
    let directory = TestDirectory::new();
    let thread = ThreadId::new("thread").unwrap();
    let store = SqliteStore::open(directory.path()).unwrap();
    store.create_thread(thread.clone()).unwrap();
    drop(store);
    let store = SqliteStore::open(directory.path()).unwrap();
    let connection = Connection::open(directory.database()).unwrap();
    connection.execute_batch("CREATE TRIGGER abort_admit BEFORE INSERT ON journal WHEN NEW.kind = 'admit' BEGIN SELECT RAISE(ABORT, 'injected admission failure'); END;").unwrap();
    drop(connection);
    let snapshot = initial("execution", 2, false);
    assert!(matches!(
        store.admit(&thread, snapshot.clone()),
        Err(StoreError::Unavailable(_))
    ));
    assert_eq!(store.execution(&snapshot.execution_id).unwrap(), None);
    assert_eq!(
        store.thread(&thread).unwrap().unwrap().active_execution_id,
        None
    );
    assert!(store.journal(&snapshot.execution_id).unwrap().is_empty());
    drop(store);
    let connection = Connection::open(directory.database()).unwrap();
    connection
        .execute_batch("DROP TRIGGER abort_admit")
        .unwrap();
    drop(connection);
    let store = SqliteStore::open(directory.path()).unwrap();
    assert!(store.admit(&thread, initial("execution", 3, false)).is_ok());
}

#[test]
fn unsigned_epoch_and_revision_round_trip_without_signed_truncation() {
    let directory = TestDirectory::new();
    let thread = ThreadId::new("thread").unwrap();
    let store = SqliteStore::open(directory.path()).unwrap();
    store.create_thread(thread.clone()).unwrap();
    let mut snapshot = store
        .admit(&thread, initial("execution", 1, false))
        .unwrap()
        .snapshot;
    drop(store);
    snapshot.revision = u64::MAX - 1;
    snapshot.owner_generation = u64::MAX - 1;
    let connection = Connection::open(directory.database()).unwrap();
    connection
        .execute(
            "UPDATE metadata SET owner_epoch = ?1",
            [u64::to_be_bytes(u64::MAX - 1).as_slice()],
        )
        .unwrap();
    connection
        .execute(
            "UPDATE executions SET revision = ?1, owner_epoch = ?2, snapshot = ?3",
            params![
                snapshot.revision.to_be_bytes().as_slice(),
                snapshot.owner_generation.to_be_bytes().as_slice(),
                serde_json::to_string(&snapshot).unwrap()
            ],
        )
        .unwrap();
    drop(connection);
    let store = SqliteStore::open(directory.path()).unwrap();
    assert_eq!(store.owner_generation(), u64::MAX);
    assert_eq!(
        store
            .execution(&snapshot.execution_id)
            .unwrap()
            .unwrap()
            .snapshot,
        snapshot
    );
    let recovered = recover(&store, &snapshot);
    assert_eq!(recovered.revision, u64::MAX);
    assert_eq!(recovered.owner_generation, u64::MAX);
    assert_eq!(
        store
            .journal(&snapshot.execution_id)
            .unwrap()
            .last()
            .unwrap()
            .revision,
        u64::MAX
    );
    drop(store);
    assert!(matches!(
        SqliteStore::open(directory.path()),
        Err(StoreError::Unavailable(_))
    ));
}

#[test]
fn current_sqlite_has_the_fixed_wal_runtime() {
    assert!(
        rusqlite::version_number() >= 3_053_002,
        "SQLite {} predates the bundled WAL fix",
        rusqlite::version()
    );
}

#[test]
fn common_store_contract() {
    let directory = TestDirectory::new();
    let store = SqliteStore::open(directory.path()).unwrap();
    support::backend_contract(&store);
}

#[test]
fn separate_process_lock_is_released_after_close_and_abrupt_exit() {
    for abrupt in [false, true] {
        let directory = TestDirectory::new();
        let mut child = Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "lock_child_helper",
                "--ignored",
                "--nocapture",
                "--test-threads=1",
            ])
            .env("B4_SQLITE_CHILD_DIRECTORY", directory.path())
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit())
            .spawn()
            .unwrap();
        let mut output = BufReader::new(child.stdout.take().unwrap());
        let mut line = String::new();
        loop {
            line.clear();
            assert_ne!(
                output.read_line(&mut line).unwrap(),
                0,
                "child exited before acquiring its lock"
            );
            if line.contains("B4_SQLITE_READY") {
                break;
            }
        }
        assert!(matches!(
            SqliteStore::open(directory.path()),
            Err(StoreError::AlreadyOwned)
        ));
        if abrupt {
            child.kill().unwrap();
        } else {
            writeln!(child.stdin.as_mut().unwrap(), "release").unwrap();
        }
        let status = child.wait().unwrap();
        assert_eq!(status.success(), !abrupt);
        let store = SqliteStore::open(directory.path()).unwrap();
        assert_eq!(store.owner_generation(), 2);
        let execution = store
            .execution(&ExecutionId::new("child-execution").unwrap())
            .unwrap()
            .unwrap();
        assert_eq!(execution.snapshot.owner_generation, 1);
        assert_eq!(
            store
                .journal(&execution.snapshot.execution_id)
                .unwrap()
                .len(),
            2
        );
        assert_eq!(recover(&store, &execution.snapshot).owner_generation, 2);
    }
}

#[test]
#[ignore = "child process helper, invoked explicitly by the parent lock test"]
fn lock_child_helper() {
    let directory = std::env::var_os("B4_SQLITE_CHILD_DIRECTORY").expect("child directory");
    let store = SqliteStore::open(PathBuf::from(directory)).unwrap();
    let thread = ThreadId::new("child-thread").unwrap();
    store.create_thread(thread.clone()).unwrap();
    let snapshot = store
        .admit(
            &thread,
            initial("child-execution", store.owner_generation(), true),
        )
        .unwrap()
        .snapshot;
    apply(&store, &snapshot, KernelCommand::Start);
    println!("B4_SQLITE_READY");
    std::io::stdout().flush().unwrap();
    let mut line = String::new();
    std::io::stdin().read_line(&mut line).unwrap();
    assert_eq!(line.trim(), "release");
    drop(store);
}
