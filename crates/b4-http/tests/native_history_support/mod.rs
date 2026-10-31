use axum::http::HeaderMap;
use b4_contract::*;
use b4_http::*;
use b4_runtime::{approval::*, *};
use b4_sqlite::SqliteStore;
use serde_json::{json, Map};
use std::{
    collections::BTreeMap,
    fs::{DirBuilder, OpenOptions},
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc, Mutex,
    },
    time::Duration,
};

#[cfg(unix)]
use std::os::unix::fs::{DirBuilderExt, OpenOptionsExt, PermissionsExt};

pub const WAIT: Duration = Duration::from_secs(15);
static NEXT_DIRECTORY: AtomicU64 = AtomicU64::new(0);

pub struct Directory(PathBuf);
impl Directory {
    pub fn new() -> Self {
        let path = std::env::temp_dir().join(format!(
            "b4-native-history-{}-{}",
            std::process::id(),
            NEXT_DIRECTORY.fetch_add(1, Ordering::Relaxed)
        ));
        let mut builder = DirBuilder::new();
        #[cfg(unix)]
        builder.mode(0o700);
        builder.create(&path).unwrap();
        for name in ["owner.lock", "runtime.sqlite3"] {
            let mut options = OpenOptions::new();
            options.write(true).create_new(true);
            #[cfg(unix)]
            options.mode(0o600);
            drop(options.open(path.join(name)).unwrap());
        }
        #[cfg(unix)]
        {
            assert_eq!(path.metadata().unwrap().permissions().mode() & 0o777, 0o700);
            for name in ["owner.lock", "runtime.sqlite3"] {
                let metadata = std::fs::symlink_metadata(path.join(name)).unwrap();
                assert!(metadata.is_file());
                assert_eq!(metadata.permissions().mode() & 0o777, 0o600);
            }
        }
        Self(path)
    }

    pub fn path(&self) -> &Path {
        &self.0
    }

    pub fn rows(&self) -> Vec<(String, Vec<Vec<rusqlite::types::Value>>)> {
        let connection = rusqlite::Connection::open(self.0.join("runtime.sqlite3")).unwrap();
        [
            "metadata",
            "threads",
            "executions",
            "journal",
            "approval_grants",
            "approval_acceptances",
        ]
        .into_iter()
        .map(|table| {
            let mut statement = connection
                .prepare(&format!("SELECT * FROM {table} ORDER BY rowid"))
                .unwrap();
            let columns = statement.column_count();
            let rows = statement
                .query_map([], |row| {
                    (0..columns)
                        .map(|column| row.get(column))
                        .collect::<rusqlite::Result<Vec<rusqlite::types::Value>>>()
                })
                .unwrap()
                .collect::<rusqlite::Result<Vec<_>>>()
                .unwrap();
            (table.into(), rows)
        })
        .collect()
    }
}
impl Drop for Directory {
    fn drop(&mut self) {
        // Fixture cleanup follows explicit normal retirement; this is no proof.
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

pub fn principal() -> Principal {
    Principal {
        application: "native-history-fixture".into(),
        tenant: "native-history-tenant".into(),
        principal: "native-history-actor".into(),
    }
}

pub struct Auth;
impl Authenticator for Auth {
    fn authenticate(&self, headers: &HeaderMap) -> Result<Principal, AuthenticationError> {
        if headers
            .get("authorization")
            .is_some_and(|value| value == "Bearer native-history-fixture")
        {
            Ok(principal())
        } else {
            Err(AuthenticationError)
        }
    }
}

pub struct Policy;
impl ResourcePolicy for Policy {
    fn authorize(&self, actor: &Principal, _: &ResourceContext) -> bool {
        actor == &principal()
    }
}

pub fn definition(revision: &str) -> Definition {
    Definition {
        route: RouteKey::new("/native-history#workflow").unwrap(),
        revision: ExecutionRevision::new(revision).unwrap(),
        batches: vec![vec![TaskSpec {
            id: TaskId::new("shared-task").unwrap(),
            effect_id: EffectId::new("shared-effect").unwrap(),
            recovery: RecoveryPolicy::Pure,
            requires_approval: false,
        }]],
        max_attempts: 3,
    }
}

pub type Calls = Arc<Mutex<Vec<(EffectInvocation, InvocationContext)>>>;
pub struct FiniteEffect {
    pub calls: Calls,
    pub version: &'static str,
}
impl NativeEffect for FiniteEffect {
    fn invoke(
        &self,
        invocation: &EffectInvocation,
        context: &InvocationContext,
    ) -> Result<NativeOutcome, EffectUncertain> {
        self.calls
            .lock()
            .unwrap()
            .push((invocation.clone(), context.clone()));
        Ok(NativeOutcome::Succeeded(result(self.version)))
    }
}

pub fn result(version: &str) -> TaskResult {
    TaskResult {
        output: json!({"selected_version": version}),
        writes: vec![StateWrite::Replace {
            field: "actual_selected_version".into(),
            value: json!(version),
        }],
    }
}

pub fn registration(revision: &str, version: &'static str, calls: Calls) -> RouteRegistration {
    RouteRegistration {
        definition: definition(revision),
        effects: BTreeMap::from([(
            EffectId::new("shared-effect").unwrap(),
            EffectBinding {
                callback: Arc::new(FiniteEffect { calls, version }),
                output_kind: OutputKind::Value,
            },
        )]),
        input_validator: None,
    }
}

pub fn seed_accepted(store: &SqliteStore) -> (ThreadView, ApprovalView) {
    let actor = principal();
    let thread = ThreadId::new("native-history-prior-thread").unwrap();
    store
        .create_thread_with(ThreadSeed {
            id: thread.clone(),
            created_at_ms: 1,
            metadata: Map::from_iter([("fixture".into(), json!("prior-v1"))]),
            origin: Some(ThreadOrigin {
                application: actor.application,
                tenant: actor.tenant,
                principal: actor.principal,
            }),
        })
        .unwrap();
    let initial = b4_kernel::new_execution(
        ExecutionId::new("native-history-prior-execution").unwrap(),
        TurnId::new("native-history-prior-turn").unwrap(),
        store.owner_generation(),
        definition("v1"),
        Map::from_iter([("input".into(), json!({"original": true}))]),
        TransitionFacts { now_ms: 2 },
    )
    .unwrap();
    store.admit(&thread, initial).unwrap();
    let view = store.thread_view(&thread).unwrap().unwrap();
    let snapshot = &view.execution.as_ref().unwrap().snapshot;
    assert_eq!(snapshot.status, ExecutionStatus::Accepted);
    assert_eq!(snapshot.owner_generation, store.owner_generation());
    assert_eq!(snapshot.definition, definition("v1"));
    assert!(snapshot.tasks.is_empty());
    let approval = store
        .approval_view(&snapshot.execution_id)
        .unwrap()
        .unwrap();
    assert!(approval.grants.is_empty());
    assert!(approval.acceptances.is_empty());
    (view, approval)
}

pub async fn quiesce(shutdown: &ShutdownHandle) {
    shutdown.begin_shutdown();
    let outcome = tokio::time::timeout(WAIT, shutdown.wait_quiescent(WAIT))
        .await
        .expect("normal shutdown observation remained finite")
        .unwrap();
    assert_eq!(outcome, ShutdownOutcome::Quiescent { faulted: false });
}

// Additive private history qualifiers; helper modules declare no tests.
#[allow(dead_code)]
mod gates;
#[allow(unused_imports)]
pub use gates::{Gate, GateCleanup};
#[allow(dead_code)]
mod expanded;
pub use expanded::*;
#[allow(dead_code)]
mod server;
#[allow(unused_imports)]
pub use server::Server;
