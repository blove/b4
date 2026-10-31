use super::*;
use crate::*;
use b4_contract::*;
use b4_runtime::*;
use b4_sqlite::SqliteStore;
use std::{
    collections::BTreeMap,
    path::PathBuf,
    sync::{Condvar, Mutex, Weak},
    time::{Duration, Instant},
};

pub(super) struct Directory(pub PathBuf);
impl Directory {
    fn new() -> Self {
        let p = std::env::temp_dir().join(format!("b4-private-startup-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir(&p).unwrap();
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(&p, std::fs::Permissions::from_mode(0o700)).unwrap();
        }
        Self(p)
    }
}
impl Drop for Directory {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}
#[derive(Default, Clone, Copy)]
pub(super) struct GateFacts {
    pub entered: bool,
    pub explicit_release: bool,
    pub returned: bool,
    pub expired: bool,
    pub cleanup_release: bool,
}
pub(super) struct Gate {
    facts: Mutex<GateFacts>,
    changed: Condvar,
    notify: tokio::sync::Notify,
    deadline: Instant,
}
impl Gate {
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            facts: Mutex::new(GateFacts::default()),
            changed: Condvar::new(),
            notify: tokio::sync::Notify::new(),
            deadline: Instant::now() + Duration::from_secs(30),
        })
    }
    pub fn hold(&self) {
        let mut facts = self.facts.lock().unwrap();
        facts.entered = true;
        self.notify.notify_waiters();
        while !facts.explicit_release && !facts.cleanup_release {
            let remaining = self.deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                facts.expired = true;
                break;
            }
            facts = self.changed.wait_timeout(facts, remaining).unwrap().0;
        }
        if Instant::now() >= self.deadline {
            facts.expired = true;
        }
        facts.returned = true;
        self.notify.notify_waiters();
    }
    pub async fn entered(&self) {
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                let notified = self.notify.notified();
                tokio::pin!(notified);
                notified.as_mut().enable();
                if self.facts.lock().unwrap().entered {
                    break;
                }
                notified.await;
            }
        })
        .await
        .expect("private cleanup did not enter");
    }
    pub fn release(&self) {
        let mut facts = self.facts.lock().unwrap();
        if Instant::now() >= self.deadline {
            facts.expired = true;
        }
        facts.explicit_release = true;
        self.changed.notify_all();
    }
    pub fn positive(&self) -> bool {
        let f = *self.facts.lock().unwrap();
        f.entered && f.explicit_release && f.returned && !f.expired && !f.cleanup_release
    }
}
pub(super) struct Cleanup(pub Arc<Gate>);
impl Drop for Cleanup {
    fn drop(&mut self) {
        let mut facts = self.0.facts.lock().unwrap();
        if !facts.explicit_release {
            facts.cleanup_release = true;
            self.0.changed.notify_all();
        }
    }
}
struct Auth;
impl Authenticator for Auth {
    fn authenticate(&self, _: &axum::http::HeaderMap) -> Result<Principal, AuthenticationError> {
        Ok(Principal {
            application: "private-startup".into(),
            tenant: "tenant".into(),
            principal: "actor".into(),
        })
    }
}
struct CarrierPayload {
    _store: Arc<SqliteStore>,
    _application: Arc<NativeApplication>,
    gate: Arc<Gate>,
}
impl Drop for CarrierPayload {
    fn drop(&mut self) {
        self.gate.hold();
    }
}
struct PanicAuth {
    store: Arc<SqliteStore>,
    application: Arc<NativeApplication>,
    gate: Arc<Gate>,
}
impl Authenticator for PanicAuth {
    fn authenticate(&self, _: &axum::http::HeaderMap) -> Result<Principal, AuthenticationError> {
        Auth.authenticate(&axum::http::HeaderMap::new())
    }
}
impl Drop for PanicAuth {
    fn drop(&mut self) {
        std::panic::panic_any(CarrierPayload {
            _store: self.store.clone(),
            _application: self.application.clone(),
            gate: self.gate.clone(),
        });
    }
}
struct Policy;
impl ResourcePolicy for Policy {
    fn authorize(&self, _: &Principal, _: &ResourceContext) -> bool {
        true
    }
}
struct Callback;
impl NativeEffect for Callback {
    fn invoke(
        &self,
        _: &EffectInvocation,
        _: &InvocationContext,
    ) -> Result<NativeOutcome, EffectUncertain> {
        Ok(NativeOutcome::Succeeded(TaskResult {
            output: serde_json::json!({}),
            writes: vec![],
        }))
    }
}
pub(super) struct Fixture {
    pub directory: Directory,
    pub owner: u64,
    pub store: Weak<SqliteStore>,
    pub application: Weak<NativeApplication>,
    pub carrier: Option<OwnedShared<SqliteStore>>,
    pub supervisor: ShutdownHandle,
    pub capability: StartupCapability,
}
impl Fixture {
    pub fn new() -> Self {
        Self::with_carrier_panic(None)
    }
    pub fn with_carrier_panic(gate: Option<Arc<Gate>>) -> Self {
        let directory = Directory::new();
        let store = Arc::new(SqliteStore::open(&directory.0).unwrap());
        let owner = store.owner_generation();
        let store_weak = Arc::downgrade(&store);
        let application = Arc::new(
            NativeApplication::new(
                ApplicationIdentity {
                    application: "private-startup".into(),
                    revision: "native-1".into(),
                },
                vec![RouteRegistration {
                    definition: Definition {
                        route: RouteKey::new("/private-startup#workflow").unwrap(),
                        revision: ExecutionRevision::new("private-1").unwrap(),
                        max_attempts: 1,
                        batches: vec![vec![TaskSpec {
                            id: TaskId::new("task").unwrap(),
                            effect_id: EffectId::new("effect").unwrap(),
                            requires_approval: false,
                            recovery: RecoveryPolicy::Pure,
                        }]],
                    },
                    effects: BTreeMap::from([(
                        EffectId::new("effect").unwrap(),
                        EffectBinding {
                            callback: Arc::new(Callback),
                            output_kind: OutputKind::Value,
                        },
                    )]),
                    input_validator: None,
                }],
            )
            .unwrap(),
        );
        let application_weak = Arc::downgrade(&application);
        let mut host = HttpHost::new_with_application(
            store.clone(),
            Arc::new(Auth),
            Arc::new(Policy),
            NativeHostConfig::new(
                application.clone(),
                ApprovalHostConfig::new("policy-1".into(), BTreeMap::new()),
            ),
        )
        .unwrap();
        // Private fixture assembly closes the real supervisor before escape;
        // the public preparing constructor remains a deliberate stage stub.
        let shared = Arc::get_mut(&mut host.shared).unwrap();
        if let Some(gate) = gate {
            shared.auth = Arc::new(PanicAuth {
                store: store.clone(),
                application: application.clone(),
                gate,
            });
        }
        shared.supervisor = ShutdownHandle::new_preparing(shared.fault.clone());
        let supervisor = host.shutdown_handle();
        let (capability, token) = supervisor.begin_startup().unwrap();
        let carrier = OwnedShared::from_registered(host.shared.clone(), token);
        drop(host);
        drop(store);
        drop(application);
        Self {
            directory,
            owner,
            store: store_weak,
            application: application_weak,
            carrier: Some(carrier),
            supervisor,
            capability,
        }
    }
    pub fn held_resources(&self) -> bool {
        let store_alive = self.store.upgrade().is_some();
        let application_alive = self.application.upgrade().is_some();
        let writer_locked = matches!(
            SqliteStore::open(&self.directory.0),
            Err(StoreError::AlreadyOwned)
        );
        store_alive && application_alive && writer_locked
    }
    pub async fn retired(&self) -> ShutdownOutcome {
        let result = self
            .supervisor
            .wait_quiescent(Duration::from_secs(5))
            .await
            .unwrap();
        assert!(self.store.upgrade().is_none());
        assert!(self.application.upgrade().is_none());
        let reopened = SqliteStore::open(&self.directory.0).unwrap();
        assert_eq!(reopened.owner_generation(), self.owner + 1);
        reopened.readiness().unwrap();
        result
    }
}
