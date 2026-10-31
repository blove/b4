use super::{script_route, Auth, OwnerPolicy};
use b4_contract::TaskResult;
use b4_http::{ApprovalHostConfig, HttpHost, NativeHostConfig, ScriptEffect};
use b4_runtime::{
    approval::{ApprovalServiceConfig, ApprovalStore},
    ApplicationIdentity, EffectBinding, EffectInvocation, EffectUncertain, InputValidator,
    InvocationContext, NativeApplication, NativeEffect, NativeOutcome, OutputKind,
    RouteRegistration,
};
use serde_json::json;
use std::{
    collections::BTreeMap,
    os::unix::fs::DirBuilderExt,
    path::PathBuf,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc, Condvar, Mutex, Weak,
    },
    time::Duration,
};

#[derive(Default)]
pub struct Gate {
    state: Mutex<(bool, bool)>,
    changed: Condvar,
}
impl Gate {
    pub fn hold(&self) {
        let mut state = self.state.lock().unwrap();
        state.0 = true;
        self.changed.notify_all();
        while !state.1 {
            let (next, timed) = self
                .changed
                .wait_timeout(state, Duration::from_secs(10))
                .unwrap();
            state = next;
            assert!(
                !timed.timed_out(),
                "controlled native effect must be released within fixture bound"
            );
        }
    }
    pub async fn entered(&self) {
        until(|| self.state.lock().unwrap().0).await;
    }
    pub fn release(&self) {
        self.state
            .lock()
            .unwrap_or_else(|poison| poison.into_inner())
            .1 = true;
        self.changed.notify_all();
    }
}
pub struct Release(pub Arc<Gate>);
impl Drop for Release {
    fn drop(&mut self) {
        self.0.release();
    }
}

pub async fn until(mut condition: impl FnMut() -> bool) {
    tokio::time::timeout(Duration::from_secs(5), async {
        while !condition() {
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
    })
    .await
    .expect("actual diagnostics fixture boundary must be observed within test bound");
}

struct Effect(Option<Arc<Gate>>);
impl NativeEffect for Effect {
    fn invoke(
        &self,
        _: &EffectInvocation,
        _: &InvocationContext,
    ) -> Result<NativeOutcome, EffectUncertain> {
        if let Some(gate) = &self.0 {
            gate.hold();
        }
        Ok(NativeOutcome::Succeeded(TaskResult {
            output: json!("private-native-output"),
            writes: vec![],
        }))
    }
}

pub fn native_host<S: ApprovalStore + 'static>(
    store: Arc<S>,
    gate: Option<Arc<Gate>>,
    validator: Option<Arc<dyn InputValidator>>,
) -> (HttpHost<S>, Weak<NativeApplication>) {
    let definition = script_route(ScriptEffect::Input, false).definition;
    let effect = definition.batches[0][0].effect_id.clone();
    let application = Arc::new(
        NativeApplication::new(
            ApplicationIdentity {
                application: "diagnostics-application".into(),
                revision: "private-application-revision".into(),
            },
            vec![RouteRegistration {
                definition,
                effects: BTreeMap::from([(
                    effect,
                    EffectBinding {
                        callback: Arc::new(Effect(gate)),
                        output_kind: OutputKind::Value,
                    },
                )]),
                input_validator: validator,
            }],
        )
        .unwrap(),
    );
    let weak = Arc::downgrade(&application);
    let mut approval = ApprovalHostConfig::new("private-policy-revision".into(), BTreeMap::new());
    if !store.approval_durable() {
        approval.service_config = ApprovalServiceConfig::VolatileFixture;
    }
    let mut config = NativeHostConfig::new(application, approval);
    config.limits.max_active_runs = 1;
    config.limits.heartbeat_ms = 10;
    (
        HttpHost::new_with_application(store, Arc::new(Auth), Arc::new(OwnerPolicy), config)
            .unwrap(),
        weak,
    )
}

static NEXT_DIRECTORY: AtomicU64 = AtomicU64::new(0);
pub struct Directory(pub PathBuf);
impl Directory {
    pub fn new() -> Self {
        let path = std::env::temp_dir().join(format!(
            "b4-request-diagnostics-http-{}-{}",
            std::process::id(),
            NEXT_DIRECTORY.fetch_add(1, Ordering::Relaxed)
        ));
        std::fs::DirBuilder::new()
            .mode(0o700)
            .create(&path)
            .expect("isolated private diagnostics fixture directory must be created");
        Self(path)
    }
}
impl Drop for Directory {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}
