//! New private process qualifiers; child infrastructure adds no case credit.
use super::*;
use b4_contract::*;
use b4_http::Operation;
use b4_runtime::{approval::ApprovalStore, NativeApplication, Store};
use std::sync::Weak;

#[path = "process_startup_test_support.rs"]
mod support;
use support::*;

pub(in crate::process) struct Witnesses {
    store: Weak<SqliteStore>,
    auth: Weak<dyn Authenticator>,
    policy: Weak<dyn ResourcePolicy>,
    application: Weak<NativeApplication>,
}
impl Witnesses {
    pub(in crate::process) fn new(
        store: &Arc<SqliteStore>,
        auth: &Arc<dyn Authenticator>,
        policy: &Arc<dyn ResourcePolicy>,
        application: Weak<NativeApplication>,
    ) -> Self {
        Self {
            store: Arc::downgrade(store),
            auth: Arc::downgrade(auth),
            policy: Arc::downgrade(policy),
            application,
        }
    }
    pub(in crate::process) fn retired(self, observer: Option<&super::super::tests::Observer>) {
        assert!(self.store.upgrade().is_none());
        assert!(self.auth.upgrade().is_none());
        assert!(self.policy.upgrade().is_none());
        assert!(self.application.upgrade().is_none());
        if let Some(observer) = observer {
            observer.marker("startup-all-weak-absent");
        }
    }
}
pub(in crate::process) fn runtime_destroyed(observer: Option<&super::super::tests::Observer>) {
    if let Some(observer) = observer {
        observer.marker("startup-runtime-destroyed");
    }
}

struct RecoverPolicy {
    observer: super::super::tests::Observer,
    inner: Arc<dyn ResourcePolicy>,
    entered: std::sync::atomic::AtomicBool,
}
impl ResourcePolicy for RecoverPolicy {
    fn authorize(&self, principal: &Principal, context: &ResourceContext) -> bool {
        if context.operation == Operation::Recover
            && !self.entered.swap(true, std::sync::atomic::Ordering::AcqRel)
        {
            hold(&self.observer.directory, "recover");
        }
        self.inner.authorize(principal, context)
    }
}
pub(in crate::process) fn policy(
    observer: Option<&super::super::tests::Observer>,
    inner: Arc<dyn ResourcePolicy>,
) -> Arc<dyn ResourcePolicy> {
    match observer
        .filter(|observer| matches!(observer.scenario.as_str(), "recover-held" | "lost-panic"))
    {
        Some(observer) => Arc::new(RecoverPolicy {
            observer: observer.clone(),
            inner,
            entered: std::sync::atomic::AtomicBool::new(false),
        }),
        None => inner,
    }
}

#[test]
fn startup_resolver_accepts_only_the_fixed_trusted_demo_tuple() {
    let origin = ThreadOrigin {
        application: "native-demo".into(),
        tenant: "local".into(),
        principal: "owner".into(),
    };
    let resolved = DemoResolver.resolve(&origin).unwrap();
    assert_eq!(resolved.application, "native-demo");
    assert_eq!(resolved.tenant, "local");
    assert_eq!(resolved.principal, "owner");
    for field in 0..3 {
        let mut foreign = origin.clone();
        match field {
            0 => foreign.application = "foreign-application".into(),
            1 => foreign.tenant = "foreign-tenant".into(),
            _ => foreign.principal = "foreign-principal".into(),
        }
        assert_eq!(
            DemoResolver.resolve(&foreign),
            Err(StartupResolutionError::Unauthorized)
        );
    }
}

#[test]
fn signal_during_held_recovery_keeps_ownership_until_normal_return() {
    let directory = Directory::new();
    let seeded = seed(&directory.data(), false);
    let mut child = OwnedChild::spawn(&directory, "recover-held");
    let address = child.listening();
    child.marker("recover-entered");
    assert_eq!(request(address, "GET", "/healthz", ""), 200);
    assert_eq!(request(address, "GET", "/readyz", ""), 503);
    assert_owned(&directory.data());
    let current = current_owner(&directory.data());
    assert_eq!(current, seeded.owner + 2);
    child.signal("-TERM");
    child.marker("draining");
    child.marker("incomplete");
    child.ping(1);
    child.assert_running();
    assert_owned(&directory.data());
    assert_absent(&directory, "startup-runtime-destroyed");
    assert_absent(&directory, "startup-all-weak-absent");
    release(&directory.path, "recover");
    child.marker("recover-returned");
    let output = child.normal_exit();
    assert!(output.stderr.is_empty());
    child.marker_after_exit("quiescent");
    child.marker_after_exit("server-success");
    child.marker_after_exit("startup-all-weak-absent");
    child.marker_after_exit("startup-runtime-destroyed");
    let successor = SqliteStore::open(directory.data()).unwrap();
    assert_eq!(successor.owner_generation(), current + 1);
    drop(successor);
    assert_eq!(output.result, "success");
}

#[test]
fn opaque_lost_server_during_preparation_keeps_payload_and_executor_owned() {
    let directory = Directory::new();
    let _seeded = seed(&directory.data(), false);
    let mut child = OwnedChild::spawn(&directory, "lost-panic");
    let _address = child.listening();
    child.marker("recover-entered");
    assert_owned(&directory.data());
    mark(&directory.path, "panic-server");
    child.marker("outer-panic-issued");
    child.marker("server-failure");
    child.marker("draining");
    child.marker("incomplete");
    child.ping(1);
    assert_absent(&directory, "opaque-payload-dropped");
    assert_absent(&directory, "startup-runtime-destroyed");
    assert_absent(&directory, "runner-returned");
    assert_owned(&directory.data());
    release(&directory.path, "recover");
    child.marker("recover-returned");
    child.marker("quiescent");
    child.ping(2);
    child.assert_running();
    assert_absent(&directory, "opaque-payload-dropped");
    assert_absent(&directory, "startup-all-weak-absent");
    assert_absent(&directory, "startup-runtime-destroyed");
    assert_owned(&directory.data());
    // Lost intentionally has no normal retirement. Killing this negative
    // control is external cleanup only, never a writer-release witness.
    child.kill_cleanup();
}

#[test]
fn readiness_precedes_settlement_of_a_legally_accepted_recovered_callback() {
    let directory = Directory::new();
    let seeded = seed(&directory.data(), true);
    let mut child = OwnedChild::spawn(&directory, "callback-held");
    let address = child.listening();
    child.marker("callback-entered");
    child.marker("startup-complete");
    assert_eq!(request(address, "GET", "/readyz", ""), 200);
    assert_eq!(request(address, "GET", "/healthz", ""), 200);
    assert_owned(&directory.data());
    let current = current_owner(&directory.data());
    assert_eq!(current, seeded.owner + 2);
    let held = captured_snapshot(
        &directory.data(),
        &seeded.view.execution.snapshot.execution_id,
    );
    assert_eq!(held.owner_generation, current);
    assert_eq!(held.definition, seeded.view.execution.snapshot.definition);
    assert_eq!(held.tasks[0], seeded.view.execution.snapshot.tasks[0]);
    assert!(held.tasks[1].approved);
    assert_eq!(held.tasks[1].status, TaskStatus::InFlight);
    assert_eq!(captured_authority(&directory.data()), seeded.authority_rows);
    child.signal("-TERM");
    child.marker("draining");
    child.marker("incomplete");
    child.ping(1);
    child.assert_running();
    assert_owned(&directory.data());
    assert_absent(&directory, "startup-runtime-destroyed");
    release(&directory.path, "callback");
    child.marker("callback-returned");
    let output = child.normal_exit();
    assert!(output.stderr.is_empty());
    child.marker_after_exit("quiescent");
    child.marker_after_exit("server-success");
    child.marker_after_exit("startup-all-weak-absent");
    child.marker_after_exit("startup-runtime-destroyed");
    let successor = SqliteStore::open(directory.data()).unwrap();
    assert_eq!(successor.owner_generation(), current + 1);
    let completed = successor
        .approval_view(&seeded.view.execution.snapshot.execution_id)
        .unwrap()
        .unwrap();
    assert_eq!(
        completed.execution.snapshot.status,
        ExecutionStatus::Completed
    );
    assert_eq!(
        completed.execution.snapshot.values["items"],
        serde_json::json!(["prepared", "released"])
    );
    assert_eq!(
        completed.execution.snapshot.definition,
        seeded.view.execution.snapshot.definition
    );
    assert_eq!(
        completed.execution.snapshot.tasks[0],
        seeded.view.execution.snapshot.tasks[0]
    );
    assert_eq!(completed.grants, seeded.view.grants);
    assert_eq!(completed.acceptances, seeded.view.acceptances);
    drop(successor);
    assert_eq!(output.result, "success");
}

#[test]
#[ignore = "private selflaunch infrastructure; not a genuine qualification case"]
fn startup_child_fixture() {
    let Some(directory) = std::env::var_os(CHILD_DIRECTORY) else {
        return;
    };
    let directory = std::path::PathBuf::from(directory);
    let scenario = std::env::var(CHILD_SCENARIO).unwrap();
    crate::install_panic_hook();
    let observer = super::super::tests::Observer {
        directory: directory.clone(),
        scenario: scenario.clone(),
    };
    let config = config(&directory.join("data"));
    let assembly = if scenario == "callback-held" {
        held_application(&directory)
    } else {
        crate::demo::assemble().unwrap()
    };
    let result = super::super::serve_owned(config, assembly, true, Some(observer));
    let code = match result {
        Ok(()) => "success",
        Err(error) => error.code(),
    };
    write_marker(&directory, "runner-result", code);
    mark(&directory, "runner-returned");
}
