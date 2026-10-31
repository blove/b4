use crate::{
    host::Shared,
    supervisor::{MutationLease, OwnerToken, StartupCapability},
};
use b4_runtime::{approval::ApprovalError, StoreError};
use std::{
    any::Any,
    future::Future,
    ops::Deref,
    panic::{catch_unwind, resume_unwind, AssertUnwindSafe},
    pin::Pin,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    task::{Context, Poll},
};

/// Fields retire in declaration order: release the framework's Shared/Store
/// capture before the final owner group can report physical quiescence.
pub(crate) struct OwnedShared<S> {
    shared: Option<Arc<Shared<S>>>,
    owner: OwnerToken,
}
impl<S> Clone for OwnedShared<S> {
    fn clone(&self) -> Self {
        Self {
            shared: self.shared.clone(),
            owner: self.owner.clone(),
        }
    }
}
impl<S> Deref for OwnedShared<S> {
    type Target = Shared<S>;
    fn deref(&self) -> &Self::Target {
        self.shared.as_deref().expect("owned_shared_retired")
    }
}
pub(crate) fn dispose_panic(mut payload: Box<dyn Any + Send>) {
    loop {
        match catch_unwind(AssertUnwindSafe(|| drop(payload))) {
            Ok(()) => break,
            // Trusted destructors can replace one Store-bearing payload with
            // another. Actual disposal remains owned, even if it never ends.
            Err(next) => payload = next,
        }
    }
}
impl<S> Drop for OwnedShared<S> {
    fn drop(&mut self) {
        let Some(shared) = self.shared.take() else {
            return;
        };
        let fault = shared.fault.clone();
        // Keep the owner field alive through capture destruction and any opaque
        // replacement payloads, including normal async request abandonment.
        if let Err(panic) = catch_unwind(AssertUnwindSafe(|| drop(shared))) {
            fault.store(true, Ordering::Release);
            dispose_panic(panic);
        }
    }
}
impl<S: Send + Sync + 'static> OwnedShared<S> {
    pub(crate) fn register(shared: Arc<Shared<S>>) -> Option<Self> {
        let owner = shared.supervisor.register_owner()?;
        Some(Self {
            shared: Some(shared),
            owner,
        })
    }
    pub(crate) fn register_startup(
        shared: Arc<Shared<S>>,
        capability: &StartupCapability,
    ) -> Option<Self> {
        let owner = shared.supervisor.register_startup_owner(capability)?;
        Some(Self::from_registered(shared, owner))
    }
    pub(crate) fn from_registered(shared: Arc<Shared<S>>, owner: OwnerToken) -> Self {
        Self {
            shared: Some(shared),
            owner,
        }
    }
    pub(crate) fn anchor(&self) -> OwnerToken {
        self.owner.clone()
    }
    pub(crate) fn shared_arc(&self) -> Arc<Shared<S>> {
        self.shared.as_ref().expect("owned_shared_retired").clone()
    }
    pub(crate) fn try_admit(&self) -> Option<MutationLease> {
        self.owner.try_admit()
    }
    /// The job can borrow Shared, but cannot outlive its counted carrier. Its
    /// closure and nested guards are consumed before carrier retirement. Panic
    /// classification happens here even when the async caller has disappeared.
    pub(crate) fn spawn<R: Send + 'static>(
        self,
        job: impl FnOnce(&OwnedShared<S>) -> R + Send + 'static,
    ) -> tokio::task::JoinHandle<R> {
        tokio::task::spawn_blocking(move || {
            let result = catch_unwind(AssertUnwindSafe(|| job(&self)));
            match result {
                Ok(value) => {
                    drop(self);
                    value
                }
                Err(panic) => {
                    self.fault.store(true, Ordering::Release);
                    // Opaque application panic payloads can retain a Store.
                    // Destroy them while this job is still counted, and expose
                    // only a static marker through Tokio's JoinError.
                    dispose_panic(panic);
                    drop(self);
                    resume_unwind(Box::new(()))
                }
            }
        })
    }
}
pub(crate) fn record_store_failure<S>(shared: &Shared<S>, error: &StoreError) {
    if !matches!(
        error,
        StoreError::ThreadNotFound
            | StoreError::ThreadBusy
            | StoreError::RevisionConflict
            | StoreError::ThreadExists
    ) {
        shared.fault.store(true, Ordering::Release);
    }
}
pub(crate) fn record_approval_failure<S>(shared: &Shared<S>, error: &ApprovalError) {
    if crate::transport::approval_failure_is_infrastructure(error) {
        shared.fault.store(true, Ordering::Release);
    }
}

/// The inner future may produce only a pure completion candidate. Keep a
/// same-group anchor outside it through every capture and opaque payload drop;
/// activation is possible only after all resource-bearing captures retire.
pub(crate) struct StartupFuture<F, S> {
    inner: Option<Pin<Box<F>>>,
    carrier: Option<OwnedShared<S>>,
    anchor: Option<OwnerToken>,
    supervisor: crate::ShutdownHandle,
    capability: StartupCapability,
    fault: Arc<AtomicBool>,
}
impl<F, S: Send + Sync + 'static> StartupFuture<F, S> {
    pub(crate) fn new(inner: F, carrier: OwnedShared<S>, capability: StartupCapability) -> Self {
        let supervisor = carrier.supervisor.clone();
        let anchor = carrier.anchor();
        let fault = carrier.fault.clone();
        Self {
            inner: Some(Box::pin(inner)),
            carrier: Some(carrier),
            anchor: Some(anchor),
            supervisor,
            capability,
            fault,
        }
    }
}
impl<F, S> StartupFuture<F, S> {
    fn fail(&self) {
        self.fault.store(true, Ordering::Release);
        self.supervisor.finish_startup(
            &self.capability,
            crate::startup::CompletionCandidate {
                progress: self.supervisor.preparation_status().progress,
                reason: Some(crate::startup::StartupIncompleteReason::Faulted),
            },
        );
    }
    fn dispose_inner(&mut self) {
        if let Some(inner) = self.inner.take() {
            if let Err(payload) = catch_unwind(AssertUnwindSafe(|| drop(inner))) {
                self.fail();
                dispose_panic(payload);
            }
        }
    }
    fn retire(&mut self) {
        self.dispose_inner();
        // OwnedShared drops Shared and any replacement opaque payload before
        // its own token; the independent same-group anchor is still alive.
        drop(self.carrier.take());
    }
}
impl<F: Future<Output = crate::startup::CompletionCandidate>, S> Future for StartupFuture<F, S> {
    type Output = ();
    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<()> {
        let this = self.get_mut();
        let result = catch_unwind(AssertUnwindSafe(|| {
            this.inner
                .as_mut()
                .expect("startup_future_finished")
                .as_mut()
                .poll(context)
        }));
        match result {
            Ok(Poll::Pending) => Poll::Pending,
            Ok(Poll::Ready(candidate)) => {
                this.retire();
                // The same gate as the shutdown cut rechecks fault after all
                // future/carrier capture destruction, before opening serving.
                this.supervisor.finish_startup(&this.capability, candidate);
                drop(this.anchor.take());
                Poll::Ready(())
            }
            Err(payload) => {
                this.fail();
                dispose_panic(payload);
                this.retire();
                drop(this.anchor.take());
                Poll::Ready(())
            }
        }
    }
}
impl<F, S> Drop for StartupFuture<F, S> {
    fn drop(&mut self) {
        if self.inner.is_some() || self.carrier.is_some() {
            self.fail();
            self.retire();
        }
        drop(self.anchor.take());
    }
}

#[cfg(test)]
#[path = "startup/tests.rs"]
mod startup_coordinator_tests;
#[cfg(test)]
#[path = "startup/ownership_tests.rs"]
mod startup_ownership_tests;
#[cfg(test)]
#[path = "startup/test_support.rs"]
mod startup_test_support;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::*;
    use b4_contract::*;
    use b4_runtime::MemoryStore;
    use std::{
        collections::BTreeMap,
        sync::{Condvar, Mutex},
        time::Duration,
    };
    #[derive(Default)]
    struct Gate {
        entered: tokio::sync::Notify,
        released: Mutex<bool>,
        changed: Condvar,
    }
    impl Gate {
        fn hold(&self) {
            self.entered.notify_one();
            let mut release = self.released.lock().unwrap();
            while !*release {
                release = self.changed.wait(release).unwrap();
            }
        }
        fn release(&self) {
            *self.released.lock().unwrap() = true;
            self.changed.notify_all();
        }
    }
    struct Release(Arc<Gate>);
    impl Drop for Release {
        fn drop(&mut self) {
            self.0.release();
        }
    }
    struct Payload {
        _store: Arc<MemoryStore>,
        gate: Arc<Gate>,
    }
    impl Drop for Payload {
        fn drop(&mut self) {
            self.gate.hold();
        }
    }
    struct Auth {
        store: Arc<MemoryStore>,
        gate: Arc<Gate>,
    }
    impl crate::Authenticator for Auth {
        fn authenticate(
            &self,
            _: &axum::http::HeaderMap,
        ) -> Result<Principal, AuthenticationError> {
            unreachable!()
        }
    }
    impl Drop for Auth {
        fn drop(&mut self) {
            std::panic::panic_any(Payload {
                _store: self.store.clone(),
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
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn async_carrier_cleanup_retains_owner_through_panic_disposal() {
        let gate = Arc::new(Gate::default());
        let _release = Release(gate.clone());
        let store = Arc::new(MemoryStore::new());
        let weak = Arc::downgrade(&store);
        let effect = EffectId::new("effect").unwrap();
        let config = HostConfig::new(vec![ScriptRoute {
            definition: Definition {
                route: RouteKey::new("/work#workflow").unwrap(),
                revision: ExecutionRevision::new("v1").unwrap(),
                batches: vec![vec![TaskSpec {
                    id: TaskId::new("task").unwrap(),
                    effect_id: effect.clone(),
                    requires_approval: false,
                    recovery: RecoveryPolicy::Pure,
                }]],
                max_attempts: 1,
            },
            effects: BTreeMap::from([(effect, ScriptEffect::Void)]),
        }]);
        let host = HttpHost::new(
            store.clone(),
            Arc::new(Auth {
                store: store.clone(),
                gate: gate.clone(),
            }),
            Arc::new(Policy),
            config,
        )
        .unwrap();
        let handle = host.shutdown_handle();
        let owned = OwnedShared::register(host.shared.clone()).unwrap();
        let entered = Arc::new(tokio::sync::Notify::new());
        let started = entered.clone();
        // This direct counted future has no external router/host state capture.
        let job = tokio::spawn(async move {
            let _owned = owned;
            started.notify_one();
            std::future::pending::<()>().await;
        });
        tokio::time::timeout(Duration::from_secs(5), entered.notified())
            .await
            .unwrap();
        handle.begin_shutdown();
        drop(host);
        drop(store);
        job.abort();
        let joined = tokio::spawn(async move {
            let result = job.await;
            drop(result);
        });
        tokio::time::timeout(Duration::from_secs(5), gate.entered.notified())
            .await
            .unwrap();
        assert!(weak.upgrade().is_some());
        assert_eq!(
            handle.wait_quiescent(Duration::ZERO).await.unwrap(),
            ShutdownOutcome::Incomplete {
                active_owners: 1,
                faulted: true
            }
        );
        gate.release();
        assert_eq!(
            handle.wait_quiescent(Duration::from_secs(5)).await.unwrap(),
            ShutdownOutcome::Quiescent { faulted: true }
        );
        joined.await.unwrap();
        assert!(weak.upgrade().is_none());
    }
}
