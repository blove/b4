use super::Gate;
use b4_contract::TaskResult;
use b4_runtime::{
    ControlledNativeOutcome, EffectInvocation, EffectUncertain, InvocationContext,
    NativeCancellationObserver, NativeCancellationSignal, NativeEffect, NativeOutcome,
};
use serde_json::json;
use std::sync::{
    atomic::{AtomicBool, AtomicUsize, Ordering},
    Arc, Mutex,
};

#[derive(Clone, Copy)]
pub enum Return {
    Acknowledge,
    Succeed,
    Fail,
    Uncertain,
    Panic,
    FalseAcknowledgement,
    ForeignSignal,
}
pub struct Callback {
    pub gate: Arc<Gate>,
    pub legacy: AtomicUsize,
    pub controlled: AtomicUsize,
    pub returned: Arc<AtomicBool>,
    pub observed_acceptance: Arc<AtomicBool>,
    pub panicked: AtomicBool,
    pub mode: Return,
    observer: Mutex<Option<NativeCancellationObserver>>,
}
impl Callback {
    pub fn new(mode: Return) -> Arc<Self> {
        Arc::new(Self {
            gate: Gate::new(),
            legacy: AtomicUsize::new(0),
            controlled: AtomicUsize::new(0),
            returned: Arc::new(AtomicBool::new(false)),
            observed_acceptance: Arc::new(AtomicBool::new(false)),
            panicked: AtomicBool::new(false),
            mode,
            observer: Mutex::new(None),
        })
    }
    pub fn observer(&self) -> Option<NativeCancellationObserver> {
        self.observer.lock().unwrap().clone()
    }
    pub fn notified(&self) -> bool {
        self.observer()
            .is_some_and(|observer| observer.cancellation_requested())
    }
    fn finish(&self) -> Result<(), EffectUncertain> {
        if !self.gate.hold() {
            return Err(EffectUncertain);
        }
        Ok(())
    }
}
pub struct Series {
    pub callbacks: Vec<Arc<Callback>>,
    next: AtomicUsize,
}
impl Series {
    pub fn new(callbacks: Vec<Arc<Callback>>) -> Arc<Self> {
        Arc::new(Self {
            callbacks,
            next: AtomicUsize::new(0),
        })
    }
    fn select(&self) -> &Callback {
        &self.callbacks[self.next.fetch_add(1, Ordering::AcqRel)]
    }
}
impl NativeEffect for Series {
    fn invoke(
        &self,
        c: &EffectInvocation,
        x: &InvocationContext,
    ) -> Result<NativeOutcome, EffectUncertain> {
        self.select().invoke(c, x)
    }
    fn invoke_controlled(
        &self,
        c: &EffectInvocation,
        x: &InvocationContext,
        o: &NativeCancellationObserver,
    ) -> Result<ControlledNativeOutcome, EffectUncertain> {
        self.select().invoke_controlled(c, x, o)
    }
}
pub fn genuine_success() -> NativeOutcome {
    NativeOutcome::Succeeded(TaskResult {
        output: json!({"actual":"cooperative-fixture"}),
        writes: vec![b4_contract::StateWrite::Replace {
            field: "effect-result".into(),
            value: json!("actual-write"),
        }],
    })
}
impl NativeEffect for Callback {
    fn invoke(
        &self,
        _: &EffectInvocation,
        _: &InvocationContext,
    ) -> Result<NativeOutcome, EffectUncertain> {
        // The pre-wiring RED must reach real InFlight and durable cancellation.
        // Legacy entry therefore holds benignly and returns a genuine outcome.
        self.legacy.fetch_add(1, Ordering::AcqRel);
        self.finish()?;
        let outcome = genuine_success();
        self.returned.store(true, Ordering::Release);
        Ok(outcome)
    }
    fn invoke_controlled(
        &self,
        _: &EffectInvocation,
        _: &InvocationContext,
        observer: &NativeCancellationObserver,
    ) -> Result<ControlledNativeOutcome, EffectUncertain> {
        self.controlled.fetch_add(1, Ordering::AcqRel);
        *self.observer.lock().unwrap() = Some(observer.clone());
        self.finish()?;
        let outcome = match self.mode {
            Return::Acknowledge => {
                let requested = observer.cancellation_requested();
                self.observed_acceptance.store(requested, Ordering::Release);
                if requested {
                    Ok(ControlledNativeOutcome::Cancelled)
                } else {
                    Err(EffectUncertain)
                }
            }
            Return::FalseAcknowledgement => Ok(ControlledNativeOutcome::Cancelled),
            Return::ForeignSignal => {
                let foreign = NativeCancellationSignal::new();
                foreign.commit_cancellation();
                assert!(foreign.observer().cancellation_requested());
                Ok(ControlledNativeOutcome::Cancelled)
            }
            Return::Succeed => Ok(ControlledNativeOutcome::Settled(genuine_success())),
            Return::Fail => Ok(ControlledNativeOutcome::Settled(NativeOutcome::Failed(
                "known-native-failure".into(),
            ))),
            Return::Uncertain => Err(EffectUncertain),
            Return::Panic => {
                self.panicked.store(true, Ordering::Release);
                panic!("fixture-only callback panic")
            }
        };
        self.returned.store(true, Ordering::Release);
        outcome
    }
}
