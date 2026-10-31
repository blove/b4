use crate::worker::{Terminal, WorkerError};
use crate::{
    viewer::{self, Frame, ViewerSender, ViewerStream},
    HostConfig,
};
use axum::http::StatusCode;
use b4_contract::*;
use b4_runtime::{
    NativeCancellationObserver, NativeCancellationSignal, StoredExecution, ThreadView,
};
use serde_json::{json, Value};
use std::{
    collections::BTreeMap,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Condvar, Mutex, MutexGuard, Weak,
    },
};
use tokio::sync::oneshot;

#[derive(Clone)]
pub(crate) struct RunIdentity {
    pub thread_id: ThreadId,
    pub thread_generation: ThreadGeneration,
    pub execution_id: ExecutionId,
    pub turn_id: TurnId,
    pub owner_generation: u64,
}
impl RunIdentity {
    pub(crate) fn matches(&self, view: &ThreadView) -> bool {
        view.thread.id == self.thread_id
            && view.thread.generation == self.thread_generation
            && view.thread.active_execution_id.as_ref() == Some(&self.execution_id)
            && view
                .execution
                .as_ref()
                .is_some_and(|execution| self.matches_execution(execution))
    }
    pub(crate) fn matches_execution(&self, execution: &StoredExecution) -> bool {
        execution.thread_id == self.thread_id
            && execution.snapshot.execution_id == self.execution_id
            && execution.snapshot.turn_id == self.turn_id
            && execution.snapshot.owner_generation == self.owner_generation
    }
}
#[derive(Clone, Copy, PartialEq)]
enum Gate {
    Active,
    Pending,
    Committed,
    Faulted,
}
pub(crate) struct WithheldCommand {
    pub principal: crate::Principal,
    pub kind: crate::permission_cancellation::OriginKind,
    pub target: b4_runtime::RunCancellationTarget,
    pub facts: TransitionFacts,
}
#[allow(clippy::large_enum_variant)] // One bounded handoff retains its sole owned result inline.
enum Withheld {
    Idle,
    Reserved,
    Ready(WithheldCommand),
    Running,
    Outcome(Result<StoredExecution, b4_runtime::permissions::PermissionError>),
    Delivered,
    Closed,
}
pub(crate) enum PermissionEntry {
    Admitted,
    ReservedRefusal,
    Fault,
}
/// The actual worker closes a stranded handoff on normal return or unwind.
/// It never transports the borrowed Driver failure out of its scope.
pub(crate) struct WithheldWorkerGuard(pub Arc<RunControl>);
impl Drop for WithheldWorkerGuard {
    fn drop(&mut self) {
        let active = {
            let mut state = self
                .0
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            state.withheld_worker_retired = true;
            state.gate == Gate::Pending
                && !matches!(state.withheld, Withheld::Idle | Withheld::Closed)
        };
        if active {
            self.0.fault();
        }
    }
}
struct Publication {
    admission_granted: bool,
    withheld_worker_retired: bool,
    withheld: Withheld,
    ready_confirmation:
        Option<Arc<b4_runtime::permissions::PermissionReadyCancellationConfirmation>>,
    gate: Gate,
    terminal: bool,
    wait: Option<oneshot::Sender<Result<Terminal, WorkerError>>>,
    viewer: Option<ViewerSender>,
    attached: BTreeMap<uuid::Uuid, ViewerSender>,
    digest: Option<Vec<Value>>,
    digest_bytes: usize,
    cached_terminal: Option<Value>,
    native_frames: usize,
    native_bytes: usize,
    native_closed: bool,
}
pub(crate) struct RunMetadata {
    pub anchor: ExecutionId,
    pub values: Value,
    pub input: Value,
    pub started_at: Value,
    pub resume: bool,
    pub recovered: bool,
}
pub(crate) enum Subscription {
    Inactive,
    Unavailable,
    Faulted,
    Capacity,
    Live { state: Value, stream: ViewerStream },
}
pub(crate) struct SubscriptionGuard {
    control: Weak<RunControl>,
    id: uuid::Uuid,
}
impl Drop for SubscriptionGuard {
    fn drop(&mut self) {
        if let Some(control) = self.control.upgrade() {
            let mut state = control
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if let Some(sender) = state.attached.remove(&self.id) {
                sender.discard();
            }
        }
    }
}
pub(crate) struct RunControl {
    pub identity: RunIdentity,
    native_cancellation: NativeCancellationSignal,
    fault: Arc<AtomicBool>,
    state: Mutex<Publication>,
    changed: Condvar,
    metadata: RunMetadata,
    max_attached: usize,
    max_frames: usize,
    max_bytes: usize,
    digest_frames: usize,
    digest_bytes: usize,
}
impl RunControl {
    pub(crate) fn new(
        identity: RunIdentity,
        fault: Arc<AtomicBool>,
        wait: oneshot::Sender<Result<Terminal, WorkerError>>,
        viewer: Option<ViewerSender>,
        metadata: RunMetadata,
        config: &HostConfig,
    ) -> Self {
        Self {
            identity,
            native_cancellation: NativeCancellationSignal::new(),
            fault,
            state: Mutex::new(Publication {
                admission_granted: false,
                withheld_worker_retired: false,
                withheld: Withheld::Idle,
                ready_confirmation: None,
                gate: Gate::Active,
                terminal: false,
                wait: Some(wait),
                viewer,
                attached: BTreeMap::new(),
                digest: (!metadata.recovered && config.turn_buffer_bytes >= 2).then(Vec::new),
                digest_bytes: 2,
                cached_terminal: None,
                native_frames: 0,
                native_bytes: 0,
                native_closed: false,
            }),
            changed: Condvar::new(),
            metadata,
            max_attached: config.max_attached_viewers,
            max_frames: config.viewer_queue_frames,
            max_bytes: config.viewer_queue_bytes,
            digest_frames: config.turn_buffer_frames,
            digest_bytes: config.turn_buffer_bytes,
        }
    }
    pub(crate) fn native_cancellation_observer(&self) -> NativeCancellationObserver {
        self.native_cancellation.observer()
    }
    fn settled_gate(&self) -> MutexGuard<'_, Publication> {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        while state.gate == Gate::Pending {
            state = self
                .changed
                .wait(state)
                .unwrap_or_else(std::sync::PoisonError::into_inner);
        }
        state
    }
    pub(crate) fn enter_callback(&self) -> Result<bool, &'static str> {
        let state = self.settled_gate();
        match state.gate {
            Gate::Committed => Ok(false),
            Gate::Active if !state.terminal => Ok(true),
            _ => Err("run_gate_faulted"),
        }
    }
    /// Entry eligibility and dispatch-intent admission share one gate decision.
    /// The returned decision never retains the publication lock in user code.
    pub(crate) fn enter_native_callback(
        &self,
        prepared: Result<crate::native_stream::Prepared, ()>,
    ) -> Result<bool, &'static str> {
        let mut state = self.settled_gate();
        match state.gate {
            Gate::Committed => Ok(false),
            Gate::Active if !state.terminal => {
                self.native_publish(&mut state, prepared);
                Ok(true)
            }
            _ => Err("run_gate_faulted"),
        }
    }
    pub(crate) fn native_chunk(
        &self,
        prepared: Result<crate::native_stream::Prepared, ()>,
    ) -> b4_runtime::NativeChunkResult {
        use b4_runtime::NativeChunkResult;
        let mut state = match self.state.try_lock() {
            Ok(state) => state,
            Err(std::sync::TryLockError::WouldBlock) => return NativeChunkResult::Unavailable,
            Err(std::sync::TryLockError::Poisoned(_)) => return NativeChunkResult::Closed,
        };
        if state.terminal
            || matches!(state.gate, Gate::Committed | Gate::Faulted)
            || state.native_closed
        {
            return NativeChunkResult::Closed;
        }
        if state.gate == Gate::Pending {
            return NativeChunkResult::Unavailable;
        }
        self.native_publish(&mut state, prepared)
    }
    pub(crate) fn native_result(&self, prepared: Result<crate::native_stream::Prepared, ()>) {
        let mut state = self.settled_gate();
        if state.gate == Gate::Active && !state.terminal {
            self.native_publish(&mut state, prepared);
        }
    }
    fn native_publish(
        &self,
        state: &mut Publication,
        prepared: Result<crate::native_stream::Prepared, ()>,
    ) -> b4_runtime::NativeChunkResult {
        use b4_runtime::NativeChunkResult;
        if state.native_closed {
            return NativeChunkResult::Closed;
        }
        let admitted = prepared.ok().filter(|p| {
            state.native_frames < self.digest_frames
                && state
                    .native_bytes
                    .checked_add(p.frame.bytes)
                    .is_some_and(|bytes| bytes <= self.digest_bytes)
                && state
                    .digest_bytes
                    .checked_add(p.entry_bytes)
                    .and_then(|bytes| bytes.checked_add(usize::from(state.native_frames > 0)))
                    .is_some_and(|bytes| bytes <= self.digest_bytes)
        });
        let Some(prepared) = admitted else {
            state.native_closed = true;
            state.digest = None;
            if let Some(sender) = state.viewer.take() {
                sender.close();
            }
            for (_, sender) in std::mem::take(&mut state.attached) {
                sender.close();
            }
            return NativeChunkResult::Limit;
        };
        state.native_frames += 1;
        state.native_bytes += prepared.frame.bytes;
        state.digest_bytes += prepared.entry_bytes + usize::from(state.native_frames > 1);
        if let Some(digest) = &mut state.digest {
            digest.push(prepared.entry);
        }
        if state
            .viewer
            .as_ref()
            .is_some_and(|sender| !sender.send(prepared.frame.clone()))
        {
            state.viewer = None;
        }
        state
            .attached
            .retain(|_, sender| sender.send(prepared.frame.clone()));
        NativeChunkResult::Accepted
    }
    fn withheld_state(&self) -> Option<MutexGuard<'_, Publication>> {
        match self.state.lock() {
            Ok(state) => Some(state),
            Err(poison) => {
                drop(poison.into_inner());
                self.fault();
                None
            }
        }
    }
    fn withheld_wait<'a>(
        &self,
        state: MutexGuard<'a, Publication>,
    ) -> Option<MutexGuard<'a, Publication>> {
        match self.changed.wait(state) {
            Ok(state) => Some(state),
            Err(poison) => {
                drop(poison.into_inner());
                self.fault();
                None
            }
        }
    }
    /// Admission and withdrawal reservation use the same mutex. An admission
    /// grant is only a veto; actual entry is recorded independently by Tracker.
    pub(crate) fn permission_withheld_enter(&self) -> PermissionEntry {
        let Some(mut state) = self.withheld_state() else {
            return PermissionEntry::Fault;
        };
        loop {
            if state.gate == Gate::Pending
                && matches!(state.withheld, Withheld::Reserved | Withheld::Ready(_))
            {
                return PermissionEntry::ReservedRefusal;
            }
            if state.gate != Gate::Pending {
                break;
            }
            state = match self.withheld_wait(state) {
                Some(state) => state,
                None => return PermissionEntry::Fault,
            };
        }
        if state.gate == Gate::Active && !state.terminal && matches!(state.withheld, Withheld::Idle)
        {
            state.admission_granted = true;
            PermissionEntry::Admitted
        } else {
            PermissionEntry::Fault
        }
    }
    pub(crate) fn reserve_permission_withheld(
        self: &Arc<Self>,
    ) -> Result<Option<CancellationPending>, &'static str> {
        let mut state = self.withheld_state().ok_or("run_gate_faulted")?;
        while state.gate == Gate::Pending {
            state = self.withheld_wait(state).ok_or("run_gate_faulted")?;
        }
        if state.gate == Gate::Faulted {
            return Err("run_gate_faulted");
        }
        if state.gate != Gate::Active
            || state.terminal
            || state.admission_granted
            || state.withheld_worker_retired
            || !matches!(state.withheld, Withheld::Idle)
        {
            return Ok(None);
        }
        state.gate = Gate::Pending;
        state.withheld = Withheld::Reserved;
        self.changed.notify_all();
        Ok(Some(CancellationPending {
            control: self.clone(),
            resolved: false,
        }))
    }
    pub(crate) fn permission_withheld_ready(&self, command: WithheldCommand) -> bool {
        let Some(mut state) = self.withheld_state() else {
            return false;
        };
        if state.gate != Gate::Pending || !matches!(state.withheld, Withheld::Reserved) {
            return false;
        }
        state.withheld = Withheld::Ready(command);
        self.changed.notify_all();
        true
    }
    /// Called only after the scoped Driver returns its actual drained failure.
    pub(crate) fn permission_withheld_command(&self) -> Option<WithheldCommand> {
        let mut state = self.withheld_state()?;
        if matches!(state.withheld, Withheld::Idle) {
            // Close the no-command drain race before ready_confirmation can
            // wait on a newly reserved Pending that needs this same worker.
            state.withheld_worker_retired = true;
        }
        while state.gate == Gate::Pending && matches!(state.withheld, Withheld::Reserved) {
            state = self.withheld_wait(state)?;
        }
        if state.gate != Gate::Pending || !matches!(state.withheld, Withheld::Ready(_)) {
            return None;
        }
        let Withheld::Ready(command) = std::mem::replace(&mut state.withheld, Withheld::Running)
        else {
            unreachable!()
        };
        Some(command)
    }
    pub(crate) fn permission_withheld_result(
        &self,
    ) -> Option<Result<StoredExecution, b4_runtime::permissions::PermissionError>> {
        let mut state = self.withheld_state()?;
        while state.gate == Gate::Pending
            && matches!(state.withheld, Withheld::Ready(_) | Withheld::Running)
        {
            state = self.withheld_wait(state)?;
        }
        if state.gate != Gate::Pending || !matches!(state.withheld, Withheld::Outcome(_)) {
            return None;
        }
        let Withheld::Outcome(result) = std::mem::replace(&mut state.withheld, Withheld::Delivered)
        else {
            unreachable!()
        };
        Some(result)
    }
    /// Keep the actual failure and all worker owners alive until management
    /// resolves Pending. Condvar releases the mutex while either party waits.
    pub(crate) fn permission_withheld_outcome(
        &self,
        outcome: Result<StoredExecution, b4_runtime::permissions::PermissionError>,
    ) -> bool {
        {
            let Some(mut state) = self.withheld_state() else {
                return false;
            };
            if state.gate != Gate::Pending || !matches!(state.withheld, Withheld::Running) {
                return false;
            }
            state.withheld = Withheld::Outcome(outcome);
            self.changed.notify_all();
        }
        let Some(mut state) = self.withheld_state() else {
            return false;
        };
        while state.gate == Gate::Pending {
            state = match self.withheld_wait(state) {
                Some(state) => state,
                None => return false,
            };
        }
        state.gate == Gate::Committed
    }
    pub(crate) fn ready_confirmation(
        &self,
    ) -> Option<Arc<b4_runtime::permissions::PermissionReadyCancellationConfirmation>> {
        self.settled_gate().ready_confirmation.clone()
    }
    pub(crate) fn committed(&self) -> bool {
        self.settled_gate().gate == Gate::Committed
    }
    pub(crate) fn begin_cancel(
        self: &Arc<Self>,
    ) -> Result<Option<CancellationPending>, &'static str> {
        let mut state = self.settled_gate();
        match state.gate {
            Gate::Active if !state.terminal => {
                state.gate = Gate::Pending;
                Ok(Some(CancellationPending {
                    control: self.clone(),
                    resolved: false,
                }))
            }
            Gate::Faulted => Err("run_gate_faulted"),
            _ => Ok(None),
        }
    }
    fn frame(state: &mut Publication, name: &'static str, payload: Value) {
        let frame = Frame::new(name, &payload);
        if state
            .viewer
            .as_ref()
            .is_some_and(|sender| !sender.send(frame.clone()))
        {
            state.viewer = None;
        }
        state
            .attached
            .retain(|_, sender| sender.send(frame.clone()));
    }
    pub(crate) fn publish(&self, name: &'static str, payload: Value) {
        let mut state = self.settled_gate();
        if state.gate == Gate::Active && !state.terminal {
            let previous_bytes = state.digest_bytes;
            if let Some(digest) = &mut state.digest {
                let entry = json!({"event":name,"data":payload});
                let bytes = entry.to_string().len() + usize::from(!digest.is_empty());
                if digest.len() >= self.digest_frames
                    || previous_bytes
                        .checked_add(bytes)
                        .is_none_or(|sum| sum > self.digest_bytes)
                {
                    state.digest = None;
                } else {
                    digest.push(entry);
                    state.digest_bytes = previous_bytes + bytes;
                }
            }
            Self::frame(&mut state, name, payload);
        }
    }
    fn terminal(state: &mut Publication, terminal: Terminal) {
        if state.terminal {
            return;
        }
        state.terminal = true;
        state.cached_terminal = Some(terminal.done.clone());
        Self::frame(state, "done", terminal.done.clone());
        if let Some(sender) = state.viewer.take() {
            sender.close();
        }
        for (_, sender) in std::mem::take(&mut state.attached) {
            sender.close();
        }
        if let Some(sender) = state.wait.take() {
            let _ = sender.send(Ok(terminal));
        }
    }
    pub(crate) fn finish(&self, terminal: Terminal) {
        let mut state = self.settled_gate();
        if state.gate == Gate::Active {
            Self::terminal(&mut state, terminal);
        }
    }
    pub(crate) fn reject(&self, error: WorkerError) {
        let mut state = self.settled_gate();
        if state.gate == Gate::Active && !state.terminal {
            state.terminal = true;
            if let Some(sender) = state.viewer.take() {
                sender.close();
            }
            for (_, sender) in std::mem::take(&mut state.attached) {
                sender.close();
            }
            if let Some(sender) = state.wait.take() {
                let _ = sender.send(Err(error));
            }
        }
    }
    pub(crate) fn fault(&self) {
        self.fault.store(true, Ordering::Release);
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        state.gate = Gate::Faulted;
        state.withheld = Withheld::Closed;
        if state.cached_terminal.is_none() {
            state.terminal = true;
            if let Some(sender) = state.viewer.take() {
                sender.close();
            }
            for (_, sender) in std::mem::take(&mut state.attached) {
                sender.close();
            }
            state.wait = None;
        }
        self.changed.notify_all();
    }
    pub(crate) fn subscribe(self: &Arc<Self>, status: &str) -> Subscription {
        let (cut, mut stream, id) = {
            // Pending freezes publication, but must never make policy wait for
            // its own callback. This cut deliberately does not wait on the gate.
            let mut state = self
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if state.cached_terminal.is_some() {
                return Subscription::Inactive;
            }
            if state.gate == Gate::Faulted {
                return Subscription::Faulted;
            }
            if state.terminal {
                return Subscription::Unavailable;
            }
            if state.attached.len() >= self.max_attached {
                return Subscription::Capacity;
            }
            let id = uuid::Uuid::new_v4();
            let (sender, stream) = viewer::channel(self.max_frames, self.max_bytes);
            let mut cut = json!({"anchor":self.metadata.anchor,"input":self.metadata.input,"interrupts":[],"live":true,"resume":self.metadata.resume,"run_started_at":self.metadata.started_at,"status":status,"turn":state.digest,"values":self.metadata.values});
            if state.digest.is_none() {
                cut.as_object_mut()
                    .unwrap()
                    .insert("turn_truncated".into(), json!(true));
            }
            state.attached.insert(id, sender);
            (cut, stream, id)
        };
        stream.guard(SubscriptionGuard {
            control: Arc::downgrade(self),
            id,
        });
        Subscription::Live { state: cut, stream }
    }
}

pub(crate) struct CancellationPending {
    control: Arc<RunControl>,
    resolved: bool,
}
impl CancellationPending {
    pub(crate) fn reject(mut self) {
        let withheld = {
            let state = self
                .control
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            !matches!(state.withheld, Withheld::Idle)
        };
        if withheld {
            self.control.fault();
            self.resolved = true;
            return;
        }
        let mut state = self
            .control
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if state.gate == Gate::Pending {
            state.gate = Gate::Active;
        }
        self.resolved = true;
        self.control.changed.notify_all();
    }
    pub(crate) fn commit(self) -> bool {
        self.commit_inner(None, false)
    }
    pub(crate) fn commit_ready(
        self,
        confirmation: b4_runtime::permissions::PermissionReadyCancellationConfirmation,
    ) -> bool {
        self.commit_inner(Some(confirmation), false)
    }
    pub(crate) fn commit_withheld(self) -> bool {
        self.commit_inner(None, true)
    }
    fn commit_inner(
        mut self,
        confirmation: Option<b4_runtime::permissions::PermissionReadyCancellationConfirmation>,
        withheld: bool,
    ) -> bool {
        let mut state = match self.control.state.lock() {
            Ok(state) => state,
            Err(poison) if withheld => {
                drop(poison.into_inner());
                self.control.fault();
                self.resolved = true;
                return false;
            }
            Err(poison) => poison.into_inner(),
        };
        let committed = state.gate == Gate::Pending
            && (!withheld || matches!(state.withheld, Withheld::Delivered));
        if committed {
            state.gate = Gate::Committed;
            if let Some(confirmation) = confirmation {
                // The manager has checked the typed result against its captured claim and target.
                state.ready_confirmation = Some(Arc::new(confirmation));
            } else if !withheld {
                self.control.native_cancellation.commit_cancellation();
            }
            RunControl::terminal(
                &mut state,
                Terminal {
                    wait_status: StatusCode::CONFLICT,
                    wait: json!({"error":{"kind":"request_error","message":"run_cancelled","details":{"code":"run_cancelled"}}}),
                    done: json!({"output":{"cancelled":true}}),
                },
            );
        }
        self.resolved = true;
        self.control.changed.notify_all();
        committed
    }
}
impl Drop for CancellationPending {
    fn drop(&mut self) {
        if !self.resolved {
            self.control.fault();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::{body::to_bytes, response::sse::Event};
    use std::time::Duration;
    fn control() -> Arc<RunControl> {
        control_with(HostConfig::new(vec![]))
    }
    fn control_with(config: HostConfig) -> Arc<RunControl> {
        let (wait, _) = oneshot::channel();
        Arc::new(RunControl::new(
            RunIdentity {
                thread_id: ThreadId::new("thread").unwrap(),
                thread_generation: ThreadGeneration::new("0".repeat(64)).unwrap(),
                execution_id: ExecutionId::new("execution").unwrap(),
                turn_id: TurnId::new("turn").unwrap(),
                owner_generation: 1,
            },
            Arc::new(AtomicBool::new(false)),
            wait,
            None,
            RunMetadata {
                anchor: ExecutionId::new("execution").unwrap(),
                values: json!({"input":"original"}),
                input: json!("original"),
                started_at: json!("time"),
                resume: false,
                recovered: false,
            },
            &config,
        ))
    }
    fn permission_withheld_command_for(control: &RunControl) -> WithheldCommand {
        WithheldCommand {
            principal: crate::Principal {
                application: "app".into(),
                tenant: "tenant".into(),
                principal: "actor".into(),
            },
            kind: crate::permission_cancellation::OriginKind::OriginalAlways,
            target: b4_runtime::RunCancellationTarget {
                thread_id: control.identity.thread_id.clone(),
                thread_generation: control.identity.thread_generation.clone(),
                execution_id: control.identity.execution_id.clone(),
                turn_id: control.identity.turn_id.clone(),
                execution_fence: b4_runtime::ExecutionFence {
                    revision: 4,
                    owner_generation: 1,
                },
                resource_revision: 4,
            },
            facts: TransitionFacts { now_ms: 1 },
        }
    }
    #[test]
    fn permission_withheld_requested_refusal_is_explicit_and_does_not_fault() {
        let requested = control();
        let pending = requested.reserve_permission_withheld().unwrap().unwrap();
        assert!(matches!(
            requested.permission_withheld_enter(),
            PermissionEntry::ReservedRefusal
        ));
        assert!(!requested.fault.load(Ordering::Acquire));
        drop(pending);
        assert!(matches!(
            requested.permission_withheld_enter(),
            PermissionEntry::Fault
        ));
        assert!(requested.fault.load(Ordering::Acquire));
        let ordinary = control();
        ordinary.begin_cancel().unwrap().unwrap().commit();
        // An ordinary false/committed gate is never misidentified as the exact
        // reserved refusal that the permission Hooks adapter may pass through.
        assert!(matches!(
            ordinary.permission_withheld_enter(),
            PermissionEntry::Fault
        ));
    }
    #[test]
    fn permission_withheld_admission_before_body_vetoes_reservation_without_entry_proof() {
        let control = control();
        assert!(matches!(
            control.permission_withheld_enter(),
            PermissionEntry::Admitted
        ));
        // No callback invocation or Tracker record is manufactured here.
        assert!(control.reserve_permission_withheld().unwrap().is_none());
        assert!(!control
            .native_cancellation_observer()
            .cancellation_requested());
        assert!(!control.fault.load(Ordering::Acquire));
        control.begin_cancel().unwrap().unwrap().reject();
    }
    #[test]
    fn permission_withheld_no_command_drain_closes_reservation_before_ordinary_wait() {
        let control = control();
        // A real noneligible Driver failure can drain before management reserves.
        assert!(control.permission_withheld_command().is_none());
        assert!(control.reserve_permission_withheld().unwrap().is_none());
        assert!(control.ready_confirmation().is_none());
        let pending = control.begin_cancel().unwrap().unwrap();
        assert!(control.permission_withheld_command().is_none());
        pending.reject();
        assert!(control.ready_confirmation().is_none());
        assert!(!control.fault.load(Ordering::Acquire));
    }
    #[test]
    fn permission_withheld_one_command_and_one_error_outcome_wait_for_pending_resolution() {
        let control = control();
        let pending = control.reserve_permission_withheld().unwrap().unwrap();
        assert!(control.permission_withheld_ready(permission_withheld_command_for(&control)));
        assert!(!control.permission_withheld_ready(permission_withheld_command_for(&control)));
        assert!(control.permission_withheld_command().is_some());
        assert!(control.permission_withheld_command().is_none());
        let (send, receive) = std::sync::mpsc::channel();
        let worker = {
            let control = control.clone();
            std::thread::spawn(move || {
                let resolved = control.permission_withheld_outcome(Err(
                    b4_runtime::permissions::PermissionError::UnsupportedProfile,
                ));
                send.send(resolved).unwrap();
            })
        };
        assert!(matches!(
            control.permission_withheld_result(),
            Some(Err(
                b4_runtime::permissions::PermissionError::UnsupportedProfile
            ))
        ));
        assert!(control.permission_withheld_result().is_none());
        assert!(
            receive.try_recv().is_err(),
            "worker retains owners through Pending"
        );
        pending.reject();
        assert!(!receive.recv_timeout(Duration::from_secs(1)).unwrap());
        worker.join().unwrap();
        assert!(!control.permission_withheld_outcome(Err(
            b4_runtime::permissions::PermissionError::UnsupportedProfile
        )));
        assert!(!control
            .native_cancellation_observer()
            .cancellation_requested());
    }
    #[test]
    fn permission_withheld_preflight_refusal_and_worker_closure_wake_both_sides() {
        for explicit_refusal in [true, false] {
            let control = control();
            let pending = control.reserve_permission_withheld().unwrap().unwrap();
            let (send, receive) = std::sync::mpsc::channel();
            let worker = {
                let control = control.clone();
                std::thread::spawn(move || {
                    send.send(control.permission_withheld_command().is_none())
                        .unwrap();
                })
            };
            if explicit_refusal {
                pending.reject();
            } else {
                drop(pending);
            }
            assert!(receive.recv_timeout(Duration::from_secs(1)).unwrap());
            worker.join().unwrap();
            assert!(control.permission_withheld_result().is_none());
            assert!(control.fault.load(Ordering::Acquire));
        }
        let control = control();
        let pending = control.reserve_permission_withheld().unwrap().unwrap();
        assert!(control.permission_withheld_ready(permission_withheld_command_for(&control)));
        drop(WithheldWorkerGuard(control.clone()));
        assert!(control.permission_withheld_result().is_none());
        assert!(control.permission_withheld_command().is_none());
        drop(pending);
    }
    #[tokio::test]
    async fn cut_during_pending_does_not_wait_and_late_fault_keeps_selected_cancel_terminal() {
        let control = control();
        let pending = control.begin_cancel().unwrap().unwrap();
        let (send, receive) = std::sync::mpsc::channel();
        {
            let control = control.clone();
            std::thread::spawn(move || {
                let _ = send.send(control.subscribe("busy"));
            });
        }
        let cut = receive.recv_timeout(Duration::from_millis(500));
        assert!(pending.commit());
        let Subscription::Live { state, mut stream } = cut.expect("cut must finish while Pending")
        else {
            panic!("expected live cut");
        };
        stream.prefix(vec![Event::default()
            .event("state")
            .data(state.to_string())]);
        control.fault();
        let body = to_bytes(viewer::response(stream, 1000).into_body(), 10000)
            .await
            .unwrap();
        let text = String::from_utf8(body.to_vec()).unwrap();
        assert!(text.starts_with("event: state"));
        assert_eq!(text.matches("event: done").count(), 1);
        assert!(text.contains("\"cancelled\":true"));
        assert!(control.fault.load(Ordering::Acquire));
    }
    #[tokio::test]
    async fn atomic_digest_cut_has_each_frame_once_before_its_tail() {
        let control = control();
        control.publish("chunk", json!("before"));
        let Subscription::Live { state, mut stream } = control.subscribe("busy") else {
            panic!("expected live cut");
        };
        assert_eq!(state["turn"], json!([{"event":"chunk","data":"before"}]));
        stream.prefix(vec![Event::default()
            .event("state")
            .data(state.to_string())]);
        control.publish("chunk", json!("after"));
        control.finish(Terminal {
            wait_status: StatusCode::OK,
            wait: Value::Null,
            done: json!({"output":null}),
        });
        let text = String::from_utf8(
            to_bytes(viewer::response(stream, 1000).into_body(), 10000)
                .await
                .unwrap()
                .to_vec(),
        )
        .unwrap();
        assert_eq!(text.matches("before").count(), 1);
        assert_eq!(text.matches("after").count(), 1);
        assert_eq!(text.matches("event: done").count(), 1);
    }
    #[tokio::test]
    async fn native_closed_projection_allows_truncated_attach_and_terminal() {
        let mut config = HostConfig::new(vec![]);
        config.turn_buffer_frames = 2;
        let (original_sender, original_stream) =
            viewer::channel(config.viewer_queue_frames, config.viewer_queue_bytes);
        let (wait, _) = oneshot::channel();
        let control = Arc::new(RunControl::new(
            RunIdentity {
                thread_id: ThreadId::new("thread").unwrap(),
                thread_generation: ThreadGeneration::new("0".repeat(64)).unwrap(),
                execution_id: ExecutionId::new("execution").unwrap(),
                turn_id: TurnId::new("turn").unwrap(),
                owner_generation: 1,
            },
            Arc::new(AtomicBool::new(false)),
            wait,
            Some(original_sender),
            RunMetadata {
                anchor: ExecutionId::new("execution").unwrap(),
                values: json!({"input":"original"}),
                input: json!("original"),
                started_at: json!("time"),
                resume: false,
                recovered: false,
            },
            &config,
        ));
        let call = b4_runtime::EffectInvocation {
            execution_id: ExecutionId::new("execution").unwrap(),
            turn_id: TurnId::new("turn").unwrap(),
            owner_generation: 1,
            task_id: TaskId::new("task").unwrap(),
            effect_id: EffectId::new("effect").unwrap(),
            attempt_id: AttemptId::new("attempt").unwrap(),
            input: serde_json::Map::from_iter([("input".into(), json!("original"))]),
        };

        assert_eq!(
            control.enter_native_callback(crate::native_stream::tool_call(
                &call,
                config.max_output_bytes,
            )),
            Ok(true)
        );
        assert_eq!(
            control.native_chunk(crate::native_stream::chunk(
                &json!("accepted"),
                config.max_output_bytes,
            )),
            b4_runtime::NativeChunkResult::Accepted
        );
        assert_eq!(
            control.native_chunk(crate::native_stream::chunk(
                &json!("limit"),
                config.max_output_bytes,
            )),
            b4_runtime::NativeChunkResult::Limit
        );
        assert_eq!(
            control.native_chunk(crate::native_stream::chunk(
                &json!("closed"),
                config.max_output_bytes,
            )),
            b4_runtime::NativeChunkResult::Closed
        );
        let original = String::from_utf8(
            to_bytes(
                viewer::response(original_stream, 1000).into_body(),
                config.viewer_queue_bytes,
            )
            .await
            .unwrap()
            .to_vec(),
        )
        .unwrap();
        assert_eq!(original.matches("event: tool_call").count(), 1);
        assert_eq!(original.matches("event: chunk").count(), 1);
        assert!(original.contains("accepted"));
        assert!(!original.contains("event: tool_result"));
        assert!(!original.contains("event: done"));
        assert!(!original.contains("limit"));
        assert!(!original.contains("closed"));

        let Subscription::Live { state, mut stream } = control.subscribe("busy") else {
            panic!("expected live truncated attachment");
        };
        assert_eq!(state["live"], true);
        assert_eq!(state["status"], "busy");
        assert_eq!(state["turn"], Value::Null);
        assert_eq!(state["turn_truncated"], true);
        stream.prefix(vec![Event::default()
            .event("state")
            .data(state.to_string())]);
        control.native_result(crate::native_stream::tool_result(
            &call,
            &TaskResult {
                output: json!({"projected":"result"}),
                writes: vec![],
            },
            config.max_output_bytes,
        ));
        control.finish(Terminal {
            wait_status: StatusCode::OK,
            wait: Value::Null,
            done: json!({"output":{"authoritative":"terminal"}}),
        });
        let attached = String::from_utf8(
            to_bytes(
                viewer::response(stream, 1000).into_body(),
                config.viewer_queue_bytes,
            )
            .await
            .unwrap()
            .to_vec(),
        )
        .unwrap();
        assert_eq!(attached.matches("event: state").count(), 1);
        assert_eq!(attached.matches("event: done").count(), 1);
        assert!(attached.contains("authoritative"));
        assert!(!attached.contains("event: chunk"));
        assert!(!attached.contains("event: tool_call"));
        assert!(!attached.contains("event: tool_result"));
    }
    #[test]
    fn subscriber_drop_immediately_releases_quiet_capacity() {
        let control = control();
        let mut viewers = Vec::new();
        for _ in 0..16 {
            viewers.push(control.subscribe("busy"));
        }
        assert!(matches!(control.subscribe("busy"), Subscription::Capacity));
        drop(viewers.pop());
        assert!(matches!(
            control.subscribe("busy"),
            Subscription::Live { .. }
        ));
    }
    #[test]
    fn inactive_cut_requires_a_selected_terminal_even_when_publication_is_closed() {
        let rejected = control();
        rejected.reject(WorkerError::Rejected("recovery_attempt_limit"));
        assert!(matches!(
            rejected.subscribe("busy"),
            Subscription::Unavailable
        ));
        let faulted = control();
        faulted.fault();
        assert!(matches!(faulted.subscribe("busy"), Subscription::Faulted));
        let completed = control();
        completed.finish(Terminal {
            wait_status: StatusCode::OK,
            wait: Value::Null,
            done: json!({"output":null}),
        });
        completed.fault();
        assert!(matches!(
            completed.subscribe("busy"),
            Subscription::Inactive
        ));
    }
    #[test]
    fn digest_bound_counts_the_entire_utf8_array_including_brackets_and_commas() {
        let entries = json!([{"event":"chunk","data":"😀"},{"event":"chunk","data":"tail"}]);
        let bytes = entries.to_string().len();
        for limit in [bytes, bytes - 1] {
            let mut config = HostConfig::new(vec![]);
            config.turn_buffer_bytes = limit;
            let control = control_with(config);
            control.publish("chunk", json!("😀"));
            control.publish("chunk", json!("tail"));
            let Subscription::Live { state, .. } = control.subscribe("busy") else {
                panic!("live cut");
            };
            if limit == bytes {
                assert_eq!(state["turn"], entries);
            } else {
                assert_eq!(state["turn"], Value::Null);
                assert_eq!(state["turn_truncated"], true);
            }
        }
    }
}
